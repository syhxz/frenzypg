package server

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
)

// ConnectionAffinity manages dedicated connections for sessions that are in active transactions.
// Non-transactional queries acquire and immediately release connections from the pool.
type ConnectionAffinity struct {
	pool               *pgxpool.Pool
	sessionConnections map[string]*pgxpool.Conn
	sessionStates      map[string]TransactionState
	sessionLastUsed    map[string]time.Time
	mutex              sync.Mutex // Use a single mutex (not RW) to prevent read-during-write races
	logger             *zap.Logger
	cleanupTicker      *time.Ticker
	stopCleanup        chan struct{}
}

func NewConnectionAffinity(pool *pgxpool.Pool, logger *zap.Logger) *ConnectionAffinity {
	ca := &ConnectionAffinity{
		pool:               pool,
		sessionConnections: make(map[string]*pgxpool.Conn),
		sessionStates:      make(map[string]TransactionState),
		sessionLastUsed:    make(map[string]time.Time),
		logger:             logger,
		cleanupTicker:      time.NewTicker(5 * time.Second),
		stopCleanup:        make(chan struct{}),
	}

	go ca.cleanupIdleSessions()

	return ca
}

// GetConnection returns a pinned connection for in-transaction sessions,
// or acquires a fresh one for non-transactional queries.
// For non-transactional queries, caller MUST call ReleaseIfIdle after use.
func (ca *ConnectionAffinity) GetConnection(ctx context.Context, sessionID string) (*pgxpool.Conn, error) {
	ca.mutex.Lock()
	defer ca.mutex.Unlock()

	ca.sessionLastUsed[sessionID] = time.Now()

	// If session already has a pinned connection (in transaction), reuse it
	if conn, exists := ca.sessionConnections[sessionID]; exists {
		return conn, nil
	}

	// Acquire a new connection
	conn, err := ca.pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire connection: %w", err)
	}

	// Pin connection for this session
	ca.sessionConnections[sessionID] = conn
	if _, hasState := ca.sessionStates[sessionID]; !hasState {
		ca.sessionStates[sessionID] = TransactionIdle
	}

	return conn, nil
}

// ReleaseIfIdle releases the connection if the session is NOT in an active transaction.
// Instead of releasing immediately, we keep the connection pinned for a short grace period
// to avoid the expensive acquire/release cycle on every transaction boundary.
// The cleanup goroutine will release truly idle sessions.
func (ca *ConnectionAffinity) ReleaseIfIdle(sessionID string) {
	ca.mutex.Lock()
	defer ca.mutex.Unlock()

	state := ca.sessionStates[sessionID]
	if state == TransactionInProgress {
		// Keep connection pinned during transaction
		return
	}

	// DON'T release immediately — keep pinned for session reuse.
	// The next BEGIN from the same session will reuse this connection
	// without a pool Acquire. The cleanup goroutine handles truly idle ones.
	ca.sessionLastUsed[sessionID] = time.Now()
}

// ReleaseConnection immediately releases the connection for a session back to the pool.
// Used when a transaction ends (COMMIT/ROLLBACK) to avoid holding connections unnecessarily.
func (ca *ConnectionAffinity) ReleaseConnection(sessionID string) {
	ca.mutex.Lock()
	defer ca.mutex.Unlock()

	if conn, exists := ca.sessionConnections[sessionID]; exists {
		conn.Release()
		delete(ca.sessionConnections, sessionID)
		delete(ca.sessionStates, sessionID)
		delete(ca.sessionLastUsed, sessionID)
	}
}

// UpdateAndCheckRelease atomically updates transaction state and returns true if
// the session should be released (not in transaction after the update).
func (ca *ConnectionAffinity) UpdateAndCheckRelease(sessionID string, query string) bool {
	ca.mutex.Lock()
	defer ca.mutex.Unlock()

	_, cmdType := IsTransactionCommand(query)
	if cmdType == "" {
		// Not a transaction command — check current state
		state := ca.sessionStates[sessionID]
		return state != TransactionInProgress
	}

	switch cmdType {
	case "BEGIN":
		ca.sessionStates[sessionID] = TransactionInProgress
		ca.logger.Debug("Transaction started", zap.String("session", sessionID))
		return false // Don't release — transaction just started
	case "COMMIT", "ROLLBACK":
		ca.sessionStates[sessionID] = TransactionIdle
		ca.sessionLastUsed[sessionID] = time.Now()
		ca.logger.Debug("Transaction ended", zap.String("session", sessionID), zap.String("command", cmdType))
		return true // Release — transaction ended
	case "SAVEPOINT", "RELEASE_SAVEPOINT", "ROLLBACK_TO_SAVEPOINT":
		ca.logger.Debug("Savepoint operation", zap.String("session", sessionID), zap.String("command", cmdType))
		return false // Don't release — still in transaction
	}

	return ca.sessionStates[sessionID] != TransactionInProgress
}

// UpdateTransactionState updates state based on the query that was just executed.
func (ca *ConnectionAffinity) UpdateTransactionState(sessionID string, query string) {
	ca.mutex.Lock()
	defer ca.mutex.Unlock()

	_, cmdType := IsTransactionCommand(query)
	if cmdType == "" {
		return
	}

	switch cmdType {
	case "BEGIN":
		ca.sessionStates[sessionID] = TransactionInProgress
		ca.logger.Debug("Transaction started", zap.String("session", sessionID))
	case "COMMIT", "ROLLBACK":
		ca.sessionStates[sessionID] = TransactionIdle
		ca.sessionLastUsed[sessionID] = time.Now()
		ca.logger.Debug("Transaction ended", zap.String("session", sessionID), zap.String("command", cmdType))
		// Keep connection pinned for session reuse — don't release here.
		// The cleanup goroutine will handle truly idle sessions.
	case "SAVEPOINT", "RELEASE_SAVEPOINT", "ROLLBACK_TO_SAVEPOINT":
		// Savepoint operations do NOT change the overall transaction state.
		// Connection stays pinned; transaction remains in progress.
		ca.logger.Debug("Savepoint operation", zap.String("session", sessionID), zap.String("command", cmdType))
	}
}

func (ca *ConnectionAffinity) IsInTransaction(sessionID string) bool {
	ca.mutex.Lock()
	defer ca.mutex.Unlock()

	state, exists := ca.sessionStates[sessionID]
	return exists && state == TransactionInProgress
}

func (ca *ConnectionAffinity) cleanupIdleSessions() {
	for {
		select {
		case <-ca.cleanupTicker.C:
			ca.cleanupExpiredSessions()
		case <-ca.stopCleanup:
			return
		}
	}
}

func (ca *ConnectionAffinity) cleanupExpiredSessions() {
	ca.mutex.Lock()
	defer ca.mutex.Unlock()

	now := time.Now()
	var expiredSessions []string

	// Release connections that have been idle (no active transaction) for > 10 seconds
	// This is fast enough to reclaim resources but avoids churn during active workloads
	idleTimeout := 10 * time.Second
	orphanTimeout := 10 * time.Minute

	for sessionID, lastUsed := range ca.sessionLastUsed {
		state := ca.sessionStates[sessionID]
		idle := now.Sub(lastUsed)

		if state != TransactionInProgress && idle > idleTimeout {
			expiredSessions = append(expiredSessions, sessionID)
		} else if state == TransactionInProgress && idle > orphanTimeout {
			// Force-expire very old transactions (likely orphaned)
			ca.logger.Warn("Force-expiring orphaned transaction session",
				zap.String("session", sessionID),
				zap.Duration("idle", idle))
			expiredSessions = append(expiredSessions, sessionID)
		}
	}

	for _, sessionID := range expiredSessions {
		if conn, exists := ca.sessionConnections[sessionID]; exists {
			// Rollback any in-progress transaction before releasing
			if ca.sessionStates[sessionID] == TransactionInProgress {
				_, _ = conn.Exec(context.Background(), "ROLLBACK")
			}
			conn.Release()
			delete(ca.sessionConnections, sessionID)
			delete(ca.sessionStates, sessionID)
			delete(ca.sessionLastUsed, sessionID)
		}
	}

	if len(expiredSessions) > 0 {
		ca.logger.Info("Cleaned up expired affinity sessions",
			zap.Int("cleaned", len(expiredSessions)),
			zap.Int("remaining", len(ca.sessionConnections)))
	}
}

func (ca *ConnectionAffinity) Close() {
	ca.mutex.Lock()
	defer ca.mutex.Unlock()

	close(ca.stopCleanup)
	ca.cleanupTicker.Stop()

	// Release all connections, rolling back active transactions
	for sessionID, conn := range ca.sessionConnections {
		if ca.sessionStates[sessionID] == TransactionInProgress {
			ca.logger.Warn("Rolling back active transaction on close", zap.String("session", sessionID))
			_, _ = conn.Exec(context.Background(), "ROLLBACK")
		}
		conn.Release()
	}

	ca.sessionConnections = make(map[string]*pgxpool.Conn)
	ca.sessionStates = make(map[string]TransactionState)
	ca.sessionLastUsed = make(map[string]time.Time)
}
