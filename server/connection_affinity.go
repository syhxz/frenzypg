package server

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
)

// ConnectionAffinity manages dedicated connections for transactions
type ConnectionAffinity struct {
	pool               *pgxpool.Pool
	sessionConnections map[string]*pgxpool.Conn
	sessionStates      map[string]TransactionState
	sessionLastUsed    map[string]time.Time
	mutex              sync.RWMutex
	logger             *zap.Logger
	cleanupTicker      *time.Ticker
	stopCleanup        chan bool
}

func NewConnectionAffinity(pool *pgxpool.Pool, logger *zap.Logger) *ConnectionAffinity {
	ca := &ConnectionAffinity{
		pool:               pool,
		sessionConnections: make(map[string]*pgxpool.Conn),
		sessionStates:      make(map[string]TransactionState),
		sessionLastUsed:    make(map[string]time.Time),
		logger:             logger,
		cleanupTicker:      time.NewTicker(30 * time.Second),
		stopCleanup:        make(chan bool),
	}

	// Start cleanup goroutine
	go ca.cleanupIdleSessions()
	
	return ca
}

func (ca *ConnectionAffinity) GetConnection(ctx context.Context, sessionID string) (*pgxpool.Conn, error) {
	ca.mutex.Lock()
	defer ca.mutex.Unlock()

	// Clean up stale connections first
	ca.cleanupStaleConnections()

	// Update last used time
	ca.sessionLastUsed[sessionID] = time.Now()

	// If we have an existing connection for this session, return it
	if conn, exists := ca.sessionConnections[sessionID]; exists {
		ca.logger.Debug("Reusing existing connection for session", zap.String("session", sessionID))
		return conn, nil
	}

	// Acquire a new connection
	conn, err := ca.pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire connection: %w", err)
	}

	ca.sessionConnections[sessionID] = conn
	ca.sessionStates[sessionID] = TransactionIdle
	ca.logger.Debug("Created new connection for session", zap.String("session", sessionID))
	
	return conn, nil
}

// cleanupStaleConnections releases connections that haven't been used recently
func (ca *ConnectionAffinity) cleanupStaleConnections() {
	now := time.Now()
	for sessionID, lastUsed := range ca.sessionLastUsed {
		// Release connections idle for more than 5 minutes
		if now.Sub(lastUsed) > 5*time.Minute {
			if conn, exists := ca.sessionConnections[sessionID]; exists {
				conn.Release()
				delete(ca.sessionConnections, sessionID)
				delete(ca.sessionStates, sessionID)
				delete(ca.sessionLastUsed, sessionID)
				ca.logger.Debug("Released stale connection", zap.String("session", sessionID))
			}
		}
	}
}

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
		ca.logger.Debug("Transaction ended", zap.String("session", sessionID), zap.String("command", cmdType))
		
		// Keep connection alive for session - don't release after transaction ends
		ca.sessionLastUsed[sessionID] = time.Now()
	}
}

func (ca *ConnectionAffinity) IsInTransaction(sessionID string) bool {
	ca.mutex.RLock()
	defer ca.mutex.RUnlock()
	
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
	expiredSessions := make([]string, 0)

	for sessionID, lastUsed := range ca.sessionLastUsed {
		// Clean up sessions idle for more than 5 minutes and not in transaction
		if now.Sub(lastUsed) > 5*time.Minute {
			if state, exists := ca.sessionStates[sessionID]; !exists || state != TransactionInProgress {
				expiredSessions = append(expiredSessions, sessionID)
			}
		}
	}

	for _, sessionID := range expiredSessions {
		if conn, exists := ca.sessionConnections[sessionID]; exists {
			ca.logger.Debug("Cleaning up expired session", zap.String("session", sessionID))
			conn.Release()
			delete(ca.sessionConnections, sessionID)
			delete(ca.sessionStates, sessionID)
			delete(ca.sessionLastUsed, sessionID)
		}
	}
}

func (ca *ConnectionAffinity) Close() {
	ca.mutex.Lock()
	defer ca.mutex.Unlock()

	// Stop cleanup goroutine
	close(ca.stopCleanup)
	ca.cleanupTicker.Stop()

	// Release all connections
	for sessionID, conn := range ca.sessionConnections {
		// Rollback any active transactions
		if state, exists := ca.sessionStates[sessionID]; exists && state == TransactionInProgress {
			ca.logger.Warn("Rolling back active transaction on close", zap.String("session", sessionID))
			_, err := conn.Exec(context.Background(), "ROLLBACK")
			if err != nil {
				ca.logger.Error("Failed to rollback transaction on close", zap.Error(err))
			}
		}
		conn.Release()
	}

	// Clear maps
	ca.sessionConnections = make(map[string]*pgxpool.Conn)
	ca.sessionStates = make(map[string]TransactionState)
	ca.sessionLastUsed = make(map[string]time.Time)
}
