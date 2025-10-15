package server

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	wire "github.com/jeroenrinzema/psql-wire"
	"go.uber.org/zap"
)

// TransactionAwareConnection wraps a connection with transaction state tracking
type TransactionAwareConnection struct {
	pool             *pgxpool.Pool
	logger           *zap.Logger
	connectionType   ConnectionType
	address          string
	
	// Transaction state per session
	sessionConnections map[string]*pgxpool.Conn
	sessionStates     map[string]TransactionState
	sessionLastUsed   map[string]time.Time
	mutex             sync.RWMutex
	
	// Cleanup management
	cleanupTicker     *time.Ticker
	stopCleanup       chan bool
}

func NewTransactionAwareConnection(logger *zap.Logger, connType ConnectionType, address string) *TransactionAwareConnection {
	tac := &TransactionAwareConnection{
		logger:             logger,
		connectionType:     connType,
		address:           address,
		sessionConnections: make(map[string]*pgxpool.Conn),
		sessionStates:     make(map[string]TransactionState),
		sessionLastUsed:   make(map[string]time.Time),
		cleanupTicker:     time.NewTicker(5 * time.Minute),
		stopCleanup:       make(chan bool),
	}
	
	// Start cleanup goroutine
	go tac.cleanupExpiredSessions()
	
	return tac
}

func (tac *TransactionAwareConnection) Connect(ctx context.Context, poolConfig *PoolConfig) error {
	config, err := pgxpool.ParseConfig(tac.address)
	if err != nil {
		return fmt.Errorf("failed to parse connection string: %w", err)
	}

	config.MaxConns = int32(poolConfig.MaxConns)
	config.MinConns = int32(poolConfig.MinConns)
	config.MaxConnLifetime = poolConfig.MaxConnLifetime
	config.MaxConnIdleTime = poolConfig.MaxConnIdleTime

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return fmt.Errorf("failed to create connection pool: %w", err)
	}

	tac.pool = pool
	return nil
}

func (tac *TransactionAwareConnection) ExecuteQueryWithSession(
	ctx context.Context,
	sessionID string,
	query string,
	writer wire.DataWriter,
	parameters []wire.Parameter) error {

	// Always use session-level connections for consistency
	conn, err := tac.getSessionConnection(ctx, sessionID)
	if err != nil {
		return fmt.Errorf("failed to get session connection: %w", err)
	}

	// Check if this is a transaction command
	isTransactionCmd, cmdType := IsTransactionCommand(query)

	// Execute the query
	if writer != nil {
		// Primary connection with result writing
		err = tac.executeWithWriter(ctx, conn, query, writer)
	} else {
		// Mirror connection without result writing - check if conn is busy
		if conn.Conn().IsClosed() {
			return fmt.Errorf("connection is closed")
		}
		_, err = conn.Exec(ctx, query)
	}

	if err != nil {
		tac.logger.Error("Query execution failed", 
			zap.String("session", sessionID),
			zap.String("query", query),
			zap.Error(err))
		
		// If transaction is aborted, clean up the session
		if strings.Contains(err.Error(), "current transaction is aborted") {
			tac.logger.Warn("Transaction aborted, cleaning up session", zap.String("session", sessionID))
			tac.cleanupSession(sessionID)
			return err
		}
		
		// If query failed and we're in transaction, mark as failed
		if tac.isSessionInTransaction(sessionID) {
			tac.updateSessionState(sessionID, TransactionFailed)
		}
		return err
	}

	// Update transaction state
	if isTransactionCmd {
		tac.updateSessionState(sessionID, tac.getNewTransactionState(cmdType))
		
		// If ROLLBACK or COMMIT, release the dedicated connection
		if cmdType == "ROLLBACK" || cmdType == "COMMIT" {
			tac.releaseSessionConnection(sessionID)
		}
	}

	return nil
}

// executeWithTempConnection executes a query with a temporary connection
func (tac *TransactionAwareConnection) executeWithTempConnection(ctx context.Context, query string, writer wire.DataWriter) error {
	// Check connection pool pressure
	stats := tac.pool.Stat()
	availableConns := stats.MaxConns() - stats.AcquiredConns()
	if availableConns < 5 {
		return fmt.Errorf("connection pool under pressure, available: %d", availableConns)
	}
	
	conn, err := tac.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("failed to acquire temporary connection: %w", err)
	}
	defer conn.Release()

	if writer != nil {
		return tac.executeWithWriter(ctx, conn, query, writer)
	} else {
		_, err = conn.Exec(ctx, query)
		return err
	}
}

func (tac *TransactionAwareConnection) getSessionConnection(ctx context.Context, sessionID string) (*pgxpool.Conn, error) {
	tac.mutex.Lock()
	defer tac.mutex.Unlock()

	// Update last used time
	tac.sessionLastUsed[sessionID] = time.Now()

	// If we already have a connection for this session, use it
	if conn, exists := tac.sessionConnections[sessionID]; exists {
		return conn, nil
	}

	// Acquire a new connection for this session
	stats := tac.pool.Stat()
	availableConns := stats.MaxConns() - stats.AcquiredConns()
	if availableConns < 1 {
		return nil, fmt.Errorf("connection pool exhausted, available: %d", availableConns)
	}
	
	// Use context with timeout for connection acquisition only
	connCtx, connCancel := context.WithTimeout(ctx, 10*time.Second)
	defer connCancel()
	
	conn, err := tac.pool.Acquire(connCtx)
	if err != nil {
		return nil, err
	}

	tac.sessionConnections[sessionID] = conn
	tac.sessionStates[sessionID] = TransactionIdle
	tac.sessionLastUsed[sessionID] = time.Now()
	
	tac.logger.Debug("Created new session connection", 
		zap.String("session", sessionID),
		zap.String("connection_type", string(tac.connectionType)))
	
	return conn, nil
}

func (tac *TransactionAwareConnection) releaseSessionConnection(sessionID string) {
	tac.mutex.Lock()
	defer tac.mutex.Unlock()

	if conn, exists := tac.sessionConnections[sessionID]; exists {
		conn.Release()
		delete(tac.sessionConnections, sessionID)
		delete(tac.sessionStates, sessionID)
		delete(tac.sessionLastUsed, sessionID)
	}
}

func (tac *TransactionAwareConnection) isSessionInTransaction(sessionID string) bool {
	tac.mutex.RLock()
	defer tac.mutex.RUnlock()
	
	state, exists := tac.sessionStates[sessionID]
	return exists && state == TransactionInProgress
}

func (tac *TransactionAwareConnection) cleanupSession(sessionID string) {
	tac.mutex.Lock()
	defer tac.mutex.Unlock()
	
	if conn, exists := tac.sessionConnections[sessionID]; exists {
		conn.Release()
		delete(tac.sessionConnections, sessionID)
	}
	delete(tac.sessionStates, sessionID)
	delete(tac.sessionLastUsed, sessionID)
}

func (tac *TransactionAwareConnection) updateSessionState(sessionID string, state TransactionState) {
	tac.mutex.Lock()
	defer tac.mutex.Unlock()
	
	tac.sessionStates[sessionID] = state
	tac.logger.Debug("Updated session transaction state", 
		zap.String("session", sessionID),
		zap.Int("state", int(state)))
}

func (tac *TransactionAwareConnection) getNewTransactionState(cmdType string) TransactionState {
	switch cmdType {
	case "BEGIN":
		return TransactionInProgress
	case "COMMIT", "ROLLBACK":
		return TransactionIdle
	default:
		return TransactionIdle
	}
}

func (tac *TransactionAwareConnection) executeWithWriter(ctx context.Context, conn *pgxpool.Conn, query string, writer wire.DataWriter) error {
	rows, err := conn.Query(ctx, query)
	if err != nil {
		return fmt.Errorf("query execution failed: %w", err)
	}
	defer rows.Close()

	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return fmt.Errorf("query execution error: %w", err)
		}
		return writer.Complete("OK")
	}

	// Process rows and write to wire protocol
	for {
		values, err := rows.Values()
		if err != nil {
			return fmt.Errorf("failed to scan row: %w", err)
		}

		if err := writer.Row(values); err != nil {
			return fmt.Errorf("failed to write row: %w", err)
		}

		if !rows.Next() {
			break
		}
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("row iteration error: %w", err)
	}

	return writer.Complete("SELECT")
}

func (tac *TransactionAwareConnection) Close() {
	// Stop cleanup goroutine
	if tac.stopCleanup != nil {
		close(tac.stopCleanup)
	}
	if tac.cleanupTicker != nil {
		tac.cleanupTicker.Stop()
	}

	tac.mutex.Lock()
	defer tac.mutex.Unlock()

	// Release all session connections
	for sessionID, conn := range tac.sessionConnections {
		// Rollback any active transactions
		if state, exists := tac.sessionStates[sessionID]; exists && state == TransactionInProgress {
			tac.logger.Warn("Rolling back active transaction on connection close", zap.String("session", sessionID))
			_, err := conn.Exec(context.Background(), "ROLLBACK")
			if err != nil {
				tac.logger.Error("Failed to rollback transaction on close", zap.Error(err))
			}
		}
		conn.Release()
	}

	// Clear all maps
	tac.sessionConnections = make(map[string]*pgxpool.Conn)
	tac.sessionStates = make(map[string]TransactionState)
	tac.sessionLastUsed = make(map[string]time.Time)

	if tac.pool != nil {
		tac.pool.Close()
	}
}

// cleanupExpiredSessions runs periodically to clean up expired sessions
func (tac *TransactionAwareConnection) cleanupExpiredSessions() {
	for {
		select {
		case <-tac.cleanupTicker.C:
			tac.performCleanup()
		case <-tac.stopCleanup:
			return
		}
	}
}

// performCleanup removes expired sessions that are not in active transactions
func (tac *TransactionAwareConnection) performCleanup() {
	tac.mutex.Lock()
	defer tac.mutex.Unlock()

	now := time.Now()
	expiredSessions := make([]string, 0)

	// More aggressive cleanup under memory pressure
	sessionTimeout := 30 * time.Minute
	if len(tac.sessionConnections) > 500 { // Memory pressure threshold
		sessionTimeout = 5 * time.Minute
		tac.logger.Warn("Memory pressure detected, using aggressive cleanup", 
			zap.Int("session_count", len(tac.sessionConnections)))
	}

	for sessionID, lastUsed := range tac.sessionLastUsed {
		// Clean up sessions based on dynamic timeout
		if now.Sub(lastUsed) > sessionTimeout {
			// Only clean up if not in active transaction
			if state, exists := tac.sessionStates[sessionID]; !exists || state != TransactionInProgress {
				expiredSessions = append(expiredSessions, sessionID)
			}
		}
	}

	cleanedCount := 0
	for _, sessionID := range expiredSessions {
		if conn, exists := tac.sessionConnections[sessionID]; exists {
			conn.Release()
			delete(tac.sessionConnections, sessionID)
			delete(tac.sessionStates, sessionID)
			delete(tac.sessionLastUsed, sessionID)
			cleanedCount++
		}
	}

	if cleanedCount > 0 {
		tac.logger.Info("Cleaned up expired transaction-aware sessions", 
			zap.Int("cleaned_sessions", cleanedCount),
			zap.Int("remaining_sessions", len(tac.sessionConnections)),
			zap.String("connection_type", string(tac.connectionType)))
	}
}
