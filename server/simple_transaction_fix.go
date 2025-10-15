package server

import (
	"context"
	"strings"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
)

// SimpleTransactionTracker tracks transaction state per connection
type SimpleTransactionTracker struct {
	connectionStates map[*pgxpool.Conn]TransactionState
	mutex           sync.RWMutex
	logger          *zap.Logger
}

var globalTransactionTracker *SimpleTransactionTracker
var trackerOnce sync.Once

func GetTransactionTracker(logger *zap.Logger) *SimpleTransactionTracker {
	trackerOnce.Do(func() {
		globalTransactionTracker = &SimpleTransactionTracker{
			connectionStates: make(map[*pgxpool.Conn]TransactionState),
			logger:          logger,
		}
	})
	return globalTransactionTracker
}

func (stt *SimpleTransactionTracker) UpdateConnectionState(conn *pgxpool.Conn, query string) {
	stt.mutex.Lock()
	defer stt.mutex.Unlock()

	trimmed := strings.TrimSpace(strings.ToUpper(query))
	
	if strings.HasPrefix(trimmed, "BEGIN") || strings.HasPrefix(trimmed, "START TRANSACTION") {
		stt.connectionStates[conn] = TransactionInProgress
		stt.logger.Debug("Transaction started on connection", zap.String("query", query))
	} else if strings.HasPrefix(trimmed, "COMMIT") {
		stt.connectionStates[conn] = TransactionIdle
		stt.logger.Debug("Transaction committed on connection", zap.String("query", query))
	} else if strings.HasPrefix(trimmed, "ROLLBACK") {
		stt.connectionStates[conn] = TransactionIdle
		stt.logger.Debug("Transaction rolled back on connection", zap.String("query", query))
	}
}

func (stt *SimpleTransactionTracker) IsInTransaction(conn *pgxpool.Conn) bool {
	stt.mutex.RLock()
	defer stt.mutex.RUnlock()
	
	state, exists := stt.connectionStates[conn]
	return exists && state == TransactionInProgress
}

func (stt *SimpleTransactionTracker) CleanupConnection(conn *pgxpool.Conn) {
	stt.mutex.Lock()
	defer stt.mutex.Unlock()
	
	// If connection is in transaction, it should be rolled back
	if state, exists := stt.connectionStates[conn]; exists && state == TransactionInProgress {
		stt.logger.Warn("Connection being cleaned up while in transaction - should rollback")
		// Execute rollback on the connection before cleanup
		_, err := conn.Exec(context.Background(), "ROLLBACK")
		if err != nil {
			stt.logger.Error("Failed to rollback transaction during cleanup", zap.Error(err))
		}
	}
	
	delete(stt.connectionStates, conn)
}
