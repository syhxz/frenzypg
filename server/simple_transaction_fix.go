package server

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
)

// trackedConn holds transaction state and last-used time for a connection
type trackedConn struct {
	state    TransactionState
	lastUsed time.Time
}

// SimpleTransactionTracker tracks transaction state per connection using PID-based keys
type SimpleTransactionTracker struct {
	connectionStates map[string]trackedConn
	mutex            sync.RWMutex
	logger           *zap.Logger
	stopCleanup      chan struct{}
}

var globalTransactionTracker *SimpleTransactionTracker
var trackerOnce sync.Once

func GetTransactionTracker(logger *zap.Logger) *SimpleTransactionTracker {
	trackerOnce.Do(func() {
		globalTransactionTracker = &SimpleTransactionTracker{
			connectionStates: make(map[string]trackedConn),
			logger:           logger,
			stopCleanup:      make(chan struct{}),
		}
		go globalTransactionTracker.cleanupLoop()
	})
	return globalTransactionTracker
}

// connKey generates a stable string key from a connection's backend PID
func connKey(conn *pgxpool.Conn) string {
	return fmt.Sprintf("pid_%d", conn.Conn().PgConn().PID())
}

func (stt *SimpleTransactionTracker) UpdateConnectionState(conn *pgxpool.Conn, query string) {
	stt.mutex.Lock()
	defer stt.mutex.Unlock()

	trimmed := strings.TrimSpace(strings.ToUpper(query))
	key := connKey(conn)

	if strings.HasPrefix(trimmed, "BEGIN") || strings.HasPrefix(trimmed, "START TRANSACTION") {
		stt.connectionStates[key] = trackedConn{state: TransactionInProgress, lastUsed: time.Now()}
		stt.logger.Debug("Transaction started on connection", zap.String("key", key))
	} else if strings.HasPrefix(trimmed, "COMMIT") {
		delete(stt.connectionStates, key)
		stt.logger.Debug("Transaction committed on connection", zap.String("key", key))
	} else if strings.HasPrefix(trimmed, "ROLLBACK") && !strings.Contains(trimmed, "TO SAVEPOINT") && !strings.Contains(trimmed, "TO ") {
		delete(stt.connectionStates, key)
		stt.logger.Debug("Transaction rolled back on connection", zap.String("key", key))
	} else if stt.connectionStates[key].state == TransactionInProgress {
		// Update lastUsed for any query within an active transaction
		stt.connectionStates[key] = trackedConn{state: TransactionInProgress, lastUsed: time.Now()}
	}
}

func (stt *SimpleTransactionTracker) IsInTransaction(conn *pgxpool.Conn) bool {
	stt.mutex.RLock()
	defer stt.mutex.RUnlock()

	tc, exists := stt.connectionStates[connKey(conn)]
	if !exists {
		return false
	}
	// Guard against PID reuse: if state is stale (>2 minutes without activity), ignore it
	if time.Since(tc.lastUsed) > 2*time.Minute {
		return false
	}
	return tc.state == TransactionInProgress
}

func (stt *SimpleTransactionTracker) CleanupConnection(conn *pgxpool.Conn) {
	stt.mutex.Lock()
	defer stt.mutex.Unlock()

	key := connKey(conn)
	// If connection is in transaction, rollback before cleanup
	if tc, exists := stt.connectionStates[key]; exists && tc.state == TransactionInProgress {
		stt.logger.Warn("Connection being cleaned up while in transaction - rolling back")
		_, err := conn.Exec(context.Background(), "ROLLBACK")
		if err != nil {
			stt.logger.Error("Failed to rollback transaction during cleanup", zap.Error(err))
		}
	}

	delete(stt.connectionStates, key)
}

// cleanupLoop periodically removes stale entries
func (stt *SimpleTransactionTracker) cleanupLoop() {
	ticker := time.NewTicker(2 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			stt.mutex.Lock()
			now := time.Now()
			for key, tc := range stt.connectionStates {
				if now.Sub(tc.lastUsed) > 10*time.Minute {
					delete(stt.connectionStates, key)
				}
			}
			stt.mutex.Unlock()
		case <-stt.stopCleanup:
			return
		}
	}
}
