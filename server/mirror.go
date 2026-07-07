package server

import (
	"context"
	"strings"
	"sync"
	"time"

	wire "github.com/jeroenrinzema/psql-wire"
	"go.uber.org/zap"
)

const (
	// maxTxBufferBytes is the maximum total bytes of buffered queries per transaction
	maxTxBufferBytes = 10 * 1024 * 1024 // 10 MB
	// maxTotalTxBuffers is the maximum number of concurrent transaction buffers
	maxTotalTxBuffers = 5000
)

// dispatchToMirrors decides whether and how to send a query to mirrors.
func (server *ProxyServerV15) dispatchToMirrors(ctx context.Context, sessionID, query, cmdType string, pgxArgs []any, parameters []wire.Parameter) {
	if len(server.mirrors) == 0 {
		return
	}

	filterConfig := server.getQueryFilterConfig()

	// Check if this query should be mirrored at all
	if !server.shouldMirrorQuery(query) {
		// Transaction commands are always mirrored (handled by shouldMirrorQuery)
		// but respect SkipRollbackMirror for ROLLBACK
		if cmdType == "ROLLBACK" && filterConfig.SkipRollbackMirror {
			return
		}
		if cmdType != "BEGIN" && cmdType != "COMMIT" && cmdType != "ROLLBACK" &&
			cmdType != "SAVEPOINT" && cmdType != "RELEASE_SAVEPOINT" && cmdType != "ROLLBACK_TO_SAVEPOINT" {
			return
		}
	}

	// Route based on transaction state
	switch {
	case cmdType == "BEGIN":
		server.startTransactionBuffer(sessionID)
	case cmdType == "COMMIT":
		server.commitTransactionBuffer(sessionID)
	case cmdType == "ROLLBACK":
		server.rollbackTransactionBuffer(sessionID)
	case cmdType == "SAVEPOINT" || cmdType == "RELEASE_SAVEPOINT" || cmdType == "ROLLBACK_TO_SAVEPOINT":
		// Savepoint commands are buffered within the transaction like regular queries
		if server.isSessionInTransaction(sessionID) {
			server.bufferTransactionQuery(sessionID, query, parameters)
		} else {
			go server.executeMirrorQueriesAsync(ctx, sessionID, query, pgxArgs)
		}
	case server.isSessionInTransaction(sessionID):
		server.bufferTransactionQuery(sessionID, query, parameters)
	default:
		go server.executeMirrorQueriesAsync(ctx, sessionID, query, pgxArgs)
	}
}

// shouldMirrorQuery determines if a query should be mirrored based on configuration.
func (server *ProxyServerV15) shouldMirrorQuery(query string) bool {
	trimmed := strings.TrimSpace(strings.ToUpper(query))
	filterConfig := server.getQueryFilterConfig()

	if filterConfig.MirrorAllQueries {
		return true
	}

	// Sequence manipulation (setval/nextval changes sequence state) — check BEFORE generic SELECT
	if strings.HasPrefix(trimmed, "SELECT SETVAL") || strings.HasPrefix(trimmed, "SELECT NEXTVAL") {
		return true
	}

	// SELECT / WITH
	if strings.HasPrefix(trimmed, "SELECT ") || strings.HasPrefix(trimmed, "WITH ") {
		return filterConfig.MirrorSelectQueries
	}

	// DDL (includes CREATE FUNCTION, CREATE TRIGGER, CREATE SEQUENCE, etc.)
	for _, prefix := range []string{"CREATE", "ALTER", "DROP", "TRUNCATE"} {
		if strings.HasPrefix(trimmed, prefix+" ") || trimmed == prefix {
			return !filterConfig.MirrorDmlOnly
		}
	}

	// Stored procedure calls and anonymous blocks
	// CALL — executes procedures (PostgreSQL 11+)
	// DO — executes anonymous code blocks
	if strings.HasPrefix(trimmed, "CALL ") || strings.HasPrefix(trimmed, "DO ") || trimmed == "DO" {
		return !filterConfig.MirrorDdlOnly
	}

	// Transaction commands
	for _, cmd := range []string{"BEGIN", "COMMIT", "ROLLBACK", "START", "END"} {
		if strings.HasPrefix(trimmed, cmd+" ") || trimmed == cmd {
			return true
		}
	}

	// SAVEPOINT commands — must be mirrored to maintain transaction state on mirrors
	if strings.HasPrefix(trimmed, "SAVEPOINT ") ||
		strings.HasPrefix(trimmed, "RELEASE SAVEPOINT ") ||
		strings.HasPrefix(trimmed, "RELEASE ") ||
		strings.HasPrefix(trimmed, "ROLLBACK TO SAVEPOINT ") ||
		strings.HasPrefix(trimmed, "ROLLBACK TO ") {
		return true
	}

	// DML
	for _, prefix := range []string{"INSERT", "UPDATE", "DELETE", "MERGE"} {
		if strings.HasPrefix(trimmed, prefix+" ") || trimmed == prefix {
			return !filterConfig.MirrorDdlOnly
		}
	}

	// Session-level commands — must mirror to keep session state consistent
	// SET, RESET, DISCARD affect subsequent query behavior
	for _, prefix := range []string{"SET ", "RESET ", "DISCARD "} {
		if strings.HasPrefix(trimmed, prefix) {
			return true
		}
	}

	// PREPARE / EXECUTE / DEALLOCATE — prepared statement management
	for _, prefix := range []string{"PREPARE ", "EXECUTE ", "DEALLOCATE "} {
		if strings.HasPrefix(trimmed, prefix) {
			return true
		}
	}
	if trimmed == "DEALLOCATE ALL" {
		return true
	}

	// COPY — data loading (FROM) should mirror, data export (TO) should not
	if strings.HasPrefix(trimmed, "COPY ") {
		if strings.Contains(trimmed, " TO ") {
			return false // COPY TO is read-only, no need to mirror
		}
		return !filterConfig.MirrorDdlOnly
	}

	// LISTEN / NOTIFY — mirror if mirroring all
	if strings.HasPrefix(trimmed, "LISTEN ") || strings.HasPrefix(trimmed, "NOTIFY ") || trimmed == "UNLISTEN" || strings.HasPrefix(trimmed, "UNLISTEN ") {
		return filterConfig.MirrorAllQueries
	}

	// Maintenance commands — don't mirror by default (mirrors maintain independently)
	// VACUUM, ANALYZE, REINDEX, CLUSTER
	if strings.HasPrefix(trimmed, "VACUUM") || strings.HasPrefix(trimmed, "ANALYZE") ||
		strings.HasPrefix(trimmed, "REINDEX") || strings.HasPrefix(trimmed, "CLUSTER") {
		return filterConfig.MirrorAllQueries
	}

	// GRANT / REVOKE / COMMENT — administrative commands that should mirror
	for _, prefix := range []string{"GRANT", "REVOKE", "COMMENT"} {
		if strings.HasPrefix(trimmed, prefix+" ") {
			return !filterConfig.MirrorDmlOnly
		}
	}

	return false
}

// --- Mirror async execution with circuit breaker ---

// executeMirrorQueriesAsync executes query on all mirrors asynchronously with circuit breaker.
func (server *ProxyServerV15) executeMirrorQueriesAsync(ctx context.Context, sessionID string, query string, args []any) {
	timeout := time.Duration(server.performanceConfig.MirrorTimeoutSecs) * time.Second
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	mirrorCtx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	var wg sync.WaitGroup
	for i, mirror := range server.mirrors {
		if mirror == nil || mirror.pool == nil {
			continue
		}

		// Circuit breaker check
		if server.mirrorBreakers != nil && !server.mirrorBreakers.Allow(i) {
			server.logger.Debug("Mirror circuit breaker open, skipping",
				zap.Int("mirror_index", i))
			continue
		}

		wg.Add(1)
		go func(mirrorIndex int, m *Connection) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					server.logger.Error("Mirror goroutine panic",
						zap.Int("mirror_index", mirrorIndex),
						zap.Any("panic", r))
					if server.mirrorBreakers != nil {
						server.mirrorBreakers.RecordFailure(mirrorIndex)
					}
				}
			}()

			err := server.executeMirrorQueryWithRetry(mirrorCtx, mirrorIndex, m, query, args)
			if err != nil {
				server.logger.Error("Mirror query failed after retries",
					zap.Int("mirror_index", mirrorIndex),
					zap.String("query", truncateQuery(query, 200)),
					zap.Error(err))
				if server.mirrorBreakers != nil {
					server.mirrorBreakers.RecordFailure(mirrorIndex)
				}
			} else {
				if server.mirrorBreakers != nil {
					server.mirrorBreakers.RecordSuccess(mirrorIndex)
				}
			}
		}(i, mirror)
	}
	wg.Wait()
}

// executeMirrorQueryWithRetry executes a query on a single mirror with retry logic.
func (server *ProxyServerV15) executeMirrorQueryWithRetry(ctx context.Context, mirrorIndex int, m *Connection, query string, args []any) error {
	maxRetries := server.performanceConfig.MirrorRetries
	if maxRetries <= 0 {
		maxRetries = 1
	}
	retryDelay := time.Duration(server.performanceConfig.RetryDelaySecs) * time.Second
	if retryDelay <= 0 {
		retryDelay = 2 * time.Second
	}

	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(retryDelay):
			}
		}

		conn, err := m.pool.Acquire(ctx)
		if err != nil {
			lastErr = err
			continue
		}

		_, err = conn.Exec(ctx, query, args...)
		conn.Release()
		if err == nil {
			return nil
		}
		lastErr = err

		// Don't retry on query-level errors (syntax, constraint violations)
		errMsg := err.Error()
		if strings.Contains(errMsg, "syntax error") ||
			strings.Contains(errMsg, "violates") ||
			strings.Contains(errMsg, "does not exist") {
			return err
		}
	}
	return lastErr
}

// executeMirrorQueriesWithSession is a legacy wrapper kept for compatibility.
func (server *ProxyServerV15) executeMirrorQueriesWithSession(ctx context.Context, sessionID string, query string, parameters []wire.Parameter) {
	args := server.convertParameters(parameters)
	server.executeMirrorQueriesAsync(ctx, sessionID, query, args)
}

// executeMirrorQueriesWithTransactionSupport executes on all mirrors with transaction support.
func (server *ProxyServerV15) executeMirrorQueriesWithTransactionSupport(ctx context.Context, query string, parameters []wire.Parameter) {
	args := server.convertParameters(parameters)
	server.executeMirrorQueriesAsync(ctx, "", query, args)
}

// executeMirrorQueries is a legacy method using Connection.ExecuteQueryV15 directly.
func (server *ProxyServerV15) executeMirrorQueries(ctx context.Context, query string, parameters []wire.Parameter, session *Session) {
	for i, mirror := range server.mirrors {
		if server.mirrorBreakers != nil && !server.mirrorBreakers.Allow(i) {
			continue
		}
		go func(mirrorIndex int, m *Connection) {
			err := m.ExecuteQueryV15(ctx, query, nil, parameters)
			if err != nil {
				server.logger.Error("Mirror query failed",
					zap.Int("mirror_index", mirrorIndex),
					zap.String("session", session.ID),
					zap.Error(err))
				if server.mirrorBreakers != nil {
					server.mirrorBreakers.RecordFailure(mirrorIndex)
				}
			} else {
				if server.mirrorBreakers != nil {
					server.mirrorBreakers.RecordSuccess(mirrorIndex)
				}
			}
		}(i, mirror)
	}
}

// --- Transaction buffering ---

func (server *ProxyServerV15) startTransactionBuffer(sessionID string) {
	server.txBufferMutex.Lock()
	defer server.txBufferMutex.Unlock()

	// Reject if too many concurrent buffers
	if len(server.txBuffers) >= maxTotalTxBuffers {
		server.logger.Error("Too many concurrent transaction buffers, rejecting new transaction",
			zap.String("session", sessionID),
			zap.Int("active_buffers", len(server.txBuffers)))
		return
	}

	now := time.Now()
	server.txBuffers[sessionID] = &TransactionBuffer{
		SessionID:    sessionID,
		Queries:      make([]string, 0, 50),
		Parameters:   make([][]wire.Parameter, 0, 50),
		CreatedAt:    now,
		LastActivity: now,
	}
}

func (server *ProxyServerV15) bufferTransactionQuery(sessionID, query string, parameters []wire.Parameter) {
	server.txBufferMutex.Lock()
	defer server.txBufferMutex.Unlock()

	buffer, exists := server.txBuffers[sessionID]
	if !exists {
		return
	}

	// Check query count limit
	maxQueries := 100
	if len(server.txBuffers) > 100 {
		maxQueries = 50
	}

	if len(buffer.Queries) >= maxQueries {
		server.logger.Error("Transaction buffer query count overflow, dropping buffer",
			zap.String("session", sessionID),
			zap.Int("query_count", len(buffer.Queries)))
		buffer.Queries = nil
		buffer.Parameters = nil
		delete(server.txBuffers, sessionID)
		return
	}

	// Check total byte size limit
	buffer.TotalBytes += int64(len(query))
	if buffer.TotalBytes > maxTxBufferBytes {
		server.logger.Error("Transaction buffer byte size overflow, dropping buffer",
			zap.String("session", sessionID),
			zap.Int64("total_bytes", buffer.TotalBytes))
		buffer.Queries = nil
		buffer.Parameters = nil
		delete(server.txBuffers, sessionID)
		return
	}

	buffer.Queries = append(buffer.Queries, query)
	buffer.Parameters = append(buffer.Parameters, parameters)
	buffer.LastActivity = time.Now()
}

func (server *ProxyServerV15) commitTransactionBuffer(sessionID string) {
	server.txBufferMutex.Lock()
	buffer, exists := server.txBuffers[sessionID]
	if exists {
		delete(server.txBuffers, sessionID)
	}
	server.txBufferMutex.Unlock()

	if !exists || buffer == nil || len(buffer.Queries) == 0 {
		return
	}

	go func() {
		timeout := time.Duration(server.performanceConfig.MirrorTimeoutSecs) * time.Second
		if timeout <= 0 {
			timeout = 2 * time.Minute
		}
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		defer func() {
			if r := recover(); r != nil {
				server.logger.Error("Transaction buffer commit panic",
					zap.String("session", sessionID), zap.Any("panic", r))
			}
			// Help GC
			buffer.Queries = nil
			buffer.Parameters = nil
		}()

		for i, mirror := range server.mirrors {
			if mirror == nil || mirror.pool == nil {
				continue
			}
			if server.mirrorBreakers != nil && !server.mirrorBreakers.Allow(i) {
				server.logger.Debug("Mirror circuit breaker open, skipping tx commit",
					zap.Int("mirror_index", i))
				continue
			}

			conn, err := mirror.pool.Acquire(ctx)
			if err != nil {
				server.logger.Error("Failed to acquire mirror connection for tx commit",
					zap.Int("mirror_index", i), zap.Error(err))
				if server.mirrorBreakers != nil {
					server.mirrorBreakers.RecordFailure(i)
				}
				continue
			}

			if !server.getQueryFilterConfig().SkipMirrorTxLocks {
				if _, err = conn.Exec(ctx, "BEGIN"); err != nil {
					conn.Release()
					if server.mirrorBreakers != nil {
						server.mirrorBreakers.RecordFailure(i)
					}
					continue
				}
			}

			txFailed := false
			for qi, query := range buffer.Queries {
				args := server.convertParameters(buffer.Parameters[qi])
				if _, err = conn.Exec(ctx, query, args...); err != nil {
					server.logger.Error("Mirror transaction query failed",
						zap.Int("mirror_index", i),
						zap.String("query", truncateQuery(query, 200)),
						zap.Error(err))
					if !server.getQueryFilterConfig().SkipMirrorTxLocks {
						conn.Exec(ctx, "ROLLBACK")
					}
					txFailed = true
					break
				}
			}

			if !txFailed && !server.getQueryFilterConfig().SkipMirrorTxLocks {
				_, err = conn.Exec(ctx, "COMMIT")
				if err != nil {
					server.logger.Error("Mirror COMMIT failed",
						zap.Int("mirror_index", i), zap.Error(err))
					txFailed = true
				}
			}

			conn.Release()

			if server.mirrorBreakers != nil {
				if txFailed {
					server.mirrorBreakers.RecordFailure(i)
				} else {
					server.mirrorBreakers.RecordSuccess(i)
				}
			}
		}
	}()
}

func (server *ProxyServerV15) rollbackTransactionBuffer(sessionID string) {
	server.txBufferMutex.Lock()
	defer server.txBufferMutex.Unlock()

	if buffer, exists := server.txBuffers[sessionID]; exists {
		buffer.Queries = nil
		buffer.Parameters = nil
	}
	delete(server.txBuffers, sessionID)
}

func (server *ProxyServerV15) isSessionInTransaction(sessionID string) bool {
	server.txBufferMutex.RLock()
	defer server.txBufferMutex.RUnlock()
	_, exists := server.txBuffers[sessionID]
	return exists
}

// truncateQuery truncates a query string for safe logging.
func truncateQuery(query string, maxLen int) string {
	if len(query) <= maxLen {
		return query
	}
	return query[:maxLen] + "..."
}
