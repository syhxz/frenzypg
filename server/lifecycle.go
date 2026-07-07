package server

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"time"

	wire "github.com/jeroenrinzema/psql-wire"
	"go.uber.org/zap"
)

func (server *ProxyServerV15) connectToPrimary(ctx context.Context, primaryAddress string) error {
	server.logger.Info("Connecting to primary")

	primary := NewConnectionWithPoolConfig(server.logger, Primary, primaryAddress, server.poolConfig)
	err := primary.Connect(ctx, primaryAddress)
	if err != nil {
		return fmt.Errorf("failed to connect to primary: %w", err)
	}
	server.primary = primary

	return nil
}

func (server *ProxyServerV15) connectToMirrors(ctx context.Context, mirrorAddresses []string) error {
	server.mirrors = make([]*Connection, 0, len(mirrorAddresses))

	for i, address := range mirrorAddresses {
		server.logger.Info("Connecting to mirror",
			zap.Int("index", i))

		mirror := NewConnectionWithPoolConfig(server.logger, Mirror, address, server.poolConfig)
		err := mirror.Connect(ctx, address)
		if err != nil {
			server.logger.Error("Failed to connect to mirror, skipping",
				zap.Int("index", i),
				zap.String("address", maskAddress(address)),
				zap.Error(err))
			continue
		}
		server.mirrors = append(server.mirrors, mirror)
	}

	if len(server.mirrors) == 0 {
		server.logger.Warn("No mirror connections established")
	}

	// Initialize circuit breakers for all mirrors
	if len(server.mirrors) > 0 {
		server.mirrorBreakers = NewMirrorCircuitBreakers(
			len(server.mirrors),
			5,                // open after 5 consecutive failures
			30*time.Second,   // try recovery after 30s
			server.logger,
		)

		// Start mirror reconnect manager
		server.mirrorReconnect = NewMirrorReconnectManager(server.mirrors, server.logger, 30*time.Second)
		server.mirrorReconnect.Start()
	}

	return nil
}

// listenAndServeWithTLSServer starts the server with TLS support using an existing wire server
func (server *ProxyServerV15) listenAndServeWithTLSServer(listenAddress string, tlsConfig *tls.Config, wireServer *wire.Server) error {
	server.logger.Info("Starting TLS-enabled server", zap.String("listen", listenAddress))

	listener, err := net.Listen("tcp", listenAddress)
	if err != nil {
		return fmt.Errorf("failed to create listener: %w", err)
	}

	tlsListener := NewLoggingTLSListener(listener, tlsConfig, server.logger)
	return wireServer.Serve(tlsListener)
}

func (server *ProxyServerV15) Close(ctx context.Context) error {
	server.logger.Info("Starting graceful shutdown")

	// 1. Stop accepting new connections
	if server.wireServer != nil {
		server.wireServer.Close()
	}

	// 2. Stop mirror reconnect manager
	if server.mirrorReconnect != nil {
		server.mirrorReconnect.Stop()
	}

	// 3. Flush pending transaction buffers (best-effort with timeout)
	server.flushPendingBuffers()

	// 4. Close primary connection
	if server.primary != nil {
		server.primary.Close(ctx)
	}

	// 5. Close mirrors
	for _, mirror := range server.mirrors {
		if mirror != nil {
			mirror.Close(ctx)
		}
	}

	// 6. Close session manager
	if server.sessionManager != nil {
		server.sessionManager.Close()
	}

	// 7. Stop cleanup routine
	server.stopCleanupRoutine()

	server.logger.Info("Graceful shutdown complete")
	return nil
}

// flushPendingBuffers attempts to flush any pending transaction buffers on shutdown.
func (server *ProxyServerV15) flushPendingBuffers() {
	server.txBufferMutex.Lock()
	pendingCount := len(server.txBuffers)
	// Take ownership of all buffers
	buffers := server.txBuffers
	server.txBuffers = make(map[string]*TransactionBuffer)
	server.txBufferMutex.Unlock()

	if pendingCount == 0 {
		return
	}

	server.logger.Warn("Flushing pending transaction buffers on shutdown",
		zap.Int("pending", pendingCount))

	filterConfig := server.getQueryFilterConfig()

	// Best-effort flush with a short timeout
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for sessionID, buffer := range buffers {
		if buffer == nil || len(buffer.Queries) == 0 {
			continue
		}

		for i, mirror := range server.mirrors {
			if mirror == nil || mirror.pool == nil {
				continue
			}

			conn, err := mirror.pool.Acquire(ctx)
			if err != nil {
				server.logger.Error("Failed to acquire mirror connection during flush",
					zap.String("session", sessionID), zap.Error(err))
				continue
			}

			if !filterConfig.SkipMirrorTxLocks {
				conn.Exec(ctx, "BEGIN")
			}

			failed := false
			for qi, query := range buffer.Queries {
				args := server.convertParameters(buffer.Parameters[qi])
				if _, err := conn.Exec(ctx, query, args...); err != nil {
					server.logger.Error("Mirror flush failed",
						zap.Int("mirror_index", i),
						zap.String("session", sessionID),
						zap.Error(err))
					if !filterConfig.SkipMirrorTxLocks {
						conn.Exec(ctx, "ROLLBACK")
					}
					failed = true
					break
				}
			}

			if !failed && !filterConfig.SkipMirrorTxLocks {
				conn.Exec(ctx, "COMMIT")
			}
			conn.Release()
		}

		buffer.Queries = nil
		buffer.Parameters = nil
	}
}

// startCleanup starts the periodic cleanup routine
func (server *ProxyServerV15) startCleanup() {
	server.cleanupTicker = time.NewTicker(30 * time.Second)
	go func() {
		for {
			select {
			case <-server.cleanupTicker.C:
				func() {
					defer func() {
						if r := recover(); r != nil {
							server.logger.Error("Cleanup routine panic (recovered)", zap.Any("panic", r))
						}
					}()
					server.cleanupExpiredBuffers()
				}()
			case <-server.stopCleanup:
				server.cleanupTicker.Stop()
				return
			}
		}
	}()
}

// stopCleanupRoutine stops the cleanup routine
func (server *ProxyServerV15) stopCleanupRoutine() {
	if server.stopCleanup != nil {
		close(server.stopCleanup)
	}
	if server.cleanupTicker != nil {
		server.cleanupTicker.Stop()
	}
}

// cleanupExpiredBuffers removes expired transaction buffers
func (server *ProxyServerV15) cleanupExpiredBuffers() {
	server.txBufferMutex.Lock()
	defer server.txBufferMutex.Unlock()

	now := time.Now()
	var expired []string

	// Dynamic timeout based on memory pressure
	timeout := 5 * time.Minute
	if len(server.txBuffers) > 50 {
		timeout = 2 * time.Minute
	}
	if len(server.txBuffers) > 200 {
		timeout = 1 * time.Minute
	}

	for sessionID, buffer := range server.txBuffers {
		if now.Sub(buffer.LastActivity) > timeout {
			expired = append(expired, sessionID)
		}
	}

	for _, sessionID := range expired {
		if buffer := server.txBuffers[sessionID]; buffer != nil {
			buffer.Queries = nil
			buffer.Parameters = nil
		}
		delete(server.txBuffers, sessionID)
	}

	if len(expired) > 0 {
		server.logger.Warn("Cleaned up expired transaction buffers",
			zap.Int("cleaned", len(expired)),
			zap.Int("remaining", len(server.txBuffers)))
	}
}

// CleanupSession removes transaction buffer for disconnected session
func (server *ProxyServerV15) CleanupSession(sessionID string) {
	server.txBufferMutex.Lock()
	defer server.txBufferMutex.Unlock()

	if buffer, exists := server.txBuffers[sessionID]; exists {
		buffer.Queries = nil
		buffer.Parameters = nil
		delete(server.txBuffers, sessionID)
		server.logger.Debug("Cleaned up transaction buffer for disconnected session",
			zap.String("session", sessionID))
	}
}

func (server *ProxyServerV15) StartTransactionBuffer(sessionID string) {
	server.startTransactionBuffer(sessionID)
}

func (server *ProxyServerV15) BufferTransactionQuery(sessionID, query string, parameters []wire.Parameter) {
	server.bufferTransactionQuery(sessionID, query, parameters)
}

func (server *ProxyServerV15) CommitTransactionBuffer(sessionID string) {
	server.commitTransactionBuffer(sessionID)
}

// PingPrimary checks if the primary database connection is alive.
func (server *ProxyServerV15) PingPrimary() error {
	if server.primary == nil || server.primary.pool == nil {
		return fmt.Errorf("primary not connected")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	return server.primary.pool.Ping(ctx)
}

// GetBufferStats returns transaction buffer statistics
func (server *ProxyServerV15) GetBufferStats() map[string]interface{} {
	server.txBufferMutex.RLock()
	defer server.txBufferMutex.RUnlock()

	totalQueries := 0
	totalBytes := int64(0)
	for _, buffer := range server.txBuffers {
		totalQueries += len(buffer.Queries)
		totalBytes += buffer.TotalBytes
	}

	return map[string]interface{}{
		"active_transactions":    len(server.txBuffers),
		"total_buffered_queries": totalQueries,
		"total_buffered_bytes":   totalBytes,
	}
}
