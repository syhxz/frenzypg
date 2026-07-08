package server

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
)

// RawProxyServer implements a high-performance PostgreSQL proxy
// that forwards raw protocol bytes without decoding message bodies.
type RawProxyServer struct {
	logger            *zap.Logger
	listenAddr        string
	primaryAddr       string
	mirrorAddrs       []string
	poolConfig        *PoolConfig
	performanceConfig *PerformanceConfig
	filterConfig      atomic.Pointer[QueryFilterConfig]

	// Mirror connection pool (for async mirroring)
	mirrorPools    []*pgxpool.Pool
	mirrorPoolsMu  sync.RWMutex

	// Circuit breaker for mirror health
	mirrorBreakers *MirrorCircuitBreakers

	// Mirror reconnect manager
	mirrorReconnectStop chan struct{}

	// Metrics
	metrics *Metrics

	// TLS config for client-facing connections
	clientTLSConfig *tls.Config

	// Connection management
	maxConnections int64 // 0 = unlimited
	activeConns    atomic.Int64
	totalConns     atomic.Int64
	connWg         sync.WaitGroup // tracks active connections for graceful drain
	idleTimeout    time.Duration  // close idle client connections after this duration

	// Transaction buffering for raw mode (mirrors commit-time replay)
	txBuffers      sync.Map // map[net.Conn]*rawTxBuffer

	// Mirror queue: durable async delivery via Kafka
	mirrorQueue *MirrorQueue
	kafkaConfig *KafkaQueueConfig

	// Shutdown
	listener net.Listener
	cancel   context.CancelFunc
}

// rawTxBuffer holds buffered SQL statements during a transaction in raw mode.
// Statements are only flushed to mirrors on COMMIT; discarded on ROLLBACK.
type rawTxBuffer struct {
	queries    []string
	totalBytes int64
}

const maxRawTxBufferBytes = 10 * 1024 * 1024 // 10MB per transaction

func NewRawProxyServer(logger *zap.Logger) *RawProxyServer {
	s := &RawProxyServer{
		logger:              logger,
		metrics:             NewMetrics("frenzy"),
		mirrorReconnectStop: make(chan struct{}),
		idleTimeout:         30 * time.Minute, // default idle timeout
	}
	defaultFilter := &QueryFilterConfig{
		SkipRollbackMirror: false,
		SkipFailedTxMirror: true,
		SkipMirrorTxLocks:  false,
	}
	s.filterConfig.Store(defaultFilter)
	return s
}

func (s *RawProxyServer) SetPoolConfig(config *PoolConfig) {
	s.poolConfig = config
}

func (s *RawProxyServer) SetPerformanceConfig(config *PerformanceConfig) {
	s.performanceConfig = config
}

func (s *RawProxyServer) SetMaxConnections(max int) {
	s.maxConnections = int64(max)
}

func (s *RawProxyServer) SetIdleTimeout(d time.Duration) {
	if d > 0 {
		s.idleTimeout = d
	}
}

func (s *RawProxyServer) SetQueryFilterConfig(config *QueryFilterConfig) {
	s.filterConfig.Store(config)
}

func (s *RawProxyServer) SetKafkaConfig(config *KafkaQueueConfig) {
	s.kafkaConfig = config
}

func (s *RawProxyServer) getFilterConfig() *QueryFilterConfig {
	return s.filterConfig.Load()
}

// ListenAndServe starts the raw proxy server.
func (s *RawProxyServer) ListenAndServe(
	ctx context.Context,
	listenAddr string,
	primaryAddr string,
	mirrorAddrs []string,
	tlsConfig *tls.Config,
) error {
	s.listenAddr = listenAddr
	s.primaryAddr = primaryAddr
	s.mirrorAddrs = mirrorAddrs
	s.clientTLSConfig = tlsConfig

	ctx, cancel := context.WithCancel(ctx)
	s.cancel = cancel

	// Initialize mirror pools
	if err := s.initMirrorPools(ctx); err != nil {
		s.logger.Error("Failed to initialize mirror pools", zap.Error(err))
		// Non-fatal: continue without mirrors
	}

	// Initialize circuit breakers for mirrors
	if len(s.mirrorPools) > 0 {
		s.mirrorBreakers = NewMirrorCircuitBreakers(
			len(s.mirrorPools),
			5,              // open after 5 consecutive failures
			30*time.Second, // try recovery after 30s
			s.logger,
		)

		// Initialize mirror queue with Kafka
		if s.kafkaConfig != nil && len(s.kafkaConfig.Brokers) > 0 {
			mq, err := NewMirrorQueue(s.kafkaConfig, s.logger)
			if err != nil {
				s.logger.Warn("Failed to create Kafka mirror queue, falling back to direct mode", zap.Error(err))
			} else {
				s.mirrorQueue = mq
				workers := s.kafkaConfig.Workers
				if workers <= 0 {
					workers = 4
				}
				s.mirrorQueue.Start(s.processMirrorTask, workers)
			}
		}
	}

	// Start listener
	listener, err := net.Listen("tcp", listenAddr)
	if err != nil {
		cancel()
		return fmt.Errorf("failed to listen on %s: %w", listenAddr, err)
	}
	s.listener = listener

	s.logger.Info("Raw proxy server started",
		zap.String("listen", listenAddr),
		zap.String("primary", maskAddress(primaryAddr)),
		zap.Int("mirrors", len(mirrorAddrs)),
		zap.Int64("max_connections", s.maxConnections),
		zap.Duration("idle_timeout", s.idleTimeout))

	// Start mirror reconnect goroutine
	if len(s.mirrorPools) > 0 {
		go s.mirrorReconnectLoop(ctx)
	}

	// Start SIGHUP config reload listener
	go s.listenForReload()

	// Accept loop
	go func() {
		<-ctx.Done()
		listener.Close()
	}()

	for {
		conn, err := listener.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				// Wait for active connections to drain (with timeout)
				s.drainConnections(10 * time.Second)
				return nil
			default:
				s.logger.Error("Accept error", zap.Error(err))
				continue
			}
		}

		// Connection limit check (atomic CAS to prevent TOCTOU race)
		if s.maxConnections > 0 {
			for {
				current := s.activeConns.Load()
				if current >= s.maxConnections {
					s.logger.Warn("Connection limit reached, rejecting",
						zap.Int64("active", current),
						zap.Int64("max", s.maxConnections))
					conn.Close()
					break
				}
				if s.activeConns.CompareAndSwap(current, current+1) {
					// Successfully incremented
					s.totalConns.Add(1)
					s.connWg.Add(1)
					if s.metrics != nil {
						s.metrics.ActiveConnections.Set(float64(s.activeConns.Load()))
					}
					go s.handleConnection(ctx, conn)
					break
				}
				// CAS failed, another goroutine incremented — retry
			}
			continue
		}

		s.activeConns.Add(1)
		s.totalConns.Add(1)
		s.connWg.Add(1)
		if s.metrics != nil {
			s.metrics.ActiveConnections.Set(float64(s.activeConns.Load()))
		}
		go s.handleConnection(ctx, conn)
	}
}

func (s *RawProxyServer) Close(ctx context.Context) error {
	s.logger.Info("Starting graceful shutdown")

	// 1. Stop accepting new connections
	if s.cancel != nil {
		s.cancel()
	}
	if s.listener != nil {
		s.listener.Close()
	}

	// 2. Stop mirror reconnect
	close(s.mirrorReconnectStop)

	// 3. Wait for active connections to drain
	s.drainConnections(15 * time.Second)

	// 4. Close mirror pools
	s.mirrorPoolsMu.Lock()
	for _, pool := range s.mirrorPools {
		if pool != nil {
			pool.Close()
		}
	}
	s.mirrorPools = nil
	s.mirrorPoolsMu.Unlock()

	// 5. Stop mirror queue (drain remaining items)
	if s.mirrorQueue != nil {
		stats := s.mirrorQueue.Stats()
		s.logger.Info("Draining mirror queue before shutdown",
			zap.Int64("local_buffer", stats["local_buffer"]),
			zap.Int64("produced", stats["produced"]),
			zap.Int64("consumed", stats["consumed"]))
		s.mirrorQueue.Stop(30 * time.Second)
	}

	s.logger.Info("Raw proxy server shut down",
		zap.Int64("total_connections_served", s.totalConns.Load()))
	return nil
}

// drainConnections waits for all active connections to finish, with timeout.
func (s *RawProxyServer) drainConnections(timeout time.Duration) {
	if s.activeConns.Load() == 0 {
		return
	}

	s.logger.Info("Waiting for active connections to drain",
		zap.Int64("active", s.activeConns.Load()),
		zap.Duration("timeout", timeout))

	done := make(chan struct{})
	go func() {
		s.connWg.Wait()
		close(done)
	}()

	select {
	case <-done:
		s.logger.Info("All connections drained")
	case <-time.After(timeout):
		s.logger.Warn("Drain timeout, forcing shutdown",
			zap.Int64("remaining", s.activeConns.Load()))
	}
}

// PingPrimary checks if the primary is reachable (for health check compatibility).
func (s *RawProxyServer) PingPrimary() error {
	conn, err := net.DialTimeout("tcp", extractHostPort(s.primaryAddr), 3*time.Second)
	if err != nil {
		return err
	}
	conn.Close()
	return nil
}

// initMirrorPools creates connection pools to each mirror for async query mirroring.
func (s *RawProxyServer) initMirrorPools(ctx context.Context) error {
	s.mirrorPools = make([]*pgxpool.Pool, 0, len(s.mirrorAddrs))

	for i, addr := range s.mirrorAddrs {
		config, err := pgxpool.ParseConfig(addr)
		if err != nil {
			s.logger.Error("Failed to parse mirror config", zap.Int("index", i), zap.Error(err))
			continue
		}

		poolCfg := s.poolConfig
		if poolCfg == nil {
			poolCfg = DefaultPoolConfig()
		}
		config.MaxConns = poolCfg.MaxConns
		config.MinConns = poolCfg.MinConns
		config.MaxConnIdleTime = poolCfg.MaxConnIdleTime
		if config.ConnConfig.TLSConfig != nil {
			config.ConnConfig.TLSConfig.ClientSessionCache = tls.NewLRUClientSessionCache(32)
		}

		pool, err := pgxpool.NewWithConfig(ctx, config)
		if err != nil {
			s.logger.Error("Failed to create mirror pool", zap.Int("index", i), zap.Error(err))
			continue
		}

		// Verify connectivity
		c, err := pool.Acquire(ctx)
		if err != nil {
			s.logger.Error("Mirror pool connectivity check failed", zap.Int("index", i), zap.Error(err))
			pool.Close()
			continue
		}
		c.Release()

		s.mirrorPools = append(s.mirrorPools, pool)
		s.logger.Info("Mirror pool initialized", zap.Int("index", i), zap.String("addr", maskAddress(addr)))
	}

	return nil
}

// handleConnection handles a single client connection by proxying to the primary.
func (s *RawProxyServer) handleConnection(ctx context.Context, clientConn net.Conn) {
	defer func() {
		clientConn.Close()
		s.activeConns.Add(-1)
		s.connWg.Done()
		if s.metrics != nil {
			s.metrics.ActiveConnections.Set(float64(s.activeConns.Load()))
		}
		if r := recover(); r != nil {
			s.logger.Error("Connection handler panic", zap.Any("panic", r))
		}
	}()

	// Connect to primary backend
	backendConn, err := s.connectToBackend(ctx)
	if err != nil {
		s.logger.Error("Failed to connect to primary", zap.Error(err))
		return
	}
	defer backendConn.Close()

	// Handle startup phase: client → proxy → backend
	// handleStartup may upgrade clientConn to TLS, returning the new conn
	finalClientConn, err := s.handleStartup(clientConn, backendConn)
	if err != nil {
		s.logger.Error("Startup handshake failed", zap.Error(err))
		return
	}

	// Main proxy loop (use the potentially TLS-upgraded connection)
	s.proxyLoop(ctx, finalClientConn, backendConn)

	// Clean up tx buffer using the same conn reference used in proxyLoop
	s.cleanupTxBuffer(finalClientConn)
}

// connectToBackend establishes a TCP connection to the primary PostgreSQL.
func (s *RawProxyServer) connectToBackend(ctx context.Context) (net.Conn, error) {
	hostPort := extractHostPort(s.primaryAddr)
	dialer := &net.Dialer{Timeout: 10 * time.Second}
	return dialer.DialContext(ctx, "tcp", hostPort)
}

// handleStartup relays the full startup/authentication exchange between client and backend.
// The proxy is transparent — it passes all messages through without modification.
// Returns the final client connection (may be TLS-upgraded) and any error.
func (s *RawProxyServer) handleStartup(clientConn, backendConn net.Conn) (net.Conn, error) {
	// Loop handles SSL negotiation (max 2 iterations: one SSL upgrade + one real startup)
	for attempts := 0; attempts < 3; attempts++ {
		// Read client startup message (no type byte, just length + payload)
		lenBuf := make([]byte, 4)
		if _, err := io.ReadFull(clientConn, lenBuf); err != nil {
			return nil, fmt.Errorf("read startup length: %w", err)
		}
		msgLen := int(binary.BigEndian.Uint32(lenBuf))
		if msgLen < 4 || msgLen > 10000 {
			return nil, fmt.Errorf("invalid startup message length: %d", msgLen)
		}

		payload := make([]byte, msgLen-4)
		if _, err := io.ReadFull(clientConn, payload); err != nil {
			return nil, fmt.Errorf("read startup payload: %w", err)
		}

		// Check if it's an SSL request (protocol version 80877103)
		if msgLen == 8 {
			version := binary.BigEndian.Uint32(payload[:4])
			if version == 80877103 { // SSLRequest
				if s.clientTLSConfig != nil {
					// Upgrade to TLS
					if _, err := clientConn.Write([]byte{'S'}); err != nil {
						return nil, fmt.Errorf("write SSL accept: %w", err)
					}
					tlsConn := tls.Server(clientConn, s.clientTLSConfig)
					if err := tlsConn.Handshake(); err != nil {
						return nil, fmt.Errorf("TLS handshake failed: %w", err)
					}
					// Continue with TLS connection — loop to re-read startup
					clientConn = tlsConn
					continue
				}
				// Tell client we don't support SSL (send 'N')
				if _, err := clientConn.Write([]byte{'N'}); err != nil {
					return nil, fmt.Errorf("write SSL reject: %w", err)
				}
				// Loop to re-read the actual startup message
				continue
			}
			// Check for cancel request (protocol version 80877102)
			if version == 80877102 { // CancelRequest
				// Forward cancel to backend and close
				backendConn.Write(lenBuf)
				backendConn.Write(payload)
				return nil, fmt.Errorf("cancel request received")
			}
		}

		// Forward startup message to backend
		if _, err := backendConn.Write(lenBuf); err != nil {
			return nil, fmt.Errorf("write startup length to backend: %w", err)
		}
		if _, err := backendConn.Write(payload); err != nil {
			return nil, fmt.Errorf("write startup payload to backend: %w", err)
		}

		// Relay authentication exchange until ReadyForQuery
		buf := make([]byte, 8192)
		for {
			// Read message from backend
			msgType, msgData, err := readRawMessage(backendConn, buf)
			if err != nil {
				return nil, fmt.Errorf("read backend auth message: %w", err)
			}

			// Forward to client
			if err := writeRawMessage(clientConn, msgType, msgData); err != nil {
				return nil, fmt.Errorf("write auth message to client: %w", err)
			}

			switch msgType {
			case 'R': // AuthenticationXxx
				// Check if backend wants password/SASL from client
				if len(msgData) >= 4 {
					authType := binary.BigEndian.Uint32(msgData[:4])
					if authType != 0 { // 0 = AuthenticationOk
						// Relay client's auth response to backend
						if err := s.relayClientAuth(clientConn, backendConn, buf); err != nil {
							return nil, err
						}
					}
				}
			case 'Z': // ReadyForQuery — startup complete
				return clientConn, nil
			case 'E': // ErrorResponse
				return nil, fmt.Errorf("backend authentication error")
			}
		}
	}
	return nil, fmt.Errorf("startup negotiation failed: too many attempts")
}

// relayClientAuth relays authentication messages from client to backend until backend responds.
// Handles multi-step auth protocols like SCRAM-SHA-256 where the server may send
// multiple challenge messages before AuthenticationOk.
func (s *RawProxyServer) relayClientAuth(clientConn, backendConn net.Conn, buf []byte) error {
	for {
		// Read from client
		msgType, msgData, err := readRawMessage(clientConn, buf)
		if err != nil {
			return fmt.Errorf("read client auth: %w", err)
		}

		// Forward to backend
		if err := writeRawMessage(backendConn, msgType, msgData); err != nil {
			return fmt.Errorf("write client auth to backend: %w", err)
		}

		// Read backend response
		respType, respData, err := readRawMessage(backendConn, buf)
		if err != nil {
			return fmt.Errorf("read backend auth response: %w", err)
		}

		// Forward to client
		if err := writeRawMessage(clientConn, respType, respData); err != nil {
			return fmt.Errorf("write backend auth response to client: %w", err)
		}

		switch respType {
		case 'R':
			if len(respData) >= 4 {
				authType := binary.BigEndian.Uint32(respData[:4])
				switch authType {
				case 0: // AuthenticationOk
					return nil
				case 12: // AuthenticationSASLFinal
					// SCRAM-SHA-256: after SASLFinal, the server will send
					// AuthenticationOk next WITHOUT any client message in between.
					// Return to the outer handleStartup loop to read it.
					return nil
				}
				// For other auth types (10=SASL, 11=SASLContinue, 3=CleartextPassword,
				// 5=MD5Password), the client needs to respond — loop again.
			}
		case 'Z':
			return nil
		case 'E':
			return fmt.Errorf("auth failed")
		}
	}
}

// proxyLoop is the main forwarding loop after startup is complete.
func (s *RawProxyServer) proxyLoop(ctx context.Context, clientConn, backendConn net.Conn) {
	// Buffer for reading messages (reused across iterations)
	clientBuf := make([]byte, 32*1024)
	backendBuf := make([]byte, 64*1024)

	// Transaction state tracking
	var inTransaction bool

	// Extended Query Protocol: track pending SQL from Parse messages
	// Key: statement name ("" for unnamed), Value: SQL text
	namedStmts := make(map[string]string)
	const maxNamedStmts = 10000 // prevent unbounded memory from malicious clients
	var pendingMirrorSQL []string // SQLs to mirror after successful Sync response
	var pendingSimpleSQL string   // SQL to mirror after successful SimpleQuery response
	var pendingResponses int      // count of messages expecting a response (for Flush handling)

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Read one message from client (with idle timeout)
		if s.idleTimeout > 0 {
			clientConn.SetReadDeadline(time.Now().Add(s.idleTimeout))
		}
		msgType, msgData, err := readRawMessage(clientConn, clientBuf)
		if err != nil {
			if err != io.EOF {
				if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
					s.logger.Debug("Client idle timeout, closing connection")
				} else {
					s.logger.Debug("Client read error", zap.Error(err))
				}
			}
			return
		}
		// Clear deadline after successful read
		clientConn.SetReadDeadline(time.Time{})

		// Handle terminate
		if msgType == 'X' {
			writeRawMessage(backendConn, msgType, msgData)
			return
		}

		// Mirror decision based on message type
		if len(s.mirrorPools) > 0 {
			switch msgType {
			case 'Q': // SimpleQuery — mirror AFTER response (to skip failed queries)
				// SQL extracted here, mirrored below after we see the response state
				pendingSimpleSQL = extractSQL(msgData)

			case 'P': // Parse — extract and store SQL by statement name
				stmtName, sql := extractParseSQL(msgData)
				if sql != "" {
					if len(namedStmts) >= maxNamedStmts {
						// Evict oldest entries to make room (remove ~10% of entries)
						evictCount := maxNamedStmts / 10
						for k := range namedStmts {
							delete(namedStmts, k)
							evictCount--
							if evictCount <= 0 {
								break
							}
						}
					}
					namedStmts[stmtName] = sql
				}

			case 'B': // Bind — map portal to the SQL from its source statement
				_, stmtName := extractBindNames(msgData)
				if sql, ok := namedStmts[stmtName]; ok {
					if !strings.Contains(sql, "$") {
						// No parameters — mirror SQL as-is
						pendingMirrorSQL = append(pendingMirrorSQL, sql)
					} else {
						// Parameterized query: extract text parameters from Bind message
						// and inline them into the SQL for mirror replay.
						if resolved, ok := resolveBindParameters(sql, msgData); ok {
							pendingMirrorSQL = append(pendingMirrorSQL, resolved)
						}
						// If parameter extraction fails (binary format, etc.),
						// we skip mirroring this query — logged at debug level.
					}
				}

			case 'E': // Execute — nothing extra needed, SQL already queued in Bind

			case 'C': // Close — clean up named statement from cache
				if len(msgData) > 1 && msgData[0] == 'S' {
					// Close(Statement): remove from namedStmts
					nameEnd := bytes.IndexByte(msgData[1:], 0)
					if nameEnd >= 0 {
						name := string(msgData[1 : 1+nameEnd])
						delete(namedStmts, name)
					}
				}

			case 'S': // Sync — will trigger mirror after response
				// pendingMirrorSQL will be mirrored after ReadyForQuery
			}
		}

		// Forward to primary backend
		if err := writeRawMessage(backendConn, msgType, msgData); err != nil {
			s.logger.Error("Backend write error", zap.Error(err))
			return
		}

		// Track messages that will generate a response from the backend
		switch msgType {
		case 'P', 'B', 'C', 'D': // Parse→ParseComplete, Bind→BindComplete, Close→CloseComplete, Describe→RowDescription/NoData
			pendingResponses++
		case 'E': // Execute→CommandComplete/DataRow+CommandComplete/EmptyQueryResponse/ErrorResponse
			pendingResponses++
		}

		// For Sync or SimpleQuery, read response until ReadyForQuery
		// For other Extended Query messages (P, B, E without Sync), read immediate response
		if msgType == 'Q' || msgType == 'S' {
			newTxState, err := s.forwardResponse(backendConn, clientConn, backendBuf)
			if err != nil {
				if err != io.EOF {
					s.logger.Debug("Backend response error", zap.Error(err))
				}
				return
			}
			pendingResponses = 0 // All responses consumed by ReadyForQuery

			// After SimpleQuery: mirror only if not in error state
			if msgType == 'Q' && pendingSimpleSQL != "" {
				if newTxState != 'E' || !s.getFilterConfig().SkipFailedTxMirror {
					s.maybeMirror(ctx, pendingSimpleSQL, inTransaction, clientConn)
				}
				pendingSimpleSQL = ""
			}

			// After Sync: mirror pending Extended Query SQLs
			if msgType == 'S' && len(pendingMirrorSQL) > 0 {
				// Only mirror if not in error state
				if newTxState != 'E' || !s.getFilterConfig().SkipFailedTxMirror {
					for _, sql := range pendingMirrorSQL {
						s.maybeMirror(ctx, sql, inTransaction, clientConn)
					}
				}
				pendingMirrorSQL = pendingMirrorSQL[:0]
			}

			// Update transaction state from ReadyForQuery byte
			inTransaction = (newTxState == 'T' || newTxState == 'E')
		} else if msgType == 'P' || msgType == 'B' || msgType == 'D' || msgType == 'E' || msgType == 'C' {
			// Extended Query sub-messages (without Flush): backend queues responses
			// (ParseComplete, BindComplete, etc.) but NOT ReadyForQuery.
			// We don't read responses here; they'll be consumed when Sync arrives.
		} else if msgType == 'H' {
			// Flush: client expects all pending backend responses immediately.
			// Read exactly pendingResponses messages from backend.
			if pendingResponses > 0 {
				if err := s.forwardPendingResponses(backendConn, clientConn, backendBuf, pendingResponses); err != nil {
					if err != io.EOF {
						s.logger.Debug("Flush response error", zap.Error(err))
					}
					return
				}
				pendingResponses = 0
			}
		} else {
			// FunctionCall ('F') or other rare message types that produce ReadyForQuery
			newTxState, err := s.forwardResponse(backendConn, clientConn, backendBuf)
			if err != nil {
				if err != io.EOF {
					s.logger.Debug("Backend response error", zap.Error(err))
				}
				return
			}
			inTransaction = (newTxState == 'T' || newTxState == 'E')
		}
	}
}

// forwardPendingResponses reads exactly `count` completion responses from backend
// and forwards them to client. Used after Flush to unblock client without waiting
// for ReadyForQuery. Each "completion" is one top-level response: ParseComplete('1'),
// BindComplete('2'), CloseComplete('3'), NoData('n'), RowDescription('T'),
// CommandComplete('C'), EmptyQueryResponse('I'), ErrorResponse('E'),
// PortalSuspended('s'). DataRow('D') messages are forwarded but don't count.
func (s *RawProxyServer) forwardPendingResponses(backendConn, clientConn net.Conn, buf []byte, count int) error {
	completed := 0
	for completed < count {
		msgType, msgData, err := readRawMessage(backendConn, buf)
		if err != nil {
			return err
		}

		if err := writeRawMessage(clientConn, msgType, msgData); err != nil {
			return err
		}

		// Count completion messages
		switch msgType {
		case '1', '2', '3', 'n', 'C', 'I', 's', 'E':
			// ParseComplete, BindComplete, CloseComplete, NoData,
			// CommandComplete, EmptyQueryResponse, PortalSuspended, ErrorResponse
			completed++
		case 'T': // RowDescription — counts as Describe response completion
			completed++
		// 'D' (DataRow), 't' (ParameterDescription), 'N' (NoticeResponse),
		// 'A' (NotificationResponse) — intermediate/async, don't count
		}
	}
	return nil
}

// forwardResponse reads all backend messages and forwards to client until ReadyForQuery.
// Returns the transaction state byte from ReadyForQuery ('I', 'T', or 'E').
// Handles COPY protocol: if backend sends CopyInResponse ('G') or CopyOutResponse ('H'),
// switches to the appropriate copy relay mode before continuing to ReadyForQuery.
func (s *RawProxyServer) forwardResponse(backendConn, clientConn net.Conn, buf []byte) (byte, error) {
	for {
		msgType, msgData, err := readRawMessage(backendConn, buf)
		if err != nil {
			return 0, err
		}

		// Forward to client
		if err := writeRawMessage(clientConn, msgType, msgData); err != nil {
			return 0, err
		}

		switch msgType {
		case 'Z': // ReadyForQuery marks end of response
			if len(msgData) >= 1 {
				return msgData[0], nil // 'I'=idle, 'T'=in tx, 'E'=error
			}
			return 'I', nil

		case 'G': // CopyInResponse — client needs to send data to backend
			if err := s.relayCopyIn(clientConn, backendConn, buf); err != nil {
				return 0, err
			}
			// After CopyIn completes, backend will send CommandComplete + ReadyForQuery
			// Continue the loop to read those

		case 'W': // CopyBothResponse (for streaming replication, rare)
			// Treat same as CopyIn for safety
			if err := s.relayCopyIn(clientConn, backendConn, buf); err != nil {
				return 0, err
			}

		case 'H': // CopyOutResponse — backend sends data to client
			// Note: this is backend 'H' (CopyOutResponse), not client 'H' (Flush)
			if err := s.relayCopyOut(backendConn, clientConn, buf); err != nil {
				return 0, err
			}
			// After CopyOut completes, backend sends CommandComplete + ReadyForQuery
			// Continue the loop to read those
		}
	}
}

// relayCopyIn relays COPY data from client to backend.
// Client sends: CopyData ('d') × N, then CopyDone ('c') or CopyFail ('f').
func (s *RawProxyServer) relayCopyIn(clientConn, backendConn net.Conn, buf []byte) error {
	for {
		msgType, msgData, err := readRawMessage(clientConn, buf)
		if err != nil {
			return fmt.Errorf("read copy-in from client: %w", err)
		}

		// Forward to backend
		if err := writeRawMessage(backendConn, msgType, msgData); err != nil {
			return fmt.Errorf("write copy-in to backend: %w", err)
		}

		// CopyDone or CopyFail ends the COPY stream
		if msgType == 'c' || msgType == 'f' {
			return nil
		}
		// 'd' = CopyData, keep relaying
	}
}

// relayCopyOut relays COPY data from backend to client.
// Backend sends: CopyData ('d') × N, then CopyDone ('c').
func (s *RawProxyServer) relayCopyOut(backendConn, clientConn net.Conn, buf []byte) error {
	for {
		msgType, msgData, err := readRawMessage(backendConn, buf)
		if err != nil {
			return fmt.Errorf("read copy-out from backend: %w", err)
		}

		// Forward to client
		if err := writeRawMessage(clientConn, msgType, msgData); err != nil {
			return fmt.Errorf("write copy-out to client: %w", err)
		}

		// CopyDone ends the COPY stream
		if msgType == 'c' {
			return nil
		}
		// 'd' = CopyData, keep relaying
	}
}

// mirrorReconnectLoop periodically checks mirror pool health and reconnects if needed.
func (s *RawProxyServer) mirrorReconnectLoop(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.checkMirrorHealth(ctx)
		case <-s.mirrorReconnectStop:
			return
		case <-ctx.Done():
			return
		}
	}
}

func (s *RawProxyServer) checkMirrorHealth(ctx context.Context) {
	checkCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	s.mirrorPoolsMu.RLock()
	pools := make([]*pgxpool.Pool, len(s.mirrorPools))
	copy(pools, s.mirrorPools)
	s.mirrorPoolsMu.RUnlock()

	for i, pool := range pools {
		if pool == nil {
			continue
		}
		if err := pool.Ping(checkCtx); err != nil {
			s.logger.Warn("Mirror ping failed",
				zap.Int("mirror", i),
				zap.Error(err))
			// Attempt to recreate the pool
			s.reconnectMirror(checkCtx, i)
		}
	}
}

func (s *RawProxyServer) reconnectMirror(ctx context.Context, index int) {
	if index >= len(s.mirrorAddrs) {
		return
	}
	addr := s.mirrorAddrs[index]

	config, err := pgxpool.ParseConfig(addr)
	if err != nil {
		s.logger.Error("Mirror reconnect: parse config failed", zap.Int("mirror", index), zap.Error(err))
		return
	}

	poolCfg := s.poolConfig
	if poolCfg == nil {
		poolCfg = DefaultPoolConfig()
	}
	config.MaxConns = poolCfg.MaxConns
	config.MinConns = poolCfg.MinConns
	config.MaxConnIdleTime = poolCfg.MaxConnIdleTime

	newPool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		s.logger.Error("Mirror reconnect: create pool failed", zap.Int("mirror", index), zap.Error(err))
		return
	}

	if err := newPool.Ping(ctx); err != nil {
		newPool.Close()
		s.logger.Error("Mirror reconnect: ping failed", zap.Int("mirror", index), zap.Error(err))
		return
	}

	// Swap old pool with new one (protected by mutex)
	s.mirrorPoolsMu.Lock()
	oldPool := s.mirrorPools[index]
	s.mirrorPools[index] = newPool
	s.mirrorPoolsMu.Unlock()
	if oldPool != nil {
		go func() {
			time.Sleep(5 * time.Second)
			oldPool.Close()
		}()
	}

	s.logger.Info("Mirror reconnected successfully", zap.Int("mirror", index))
}

// listenForReload listens for SIGHUP to hot-reload filter configuration.
func (s *RawProxyServer) listenForReload() {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGHUP)

	for {
		select {
		case <-sigCh:
			s.logger.Info("Received SIGHUP, reloading filter config not implemented for raw mode (use config file restart)")
			// Future: reload filter config from file
		case <-s.mirrorReconnectStop:
			signal.Stop(sigCh)
			return
		}
	}
}

// maybeMirror decides whether to mirror a SQL query and sends it asynchronously.
// When in a transaction, it buffers queries and only flushes to mirrors on COMMIT.
// On ROLLBACK, the buffer is discarded — preventing data inconsistency.
// Handles multi-statement SimpleQuery (e.g., "BEGIN; INSERT ...; COMMIT;") by
// splitting on semicolons and processing each statement individually.
func (s *RawProxyServer) maybeMirror(ctx context.Context, sql string, inTransaction bool, clientConn net.Conn) {
	if sql == "" {
		return
	}

	// Check for multi-statement: if SQL contains ';' followed by non-whitespace,
	// split and process each statement separately.
	if strings.Contains(sql, ";") {
		trimmedCheck := strings.TrimSpace(sql)
		// Quick check: does it have content after the first semicolon?
		semiIdx := strings.Index(trimmedCheck, ";")
		if semiIdx >= 0 && semiIdx < len(trimmedCheck)-1 {
			rest := strings.TrimSpace(trimmedCheck[semiIdx+1:])
			if rest != "" && rest != ";" {
				// Multi-statement — split and process each
				stmts := splitStatements(sql)
				for _, stmt := range stmts {
					s.maybeMirrorSingle(ctx, stmt, inTransaction, clientConn)
					// Update inTransaction based on what we just processed
					upper := strings.TrimSpace(strings.ToUpper(stmt))
					if strings.HasPrefix(upper, "BEGIN") || strings.HasPrefix(upper, "START TRANSACTION") {
						inTransaction = true
					} else if strings.HasPrefix(upper, "COMMIT") || strings.HasPrefix(upper, "ROLLBACK") ||
						upper == "END" || strings.HasPrefix(upper, "END;") || strings.HasPrefix(upper, "END ") {
						inTransaction = false
					}
				}
				return
			}
		}
	}

	s.maybeMirrorSingle(ctx, sql, inTransaction, clientConn)
}

// splitStatements splits a multi-statement SQL string on semicolons,
// respecting single-quoted strings (doesn't split inside quotes).
func splitStatements(sql string) []string {
	var stmts []string
	var current strings.Builder
	inQuote := false
	for _, ch := range sql {
		if ch == '\'' && !inQuote {
			inQuote = true
			current.WriteRune(ch)
		} else if ch == '\'' && inQuote {
			inQuote = false
			current.WriteRune(ch)
		} else if ch == ';' && !inQuote {
			stmt := strings.TrimSpace(current.String())
			if stmt != "" {
				stmts = append(stmts, stmt)
			}
			current.Reset()
		} else {
			current.WriteRune(ch)
		}
	}
	// Last statement (no trailing semicolon)
	if stmt := strings.TrimSpace(current.String()); stmt != "" {
		stmts = append(stmts, stmt)
	}
	return stmts
}

// maybeMirrorSingle handles a single SQL statement for mirroring.
func (s *RawProxyServer) maybeMirrorSingle(ctx context.Context, sql string, inTransaction bool, clientConn net.Conn) {
	if sql == "" {
		return
	}

	filterConfig := s.getFilterConfig()

	// Use existing filter logic
	trimmed := strings.TrimSpace(strings.ToUpper(sql))

	// Handle transaction control commands specially for buffering
	isBegin := strings.HasPrefix(trimmed, "BEGIN") || strings.HasPrefix(trimmed, "START TRANSACTION")
	isCommit := strings.HasPrefix(trimmed, "COMMIT") || trimmed == "END" || strings.HasPrefix(trimmed, "END;") || strings.HasPrefix(trimmed, "END ")
	isRollback := strings.HasPrefix(trimmed, "ROLLBACK")

	// Transaction buffering logic
	if isBegin {
		// Start a new transaction buffer.
		// We do NOT send BEGIN to mirror now — it will be sent as part of
		// the atomic transaction replay when COMMIT arrives.
		s.txBuffers.Store(clientConn, &rawTxBuffer{})
		return
	}

	if isCommit {
		// Flush buffered queries to mirror, then COMMIT
		if bufVal, ok := s.txBuffers.LoadAndDelete(clientConn); ok {
			buf := bufVal.(*rawTxBuffer)
			if filterConfig.SkipMirrorTxLocks {
				// No transaction wrapping — replay statements individually
				for _, q := range buf.queries {
					s.doMirror(ctx, q)
				}
			} else {
				// Replay as a single transaction on one connection per mirror
				allSQL := make([]string, 0, len(buf.queries)+2)
				allSQL = append(allSQL, "BEGIN")
				allSQL = append(allSQL, buf.queries...)
				allSQL = append(allSQL, sql) // COMMIT
				s.doMirrorTransaction(ctx, allSQL)
			}
		} else if !filterConfig.SkipMirrorTxLocks {
			// No buffer (e.g., implicit transaction) — mirror COMMIT directly
			s.doMirror(ctx, sql)
		}
		return
	}

	if isRollback {
		// Discard the buffer — do NOT mirror anything from this transaction.
		// Since we never sent BEGIN to mirror, no ROLLBACK is needed either.
		s.txBuffers.Delete(clientConn)
		return
	}

	// Regular statement: buffer if in transaction, else mirror immediately
	if bufVal, ok := s.txBuffers.Load(clientConn); ok {
		// In a buffered transaction — add to buffer
		buf := bufVal.(*rawTxBuffer)
		if s.shouldMirrorSQL(trimmed, filterConfig) {
			if buf.totalBytes+int64(len(sql)) <= maxRawTxBufferBytes {
				buf.queries = append(buf.queries, sql)
				buf.totalBytes += int64(len(sql))
			} else {
				s.logger.Warn("Transaction buffer exceeded limit, dropping query",
					zap.Int64("buffer_bytes", buf.totalBytes))
			}
		}
		return
	}

	// Not in a transaction — mirror immediately if allowed
	if s.shouldMirrorSQL(trimmed, filterConfig) {
		s.doMirror(ctx, sql)
	}
}

// shouldMirrorSQL checks filter rules to determine if a SQL statement should be mirrored.
func (s *RawProxyServer) shouldMirrorSQL(trimmed string, filterConfig *QueryFilterConfig) bool {
	if filterConfig.MirrorAllQueries {
		return true
	}

	if strings.HasPrefix(trimmed, "SAVEPOINT") || strings.HasPrefix(trimmed, "RELEASE") {
		return true
	}

	if strings.HasPrefix(trimmed, "SELECT") || strings.HasPrefix(trimmed, "WITH") {
		if strings.HasPrefix(trimmed, "SELECT SETVAL") || strings.HasPrefix(trimmed, "SELECT NEXTVAL") {
			return true
		}
		return filterConfig.MirrorSelectQueries
	}

	if strings.HasPrefix(trimmed, "INSERT") || strings.HasPrefix(trimmed, "UPDATE") ||
		strings.HasPrefix(trimmed, "DELETE") || strings.HasPrefix(trimmed, "MERGE") {
		return !filterConfig.MirrorDdlOnly
	}

	if strings.HasPrefix(trimmed, "CREATE") || strings.HasPrefix(trimmed, "ALTER") ||
		strings.HasPrefix(trimmed, "DROP") || strings.HasPrefix(trimmed, "TRUNCATE") {
		return !filterConfig.MirrorDmlOnly
	}

	if strings.HasPrefix(trimmed, "CALL") || strings.HasPrefix(trimmed, "DO") {
		return !filterConfig.MirrorDdlOnly
	}

	if strings.HasPrefix(trimmed, "SET ") || strings.HasPrefix(trimmed, "RESET ") ||
		strings.HasPrefix(trimmed, "DISCARD ") {
		return true
	}

	if strings.HasPrefix(trimmed, "COPY") {
		if strings.Contains(trimmed, " TO ") {
			return false
		}
		return !filterConfig.MirrorDdlOnly
	}

	if strings.HasPrefix(trimmed, "GRANT") || strings.HasPrefix(trimmed, "REVOKE") ||
		strings.HasPrefix(trimmed, "COMMENT") {
		return !filterConfig.MirrorDmlOnly
	}

	return false
}

// doMirror sends a SQL statement to mirrors via the durable queue.
func (s *RawProxyServer) doMirror(ctx context.Context, sql string) {
	if s.metrics != nil {
		s.metrics.RecordMirrorQuery()
	}
	if s.mirrorQueue != nil {
		s.mirrorQueue.EnqueueSQL(sql)
	} else {
		go s.executeMirrorQuery(ctx, sql)
	}
}

// doMirrorTransaction sends a batch of SQL statements to mirrors via the durable queue.
func (s *RawProxyServer) doMirrorTransaction(ctx context.Context, statements []string) {
	if s.metrics != nil {
		s.metrics.RecordMirrorQuery()
	}
	if s.mirrorQueue != nil {
		s.mirrorQueue.EnqueueTransaction(statements)
	} else {
		go s.executeMirrorTransaction(ctx, statements)
	}
}

// processMirrorTask is the queue consumer callback that executes a task on mirrors.
func (s *RawProxyServer) processMirrorTask(task *mirrorTask) {
	ctx := context.Background()
	if task.Transaction != nil {
		s.executeMirrorTransaction(ctx, task.Transaction)
	} else if task.SQL != "" {
		s.executeMirrorQuery(ctx, task.SQL)
	}
}

// executeMirrorTransaction executes a sequence of SQL statements on a single
// connection per mirror pool, preserving transaction semantics.
func (s *RawProxyServer) executeMirrorTransaction(ctx context.Context, statements []string) {
	timeout := 30 * time.Second
	if s.performanceConfig != nil && s.performanceConfig.MirrorTimeoutSecs > 0 {
		timeout = time.Duration(s.performanceConfig.MirrorTimeoutSecs) * time.Second
	}

	mirrorCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	s.mirrorPoolsMu.RLock()
	pools := make([]*pgxpool.Pool, len(s.mirrorPools))
	copy(pools, s.mirrorPools)
	s.mirrorPoolsMu.RUnlock()

	var wg sync.WaitGroup
	for i, pool := range pools {
		if pool == nil {
			continue
		}
		if s.mirrorBreakers != nil && !s.mirrorBreakers.Allow(i) {
			continue
		}
		wg.Add(1)
		go func(idx int, p *pgxpool.Pool) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					s.logger.Error("Mirror transaction panic", zap.Int("mirror", idx), zap.Any("panic", r))
				}
			}()

			conn, err := p.Acquire(mirrorCtx)
			if err != nil {
				s.logger.Debug("Mirror acquire failed", zap.Int("mirror", idx), zap.Error(err))
				if s.mirrorBreakers != nil {
					s.mirrorBreakers.RecordFailure(idx)
				}
				return
			}
			released := false
			defer func() {
				if !released {
					conn.Release()
				}
			}()

			// Execute all statements sequentially on the same connection
			for _, sql := range statements {
				_, err = conn.Exec(mirrorCtx, sql)
				if err != nil {
					s.logger.Debug("Mirror transaction exec failed",
						zap.Int("mirror", idx),
						zap.String("sql", truncateQuery(sql, 100)),
						zap.Error(err))
					if s.mirrorBreakers != nil {
						s.mirrorBreakers.RecordFailure(idx)
					}
					if s.metrics != nil {
						s.metrics.RecordMirrorFailure()
					}
					// Check if this is a transient error worth retrying
					errStr := err.Error()
					isTransient := strings.Contains(errStr, "connection reset") ||
						strings.Contains(errStr, "broken pipe") ||
						strings.Contains(errStr, "timeout") ||
						strings.Contains(errStr, "connection refused")

					if isTransient {
						// Release dead connection and acquire a fresh one for retry
						conn.Release()
						released = true
						s.logger.Debug("Retrying mirror transaction on fresh connection",
							zap.Int("mirror", idx))
						conn2, err2 := p.Acquire(mirrorCtx)
						if err2 != nil {
							s.logger.Debug("Mirror retry acquire failed", zap.Int("mirror", idx), zap.Error(err2))
							return
						}
						defer conn2.Release()
						retryOk := true
						for _, retrySql := range statements {
							if _, retryErr := conn2.Exec(mirrorCtx, retrySql); retryErr != nil {
								conn2.Exec(mirrorCtx, "ROLLBACK")
								retryOk = false
								break
							}
						}
						if retryOk {
							if s.mirrorBreakers != nil {
								s.mirrorBreakers.RecordSuccess(idx)
							}
						}
					} else {
						// Permanent error (constraint violation, syntax, etc.) — rollback
						conn.Exec(mirrorCtx, "ROLLBACK")
					}
					return
				}
			}
			// All statements succeeded
			if s.mirrorBreakers != nil {
				s.mirrorBreakers.RecordSuccess(idx)
			}
		}(i, pool)
	}
	wg.Wait()
}

// cleanupTxBuffer removes the transaction buffer for a connection (call on connection close).
func (s *RawProxyServer) cleanupTxBuffer(clientConn net.Conn) {
	s.txBuffers.Delete(clientConn)
}

// executeMirrorQuery sends a query to all mirror pools asynchronously.
func (s *RawProxyServer) executeMirrorQuery(ctx context.Context, sql string) {
	timeout := 30 * time.Second
	if s.performanceConfig != nil && s.performanceConfig.MirrorTimeoutSecs > 0 {
		timeout = time.Duration(s.performanceConfig.MirrorTimeoutSecs) * time.Second
	}

	mirrorCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	s.mirrorPoolsMu.RLock()
	pools := make([]*pgxpool.Pool, len(s.mirrorPools))
	copy(pools, s.mirrorPools)
	s.mirrorPoolsMu.RUnlock()

	var wg sync.WaitGroup
	for i, pool := range pools {
		if pool == nil {
			continue
		}
		// Circuit breaker check
		if s.mirrorBreakers != nil && !s.mirrorBreakers.Allow(i) {
			continue
		}
		wg.Add(1)
		go func(idx int, p *pgxpool.Pool) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					s.logger.Error("Mirror query panic", zap.Int("mirror", idx), zap.Any("panic", r))
				}
			}()

			conn, err := p.Acquire(mirrorCtx)
			if err != nil {
				s.logger.Debug("Mirror acquire failed", zap.Int("mirror", idx), zap.Error(err))
				if s.mirrorBreakers != nil {
					s.mirrorBreakers.RecordFailure(idx)
				}
				return
			}
			defer conn.Release()

			_, err = conn.Exec(mirrorCtx, sql)
			if err != nil {
				s.logger.Debug("Mirror exec failed",
					zap.Int("mirror", idx),
					zap.String("sql", truncateQuery(sql, 100)),
					zap.Error(err))
				if s.mirrorBreakers != nil {
					s.mirrorBreakers.RecordFailure(idx)
				}
				if s.metrics != nil {
					s.metrics.RecordMirrorFailure()
				}
			} else {
				if s.mirrorBreakers != nil {
					s.mirrorBreakers.RecordSuccess(idx)
				}
			}
		}(i, pool)
	}
	wg.Wait()
}

// --- Protocol helpers ---

// readRawMessage reads a single PostgreSQL protocol message (type + length + data).
// Returns msgType, payload (excluding the 4-byte length), and error.
// Uses the provided buffer to avoid allocations for small messages.
// IMPORTANT: The returned data slice may reference the shared buffer. The caller
// must finish using it before the next call to readRawMessage with the same buffer.
func readRawMessage(conn net.Conn, buf []byte) (byte, []byte, error) {
	// Read type (1 byte) + length (4 bytes)
	header := buf[:5]
	if _, err := io.ReadFull(conn, header); err != nil {
		return 0, nil, err
	}

	msgType := header[0]
	msgLen := int(binary.BigEndian.Uint32(header[1:5])) // includes self (4 bytes)

	if msgLen < 4 {
		return 0, nil, fmt.Errorf("invalid message length: %d", msgLen)
	}
	if msgLen > 256<<20 { // 256MB sanity limit
		return 0, nil, fmt.Errorf("message too large: %d bytes", msgLen)
	}

	dataLen := msgLen - 4
	var data []byte
	if dataLen <= len(buf)-5 {
		// Fits in reusable buffer
		data = buf[5 : 5+dataLen]
	} else {
		// Need to allocate for large messages
		data = make([]byte, dataLen)
	}

	if dataLen > 0 {
		if _, err := io.ReadFull(conn, data); err != nil {
			return 0, nil, err
		}
	}

	return msgType, data, nil
}

// writeRawMessage writes a complete PostgreSQL protocol message.
func writeRawMessage(conn net.Conn, msgType byte, data []byte) error {
	msgLen := int32(len(data) + 4)
	header := [5]byte{msgType}
	binary.BigEndian.PutUint32(header[1:], uint32(msgLen))

	// Use writev-style write: header + data in one syscall if possible
	if w, ok := conn.(*net.TCPConn); ok {
		buffers := net.Buffers{header[:], data}
		_, err := buffers.WriteTo(w)
		return err
	}

	// For non-TCP connections (e.g. TLS): merge into single write to avoid
	// partial message on error and reduce syscall overhead
	combined := make([]byte, 5+len(data))
	copy(combined, header[:])
	copy(combined[5:], data)
	_, err := conn.Write(combined)
	return err
}

// extractSQL extracts the SQL string from a SimpleQuery ('Q') message payload.
// The payload is a null-terminated SQL string.
func extractSQL(payload []byte) string {
	if len(payload) == 0 {
		return ""
	}
	// Remove trailing null byte
	if payload[len(payload)-1] == 0 {
		return string(payload[:len(payload)-1])
	}
	return string(payload)
}

// extractHostPort extracts host:port from a PostgreSQL connection URL.
func extractHostPort(connStr string) string {
	// Handle postgresql:// URL format
	if strings.Contains(connStr, "://") {
		// Remove scheme
		after := strings.SplitN(connStr, "://", 2)[1]
		// Remove user:pass@
		if idx := strings.Index(after, "@"); idx >= 0 {
			after = after[idx+1:]
		}
		// Remove /database and ?params
		if idx := strings.IndexAny(after, "/?"); idx >= 0 {
			after = after[:idx]
		}
		// Ensure port
		if !strings.Contains(after, ":") {
			after += ":5432"
		}
		return after
	}
	// Fallback: assume it's already host:port
	return connStr
}

// extractParseSQL extracts the statement name and SQL from a Parse ('P') message.
// Parse message format: name(string\0) + query(string\0) + numParams(int16) + paramOIDs...
func extractParseSQL(payload []byte) (stmtName string, sql string) {
	if len(payload) == 0 {
		return "", ""
	}

	// Find first null byte (end of statement name)
	nameEnd := bytes.IndexByte(payload, 0)
	if nameEnd < 0 {
		return "", ""
	}
	stmtName = string(payload[:nameEnd])

	// SQL starts after the null byte
	rest := payload[nameEnd+1:]
	sqlEnd := bytes.IndexByte(rest, 0)
	if sqlEnd < 0 {
		// No null terminator, take all remaining
		sql = string(rest)
	} else {
		sql = string(rest[:sqlEnd])
	}

	return stmtName, sql
}

// extractBindNames extracts portal name and source statement name from a Bind ('B') message.
// Bind message format: portal(string\0) + statement(string\0) + ...
func extractBindNames(payload []byte) (portal string, stmtName string) {
	if len(payload) == 0 {
		return "", ""
	}

	// Find portal name
	portalEnd := bytes.IndexByte(payload, 0)
	if portalEnd < 0 {
		return "", ""
	}
	portal = string(payload[:portalEnd])

	// Find statement name
	rest := payload[portalEnd+1:]
	stmtEnd := bytes.IndexByte(rest, 0)
	if stmtEnd < 0 {
		return portal, ""
	}
	stmtName = string(rest[:stmtEnd])

	return portal, stmtName
}

// resolveBindParameters extracts text-format parameter values from a Bind message
// and substitutes them into the SQL template (replacing $1, $2, etc.).
// Returns the resolved SQL and true on success, or ("", false) if parameters are
// in binary format or the message is malformed.
//
// Bind message layout after portal\0 + statement\0:
//   numFormatCodes(int16) + formatCodes(int16 × N) +
//   numParams(int16) + for each param: len(int32) + value(bytes) +
//   numResultFormats(int16) + resultFormats(int16 × N)
func resolveBindParameters(sql string, payload []byte) (string, bool) {
	if len(payload) == 0 {
		return "", false
	}

	// Skip portal name
	portalEnd := bytes.IndexByte(payload, 0)
	if portalEnd < 0 {
		return "", false
	}
	pos := portalEnd + 1

	// Skip statement name
	stmtEnd := bytes.IndexByte(payload[pos:], 0)
	if stmtEnd < 0 {
		return "", false
	}
	pos += stmtEnd + 1

	// Read number of parameter format codes
	if pos+2 > len(payload) {
		return "", false
	}
	numFormatCodes := int(binary.BigEndian.Uint16(payload[pos:]))
	pos += 2

	// Read format codes (0=text, 1=binary)
	formatCodes := make([]int16, numFormatCodes)
	for i := 0; i < numFormatCodes; i++ {
		if pos+2 > len(payload) {
			return "", false
		}
		formatCodes[i] = int16(binary.BigEndian.Uint16(payload[pos:]))
		pos += 2
	}

	// Read number of parameters
	if pos+2 > len(payload) {
		return "", false
	}
	numParams := int(binary.BigEndian.Uint16(payload[pos:]))
	pos += 2

	if numParams == 0 {
		// No parameters — shouldn't have $N in SQL but return as-is
		return sql, true
	}

	// Extract parameter values
	params := make([]string, numParams)
	for i := 0; i < numParams; i++ {
		if pos+4 > len(payload) {
			return "", false
		}
		paramLen := int32(binary.BigEndian.Uint32(payload[pos:]))
		pos += 4

		if paramLen == -1 {
			// NULL parameter
			params[i] = "NULL"
			continue
		}

		if pos+int(paramLen) > len(payload) {
			return "", false
		}

		// Determine format for this parameter
		var isBinary bool
		if numFormatCodes == 0 {
			isBinary = false // default is text
		} else if numFormatCodes == 1 {
			isBinary = formatCodes[0] == 1 // single code applies to all
		} else if i < numFormatCodes {
			isBinary = formatCodes[i] == 1
		}

		if isBinary {
			// Binary format — we cannot safely convert without type OID info.
			// Skip mirroring this query.
			return "", false
		}

		// Text format — value is the literal text representation
		params[i] = string(payload[pos : pos+int(paramLen)])
		pos += int(paramLen)
	}

	// Substitute parameters into SQL: replace $N with safely quoted value
	// Use PostgreSQL dollar-quoting to prevent SQL injection. Dollar-quoting
	// ($$value$$) treats content literally without any escape interpretation,
	// immune to backslash attacks and standard_conforming_strings settings.
	// For numeric values, we pass them unquoted (safe since they contain only digits/dots/signs).
	resolved := sql
	for i := numParams; i >= 1; i-- {
		placeholder := fmt.Sprintf("$%d", i)
		var replacement string
		if params[i-1] == "NULL" {
			replacement = "NULL"
		} else if isNumericLiteral(params[i-1]) {
			// Pure numeric value — safe to inline without quoting
			replacement = params[i-1]
		} else {
			// Find a dollar-quote tag that doesn't appear in the value
			tag := findSafeDollarTag(params[i-1])
			replacement = tag + params[i-1] + tag
		}
		resolved = strings.ReplaceAll(resolved, placeholder, replacement)
	}

	return resolved, true
}

// isNumericLiteral checks if a string is a safe numeric literal (integer, decimal, or scientific notation).
// Only characters allowed: digits, one optional leading minus/plus, one optional dot, optional 'e'/'E'.
func isNumericLiteral(s string) bool {
	if len(s) == 0 {
		return false
	}
	hasDigit := false
	hasDot := false
	hasE := false
	for i, c := range s {
		switch {
		case c >= '0' && c <= '9':
			hasDigit = true
		case c == '-' || c == '+':
			// Only at start or right after 'e'/'E'
			if i != 0 && (i < 2 || (s[i-1] != 'e' && s[i-1] != 'E')) {
				return false
			}
		case c == '.':
			if hasDot || hasE {
				return false
			}
			hasDot = true
		case c == 'e' || c == 'E':
			if hasE || !hasDigit {
				return false
			}
			hasE = true
		default:
			return false
		}
	}
	return hasDigit
}

// findSafeDollarTag finds a $$-based quote tag that doesn't appear in the value.
// This guarantees no injection is possible regardless of the value content.
func findSafeDollarTag(value string) string {
	tag := "$$"
	if !strings.Contains(value, tag) {
		return tag
	}
	// Try $fz$, $fz1$, $fz2$, ...
	for i := 0; i < 100; i++ {
		if i == 0 {
			tag = "$fz$"
		} else {
			tag = fmt.Sprintf("$fz%d$", i)
		}
		if !strings.Contains(value, tag) {
			return tag
		}
	}
	// Extremely unlikely fallback — use a random-ish tag
	tag = fmt.Sprintf("$fz%d$", time.Now().UnixNano()%99999)
	return tag
}
