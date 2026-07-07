package server

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
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
	mirrorPools []*pgxpool.Pool

	// Circuit breaker for mirror health
	mirrorBreakers *MirrorCircuitBreakers

	// Metrics
	metrics *Metrics

	// TLS config for client-facing connections
	clientTLSConfig *tls.Config

	// Stats
	activeConns atomic.Int64
	totalConns  atomic.Int64

	// Shutdown
	listener net.Listener
	cancel   context.CancelFunc
}

func NewRawProxyServer(logger *zap.Logger) *RawProxyServer {
	s := &RawProxyServer{
		logger:  logger,
		metrics: NewMetrics("frenzy"),
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

func (s *RawProxyServer) SetQueryFilterConfig(config *QueryFilterConfig) {
	s.filterConfig.Store(config)
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
		zap.Int("mirrors", len(mirrorAddrs)))

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
				return nil
			default:
				s.logger.Error("Accept error", zap.Error(err))
				continue
			}
		}

		s.activeConns.Add(1)
		s.totalConns.Add(1)
		if s.metrics != nil {
			s.metrics.ActiveConnections.Set(float64(s.activeConns.Load()))
		}
		go s.handleConnection(ctx, conn)
	}
}

func (s *RawProxyServer) Close(ctx context.Context) error {
	if s.cancel != nil {
		s.cancel()
	}
	if s.listener != nil {
		s.listener.Close()
	}
	for _, pool := range s.mirrorPools {
		if pool != nil {
			pool.Close()
		}
	}
	s.logger.Info("Raw proxy server shut down")
	return nil
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
				// Continue with TLS connection — re-read startup
				return s.handleStartup(tlsConn, backendConn)
			}
			// Tell client we don't support SSL (send 'N')
			if _, err := clientConn.Write([]byte{'N'}); err != nil {
				return nil, fmt.Errorf("write SSL reject: %w", err)
			}
			// Re-read the actual startup message
			return s.handleStartup(clientConn, backendConn)
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

// relayClientAuth relays authentication messages from client to backend until backend responds.
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
				if authType == 0 { // AuthenticationOk
					return nil
				}
				// Continue relaying (e.g., SASL multi-step)
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
	var pendingMirrorSQL []string // SQLs to mirror after successful Sync response
	var pendingResponses int      // count of messages expecting a response (for Flush handling)

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Read one message from client
		msgType, msgData, err := readRawMessage(clientConn, clientBuf)
		if err != nil {
			if err != io.EOF {
				s.logger.Debug("Client read error", zap.Error(err))
			}
			return
		}

		// Handle terminate
		if msgType == 'X' {
			writeRawMessage(backendConn, msgType, msgData)
			return
		}

		// Mirror decision based on message type
		if len(s.mirrorPools) > 0 {
			switch msgType {
			case 'Q': // SimpleQuery — extract SQL directly
				sql := extractSQL(msgData)
				s.maybeMirror(ctx, sql, inTransaction)

			case 'P': // Parse — extract and store SQL by statement name
				stmtName, sql := extractParseSQL(msgData)
				if sql != "" {
					namedStmts[stmtName] = sql
				}

			case 'B': // Bind — map portal to the SQL from its source statement
				_, stmtName := extractBindNames(msgData)
				if sql, ok := namedStmts[stmtName]; ok {
					pendingMirrorSQL = append(pendingMirrorSQL, sql)
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

			// After Sync: mirror pending Extended Query SQLs
			if msgType == 'S' && len(pendingMirrorSQL) > 0 {
				// Only mirror if not in error state
				if newTxState != 'E' || !s.getFilterConfig().SkipFailedTxMirror {
					for _, sql := range pendingMirrorSQL {
						s.maybeMirror(ctx, sql, inTransaction)
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
		// 'D' (DataRow), 't' (ParameterDescription) — intermediate, don't count
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

// maybeMirror decides whether to mirror a SQL query and sends it asynchronously.
func (s *RawProxyServer) maybeMirror(ctx context.Context, sql string, inTransaction bool) {
	if sql == "" {
		return
	}

	filterConfig := s.getFilterConfig()

	// Use existing filter logic
	trimmed := strings.TrimSpace(strings.ToUpper(sql))

	shouldMirror := false

	if filterConfig.MirrorAllQueries {
		shouldMirror = true
	} else if strings.HasPrefix(trimmed, "BEGIN") || strings.HasPrefix(trimmed, "COMMIT") ||
		strings.HasPrefix(trimmed, "END") || strings.HasPrefix(trimmed, "ROLLBACK") ||
		strings.HasPrefix(trimmed, "SAVEPOINT") || strings.HasPrefix(trimmed, "RELEASE") {
		// Transaction control — always mirror (unless skip rollback)
		if strings.HasPrefix(trimmed, "ROLLBACK") && filterConfig.SkipRollbackMirror {
			shouldMirror = false
		} else {
			shouldMirror = true
		}
	} else if strings.HasPrefix(trimmed, "SELECT") || strings.HasPrefix(trimmed, "WITH") {
		// SELECT — check setval/nextval
		if strings.HasPrefix(trimmed, "SELECT SETVAL") || strings.HasPrefix(trimmed, "SELECT NEXTVAL") {
			shouldMirror = true
		} else {
			shouldMirror = filterConfig.MirrorSelectQueries
		}
	} else if strings.HasPrefix(trimmed, "INSERT") || strings.HasPrefix(trimmed, "UPDATE") ||
		strings.HasPrefix(trimmed, "DELETE") || strings.HasPrefix(trimmed, "MERGE") {
		shouldMirror = !filterConfig.MirrorDdlOnly
	} else if strings.HasPrefix(trimmed, "CREATE") || strings.HasPrefix(trimmed, "ALTER") ||
		strings.HasPrefix(trimmed, "DROP") || strings.HasPrefix(trimmed, "TRUNCATE") {
		shouldMirror = !filterConfig.MirrorDmlOnly
	} else if strings.HasPrefix(trimmed, "CALL") || strings.HasPrefix(trimmed, "DO") {
		shouldMirror = !filterConfig.MirrorDdlOnly
	} else if strings.HasPrefix(trimmed, "SET ") || strings.HasPrefix(trimmed, "RESET ") ||
		strings.HasPrefix(trimmed, "DISCARD ") {
		shouldMirror = true
	} else if strings.HasPrefix(trimmed, "COPY") {
		if strings.Contains(trimmed, " TO ") {
			shouldMirror = false
		} else {
			shouldMirror = !filterConfig.MirrorDdlOnly
		}
	} else if strings.HasPrefix(trimmed, "GRANT") || strings.HasPrefix(trimmed, "REVOKE") ||
		strings.HasPrefix(trimmed, "COMMENT") {
		shouldMirror = !filterConfig.MirrorDmlOnly
	}

	if shouldMirror {
		if s.metrics != nil {
			s.metrics.RecordMirrorQuery()
		}
		go s.executeMirrorQuery(ctx, sql)
	}
}

// executeMirrorQuery sends a query to all mirror pools asynchronously.
func (s *RawProxyServer) executeMirrorQuery(ctx context.Context, sql string) {
	timeout := 30 * time.Second
	if s.performanceConfig != nil && s.performanceConfig.MirrorTimeoutSecs > 0 {
		timeout = time.Duration(s.performanceConfig.MirrorTimeoutSecs) * time.Second
	}

	mirrorCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var wg sync.WaitGroup
	for i, pool := range s.mirrorPools {
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
	if msgLen > 1<<30 { // 1GB sanity limit
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

	// Fallback for non-TCP connections (e.g. TLS)
	if _, err := conn.Write(header[:]); err != nil {
		return err
	}
	if len(data) > 0 {
		if _, err := conn.Write(data); err != nil {
			return err
		}
	}
	return nil
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
