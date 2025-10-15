package server

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	wire "github.com/jeroenrinzema/psql-wire"
	"github.com/lib/pq/oid"
	"go.uber.org/zap"
)

// QueryFilterConfig defines query filtering options
type QueryFilterConfig struct {
	MirrorAllQueries    bool
	MirrorSelectQueries bool
	MirrorDdlOnly       bool
	MirrorDmlOnly       bool
	SkipRollbackMirror  bool
	SkipFailedTxMirror  bool
	SkipMirrorTxLocks   bool  // Skip BEGIN/COMMIT on mirrors to avoid transaction locks
}

type TransactionBuffer struct {
	SessionID    string
	Queries      []string
	Parameters   [][]wire.Parameter
	CreatedAt    time.Time
	LastActivity time.Time
}

type ProxyServerV15 struct {
	logger           *zap.Logger
	primary          *Connection
	mirrors          []*Connection
	poolConfig       *PoolConfig
	performanceConfig *PerformanceConfig
	queryFilterConfig *QueryFilterConfig
	sessionManager   *SessionManager
	
	// Transaction-aware connections
	txAwarePrimary  *TransactionAwareConnection
	txAwareMirrors  []*TransactionAwareConnection
	
	// Transaction buffering for commit-time mirroring
	txBuffers        map[string]*TransactionBuffer
	txBufferMutex    sync.RWMutex
	cleanupTicker    *time.Ticker
	stopCleanup      chan struct{}
}

func NewProxyServerV15(logger *zap.Logger) *ProxyServerV15 {
	server := &ProxyServerV15{
		logger:           logger,
		txBuffers:        make(map[string]*TransactionBuffer),
		poolConfig:       DefaultPoolConfig(),
		performanceConfig: DefaultPerformanceConfig(),
		queryFilterConfig: &QueryFilterConfig{
			SkipRollbackMirror: false, // Enable ROLLBACK mirroring
			SkipFailedTxMirror: true,  // Default: skip failed transaction mirroring
			SkipMirrorTxLocks:  false, // Default: use transactions on mirrors
		},
		sessionManager:   NewSessionManager(logger),
		stopCleanup:      make(chan struct{}),
	}
	
	// Start cleanup routine
	server.startCleanup()
	
	return server
}

// SetQueryFilterConfig sets the query filtering configuration
func (server *ProxyServerV15) SetQueryFilterConfig(config *QueryFilterConfig) {
	server.queryFilterConfig = config
}

// SetPoolConfig sets the connection pool configuration
func (server *ProxyServerV15) SetPoolConfig(config *PoolConfig) {
	server.poolConfig = config
}

// SetPerformanceConfig sets the performance configuration
func (server *ProxyServerV15) SetPerformanceConfig(config *PerformanceConfig) {
	server.performanceConfig = config
}

func (server *ProxyServerV15) ListenAndServe(
	ctx context.Context,
	listenAddress string,
	primaryAddress string,
	mirrorAddresses []string,
	tlsConfig *tls.Config) error {

	server.logger.Info("Starting proxy server v15", zap.String("listen", listenAddress))

	// Connect to primary
	err := server.connectToPrimary(ctx, primaryAddress)
	if err != nil {
		return fmt.Errorf("failed to connect to primary: %w", err)
	}

	// Connect to mirrors
	err = server.connectToMirrors(ctx, mirrorAddresses)
	if err != nil {
		return fmt.Errorf("failed to connect to mirrors: %w", err)
	}

	// Start server with TLS support if configured
	if tlsConfig != nil {
		return server.listenAndServeWithTLS(listenAddress, tlsConfig)
	}

	// Start server using v0.15.0 API without TLS
	return wire.ListenAndServe(listenAddress, server.parseQuery)
}

// parseQuery implements ParseFn for psql-wire v0.15.0
func (server *ProxyServerV15) parseQuery(ctx context.Context, query string) (wire.PreparedStatements, error) {
	server.logger.Debug("Parsing query", zap.String("query", query))
	
	// Split multi-command queries by semicolon
	commands := server.splitCommands(query)
	
	var statements []*wire.PreparedStatement
	for _, cmd := range commands {
		cmd = strings.TrimSpace(cmd)
		if cmd == "" {
			continue
		}
		
		// Get columns for this command
		columns, err := server.getQueryColumns(ctx, cmd)
		if err != nil {
			server.logger.Debug("Could not get columns, using empty columns", zap.Error(err))
			columns = wire.Columns{}
		}
		
		// Create statement for this command
		stmt := wire.NewStatement(func(ctx context.Context, writer wire.DataWriter, parameters []wire.Parameter) error {
			return server.executeQuery(ctx, cmd, writer, parameters)
		}, wire.WithColumns(columns))
		
		statements = append(statements, stmt)
	}
	
	return wire.PreparedStatements(statements), nil
}

// splitCommands splits a query string by semicolons, handling quoted strings
func (server *ProxyServerV15) splitCommands(query string) []string {
	var commands []string
	var current strings.Builder
	inQuotes := false
	var quoteChar rune
	
	for _, r := range query {
		switch r {
		case '\'', '"':
			if !inQuotes {
				inQuotes = true
				quoteChar = r
			} else if r == quoteChar {
				inQuotes = false
			}
			current.WriteRune(r)
		case ';':
			if !inQuotes {
				cmd := strings.TrimSpace(current.String())
				if cmd != "" {
					commands = append(commands, cmd)
				}
				current.Reset()
			} else {
				current.WriteRune(r)
			}
		default:
			current.WriteRune(r)
		}
	}
	
	// Add the last command if any
	cmd := strings.TrimSpace(current.String())
	if cmd != "" {
		commands = append(commands, cmd)
	}
	
	return commands
}

// getQueryColumns executes a query to get column information without returning data
func (server *ProxyServerV15) getQueryColumns(ctx context.Context, query string) (wire.Columns, error) {
	if server.primary == nil || server.primary.pool == nil {
		return wire.Columns{}, fmt.Errorf("primary connection not available")
	}

	// Skip column detection for non-SELECT queries
	trimmedQuery := strings.TrimSpace(strings.ToUpper(query))
	if !strings.HasPrefix(trimmedQuery, "SELECT") && 
	   !strings.HasPrefix(trimmedQuery, "WITH") && 
	   !strings.HasPrefix(trimmedQuery, "SHOW") &&
	   !strings.HasPrefix(trimmedQuery, "EXPLAIN") {
		return wire.Columns{}, nil
	}

	conn, err := server.primary.pool.Acquire(ctx)
	if err != nil {
		return wire.Columns{}, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	// Execute query with LIMIT 0 to get column information only
	limitQuery := query + " LIMIT 0"
	rows, err := conn.Query(ctx, limitQuery)
	if err != nil {
		// If LIMIT 0 fails, try the original query approach
		rows, err = conn.Query(ctx, query)
		if err != nil {
			return wire.Columns{}, fmt.Errorf("failed to execute query for columns: %w", err)
		}
	}
	defer rows.Close()

	// Get column information
	columns := wire.Columns{}
	for _, field := range rows.FieldDescriptions() {
		dataTypeOID := oid.Oid(field.DataTypeOID)
		column := wire.Column{
			Table: 0,
			Name:  field.Name,
			Oid:   dataTypeOID,
		}
		columns = append(columns, column)
	}

	server.logger.Debug("Detected columns", zap.Int("column_count", len(columns)))
	return columns, nil
}

// executeQuery executes the query on primary and mirrors with transaction state management
func (server *ProxyServerV15) executeQuery(ctx context.Context, query string, writer wire.DataWriter, parameters []wire.Parameter) error {
	server.logger.Debug("Executing query", zap.String("query", query))

	// Check if this is a transaction command
	_, cmdType := IsTransactionCommand(query)
	
	// Execute on primary using transaction-aware method
	err := server.primary.ExecuteQueryV15WithTransactionSupport(ctx, query, writer, parameters)
	if err != nil {
		server.logger.Error("Primary query failed", zap.Error(err))
		return err
	}

	// Mirror transaction commands with filtering support
	shouldMirrorTx := false
	if cmdType == "BEGIN" || cmdType == "COMMIT" {
		shouldMirrorTx = true
		server.logger.Debug("Transaction command mirroring", 
			zap.String("command", cmdType), 
			zap.Bool("will_mirror", true))
	} else if cmdType == "ROLLBACK" && !server.queryFilterConfig.SkipRollbackMirror {
		shouldMirrorTx = true
		server.logger.Debug("ROLLBACK command mirroring", 
			zap.String("command", cmdType), 
			zap.Bool("will_mirror", true),
			zap.Bool("skip_rollback_mirror", server.queryFilterConfig.SkipRollbackMirror))
	} else if cmdType == "ROLLBACK" {
		server.logger.Info("ROLLBACK command skipped", 
			zap.String("command", cmdType), 
			zap.Bool("will_mirror", false),
			zap.Bool("skip_rollback_mirror", server.queryFilterConfig.SkipRollbackMirror))
	}
	
	if server.shouldMirrorQuery(query) || shouldMirrorTx {
		server.logger.Debug("Query marked for mirroring", 
			zap.String("query", query),
			zap.Bool("shouldMirror", server.shouldMirrorQuery(query)),
			zap.Bool("shouldMirrorTx", shouldMirrorTx))
		
		sessionID := getSessionIDFromContext(ctx)
		
		// Check if this is a transaction command
		_, cmdType := IsTransactionCommand(query)
		server.logger.Debug("Transaction command check", 
			zap.String("query", query),
			zap.String("cmdType", cmdType))
		
		if cmdType == "BEGIN" {
			// Start buffering transaction
			server.startTransactionBuffer(sessionID)
		} else if cmdType == "COMMIT" || cmdType == "END" {
			// Mirror entire buffered transaction
			server.commitTransactionBuffer(sessionID)
		} else if cmdType == "ROLLBACK" {
			// Discard buffered transaction
			server.rollbackTransactionBuffer(sessionID)
		} else if server.isSessionInTransaction(sessionID) {
			// Buffer query in transaction
			server.bufferTransactionQuery(sessionID, query, parameters)
		} else {
			// Non-transaction query - mirror immediately
			server.logger.Debug("Starting mirror execution", zap.String("query", query))
			go server.executeMirrorQueriesWithSession(context.Background(), sessionID, query, parameters)
		}
	} else {
		server.logger.Debug("Query not mirrored", 
			zap.String("query", query), 
			zap.String("reason", "not eligible"),
			zap.Bool("should_mirror", server.shouldMirrorQuery(query)))
	}

	return nil
}

// executeMirrorQueriesWithTransactionSupport executes query on all mirrors with transaction support
func (server *ProxyServerV15) executeMirrorQueriesWithTransactionSupport(ctx context.Context, query string, parameters []wire.Parameter) {
	cmdType := strings.ToUpper(strings.TrimSpace(strings.Fields(query)[0]))
	server.logger.Debug("Executing on mirrors", 
		zap.String("query", query),
		zap.Int("mirror_count", len(server.mirrors)))
	
	for i, mirror := range server.mirrors {
		go func(mirrorIndex int, m *Connection) {
			if cmdType == "ROLLBACK" {
				server.logger.Info("Executing ROLLBACK on mirror", 
					zap.Int("mirror_index", mirrorIndex),
					zap.String("query", query))
			}
			
			err := m.ExecuteQueryV15WithTransactionSupport(ctx, query, nil, parameters)
			if err != nil {
				server.logger.Error("Mirror query failed", 
					zap.Int("mirror_index", mirrorIndex),
					zap.String("query", query),
					zap.Error(err))
			} else if cmdType == "ROLLBACK" {
				server.logger.Info("ROLLBACK completed on mirror", 
					zap.Int("mirror_index", mirrorIndex))
			}
		}(i, mirror)
	}
}

func (server *ProxyServerV15) startTransactionBuffer(sessionID string) {
	server.txBufferMutex.Lock()
	defer server.txBufferMutex.Unlock()
	
	now := time.Now()
	server.logger.Debug("Starting transaction buffer", zap.String("session", sessionID))
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
	
	if buffer, exists := server.txBuffers[sessionID]; exists {
		// More aggressive size limits to prevent memory leaks
		maxQueries := 100 // Reduced from 300
		if len(server.txBuffers) > 100 { // Memory pressure
			maxQueries = 50
		}
		
		if len(buffer.Queries) >= maxQueries {
			server.logger.Error("Transaction buffer overflow, forcing cleanup", 
				zap.String("session", sessionID),
				zap.Int("query_count", len(buffer.Queries)),
				zap.Int("max_queries", maxQueries),
				zap.Int("total_buffers", len(server.txBuffers)))
			// Force cleanup to prevent memory leak
			buffer.Queries = nil
			buffer.Parameters = nil
			delete(server.txBuffers, sessionID)
			return
		}
		
		buffer.Queries = append(buffer.Queries, query)
		buffer.Parameters = append(buffer.Parameters, parameters)
		buffer.LastActivity = time.Now()
	}
}

func (server *ProxyServerV15) commitTransactionBuffer(sessionID string) {
	server.txBufferMutex.Lock()
	buffer, exists := server.txBuffers[sessionID]
	if exists {
		delete(server.txBuffers, sessionID)
	}
	server.txBufferMutex.Unlock()
	
	server.logger.Debug("Committing transaction buffer", 
		zap.String("session", sessionID),
		zap.Bool("exists", exists),
		zap.Int("query_count", func() int {
			if buffer != nil {
				return len(buffer.Queries)
			}
			return 0
		}()))
	
	if exists && buffer != nil {
		// Mirror entire transaction with timeout context
		go func() {
			// Add timeout context to prevent goroutine leaks
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			
			// Clear buffer data after use to help GC
			defer func() {
				if r := recover(); r != nil {
					server.logger.Error("Transaction buffer commit panic", 
						zap.String("session", sessionID),
						zap.Any("panic", r))
				}
				buffer.Queries = nil
				buffer.Parameters = nil
			}()
			
			if server.queryFilterConfig.SkipMirrorTxLocks {
				// Execute queries without transaction wrapper but with connection reuse
				for _, mirror := range server.txAwareMirrors {
					conn, err := mirror.pool.Acquire(ctx)
					if err != nil {
						server.logger.Error("Failed to acquire mirror connection", zap.Error(err))
						continue
					}
					
					for _, query := range buffer.Queries {
						_, err = conn.Exec(ctx, query)
						if err != nil {
							server.logger.Error("Mirror query failed", zap.String("query", query), zap.Error(err))
						}
					}
					
					conn.Release()
				}
			} else {
				// Use a temporary connection for the entire transaction
				for _, mirror := range server.txAwareMirrors {
					conn, err := mirror.pool.Acquire(context.Background())
					if err != nil {
						server.logger.Error("Failed to acquire mirror connection", zap.Error(err))
						continue
					}
					
					// Execute entire transaction on this connection
					_, err = conn.Exec(context.Background(), "BEGIN")
					if err != nil {
						conn.Release()
						continue
					}
					
					for _, query := range buffer.Queries {
						_, err = conn.Exec(context.Background(), query)
						if err != nil {
							server.logger.Error("Mirror query failed", zap.String("query", query), zap.Error(err))
							conn.Exec(context.Background(), "ROLLBACK")
							break
						}
					}
					
					if err == nil {
						_, err = conn.Exec(context.Background(), "COMMIT")
					}
					
					conn.Release()
				}
			}
		}()
	}
}

func (server *ProxyServerV15) rollbackTransactionBuffer(sessionID string) {
	server.txBufferMutex.Lock()
	defer server.txBufferMutex.Unlock()
	
	if buffer, exists := server.txBuffers[sessionID]; exists {
		// Clear buffer data to help GC
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

// executeMirrorQueriesWithSession executes query on all mirrors with session awareness
func (server *ProxyServerV15) executeMirrorQueriesWithSession(ctx context.Context, sessionID string, query string, parameters []wire.Parameter) {
	server.logger.Debug("Mirror execution starting", zap.String("query", query), zap.String("session", sessionID))
	
	// Create independent timeout context for mirror operations
	mirrorCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	
	// Use transaction-aware mirrors if available
	if len(server.txAwareMirrors) > 0 {
		for i, mirror := range server.txAwareMirrors {
			go func(mirrorIndex int, tm *TransactionAwareConnection) {
				defer func() {
					if r := recover(); r != nil {
						server.logger.Error("Mirror goroutine panic", 
							zap.Int("mirror_index", mirrorIndex),
							zap.String("session", sessionID),
							zap.Any("panic", r))
					}
				}()
				
				err := tm.ExecuteQueryWithSession(mirrorCtx, sessionID, query, nil, parameters)
				if err != nil {
					server.logger.Error("Mirror query failed", 
						zap.Int("mirror_index", mirrorIndex),
						zap.String("session", sessionID),
						zap.String("query", query),
						zap.Error(err))
				} else {
					server.logger.Debug("Mirror query succeeded", 
						zap.Int("mirror_index", mirrorIndex),
						zap.String("session", sessionID),
						zap.String("query", query))
				}
			}(i, mirror)
		}
		
		// Cancel context after a delay to allow goroutines to complete
		go func() {
			time.Sleep(35 * time.Second)
			cancel()
		}()
	} else {
		server.logger.Debug("No transaction-aware mirrors available")
		// Fallback to original mirrors
		for i, mirror := range server.mirrors {
			go func(mirrorIndex int, m *Connection) {
				defer func() {
					if r := recover(); r != nil {
						server.logger.Error("Mirror goroutine panic", 
							zap.Int("mirror_index", mirrorIndex),
							zap.String("session", sessionID),
							zap.Any("panic", r))
					}
				}()
				
				err := m.ExecuteQueryV15(mirrorCtx, query, nil, parameters)
				if err != nil {
					server.logger.Error("Mirror query failed", 
						zap.Int("mirror_index", mirrorIndex),
						zap.String("session", sessionID),
						zap.Error(err))
				}
			}(i, mirror)
		}
	}
}

// Helper function to get session ID from context
func getSessionIDFromContext(ctx context.Context) string {
	// Extract session ID from context or generate unique one based on goroutine
	if sessionID := ctx.Value("session_id"); sessionID != nil {
		return sessionID.(string)
	}
	// Use goroutine ID as session identifier for connection affinity
	return fmt.Sprintf("session_%d", getGoroutineID())
}

// getGoroutineID returns current goroutine ID for session tracking
func getGoroutineID() int64 {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	idField := strings.Fields(strings.TrimPrefix(string(buf[:n]), "goroutine "))[0]
	id, _ := strconv.ParseInt(idField, 10, 64)
	return id
}

// shouldMirrorQuery determines if a query should be mirrored based on configuration
func (server *ProxyServerV15) shouldMirrorQuery(query string) bool {
	trimmed := strings.TrimSpace(strings.ToUpper(query))
	
	// If mirror all queries is enabled, mirror everything
	if server.queryFilterConfig.MirrorAllQueries {
		server.logger.Debug("Mirroring query - mirror all enabled", zap.String("query", trimmed))
		return true
	}
	
	// Check for SELECT queries
	if strings.HasPrefix(trimmed, "SELECT ") || strings.HasPrefix(trimmed, "WITH ") {
		result := server.queryFilterConfig.MirrorSelectQueries
		server.logger.Debug("SELECT query mirror decision", zap.String("query", trimmed), zap.Bool("mirror", result))
		return result
	}
	
	// DDL statements
	ddlPrefixes := []string{"CREATE", "ALTER", "DROP", "TRUNCATE"}
	for _, prefix := range ddlPrefixes {
		if strings.HasPrefix(trimmed, prefix+" ") || trimmed == prefix {
			result := !server.queryFilterConfig.MirrorDmlOnly
			server.logger.Info("DDL query mirror decision", 
				zap.String("query", trimmed), 
				zap.Bool("mirror", result),
				zap.Bool("mirror_dml_only", server.queryFilterConfig.MirrorDmlOnly))
			return result
		}
	}
	
	// Transaction commands
	txCommands := []string{"BEGIN", "COMMIT", "ROLLBACK", "START", "END"}
	for _, cmd := range txCommands {
		if strings.HasPrefix(trimmed, cmd+" ") || trimmed == cmd {
			server.logger.Debug("Transaction command mirror decision", zap.String("query", trimmed), zap.Bool("mirror", true))
			return true
		}
	}
	
	// DML statements  
	dmlPrefixes := []string{"INSERT", "UPDATE", "DELETE"}
	for _, prefix := range dmlPrefixes {
		if strings.HasPrefix(trimmed, prefix+" ") || trimmed == prefix {
			result := !server.queryFilterConfig.MirrorDdlOnly
			server.logger.Debug("DML query mirror decision", zap.String("query", trimmed), zap.Bool("mirror", result))
			return result
		}
	}
	
	// Default: don't mirror other queries (SHOW, EXPLAIN, etc.)
	return false
}

// executeMirrorQueries executes query on all mirrors with session awareness
func (server *ProxyServerV15) executeMirrorQueries(ctx context.Context, query string, parameters []wire.Parameter, session *Session) {
	for i, mirror := range server.mirrors {
		go func(mirrorIndex int, m *Connection) {
			err := m.ExecuteQueryV15(ctx, query, nil, parameters)
			if err != nil {
				server.logger.Error("Mirror query failed", 
					zap.Int("mirror_index", mirrorIndex),
					zap.String("session", session.ID),
					zap.Error(err))
			}
		}(i, mirror)
	}
}

func (server *ProxyServerV15) connectToPrimary(ctx context.Context, primaryAddress string) error {
	server.logger.Info("Connecting to primary", zap.String("address", primaryAddress))
	
	// Create original connection for backward compatibility
	primary := NewConnectionWithPoolConfig(server.logger, Primary, primaryAddress, server.poolConfig)
	err := primary.Connect(ctx, primaryAddress)
	if err != nil {
		return fmt.Errorf("failed to connect to primary: %w", err)
	}
	server.primary = primary
	
	// Create transaction-aware connection
	server.txAwarePrimary = NewTransactionAwareConnection(server.logger, Primary, primaryAddress)
	err = server.txAwarePrimary.Connect(ctx, server.poolConfig)
	if err != nil {
		server.logger.Warn("Failed to create transaction-aware primary connection, using fallback", zap.Error(err))
		server.txAwarePrimary = nil
	}
	
	return nil
}

func (server *ProxyServerV15) connectToMirrors(ctx context.Context, mirrorAddresses []string) error {
	server.mirrors = make([]*Connection, len(mirrorAddresses))
	server.txAwareMirrors = make([]*TransactionAwareConnection, len(mirrorAddresses))
	
	for i, address := range mirrorAddresses {
		server.logger.Info("Connecting to mirror", 
			zap.Int("index", i), 
			zap.String("address", address))
		
		// Create original connection for backward compatibility
		mirror := NewConnectionWithPoolConfig(server.logger, Mirror, address, server.poolConfig)
		err := mirror.Connect(ctx, address)
		if err != nil {
			return fmt.Errorf("failed to connect to mirror %d: %w", i, err)
		}
		server.mirrors[i] = mirror
		
		// Create transaction-aware connection
		txAwareMirror := NewTransactionAwareConnection(server.logger, Mirror, address)
		err = txAwareMirror.Connect(ctx, server.poolConfig)
		if err != nil {
			server.logger.Warn("Failed to create transaction-aware mirror connection", 
				zap.Int("index", i), zap.Error(err))
			server.txAwareMirrors[i] = nil
		} else {
			server.txAwareMirrors[i] = txAwareMirror
		}
	}
	
	return nil
}

// listenAndServeWithTLS starts the server with TLS support
func (server *ProxyServerV15) listenAndServeWithTLS(listenAddress string, tlsConfig *tls.Config) error {
	server.logger.Info("Starting TLS-enabled server", zap.String("listen", listenAddress))

	// Create TCP listener
	listener, err := net.Listen("tcp", listenAddress)
	if err != nil {
		return fmt.Errorf("failed to create listener: %w", err)
	}
	defer listener.Close()

	// Wrap with TLS logging listener
	tlsListener := NewLoggingTLSListener(listener, tlsConfig, server.logger)

	// Create wire server and serve with custom listener
	wireServer, err := wire.NewServer(server.parseQuery, wire.TLSConfig(tlsConfig))
	if err != nil {
		return fmt.Errorf("failed to create wire server: %w", err)
	}
	return wireServer.Serve(tlsListener)
}

func (server *ProxyServerV15) Close(ctx context.Context) error {
	if server.primary != nil {
		server.primary.Close(ctx)
	}
	
	for _, mirror := range server.mirrors {
		if mirror != nil {
			mirror.Close(ctx)
		}
	}
	
	// Stop cleanup routine
	server.stopCleanupRoutine()
	
	return nil
}

// startCleanup starts the periodic cleanup routine
func (server *ProxyServerV15) startCleanup() {
	server.cleanupTicker = time.NewTicker(30 * time.Second) // More frequent cleanup
	go func() {
		defer func() {
			if r := recover(); r != nil {
				server.logger.Error("Cleanup goroutine panic", zap.Any("panic", r))
			}
		}()
		
		for {
			select {
			case <-server.cleanupTicker.C:
				server.cleanupExpiredBuffers()
			case <-server.stopCleanup:
				server.cleanupTicker.Stop()
				return
			}
		}
	}()
}

// stopCleanup stops the cleanup routine
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
	expired := []string{}
	
	// More aggressive timeout based on memory pressure
	timeout := 5 * time.Minute // Reduced from 10 minutes
	if len(server.txBuffers) > 50 { // Memory pressure
		timeout = 2 * time.Minute
	}
	
	for sessionID, buffer := range server.txBuffers {
		if now.Sub(buffer.LastActivity) > timeout {
			expired = append(expired, sessionID)
		}
	}
	
	for _, sessionID := range expired {
		if buffer := server.txBuffers[sessionID]; buffer != nil {
			// Clear buffer data to help GC
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
		// Clear buffer data to help GC
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

// GetBufferStats returns transaction buffer statistics
func (server *ProxyServerV15) GetBufferStats() map[string]interface{} {
	server.txBufferMutex.RLock()
	defer server.txBufferMutex.RUnlock()
	
	totalQueries := 0
	for _, buffer := range server.txBuffers {
		totalQueries += len(buffer.Queries)
	}
	
	return map[string]interface{}{
		"active_transactions":     len(server.txBuffers),
		"total_buffered_queries": totalQueries,
	}
}
