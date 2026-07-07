package server

import (
	"context"
	"crypto/tls"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	wire "github.com/jeroenrinzema/psql-wire"
	"github.com/lib/pq/oid"
	"go.uber.org/zap"
)

type sessionIDKey struct{}

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
	TotalBytes   int64
	CreatedAt    time.Time
	LastActivity time.Time
}

type ProxyServerV15 struct {
	logger           *zap.Logger
	primary          *Connection
	mirrors          []*Connection
	poolConfig       *PoolConfig
	performanceConfig *PerformanceConfig
	queryFilterConfig atomic.Pointer[QueryFilterConfig]
	sessionManager   *SessionManager
	wireServer       *wire.Server
	
	// Transaction buffering for commit-time mirroring
	txBuffers        map[string]*TransactionBuffer
	txBufferMutex    sync.RWMutex
	cleanupTicker    *time.Ticker
	stopCleanup      chan struct{}

	// Circuit breaker for mirror health
	mirrorBreakers   *MirrorCircuitBreakers

	// Mirror reconnection manager
	mirrorReconnect  *MirrorReconnectManager

	// Column cache: avoids repeated PREPARE round-trips for same query patterns
	columnCache      sync.Map // map[string]wire.Columns
}

func NewProxyServerV15(logger *zap.Logger) *ProxyServerV15 {
	defaultFilter := &QueryFilterConfig{
		SkipRollbackMirror: false, // Enable ROLLBACK mirroring
		SkipFailedTxMirror: true,  // Default: skip failed transaction mirroring
		SkipMirrorTxLocks:  false, // Default: use transactions on mirrors
	}

	server := &ProxyServerV15{
		logger:           logger,
		txBuffers:        make(map[string]*TransactionBuffer),
		poolConfig:       DefaultPoolConfig(),
		performanceConfig: DefaultPerformanceConfig(),
		sessionManager:   NewSessionManager(logger),
		stopCleanup:      make(chan struct{}),
	}
	server.queryFilterConfig.Store(defaultFilter)
	
	// Start cleanup routine
	server.startCleanup()
	
	return server
}

// SetQueryFilterConfig sets the query filtering configuration (safe for concurrent access).
func (server *ProxyServerV15) SetQueryFilterConfig(config *QueryFilterConfig) {
	server.queryFilterConfig.Store(config)
}

// getQueryFilterConfig retrieves the current query filter config atomically.
func (server *ProxyServerV15) getQueryFilterConfig() *QueryFilterConfig {
	return server.queryFilterConfig.Load()
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

	// Create wire server
	var wireOpts []wire.OptionFn
	if tlsConfig != nil {
		wireOpts = append(wireOpts, wire.TLSConfig(tlsConfig))
	}

	wireServer, err := wire.NewServer(server.parseQuery, wireOpts...)
	if err != nil {
		return fmt.Errorf("failed to create wire server: %w", err)
	}
	server.wireServer = wireServer

	// Listen for context cancellation to trigger graceful shutdown
	go func() {
		<-ctx.Done()
		server.logger.Info("Context cancelled, shutting down wire server")
		wireServer.Close()
	}()

	// Start server
	if tlsConfig != nil {
		return server.listenAndServeWithTLSServer(listenAddress, tlsConfig, wireServer)
	}
	return wireServer.ListenAndServe(listenAddress)
}

// parseQuery implements ParseFn for psql-wire v0.15.0
func (server *ProxyServerV15) parseQuery(ctx context.Context, query string) (wire.PreparedStatements, error) {
	server.logger.Debug("Parsing query", zap.String("query", query))
	
	// Split multi-command queries by semicolon (handling quoted strings)
	commands := server.splitCommands(query)
	
	var statements []*wire.PreparedStatement
	for _, cmd := range commands {
		cmd = strings.TrimSpace(cmd)
		if cmd == "" {
			continue
		}
		
		// Detect parameters ($1, $2, etc.) in the query
		params := wire.ParseParameters(cmd)
		
		// Determine columns for result-returning queries (cached)
		var columns wire.Columns
		if isResultReturningQuery(cmd) {
			columns = server.getCachedColumns(ctx, cmd)
		}
		
		// Capture cmd in closure
		capturedCmd := cmd
		
		// Build statement options
		opts := []wire.PreparedOptionFn{}
		if len(columns) > 0 {
			opts = append(opts, wire.WithColumns(columns))
		}
		if len(params) > 0 {
			opts = append(opts, wire.WithParameters(params))
		}
		
		stmt := wire.NewStatement(func(ctx context.Context, writer wire.DataWriter, parameters []wire.Parameter) error {
			return server.executeQuery(ctx, capturedCmd, writer, parameters)
		}, opts...)
		
		statements = append(statements, stmt)
	}
	
	if len(statements) == 0 {
		// Return empty result for empty query
		stmt := wire.NewStatement(func(ctx context.Context, writer wire.DataWriter, parameters []wire.Parameter) error {
			return writer.Complete("OK")
		})
		statements = append(statements, stmt)
	}
	
	return wire.PreparedStatements(statements), nil
}

// getCachedColumns returns cached column info or fetches it once via PREPARE.
func (server *ProxyServerV15) getCachedColumns(ctx context.Context, query string) wire.Columns {
	// Normalize query for cache key: replace numeric literals with placeholders
	// so "WHERE aid = 123" and "WHERE aid = 456" hit the same cache entry
	cacheKey := normalizeQueryForCache(query)

	// Check cache first
	if cached, ok := server.columnCache.Load(cacheKey); ok {
		return cached.(wire.Columns)
	}

	// Cache miss — fetch columns via PREPARE (one-time cost per unique query pattern)
	columns, err := server.getQueryColumnsWithPrepare(ctx, query)
	if err != nil {
		server.logger.Debug("Column detection failed, proceeding without", zap.Error(err))
		return nil
	}

	// Store in cache
	server.columnCache.Store(cacheKey, columns)
	return columns
}

// normalizeQueryForCache strips numeric literals to create a stable cache key
// for queries that differ only in parameter values.
func normalizeQueryForCache(query string) string {
	var sb strings.Builder
	sb.Grow(len(query))
	runes := []rune(query)
	n := len(runes)
	i := 0

	for i < n {
		ch := runes[i]

		// Skip single-quoted strings entirely (preserve in cache key)
		if ch == '\'' {
			sb.WriteRune(ch)
			i++
			for i < n && runes[i] != '\'' {
				sb.WriteRune(runes[i])
				i++
			}
			if i < n {
				sb.WriteRune(runes[i])
				i++
			}
			continue
		}

		// Replace numeric literals with ?
		if ch >= '0' && ch <= '9' {
			// Don't replace if part of an identifier (e.g. table1, col2)
			if i > 0 {
				prev := runes[i-1]
				if prev == '_' || (prev >= 'a' && prev <= 'z') || (prev >= 'A' && prev <= 'Z') {
					sb.WriteRune(ch)
					i++
					continue
				}
			}
			sb.WriteByte('?')
			// Skip remaining digits and decimal point
			for i < n && ((runes[i] >= '0' && runes[i] <= '9') || runes[i] == '.') {
				i++
			}
			continue
		}

		sb.WriteRune(ch)
		i++
	}

	return sb.String()
}

// getQueryColumnsWithPrepare gets column information using a fast approach.
// For SELECT/WITH/SHOW queries, wraps in a subquery with LIMIT 0 to get column descriptions.
// For non-result queries, returns empty columns immediately.
func (server *ProxyServerV15) getQueryColumnsWithPrepare(ctx context.Context, query string) (wire.Columns, error) {
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

	// Fast approach: append LIMIT 0 to get column info without data.
	// If query already has LIMIT, this may cause syntax error — fallback to subquery.
	limitQuery := query + " LIMIT 0"
	rows, err := conn.Query(ctx, limitQuery)
	if err != nil {
		// Fallback: wrap in subquery for queries that already have LIMIT/OFFSET
		limitQuery = fmt.Sprintf("SELECT * FROM (%s) AS _frenzy_cols LIMIT 0", query)
		rows, err = conn.Query(ctx, limitQuery)
		if err != nil {
			return wire.Columns{}, nil
		}
	}
	defer rows.Close()

	columns := wire.Columns{}
	for _, field := range rows.FieldDescriptions() {
		columns = append(columns, wire.Column{
			Table: 0,
			Name:  field.Name,
			Oid:   oid.Oid(field.DataTypeOID),
		})
	}
	return columns, nil
}

// getQueryColumnsSimple is no longer used — kept as a no-op for interface compatibility.
func (server *ProxyServerV15) getQueryColumnsSimple(ctx context.Context, query string) (wire.Columns, error) {
	return nil, fmt.Errorf("column detection not available for this query type")
}

// splitCommands splits a query string by semicolons, handling:
// - Single-quoted strings (with '' escape)
// - Double-quoted identifiers
// - Dollar-quoted strings ($$...$$, $tag$...$tag$)
// - Single-line comments (--)
// - Multi-line comments (/* ... */)
func (server *ProxyServerV15) splitCommands(query string) []string {
	var commands []string
	var current strings.Builder
	runes := []rune(query)
	n := len(runes)
	i := 0

	for i < n {
		ch := runes[i]

		// Single-line comment: skip to end of line
		if ch == '-' && i+1 < n && runes[i+1] == '-' {
			for i < n && runes[i] != '\n' {
				current.WriteRune(runes[i])
				i++
			}
			continue
		}

		// Multi-line comment: skip to */
		if ch == '/' && i+1 < n && runes[i+1] == '*' {
			current.WriteRune(runes[i])
			current.WriteRune(runes[i+1])
			i += 2
			depth := 1
			for i < n && depth > 0 {
				if runes[i] == '/' && i+1 < n && runes[i+1] == '*' {
					depth++
					current.WriteRune(runes[i])
					current.WriteRune(runes[i+1])
					i += 2
				} else if runes[i] == '*' && i+1 < n && runes[i+1] == '/' {
					depth--
					current.WriteRune(runes[i])
					current.WriteRune(runes[i+1])
					i += 2
				} else {
					current.WriteRune(runes[i])
					i++
				}
			}
			continue
		}

		// Dollar-quoted string: $tag$...$tag$
		if ch == '$' {
			// Try to read the tag
			tagEnd := i + 1
			for tagEnd < n && (runes[tagEnd] == '_' || (runes[tagEnd] >= 'a' && runes[tagEnd] <= 'z') || (runes[tagEnd] >= 'A' && runes[tagEnd] <= 'Z') || (runes[tagEnd] >= '0' && runes[tagEnd] <= '9')) {
				tagEnd++
			}
			if tagEnd < n && runes[tagEnd] == '$' {
				// Found opening dollar quote: $tag$
				tag := string(runes[i : tagEnd+1])
				// Write the opening tag
				for _, r := range tag {
					current.WriteRune(r)
				}
				i = tagEnd + 1
				// Find closing tag
				for i < n {
					if runes[i] == '$' {
						// Check if closing tag matches
						remaining := string(runes[i:])
						if strings.HasPrefix(remaining, tag) {
							for _, r := range tag {
								current.WriteRune(r)
							}
							i += len([]rune(tag))
							break
						}
					}
					current.WriteRune(runes[i])
					i++
				}
				continue
			}
			// Not a dollar quote, just a $ character
			current.WriteRune(ch)
			i++
			continue
		}

		// Single-quoted string with '' escape handling
		if ch == '\'' {
			current.WriteRune(ch)
			i++
			for i < n {
				if runes[i] == '\'' {
					current.WriteRune(runes[i])
					i++
					// Check for escaped quote ''
					if i < n && runes[i] == '\'' {
						current.WriteRune(runes[i])
						i++
						continue
					}
					break
				}
				// Handle backslash escape in E'...' strings
				if runes[i] == '\\' && i+1 < n {
					current.WriteRune(runes[i])
					i++
					current.WriteRune(runes[i])
					i++
					continue
				}
				current.WriteRune(runes[i])
				i++
			}
			continue
		}

		// Double-quoted identifier
		if ch == '"' {
			current.WriteRune(ch)
			i++
			for i < n {
				if runes[i] == '"' {
					current.WriteRune(runes[i])
					i++
					// Check for escaped quote ""
					if i < n && runes[i] == '"' {
						current.WriteRune(runes[i])
						i++
						continue
					}
					break
				}
				current.WriteRune(runes[i])
				i++
			}
			continue
		}

		// Semicolon — command separator
		if ch == ';' {
			cmd := strings.TrimSpace(current.String())
			if cmd != "" {
				commands = append(commands, cmd)
			}
			current.Reset()
			i++
			continue
		}

		// Regular character
		current.WriteRune(ch)
		i++
	}

	// Add the last command
	cmd := strings.TrimSpace(current.String())
	if cmd != "" {
		commands = append(commands, cmd)
	}

	return commands
}

