package server

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"time"

	wire "github.com/jeroenrinzema/psql-wire"
	"go.uber.org/zap"
)

type ProxyServer struct {
	logger           *zap.Logger
	primary          *Connection
	mirrors          []*Connection
	poolConfig       *PoolConfig // Connection pool configuration
	performanceConfig *PerformanceConfig // Performance optimization settings
}

func NewProxyServer(logger *zap.Logger) *ProxyServer {
	return &ProxyServer{
		logger:           logger,
		poolConfig:       DefaultPoolConfig(),
		performanceConfig: DefaultPerformanceConfig(),
	}
}

func NewProxyServerWithPoolConfig(logger *zap.Logger, poolConfig *PoolConfig) *ProxyServer {
	return &ProxyServer{
		logger:           logger,
		poolConfig:       poolConfig,
		performanceConfig: DefaultPerformanceConfig(),
	}
}

func NewProxyServerWithConfigs(logger *zap.Logger, poolConfig *PoolConfig, perfConfig *PerformanceConfig) *ProxyServer {
	return &ProxyServer{
		logger:           logger,
		poolConfig:       poolConfig,
		performanceConfig: perfConfig,
	}
}

func (server *ProxyServer) ListenAndServe(
	ctx context.Context,
	listenAddress string,
	primaryAddress string,
	mirrorAddresses []string) error {
	return server.ListenAndServeWithTLS(ctx, listenAddress, primaryAddress, mirrorAddresses, nil)
}

func (server *ProxyServer) ListenAndServeWithTLS(
	ctx context.Context,
	listenAddress string,
	primaryAddress string,
	mirrorAddresses []string,
	tlsConfig *tls.Config) error {

	server.logger.Info("Starting Frenzy proxy server")

	primaryName := "primary"
	primaryConnection := NewConnectionWithPoolConfig(server.logger.Named(primaryName), Primary, primaryName, server.poolConfig)
	server.logger.Info("Connecting to primary database", zap.String("address", primaryAddress))
	
	err := primaryConnection.Connect(ctx, primaryAddress)
	if err != nil {
		server.logger.Error("Failed to connect to primary database", zap.Error(err))
		return err
	}
	server.primary = primaryConnection
	server.logger.Info("Successfully connected to primary database")

	server.logger.Info("Connecting to mirror databases", zap.Int("count", len(mirrorAddresses)))
	err = server.connectToMirrors(ctx, mirrorAddresses)
	if err != nil {
		server.logger.Error("Failed to connect to mirror databases", zap.Error(err))
		return err
	}
	server.logger.Info("Successfully connected to all mirror databases")

	// Create listener
	listener, err := net.Listen("tcp", listenAddress)
	if err != nil {
		server.logger.Error("Failed to create listener", zap.String("address", listenAddress), zap.Error(err))
		return err
	}
	server.logger.Info("Created TCP listener", zap.String("address", listenAddress))

	// Wrap with TLS if configured
	if tlsConfig != nil {
		server.logger.Info("Wrapping listener with TLS", 
			zap.String("address", listenAddress),
			zap.String("min_version", server.getTLSVersionString(tlsConfig.MinVersion)))
		listener = NewLoggingTLSListener(listener, tlsConfig, server.logger.Named("tls"))
		server.logger.Info("TLS listener created", zap.String("address", listenAddress))
	} else {
		server.logger.Info("TLS not enabled, using plain TCP", zap.String("address", listenAddress))
		// Even without TLS, add connection logging
		listener = &LoggingListener{Listener: listener, logger: server.logger.Named("tcp")}
	}

	// Create wire server using the v0.15.0 API (ParseFn handler)
	var postgresListener *wire.Server
	server.logger.Debug("Creating PostgreSQL wire protocol server")
	postgresListener, err = wire.NewServer(server.parseQuery)
	if err != nil {
		server.logger.Error("Failed to create wire server", zap.Error(err))
		return err
	}
	
	server.adoptPostgresVersion(postgresListener)
	server.logger.Info("Wire server created and configured")
	
	// Use the configured listener (with or without TLS)
	server.logger.Info("Starting to serve connections", zap.String("address", listenAddress))
	return postgresListener.Serve(listener)
}

// getTLSVersionString converts TLS version number to string for logging
func (server *ProxyServer) getTLSVersionString(version uint16) string {
	switch version {
	case tls.VersionTLS10:
		return "TLS 1.0"
	case tls.VersionTLS11:
		return "TLS 1.1"
	case tls.VersionTLS12:
		return "TLS 1.2"
	case tls.VersionTLS13:
		return "TLS 1.3"
	default:
		return fmt.Sprintf("Unknown (%d)", version)
	}
}

// parseQuery implements ParseFn for psql-wire v0.15.0
func (server *ProxyServer) parseQuery(ctx context.Context, query string) (wire.PreparedStatements, error) {
	server.logger.Debug("New client connection established")
	server.logger.Debug("PostgreSQL protocol negotiation completed")
	
	// Create a prepared statement that executes the query
	stmt := wire.NewStatement(func(ctx context.Context, writer wire.DataWriter, parameters []wire.Parameter) error {
		// Convert parameters to string slice for compatibility
		paramStrings := make([]string, len(parameters))
		for i, param := range parameters {
			paramStrings[i] = string(param.Value())
		}
		
		return server.handleLegacy(ctx, query, writer, paramStrings)
	})
	
	return wire.Prepared(stmt), nil
}

// handleLegacyWithLogging wraps the original handler with connection logging
func (server *ProxyServer) handleLegacyWithLogging(
	ctx context.Context,
	query string,
	writer wire.DataWriter,
	parameters []string) error {

	server.logger.Debug("New client connection established")
	server.logger.Debug("PostgreSQL protocol negotiation completed")
	
	return server.handleLegacy(ctx, query, writer, parameters)
}

func (server *ProxyServer) adoptPostgresVersion(postgresListener *wire.Server) {
	// We adopt and announce as the same version as the Primary.
	postgresListener.Version = server.primary.pgServerVersion
}

func (server *ProxyServer) connectToMirrors(
	ctx context.Context,
	mirrorAddresses []string) error {

	for index, mirrorAddress := range mirrorAddresses {
		name := "mirror-" + strconv.Itoa(index+1)
		connection := NewConnectionWithPoolConfig(server.logger.Named(name), Mirror, name, server.poolConfig)
		err := connection.Connect(ctx, mirrorAddress)
		if err != nil {
			return err
		}
		server.mirrors = append(server.mirrors, connection)
	}
	return nil
}

// executeMirrorWithRetry executes a query on a mirror with retry logic
func (server *ProxyServer) executeMirrorWithRetry(mirrorIndex int, mirror *Connection, query string, isSimpleCommand bool) {
	maxRetries := server.performanceConfig.MirrorRetries
	retryDelay := time.Duration(server.performanceConfig.RetryDelaySecs) * time.Second
	timeout := time.Duration(server.performanceConfig.MirrorTimeoutSecs) * time.Second
	
	for attempt := 0; attempt <= maxRetries; attempt++ {
		// Create context with timeout for each attempt
		mirrorCtx, cancel := context.WithTimeout(context.Background(), timeout)
		
		var err error
		if isSimpleCommand {
			err = mirror.ExecuteSimpleCommand(mirrorCtx, query)
		} else {
			err = mirror.ExecuteQuery(mirrorCtx, query, nil) // No writer for mirrors
		}
		
		cancel() // Always cancel the context
		
		if err == nil {
			// Success
			if attempt > 0 {
				server.logger.Info("Mirror operation succeeded after retry",
					zap.Int("mirror_index", mirrorIndex),
					zap.Int("attempt", attempt+1),
					zap.String("query", query))
			}
			return
		}
		
		// Log the error
		if attempt < maxRetries {
			server.logger.Warn("Mirror operation failed, retrying",
				zap.Int("mirror_index", mirrorIndex),
				zap.Int("attempt", attempt+1),
				zap.Int("max_retries", maxRetries),
				zap.Duration("retry_delay", retryDelay),
				zap.Error(err))
			
			// Wait before retry
			time.Sleep(retryDelay)
		} else {
			server.logger.Error("Mirror operation failed after all retries",
				zap.Int("mirror_index", mirrorIndex),
				zap.Int("total_attempts", attempt+1),
				zap.String("query", query),
				zap.Error(err))
		}
	}
}

// executeMirrorCommandWithRetry executes a simple command on a mirror with retry logic
func (server *ProxyServer) executeMirrorCommandWithRetry(mirrorIndex int, mirror *Connection, command string, commandIndex int, isMultiCommand bool) {
	maxRetries := server.performanceConfig.MirrorRetries
	retryDelay := time.Duration(server.performanceConfig.RetryDelaySecs) * time.Second
	timeout := time.Duration(server.performanceConfig.MirrorTimeoutSecs) * time.Second
	
	for attempt := 0; attempt <= maxRetries; attempt++ {
		// Create context with timeout for each attempt
		mirrorCtx, cancel := context.WithTimeout(context.Background(), timeout)
		
		err := mirror.ExecuteSimpleCommand(mirrorCtx, command)
		cancel() // Always cancel the context
		
		if err == nil {
			// Success
			if attempt > 0 {
				server.logger.Info("Mirror command succeeded after retry",
					zap.Int("mirror_index", mirrorIndex),
					zap.Int("command_index", commandIndex),
					zap.Int("attempt", attempt+1),
					zap.String("command", command))
			}
			return
		}
		
		// Log the error
		if attempt < maxRetries {
			server.logger.Warn("Mirror command failed, retrying",
				zap.Int("mirror_index", mirrorIndex),
				zap.Int("command_index", commandIndex),
				zap.Int("attempt", attempt+1),
				zap.Int("max_retries", maxRetries),
				zap.Duration("retry_delay", retryDelay),
				zap.String("command", command),
				zap.Error(err))
			
			// Wait before retry
			time.Sleep(retryDelay)
		} else {
			if isMultiCommand {
				server.logger.Error("Mirror multi-command query failed after all retries",
					zap.Int("mirror_index", mirrorIndex),
					zap.Int("command_index", commandIndex),
					zap.Int("total_attempts", attempt+1),
					zap.String("command", command),
					zap.Error(err))
			} else {
				server.logger.Error("Mirror command failed after all retries",
					zap.Int("mirror_index", mirrorIndex),
					zap.Int("total_attempts", attempt+1),
					zap.String("command", command),
					zap.Error(err))
			}
		}
	}
}

func (server *ProxyServer) Close(ctx context.Context) error {
	for _, mirror := range server.mirrors {
		err := mirror.Close(ctx)
		if err != nil {
			log.Printf("Failed to close mirror %s", err)
			return err
		}
	}
	return nil
}

// Handler for psql-wire v0.4.0 API (SimpleQuery style)
func (server *ProxyServer) handleLegacy(
	ctx context.Context,
	query string,
	writer wire.DataWriter,
	parameters []string) error {

	// Add panic recovery
	defer func() {
		if r := recover(); r != nil {
			server.logger.Error("PANIC in query handler", 
				zap.Any("panic", r),
				zap.String("query", query),
				zap.Strings("parameters", parameters))
		}
	}()

	server.logger.Debug("Received query", zap.String("query", query))

	// Debug multi-command detection
	isMulti := server.isMultiCommandQuery(query)
	commands := server.splitCommands(query)
	server.logger.Debug("Multi-command analysis", 
		zap.Bool("is_multi_command", isMulti),
		zap.Int("command_count", len(commands)),
		zap.Strings("commands", commands))

	// Check if it's a multi-command query
	if isMulti {
		server.logger.Debug("Processing as multi-command query")
		return server.handleMultiCommandQuery(ctx, query, writer)
	}

	server.logger.Debug("Processing as single command")
	
	// Process single command
	err := server.primary.ExecuteQuery(ctx, query, writer)
	if err != nil {
		server.logger.Error("Primary query failed", zap.Error(err))
		return err
	}
	
	// Execute on mirrors - async or sync based on configuration
	if server.performanceConfig.AsyncMirrors {
		// Asynchronous mirror processing (fire and forget)
		for i, mirror := range server.mirrors {
			go func(mirrorIndex int, m *Connection) {
				server.executeMirrorWithRetry(mirrorIndex, m, query, false)
			}(i, mirror)
		}
	} else {
		// Synchronous mirror processing (original behavior)
		for i, mirror := range server.mirrors {
			err := mirror.ExecuteQuery(ctx, query, writer)
			if err != nil {
				server.logger.Error("Mirror query failed", 
					zap.Int("mirror_index", i), 
					zap.Error(err))
				// Continue execution, don't interrupt due to mirror failure
			}
		}
	}
	return nil
}

// isMultiCommandQuery 检查查询是否包含多个命令
func (server *ProxyServer) isMultiCommandQuery(query string) bool {
	// 清理查询字符串
	trimmed := strings.TrimSpace(query)
	if trimmed == "" {
		return false
	}
	
	// Simple but effective method: treat any query containing semicolon as multi-command
	// This avoids complex parsing logic
	return strings.Contains(trimmed, ";")
}

// handleMultiCommandQuery processes multi-command queries
func (server *ProxyServer) handleMultiCommandQuery(ctx context.Context, query string, writer wire.DataWriter) error {
	// Split commands
	commands := server.splitCommands(query)
	
	server.logger.Debug("Processing multi-command query", 
		zap.Int("command_count", len(commands)),
		zap.String("original_query", query))
	
	var lastError error
	hasResults := false
	
	for i, cmd := range commands {
		cmd = strings.TrimSpace(cmd)
		if cmd == "" {
			continue
		}
		
		server.logger.Debug("Executing command", 
			zap.Int("index", i+1), 
			zap.Int("total", len(commands)),
			zap.String("command", cmd))
		
		// For multi-command queries, only the first result-returning command uses writer
		if server.isResultCommand(cmd) && !hasResults {
			// This is the first result-returning command, use writer
			err := server.primary.ExecuteQuery(ctx, cmd, writer)
			if err != nil {
				server.logger.Error("Multi-command query failed", 
					zap.Int("command_index", i+1),
					zap.String("command", cmd),
					zap.Error(err))
				lastError = err
				// 对于结果命令失败，直接返回错误
				return err
			} else {
				hasResults = true
			}
		} else {
			// This is SET command or other non-result-returning command, execute directly
			err := server.primary.ExecuteSimpleCommand(ctx, cmd)
			if err != nil {
				server.logger.Error("Multi-command simple command failed", 
					zap.Int("command_index", i+1),
					zap.String("command", cmd),
					zap.Error(err))
				lastError = err
				// For simple commands, log error but continue execution
			}
		}
		
		// Execute on mirrors - async or sync based on configuration
		if server.performanceConfig.AsyncMirrors {
			// Asynchronous mirror processing
			for j, mirror := range server.mirrors {
				go func(mirrorIndex int, m *Connection, command string, cmdIndex int) {
					server.executeMirrorCommandWithRetry(mirrorIndex, m, command, cmdIndex, true)
				}(j, mirror, cmd, i+1)
			}
		} else {
			// Synchronous mirror processing (original behavior)
			for j, mirror := range server.mirrors {
				err := mirror.ExecuteSimpleCommand(ctx, cmd)
				if err != nil {
					server.logger.Error("Mirror multi-command query failed", 
						zap.Int("mirror_index", j),
						zap.Int("command_index", i+1),
						zap.String("command", cmd),
						zap.Error(err))
					// Mirror failures don't affect main query
				}
			}
		}
	}
	
	// If no command returned results, send a simple completion response
	if !hasResults && lastError == nil {
		err := writer.Complete("OK")
		if err != nil {
			server.logger.Error("Failed to send completion response", zap.Error(err))
			return err
		}
	}
	
	return lastError
}

// isResultCommand determines if a command returns result set
func (server *ProxyServer) isResultCommand(cmd string) bool {
	cmdUpper := strings.ToUpper(strings.TrimSpace(cmd))
	
	// Commands that return results
	resultCommands := []string{"SELECT", "WITH", "VALUES", "TABLE", "SHOW"}
	
	for _, resultCmd := range resultCommands {
		if strings.HasPrefix(cmdUpper, resultCmd) {
			return true
		}
	}
	
	return false
}

// splitCommands splits multi-command query
func (server *ProxyServer) splitCommands(query string) []string {
	// Simple command splitting implementation
	// Note: This is a simplified version, doesn't handle semicolons within strings
	commands := strings.Split(query, ";")
	
	// Filter empty commands
	var result []string
	for _, cmd := range commands {
		cmd = strings.TrimSpace(cmd)
		if cmd != "" {
			result = append(result, cmd)
		}
	}
	
	return result
}
