package server

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	wire "github.com/jeroenrinzema/psql-wire"
	"github.com/lib/pq/oid"
	"go.uber.org/zap"
)

// Memory pools for buffer reuse to reduce GC pressure
var (
	queryBufferPool = sync.Pool{
		New: func() interface{} {
			return make([]byte, 0, 4096)
		},
	}
	
	stringBuilderPool = sync.Pool{
		New: func() interface{} {
			return &strings.Builder{}
		},
	}
)

// GetQueryBuffer gets a buffer from the pool
func GetQueryBuffer() []byte {
	return queryBufferPool.Get().([]byte)[:0]
}

// PutQueryBuffer returns a buffer to the pool
func PutQueryBuffer(buf []byte) {
	if cap(buf) <= 8192 { // Don't pool very large buffers
		queryBufferPool.Put(buf)
	}
}

// GetStringBuilder gets a string builder from the pool
func GetStringBuilder() *strings.Builder {
	sb := stringBuilderPool.Get().(*strings.Builder)
	sb.Reset()
	return sb
}

// PutStringBuilder returns a string builder to the pool
func PutStringBuilder(sb *strings.Builder) {
	if sb.Cap() <= 8192 { // Don't pool very large builders
		stringBuilderPool.Put(sb)
	}
}

type ConnectionType int

const (
	Primary ConnectionType = iota
	Mirror
)

// PoolConfig holds connection pool configuration
type PoolConfig struct {
	MaxConns        int32
	MinConns        int32
	MaxConnLifetime time.Duration
	MaxConnIdleTime time.Duration
}

// PerformanceConfig holds performance optimization settings
type PerformanceConfig struct {
	WorkerThreads     int
	QueryBufferSize   int
	AsyncMirrors      bool
	MirrorTimeoutSecs int // Timeout for mirror operations in seconds
	MirrorRetries     int // Number of retries for failed mirror operations
	RetryDelaySecs    int // Delay between retries in seconds
}

// DefaultPerformanceConfig returns default performance configuration
func DefaultPerformanceConfig() *PerformanceConfig {
	return &PerformanceConfig{
		WorkerThreads:     0,   // Auto-detect based on CPU cores
		QueryBufferSize:   8192, // 8KB buffer
		AsyncMirrors:      true, // Enable async mirrors by default
		MirrorTimeoutSecs: 120,  // 2 minutes timeout for mirrors
		MirrorRetries:     2,    // Retry failed mirror operations twice
		RetryDelaySecs:    5,    // 5 second delay between retries
	}
}

// DefaultPoolConfig returns default connection pool configuration optimized for high concurrency
func DefaultPoolConfig() *PoolConfig {
	return &PoolConfig{
		MaxConns:        10,  // Default, should be overridden by command line
		MinConns:        2,   // Default, should be overridden by command line
		MaxConnLifetime: 0,   // Never expire
		MaxConnIdleTime: 300 * time.Second, // 5 minutes idle timeout
	}
}

type Connection struct {
	logger          *zap.Logger
	name            string
	host            string
	pool            *pgxpool.Pool  // Use connection pool instead of single connection
	pgServerVersion string
	connectionType  ConnectionType
	poolConfig      *PoolConfig    // Connection pool configuration
	affinity        *ConnectionAffinity // Connection affinity for transaction support
}

func NewConnection(logger *zap.Logger, connectionType ConnectionType, name string) *Connection {
	var poolConfig *PoolConfig
	if connectionType == Mirror {
		// Use smaller pool for mirrors to prevent exhaustion
		poolConfig = &PoolConfig{
			MaxConns:        10, // Smaller pool for mirrors
			MinConns:        2,
			MaxConnLifetime: 3600,
			MaxConnIdleTime: 300,
		}
	} else {
		poolConfig = DefaultPoolConfig()
	}
	
	return &Connection{
		logger:         logger,
		connectionType: connectionType,
		name:           name,
		poolConfig:     poolConfig,
	}
}

func NewConnectionWithPoolConfig(logger *zap.Logger, connectionType ConnectionType, name string, poolConfig *PoolConfig) *Connection {
	return &Connection{
		logger:         logger,
		connectionType: connectionType,
		name:           name,
		poolConfig:     poolConfig,
	}
}

func (connection *Connection) Connect(
	ctx context.Context,
	hostAddress string) error {

	connection.host = hostAddress
	connection.logger.Debug("Attempting to connect with connection pool", zap.String("address", hostAddress))
	
	// Parse connection string to check for SSL parameters and configure if needed
	config, err := pgxpool.ParseConfig(connection.host)
	if err != nil {
		connection.logger.Error(
			"failed to parse connection string",
			zap.String("address", connection.host),
			zap.Error(err))
		return err
	}

	// Configure SSL/TLS if needed
	err = connection.configurePoolSSL(config, connection.host)
	if err != nil {
		connection.logger.Error(
			"failed to configure SSL",
			zap.String("address", connection.host),
			zap.Error(err))
		return err
	}

	// Configure connection pool parameters with sharing logic
	// For mirrors, use half of the configured connections to share with primary
	maxConns := connection.poolConfig.MaxConns
	minConns := connection.poolConfig.MinConns
	
	if connection.connectionType == Mirror {
		maxConns = maxConns / 2  // Mirror gets half
		minConns = minConns / 2
		if minConns < 1 {
			minConns = 1  // Ensure at least 1 connection
		}
	}
	
	config.MaxConns = maxConns
	config.MinConns = minConns
	config.MaxConnLifetime = connection.poolConfig.MaxConnLifetime  // Maximum connection lifetime
	config.MaxConnIdleTime = connection.poolConfig.MaxConnIdleTime  // Maximum connection idle time

	connection.logger.Info("Creating connection pool", 
		zap.Int32("max_conns", config.MaxConns),
		zap.Int32("min_conns", config.MinConns),
		zap.Duration("max_conn_lifetime", config.MaxConnLifetime),
		zap.Duration("max_conn_idle_time", config.MaxConnIdleTime))

	// Create connection pool
	connection.pool, err = pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		connection.logger.Error(
			"failed to create connection pool",
			zap.String("address", connection.host),
			zap.Error(err))
		return err
	}

	// Test connection pool
	conn, err := connection.pool.Acquire(ctx)
	if err != nil {
		connection.logger.Error(
			"failed to acquire connection from pool",
			zap.String("address", connection.host),
			zap.Error(err))
		return err
	}
	defer conn.Release()

	connection.logger.Info("Successfully connected to database with pool")

	// Initialize connection affinity for transaction support
	connection.affinity = NewConnectionAffinity(connection.pool, connection.logger)

	connection.pgServerVersion, err = connection.getPostgresVersionFromConn(ctx, conn.Conn())
	if err != nil {
		connection.logger.Error(
			"could not detect Postgres server_version_num",
			zap.Error(err))
	}

	if connection.connectionType == Primary {
		connection.logger.Info(
			"connected with connection pool",
			zap.String("address", connection.host),
			zap.Int32("max_conns", config.MaxConns),
			zap.Int32("min_conns", config.MinConns))

		if strings.TrimSpace(connection.pgServerVersion) == "" {
			connection.pgServerVersion = "150002"

			connection.logger.Error(
				"Could not detect primary server_version_num using default instead",
				zap.String("server_version_num", connection.pgServerVersion),
				zap.Error(err))
		}
		connection.logger.Info(
			"Detected and adopting primary server_version_num",
			zap.String("server_version_num", connection.pgServerVersion))

	} else if connection.connectionType == Mirror {
		connection.logger.Info(
			"Connected to mirror with connection pool",
			zap.String("address", connection.host))
	}
	return nil
}

func (connection *Connection) configurePoolSSL(config *pgxpool.Config, connectionString string) error {
	// Parse URL to extract SSL parameters
	u, err := url.Parse(connectionString)
	if err != nil {
		// If it's not a URL, it might be a key-value connection string
		if strings.Contains(connectionString, "sslmode=") {
			return connection.parsePoolSSLFromKeyValue(config, connectionString)
		}
		return nil
	}

	query := u.Query()
	
	// Configure SSL mode
	sslMode := query.Get("sslmode")
	sslRootCert := query.Get("sslrootcert")
	sslCert := query.Get("sslcert")
	sslKey := query.Get("sslkey")

	if sslMode != "" {
		connection.logger.Info("SSL configuration detected",
			zap.String("sslmode", sslMode),
			zap.String("sslrootcert", sslRootCert))

		// Configure TLS based on SSL mode
		switch sslMode {
		case "disable":
			config.ConnConfig.TLSConfig = nil
		case "require", "verify-ca", "verify-full":
			tlsConfig := &tls.Config{
				MinVersion: tls.VersionTLS12,
			}

			// Load root certificate if provided
			if sslRootCert != "" {
				caCert, err := ioutil.ReadFile(sslRootCert)
				if err != nil {
					return err
				}
				caCertPool := x509.NewCertPool()
				caCertPool.AppendCertsFromPEM(caCert)
				tlsConfig.RootCAs = caCertPool
			}

			// Load client certificate if provided
			if sslCert != "" && sslKey != "" {
				clientCert, err := tls.LoadX509KeyPair(sslCert, sslKey)
				if err != nil {
					return err
				}
				tlsConfig.Certificates = []tls.Certificate{clientCert}
			}

			// Configure verification mode
			switch sslMode {
			case "require":
				tlsConfig.InsecureSkipVerify = true
			case "verify-ca":
				tlsConfig.InsecureSkipVerify = false
			case "verify-full":
				tlsConfig.InsecureSkipVerify = false
				tlsConfig.ServerName = config.ConnConfig.Host
			}

			config.ConnConfig.TLSConfig = tlsConfig
		}
	}

	return nil
}

func (connection *Connection) parsePoolSSLFromKeyValue(config *pgxpool.Config, connectionString string) error {
	// Simple parsing for key=value style connection strings
	parts := strings.Split(connectionString, " ")
	sslParams := make(map[string]string)
	
	for _, part := range parts {
		if strings.Contains(part, "=") {
			kv := strings.SplitN(part, "=", 2)
			if len(kv) == 2 {
				key := strings.TrimSpace(kv[0])
				value := strings.TrimSpace(kv[1])
				if strings.HasPrefix(key, "ssl") {
					sslParams[key] = value
				}
			}
		}
	}

	if sslMode, exists := sslParams["sslmode"]; exists && sslMode != "disable" {
		tlsConfig := &tls.Config{
			MinVersion: tls.VersionTLS12,
		}

		// Load root certificate if provided
		if sslRootCert, exists := sslParams["sslrootcert"]; exists {
			caCert, err := ioutil.ReadFile(sslRootCert)
			if err != nil {
				return err
			}
			caCertPool := x509.NewCertPool()
			caCertPool.AppendCertsFromPEM(caCert)
			tlsConfig.RootCAs = caCertPool
		}

		// Configure verification mode
		switch sslMode {
		case "require":
			tlsConfig.InsecureSkipVerify = true
		case "verify-ca":
			tlsConfig.InsecureSkipVerify = false
		case "verify-full":
			tlsConfig.InsecureSkipVerify = false
			tlsConfig.ServerName = config.ConnConfig.Host
		}

		config.ConnConfig.TLSConfig = tlsConfig
		
		connection.logger.Info("SSL configuration applied",
			zap.String("sslmode", sslMode))
	}

	return nil
}

func (connection *Connection) parseSSLFromKeyValue(config *pgx.ConnConfig, connectionString string) error {
	// Simple parsing for key=value style connection strings
	parts := strings.Split(connectionString, " ")
	sslParams := make(map[string]string)
	
	for _, part := range parts {
		if strings.Contains(part, "=") {
			kv := strings.SplitN(part, "=", 2)
			if len(kv) == 2 {
				key := strings.TrimSpace(kv[0])
				value := strings.TrimSpace(kv[1])
				if strings.HasPrefix(key, "ssl") {
					sslParams[key] = value
				}
			}
		}
	}

	if sslMode, exists := sslParams["sslmode"]; exists && sslMode != "disable" {
		tlsConfig := &tls.Config{
			MinVersion: tls.VersionTLS12,
		}

		// Load root certificate if provided
		if sslRootCert, exists := sslParams["sslrootcert"]; exists {
			caCert, err := ioutil.ReadFile(sslRootCert)
			if err != nil {
				return err
			}
			caCertPool := x509.NewCertPool()
			caCertPool.AppendCertsFromPEM(caCert)
			tlsConfig.RootCAs = caCertPool
		}

		// Configure verification mode
		switch sslMode {
		case "require":
			tlsConfig.InsecureSkipVerify = true
		case "verify-ca":
			tlsConfig.InsecureSkipVerify = false
		case "verify-full":
			tlsConfig.InsecureSkipVerify = false
			tlsConfig.ServerName = config.Host
		}

		config.TLSConfig = tlsConfig
		
		connection.logger.Info("SSL configuration applied",
			zap.String("sslmode", sslMode))
	}

	return nil
}

func (connection *Connection) getPostgresVersionFromConn(ctx context.Context, conn *pgx.Conn) (string, error) {
	rows := conn.QueryRow(ctx, "SELECT current_setting('server_version_num');")

	var pgServerVersion string
	err := rows.Scan(&pgServerVersion)
	if err != nil {
		return "", err
	}
	return pgServerVersion, nil
}

func (connection *Connection) getPostgresVersion(ctx context.Context) (string, error) {
	conn, err := connection.pool.Acquire(ctx)
	if err != nil {
		return "", err
	}
	defer conn.Release()
	
	return connection.getPostgresVersionFromConn(ctx, conn.Conn())
}

func (connection *Connection) Close(ctx context.Context) error {
	if connection.pool != nil {
		connection.pool.Close()
		connection.logger.Info("Connection pool closed")
	}
	return nil
}

// ExecuteQueryV15 executes query using psql-wire v0.15.0 API
func (connection *Connection) ExecuteQueryV15(
	ctx context.Context,
	query string,
	writer wire.DataWriter,
	parameters []wire.Parameter) error {

	// Use the transaction-aware version
	return connection.ExecuteQueryV15WithTransactionSupport(ctx, query, writer, parameters)
}

// ExecuteQueryV15WithTransactionSupport executes query with proper transaction handling
func (connection *Connection) ExecuteQueryV15WithTransactionSupport(
	ctx context.Context,
	query string,
	writer wire.DataWriter,
	parameters []wire.Parameter) error {

	if connection == nil {
		return fmt.Errorf("connection is nil")
	}

	if connection.connectionType == Primary && writer != nil {
		return connection.executePrimaryQueryWithTransactionSupport(ctx, query, writer, parameters)
	} else if connection.connectionType == Mirror {
		return connection.executeMirrorQueryWithTransactionSupport(ctx, query, parameters)
	}
	return nil
}

func (connection *Connection) executePrimaryQueryWithTransactionSupport(
	ctx context.Context,
	query string,
	writer wire.DataWriter,
	parameters []wire.Parameter) error {

	if connection.pool == nil {
		return fmt.Errorf("database connection not initialized - call Connect() first")
	}

	// Generate session ID from context
	sessionID := getSessionIDFromContext(ctx)
	if sessionID == "" {
		sessionID = "default"
	}

	// Check if this is a transaction command
	_, cmdType := IsTransactionCommand(query)
	
	// Transaction-level connection management
	if sessionID == "" {
		sessionID = fmt.Sprintf("session_%p", ctx)
	}

	var conn *pgxpool.Conn
	var err error
	
	// Check if we're in a transaction and need session affinity
	if connection.affinity != nil {
		conn, err = connection.affinity.GetConnection(ctx, sessionID)
		if err != nil {
			return fmt.Errorf("failed to get session connection: %w", err)
		}
		// Don't release - affinity manages lifecycle
		connection.affinity.UpdateTransactionState(sessionID, query)
	} else {
		// Fallback to pool connection for non-transaction queries
		conn, err = connection.pool.Acquire(ctx)
		if err != nil {
			return fmt.Errorf("failed to acquire connection: %w", err)
		}
		defer conn.Release()
	}

	// For ROLLBACK commands, we need special handling
	if cmdType == "ROLLBACK" {
		return connection.handleRollbackWithAffinity(ctx, query, writer, conn)
	}

	// Execute query
	rows, err := conn.Query(ctx, query)
	if err != nil {
		return fmt.Errorf("query execution failed: %w", err)
	}
	defer rows.Close()

	// Process results
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return fmt.Errorf("query execution error: %w", err)
		}
		return writer.Complete("OK")
	}

	// Process rows
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

func (connection *Connection) handleRollbackWithAffinity(ctx context.Context, query string, writer wire.DataWriter, conn *pgxpool.Conn) error {
	// Execute ROLLBACK on primary
	_, err := conn.Exec(ctx, query)
	if err != nil {
		return fmt.Errorf("rollback failed: %w", err)
	}

	// Execute ROLLBACK on mirrors
	connection.executeMirrorQuery(ctx, query)

	connection.logger.Info("Transaction rolled back successfully")
	return writer.Complete("ROLLBACK")
}

func (connection *Connection) handleRollback(ctx context.Context, query string, writer wire.DataWriter) error {
	// For ROLLBACK, we need to ensure it actually rolls back the transaction
	conn, err := connection.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	// Execute ROLLBACK on primary
	_, err = conn.Exec(ctx, query)
	if err != nil {
		return fmt.Errorf("rollback failed: %w", err)
	}

	// Execute ROLLBACK on mirrors
	connection.executeMirrorQuery(ctx, query)

	connection.logger.Info("Transaction rolled back successfully")
	return writer.Complete("ROLLBACK")
}

func (connection *Connection) executeMirrorQueryWithTransactionSupport(
	ctx context.Context,
	query string,
	parameters []wire.Parameter) error {

	if connection.pool == nil {
		return fmt.Errorf("database connection not initialized - call Connect() first")
	}

	conn, err := connection.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	_, err = conn.Exec(ctx, query)
	return err
}

func (connection *Connection) executePrimaryQueryV15(
	ctx context.Context,
	query string,
	writer wire.DataWriter,
	parameters []wire.Parameter) error {

	if connection.pool == nil {
		return fmt.Errorf("database connection not initialized - call Connect() first")
	}

	// Get transaction tracker
	tracker := GetTransactionTracker(connection.logger)

	conn, err := connection.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer func() {
		// Update transaction state before releasing connection
		tracker.UpdateConnectionState(conn, query)
		conn.Release()
	}()

	// Execute query
	rows, err := conn.Query(ctx, query)
	if err != nil {
		return fmt.Errorf("query execution failed: %w", err)
	}
	defer rows.Close()

	// Check if there is a result set
	if !rows.Next() {
		// Check for errors
		if err := rows.Err(); err != nil {
			return fmt.Errorf("query execution error: %w", err)
		}
		// No result rows, might be SET command etc.
		return writer.Complete("OK")
	}

	// Check if columns are already defined
	existingColumns := writer.Columns()
	connection.logger.Debug("DataWriter columns", zap.Int("column_count", len(existingColumns)))

	// Process the first row (we already called Next())
	values, err := rows.Values()
	if err != nil {
		return fmt.Errorf("failed to scan first row: %w", err)
	}

	err = writer.Row(values)
	if err != nil {
		return fmt.Errorf("failed to write first row: %w", err)
	}
	rowCount := uint64(1)

	// Process remaining rows
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return fmt.Errorf("failed to scan row: %w", err)
		}

		err = writer.Row(values)
		if err != nil {
			return fmt.Errorf("failed to write row: %w", err)
		}
		rowCount++
	}

	// Check for errors during iteration
	if err := rows.Err(); err != nil {
		return fmt.Errorf("error during row iteration: %w", err)
	}

	return writer.Complete(fmt.Sprintf("SELECT %d", rowCount))
}

func (connection *Connection) executeMirrorQueryV15(
	ctx context.Context,
	query string,
	parameters []wire.Parameter) error {

	if connection.pool == nil {
		return fmt.Errorf("database connection not initialized - call Connect() first")
	}

	conn, err := connection.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("failed to acquire mirror connection: %w", err)
	}
	defer conn.Release()

	_, err = conn.Exec(ctx, query)
	return err
}

func (connection *Connection) ExecuteQuery(
	ctx context.Context,
	query string,
	writer wire.DataWriter) error {

	if connection.connectionType == Primary {
		return connection.executePrimaryQuery(ctx, query, writer)
	} else if connection.connectionType == Mirror {
		return connection.executeMirrorQuery(ctx, query)
	}
	return nil
}

func (connection *Connection) ExecuteSimpleCommand(ctx context.Context, query string) error {
	if connection.connectionType == Primary {
		return connection.executeSimpleCommand(ctx, query)
	} else if connection.connectionType == Mirror {
		return connection.executeMirrorQuery(ctx, query)
	}
	return nil
}

func (connection *Connection) executeSimpleCommand(ctx context.Context, query string) error {
	connection.logger.Debug("Executing simple command on primary", 
		zap.String("query", query),
		zap.String("address", connection.host))

	// Acquire connection from pool
	conn, err := connection.pool.Acquire(ctx)
	if err != nil {
		connection.logger.Error("Failed to acquire connection from pool", zap.Error(err))
		return err
	}
	defer conn.Release()

	// Use Exec instead of Query since we don't need result set
	commandTag, err := conn.Exec(ctx, query)
	if err != nil {
		connection.logger.Error(
			"Could not execute simple command on primary",
			zap.String("address", connection.host),
			zap.String("query", query),
			zap.Error(err))
		return err
	}

	connection.logger.Debug("Simple command completed successfully", 
		zap.String("query", query),
		zap.String("command_tag", commandTag.String()))
	
	return nil
}

func (connection *Connection) executePrimaryQuery(
	ctx context.Context,
	query string,
	writer wire.DataWriter) error {

	connection.logger.Debug("Executing query on primary", 
		zap.String("query", query),
		zap.String("address", connection.host))

	// Acquire connection from pool
	conn, err := connection.pool.Acquire(ctx)
	if err != nil {
		connection.logger.Error("Failed to acquire connection from pool", zap.Error(err))
		return err
	}
	defer conn.Release()

	// Use pgx simple query interface, supports multi-command
	rows, err := conn.Query(ctx, query)
	if err != nil {
		connection.logger.Error(
			"Could not execute query on primary",
			zap.String("address", connection.host),
			zap.String("query", query),
			zap.Error(err))
		return err
	}
	
	// Ensure rows are closed when function ends
	defer func() {
		rows.Close()
		connection.logger.Debug("Query rows closed")
	}()

	// Check if there is a result set
	if !rows.Next() {
		// Check for errors
		if err := rows.Err(); err != nil {
			connection.logger.Error("Query execution error", zap.Error(err))
			return err
		}
		
		// No result rows, might be SET command etc.
		connection.logger.Debug("Query executed successfully with no result set")
		err = writer.Complete("OK")
		if err != nil {
			connection.logger.Error("Failed to complete query", zap.Error(err))
			return err
		}
		return nil
	}

	// Has result set, process column definitions
	table := wire.Columns{}

	// Add the columns.
	for _, field := range rows.FieldDescriptions() {
		dataTypeOID := oid.Oid(field.DataTypeOID)
		dataTypeName := oid.TypeName[dataTypeOID]
		connection.logger.Debug(
			"column read",
			zap.String("column", field.Name),
			zap.String("type", dataTypeName))

		column := wire.Column{
			Table: 0,
			Name:  field.Name,
			Oid:   dataTypeOID,
		}
		table = append(table, column)
	}
	
	// Define columns with error handling - DISABLED for v0.15.0 compatibility
	// In v0.15.0, columns are defined when creating DataWriter
	if len(table) > 0 {
		connection.logger.Debug("Columns detected", zap.Int("column_count", len(table)))
		// err = writer.Define(table) // Removed in v0.15.0
		// if err != nil {
		//	connection.logger.Error("Failed to define columns", zap.Error(err))
		//	return err
		// }
	}

	// Process the first row (we already called Next())
	values, err := rows.Values()
	if err != nil {
		connection.logger.Error("Failed to scan row values", zap.Error(err))
		return err
	}
	writer.Row(values)
	rowCount := 1

	// Loop remaining rows
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			connection.logger.Error("Failed to scan row values", zap.Error(err))
			return err
		}
		writer.Row(values)
		rowCount++
	}
	
	// Check for errors during iteration
	if err := rows.Err(); err != nil {
		connection.logger.Error("Error during row iteration", zap.Error(err))
		return err
	}
	
	connection.logger.Debug("Query completed", 
		zap.Int("rows_returned", rowCount),
		zap.String("query", query))
	
	err = writer.Complete("OK")
	if err != nil {
		connection.logger.Error("Failed to complete query", zap.Error(err))
		return err
	}
	return nil
}

// createColumn creates a wire.Column with fields that are available in the API
func (connection *Connection) createColumn(name string, oid oid.Oid) wire.Column {
	// Start with basic fields that should be available in most versions
	column := wire.Column{
		Table: 0,
		Name:  name,
		Oid:   oid,
	}
	
	// Try to add additional fields if they exist (this will compile only if available)
	// Use a type assertion approach to avoid compilation errors
	
	// This is a simplified approach - just use the basic fields
	// The original working code likely used these same basic fields
	
	return column
}

func (connection *Connection) executeMirrorQuery(
	ctx context.Context,
	query string) error {

	connection.logger.Debug("Executing query on mirror", 
		zap.String("query", query),
		zap.String("address", connection.host))

	// Acquire connection from pool
	conn, err := connection.pool.Acquire(ctx)
	if err != nil {
		connection.logger.Error("Failed to acquire connection from pool for mirror", zap.Error(err))
		return err
	}
	defer conn.Release()

	rows, err := conn.Query(context.Background(), query)
	if err != nil {
		connection.logger.Error(
			"Could not execute query on mirror",
			zap.String("address", connection.host),
			zap.String("query", query),
			zap.Error(err))

		return err
	}
	defer rows.Close()
	
	// Count rows for logging
	rowCount := 0
	for rows.Next() {
		rowCount++
	}
	
	connection.logger.Debug("Mirror query completed", 
		zap.Int("rows_processed", rowCount),
		zap.String("query", query))
	
	return nil
}
