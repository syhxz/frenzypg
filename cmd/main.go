package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"net/url"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/akamensky/argparse"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/syhxz/frenzypg/server"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	lumberjack "gopkg.in/natefinch/lumberjack.v2"
)

// Build-time variables (set via ldflags)
var (
	version   = "dev"
	buildTime = "unknown"
)

func main() {
	// Handle --version early (before argparse)
	for _, arg := range os.Args[1:] {
		if arg == "--version" || arg == "-v" {
			fmt.Printf("frenzy %s (built %s)\n", version, buildTime)
			os.Exit(0)
		}
	}

	// Handle --config mode: if --config is present, use config file instead of CLI args
	for i, arg := range os.Args[1:] {
		if arg == "--config" || arg == "-c" {
			if i+1 < len(os.Args)-1 {
				runWithConfig(os.Args[i+2])
				return
			}
			fmt.Fprintf(os.Stderr, "Error: --config requires a file path\n")
			os.Exit(1)
		}
		if strings.HasPrefix(arg, "--config=") {
			runWithConfig(strings.TrimPrefix(arg, "--config="))
			return
		}
	}

	// CLI mode
	runWithCLI()
}

func runWithCLI() {
	logger, err := configureLogger()
	if err != nil {
		log.Fatal(err)
	}
	defer logger.Sync()

	parser := argparse.NewParser("frenzy", "Frenzy Mirroring Postgres Proxy")
	listenAddress := parser.String("l", "listen", &argparse.Options{Required: true, Help: "Listening port."})
	primaryAddress := parser.String("p", "primary", &argparse.Options{Required: true, Help: "Primary Postgres connection string."})
	var mirrorsAddresses *[]string = parser.StringList("m", "mirror", &argparse.Options{Required: false, Help: "Mirror Postgres connection string."})

	// Proxy mode selection
	proxyMode := parser.String("", "mode", &argparse.Options{
		Required: false,
		Help:     "Proxy mode: 'raw' (high-performance protocol forwarding, default) or 'wire' (psql-wire, full decode)",
		Default:  "raw",
	})

	// SSL/TLS configuration options
	tlsCertFile := parser.String("", "tls-cert", &argparse.Options{Required: false, Help: "TLS certificate file path."})
	tlsKeyFile := parser.String("", "tls-key", &argparse.Options{Required: false, Help: "TLS private key file path."})
	tlsCAFile := parser.String("", "tls-ca", &argparse.Options{Required: false, Help: "TLS CA certificate file path."})
	tlsServerName := parser.String("", "tls-server-name", &argparse.Options{Required: false, Help: "TLS server name for verification."})
	tlsSkipVerify := parser.Flag("", "tls-skip-verify", &argparse.Options{Required: false, Help: "Skip TLS certificate verification."})
	enableTLS := parser.Flag("", "enable-tls", &argparse.Options{Required: false, Help: "Enable TLS for the proxy server."})

	// Connection pool configuration options
	maxConns := parser.Int("", "max-conns", &argparse.Options{
		Required: false,
		Help:     "Maximum number of connections in the pool",
		Default:  10,
	})
	minConns := parser.Int("", "min-conns", &argparse.Options{
		Required: false,
		Help:     "Minimum number of connections in the pool",
		Default:  2,
	})
	maxConnLifetime := parser.Int("", "max-conn-lifetime", &argparse.Options{
		Required: false,
		Help:     "Maximum connection lifetime in seconds (0 means never expire)",
		Default:  0,
	})
	maxConnIdleTime := parser.Int("", "max-conn-idle-time", &argparse.Options{
		Required: false,
		Help:     "Maximum connection idle time in seconds (0 means never expire)",
		Default:  300,
	})

	// Performance optimization options
	workerThreads := parser.Int("", "worker-threads", &argparse.Options{
		Required: false,
		Help:     "Number of worker threads for query processing (0 = auto-detect)",
		Default:  0,
	})
	queryBufferSize := parser.Int("", "query-buffer-size", &argparse.Options{
		Required: false,
		Help:     "Query buffer size in bytes",
		Default:  8192,
	})
	asyncMirrors := parser.Flag("", "async-mirrors", &argparse.Options{
		Required: false,
		Help:     "Process mirror queries asynchronously (don't block primary response)",
	})
	mirrorTimeout := parser.Int("", "mirror-timeout", &argparse.Options{
		Required: false,
		Help:     "Timeout for mirror operations in seconds",
		Default:  120,
	})
	mirrorRetries := parser.Int("", "mirror-retries", &argparse.Options{
		Required: false,
		Help:     "Number of retries for failed mirror operations",
		Default:  2,
	})
	retryDelay := parser.Int("", "retry-delay", &argparse.Options{
		Required: false,
		Help:     "Delay between retries in seconds",
		Default:  5,
	})

	// Query filtering options
	mirrorAllQueries := parser.Flag("", "mirror-all-queries", &argparse.Options{
		Required: false,
		Help:     "Mirror all queries (default: only DDL/DML)",
	})
	mirrorSelectQueries := parser.Flag("", "mirror-select-queries", &argparse.Options{
		Required: false,
		Help:     "Mirror SELECT queries",
	})
	mirrorDdlOnly := parser.Flag("", "mirror-ddl-only", &argparse.Options{
		Required: false,
		Help:     "Mirror only DDL statements (CREATE, ALTER, DROP, TRUNCATE)",
	})
	mirrorDmlOnly := parser.Flag("", "mirror-dml-only", &argparse.Options{
		Required: false,
		Help:     "Mirror only DML statements (INSERT, UPDATE, DELETE)",
	})
	skipRollbackMirror := parser.Flag("", "skip-rollback-mirror", &argparse.Options{
		Required: false,
		Help:     "Skip mirroring ROLLBACK transactions",
	})
	skipFailedTxMirror := parser.Flag("", "skip-failed-tx-mirror", &argparse.Options{
		Required: false,
		Help:     "Skip mirroring failed transactions",
	})
	skipMirrorTxLocks := parser.Flag("", "skip-mirror-tx-locks", &argparse.Options{
		Required: false,
		Help:     "Skip BEGIN/COMMIT on mirrors to avoid transaction locks",
	})

	// Service management options
	showVersion := parser.Flag("v", "version", &argparse.Options{
		Required: false,
		Help:     "Show version and exit",
	})
	pidFile := parser.String("", "pid-file", &argparse.Options{
		Required: false,
		Help:     "Write PID to file",
	})
	healthPort := parser.Int("", "health-port", &argparse.Options{
		Required: false,
		Help:     "HTTP health check port (0 = disabled)",
		Default:  0,
	})

	// Parse input
	err = parser.Parse(os.Args)
	if err != nil {
		fmt.Print(parser.Usage(err))
		fmt.Printf("EXAMPLE\nfrenzy --listen :5432 --primary postgresql://postgres:password@localhost:5441/postgres --mirror postgresql://postgres:password@localhost:5442/postgres\n")
		fmt.Printf("WITH TLS\nfrenzy --listen :5432 --enable-tls --tls-cert server.crt --tls-key server.key --tls-ca ca.pem --primary postgresql://postgres:password@localhost:5441/postgres --mirror postgresql://postgres:password@localhost:5442/postgres\n")
		os.Exit(1)
	}

	// Handle --version
	if *showVersion {
		fmt.Printf("frenzy %s (built %s)\n", version, buildTime)
		os.Exit(0)
	}

	// Write PID file
	if *pidFile != "" {
		pid := os.Getpid()
		if err := os.WriteFile(*pidFile, []byte(strconv.Itoa(pid)+"\n"), 0600); err != nil {
			logger.Fatal("Failed to write PID file", zap.String("path", *pidFile), zap.Error(err))
		}
		defer os.Remove(*pidFile)
		logger.Info("PID file written", zap.String("path", *pidFile), zap.Int("pid", pid))
	}

	// Configure TLS if enabled
	var tlsConfig *tls.Config
	if *enableTLS {
		tlsConfig, err = configureTLS(*tlsCertFile, *tlsKeyFile, *tlsCAFile, *tlsServerName, *tlsSkipVerify)
		if err != nil {
			logger.Fatal("Failed to configure TLS", zap.Error(err))
		}
		logger.Info("TLS enabled for proxy server")
	}

	mirrors := *mirrorsAddresses
	logFields := make([]zapcore.Field, 0)
	logFields = append(logFields, zap.String("listen", *listenAddress))
	logFields = append(logFields, zap.String("primary", maskConnectionString(*primaryAddress)))
	logFields = append(logFields, zap.Bool("tls_enabled", *enableTLS))
	for index, mirror := range mirrors {
		field := zapcore.Field{
			Key:    "mirror-" + fmt.Sprintf("%d", index+1),
			Type:   zapcore.StringType,
			String: maskConnectionString(mirror),
		}
		logFields = append(logFields, field)
	}

	logger.Info("frenzy starting up", logFields...)

	// Create connection pool configuration
	poolConfig := &server.PoolConfig{
		MaxConns:        int32(*maxConns),
		MinConns:        int32(*minConns),
		MaxConnLifetime: time.Duration(*maxConnLifetime) * time.Second,
		MaxConnIdleTime: time.Duration(*maxConnIdleTime) * time.Second,
	}

	// Create performance configuration
	performanceConfig := &server.PerformanceConfig{
		WorkerThreads:     *workerThreads,
		QueryBufferSize:   *queryBufferSize,
		AsyncMirrors:      *asyncMirrors,
		MirrorTimeoutSecs: *mirrorTimeout,
		MirrorRetries:     *mirrorRetries,
		RetryDelaySecs:    *retryDelay,
	}

	// Auto-detect worker threads if not specified
	if performanceConfig.WorkerThreads == 0 {
		performanceConfig.WorkerThreads = runtime.NumCPU()
	}

	logger.Info("Connection pool configuration",
		zap.Int32("max_conns", poolConfig.MaxConns),
		zap.Int32("min_conns", poolConfig.MinConns),
		zap.Duration("max_conn_lifetime", poolConfig.MaxConnLifetime),
		zap.Duration("max_conn_idle_time", poolConfig.MaxConnIdleTime))

	logger.Info("Performance configuration",
		zap.Int("worker_threads", performanceConfig.WorkerThreads),
		zap.Int("query_buffer_size", performanceConfig.QueryBufferSize),
		zap.Bool("async_mirrors", performanceConfig.AsyncMirrors),
		zap.Int("mirror_timeout_secs", performanceConfig.MirrorTimeoutSecs),
		zap.Int("mirror_retries", performanceConfig.MirrorRetries),
		zap.Int("retry_delay_secs", performanceConfig.RetryDelaySecs),
		zap.Bool("mirror_all_queries", *mirrorAllQueries),
		zap.Bool("mirror_select_queries", *mirrorSelectQueries),
		zap.Bool("mirror_ddl_only", *mirrorDdlOnly),
		zap.Bool("mirror_dml_only", *mirrorDmlOnly),
		zap.Bool("skip_rollback_mirror", *skipRollbackMirror),
		zap.Bool("skip_failed_tx_mirror", *skipFailedTxMirror),
		zap.Bool("skip_mirror_tx_locks", *skipMirrorTxLocks))

	// Create query filter config
	queryFilterConfig := &server.QueryFilterConfig{
		MirrorAllQueries:    *mirrorAllQueries,
		MirrorSelectQueries: *mirrorSelectQueries,
		MirrorDdlOnly:       *mirrorDdlOnly,
		MirrorDmlOnly:       *mirrorDmlOnly,
		SkipRollbackMirror:  *skipRollbackMirror,
		SkipFailedTxMirror:  *skipFailedTxMirror,
		SkipMirrorTxLocks:   *skipMirrorTxLocks,
	}

	srv := server.NewProxyServerV15(logger)
	srv.SetPoolConfig(poolConfig)
	srv.SetPerformanceConfig(performanceConfig)
	srv.SetQueryFilterConfig(queryFilterConfig)

	// Graceful shutdown on SIGINT/SIGTERM
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// Select proxy mode
	mode := strings.ToLower(strings.TrimSpace(*proxyMode))
	switch mode {
	case "raw":
		logger.Info("Starting in RAW proxy mode (high-performance protocol forwarding)")
		rawSrv := server.NewRawProxyServer(logger)
		rawSrv.SetPoolConfig(poolConfig)
		rawSrv.SetPerformanceConfig(performanceConfig)
		rawSrv.SetQueryFilterConfig(queryFilterConfig)

		go func() {
			sig := <-sigCh
			logger.Info("Received shutdown signal", zap.String("signal", sig.String()))
			cancel()
			rawSrv.Close(ctx)
		}()

		// Start health check endpoint
		if *healthPort > 0 {
			go startRawHealthCheck(*healthPort, logger, rawSrv)
		}

		if err := rawSrv.ListenAndServe(ctx, *listenAddress, *primaryAddress, *mirrorsAddresses, tlsConfig); err != nil {
			logger.Fatal("Server failed", zap.Error(err))
		}

	default: // "wire" or unrecognized defaults to wire mode
		if mode != "wire" && mode != "" {
			logger.Warn("Unknown proxy mode, defaulting to 'wire'", zap.String("mode", mode))
		}
		logger.Info("Starting in WIRE proxy mode (psql-wire, full protocol decode)")

		go func() {
			sig := <-sigCh
			logger.Info("Received shutdown signal", zap.String("signal", sig.String()))
			cancel()
			srv.Close(ctx)
		}()

		// Start health check endpoint (with server reference for liveness checks)
		if *healthPort > 0 {
			go startHealthCheck(*healthPort, logger, srv)
		}

		if err := srv.ListenAndServe(ctx, *listenAddress, *primaryAddress, *mirrorsAddresses, tlsConfig); err != nil {
			logger.Fatal("Server failed", zap.Error(err))
		}
	}
}

// maskConnectionString masks the password in a connection string for safe logging
func maskConnectionString(connStr string) string {
	u, err := url.Parse(connStr)
	if err != nil {
		if strings.Contains(connStr, "password=") {
			parts := strings.Split(connStr, " ")
			for i, part := range parts {
				if strings.HasPrefix(part, "password=") {
					parts[i] = "password=***"
				}
			}
			return strings.Join(parts, " ")
		}
		return connStr
	}
	if u.User != nil {
		u.User = url.UserPassword(u.User.Username(), "***")
	}
	return u.String()
}

func configureTLS(certFile, keyFile, caFile, serverName string, skipVerify bool) (*tls.Config, error) {
	tlsConfig := &tls.Config{
		InsecureSkipVerify: skipVerify,
		ServerName:         serverName,
		MinVersion:         tls.VersionTLS12,
	}

	if certFile != "" && keyFile != "" {
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load server certificate: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	if caFile != "" {
		caCert, err := os.ReadFile(caFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read CA certificate: %w", err)
		}

		caCertPool := x509.NewCertPool()
		if !caCertPool.AppendCertsFromPEM(caCert) {
			return nil, fmt.Errorf("failed to parse CA certificate")
		}

		tlsConfig.RootCAs = caCertPool
		tlsConfig.ClientCAs = caCertPool
	}

	return tlsConfig, nil
}

func configureLogger() (*zap.Logger, error) {
	logFormat := os.Getenv("FRENZY_LOG_FORMAT") // "json" or "console" (default)
	logFile := os.Getenv("FRENZY_LOG_FILE")     // optional file path
	logLevel := os.Getenv("FRENZY_LOG_LEVEL")

	// Log rotation settings (via environment variables)
	logMaxSize := getEnvInt("FRENZY_LOG_MAX_SIZE", 100)      // max MB per file
	logMaxBackups := getEnvInt("FRENZY_LOG_MAX_BACKUPS", 7)  // max number of old files
	logMaxAge := getEnvInt("FRENZY_LOG_MAX_AGE", 30)         // max days to retain
	logCompress := os.Getenv("FRENZY_LOG_COMPRESS") != "false" // compress rotated files

	// Determine log level
	var level zapcore.Level
	switch strings.ToLower(logLevel) {
	case "debug":
		level = zap.DebugLevel
	case "info":
		level = zap.InfoLevel
	case "warn", "warning":
		level = zap.WarnLevel
	case "error":
		level = zap.ErrorLevel
	default:
		level = zap.InfoLevel
	}

	// Build encoder
	var encoder zapcore.Encoder
	if logFormat == "json" {
		encoderConfig := zap.NewProductionEncoderConfig()
		encoderConfig.TimeKey = "timestamp"
		encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
		encoder = zapcore.NewJSONEncoder(encoderConfig)
	} else {
		encoderConfig := zap.NewDevelopmentEncoderConfig()
		encoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
		encoder = zapcore.NewConsoleEncoder(encoderConfig)
	}

	// Build cores: always write to stdout
	cores := []zapcore.Core{
		zapcore.NewCore(encoder, zapcore.AddSync(os.Stdout), level),
	}

	// If log file is specified, add rotating file writer
	if logFile != "" {
		fileWriter := &lumberjack.Logger{
			Filename:   logFile,
			MaxSize:    logMaxSize,    // megabytes
			MaxBackups: logMaxBackups, // number of old files
			MaxAge:     logMaxAge,     // days
			Compress:   logCompress,   // gzip rotated files
			LocalTime:  true,
		}

		// Use JSON encoder for file output regardless of console format
		fileEncoderConfig := zap.NewProductionEncoderConfig()
		fileEncoderConfig.TimeKey = "timestamp"
		fileEncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
		fileEncoder := zapcore.NewJSONEncoder(fileEncoderConfig)

		cores = append(cores, zapcore.NewCore(
			fileEncoder,
			zapcore.AddSync(fileWriter),
			level,
		))
	}

	// Combine cores
	core := zapcore.NewTee(cores...)
	logger := zap.New(core, zap.AddCaller())
	return logger.Named("frenzy"), nil
}

// getEnvInt reads an integer from environment variable with a default value.
func getEnvInt(key string, defaultVal int) int {
	val := os.Getenv(key)
	if val == "" {
		return defaultVal
	}
	n, err := strconv.Atoi(val)
	if err != nil {
		return defaultVal
	}
	return n
}

// startHealthCheck starts an HTTP health check endpoint bound to localhost only
// startHealthCheck starts an HTTP health check endpoint bound to localhost only
func startHealthCheck(port int, logger *zap.Logger, srv *server.ProxyServerV15) {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		// Check if primary is reachable
		healthy := true
		details := "ok"
		if srv != nil {
			if err := srv.PingPrimary(); err != nil {
				healthy = false
				details = fmt.Sprintf("primary unreachable: %v", err)
			}
		}

		if healthy {
			w.WriteHeader(http.StatusOK)
			fmt.Fprintf(w, `{"status":"ok","mode":"wire","version":"%s","uptime":"%s"}`, version, time.Since(startTime).String())
		} else {
			w.WriteHeader(http.StatusServiceUnavailable)
			fmt.Fprintf(w, `{"status":"unhealthy","detail":"%s","version":"%s","uptime":"%s"}`, details, version, time.Since(startTime).String())
		}
	})
	mux.HandleFunc("/version", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"version":"%s","build_time":"%s"}`, version, buildTime)
	})

	// Register pprof handlers for profiling (use DefaultServeMux which has pprof registered)
	mux.Handle("/debug/pprof/", http.DefaultServeMux)

	addr := fmt.Sprintf("127.0.0.1:%d", port)
	logger.Info("Health check endpoint started (with pprof)", zap.String("address", addr))
	if err := http.ListenAndServe(addr, mux); err != nil {
		logger.Error("Health check endpoint failed", zap.Error(err))
	}
}

// startRawHealthCheck starts an HTTP health check endpoint for the raw proxy server.
func startRawHealthCheck(port int, logger *zap.Logger, srv *server.RawProxyServer) {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		healthy := true
		details := "ok"
		if srv != nil {
			if err := srv.PingPrimary(); err != nil {
				healthy = false
				details = fmt.Sprintf("primary unreachable: %v", err)
			}
		}

		if healthy {
			w.WriteHeader(http.StatusOK)
			fmt.Fprintf(w, `{"status":"ok","mode":"raw","version":"%s","uptime":"%s"}`, version, time.Since(startTime).String())
		} else {
			w.WriteHeader(http.StatusServiceUnavailable)
			fmt.Fprintf(w, `{"status":"unhealthy","detail":"%s","version":"%s","uptime":"%s"}`, details, version, time.Since(startTime).String())
		}
	})
	mux.HandleFunc("/version", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"version":"%s","build_time":"%s","mode":"raw"}`, version, buildTime)
	})

	// Prometheus metrics endpoint
	mux.Handle("/metrics", promhttp.Handler())

	mux.Handle("/debug/pprof/", http.DefaultServeMux)

	addr := fmt.Sprintf("127.0.0.1:%d", port)
	logger.Info("Health check endpoint started (raw mode, with pprof + metrics)", zap.String("address", addr))
	if err := http.ListenAndServe(addr, mux); err != nil {
		logger.Error("Health check endpoint failed", zap.Error(err))
	}
}

// runWithConfig starts the server using a YAML configuration file.
func runWithConfig(configPath string) {
	logger, err := configureLogger()
	if err != nil {
		log.Fatal(err)
	}
	defer logger.Sync()

	cfg, err := loadConfig(configPath)
	if err != nil {
		logger.Fatal("Failed to load config", zap.String("path", configPath), zap.Error(err))
	}

	// Resolve connection URLs with passwords from env
	primaryURL := resolveConnectionURL(cfg.Primary.URL, cfg.Primary.PasswordEnv)
	var mirrorURLs []string
	for _, m := range cfg.Mirrors {
		mirrorURLs = append(mirrorURLs, resolveConnectionURL(m.URL, m.PasswordEnv))
	}

	// TLS
	var tlsConfig *tls.Config
	if cfg.TLS.Enabled {
		tlsConfig, err = configureTLS(cfg.TLS.CertFile, cfg.TLS.KeyFile, cfg.TLS.CAFile, cfg.TLS.ServerName, cfg.TLS.SkipVerify)
		if err != nil {
			logger.Fatal("Failed to configure TLS", zap.Error(err))
		}
	}

	// Pool config
	poolConfig := &server.PoolConfig{
		MaxConns:        int32(cfg.Pool.MaxConns),
		MinConns:        int32(cfg.Pool.MinConns),
		MaxConnLifetime: time.Duration(cfg.Pool.MaxConnLifetime) * time.Second,
		MaxConnIdleTime: time.Duration(cfg.Pool.MaxConnIdleTime) * time.Second,
	}
	if poolConfig.MaxConns == 0 {
		poolConfig.MaxConns = 10
	}
	if poolConfig.MinConns == 0 {
		poolConfig.MinConns = 2
	}
	if poolConfig.MaxConnIdleTime == 0 {
		poolConfig.MaxConnIdleTime = 300 * time.Second
	}

	// Performance config
	perfConfig := &server.PerformanceConfig{
		WorkerThreads:     cfg.Performance.WorkerThreads,
		QueryBufferSize:   cfg.Performance.QueryBufferSize,
		AsyncMirrors:      cfg.Performance.AsyncMirrors,
		MirrorTimeoutSecs: cfg.Performance.MirrorTimeout,
		MirrorRetries:     cfg.Performance.MirrorRetries,
		RetryDelaySecs:    cfg.Performance.RetryDelay,
	}
	if perfConfig.QueryBufferSize == 0 {
		perfConfig.QueryBufferSize = 8192
	}
	if perfConfig.MirrorTimeoutSecs == 0 {
		perfConfig.MirrorTimeoutSecs = 120
	}
	if perfConfig.WorkerThreads == 0 {
		perfConfig.WorkerThreads = runtime.NumCPU()
	}

	// Query filter config
	filterConfig := &server.QueryFilterConfig{
		MirrorAllQueries:    cfg.Filter.MirrorAllQueries,
		MirrorSelectQueries: cfg.Filter.MirrorSelectQueries,
		MirrorDdlOnly:       cfg.Filter.MirrorDdlOnly,
		MirrorDmlOnly:       cfg.Filter.MirrorDmlOnly,
		SkipRollbackMirror:  cfg.Filter.SkipRollbackMirror,
		SkipFailedTxMirror:  cfg.Filter.SkipFailedTxMirror,
		SkipMirrorTxLocks:   cfg.Filter.SkipMirrorTxLocks,
	}

	logger.Info("Starting frenzy with config file",
		zap.String("config", configPath),
		zap.String("listen", cfg.Listen),
		zap.String("mode", cfg.Mode),
		zap.Int("mirrors", len(mirrorURLs)))

	// PID file
	if cfg.Service.PIDFile != "" {
		pid := os.Getpid()
		if err := os.WriteFile(cfg.Service.PIDFile, []byte(strconv.Itoa(pid)+"\n"), 0600); err != nil {
			logger.Fatal("Failed to write PID file", zap.Error(err))
		}
		defer os.Remove(cfg.Service.PIDFile)
	}

	// Graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// Select proxy mode
	mode := strings.ToLower(strings.TrimSpace(cfg.Mode))
	switch mode {
	case "raw":
		logger.Info("Starting in RAW proxy mode (high-performance protocol forwarding)")
		rawSrv := server.NewRawProxyServer(logger)
		rawSrv.SetPoolConfig(poolConfig)
		rawSrv.SetPerformanceConfig(perfConfig)
		rawSrv.SetQueryFilterConfig(filterConfig)

		go func() {
			sig := <-sigCh
			logger.Info("Received shutdown signal", zap.String("signal", sig.String()))
			cancel()
			rawSrv.Close(ctx)
		}()

		if cfg.Service.HealthPort > 0 {
			go startRawHealthCheck(cfg.Service.HealthPort, logger, rawSrv)
		}

		if err := rawSrv.ListenAndServe(ctx, cfg.Listen, primaryURL, mirrorURLs, tlsConfig); err != nil {
			logger.Fatal("Server failed", zap.Error(err))
		}

	default: // "wire" or unrecognized defaults to wire mode
		if mode != "wire" && mode != "" {
			logger.Warn("Unknown proxy mode, defaulting to 'wire'", zap.String("mode", mode))
		}
		logger.Info("Starting in WIRE proxy mode (psql-wire, full protocol decode)")

		srv := server.NewProxyServerV15(logger)
		srv.SetPoolConfig(poolConfig)
		srv.SetPerformanceConfig(perfConfig)
		srv.SetQueryFilterConfig(filterConfig)

		go func() {
			sig := <-sigCh
			logger.Info("Received shutdown signal", zap.String("signal", sig.String()))
			cancel()
			srv.Close(ctx)
		}()

		if cfg.Service.HealthPort > 0 {
			go startHealthCheck(cfg.Service.HealthPort, logger, srv)
		}

		if err := srv.ListenAndServe(ctx, cfg.Listen, primaryURL, mirrorURLs, tlsConfig); err != nil {
			logger.Fatal("Server failed", zap.Error(err))
		}
	}
}

var startTime = time.Now()
