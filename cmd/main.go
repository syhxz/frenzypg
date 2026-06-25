package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log"
	"net/url"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/akamensky/argparse"
	"github.com/syhxz/frenzypg/server"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func main() {
	logger, err := configureLogger()
	if err != nil {
		log.Fatal(err)
	}
	defer logger.Sync()

	parser := argparse.NewParser("print", "Frenzy Mirroring Postgres Proxy")
	listenAddress := parser.String("l", "listen", &argparse.Options{Required: true, Help: "Listening port."})
	primaryAddress := parser.String("p", "primary", &argparse.Options{Required: true, Help: "Primary Postgres connection string."})
	var mirrorsAddresses *[]string = parser.StringList("m", "mirror", &argparse.Options{Required: true, Help: "Mirror Postgres connection string."})

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

	// Parse input
	err = parser.Parse(os.Args)
	if err != nil {
		fmt.Print(parser.Usage(err))
		fmt.Printf("EXAMPLE\nfrenzy --listen :5432 --primary postgresql://postgres:password@localhost:5441/postgres --mirror postgresql://postgres:password@localhost:5442/postgres\n")
		fmt.Printf("WITH TLS\nfrenzy --listen :5432 --enable-tls --tls-cert server.crt --tls-key server.key --tls-ca ca.pem --primary postgresql://postgres:password@localhost:5441/postgres --mirror postgresql://postgres:password@localhost:5442/postgres\n")
		os.Exit(1)
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
	go func() {
		sig := <-sigCh
		logger.Info("Received shutdown signal", zap.String("signal", sig.String()))
		srv.Close(ctx)
		cancel()
		os.Exit(0)
	}()

	if err := srv.ListenAndServe(ctx, *listenAddress, *primaryAddress, *mirrorsAddresses, tlsConfig); err != nil {
		logger.Fatal("Server failed", zap.Error(err))
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
	config := zap.NewDevelopmentConfig()

	logLevel := os.Getenv("FRENZY_LOG_LEVEL")
	switch strings.ToLower(logLevel) {
	case "debug":
		config.Level = zap.NewAtomicLevelAt(zap.DebugLevel)
	case "info":
		config.Level = zap.NewAtomicLevelAt(zap.InfoLevel)
	case "warn", "warning":
		config.Level = zap.NewAtomicLevelAt(zap.WarnLevel)
	case "error":
		config.Level = zap.NewAtomicLevelAt(zap.ErrorLevel)
	default:
		config.Level = zap.NewAtomicLevelAt(zap.InfoLevel)
	}

	config.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	logger, err := config.Build()
	return logger.Named("frenzy"), err
}
