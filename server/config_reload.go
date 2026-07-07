package server

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/yaml.v3"
)

// ReloadableConfig contains configuration fields that can be changed at runtime.
type ReloadableConfig struct {
	MaxConnections  int
	MirrorTimeout   int
	MirrorRetries   int
	RetryDelay      int
	LogLevel        string
	QueryFilter     QueryFilterConfig
}

// ConfigReloader watches for SIGHUP and reloads configuration.
type ConfigReloader struct {
	configPath string
	logger     *zap.Logger
	stopChan   chan struct{}
	onReload   func(*ReloadableConfig)
}

func NewConfigReloader(configPath string, logger *zap.Logger, onReload func(*ReloadableConfig)) *ConfigReloader {
	return &ConfigReloader{
		configPath: configPath,
		logger:     logger,
		stopChan:   make(chan struct{}),
		onReload:   onReload,
	}
}

// Start begins listening for SIGHUP signals.
func (cr *ConfigReloader) Start() {
	if cr.configPath == "" {
		cr.logger.Info("Config reload disabled (no config file path)")
		return
	}

	go cr.run()
	cr.logger.Info("Config hot-reload enabled (send SIGHUP to reload)",
		zap.String("config", cr.configPath))
}

// Stop halts the config reloader.
func (cr *ConfigReloader) Stop() {
	close(cr.stopChan)
}

func (cr *ConfigReloader) run() {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGHUP)
	defer signal.Stop(sigCh)

	for {
		select {
		case <-sigCh:
			cr.logger.Info("SIGHUP received, reloading configuration",
				zap.String("config", cr.configPath))
			cr.reload()
		case <-cr.stopChan:
			return
		}
	}
}

func (cr *ConfigReloader) reload() {
	data, err := os.ReadFile(cr.configPath)
	if err != nil {
		cr.logger.Error("Failed to read config file for reload",
			zap.String("path", cr.configPath),
			zap.Error(err))
		return
	}

	// Safe env expansion with allowlist
	expanded := os.Expand(string(data), safeEnvExpandServer)

	var raw reloadConfigRaw
	if err := yaml.Unmarshal([]byte(expanded), &raw); err != nil {
		cr.logger.Error("Failed to parse config file for reload",
			zap.String("path", cr.configPath),
			zap.Error(err))
		return
	}

	reloadable := &ReloadableConfig{
		MaxConnections: raw.Pool.MaxConns,
		MirrorTimeout:  raw.Performance.MirrorTimeout,
		MirrorRetries:  raw.Performance.MirrorRetries,
		RetryDelay:     raw.Performance.RetryDelay,
		LogLevel:       raw.LogLevel,
		QueryFilter: QueryFilterConfig{
			MirrorAllQueries:    raw.Filter.MirrorAllQueries,
			MirrorSelectQueries: raw.Filter.MirrorSelectQueries,
			MirrorDdlOnly:       raw.Filter.MirrorDdlOnly,
			MirrorDmlOnly:       raw.Filter.MirrorDmlOnly,
			SkipRollbackMirror:  raw.Filter.SkipRollbackMirror,
			SkipFailedTxMirror:  raw.Filter.SkipFailedTxMirror,
			SkipMirrorTxLocks:   raw.Filter.SkipMirrorTxLocks,
		},
	}

	cr.logger.Info("Configuration reloaded successfully",
		zap.Int("max_connections", reloadable.MaxConnections),
		zap.Int("mirror_timeout", reloadable.MirrorTimeout),
		zap.String("log_level", reloadable.LogLevel))

	if cr.onReload != nil {
		cr.onReload(reloadable)
	}
}

// reloadConfigRaw is a minimal YAML structure for reloadable config fields.
type reloadConfigRaw struct {
	LogLevel string `yaml:"log_level"`

	Pool struct {
		MaxConns int `yaml:"max_conns"`
	} `yaml:"pool"`

	Performance struct {
		MirrorTimeout int `yaml:"mirror_timeout"`
		MirrorRetries int `yaml:"mirror_retries"`
		RetryDelay    int `yaml:"retry_delay"`
	} `yaml:"performance"`

	Filter struct {
		MirrorAllQueries    bool `yaml:"mirror_all_queries"`
		MirrorSelectQueries bool `yaml:"mirror_select_queries"`
		MirrorDdlOnly       bool `yaml:"mirror_ddl_only"`
		MirrorDmlOnly       bool `yaml:"mirror_dml_only"`
		SkipRollbackMirror  bool `yaml:"skip_rollback_mirror"`
		SkipFailedTxMirror  bool `yaml:"skip_failed_tx_mirror"`
		SkipMirrorTxLocks   bool `yaml:"skip_mirror_tx_locks"`
	} `yaml:"filter"`
}

// safeEnvExpandServer is a server-package version of safe env expansion.
func safeEnvExpandServer(key string) string {
	allowedPrefixes := []string{"FRENZY_", "PG_", "DATABASE_", "DB_"}
	for _, prefix := range allowedPrefixes {
		if strings.HasPrefix(key, prefix) {
			return os.Getenv(key)
		}
	}
	return ""
}

// ParseLogLevel converts a string log level to zapcore.Level.
func ParseLogLevel(level string) zapcore.Level {
	switch strings.ToLower(level) {
	case "debug":
		return zap.DebugLevel
	case "info":
		return zap.InfoLevel
	case "warn", "warning":
		return zap.WarnLevel
	case "error":
		return zap.ErrorLevel
	default:
		return zap.InfoLevel
	}
}

// --- Admin Web Panel ---

// AdminPanel provides a simple HTTP admin interface for runtime status.
type AdminPanel struct {
	server *ProxyServerV15
	logger *zap.Logger
}

func NewAdminPanel(server *ProxyServerV15, logger *zap.Logger) *AdminPanel {
	return &AdminPanel{server: server, logger: logger}
}

// Start starts the admin panel HTTP server.
func (ap *AdminPanel) Start(addr string) {
	mux := ap.createMux()
	ap.logger.Info("Admin panel started", zap.String("address", addr))
	go func() {
		if err := (&http.Server{Addr: addr, Handler: mux}).ListenAndServe(); err != nil {
			ap.logger.Error("Admin panel failed", zap.Error(err))
		}
	}()
}

func (ap *AdminPanel) createMux() *http.ServeMux {
	mux := http.NewServeMux()

	mux.HandleFunc("/admin/status", ap.handleStatus)
	mux.HandleFunc("/admin/buffers", ap.handleBuffers)
	mux.HandleFunc("/admin/mirrors", ap.handleMirrors)
	mux.HandleFunc("/admin/config", ap.handleConfig)

	return mux
}

func (ap *AdminPanel) handleStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	stats := ap.server.GetBufferStats()

	// Get pool stats
	primaryPool := "disconnected"
	if ap.server.primary != nil && ap.server.primary.pool != nil {
		stat := ap.server.primary.pool.Stat()
		primaryPool = fmt.Sprintf("total=%d idle=%d acquired=%d", stat.TotalConns(), stat.IdleConns(), stat.AcquiredConns())
	}

	fmt.Fprintf(w, `{"primary_pool":"%s","active_transactions":%d,"buffered_queries":%d,"buffered_bytes":%d,"mirrors":%d}`,
		primaryPool,
		stats["active_transactions"],
		stats["total_buffered_queries"],
		stats["total_buffered_bytes"],
		len(ap.server.mirrors))
}

func (ap *AdminPanel) handleBuffers(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	stats := ap.server.GetBufferStats()
	fmt.Fprintf(w, `{"active_transactions":%d,"total_buffered_queries":%d,"total_buffered_bytes":%d}`,
		stats["active_transactions"],
		stats["total_buffered_queries"],
		stats["total_buffered_bytes"])
}

func (ap *AdminPanel) handleMirrors(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte("["))
	for i, mirror := range ap.server.mirrors {
		if i > 0 {
			w.Write([]byte(","))
		}
		status := "disconnected"
		poolInfo := ""
		if mirror != nil && mirror.pool != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			err := mirror.pool.Ping(ctx)
			cancel()
			if err == nil {
				status = "healthy"
			} else {
				status = "unhealthy"
			}
			stat := mirror.pool.Stat()
			poolInfo = fmt.Sprintf(",\"total_conns\":%d,\"idle_conns\":%d", stat.TotalConns(), stat.IdleConns())
		}

		cbState := "unknown"
		if ap.server.mirrorBreakers != nil && ap.server.mirrorBreakers.Allow(i) {
			cbState = "closed"
		} else if ap.server.mirrorBreakers != nil {
			cbState = "open"
		}

		fmt.Fprintf(w, `{"index":%d,"status":"%s","circuit_breaker":"%s"%s}`, i, status, cbState, poolInfo)
	}
	w.Write([]byte("]"))
}

func (ap *AdminPanel) handleConfig(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	cfg := ap.server.getQueryFilterConfig()
	fmt.Fprintf(w, `{"mirror_all_queries":%t,"mirror_select_queries":%t,"mirror_ddl_only":%t,"mirror_dml_only":%t,"skip_rollback_mirror":%t,"skip_mirror_tx_locks":%t}`,
		cfg.MirrorAllQueries, cfg.MirrorSelectQueries, cfg.MirrorDdlOnly, cfg.MirrorDmlOnly, cfg.SkipRollbackMirror, cfg.SkipMirrorTxLocks)
}
