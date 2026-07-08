package server

import (
	"fmt"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

// Metrics holds all Prometheus metric collectors for the proxy.
type Metrics struct {
	// Counters
	QueriesTotal       *prometheus.CounterVec
	MirrorQueriesTotal prometheus.Counter
	MirrorFailures     prometheus.Counter
	MirrorRetries      prometheus.Counter

	// Gauges
	ActiveConnections  prometheus.Gauge
	PoolSize           *prometheus.GaugeVec
	ActiveTransactions prometheus.Gauge
	TxBufferCount      prometheus.Gauge
	TxBufferBytes      prometheus.Gauge
	CircuitBreakerState *prometheus.GaugeVec

	// Histograms
	QueryDuration *prometheus.HistogramVec
	MirrorLag     prometheus.Histogram
}

// NewMetrics creates and registers all Prometheus metrics.
func NewMetrics(namespace string) *Metrics {
	m := &Metrics{
		QueriesTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "queries_total",
			Help:      "Total number of queries processed",
		}, []string{"type"}),

		MirrorQueriesTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "mirror_queries_total",
			Help:      "Total number of queries sent to mirrors",
		}),

		MirrorFailures: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "mirror_failures_total",
			Help:      "Total number of mirror query failures",
		}),

		MirrorRetries: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "mirror_retries_total",
			Help:      "Total number of mirror query retries",
		}),

		ActiveConnections: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "active_connections",
			Help:      "Number of active client connections",
		}),

		PoolSize: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "pool_connections",
			Help:      "Connection pool stats",
		}, []string{"target", "state"}),

		ActiveTransactions: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "active_transactions",
			Help:      "Number of active transaction buffers",
		}),

		TxBufferCount: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "tx_buffer_queries",
			Help:      "Total buffered queries across all transactions",
		}),

		TxBufferBytes: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "tx_buffer_bytes",
			Help:      "Total bytes buffered across all transactions",
		}),

		CircuitBreakerState: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "circuit_breaker_state",
			Help:      "Circuit breaker state (0=closed, 1=open, 2=half-open)",
		}, []string{"mirror"}),

		QueryDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "query_duration_seconds",
			Help:      "Query execution duration in seconds",
			Buckets:   []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
		}, []string{"target"}),

		MirrorLag: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "mirror_lag_seconds",
			Help:      "Mirror replication lag in seconds",
			Buckets:   []float64{.01, .05, .1, .25, .5, 1, 2, 5, 10, 30},
		}),
	}

	// Use Register instead of MustRegister to avoid panic on duplicate registration
	// (e.g., in tests or if NewMetrics is called multiple times)
	collectors := []prometheus.Collector{
		m.QueriesTotal,
		m.MirrorQueriesTotal,
		m.MirrorFailures,
		m.MirrorRetries,
		m.ActiveConnections,
		m.PoolSize,
		m.ActiveTransactions,
		m.TxBufferCount,
		m.TxBufferBytes,
		m.CircuitBreakerState,
		m.QueryDuration,
		m.MirrorLag,
	}
	for _, c := range collectors {
		_ = prometheus.Register(c) // ignore already-registered errors
	}

	return m
}

// RecordQuery increments the query counter by type.
func (m *Metrics) RecordQuery(queryType string) {
	m.QueriesTotal.WithLabelValues(queryType).Inc()
}

// RecordMirrorQuery increments the mirror query counter.
func (m *Metrics) RecordMirrorQuery() {
	m.MirrorQueriesTotal.Inc()
}

// RecordMirrorFailure increments the mirror failure counter.
func (m *Metrics) RecordMirrorFailure() {
	m.MirrorFailures.Inc()
}

// RecordMirrorRetry increments the mirror retry counter.
func (m *Metrics) RecordMirrorRetry() {
	m.MirrorRetries.Inc()
}

// ObserveQueryDuration records query execution time.
func (m *Metrics) ObserveQueryDuration(target string, duration time.Duration) {
	m.QueryDuration.WithLabelValues(target).Observe(duration.Seconds())
}

// UpdatePoolStats updates connection pool gauges.
func (m *Metrics) UpdatePoolStats(target string, pool *pgxpool.Pool) {
	if pool == nil {
		return
	}
	stat := pool.Stat()
	m.PoolSize.WithLabelValues(target, "total").Set(float64(stat.TotalConns()))
	m.PoolSize.WithLabelValues(target, "idle").Set(float64(stat.IdleConns()))
	m.PoolSize.WithLabelValues(target, "acquired").Set(float64(stat.AcquiredConns()))
}

// UpdateBufferStats updates transaction buffer gauges.
func (m *Metrics) UpdateBufferStats(count int, bytes int64) {
	m.ActiveTransactions.Set(float64(count))
	m.TxBufferBytes.Set(float64(bytes))
}

// UpdateCircuitState updates circuit breaker state gauge.
func (m *Metrics) UpdateCircuitState(mirrorIndex int, state int32) {
	m.CircuitBreakerState.WithLabelValues(fmt.Sprintf("mirror_%d", mirrorIndex)).Set(float64(state))
}

// StartMetricsServer starts the Prometheus metrics HTTP server.
func StartMetricsServer(addr string, logger *zap.Logger) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	logger.Info("Metrics server started", zap.String("address", addr))
	if err := http.ListenAndServe(addr, mux); err != nil {
		logger.Error("Metrics server failed", zap.Error(err))
	}
}

// --- Connection Limiter ---

// ConnectionLimiter limits the maximum number of concurrent client connections.
type ConnectionLimiter struct {
	maxConnections    atomic.Int64
	activeConnections atomic.Int64
	logger            *zap.Logger
}

func NewConnectionLimiter(maxConnections int, logger *zap.Logger) *ConnectionLimiter {
	cl := &ConnectionLimiter{logger: logger}
	cl.maxConnections.Store(int64(maxConnections))
	return cl
}

// Allow checks if a new connection is allowed. Returns true and increments if under limit.
// Uses CAS loop to prevent TOCTOU race under concurrent connection storms.
func (cl *ConnectionLimiter) Allow() bool {
	max := cl.maxConnections.Load()
	if max <= 0 {
		// 0 means unlimited
		cl.activeConnections.Add(1)
		return true
	}
	for {
		current := cl.activeConnections.Load()
		if current >= max {
			cl.logger.Warn("Connection limit reached",
				zap.Int64("active", current),
				zap.Int64("max", max))
			return false
		}
		if cl.activeConnections.CompareAndSwap(current, current+1) {
			return true
		}
		// CAS failed — another goroutine incremented. Retry.
	}
}

// Release decrements the active connection count.
func (cl *ConnectionLimiter) Release() {
	cl.activeConnections.Add(-1)
}

// ActiveCount returns the current number of active connections.
func (cl *ConnectionLimiter) ActiveCount() int64 {
	return cl.activeConnections.Load()
}

// SetMaxConnections updates the maximum connection limit at runtime.
func (cl *ConnectionLimiter) SetMaxConnections(max int) {
	cl.maxConnections.Store(int64(max))
	cl.logger.Info("Connection limit updated", zap.Int("max", max))
}
