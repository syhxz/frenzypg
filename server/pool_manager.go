package server

import (
	"context"
	"strings"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
)

// PoolManager manages separate connection pools for different workload types
type PoolManager struct {
	logger      *zap.Logger
	readPool    *pgxpool.Pool
	writePool   *pgxpool.Pool
	mirrorPools []*pgxpool.Pool
	mutex       sync.RWMutex
}

// NewPoolManager creates a new pool manager with separate read/write pools
func NewPoolManager(logger *zap.Logger, poolConfig *PoolConfig) *PoolManager {
	return &PoolManager{
		logger:      logger,
		mirrorPools: make([]*pgxpool.Pool, 0),
	}
}

// CreateReadPool creates a connection pool optimized for read operations
func (pm *PoolManager) CreateReadPool(ctx context.Context, connectionString string, poolConfig *PoolConfig) error {
	config, err := pgxpool.ParseConfig(connectionString)
	if err != nil {
		return err
	}

	// Optimize for read operations
	config.MaxConns = poolConfig.MaxConns
	config.MinConns = poolConfig.MinConns / 2 // Fewer min connections for reads
	config.MaxConnLifetime = poolConfig.MaxConnLifetime
	config.MaxConnIdleTime = poolConfig.MaxConnIdleTime

	pm.readPool, err = pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return err
	}

	pm.logger.Info("Read pool created",
		zap.Int32("max_conns", config.MaxConns),
		zap.Int32("min_conns", config.MinConns))

	return nil
}

// CreateWritePool creates a connection pool optimized for write operations
func (pm *PoolManager) CreateWritePool(ctx context.Context, connectionString string, poolConfig *PoolConfig) error {
	config, err := pgxpool.ParseConfig(connectionString)
	if err != nil {
		return err
	}

	// Optimize for write operations
	config.MaxConns = poolConfig.MaxConns / 2 // Fewer max connections for writes
	config.MinConns = poolConfig.MinConns     // More min connections for writes
	config.MaxConnLifetime = poolConfig.MaxConnLifetime
	config.MaxConnIdleTime = poolConfig.MaxConnIdleTime / 2 // Shorter idle time for writes

	pm.writePool, err = pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return err
	}

	pm.logger.Info("Write pool created",
		zap.Int32("max_conns", config.MaxConns),
		zap.Int32("min_conns", config.MinConns))

	return nil
}

// AddMirrorPool adds a connection pool for a mirror database
func (pm *PoolManager) AddMirrorPool(ctx context.Context, connectionString string, poolConfig *PoolConfig) error {
	config, err := pgxpool.ParseConfig(connectionString)
	if err != nil {
		return err
	}

	// Optimize for mirror operations (can be more relaxed)
	config.MaxConns = poolConfig.MaxConns / 4 // Fewer connections for mirrors
	config.MinConns = 1                       // Minimal connections for mirrors
	config.MaxConnLifetime = poolConfig.MaxConnLifetime
	config.MaxConnIdleTime = poolConfig.MaxConnIdleTime * 2 // Longer idle time for mirrors

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return err
	}

	pm.mutex.Lock()
	pm.mirrorPools = append(pm.mirrorPools, pool)
	pm.mutex.Unlock()

	pm.logger.Info("Mirror pool created",
		zap.Int("mirror_index", len(pm.mirrorPools)-1),
		zap.Int32("max_conns", config.MaxConns),
		zap.Int32("min_conns", config.MinConns))

	return nil
}

// GetPool returns the appropriate pool based on query type
func (pm *PoolManager) GetPool(query string) *pgxpool.Pool {
	if pm.isReadQuery(query) {
		return pm.readPool
	}
	return pm.writePool
}

// GetMirrorPools returns all mirror pools
func (pm *PoolManager) GetMirrorPools() []*pgxpool.Pool {
	pm.mutex.RLock()
	defer pm.mutex.RUnlock()
	
	pools := make([]*pgxpool.Pool, len(pm.mirrorPools))
	copy(pools, pm.mirrorPools)
	return pools
}

// isReadQuery determines if a query is read-only
func (pm *PoolManager) isReadQuery(query string) bool {
	query = strings.TrimSpace(strings.ToUpper(query))
	
	// Read-only query patterns
	readPatterns := []string{
		"SELECT",
		"WITH", // CTE that starts with SELECT
		"SHOW",
		"EXPLAIN",
		"DESCRIBE",
		"\\", // psql commands
	}
	
	for _, pattern := range readPatterns {
		if strings.HasPrefix(query, pattern) {
			return true
		}
	}
	
	return false
}

// Close closes all connection pools
func (pm *PoolManager) Close() {
	if pm.readPool != nil {
		pm.readPool.Close()
		pm.logger.Info("Read pool closed")
	}
	
	if pm.writePool != nil {
		pm.writePool.Close()
		pm.logger.Info("Write pool closed")
	}
	
	pm.mutex.Lock()
	for i, pool := range pm.mirrorPools {
		pool.Close()
		pm.logger.Info("Mirror pool closed", zap.Int("mirror_index", i))
	}
	pm.mirrorPools = nil
	pm.mutex.Unlock()
}

// Stats returns connection pool statistics
func (pm *PoolManager) Stats() map[string]interface{} {
	stats := make(map[string]interface{})
	
	if pm.readPool != nil {
		readStats := pm.readPool.Stat()
		stats["read_pool"] = map[string]interface{}{
			"acquired_conns":     readStats.AcquiredConns(),
			"constructing_conns": readStats.ConstructingConns(),
			"idle_conns":         readStats.IdleConns(),
			"max_conns":          readStats.MaxConns(),
			"total_conns":        readStats.TotalConns(),
		}
	}
	
	if pm.writePool != nil {
		writeStats := pm.writePool.Stat()
		stats["write_pool"] = map[string]interface{}{
			"acquired_conns":     writeStats.AcquiredConns(),
			"constructing_conns": writeStats.ConstructingConns(),
			"idle_conns":         writeStats.IdleConns(),
			"max_conns":          writeStats.MaxConns(),
			"total_conns":        writeStats.TotalConns(),
		}
	}
	
	pm.mutex.RLock()
	mirrorStats := make([]map[string]interface{}, len(pm.mirrorPools))
	for i, pool := range pm.mirrorPools {
		poolStats := pool.Stat()
		mirrorStats[i] = map[string]interface{}{
			"acquired_conns":     poolStats.AcquiredConns(),
			"constructing_conns": poolStats.ConstructingConns(),
			"idle_conns":         poolStats.IdleConns(),
			"max_conns":          poolStats.MaxConns(),
			"total_conns":        poolStats.TotalConns(),
		}
	}
	pm.mutex.RUnlock()
	
	stats["mirror_pools"] = mirrorStats
	
	return stats
}
