package server

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
)

// MirrorReconnectManager periodically checks mirror health and reconnects if needed.
type MirrorReconnectManager struct {
	mirrors       []*Connection
	logger        *zap.Logger
	checkInterval time.Duration
	stopChan      chan struct{}
}

func NewMirrorReconnectManager(mirrors []*Connection, logger *zap.Logger, checkInterval time.Duration) *MirrorReconnectManager {
	if checkInterval <= 0 {
		checkInterval = 30 * time.Second
	}
	return &MirrorReconnectManager{
		mirrors:       mirrors,
		logger:        logger,
		checkInterval: checkInterval,
		stopChan:      make(chan struct{}),
	}
}

// Start begins periodic health checking of all mirrors.
func (mrm *MirrorReconnectManager) Start() {
	go mrm.run()
}

// Stop halts the reconnection manager.
func (mrm *MirrorReconnectManager) Stop() {
	close(mrm.stopChan)
}

func (mrm *MirrorReconnectManager) run() {
	ticker := time.NewTicker(mrm.checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			mrm.checkAllMirrors()
		case <-mrm.stopChan:
			return
		}
	}
}

func (mrm *MirrorReconnectManager) checkAllMirrors() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for i, mirror := range mrm.mirrors {
		if mirror == nil {
			continue
		}

		if mirror.pool == nil {
			// Pool was never created or was closed — attempt reconnect
			mrm.logger.Warn("Mirror pool is nil, attempting reconnect",
				zap.Int("mirror_index", i))
			mrm.reconnect(ctx, i, mirror)
			continue
		}

		// Ping to check health
		err := mirror.pool.Ping(ctx)
		if err != nil {
			mrm.logger.Warn("Mirror ping failed, attempting reconnect",
				zap.Int("mirror_index", i),
				zap.Error(err))
			mrm.reconnect(ctx, i, mirror)
		}
	}
}

func (mrm *MirrorReconnectManager) reconnect(ctx context.Context, index int, mirror *Connection) {
	err := mirror.Reconnect(ctx)
	if err != nil {
		mrm.logger.Error("Mirror reconnect failed",
			zap.Int("mirror_index", index),
			zap.String("address", maskAddress(mirror.host)),
			zap.Error(err))
	} else {
		mrm.logger.Info("Mirror reconnected successfully",
			zap.Int("mirror_index", index))
	}
}

// Reconnect closes the existing pool and creates a new one.
// Uses atomic swap to avoid disrupting in-flight queries on the old pool.
func (connection *Connection) Reconnect(ctx context.Context) error {
	if connection.host == "" {
		return fmt.Errorf("no host address stored for reconnection")
	}

	// Parse connection config
	config, err := pgxpool.ParseConfig(connection.host)
	if err != nil {
		return fmt.Errorf("failed to parse connection string: %w", err)
	}

	// Apply pool configuration
	if connection.poolConfig != nil {
		config.MaxConns = connection.poolConfig.MaxConns
		config.MinConns = connection.poolConfig.MinConns
		config.MaxConnLifetime = connection.poolConfig.MaxConnLifetime
		config.MaxConnIdleTime = connection.poolConfig.MaxConnIdleTime
	}

	// Configure SSL
	err = connection.configurePoolSSL(config, connection.host)
	if err != nil {
		return fmt.Errorf("failed to configure SSL for reconnect: %w", err)
	}

	// Create new pool first (before touching old one)
	newPool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return fmt.Errorf("failed to create new connection pool: %w", err)
	}

	// Verify new pool works
	if err := newPool.Ping(ctx); err != nil {
		newPool.Close()
		return fmt.Errorf("reconnect ping failed: %w", err)
	}

	// Swap: replace old pool with new one
	oldPool := connection.pool
	connection.pool = newPool

	// Close old pool in background (let in-flight queries finish)
	if oldPool != nil {
		go func() {
			// Give in-flight queries a grace period before force-closing
			time.Sleep(5 * time.Second)
			oldPool.Close()
		}()
	}

	// Re-initialize affinity if needed
	if connection.connectionType == Primary {
		connection.affinity = NewConnectionAffinity(connection.pool, connection.logger)
	}

	connection.logger.Info("Connection pool recreated",
		zap.String("type", connectionTypeName(connection.connectionType)),
		zap.String("address", maskAddress(connection.host)))

	return nil
}
