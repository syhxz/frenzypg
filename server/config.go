package server

import (
	"net/url"
	"strings"
	"time"
)

// ConnectionType distinguishes primary from mirror connections.
type ConnectionType int

const (
	Primary ConnectionType = iota
	Mirror
)

func connectionTypeName(ct ConnectionType) string {
	switch ct {
	case Primary:
		return "primary"
	case Mirror:
		return "mirror"
	default:
		return "unknown"
	}
}

// PoolConfig holds connection pool configuration.
type PoolConfig struct {
	MaxConns        int32
	MinConns        int32
	MaxConnLifetime time.Duration
	MaxConnIdleTime time.Duration
}

// DefaultPoolConfig returns default connection pool configuration.
func DefaultPoolConfig() *PoolConfig {
	return &PoolConfig{
		MaxConns:        20,
		MinConns:        5,
		MaxConnLifetime: 30 * time.Minute,
		MaxConnIdleTime: 5 * time.Minute,
	}
}

// PerformanceConfig holds performance optimization settings.
type PerformanceConfig struct {
	WorkerThreads     int
	QueryBufferSize   int
	AsyncMirrors      bool
	MirrorTimeoutSecs int
	MirrorRetries     int
	RetryDelaySecs    int
}

// DefaultPerformanceConfig returns default performance configuration.
func DefaultPerformanceConfig() *PerformanceConfig {
	return &PerformanceConfig{
		WorkerThreads:     0,
		QueryBufferSize:   8192,
		AsyncMirrors:      true,
		MirrorTimeoutSecs: 120,
		MirrorRetries:     2,
		RetryDelaySecs:    5,
	}
}

// maskAddress masks password in a connection string for safe logging.
func maskAddress(connStr string) string {
	// Handle postgresql:// URL format
	if strings.Contains(connStr, "://") {
		u, err := url.Parse(connStr)
		if err == nil && u.User != nil {
			u.User = url.UserPassword(u.User.Username(), "***")
			return u.String()
		}
	}
	// Handle key=value format
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
