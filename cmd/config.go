package main

import (
	"fmt"
	"net/url"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

// Config represents the full configuration file structure.
type Config struct {
	Listen string `yaml:"listen"`
	Mode   string `yaml:"mode"` // "wire" (default) or "raw"

	Primary struct {
		URL         string `yaml:"url"`
		PasswordEnv string `yaml:"password_env"`
	} `yaml:"primary"`

	Mirrors []struct {
		URL         string `yaml:"url"`
		PasswordEnv string `yaml:"password_env"`
	} `yaml:"mirrors"`

	TLS struct {
		Enabled    bool   `yaml:"enabled"`
		CertFile   string `yaml:"cert_file"`
		KeyFile    string `yaml:"key_file"`
		CAFile     string `yaml:"ca_file"`
		ServerName string `yaml:"server_name"`
		SkipVerify bool   `yaml:"skip_verify"`
	} `yaml:"tls"`

	Pool struct {
		MaxConns        int `yaml:"max_conns"`
		MinConns        int `yaml:"min_conns"`
		MaxConnLifetime int `yaml:"max_conn_lifetime"`
		MaxConnIdleTime int `yaml:"max_conn_idle_time"`
	} `yaml:"pool"`

	Performance struct {
		WorkerThreads   int  `yaml:"worker_threads"`
		QueryBufferSize int  `yaml:"query_buffer_size"`
		AsyncMirrors    bool `yaml:"async_mirrors"`
		MirrorTimeout   int  `yaml:"mirror_timeout"`
		MirrorRetries   int  `yaml:"mirror_retries"`
		RetryDelay      int  `yaml:"retry_delay"`
	} `yaml:"performance"`

	Filter struct {
		MirrorAllQueries    bool `yaml:"mirror_all_queries"`
		MirrorSelectQueries bool `yaml:"mirror_select_queries"`
		MirrorDdlOnly       bool `yaml:"mirror_ddl_only"`
		MirrorDmlOnly       bool `yaml:"mirror_dml_only"`
		SkipRollbackMirror  bool `yaml:"skip_rollback_mirror"`
		MirrorFailedTx      bool `yaml:"mirror_failed_tx"`  // opt-in: set true to mirror failed transactions (default: skip)
		SkipMirrorTxLocks   bool `yaml:"skip_mirror_tx_locks"`
	} `yaml:"filter"`

	Service struct {
		PIDFile    string `yaml:"pid_file"`
		HealthPort int    `yaml:"health_port"`
	} `yaml:"service"`

	Kafka struct {
		Brokers         []string `yaml:"brokers"`
		Topic           string   `yaml:"topic"`
		Partitions      int      `yaml:"partitions"`
		SASLMechanism   string   `yaml:"sasl_mechanism"`
		SASLUsername    string   `yaml:"sasl_username"`
		SASLPassword    string   `yaml:"sasl_password"`
		SASLPasswordEnv string   `yaml:"sasl_password_env"`
		TLSEnabled      bool     `yaml:"tls_enabled"`
		TLSSkipVerify   bool     `yaml:"tls_skip_verify"`
		TLSCAFile       string   `yaml:"tls_ca_file"`
		TLSCertFile     string   `yaml:"tls_cert_file"`
		TLSKeyFile      string   `yaml:"tls_key_file"`
		LocalBufferSize int      `yaml:"local_buffer_size"`
		Workers         int      `yaml:"workers"`
		GroupID         string   `yaml:"group_id"`
	} `yaml:"kafka"`
}

// loadConfig reads and parses the YAML configuration file.
func loadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	// Expand only explicitly allowed environment variables using a safe mapping.
	// This prevents arbitrary env var leakage if the config file is from an untrusted source.
	expanded := os.Expand(string(data), safeEnvExpand)

	var cfg Config
	if err := yaml.Unmarshal([]byte(expanded), &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	return &cfg, nil
}

// safeEnvExpand only expands environment variables with known safe prefixes.
// Variables that don't match allowed prefixes are left unexpanded (returned as empty).
func safeEnvExpand(key string) string {
	allowedPrefixes := []string{
		"FRENZY_",
		"PG_",
		"DATABASE_",
		"DB_",
	}
	for _, prefix := range allowedPrefixes {
		if strings.HasPrefix(key, prefix) {
			return os.Getenv(key)
		}
	}
	// Not in allowlist — return empty to prevent leakage of arbitrary env vars
	return ""
}

// resolveConnectionURL injects the password from environment variable into the URL.
func resolveConnectionURL(connURL, passwordEnv string) string {
	if passwordEnv == "" {
		return connURL
	}

	password := os.Getenv(passwordEnv)
	if password == "" {
		return connURL
	}

	u, err := url.Parse(connURL)
	if err != nil {
		// Fallback for key=value format
		if !strings.Contains(connURL, "password=") {
			return connURL + " password=" + password
		}
		return connURL
	}

	// Set or replace password in URL
	username := ""
	if u.User != nil {
		username = u.User.Username()
	}
	u.User = url.UserPassword(username, password)
	return u.String()
}
