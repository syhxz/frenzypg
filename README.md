```
  _____
_/ ____\______   ____   ____ ___________.__.
\   __\\_  __ \_/ __ \ /    \\___   <   |  |
 |  |   |  | \/\  ___/|   |  \/    / \___  |
 |__|   |__|    \___  >___|  /_____ \/ ____|
                    \/     \/      \/\/
```

# FrenzyPG

A high-performance PostgreSQL wire protocol mirroring proxy. It sits between your application and PostgreSQL, forwarding queries to a primary database while asynchronously replicating them to one or more mirror databases.

## Features

- **Dual Proxy Mode** — Choose between `raw` (high-performance byte forwarding, default) and `wire` (full protocol decode)
- **Query Mirroring** — Mirror production traffic to shadow instances in real time
- **Extended Query Protocol** — Full support for parameterized queries (`$1`, `$2`, ...) in both modes
- **Transaction Awareness** — Buffered transactions replayed atomically on mirrors at commit time
- **SAVEPOINT Support** — Full support for SAVEPOINT, ROLLBACK TO, and RELEASE
- **Stored Procedures** — CALL, DO, CREATE FUNCTION all properly mirrored
- **COPY Support** — COPY FROM STDIN piped to primary; file-based COPY mirrored
- **Session State** — SET, RESET, DISCARD replicated to keep mirrors consistent
- **SSL/TLS** — TLS 1.2/1.3 for both client-facing and backend connections
- **Connection Pooling** — Configurable pools with session affinity for transactions
- **Connection Limiting** — Maximum client connection enforcement
- **Query Filtering** — Mirror all, DDL-only, DML-only, or custom combinations
- **Circuit Breaker** — Automatic mirror isolation on failure with recovery detection
- **Mirror Auto-Reconnect** — Periodic health check with automatic pool recreation
- **Retry Logic** — Configurable retries with backoff for failed mirror operations
- **Prometheus Metrics** — Full observability with query counters, latency histograms, pool stats
- **Admin Panel** — HTTP endpoints for runtime status inspection
- **Config Hot-Reload** — SIGHUP-triggered reload without restart
- **Log Rotation** — Built-in file rotation (max size, max age, compression)
- **Health Check** — HTTP `/health` endpoint with primary connectivity verification
- **Graceful Shutdown** — Buffer flush + clean connection draining on SIGTERM/SIGINT
- **Kubernetes Ready** — Stateless, multi-replica, with HPA and PDB support

## Quick Start

### Build

```bash
make build
```

### Run (CLI mode)

```bash
# Wire mode — full protocol decode, supports transaction buffering
./bin/frenzy \
  --listen :5432 \
  --primary "postgresql://postgres:password@localhost:5433/mydb" \
  --mirror "postgresql://postgres:password@mirror-host:5432/mydb" \
  --async-mirrors \
  --health-port 8080

# Raw mode — high-performance byte forwarding (36x faster than wire mode)
./bin/frenzy \
  --listen :5432 \
  --mode raw \
  --primary "postgresql://postgres:password@localhost:5433/mydb" \
  --mirror "postgresql://postgres:password@mirror-host:5432/mydb" \
  --mirror-all-queries \
  --health-port 8080
```

### Run (Config file mode)

```bash
export FRENZY_PRIMARY_PASSWORD="primary_password"
export FRENZY_MIRROR_PASSWORD="mirror_password"

./bin/frenzy --config /etc/frenzy/frenzy.yaml
```

### Version

```bash
./bin/frenzy --version
```

## Configuration

### Config File (Recommended)

See [`deploy/frenzy.yaml.example`](deploy/frenzy.yaml.example) for a full example.

```yaml
listen: ":5432"
mode: "raw"  # "wire" (default) or "raw"

primary:
  url: "postgresql://postgres@localhost:5433/dbname"
  password_env: "FRENZY_PRIMARY_PASSWORD"

mirrors:
  - url: "postgresql://dbmgr@mirror-host:5432/dbname"
    password_env: "FRENZY_MIRROR_PASSWORD"

pool:
  max_conns: 20
  min_conns: 5
  max_conn_idle_time: 300

performance:
  async_mirrors: true
  mirror_timeout: 30
  mirror_retries: 2
  retry_delay: 3

filter:
  mirror_all_queries: false
  mirror_select_queries: false
  mirror_ddl_only: false
  mirror_dml_only: false
  skip_rollback_mirror: false
  skip_mirror_tx_locks: false

service:
  pid_file: "/var/run/frenzy/frenzy.pid"
  health_port: 8080
```

Passwords are injected via environment variables referenced by `password_env`. They never appear in config files or logs.

### CLI Options

| Option | Default | Description |
|--------|---------|-------------|
| `--listen` | (required) | Listen address (e.g. `:5432`) |
| `--primary` | (required) | Primary PostgreSQL connection URL |
| `--mirror` | (optional) | Mirror PostgreSQL connection URL (repeatable) |
| `--mode` | wire | Proxy mode: `wire` (full decode) or `raw` (byte forwarding) |
| `--config` | | YAML config file path (replaces all CLI options) |
| `--enable-tls` | false | Enable TLS for client connections |
| `--tls-cert` | | TLS certificate file |
| `--tls-key` | | TLS private key file |
| `--tls-ca` | | TLS CA certificate file |
| `--max-conns` | 10 | Max connections in pool |
| `--min-conns` | 2 | Min connections in pool |
| `--async-mirrors` | false | Process mirrors asynchronously |
| `--mirror-timeout` | 120 | Mirror operation timeout (seconds) |
| `--mirror-retries` | 2 | Retry count for failed mirror ops |
| `--mirror-all-queries` | false | Mirror all queries including SELECT |
| `--mirror-select-queries` | false | Mirror SELECT queries |
| `--mirror-ddl-only` | false | Mirror only DDL (CREATE/ALTER/DROP) |
| `--mirror-dml-only` | false | Mirror only DML (INSERT/UPDATE/DELETE) |
| `--skip-rollback-mirror` | false | Skip mirroring ROLLBACK |
| `--skip-mirror-tx-locks` | false | Skip BEGIN/COMMIT wrappers on mirrors |
| `--health-port` | 0 | HTTP health check port (0=disabled) |
| `--pid-file` | | Write PID to file |
| `-v, --version` | | Show version and exit |

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `FRENZY_LOG_LEVEL` | info | Log level: debug, info, warn, error |
| `FRENZY_LOG_FORMAT` | console | Log format: console, json |
| `FRENZY_LOG_FILE` | (stdout only) | Log file path (enables rotation) |
| `FRENZY_LOG_MAX_SIZE` | 100 | Max log file size in MB before rotation |
| `FRENZY_LOG_MAX_BACKUPS` | 7 | Max rotated log files to keep |
| `FRENZY_LOG_MAX_AGE` | 30 | Max days to retain rotated logs |
| `FRENZY_LOG_COMPRESS` | true | Compress rotated log files with gzip |

## Proxy Modes

FrenzyPG supports two proxy modes, selectable via `--mode` (CLI) or `mode:` (YAML):

### Wire Mode (`--mode wire`)

```
Client ←→ psql-wire ←→ Query Handler ←→ pgx/pgxpool ←→ PostgreSQL
         decode req     parse SQL        re-encode req
         encode resp    route decision   decode resp
```

- Full PostgreSQL wire protocol decode/encode via [psql-wire](https://github.com/jeroenrinzema/psql-wire)
- Supports **Extended Query Protocol** (prepared statements, `$1` parameters)
- Transaction buffering with commit-time replay on mirrors
- Column type detection and proper result encoding
- Best for: applications using parameterized queries or ORMs (JDBC, pgx, libpq)

### Raw Mode (`--mode raw`, default)

```
Client ←→ [Header Parse] ←→ Route Decision ←→ [Raw Byte Forward] ←→ PostgreSQL
         5 bytes only       mirror if needed    zero-copy relay
         (type + length)    Parse SQL extract   writev syscall
```

- Only parses the 5-byte message header (1 byte type + 4 byte length)
- Message body forwarded as raw bytes without full decode — **zero-copy forwarding**
- Uses `net.Buffers` (writev) for minimal syscall overhead
- **SimpleQuery** (`'Q'`): SQL extracted directly for mirror filtering
- **Extended Query**: SQL extracted from Parse (`'P'`) message, mirrored after Sync response
- Full startup/authentication relay (SSL, SASL, md5 all transparent)
- Transaction state tracked via ReadyForQuery byte
- Best for: high-throughput production mirroring (default mode)

### Mode Comparison

| Feature | Wire Mode | Raw Mode |
|---------|:---------:|:--------:|
| SimpleQuery protocol | ✅ | ✅ |
| Extended Query (prepared statements) | ✅ | ✅ |
| Extended Query mirroring | ✅ | ✅ (Parse SQL extraction) |
| Transaction buffering | ✅ (commit-time replay) | ❌ (async per-statement) |
| Query filtering & mirror | ✅ | ✅ |
| Column type detection | ✅ | ❌ (transparent) |
| COPY FROM STDIN | ✅ | ✅ (pass-through) |
| Performance overhead | ~98% | ~40% |

## Benchmark

Tested with pgbench (scale=1, 100K rows), PostgreSQL 16 (local).  
Mirror: Aurora PostgreSQL (cross-AZ, async mirroring).

### Throughput (TPS)

| Target | Protocol | Clients | TPS | Latency (avg) | vs Direct |
|--------|----------|:-------:|----:|:-------------:|:---------:|
| Direct PostgreSQL | simple | 4 | 10,358 | 0.39 ms | baseline |
| **Raw Proxy** (+ mirror) | simple | 4 | **5,936** | **0.67 ms** | -43% |
| **Raw Proxy** (+ mirror) | extended | 4 | **5,372** | **0.75 ms** | -48% |
| Direct PostgreSQL | simple | 16 | 9,135 | 1.75 ms | baseline |
| **Raw Proxy** (+ mirror) | simple | 16 | **7,164** | **2.23 ms** | -22% |
| Wire Proxy (+ mirror) | simple | 4 | 168 | 23.9 ms | -98% |

### Latency Percentiles (SELECT-only, SimpleQuery)

| Percentile | Direct (4c) | Raw Proxy (4c) | Delta | Direct (16c) | Raw Proxy (16c) | Delta |
|:----------:|:-----------:|:--------------:|:-----:|:------------:|:---------------:|:-----:|
| **P50** | 0.34 ms | 0.56 ms | +0.22 ms | 1.44 ms | 1.88 ms | +0.44 ms |
| **P95** | 0.64 ms | 1.18 ms | +0.54 ms | 3.74 ms | 4.24 ms | +0.51 ms |
| **P99** | 0.80 ms | 1.72 ms | +0.92 ms | 4.93 ms | 6.83 ms | +1.90 ms |
| **Max** | 14.6 ms | 9.9 ms | — | 10.6 ms | 59.5 ms | tail |

### TPC-B (read-write mixed, 4 clients)

| Target | TPS | Latency (avg) | vs Direct |
|--------|----:|:-------------:|:---------:|
| Direct PostgreSQL | 675 | 5.93 ms | baseline |
| **Raw Proxy** (+ mirror) | **595** | **6.72 ms** | -12% |

### Key Takeaways

- **Raw mode is 36x faster than Wire mode**
- **P50 overhead: only +0.2~0.4 ms** — the inherent cost of one extra TCP hop
- **P95 stable at +0.5 ms** — production acceptable
- **TPC-B (write) overhead only +0.8 ms** — mirror is fully async, doesn't block writes
- **High concurrency (16c) scales well** — only 22% TPS drop vs direct
- P99 tail latency at 16c due to goroutine scheduling + GC, rare occurrence

## Query Routing

The following table shows which SQL statements are mirrored by default:

| Statement | Mirrored | Condition |
|-----------|:--------:|-----------|
| `INSERT`, `UPDATE`, `DELETE`, `MERGE` | ✅ | Unless `--mirror-ddl-only` |
| `CREATE`, `ALTER`, `DROP`, `TRUNCATE` | ✅ | Unless `--mirror-dml-only` |
| `CALL procedure()` | ✅ | Unless `--mirror-ddl-only` |
| `DO $$ ... $$` | ✅ | Unless `--mirror-ddl-only` |
| `BEGIN`, `COMMIT`, `ROLLBACK` | ✅ | Always |
| `SAVEPOINT`, `RELEASE`, `ROLLBACK TO` | ✅ | Always |
| `SET`, `RESET`, `DISCARD` | ✅ | Always |
| `PREPARE`, `EXECUTE`, `DEALLOCATE` | ✅ | Always |
| `GRANT`, `REVOKE`, `COMMENT` | ✅ | Unless `--mirror-dml-only` |
| `COPY table FROM '/file'` | ✅ | Unless `--mirror-ddl-only` |
| `SELECT setval(...)` / `nextval(...)` | ✅ | Always |
| `SELECT` / `WITH` | ❌ | Only with `--mirror-select-queries` |
| `COPY table TO ...` | ❌ | Read-only, never mirrored |
| `COPY table FROM STDIN` | ⚠️ | Primary only (data stream cannot be replicated) |
| `LISTEN`, `NOTIFY` | ❌ | Only with `--mirror-all-queries` |
| `VACUUM`, `ANALYZE`, `REINDEX` | ❌ | Only with `--mirror-all-queries` |

## Observability

### Health Check

```bash
# Primary reachable → 200
curl http://localhost:8080/health
{"status":"ok","version":"v1.0.0","uptime":"2h30m15s"}

# Primary unreachable → 503
{"status":"unhealthy","detail":"primary unreachable: ...","version":"v1.0.0","uptime":"2h30m15s"}
```

### Prometheus Metrics

Available at `/metrics` (default port 9090):

| Metric | Type | Description |
|--------|------|-------------|
| `frenzy_queries_total` | Counter | Queries by type (select/insert/update/delete/ddl/other) |
| `frenzy_mirror_queries_total` | Counter | Total queries sent to mirrors |
| `frenzy_mirror_failures_total` | Counter | Mirror query failures |
| `frenzy_mirror_retries_total` | Counter | Mirror retry attempts |
| `frenzy_query_duration_seconds` | Histogram | Query latency by target (primary/mirror) |
| `frenzy_active_connections` | Gauge | Current client connections |
| `frenzy_pool_connections` | Gauge | Pool stats by target and state |
| `frenzy_active_transactions` | Gauge | Buffered transaction count |
| `frenzy_tx_buffer_bytes` | Gauge | Total bytes buffered |
| `frenzy_circuit_breaker_state` | Gauge | Per-mirror circuit state (0=closed/1=open/2=half-open) |

### Admin Panel

```bash
curl http://localhost:8080/admin/status   # Overall status
curl http://localhost:8080/admin/mirrors  # Per-mirror health + circuit breaker
curl http://localhost:8080/admin/buffers  # Transaction buffer stats
curl http://localhost:8080/admin/config   # Current filter configuration
```

## Config Hot-Reload

Send SIGHUP to reload configuration without restarting:

```bash
kill -HUP $(cat /var/run/frenzy/frenzy.pid)
```

Reloadable fields: query filter rules, mirror timeout/retries, log level, max connections.

Non-reloadable fields (require restart): listen address, primary/mirror URLs, TLS config, pool size.

## Circuit Breaker

Each mirror has an independent circuit breaker:

```
CLOSED ──(5 failures)──→ OPEN ──(30s timeout)──→ HALF-OPEN ──(3 successes)──→ CLOSED
                           ▲                         │
                           └────(any failure)─────────┘
```

- **Closed**: Normal operation, all queries forwarded
- **Open**: Mirror isolated, queries skipped (no goroutine/connection waste)
- **Half-Open**: Testing recovery, limited requests allowed

## Deployment

### systemd

```bash
sudo bash deploy/install.sh
sudo systemctl start frenzy
sudo systemctl enable frenzy
```

### Docker

```bash
docker build -t frenzypg .
docker run -d \
  -e FRENZY_PRIMARY_PASSWORD=xxx \
  -e FRENZY_MIRROR_PASSWORD=xxx \
  -v /path/to/frenzy.yaml:/etc/frenzy/frenzy.yaml \
  -p 5432:5432 -p 8080:8080 \
  frenzypg --config /etc/frenzy/frenzy.yaml
```

### Kubernetes

Full K8s manifest at [`deploy/k8s.yaml`](deploy/k8s.yaml) including:

- **Deployment** (3 replicas, rolling update)
- **Service** (ClusterIP, TCP L4 load balancing)
- **HPA** (auto-scale 2-10 pods on CPU/connections)
- **PDB** (minAvailable: 2 during disruptions)
- **ConfigMap** + **Secret** for configuration
- **ServiceMonitor** for Prometheus Operator

```bash
kubectl apply -f deploy/k8s.yaml

# Application connection string:
# postgresql://user:pass@frenzy-proxy.frenzy.svc.cluster.local:5432/mydb
```

FrenzyPG is stateless — transaction buffers are ephemeral and only affect mirror replication. Primary data integrity is never at risk from pod restarts.

## Architecture

```
Client App
    │
    ▼ (PostgreSQL wire protocol)
┌──────────────────────────────────────────────────┐
│                FrenzyPG Proxy                     │
│                                                  │
│  ┌─────────────────────────────────────────────┐ │
│  │            Mode Selection                    │ │
│  │   --mode wire          --mode raw            │ │
│  └──────┬────────────────────────┬─────────────┘ │
│         │                        │               │
│  ┌──────▼──────────┐    ┌───────▼────────────┐  │
│  │   Wire Mode     │    │    Raw Mode        │  │
│  │  (psql-wire)    │    │  (pgproto3 byte)   │  │
│  │                 │    │                    │  │
│  │ • Full decode   │    │ • 5-byte header    │  │
│  │ • Extended QP   │    │ • Zero-copy fwd    │  │
│  │ • Tx buffering  │    │ • writev syscall   │  │
│  │ • Column detect │    │ • Simple+Extended │  │
│  └──────┬──────────┘    └───────┬────────────┘  │
│         │                       │                │
│  ┌──────▼───────────────────────▼─────────────┐  │
│  │          Mirror Dispatch                    │  │
│  │  • Query filtering (DDL/DML/SELECT/ALL)     │  │
│  │  • Async goroutine execution                │  │
│  │  • Circuit breaker (per mirror)             │  │
│  │  • Retry with backoff                       │  │
│  └──────┬───────────────────────┬─────────────┘  │
│         │                       │                │
│  ┌──────▼────┐          ┌──────▼──────────┐     │
│  │  Primary  │          │  Mirror Pool(s) │     │
│  │  (pool)   │          │  (pgxpool)      │     │
│  └──────┬────┘          └──────┬──────────┘     │
└─────────┼──────────────────────┼─────────────────┘
          │                      │
          ▼                      ▼
    PostgreSQL              PostgreSQL / Aurora
     (primary)              (mirror)
```

## Project Structure

```
frenzypg/
├── cmd/
│   ├── main.go                # Entry point, CLI parsing, signal handling
│   └── config.go              # YAML config loader, env var resolution
├── server/
│   ├── proxy.go               # Core proxy struct, wire protocol parsing
│   ├── raw_proxy.go           # Raw byte-forwarding proxy (high-performance mode)
│   ├── query_executor.go      # Primary execution, result streaming
│   ├── mirror.go              # Mirror dispatch, filtering, tx buffering
│   ├── copy_handler.go        # COPY FROM STDIN / TO STDOUT / file-based
│   ├── lifecycle.go           # Connect, Close, cleanup routines
│   ├── connection.go          # Database connection, pool, SSL
│   ├── connection_affinity.go # Transaction session pinning
│   ├── circuit_breaker.go     # Per-mirror circuit breaker
│   ├── mirror_reconnect.go    # Automatic mirror health check + reconnect
│   ├── metrics.go             # Prometheus metrics + connection limiter
│   ├── config_reload.go       # SIGHUP hot-reload + admin panel
│   ├── cancel_tracker.go      # Mirror operation cancellation
│   ├── session_manager.go     # Session state, IsTransactionCommand
│   ├── session_id.go          # Stable session ID via psql-wire
│   ├── simple_transaction_fix.go # Global tx state tracker
│   ├── tls_listener.go        # TLS with PostgreSQL SSL negotiation
│   ├── config.go              # PoolConfig, PerformanceConfig types
│   └── doc.go                 # Package documentation
├── deploy/
│   ├── k8s.yaml               # Full Kubernetes manifest
│   ├── frenzy.service         # systemd unit file
│   ├── frenzy.yaml.example    # Config file template
│   ├── credentials.example    # Credentials file template
│   ├── logrotate.conf         # System logrotate config
│   └── install.sh             # Installation script
├── Dockerfile
├── Makefile
└── .gitignore
```

## Known Limitations

| Limitation | Impact | Workaround |
|-----------|--------|------------|
| `COPY FROM STDIN` data not mirrored | Mirror misses bulk loads via `\copy` or client STDIN | Use `COPY FROM '/file'` with shared storage |
| `COPY TO STDOUT` not supported | Client receives error | Use `COPY TO '/file'` or SELECT |
| Non-deterministic functions (`random()`, `uuid_generate_v4()`) | Values differ between primary and mirror | Expected for statement-level replication |
| `nextval()` sequence values | Different on primary vs mirror | Acceptable for testing; use WAL replication for exact consistency |
| `now()` / `current_timestamp` | Slight time difference on mirror | Typically sub-second, acceptable |
| Extended Query Protocol pipeline mode | Wire mode depends on psql-wire library support; Raw mode fully supports (transparent relay) | Use raw mode for pipeline-heavy workloads |

## Security

- Passwords injected via environment variables, never in config files
- All log output masks connection passwords as `***`
- Admin/health endpoints bound to 127.0.0.1 by default
- PID file uses 0600 permissions
- Config env expansion uses allowlist (`FRENZY_*`, `PG_*`, `DB_*`, `DATABASE_*`)
- TLS 1.2+ enforced for all encrypted connections
- systemd service runs with `NoNewPrivileges`, `ProtectSystem=strict`, `PrivateTmp`

## License

MIT License. See [LICENSE](LICENSE).
