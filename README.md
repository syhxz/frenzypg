```
  _____                                     
_/ ____\______   ____   ____ ___________.__.
\   __\\_  __ \_/ __ \ /    \\___   <   |  |
 |  |   |  | \/\  ___/|   |  \/    / \___  |
 |__|   |__|    \___  >___|  /_____ \/ ____|
                    \/     \/      \/\/     
```

A high-performance Postgres wire protocol aware mirroring proxy with SSL/TLS support and connection pooling.Added some additional features on frenzy.

# Features

- 🔄 **Real-time Query Mirroring**: Mirror production traffic to shadow instances
- 🔒 **Full SSL/TLS Support**: TLS 1.2/1.3 support for both proxy and backend connections
- 🏊 **Connection Pooling**: Configurable connection pools for high concurrency
- 🔗 **Multi-command Support**: Handle complex queries with multiple statements
- 📊 **Comprehensive Logging**: Detailed logging with configurable levels
- ⚡ **High Performance**: Built with Go for optimal performance
- 🛡️ **Production Ready**: Robust error handling and connection management

# Getting Started

Frenzy is a Postgres mirroring proxy that allows mirroring production traffic to shadow instances. It takes 1 `primary` connection string for production requests and multiple `mirror` connection strings for duplicated traffic.

## Building

### Prerequisites
- Go 1.20 or later
- Make (optional)

### Compile
```bash
# Using make
make

# Or directly with go
go build -o bin/frenzy ./cmd/main.go
```

# Usage

## Basic Usage
```bash
./bin/frenzy --listen :5432 \
    --primary postgresql://postgres:password@localhost:5441/postgres \
    --mirror postgresql://postgres:password@localhost:5442/postgres
```

## Connection Pool Configuration
Configure connection pools for better concurrency handling:
```bash
./bin/frenzy --listen :5432 \
    --max-conns 20 \
    --min-conns 5 \
    --max-conn-lifetime 3600 \
    --max-conn-idle-time 1800 \
    --primary postgresql://postgres:password@localhost:5441/postgres \
    --mirror postgresql://postgres:password@localhost:5442/postgres
```

## SSL/TLS Configuration

### Enable TLS for the proxy server
```bash
./bin/frenzy --listen :5432 \
    --enable-tls \
    --tls-cert server.crt \
    --tls-key server.key \
    --primary postgresql://postgres:password@localhost:5441/postgres \
    --mirror postgresql://postgres:password@localhost:5442/postgres
```

### Connect to SSL-enabled PostgreSQL backends
```bash
./bin/frenzy --listen :5432 \
    --primary "postgresql://postgres:password@rds-endpoint.region.rds.amazonaws.com:5432/postgres?sslmode=require&sslrootcert=rds-ca-certs.pem" \
    --mirror "postgresql://postgres:password@mirror-endpoint:5432/postgres?sslmode=require&sslrootcert=rds-ca-certs.pem"
```

### Full SSL/TLS setup with connection pooling
```bash
./bin/frenzy --listen :5432 \
    --enable-tls \
    --tls-cert server.crt \
    --tls-key server.key \
    --tls-ca ca.pem \
    --max-conns 50 \
    --min-conns 10 \
    --primary "postgresql://postgres:password@rds-endpoint.region.rds.amazonaws.com:5432/postgres?sslmode=verify-ca&sslrootcert=rds-ca-certs.pem" \
    --mirror "postgresql://postgres:password@mirror-endpoint:5432/postgres?sslmode=verify-ca&sslrootcert=rds-ca-certs.pem"
```

# Command Line Options

## Basic Options
- `--listen` / `-l`: Listening address and port (required)
- `--primary` / `-p`: Primary PostgreSQL connection string (required)
- `--mirror` / `-m`: Mirror PostgreSQL connection string (required, can be specified multiple times)

## Connection Pool Options
- `--max-conns`: Maximum number of connections in the pool (default: 10)
- `--min-conns`: Minimum number of connections in the pool (default: 2)
- `--max-conn-lifetime`: Maximum connection lifetime in seconds (default: 0, never expire)
- `--max-conn-idle-time`: Maximum connection idle time in seconds (default: 0, never expire)

## SSL/TLS Options
- `--enable-tls`: Enable TLS for the proxy server
- `--tls-cert`: Path to TLS certificate file
- `--tls-key`: Path to TLS private key file  
- `--tls-ca`: Path to TLS CA certificate file
- `--tls-server-name`: TLS server name for verification
- `--tls-skip-verify`: Skip TLS certificate verification (for testing only)

# Supported Features

## Query Types
- ✅ Simple SELECT queries
- ✅ Multi-statement queries (e.g., `SET work_mem=1; SELECT * FROM table;`)
- ✅ DDL statements (CREATE, ALTER, DROP)
- ✅ DML statements (INSERT, UPDATE, DELETE)
- ✅ PostgreSQL-specific commands (`\l`, `\d`, etc.)
- ✅ Prepared statements
- ✅ Transactions

## SSL/TLS Support
- ✅ TLS 1.2 and TLS 1.3
- ✅ Client certificate authentication
- ✅ Certificate verification modes (require, verify-ca, verify-full)
- ✅ Custom CA certificates
- ✅ SNI (Server Name Indication)

## Connection Management
- ✅ Connection pooling with configurable parameters
- ✅ Automatic connection recovery
- ✅ Concurrent client support
- ✅ Connection lifecycle management

# Testing

## Basic Connection Test
```bash
PGPASSWORD=password psql -U postgres -h localhost -p 5432 -c "SELECT version();"
```

## SSL Connection Test
```bash
PGPASSWORD=password psql -U postgres -h localhost -p 5432 -c "SELECT version();" --set=sslmode=require
```

## Multi-command Test
```bash
PGPASSWORD=password psql -U postgres -h localhost -p 5432 -c "SET work_mem='1MB'; SELECT current_setting('work_mem');"
```

## Connection Pool Test
```bash
# Test concurrent connections
for i in {1..10}; do
  PGPASSWORD=password psql -U postgres -h localhost -p 5432 -c "SELECT pg_backend_pid(), now();" &
done
wait
```

# Logging

## Log Levels
Frenzy supports configurable logging levels:
- `ERROR`: Only error messages
- `WARN`: Warnings and errors
- `INFO`: General information (default)
- `DEBUG`: Detailed debugging information

## Environment Variables
```bash
# Set log level via environment variable
export FRENZY_LOG_LEVEL=debug
./bin/frenzy --listen :5432 --primary ... --mirror ...

# Or use INFO level by default (disable debug logs)
export FRENZY_LOG_LEVEL=info
```

# Performance Tuning

## Connection Pool Sizing
- **High concurrency**: `--max-conns 50-100`
- **Resource constrained**: `--max-conns 5-10`
- **Long-running connections**: `--max-conn-lifetime 0`
- **Frequent reconnections**: Set appropriate `--max-conn-idle-time`

## Example Production Configuration
```bash
./bin/frenzy --listen :5432 \
    --max-conns 100 \
    --min-conns 20 \
    --max-conn-lifetime 7200 \
    --max-conn-idle-time 3600 \
    --enable-tls \
    --tls-cert /etc/ssl/certs/frenzy.crt \
    --tls-key /etc/ssl/private/frenzy.key \
    --tls-ca /etc/ssl/certs/ca.pem \
    --primary "postgresql://user:pass@primary.db:5432/db?sslmode=verify-full&sslrootcert=/etc/ssl/certs/db-ca.pem" \
    --mirror "postgresql://user:pass@mirror1.db:5432/db?sslmode=verify-full&sslrootcert=/etc/ssl/certs/db-ca.pem" \
    --mirror "postgresql://user:pass@mirror2.db:5432/db?sslmode=verify-full&sslrootcert=/etc/ssl/certs/db-ca.pem"
```

# Troubleshooting

## Common Issues

### TLS Handshake Failures
```bash
# Test TLS connectivity
openssl s_client -connect localhost:5432 -CAfile ca.pem

# Check certificate validity
openssl x509 -in server.crt -text -noout
```

### Connection Pool Issues
- Monitor connection pool usage in logs
- Adjust `--max-conns` based on your workload
- Check database connection limits

### Multi-command Query Issues
- Ensure queries are properly terminated with semicolons
- Check logs for command parsing details

## Debug Mode
Enable debug logging for detailed troubleshooting:
```bash
export FRENZY_LOG_LEVEL=debug
./bin/frenzy --listen :5432 --primary ... --mirror ...
```

# Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

# License

This project is licensed under the MIT License - see the LICENSE file for details.

# Acknowledgments

- Built with [pgx](https://github.com/jackc/pgx) for PostgreSQL connectivity
- Uses [psql-wire](https://github.com/jeroenrinzema/psql-wire) for wire protocol handling
- Logging powered by [zap](https://github.com/uber-go/zap)
