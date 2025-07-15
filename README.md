```
  _____                                     
_/ ____\______   ____   ____ ___________.__.
\   __\\_  __ \_/ __ \ /    \\___   <   |  |
 |  |   |  | \/\  ___/|   |  \/    / \___  |
 |__|   |__|    \___  >___|  /_____ \/ ____|
                    \/     \/      \/\/     
```

A Postgres wire protocol aware mirroring proxy with SSL/TLS support.Add SSL/TLS and connection pool support on frenzy.

# Getting started
Frenzy is an experimental Postgres mirroring proxy that allows mirroring production traffic to shadow instances. Frenzy allows you to service production traffic from a `primary` while measuring one or more `mirror` shadow instances.

Frenzy takes 1 `primary` connection string that will be used to respond to production requests and multiple `mirror` connection strings that will receive the duplicated traffic from the primary.

# Building
To compile the `bin/frenzy` binary run the following.
```
make
```

# Using

## Basic Usage
Provide 1 primary Postgres connection string and one or many mirror Postgres connection strings.
```
./bin/frenzy --listen :5432 \
    --primary postgresql://postgres:password@localhost:5441/postgres \
    --mirror postgresql://postgres:password@localhost:5442/postgres
```

## SSL/TLS Configuration

### Enable TLS for the proxy server
```
./bin/frenzy --listen :5432 \
    --enable-tls \
    --tls-cert server.crt \
    --tls-key server.key \
    --primary postgresql://postgres:password@localhost:5441/postgres \
    --mirror postgresql://postgres:password@localhost:5442/postgres
```

### Connect to SSL-enabled PostgreSQL backends
```
./bin/frenzy --listen :5432 \
    --primary "postgresql://postgres:password@rds-endpoint.region.rds.amazonaws.com:5432/postgres?sslmode=require&sslrootcert=rds-ca-certs.pem" \
    --mirror "postgresql://postgres:password@mirror-endpoint:5432/postgres?sslmode=require&sslrootcert=rds-ca-certs.pem"
```

### Full SSL/TLS setup (proxy and backends)
```
./bin/frenzy --listen :5432 \
    --enable-tls \
    --tls-cert server.crt \
    --tls-key server.key \
    --tls-ca ca.pem \
    --primary "postgresql://postgres:password@rds-endpoint.region.rds.amazonaws.com:5432/postgres?sslmode=verify-ca&sslrootcert=rds-ca-certs.pem" \
    --mirror "postgresql://postgres:password@mirror-endpoint:5432/postgres?sslmode=verify-ca&sslrootcert=rds-ca-certs.pem"
```

## SSL/TLS Options

- `--enable-tls`: Enable TLS for the proxy server
- `--tls-cert`: Path to TLS certificate file
- `--tls-key`: Path to TLS private key file  
- `--tls-ca`: Path to TLS CA certificate file
- `--tls-server-name`: TLS server name for verification
- `--tls-skip-verify`: Skip TLS certificate verification (for testing)

For detailed SSL configuration, see [SSL_CONFIG.md](SSL_CONFIG.md).

# Supported Queries
Right now I am testing with a simple hello world query.
```
PGPASSWORD=password psql -U postgres -h localhost -p 5432 -c "SELECT version();"
```
Surprisingly I've seen more complicated queries work already! `\l` in the `psql` console also dispatches a more complicated query that seems to work!

## SSL Client Connection
When TLS is enabled, connect using SSL:
```
PGPASSWORD=password psql -U postgres -h localhost -p 5432 -c "SELECT version();" --set=sslmode=require
```
