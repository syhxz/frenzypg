# Frenzy SSL/TLS Configuration Guide

This guide explains how to configure SSL/TLS certificates for the Frenzy PostgreSQL mirroring proxy.

## Server-side TLS Configuration

### Basic TLS Setup

To enable TLS for the proxy server, you need:

1. A server certificate (`server.crt`)
2. A server private key (`server.key`)
3. Optionally, a CA certificate (`ca.pem`)

### Command Line Options

```bash
# Enable TLS with server certificate
./bin/frenzy --listen :5432 \
    --enable-tls \
    --tls-cert server.crt \
    --tls-key server.key \
    --primary "postgresql://postgres:password@localhost:5441/postgres" \
    --mirror "postgresql://postgres:password@localhost:5442/postgres"

# Enable TLS with CA certificate verification
./bin/frenzy --listen :5432 \
    --enable-tls \
    --tls-cert server.crt \
    --tls-key server.key \
    --tls-ca ca.pem \
    --tls-server-name "frenzy-proxy" \
    --primary "postgresql://postgres:password@localhost:5441/postgres" \
    --mirror "postgresql://postgres:password@localhost:5442/postgres"

# Skip TLS verification (for testing)
./bin/frenzy --listen :5432 \
    --enable-tls \
    --tls-cert server.crt \
    --tls-key server.key \
    --tls-skip-verify \
    --primary "postgresql://postgres:password@localhost:5441/postgres" \
    --mirror "postgresql://postgres:password@localhost:5442/postgres"
```

## Backend Database SSL Configuration

### Connecting to SSL-enabled PostgreSQL servers

You can configure SSL connections to your primary and mirror databases using connection string parameters:

```bash
# Using sslmode=require
./bin/frenzy --listen :5432 \
    --primary "postgresql://postgres:password@rds-endpoint.region.rds.amazonaws.com:5432/postgres?sslmode=require" \
    --mirror "postgresql://postgres:password@mirror-endpoint:5432/postgres?sslmode=require"

# Using SSL certificate verification
./bin/frenzy --listen :5432 \
    --primary "postgresql://postgres:password@rds-endpoint.region.rds.amazonaws.com:5432/postgres?sslmode=verify-ca&sslrootcert=rds-ca-certs.pem" \
    --mirror "postgresql://postgres:password@mirror-endpoint:5432/postgres?sslmode=verify-ca&sslrootcert=rds-ca-certs.pem"

# Full SSL verification
./bin/frenzy --listen :5432 \
    --primary "postgresql://postgres:password@rds-endpoint.region.rds.amazonaws.com:5432/postgres?sslmode=verify-full&sslrootcert=rds-ca-certs.pem" \
    --mirror "postgresql://postgres:password@mirror-endpoint:5432/postgres?sslmode=verify-full&sslrootcert=rds-ca-certs.pem"
```

### SSL Modes

- `disable`: No SSL
- `require`: SSL required, but no certificate verification
- `verify-ca`: SSL required, verify CA certificate
- `verify-full`: SSL required, verify CA certificate and hostname

## AWS RDS Configuration

### Download RDS SSL Certificate

```bash
# Download AWS RDS global certificate bundle
curl -o rds-ca-certs.pem https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem
```

### Example with AWS RDS

```bash
./bin/frenzy --listen :5432 \
    --enable-tls \
    --tls-cert server.crt \
    --tls-key server.key \
    --primary "postgresql://myuser:mypassword@mydb.cluster-xyz.us-east-1.rds.amazonaws.com:5432/postgres?sslmode=require&sslrootcert=rds-ca-certs.pem" \
    --mirror "postgresql://myuser:mypassword@mydb-mirror.cluster-abc.us-east-1.rds.amazonaws.com:5432/postgres?sslmode=require&sslrootcert=rds-ca-certs.pem"
```

## Generating Test Certificates

### Create a simple CA and server certificate for testing:

```bash
# Generate CA private key
openssl genrsa -out ca.key 4096

# Generate CA certificate
openssl req -new -x509 -key ca.key -sha256 -subj "/C=US/ST=CA/O=MyOrg/CN=MyCA" -days 3650 -out ca.pem

# Generate server private key
openssl genrsa -out server.key 4096

# Generate server certificate signing request
openssl req -new -key server.key -out server.csr -subj "/C=US/ST=CA/O=MyOrg/CN=localhost"

# Sign server certificate with CA
openssl x509 -req -in server.csr -CA ca.pem -CAkey ca.key -CAcreateserial -out server.crt -days 365 -sha256

# Clean up
rm server.csr ca.key
```

## Client Connection

### Connect to Frenzy with SSL

```bash
# Connect with SSL required
psql "host=localhost port=5432 dbname=postgres user=myuser sslmode=require"

# Connect with certificate verification
psql "host=localhost port=5432 dbname=postgres user=myuser sslmode=verify-ca sslrootcert=ca.pem"

# Connect with full verification
psql "host=localhost port=5432 dbname=postgres user=myuser sslmode=verify-full sslrootcert=ca.pem"
```

## Environment Variables

You can also use environment variables for SSL configuration:

```bash
export FRENZY_TLS_CERT="server.crt"
export FRENZY_TLS_KEY="server.key"
export FRENZY_TLS_CA="ca.pem"
export FRENZY_TLS_SERVER_NAME="localhost"

./bin/frenzy --listen :5432 --enable-tls \
    --primary "postgresql://postgres:password@localhost:5441/postgres" \
    --mirror "postgresql://postgres:password@localhost:5442/postgres"
```

## Troubleshooting

### Common Issues

1. **Certificate verification failed**: Check that the server name matches the certificate CN
2. **Connection refused**: Ensure the TLS port is correctly configured
3. **SSL handshake failed**: Verify certificate and key files are readable and valid

### Debug Logging

Enable debug logging to troubleshoot SSL issues:

```bash
export FRENZY_LOG_LEVEL=debug
./bin/frenzy --listen :5432 --enable-tls ...
```
