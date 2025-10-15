#!/bin/bash

# Frenzy SSL Configuration Test Script

set -e

echo "=== Frenzy SSL Configuration Test ==="

# Create test certificates directory
mkdir -p test_certs
cd test_certs

echo "1. Generating test certificates..."

# Generate CA private key
if [ ! -f ca.key ]; then
    openssl genrsa -out ca.key 4096
    echo "   ✓ CA private key generated"
fi

# Generate CA certificate
if [ ! -f ca.pem ]; then
    openssl req -new -x509 -key ca.key -sha256 \
        -subj "/C=US/ST=CA/O=FrenzyTest/CN=FrenzyTestCA" \
        -days 365 -out ca.pem
    echo "   ✓ CA certificate generated"
fi

# Generate server private key
if [ ! -f server.key ]; then
    openssl genrsa -out server.key 4096
    echo "   ✓ Server private key generated"
fi

# Generate server certificate signing request
if [ ! -f server.csr ]; then
    openssl req -new -key server.key -out server.csr \
        -subj "/C=US/ST=CA/O=FrenzyTest/CN=localhost"
    echo "   ✓ Server CSR generated"
fi

# Sign server certificate with CA
if [ ! -f server.crt ]; then
    openssl x509 -req -in server.csr -CA ca.pem -CAkey ca.key \
        -CAcreateserial -out server.crt -days 365 -sha256
    echo "   ✓ Server certificate signed"
fi

cd ..

echo "2. Building Frenzy..."
make

echo "3. Certificate files created in test_certs/:"
ls -la test_certs/

echo ""
echo "=== Test Commands ==="
echo ""
echo "To test Frenzy with SSL enabled:"
echo ""
echo "./bin/frenzy --listen :5432 \\"
echo "    --enable-tls \\"
echo "    --tls-cert test_certs/server.crt \\"
echo "    --tls-key test_certs/server.key \\"
echo "    --tls-ca test_certs/ca.pem \\"
echo "    --primary 'postgresql://postgres:password@localhost:5441/postgres' \\"
echo "    --mirror 'postgresql://postgres:password@localhost:5442/postgres'"
echo ""
echo "To connect with psql:"
echo ""
echo "psql 'host=localhost port=5432 dbname=postgres user=postgres sslmode=require'"
echo ""
echo "Or with certificate verification:"
echo ""
echo "psql 'host=localhost port=5432 dbname=postgres user=postgres sslmode=verify-ca sslrootcert=test_certs/ca.pem'"
echo ""
echo "=== AWS RDS Example ==="
echo ""
echo "# Download RDS certificate"
echo "curl -o rds-ca-certs.pem https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem"
echo ""
echo "# Run with RDS backend"
echo "./bin/frenzy --listen :5432 \\"
echo "    --enable-tls \\"
echo "    --tls-cert test_certs/server.crt \\"
echo "    --tls-key test_certs/server.key \\"
echo "    --primary 'postgresql://user:pass@your-rds.region.rds.amazonaws.com:5432/db?sslmode=require&sslrootcert=rds-ca-certs.pem' \\"
echo "    --mirror 'postgresql://user:pass@your-mirror.region.rds.amazonaws.com:5432/db?sslmode=require&sslrootcert=rds-ca-certs.pem'"
echo ""
echo "=== Cleanup ==="
echo "To clean up test certificates: rm -rf test_certs/"
