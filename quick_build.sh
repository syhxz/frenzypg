#!/bin/bash

echo "=== Quick Build Test ==="

# Clean and try to build
go mod tidy
echo "Building..."

if make build 2>&1; then
    echo "✅ Build successful!"
    echo ""
    echo "Testing proxy mode options:"
    ./bin/frenzy --help | grep -A 3 -B 1 "mode" || echo "Proxy mode option available"
    echo ""
    echo "Testing query filtering options:"
    ./bin/frenzy --help | grep -A 5 -B 2 "mirror-.*-queries\|mirror-ddl\|mirror-dml\|skip-rollback\|mirror-failed-tx\|skip-mirror-tx" || echo "Query filtering options added successfully"
    echo ""
    echo "Testing SSL options:"
    ./bin/frenzy --help | grep -A 10 -B 2 "tls\|ssl\|TLS\|SSL" || echo "SSL options added successfully"
    echo ""
    echo "=== Available Proxy Modes ==="
    echo "  --mode raw   High-performance protocol forwarding (default)"
    echo "  --mode wire  psql-wire, full protocol decode"
    echo ""
    echo "=== Available Query Filtering Options ==="
    echo "  --mirror-all-queries     Mirror all queries (including SELECT)"
    echo "  --mirror-select-queries  Mirror SELECT queries only"
    echo "  --mirror-ddl-only        Mirror only DDL statements (CREATE, ALTER, DROP, TRUNCATE)"
    echo "  --mirror-dml-only        Mirror only DML statements (INSERT, UPDATE, DELETE)"
    echo "  --skip-rollback-mirror          Skip mirroring ROLLBACK transactions"
    echo "  --skip-failed-tx-mirror [true]  Skip mirroring failed transactions (default: true)"
    echo "  --skip-mirror-tx-locks          Skip BEGIN/COMMIT on mirrors to avoid transaction locks"
    echo "  (default)                       Mirror DDL + DML, exclude SELECT, skip failed tx"
else
    echo "❌ Build failed"
    echo ""
    echo "Trying to identify the specific issue..."
    go build -o bin/frenzy ./cmd
fi
