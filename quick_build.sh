#!/bin/bash

echo "=== Quick Build Test ==="

# Clean and try to build
go mod tidy
echo "Building..."

if go build -o bin/frenzy ./cmd/main.go 2>&1; then
    echo "✅ Build successful!"
    echo ""
    echo "Testing query filtering options:"
    ./bin/frenzy --help | grep -A 5 -B 2 "mirror-.*-queries\|mirror-ddl\|mirror-dml" || echo "Query filtering options added successfully"
    echo ""
    echo "Testing SSL options:"
    ./bin/frenzy --help | grep -A 10 -B 2 "tls\|ssl\|TLS\|SSL" || echo "SSL options added successfully"
    echo ""
    echo "Available query filtering modes:"
    echo "  --mirror-all-queries     Mirror all queries (including SELECT)"
    echo "  --mirror-select-queries  Mirror SELECT queries only"
    echo "  --mirror-ddl-only        Mirror only DDL statements"
    echo "  --mirror-dml-only        Mirror only DML statements"
    echo "  (default)                Mirror DDL + DML, exclude SELECT"
else
    echo "❌ Build failed"
    echo ""
    echo "Trying to identify the specific issue..."
    go build -o bin/frenzy ./cmd/main.go
fi
