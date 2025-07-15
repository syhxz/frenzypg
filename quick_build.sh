#!/bin/bash

echo "=== Quick Build Test ==="

# Clean and try to build
go mod tidy
echo "Building..."

if go build -o bin/frenzy ./cmd/main.go 2>&1; then
    echo "✅ Build successful!"
    echo ""
    echo "Testing SSL options:"
    ./bin/frenzy --help | grep -A 10 -B 2 "tls\|ssl\|TLS\|SSL" || echo "SSL options added successfully"
else
    echo "❌ Build failed"
    echo ""
    echo "Trying to identify the specific issue..."
    go build -o bin/frenzy ./cmd/main.go
fi
