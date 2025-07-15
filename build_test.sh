#!/bin/bash

echo "=== Testing Frenzy Build with SSL Support ==="

# Clean go modules cache for this project
echo "1. Cleaning go modules..."
go clean -cache
go clean -modcache 2>/dev/null || true

echo "2. Updating go modules..."
go mod tidy

echo "3. Downloading dependencies..."
go mod download

echo "4. Attempting to build frenzy..."
echo "   Using psql-wire version: $(grep 'psql-wire' go.mod)"

if go build -o bin/frenzy ./cmd/main.go; then
    echo "✓ Build successful with go build!"
    
    # Also try make if it exists
    if [ -f Makefile ]; then
        echo "5. Testing with make..."
        if make; then
            echo "✓ Make build also successful!"
        else
            echo "⚠ Make failed, but go build worked"
        fi
    fi
    
    echo ""
    echo "=== SSL Configuration Test ==="
    echo "Run the following to test SSL configuration:"
    echo ""
    echo "./bin/frenzy --help"
    echo ""
    echo "Look for the new SSL options:"
    echo "  --enable-tls"
    echo "  --tls-cert"
    echo "  --tls-key"
    echo "  --tls-ca"
    echo "  --tls-server-name"
    echo "  --tls-skip-verify"
    
else
    echo "✗ Build failed!"
    echo ""
    echo "Trying alternative psql-wire versions..."
    
    # Try version 0.3.0
    echo "Trying psql-wire v0.3.0..."
    sed -i.bak 's/psql-wire v[0-9]\+\.[0-9]\+\.[0-9]\+/psql-wire v0.3.0/' go.mod
    go mod tidy
    
    if go build -o bin/frenzy ./cmd/main.go; then
        echo "✓ Build successful with psql-wire v0.3.0!"
    else
        echo "✗ Still failing with v0.3.0"
        
        # Try version 0.2.0
        echo "Trying psql-wire v0.2.0..."
        sed -i.bak 's/psql-wire v[0-9]\+\.[0-9]\+\.[0-9]\+/psql-wire v0.2.0/' go.mod
        go mod tidy
        
        if go build -o bin/frenzy ./cmd/main.go; then
            echo "✓ Build successful with psql-wire v0.2.0!"
        else
            echo "✗ Build failed with multiple versions"
            echo ""
            echo "The psql-wire API has changed significantly."
            echo "You may need to check the original working version"
            echo "or adapt the code to the current API."
        fi
    fi
fi

echo ""
echo "=== Current go.mod status ==="
grep -A 5 -B 5 psql-wire go.mod
