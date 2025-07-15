#!/bin/bash

echo "=== Frenzy Debug 模式启动（包含连接和 TLS 握手日志）==="

# 重新编译
echo "1. 重新编译 Frenzy..."
go build -o bin/frenzy ./cmd/main.go

if [ $? -ne 0 ]; then
    echo "❌ 编译失败"
    exit 1
fi

echo "✅ 编译成功"

# 设置调试环境变量
export FRENZY_LOG_LEVEL=debug
export GODEBUG=gctrace=1

echo "2. 启动 Frenzy (Debug 模式)..."
echo "   日志级别: DEBUG"
echo "   连接日志: 启用"
echo "   TLS 握手日志: 启用"
echo "   数据传输日志: 启用"
echo ""

# 启动 Frenzy
./bin/frenzy --listen :5432 \
    --enable-tls \
    --tls-cert test_certs/server.crt \
    --tls-key test_certs/server.key \
    --tls-ca test_certs/ca.pem \
    --primary "postgresql://dbmgr:password@172.31.38.228:5432/postgres" \
    --mirror "postgresql://dbmgr:password@172.31.38.228:5432/postgres"
