#!/bin/bash

echo "=== 测试 Frenzy 错误处理改进 ==="

# 重新编译
echo "1. 重新编译 Frenzy..."
go build -o bin/frenzy ./cmd/main.go

if [ $? -ne 0 ]; then
    echo "❌ 编译失败"
    exit 1
fi

echo "✅ 编译成功"

# 测试不使用 TLS
echo "2. 测试基本连接（无 TLS）..."
./bin/frenzy --listen :5433 \
    --primary "postgresql://dbmgr:password@172.31.38.228:5432/postgres" \
    --mirror "postgresql://dbmgr:password@172.31.38.228:5432/postgres" &

FRENZY_PID=$!
echo "Frenzy PID: $FRENZY_PID"
sleep 3

# 检查是否启动成功
if kill -0 $FRENZY_PID 2>/dev/null; then
    echo "✅ Frenzy 启动成功"
else
    echo "❌ Frenzy 启动失败"
    exit 1
fi

# 测试连接
echo "3. 测试连接..."
timeout 10 psql "host=localhost port=5433 dbname=postgres user=dbmgr sslmode=disable" -c "SELECT version();" 2>&1

# 检查 Frenzy 是否还在运行
if kill -0 $FRENZY_PID 2>/dev/null; then
    echo "✅ Frenzy 在连接后仍在运行"
    kill $FRENZY_PID
    echo "✅ 基本连接测试成功"
else
    echo "❌ Frenzy 在连接后崩溃"
fi

echo ""
echo "4. 测试 TLS 连接..."
./bin/frenzy --listen :5432 \
    --enable-tls \
    --tls-cert test_certs/server.crt \
    --tls-key test_certs/server.key \
    --tls-ca test_certs/ca.pem \
    --primary "postgresql://dbmgr:password@172.31.38.228:5432/postgres" \
    --mirror "postgresql://dbmgr:password@172.31.38.228:5432/postgres" &

FRENZY_TLS_PID=$!
echo "Frenzy TLS PID: $FRENZY_TLS_PID"
sleep 3

# 测试 TLS 连接
echo "5. 测试 TLS 连接..."
timeout 10 psql "host=localhost port=5432 dbname=postgres user=dbmgr sslmode=verify-full sslrootcert=test_certs/ca.pem" -c "SELECT 1;" 2>&1

# 检查 TLS Frenzy 状态
if kill -0 $FRENZY_TLS_PID 2>/dev/null; then
    echo "✅ TLS Frenzy 在连接后仍在运行"
    kill $FRENZY_TLS_PID
    echo "✅ TLS 连接测试完成"
else
    echo "❌ TLS Frenzy 在连接后崩溃"
fi

echo ""
echo "=== 测试完成 ==="
echo "现在 Frenzy 有了更详细的错误日志，可以帮助诊断问题"
echo "运行时请观察日志输出以获取详细的错误信息"
