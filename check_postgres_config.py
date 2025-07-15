#!/usr/bin/env python3
"""
PostgreSQL配置检查工具
检查可能导致连接断开的配置问题
"""

import psycopg2
import sys

DB_CONFIG = {
    'host': 'localhost',
    'database': 'postgres',
    'user': 'postgres',
    'password': 'password',  # 请修改
    'port': 5432,
    'sslmode': 'disable',
    'connect_timeout': 10
}

def check_postgres_config():
    """检查PostgreSQL配置"""
    try:
        print("PostgreSQL配置检查工具")
        print("=" * 50)
        
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # 检查关键配置
        configs_to_check = [
            ('max_connections', '最大连接数'),
            ('shared_buffers', '共享缓冲区'),
            ('work_mem', '工作内存'),
            ('maintenance_work_mem', '维护工作内存'),
            ('checkpoint_timeout', '检查点超时'),
            ('wal_buffers', 'WAL缓冲区'),
            ('effective_cache_size', '有效缓存大小'),
            ('random_page_cost', '随机页面成本'),
            ('tcp_keepalives_idle', 'TCP保活空闲时间'),
            ('tcp_keepalives_interval', 'TCP保活间隔'),
            ('tcp_keepalives_count', 'TCP保活计数'),
            ('statement_timeout', '语句超时'),
            ('lock_timeout', '锁超时'),
            ('idle_in_transaction_session_timeout', '事务空闲超时'),
        ]
        
        print("当前PostgreSQL配置:")
        print("-" * 30)
        
        for config_name, description in configs_to_check:
            try:
                cursor.execute(f"SHOW {config_name};")
                value = cursor.fetchone()[0]
                print(f"{description:20} ({config_name:30}): {value}")
            except Exception as e:
                print(f"{description:20} ({config_name:30}): 无法获取 - {e}")
        
        # 检查当前连接数
        print("\n连接状态:")
        print("-" * 30)
        cursor.execute("""
            SELECT 
                count(*) as total_connections,
                count(*) FILTER (WHERE state = 'active') as active_connections,
                count(*) FILTER (WHERE state = 'idle') as idle_connections
            FROM pg_stat_activity;
        """)
        conn_stats = cursor.fetchone()
        print(f"总连接数: {conn_stats[0]}")
        print(f"活跃连接: {conn_stats[1]}")
        print(f"空闲连接: {conn_stats[2]}")
        
        # 检查数据库版本
        cursor.execute("SELECT version();")
        version = cursor.fetchone()[0]
        print(f"\nPostgreSQL版本: {version}")
        
        # 检查表空间
        cursor.execute("SELECT pg_size_pretty(pg_database_size(current_database()));")
        db_size = cursor.fetchone()[0]
        print(f"数据库大小: {db_size}")
        
        # 建议的优化配置
        print("\n建议的配置优化:")
        print("-" * 30)
        print("1. 如果经常出现连接断开，考虑调整:")
        print("   - tcp_keepalives_idle = 600")
        print("   - tcp_keepalives_interval = 30") 
        print("   - tcp_keepalives_count = 3")
        print("   - statement_timeout = 0 (或适当值)")
        print("   - idle_in_transaction_session_timeout = 0")
        
        print("\n2. 对于批量插入优化:")
        print("   - work_mem = 64MB (会话级别)")
        print("   - maintenance_work_mem = 256MB")
        print("   - checkpoint_timeout = 15min")
        print("   - wal_buffers = 16MB")
        
        print("\n3. 连接池设置:")
        print("   - max_connections 应该足够大")
        print("   - 考虑使用连接池工具如pgbouncer")
        
        cursor.close()
        conn.close()
        
        print("\n✅ 配置检查完成")
        
    except Exception as e:
        print(f"❌ 配置检查失败: {e}")
        return False
    
    return True

def test_bulk_insert():
    """测试批量插入稳定性"""
    try:
        print("\n批量插入稳定性测试:")
        print("-" * 30)
        
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # 创建测试表
        cursor.execute("DROP TABLE IF EXISTS test_bulk_insert;")
        cursor.execute("""
            CREATE TABLE test_bulk_insert (
                id INTEGER,
                data TEXT
            );
        """)
        conn.commit()
        
        # 测试不同批次大小
        batch_sizes = [10, 50, 100, 500, 1000]
        
        for batch_size in batch_sizes:
            try:
                # 生成测试数据
                test_data = [(i, f'test_data_{i}' * 10) for i in range(batch_size)]
                
                # 插入数据
                cursor.executemany(
                    "INSERT INTO test_bulk_insert (id, data) VALUES (%s, %s)",
                    test_data
                )
                conn.commit()
                
                print(f"✅ 批次大小 {batch_size:4d}: 成功")
                
                # 清理数据
                cursor.execute("DELETE FROM test_bulk_insert;")
                conn.commit()
                
            except Exception as e:
                print(f"❌ 批次大小 {batch_size:4d}: 失败 - {e}")
        
        # 清理测试表
        cursor.execute("DROP TABLE test_bulk_insert;")
        conn.commit()
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ 批量插入测试失败: {e}")

if __name__ == "__main__":
    if check_postgres_config():
        test_bulk_insert()
    else:
        sys.exit(1)
