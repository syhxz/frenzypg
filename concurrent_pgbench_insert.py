#!/usr/bin/env python3
"""
PostgreSQL pgbench_accounts 表并发写入脚本
支持多进程并发写入10万条数据
"""

import psycopg2
from psycopg2 import pool
import multiprocessing as mp
import random
import time
import logging
from typing import List, Dict, Any
import argparse
import sys
import io
import csv
import os

# 配置参数
TOTAL_RECORDS = 100_000  # 10万条记录
BATCH_SIZE = 500         # 减少批次大小，每批次500条（原来1000条）
DEFAULT_PARALLEL_DEGREE = 4  # 默认并发度

# 数据库配置
DB_CONFIG = {
    'host': 'localhost',
    'database': 'postgres',
    'user': 'postgres',
    'password': 'password',
    'port': 5432,
    # TLS/SSL 配置 (可选)
    'sslmode': None,        # disable, allow, prefer, require, verify-ca, verify-full
    'sslcert': None,        # 客户端证书文件路径
    'sslkey': None,         # 客户端私钥文件路径
    'sslrootcert': None,    # CA根证书文件路径
    'sslcrl': None,         # 证书撤销列表文件路径 (可选)
}

def setup_logging():
    """设置日志"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(processName)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('pgbench_insert.log'),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

def validate_tls_config(db_config: Dict[str, Any]) -> Dict[str, Any]:
    """验证和清理TLS配置"""
    config = db_config.copy()
    
    # 验证SSL模式
    valid_ssl_modes = ['disable', 'allow', 'prefer', 'require', 'verify-ca', 'verify-full']
    if config.get('sslmode') and config['sslmode'] not in valid_ssl_modes:
        logging.warning(f"无效的SSL模式: {config['sslmode']}, 使用默认值 'require'")
        config['sslmode'] = 'require'
    
    # 检查证书文件是否存在
    cert_files = {
        'sslcert': '客户端证书',
        'sslkey': '客户端私钥', 
        'sslrootcert': 'CA根证书',
        'sslcrl': '证书撤销列表'
    }
    
    for cert_param, desc in cert_files.items():
        if config.get(cert_param):
            if not os.path.exists(config[cert_param]):
                logging.warning(f"{desc}文件不存在: {config[cert_param]}")
                config[cert_param] = None
            else:
                logging.info(f"✓ 找到{desc}文件: {config[cert_param]}")
    
    # 移除None值以避免psycopg2错误
    config = {k: v for k, v in config.items() if v is not None}
    
    return config

def verify_ssl_connection(conn: psycopg2.extensions.connection, ssl_mode: str = None) -> bool:
    """验证SSL连接状态 - 不使用SQL查询"""
    try:
        # 方法1: 检查连接对象的SSL属性
        if hasattr(conn, 'ssl_in_use'):
            return conn.ssl_in_use
        
        # 方法2: 检查连接信息
        conn_info = conn.get_dsn_parameters()
        if conn_info.get('sslmode') in ['require', 'verify-ca', 'verify-full']:
            # 如果要求SSL且连接成功，则认为SSL已启用
            return True
        
        # 方法3: 根据配置推断
        if ssl_mode in ['require', 'verify-ca', 'verify-full']:
            return True
        
        return False
        
    except Exception as e:
        logging.debug(f"无法验证SSL状态: {e}")
        # 如果无法确定，根据配置保守估计
        return ssl_mode in ['require', 'verify-ca', 'verify-full'] if ssl_mode else False

def create_secure_connection(db_config: Dict[str, Any]) -> psycopg2.extensions.connection:
    """创建安全的PostgreSQL连接"""
    try:
        # 验证和清理配置
        clean_config = validate_tls_config(db_config)
        
        # 添加连接超时设置
        if 'connect_timeout' not in clean_config:
            clean_config['connect_timeout'] = 10  # 10秒超时
        
        print(f"正在连接到 {clean_config['host']}:{clean_config['port']}/{clean_config['database']}...")
        
        # 建立连接
        conn = psycopg2.connect(**clean_config)
        
        # 验证SSL连接状态 - 不使用SQL查询
        ssl_mode = clean_config.get('sslmode')
        ssl_active = verify_ssl_connection(conn, ssl_mode)
        
        if ssl_active:
            logging.info(f"✓ 安全SSL/TLS连接已建立 (模式: {ssl_mode or '默认'})")
        else:
            if ssl_mode in ['require', 'verify-ca', 'verify-full']:
                logging.warning("⚠ 要求SSL连接但可能未激活SSL/TLS")
            else:
                logging.info("○ 数据库连接已建立 (未配置SSL)")
        
        return conn
        
    except Exception as e:
        print(f"✗ 创建安全连接失败: {e}")
        logging.error(f"创建安全连接失败: {e}")
        raise

def test_secure_connection(db_config: Dict[str, Any]) -> bool:
    """测试安全数据库连接"""
    try:
        print("正在测试数据库连接...")
        conn = create_secure_connection(db_config)
        cursor = conn.cursor()
        # 使用简单的查询测试连接
        cursor.execute("SELECT current_database(), current_user;")
        db_info = cursor.fetchone()
        cursor.close()
        conn.close()
        print(f"✓ 数据库连接测试成功: 数据库={db_info[0]}, 用户={db_info[1]}")
        logging.info(f"✓ 数据库连接测试成功: 数据库={db_info[0]}, 用户={db_info[1]}")
        return True
    except Exception as e:
        print(f"✗ 数据库连接测试失败: {e}")
        logging.error(f"✗ 数据库连接测试失败: {e}")
        return False

class DataGenerator:
    """数据生成器"""
    
    def __init__(self, start_aid: int):
        self.current_aid = start_aid
        
    def generate_record(self) -> tuple:
        """生成单条记录"""
        aid = self.current_aid
        bid = random.randint(1, 100)  # 分支ID 1-100
        abalance = random.randint(0, 1000000)  # 账户余额 0-100万
        # filler字段：84个字符的填充数据
        filler = ''.join(random.choices('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 ', k=84))
        
        self.current_aid += 1
        return (aid, bid, abalance, filler)
    
    def generate_batch(self, batch_size: int) -> List[tuple]:
        """生成一批数据"""
        return [self.generate_record() for _ in range(batch_size)]

def data_generator_worker(start_aid: int, record_count: int, result_queue: mp.Queue, worker_id: int):
    """数据生成工作进程 - 改进版本"""
    logger = logging.getLogger(f"Generator-{worker_id}")
    generator = DataGenerator(start_aid)
    
    batch_records = []
    generated_count = 0
    
    try:
        logger.info(f"生成器 {worker_id} 开始，目标记录数: {record_count}")
        
        while generated_count < record_count:
            try:
                remaining = min(BATCH_SIZE, record_count - generated_count)
                batch = generator.generate_batch(remaining)
                batch_records.extend(batch)
                generated_count += len(batch)
                
                if len(batch_records) >= BATCH_SIZE:
                    # 尝试发送批次，带超时
                    try:
                        result_queue.put(('batch', batch_records, worker_id), timeout=30)
                        batch_records = []
                    except:
                        logger.error(f"生成器 {worker_id} 队列发送超时")
                        break
                    
                    if generated_count % 5000 == 0:
                        logger.info(f"生成器 {worker_id} 已生成 {generated_count:,} 条记录")
                        
            except Exception as e:
                logger.error(f"生成器 {worker_id} 生成数据时出错: {e}")
                break
        
        # 发送剩余记录
        if batch_records:
            try:
                result_queue.put(('batch', batch_records, worker_id), timeout=30)
            except:
                logger.error(f"生成器 {worker_id} 发送剩余批次超时")
        
        # 发送完成信号
        try:
            result_queue.put(('done', worker_id, None), timeout=10)
        except:
            logger.error(f"生成器 {worker_id} 发送完成信号超时")
            
        logger.info(f"生成器 {worker_id} 完成，共生成 {generated_count:,} 条记录")
        
    except Exception as e:
        logger.error(f"生成器 {worker_id} 失败: {e}")
        try:
            result_queue.put(('error', str(e), worker_id), timeout=5)
        except:
            pass

class DatabaseWriter:
    """数据库写入器"""
    
    def __init__(self, db_config: Dict[str, Any], writer_id: int):
        self.db_config = db_config
        self.writer_id = writer_id
        self.logger = logging.getLogger(f"Writer-{writer_id}")
        self.conn = None
        self.cursor = None
        
    def connect(self) -> bool:
        """连接数据库 - 带重试机制"""
        max_retries = 3
        retry_delay = 1
        
        for attempt in range(max_retries):
            try:
                # 确保先关闭旧连接
                self.close()
                
                self.conn = create_secure_connection(self.db_config)
                self.cursor = self.conn.cursor()
                self.conn.autocommit = False
                
                # 优化连接设置
                self.cursor.execute("SET work_mem = '64MB';")  # 减少内存使用
                self.cursor.execute("SET maintenance_work_mem = '64MB';")
                # 添加连接保活设置
                self.cursor.execute("SET tcp_keepalives_idle = 600;")
                self.cursor.execute("SET tcp_keepalives_interval = 30;")
                self.cursor.execute("SET tcp_keepalives_count = 3;")
                self.conn.commit()
                
                self.logger.info(f"安全数据库连接成功 (尝试 {attempt + 1})")
                return True
                
            except Exception as e:
                self.logger.error(f"数据库连接失败 (尝试 {attempt + 1}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                    retry_delay *= 2
                else:
                    return False
        
        return False
    
    def insert_batch_copy(self, records: List[tuple]) -> bool:
        """使用COPY命令批量插入数据"""
        if not self.conn or not self.cursor:
            if not self.connect():
                return False
        
        try:
            # 创建CSV缓冲区
            csv_buffer = io.StringIO()
            csv_writer = csv.writer(csv_buffer, delimiter='\t', quoting=csv.QUOTE_MINIMAL)
            
            for record in records:
                csv_writer.writerow(record)
            
            csv_buffer.seek(0)
            
            # 使用COPY命令
            self.cursor.copy_from(
                csv_buffer,
                'pgbench_accounts',
                columns=('aid', 'bid', 'abalance', 'filler'),
                sep='\t'
            )
            
            self.conn.commit()
            csv_buffer.close()
            return True
            
        except Exception as e:
            self.logger.error(f"COPY插入失败: {e}")
            if self.conn:
                try:
                    self.conn.rollback()
                except:
                    pass
            return False
    
    def insert_batch_executemany(self, records: List[tuple]) -> bool:
        """使用executemany批量插入数据 - 带重试和连接恢复"""
        max_retries = 3
        retry_delay = 1
        
        for attempt in range(max_retries):
            try:
                if not self.conn or not self.cursor:
                    if not self.connect():
                        if attempt < max_retries - 1:
                            self.logger.warning(f"连接失败，{retry_delay}秒后重试...")
                            time.sleep(retry_delay)
                            retry_delay *= 2
                            continue
                        return False
                
                # 对于executemany，使用较小的批次以避免服务器压力
                chunk_size = min(50, len(records))  # 每次最多50条记录
                
                insert_sql = """
                    INSERT INTO pgbench_accounts (aid, bid, abalance, filler) 
                    VALUES (%s, %s, %s, %s)
                """
                
                # 分块插入
                for i in range(0, len(records), chunk_size):
                    chunk = records[i:i + chunk_size]
                    
                    try:
                        self.cursor.executemany(insert_sql, chunk)
                        self.conn.commit()
                    except psycopg2.OperationalError as e:
                        self.logger.error(f"连接错误，尝试重新连接: {e}")
                        # 重新连接
                        self.close()
                        if not self.connect():
                            raise e
                        # 重试当前块
                        self.cursor.executemany(insert_sql, chunk)
                        self.conn.commit()
                
                return True
                
            except psycopg2.OperationalError as e:
                self.logger.error(f"连接操作错误 (尝试 {attempt + 1}): {e}")
                # 强制重新连接
                self.close()
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                    retry_delay *= 2
                    continue
                return False
                
            except psycopg2.InterfaceError as e:
                self.logger.error(f"接口错误 (尝试 {attempt + 1}): {e}")
                # 强制重新连接
                self.close()
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                    retry_delay *= 2
                    continue
                return False
                
            except Exception as e:
                self.logger.error(f"executemany插入失败 (尝试 {attempt + 1}): {e}")
                if self.conn:
                    try:
                        self.conn.rollback()
                    except:
                        # 连接可能已断开，强制重新连接
                        self.close()
                
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                    retry_delay *= 2
                    continue
                return False
        
        return False
    
    def close(self):
        """关闭连接 - 改进的清理方法"""
        try:
            if self.cursor:
                try:
                    self.cursor.close()
                except:
                    pass
                self.cursor = None
                
            if self.conn:
                try:
                    if not self.conn.closed:
                        self.conn.close()
                except:
                    pass
                self.conn = None
                
            self.logger.debug("数据库连接已关闭")
        except Exception as e:
            self.logger.error(f"关闭连接时出错: {e}")
            # 强制设置为None
            self.cursor = None
            self.conn = None

def database_writer_worker(batch_queue: mp.Queue, db_config: Dict[str, Any], 
                          writer_id: int, stats_queue: mp.Queue, use_copy: bool = True):
    """数据库写入工作进程"""
    logger = logging.getLogger(f"Writer-{writer_id}")
    writer = DatabaseWriter(db_config, writer_id)
    
    try:
        batches_processed = 0
        records_processed = 0
        
        while True:
            try:
                batch_data = batch_queue.get(timeout=30)
                
                if batch_data is None:  # 关闭信号
                    break
                
                batch_type, records, source_worker = batch_data
                
                if batch_type == 'batch':
                    # 选择插入方法
                    if use_copy:
                        success = writer.insert_batch_copy(records)
                    else:
                        success = writer.insert_batch_executemany(records)
                    
                    if success:
                        batches_processed += 1
                        records_processed += len(records)
                        stats_queue.put(('written', len(records), writer_id))
                        
                        if batches_processed % 10 == 0:
                            logger.info(f"已处理 {batches_processed} 批次，{records_processed:,} 条记录")
                    else:
                        logger.error(f"写入批次失败，来源工作器 {source_worker}")
                        stats_queue.put(('error', len(records), writer_id))
                
            except Exception as e:
                logger.error(f"写入器错误: {e}")
                stats_queue.put(('error', 0, writer_id))
        
        logger.info(f"写入器 {writer_id} 完成: {batches_processed} 批次，{records_processed:,} 条记录")
        
    except Exception as e:
        logger.error(f"写入器 {writer_id} 失败: {e}")
    finally:
        writer.close()

class ConcurrentPgbenchInserter:
    """并发插入协调器"""
    
    def __init__(self, db_config: Dict[str, Any], parallel_degree: int, use_copy: bool = True):
        self.db_config = db_config
        self.parallel_degree = parallel_degree
        self.use_copy = use_copy
        self.logger = setup_logging()
    
    def create_table_if_not_exists(self):
        """创建表（如果不存在）"""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS pgbench_accounts (
            aid INTEGER NOT NULL,
            bid INTEGER,
            abalance INTEGER,
            filler CHAR(84)
        );
        
        -- 创建索引
        CREATE INDEX IF NOT EXISTS idx_pgbench_accounts_aid ON pgbench_accounts(aid);
        CREATE INDEX IF NOT EXISTS idx_pgbench_accounts_bid ON pgbench_accounts(bid);
        """
        
        try:
            conn = create_secure_connection(self.db_config)
            cursor = conn.cursor()
            cursor.execute(create_table_sql)
            conn.commit()
            cursor.close()
            conn.close()
            self.logger.info("表和索引创建或验证完成")
        except Exception as e:
            self.logger.error(f"创建表失败: {e}")
            raise
    
    def run_concurrent_insert(self):
        """执行并发插入"""
        print("正在初始化并发插入...")
        self.logger.info(f"开始并发数据插入，并发度: {self.parallel_degree}")
        self.logger.info(f"目标记录数: {TOTAL_RECORDS:,}")
        self.logger.info(f"使用方法: {'COPY' if self.use_copy else 'executemany'}")
        
        try:
            print("正在创建表...")
            self.create_table_if_not_exists()
            print("✓ 表创建完成")
            
            # 计算工作分配
            records_per_worker = TOTAL_RECORDS // self.parallel_degree
            print(f"每个工作进程处理 {records_per_worker:,} 条记录")
            
            # 创建队列
            batch_queue = mp.Queue(maxsize=self.parallel_degree * 5)
            stats_queue = mp.Queue()
            
            print("正在启动数据库写入进程...")
            # 启动数据库写入进程
            writer_processes = []
            for i in range(self.parallel_degree):
                p = mp.Process(
                    target=database_writer_worker,
                    args=(batch_queue, self.db_config, i, stats_queue, self.use_copy)
                )
                p.start()
                writer_processes.append(p)
            print(f"✓ 启动了 {len(writer_processes)} 个写入进程")
            
            print("正在启动数据生成进程...")
            # 启动数据生成进程
            generator_processes = []
            current_aid = 1
            
            for i in range(self.parallel_degree):
                record_count = records_per_worker
                if i == self.parallel_degree - 1:  # 最后一个进程处理剩余记录
                    record_count = TOTAL_RECORDS - (records_per_worker * (self.parallel_degree - 1))
                
                p = mp.Process(
                    target=data_generator_worker,
                    args=(current_aid, record_count, batch_queue, i)
                )
                p.start()
                generator_processes.append(p)
                current_aid += record_count
            print(f"✓ 启动了 {len(generator_processes)} 个生成进程")
            
            print("开始监控进度...")
            # 监控进度
            self.monitor_progress(generator_processes, writer_processes, batch_queue, stats_queue)
            
        except Exception as e:
            print(f"✗ 并发插入失败: {e}")
            self.logger.error(f"并发插入失败: {e}")
            raise
    
    def monitor_progress(self, generator_processes, writer_processes, batch_queue, stats_queue):
        """监控进度"""
        start_time = time.time()
        total_written = 0
        completed_generators = 0
        last_progress_time = start_time
        
        print("开始数据生成和插入...")
        
        try:
            while completed_generators < len(generator_processes) or not batch_queue.empty():
                try:
                    # 检查完成的生成器
                    for i, p in enumerate(generator_processes):
                        if not p.is_alive() and p.exitcode is not None:
                            if not hasattr(p, '_completed'):
                                completed_generators += 1
                                p._completed = True
                                print(f"✓ 生成器 {i} 完成")
                                self.logger.info(f"生成器 {i} 完成")
                    
                    # 处理统计信息
                    stats_processed = 0
                    try:
                        while stats_processed < 100:  # 限制每次处理的统计数量
                            stat_type, count, worker_id = stats_queue.get_nowait()
                            stats_processed += 1
                            
                            if stat_type == 'written':
                                total_written += count
                                
                                # 每1000条记录或每5秒显示一次进度
                                current_time = time.time()
                                if (total_written % 1000 == 0) or (current_time - last_progress_time > 5):
                                    elapsed_time = current_time - start_time
                                    rate = total_written / elapsed_time if elapsed_time > 0 else 0
                                    remaining = (TOTAL_RECORDS - total_written) / rate if rate > 0 else 0
                                    
                                    progress_msg = (
                                        f"进度: {total_written:,}/{TOTAL_RECORDS:,} "
                                        f"({total_written/TOTAL_RECORDS*100:.1f}%) - "
                                        f"速度: {rate:.0f} 记录/秒 - "
                                        f"预计剩余: {remaining:.0f} 秒"
                                    )
                                    print(progress_msg)
                                    self.logger.info(progress_msg)
                                    last_progress_time = current_time
                            elif stat_type == 'error':
                                print(f"⚠ 写入器 {worker_id} 报告错误")
                                self.logger.warning(f"写入器 {worker_id} 报告错误")
                    except:
                        pass  # 队列为空
                    
                    # 检查是否所有进程都还活着
                    dead_processes = []
                    for i, p in enumerate(generator_processes + writer_processes):
                        if not p.is_alive() and p.exitcode != 0:
                            dead_processes.append(f"进程-{i}")
                    
                    if dead_processes:
                        print(f"⚠ 发现异常退出的进程: {', '.join(dead_processes)}")
                    
                    time.sleep(0.1)  # 短暂休眠避免CPU占用过高
                    
                except KeyboardInterrupt:
                    print("\n收到中断信号，正在关闭...")
                    self.logger.info("收到中断信号，正在关闭...")
                    break
            
            print("等待所有生成器完成...")
            # 等待所有生成器完成
            for i, p in enumerate(generator_processes):
                p.join(timeout=30)
                if p.is_alive():
                    print(f"强制终止生成器 {i}")
                    p.terminate()
            
            print("发送关闭信号给写入器...")
            # 发送关闭信号给写入器
            for _ in writer_processes:
                try:
                    batch_queue.put(None, timeout=5)
                except:
                    pass
            
            print("等待写入器完成...")
            # 等待写入器完成
            for i, p in enumerate(writer_processes):
                p.join(timeout=30)
                if p.is_alive():
                    print(f"强制终止写入器 {i}")
                    p.terminate()
            
            total_time = time.time() - start_time
            final_msg = (
                f"并发插入完成！"
                f"总记录数: {total_written:,} "
                f"总时间: {total_time:.2f} 秒 "
                f"平均速度: {total_written/total_time:.0f} 记录/秒"
            )
            print(final_msg)
            self.logger.info(final_msg)
            
        except Exception as e:
            print(f"✗ 监控失败: {e}")
            self.logger.error(f"监控失败: {e}")
            # 清理进程
            for p in generator_processes + writer_processes:
                if p.is_alive():
                    p.terminate()

def main():
    """主函数"""
    # 声明全局变量
    global TOTAL_RECORDS, BATCH_SIZE
    
    parser = argparse.ArgumentParser(description='PostgreSQL pgbench_accounts 并发插入脚本 (支持TLS)')
    parser.add_argument('--parallel-degree', '-p', type=int, default=DEFAULT_PARALLEL_DEGREE,
                       help=f'并发进程数 (默认: {DEFAULT_PARALLEL_DEGREE})')
    parser.add_argument('--records', '-r', type=int, default=TOTAL_RECORDS,
                       help=f'总记录数 (默认: {TOTAL_RECORDS:,})')
    parser.add_argument('--batch-size', '-b', type=int, default=BATCH_SIZE,
                       help=f'批次大小 (默认: {BATCH_SIZE:,})')
    
    # 数据库连接参数
    parser.add_argument('--host', default=DB_CONFIG['host'], help='数据库主机')
    parser.add_argument('--database', '-d', default=DB_CONFIG['database'], help='数据库名')
    parser.add_argument('--user', '-u', default=DB_CONFIG['user'], help='数据库用户')
    parser.add_argument('--password', default=DB_CONFIG['password'], help='数据库密码')
    parser.add_argument('--port', type=int, default=DB_CONFIG['port'], help='数据库端口')
    
    # TLS/SSL 参数
    tls_group = parser.add_argument_group('TLS/SSL 选项')
    tls_group.add_argument('--ssl-mode', 
                          choices=['disable', 'allow', 'prefer', 'require', 'verify-ca', 'verify-full'],
                          help='SSL连接模式')
    tls_group.add_argument('--ssl-cert', help='客户端证书文件路径')
    tls_group.add_argument('--ssl-key', help='客户端私钥文件路径')
    tls_group.add_argument('--ssl-root-cert', help='CA根证书文件路径')
    tls_group.add_argument('--ssl-crl', help='证书撤销列表文件路径')
    
    # 其他参数
    parser.add_argument('--use-executemany', action='store_true', 
                       help='使用executemany而不是COPY (COPY更快)')
    parser.add_argument('--auto-confirm', action='store_true', help='跳过确认提示')
    
    args = parser.parse_args()
    
    # 更新全局配置
    TOTAL_RECORDS = args.records
    BATCH_SIZE = args.batch_size
    
    # 更新数据库配置
    db_config = {
        'host': args.host,
        'database': args.database,
        'user': args.user,
        'password': args.password or os.getenv('PGPASSWORD'),
        'port': args.port
    }
    
    # 添加TLS配置
    tls_enabled = False
    if args.ssl_mode:
        db_config['sslmode'] = args.ssl_mode
        tls_enabled = True
    if args.ssl_cert:
        db_config['sslcert'] = args.ssl_cert
        tls_enabled = True
    if args.ssl_key:
        db_config['sslkey'] = args.ssl_key
        tls_enabled = True
    if args.ssl_root_cert:
        db_config['sslrootcert'] = args.ssl_root_cert
        tls_enabled = True
    if args.ssl_crl:
        db_config['sslcrl'] = args.ssl_crl
        tls_enabled = True
    
    print("PostgreSQL pgbench_accounts 并发插入脚本 (支持TLS)")
    print(f"目标记录数: {TOTAL_RECORDS:,}")
    print(f"并发度: {args.parallel_degree}")
    print(f"批次大小: {BATCH_SIZE:,}")
    print(f"数据库: {args.host}:{args.port}/{args.database}")
    print(f"插入方法: {'executemany' if args.use_executemany else 'COPY'}")
    if tls_enabled:
        print(f"TLS/SSL: 启用 (模式: {args.ssl_mode or '默认'})")
        if args.ssl_cert:
            print(f"客户端证书: {args.ssl_cert}")
        if args.ssl_root_cert:
            print(f"CA根证书: {args.ssl_root_cert}")
    else:
        print("TLS/SSL: 未配置")
    print(f"预计内存使用: ~{args.parallel_degree * BATCH_SIZE * 200 / 1024 / 1024:.1f} MB")
    
    if not args.auto_confirm:
        response = input("\n是否开始并发数据插入? (y/N): ")
        if response.lower() != 'y':
            print("插入已取消")
            return
    
    # 验证并发度
    max_parallel = mp.cpu_count()
    if args.parallel_degree > max_parallel:
        print(f"警告: 并发度 {args.parallel_degree} 超过CPU核心数 {max_parallel}")
        if not args.auto_confirm:
            response = input("是否继续? (y/N): ")
            if response.lower() != 'y':
                return
    
    # 测试数据库连接
    print("正在测试数据库连接...")
    if not test_secure_connection(db_config):
        print("请检查数据库配置和TLS证书设置")
        sys.exit(1)
    
    print("开始并发插入...")
    # 开始并发插入
    try:
        inserter = ConcurrentPgbenchInserter(db_config, args.parallel_degree, not args.use_executemany)
        inserter.run_concurrent_insert()
    except Exception as e:
        print(f"✗ 插入过程中发生错误: {e}")
        logging.error(f"插入过程中发生错误: {e}")
        sys.exit(1)

if __name__ == "__main__":
    # 设置多进程启动方法 - 使用fork而不是spawn来避免卡住
    try:
        mp.set_start_method('fork', force=True)
    except RuntimeError:
        # 如果fork不可用（如在Windows上），使用spawn
        mp.set_start_method('spawn', force=True)
    
    main()

"""
TLS使用示例:

1. 基本SSL连接:
python concurrent_pgbench_insert.py --ssl-mode require

2. 使用客户端证书认证:
python concurrent_pgbench_insert.py \
    --ssl-mode verify-full \
    --ssl-cert /path/to/client.crt \
    --ssl-key /path/to/client.key \
    --ssl-root-cert /path/to/ca.crt

3. AWS RDS SSL连接:
python concurrent_pgbench_insert.py \
    --ssl-mode require \
    --ssl-root-cert /path/to/rds-ca-2019-root.pem

4. 完整示例:
python concurrent_pgbench_insert.py \
    --host your-db-host.com \
    --port 5432 \
    --database mydb \
    --user myuser \
    --password mypass \
    --ssl-mode verify-full \
    --ssl-cert /etc/ssl/certs/client.crt \
    --ssl-key /etc/ssl/private/client.key \
    --ssl-root-cert /etc/ssl/certs/ca.crt \
    --records 100000 \
    --parallel-degree 4 \
    --batch-size 5000

SSL模式说明:
- disable: 禁用SSL
- allow: 如果服务器支持则使用SSL
- prefer: 优先使用SSL，但允许非SSL连接
- require: 要求SSL连接
- verify-ca: 要求SSL并验证CA证书
- verify-full: 要求SSL并完全验证证书
"""
