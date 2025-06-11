#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
增强版数据消费器 - Mac端

基于最佳实践的高性能数据消费和存储系统
"""

import json
import redis
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from clickhouse_driver import Client
import threading
import logging
import time
import traceback
import queue
from collections import defaultdict, deque
import os
import psutil


class EnhancedDataConsumer:
    """增强版数据消费器"""

    def __init__(self, config):
        """
        初始化增强版消费器

        Args:
            config (dict): 配置字典
        """
        self.config = config
        self.logger = logging.getLogger(__name__)

        # Redis连接
        self.init_redis_connection(config)

        # ClickHouse连接
        self.init_clickhouse_connection(config)

        # 运行状态
        self.is_running = False

        # 数据队列和缓存
        self.data_queue = queue.Queue(maxsize=20000)
        self.batch_data = defaultdict(list)  # 按股票分组的批量数据

        # 批量处理配置
        self.batch_size = config.get('system', {}).get('batch_size', 1000)
        self.batch_timeout = config.get('system', {}).get('batch_timeout', 5.0)
        self.worker_count = config.get('system', {}).get('worker_count', 4)

        # 性能统计
        self.stats = {
            'total_consumed': 0,
            'total_inserted': 0,
            'insert_errors': 0,
            'data_quality_errors': 0,
            'last_insert_time': None,
            'start_time': datetime.now(),
            'processed_symbols': set()
        }

        # 数据质量检查
        self.quality_check = config.get('data', {}).get('quality_check', True)

        # 重试配置
        self.max_retry_times = config.get('system', {}).get('max_retry_times', 3)

    def init_redis_connection(self, config):
        """初始化Redis连接"""
        redis_config = config.get('redis', {})
        self.redis_client = redis.StrictRedis(
            host=redis_config.get('host', '8.217.201.221'),
            port=redis_config.get('port', 16379),
            password=redis_config.get('password', 'quantide666'),
            db=redis_config.get('db', 0),
            decode_responses=True,
            socket_connect_timeout=10,
            socket_timeout=10,
            retry_on_timeout=True,
            connection_pool=redis.ConnectionPool(
                max_connections=20,
                retry_on_timeout=True
            )
        )

    def init_clickhouse_connection(self, config):
        """初始化ClickHouse连接"""
        ch_config = config.get('clickhouse', {})
        self.clickhouse_client = Client(
            host=ch_config.get('host', 'localhost'),
            port=ch_config.get('port', 9000),
            user=ch_config.get('user', 'default'),
            password=ch_config.get('password', ''),
            database=ch_config.get('database', 'default'),
            settings={
                'max_execution_time': 300,
                'max_memory_usage': 10000000000,
                'use_numpy': True
            }
        )

    def start_enhanced_consumption(self):
        """启动增强版数据消费"""
        try:
            # 连接测试
            if not self.test_connections():
                self.logger.error("连接测试失败")
                return False

            # 初始化数据库表
            self.init_database_tables()

            # 启动工作线程
            self.start_worker_threads()

            # 启动监控线程
            self.start_monitoring_threads()

            self.is_running = True
            self.logger.info("增强版数据消费器启动成功")

            # 开始消费数据
            self.consume_data_loop()

            return True

        except Exception as e:
            self.logger.error(f"启动消费失败: {e}")
            self.logger.error(traceback.format_exc())
            return False

    def test_connections(self):
        """测试连接"""
        try:
            # 测试Redis
            self.redis_client.ping()
            self.logger.info("Redis连接正常")

            # 测试ClickHouse
            result = self.clickhouse_client.execute('SELECT 1')
            if result == [(1,)]:
                self.logger.info("ClickHouse连接正常")
                return True
            else:
                self.logger.error("ClickHouse连接异常")
                return False

        except Exception as e:
            self.logger.error(f"连接测试失败: {e}")
            return False

    def init_database_tables(self):
        """初始化数据库表"""
        try:
            # 创建增强版分钟线表
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS minute_bars_enhanced (
                symbol String,
                frame DateTime,
                open Float64,
                high Float64,
                low Float64,
                close Float64,
                vol Float64,
                amount Float64,
                timestamp Float64,
                data_source String DEFAULT 'qmt_enhanced',
                quality_score Float32 DEFAULT 1.0,
                created_at DateTime DEFAULT now()
            ) ENGINE = MergeTree()
            PARTITION BY toYYYYMM(frame)
            ORDER BY (symbol, frame)
            SETTINGS index_granularity = 8192
            """

            self.clickhouse_client.execute(create_table_sql)
            self.logger.info("数据库表初始化完成")

            # 创建物化视图用于数据聚合
            self.create_materialized_views()

        except Exception as e:
            self.logger.error(f"初始化数据库表失败: {e}")
            raise

    def create_materialized_views(self):
        """创建物化视图"""
        try:
            # 5分钟聚合视图
            mv_5min_sql = """
            CREATE MATERIALIZED VIEW IF NOT EXISTS minute_bars_5min_enhanced_mv
            TO minute_bars_5min_enhanced
            AS SELECT
                symbol,
                toStartOfInterval(frame, INTERVAL 5 MINUTE) as frame,
                argMin(open, frame) as open,
                max(high) as high,
                min(low) as low,
                argMax(close, frame) as close,
                sum(vol) as vol,
                sum(amount) as amount,
                avg(quality_score) as avg_quality_score,
                now() as created_at
            FROM minute_bars_enhanced
            GROUP BY symbol, toStartOfInterval(frame, INTERVAL 5 MINUTE)
            """

            # 创建5分钟聚合表
            create_5min_table_sql = """
            CREATE TABLE IF NOT EXISTS minute_bars_5min_enhanced (
                symbol String,
                frame DateTime,
                open Float64,
                high Float64,
                low Float64,
                close Float64,
                vol Float64,
                amount Float64,
                avg_quality_score Float32,
                created_at DateTime
            ) ENGINE = MergeTree()
            PARTITION BY toYYYYMM(frame)
            ORDER BY (symbol, frame)
            """

            self.clickhouse_client.execute(create_5min_table_sql)
            self.clickhouse_client.execute(mv_5min_sql)

            self.logger.info("物化视图创建完成")

        except Exception as e:
            self.logger.warning(f"创建物化视图失败: {e}")

    def consume_data_loop(self):
        """数据消费主循环"""
        self.logger.info("开始消费Redis数据...")

        while self.is_running:
            try:
                # 从Redis队列获取数据
                data = self.redis_client.brpop("minute_bar_queue", timeout=1)

                if data:
                    queue_name, json_data = data

                    try:
                        minute_bar = json.loads(json_data)

                        # 数据质量检查
                        if self.quality_check:
                            quality_score = self.calculate_quality_score(minute_bar)
                            minute_bar['quality_score'] = quality_score

                            if quality_score < 0.5:  # 质量分数过低
                                self.stats['data_quality_errors'] += 1
                                continue

                        # 添加到处理队列
                        self.data_queue.put(minute_bar, timeout=0.1)
                        self.stats['total_consumed'] += 1
                        self.stats['processed_symbols'].add(minute_bar.get('symbol', ''))

                    except json.JSONDecodeError as e:
                        self.logger.warning(f"JSON解析失败: {e}")
                    except queue.Full:
                        self.logger.warning("数据队列已满，丢弃数据")

            except Exception as e:
                self.logger.error(f"消费数据失败: {e}")
                time.sleep(1)

    def calculate_quality_score(self, minute_bar):
        """计算数据质量分数"""
        try:
            score = 1.0

            # 检查必要字段
            required_fields = ['symbol', 'open', 'high', 'low', 'close', 'vol']
            for field in required_fields:
                if field not in minute_bar or minute_bar[field] is None:
                    score -= 0.2

            # 检查价格逻辑
            try:
                open_price = float(minute_bar.get('open', 0))
                high_price = float(minute_bar.get('high', 0))
                low_price = float(minute_bar.get('low', 0))
                close_price = float(minute_bar.get('close', 0))

                if high_price < low_price:
                    score -= 0.3

                if not (low_price <= open_price <= high_price):
                    score -= 0.2

                if not (low_price <= close_price <= high_price):
                    score -= 0.2

            except (ValueError, TypeError):
                score -= 0.3

            # 检查成交量
            try:
                volume = float(minute_bar.get('vol', 0))
                if volume < 0:
                    score -= 0.1
            except (ValueError, TypeError):
                score -= 0.1

            return max(0.0, min(1.0, score))

        except Exception as e:
            self.logger.debug(f"计算质量分数失败: {e}")
            return 0.5  # 默认中等质量

    def start_worker_threads(self):
        """启动工作线程"""
        # 批量插入工作线程
        for i in range(self.worker_count):
            worker_thread = threading.Thread(
                target=self.batch_insert_worker,
                args=(f"worker-{i}",),
                daemon=True
            )
            worker_thread.start()

        # 数据清理线程
        cleanup_thread = threading.Thread(target=self.data_cleanup_worker, daemon=True)
        cleanup_thread.start()

    def batch_insert_worker(self, worker_name):
        """批量插入工作线程"""
        batch_data = []
        last_insert_time = time.time()

        self.logger.info(f"启动批量插入工作线程: {worker_name}")

        while True:
            try:
                # 从队列获取数据
                try:
                    data = self.data_queue.get(timeout=0.5)
                    batch_data.append(data)
                except queue.Empty:
                    pass

                current_time = time.time()

                # 批量插入条件：达到批量大小或超时
                if (len(batch_data) >= self.batch_size or
                    (batch_data and current_time - last_insert_time >= self.batch_timeout)):

                    if batch_data:
                        self.batch_insert_to_clickhouse(batch_data, worker_name)
                        batch_data.clear()
                        last_insert_time = current_time

                if not self.is_running:
                    # 处理剩余数据
                    if batch_data:
                        self.batch_insert_to_clickhouse(batch_data, worker_name)
                    break

            except Exception as e:
                self.logger.error(f"批量插入工作线程{worker_name}错误: {e}")
                time.sleep(1)

    def batch_insert_to_clickhouse(self, batch_data, worker_name):
        """批量插入到ClickHouse"""
        try:
            if not batch_data:
                return

            # 准备批量数据
            insert_data = []
            for minute_bar in batch_data:
                try:
                    # 数据格式转换
                    row = (
                        minute_bar.get('symbol', ''),
                        self.parse_datetime(minute_bar.get('frame')),
                        float(minute_bar.get('open', 0)),
                        float(minute_bar.get('high', 0)),
                        float(minute_bar.get('low', 0)),
                        float(minute_bar.get('close', 0)),
                        float(minute_bar.get('vol', 0)),
                        float(minute_bar.get('amount', 0)),
                        float(minute_bar.get('timestamp', time.time())),
                        'qmt_enhanced',
                        float(minute_bar.get('quality_score', 1.0)),
                        datetime.now()
                    )
                    insert_data.append(row)

                except Exception as e:
                    self.logger.debug(f"数据格式转换失败: {e}")
                    continue

            if not insert_data:
                return

            # 执行批量插入
            insert_sql = """
            INSERT INTO minute_bars_enhanced
            (symbol, frame, open, high, low, close, vol, amount, timestamp, data_source, quality_score, created_at)
            VALUES
            """

            self.clickhouse_client.execute(insert_sql, insert_data)

            self.stats['total_inserted'] += len(insert_data)
            self.stats['last_insert_time'] = datetime.now()

            self.logger.debug(f"{worker_name} 批量插入 {len(insert_data)} 条数据")

        except Exception as e:
            self.stats['insert_errors'] += len(batch_data)
            self.logger.error(f"{worker_name} 批量插入失败: {e}")

    def parse_datetime(self, frame_str):
        """解析时间字符串"""
        try:
            if isinstance(frame_str, str):
                # 尝试多种时间格式
                formats = [
                    '%Y-%m-%d %H:%M:%S',
                    '%Y-%m-%dT%H:%M:%S',
                    '%Y%m%d%H%M%S'
                ]

                for fmt in formats:
                    try:
                        return datetime.strptime(frame_str, fmt)
                    except ValueError:
                        continue

                # 如果都失败，返回当前时间
                return datetime.now()
            else:
                return datetime.now()

        except Exception as e:
            self.logger.debug(f"时间解析失败: {e}")
            return datetime.now()

    def data_cleanup_worker(self):
        """数据清理工作线程"""
        while True:
            try:
                if self.is_running:
                    # 清理过期数据
                    cleanup_date = datetime.now() - timedelta(days=30)

                    cleanup_sql = """
                    ALTER TABLE minute_bars_enhanced
                    DELETE WHERE frame < %(cleanup_date)s
                    """

                    try:
                        self.clickhouse_client.execute(cleanup_sql, {'cleanup_date': cleanup_date})
                        self.logger.info(f"清理{cleanup_date}之前的数据")
                    except Exception as e:
                        self.logger.warning(f"数据清理失败: {e}")

                time.sleep(86400)  # 每天清理一次

            except Exception as e:
                self.logger.error(f"数据清理工作线程错误: {e}")
                time.sleep(3600)

    def start_monitoring_threads(self):
        """启动监控线程"""
        # 性能监控线程
        monitor_thread = threading.Thread(target=self.performance_monitor, daemon=True)
        monitor_thread.start()

        # 资源监控线程
        resource_thread = threading.Thread(target=self.resource_monitor, daemon=True)
        resource_thread.start()

    def performance_monitor(self):
        """性能监控"""
        while True:
            try:
                if self.is_running:
                    # 计算性能指标
                    runtime = datetime.now() - self.stats['start_time']
                    runtime_seconds = runtime.total_seconds()

                    # 计算速率
                    consume_rate = self.stats['total_consumed'] / max(runtime_seconds, 1)
                    insert_rate = self.stats['total_inserted'] / max(runtime_seconds, 1)

                    # 计算成功率
                    success_rate = self.stats['total_inserted'] / max(self.stats['total_consumed'], 1) * 100

                    # 队列状态
                    queue_size = self.data_queue.qsize()
                    symbol_count = len(self.stats['processed_symbols'])

                    # 打印性能报告
                    self.logger.info("=" * 60)
                    self.logger.info("Mac端性能监控报告")
                    self.logger.info("=" * 60)
                    self.logger.info(f"运行时间: {runtime}")
                    self.logger.info(f"消费数据: {self.stats['total_consumed']}条 ({consume_rate:.1f}/秒)")
                    self.logger.info(f"插入数据: {self.stats['total_inserted']}条 ({insert_rate:.1f}/秒)")
                    self.logger.info(f"成功率: {success_rate:.1f}%")
                    self.logger.info(f"插入错误: {self.stats['insert_errors']}次")
                    self.logger.info(f"质量错误: {self.stats['data_quality_errors']}次")
                    self.logger.info(f"队列大小: {queue_size}")
                    self.logger.info(f"处理股票: {symbol_count}只")
                    self.logger.info(f"最后插入: {self.stats['last_insert_time']}")
                    self.logger.info("=" * 60)

                time.sleep(60)  # 每分钟报告一次

            except Exception as e:
                self.logger.error(f"性能监控错误: {e}")
                time.sleep(60)

    def resource_monitor(self):
        """系统资源监控"""
        while True:
            try:
                if self.is_running:
                    # 获取系统资源信息
                    process = psutil.Process()

                    # CPU和内存使用率
                    cpu_percent = process.cpu_percent()
                    memory_info = process.memory_info()
                    memory_mb = memory_info.rss / 1024 / 1024

                    # 系统整体资源
                    system_cpu = psutil.cpu_percent()
                    system_memory = psutil.virtual_memory().percent

                    # 如果资源使用过高，记录警告
                    if cpu_percent > 80:
                        self.logger.warning(f"CPU使用率过高: {cpu_percent:.1f}%")

                    if memory_mb > 2000:  # 超过2GB
                        self.logger.warning(f"内存使用过高: {memory_mb:.1f}MB")

                    if self.data_queue.qsize() > 10000:
                        self.logger.warning(f"数据队列积压: {self.data_queue.qsize()}")

                    # 定期记录资源使用情况
                    if int(time.time()) % 300 == 0:  # 每5分钟记录一次
                        self.logger.info(f"Mac端资源使用 - CPU: {cpu_percent:.1f}%, 内存: {memory_mb:.1f}MB, "
                                       f"系统CPU: {system_cpu:.1f}%, 系统内存: {system_memory:.1f}%")

                time.sleep(30)  # 每30秒检查一次

            except Exception as e:
                self.logger.error(f"资源监控错误: {e}")
                time.sleep(60)

    def stop_consumption(self):
        """停止数据消费"""
        try:
            self.logger.info("正在停止增强版数据消费器...")
            self.is_running = False

            # 处理剩余队列数据
            remaining_data = []
            while not self.data_queue.empty():
                try:
                    data = self.data_queue.get_nowait()
                    remaining_data.append(data)
                except queue.Empty:
                    break

            if remaining_data:
                self.logger.info(f"处理剩余数据: {len(remaining_data)}条")
                self.batch_insert_to_clickhouse(remaining_data, "final")

            # 打印最终统计
            self.print_final_stats()

            self.logger.info("增强版数据消费器已停止")

        except Exception as e:
            self.logger.error(f"停止消费时出错: {e}")

    def print_final_stats(self):
        """打印最终统计信息"""
        try:
            runtime = datetime.now() - self.stats['start_time']

            self.logger.info("=" * 60)
            self.logger.info("Mac端最终统计报告")
            self.logger.info("=" * 60)
            self.logger.info(f"总运行时间: {runtime}")
            self.logger.info(f"消费数据总数: {self.stats['total_consumed']}")
            self.logger.info(f"插入数据总数: {self.stats['total_inserted']}")
            self.logger.info(f"插入错误次数: {self.stats['insert_errors']}")
            self.logger.info(f"质量错误次数: {self.stats['data_quality_errors']}")
            self.logger.info(f"处理股票数量: {len(self.stats['processed_symbols'])}")

            if runtime.total_seconds() > 0:
                avg_consume_rate = self.stats['total_consumed'] / runtime.total_seconds()
                avg_insert_rate = self.stats['total_inserted'] / runtime.total_seconds()
                self.logger.info(f"平均消费速率: {avg_consume_rate:.2f} 条/秒")
                self.logger.info(f"平均插入速率: {avg_insert_rate:.2f} 条/秒")

            self.logger.info("=" * 60)

        except Exception as e:
            self.logger.error(f"打印统计信息失败: {e}")
