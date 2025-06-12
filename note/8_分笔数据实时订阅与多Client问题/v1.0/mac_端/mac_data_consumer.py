#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Mac端数据消费器

从远程Redis消费分钟线数据，存储到本地ClickHouse
"""

import json
import time
import redis
from clickhouse_driver import Client
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import logging
import traceback


class MacDataConsumer:
    """Mac端数据消费器"""
    
    def __init__(self, config):
        """
        初始化消费器
        
        Args:
            config (dict): 配置字典
        """
        # 连接远程Redis
        redis_config = config.get('redis', {})
        self.redis_client = redis.StrictRedis(
            host=redis_config.get('host', '8.217.201.221'),
            port=redis_config.get('port', 16379),
            password=redis_config.get('password'),
            db=redis_config.get('db', 0),
            decode_responses=True,
            socket_connect_timeout=5,
            socket_timeout=5,
            retry_on_timeout=True
        )
        
        # 连接本地ClickHouse
        ch_config = config.get('clickhouse', {})
        self.clickhouse_client = Client(
            host=ch_config.get('host', 'localhost'),
            port=ch_config.get('port', 9000),
            database=ch_config.get('database', 'market_data'),
            user=ch_config.get('user', 'default'),
            password=ch_config.get('password', '')
        )
        
        self.batch_size = config.get('system', {}).get('batch_size', 1000)
        self.max_retry_times = config.get('system', {}).get('max_retry_times', 3)
        self.logger = logging.getLogger(__name__)
        self.is_running = False
        
        # 统计信息
        self.stats = {
            'total_consumed': 0,
            'total_inserted': 0,
            'insert_errors': 0,
            'last_insert_time': None
        }
        
    def test_connections(self):
        """测试连接"""
        try:
            # 测试Redis连接
            self.redis_client.ping()
            self.logger.info("Redis连接测试成功")
            
            # 测试ClickHouse连接
            result = self.clickhouse_client.execute("SELECT 1")
            self.logger.info("ClickHouse连接测试成功")
            
            return True
        except Exception as e:
            self.logger.error(f"连接测试失败: {e}")
            return False
    
    def start_consuming(self, num_workers=4):
        """
        启动消费进程
        
        Args:
            num_workers (int): 工作线程数量
        """
        self.is_running = True
        self.logger.info("启动Mac端数据消费服务...")
        
        # 测试连接
        if not self.test_connections():
            self.logger.error("连接测试失败，无法启动消费")
            return
        
        # 启动统计信息打印线程
        stats_thread = threading.Thread(target=self.print_stats, daemon=True)
        stats_thread.start()
        
        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            futures = []
            for i in range(num_workers):
                future = executor.submit(self.consume_worker, f"worker-{i}")
                futures.append(future)
                
            # 等待所有工作线程完成
            for future in futures:
                future.result()
                
    def consume_worker(self, worker_name):
        """
        消费工作线程
        
        Args:
            worker_name (str): 工作线程名称
        """
        self.logger.info(f"启动消费工作线程: {worker_name}")
        
        batch_data = []
        last_insert_time = time.time()
        
        while self.is_running:
            try:
                # 从队列中获取数据
                result = self.redis_client.brpop("minute_bar_queue", timeout=1)
                
                if result is None:
                    # 队列为空，检查是否需要批量插入
                    if batch_data and (time.time() - last_insert_time) > 5:
                        self.batch_insert_clickhouse(batch_data)
                        batch_data = []
                        last_insert_time = time.time()
                    continue
                
                _, json_data = result
                minute_bar = json.loads(json_data)
                batch_data.append(minute_bar)
                self.stats['total_consumed'] += 1
                
                # 达到批量大小或超时，执行批量插入
                if len(batch_data) >= self.batch_size or (time.time() - last_insert_time) > 10:
                    self.batch_insert_clickhouse(batch_data)
                    batch_data = []
                    last_insert_time = time.time()
                    
            except Exception as e:
                self.logger.error(f"消费数据异常 [{worker_name}]: {e}")
                self.logger.error(traceback.format_exc())
                time.sleep(1)
                
        # 处理剩余数据
        if batch_data:
            self.batch_insert_clickhouse(batch_data)
            
    def batch_insert_clickhouse(self, batch_data):
        """
        批量插入ClickHouse
        
        Args:
            batch_data (list): 批量数据列表
        """
        if not batch_data:
            return
            
        retry_count = 0
        
        while retry_count < self.max_retry_times:
            try:
                query = """
                INSERT INTO minute_bars 
                (symbol, frame, open, high, low, close, vol, amount, created_at)
                VALUES
                """
                
                values = []
                for bar in batch_data:
                    # 处理时间字段
                    frame_time = bar['frame']
                    if isinstance(frame_time, str):
                        try:
                            frame_time = datetime.fromisoformat(frame_time.replace('Z', '+00:00'))
                        except:
                            frame_time = datetime.now()
                    
                    values.append((
                        bar['symbol'],
                        frame_time,
                        float(bar['open']),
                        float(bar['high']),
                        float(bar['low']),
                        float(bar['close']),
                        float(bar['vol']),
                        float(bar['amount']),
                        datetime.now()
                    ))
                
                self.clickhouse_client.execute(query, values)
                
                # 更新统计信息
                self.stats['total_inserted'] += len(batch_data)
                self.stats['last_insert_time'] = datetime.now()
                
                self.logger.info(f"成功插入{len(batch_data)}条分钟线数据")
                break
                
            except Exception as e:
                retry_count += 1
                self.stats['insert_errors'] += 1
                self.logger.error(f"批量插入ClickHouse失败 (重试 {retry_count}/{self.max_retry_times}): {e}")
                
                if retry_count < self.max_retry_times:
                    time.sleep(1)  # 等待1秒后重试
                else:
                    self.logger.error(f"批量插入最终失败，丢弃{len(batch_data)}条数据")
                    
    def print_stats(self):
        """打印统计信息"""
        import threading
        
        while self.is_running:
            try:
                time.sleep(60)  # 每分钟打印一次
                self.logger.info(
                    f"统计信息 - 消费: {self.stats['total_consumed']}, "
                    f"插入: {self.stats['total_inserted']}, "
                    f"错误: {self.stats['insert_errors']}, "
                    f"最后插入: {self.stats['last_insert_time']}"
                )
            except Exception as e:
                self.logger.error(f"打印统计信息失败: {e}")
                
    def stop_consuming(self):
        """停止消费"""
        self.is_running = False
        self.logger.info("停止数据消费")
