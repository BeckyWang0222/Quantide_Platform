#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
QMT增强版数据订阅器 - Windows端
基于QMT最佳实践的高性能数据订阅系统

主要改进：
1. 使用全推行情订阅 (subscribe_whole_quote)
2. 数据缓存和批量处理
3. 性能监控和质量检查
4. 更好的错误处理和恢复机制
"""

import json
import redis
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from xtquant import xtdata
import threading
import logging
import time
import traceback
import queue
from collections import defaultdict, deque
import os
import psutil


class QMTEnhancedSubscriber:
    """QMT增强版数据订阅器"""

    def __init__(self, config):
        """
        初始化增强版订阅器

        Args:
            config (dict): 配置字典
        """
        self.config = config
        self.logger = logging.getLogger(__name__)

        # Redis连接配置
        self.init_redis_connection(config)

        # QMT配置
        self.qmt_config = config.get('qmt', {})
        self.stock_list = []

        # 运行状态
        self.is_running = False
        self.subscription_mode = self.qmt_config.get('subscription_mode', 'whole_quote')  # whole_quote 或 individual

        # 数据缓存和队列
        self.data_queue = queue.Queue(maxsize=10000)
        self.data_cache = defaultdict(deque)  # 按股票代码缓存最近数据
        self.cache_size = self.qmt_config.get('cache_size', 100)

        # 批量处理配置
        self.batch_size = config.get('system', {}).get('batch_size', 100)
        self.batch_timeout = config.get('system', {}).get('batch_timeout', 1.0)

        # 性能监控
        self.stats = {
            'total_received': 0,
            'total_published': 0,
            'total_cached': 0,
            'publish_errors': 0,
            'data_quality_errors': 0,
            'last_publish_time': None,
            'start_time': datetime.now(),
            'subscription_count': 0
        }

        # 数据质量检查配置
        self.quality_check = self.qmt_config.get('quality_check', True)
        self.price_change_threshold = self.qmt_config.get('price_change_threshold', 0.2)  # 20%涨跌幅限制

        # 重试配置
        self.max_retry_times = config.get('system', {}).get('max_retry_times', 3)
        self.retry_delay = config.get('system', {}).get('retry_delay', 1)

    def init_redis_connection(self, config):
        """初始化Redis连接"""
        redis_config = config.get('redis', {})
        self.redis_client = redis.StrictRedis(
            host=redis_config.get('host', '8.217.201.221'),
            port=redis_config.get('port', 16379),
            password=redis_config.get('password', 'quantide666'),
            db=redis_config.get('db', 0),
            decode_responses=True,
            socket_connect_timeout=5,
            socket_timeout=5,
            retry_on_timeout=True,
            connection_pool=redis.ConnectionPool(
                max_connections=20,
                retry_on_timeout=True
            )
        )

    def get_all_stock_codes(self):
        """
        获取全市场股票代码
        使用QMT API获取完整的股票列表
        """
        try:
            all_stocks = []

            # 尝试使用QMT API获取股票列表
            try:
                # 获取沪深A股列表
                sz_stocks = xtdata.get_stock_list_in_sector('深圳A股')
                sh_stocks = xtdata.get_stock_list_in_sector('上海A股')

                if sz_stocks:
                    all_stocks.extend(sz_stocks)
                    self.logger.info(f"获取深圳A股: {len(sz_stocks)}只")

                if sh_stocks:
                    all_stocks.extend(sh_stocks)
                    self.logger.info(f"获取上海A股: {len(sh_stocks)}只")

            except Exception as e:
                self.logger.warning(f"QMT API获取股票列表失败: {e}")
                # 使用预定义列表作为备选
                all_stocks = self._generate_comprehensive_stock_list()

            # 添加主要指数
            indexes = [
                "000001.SH", "399001.SZ", "399006.SZ", "000300.SH",
                "000905.SH", "000852.SH", "688000.SH"
            ]
            all_stocks.extend(indexes)

            # 去重并排序
            all_stocks = sorted(list(set(all_stocks)))

            self.logger.info(f"最终获取股票代码: {len(all_stocks)}只")
            return all_stocks

        except Exception as e:
            self.logger.error(f"获取股票代码失败: {e}")
            return self._generate_comprehensive_stock_list()

    def _generate_comprehensive_stock_list(self):
        """生成更全面的股票代码列表"""
        stock_list = []

        # 深圳市场
        # 主板 000001-000999
        stock_list.extend([f"{i:06d}.SZ" for i in range(1, 1000)])
        # 中小板 002001-002999
        stock_list.extend([f"{i:06d}.SZ" for i in range(2001, 3000)])
        # 创业板 300001-301000
        stock_list.extend([f"{i:06d}.SZ" for i in range(300001, 301001)])

        # 上海市场
        # 主板 600000-605000
        stock_list.extend([f"{i:06d}.SH" for i in range(600000, 605001)])
        # 科创板 688001-689000
        stock_list.extend([f"{i:06d}.SH" for i in range(688001, 689001)])

        self.logger.info(f"生成预定义股票列表: {len(stock_list)}只")
        return stock_list

    def start_enhanced_subscription(self):
        """启动增强版订阅"""
        try:
            # 连接测试
            if not self.test_connections():
                self.logger.error("连接测试失败")
                return False

            # 获取股票列表
            self.logger.info("获取全市场股票代码...")
            self.stock_list = self.get_all_stock_codes()

            if not self.stock_list:
                self.logger.error("未能获取股票代码")
                return False

            # 应用数量限制
            max_count = self.qmt_config.get('max_subscribe_count', 1000)
            if len(self.stock_list) > max_count:
                self.logger.info(f"限制订阅数量: {len(self.stock_list)} -> {max_count}")
                self.stock_list = self.stock_list[:max_count]

            # 启动数据处理线程
            self.start_data_processing_threads()

            # 根据配置选择订阅模式
            if self.subscription_mode == 'whole_quote':
                success = self.start_whole_quote_subscription()
            else:
                success = self.start_individual_subscription()

            if success:
                self.is_running = True
                self.logger.info("增强版订阅启动成功")

                # 启动监控线程
                self.start_monitoring_threads()

                # 保持运行
                self.keep_running()

            return success

        except Exception as e:
            self.logger.error(f"启动订阅失败: {e}")
            self.logger.error(traceback.format_exc())
            return False

    def start_whole_quote_subscription(self):
        """启动全推行情订阅"""
        try:
            self.logger.info("尝试启动全推行情订阅...")

            # 使用全推行情订阅
            result = xtdata.subscribe_whole_quote(
                code_list=self.stock_list,
                callback=self.on_whole_quote_data
            )

            if result:
                self.stats['subscription_count'] = len(self.stock_list)
                self.logger.info(f"全推行情订阅成功: {len(self.stock_list)}只股票")
                return True
            else:
                self.logger.warning("全推行情订阅失败，切换到单股订阅模式")
                return self.start_individual_subscription()

        except Exception as e:
            self.logger.error(f"全推行情订阅失败: {e}")
            self.logger.info("切换到单股订阅模式...")
            return self.start_individual_subscription()

    def start_individual_subscription(self):
        """启动单股订阅模式"""
        try:
            self.logger.info("启动单股订阅模式...")

            subscription_count = 0
            max_subscriptions = min(len(self.stock_list), 200)  # 限制单股订阅数量

            for i, stock_code in enumerate(self.stock_list[:max_subscriptions]):
                try:
                    seq = xtdata.subscribe_quote(
                        stock_code=stock_code,
                        period='1m',
                        callback=self.on_individual_quote_data
                    )

                    if seq is not None:
                        subscription_count += 1

                    # 控制订阅频率
                    if (i + 1) % 20 == 0:
                        time.sleep(0.1)
                        self.logger.info(f"订阅进度: {i+1}/{max_subscriptions}")

                except Exception as e:
                    self.logger.debug(f"订阅{stock_code}失败: {e}")

            self.stats['subscription_count'] = subscription_count
            self.logger.info(f"单股订阅完成: {subscription_count}只股票")

            return subscription_count > 0

        except Exception as e:
            self.logger.error(f"单股订阅失败: {e}")
            return False

    def test_connections(self):
        """测试连接"""
        try:
            # 测试Redis
            self.redis_client.ping()
            self.logger.info("Redis连接正常")

            # 测试QMT
            xtdata.connect()
            test_data = xtdata.get_instrument_detail("000001.SZ")
            if test_data:
                self.logger.info("QMT连接正常")
                return True
            else:
                self.logger.error("QMT连接异常")
                return False

        except Exception as e:
            self.logger.error(f"连接测试失败: {e}")
            return False

    def on_whole_quote_data(self, data):
        """全推行情数据回调"""
        try:
            if data:
                self.stats['total_received'] += len(data)

                # 将数据放入队列进行批量处理
                for symbol, quote_data in data.items():
                    processed_data = self.process_quote_data(symbol, quote_data)
                    if processed_data:
                        self.data_queue.put(processed_data, timeout=0.1)

        except Exception as e:
            self.logger.error(f"处理全推行情数据失败: {e}")

    def on_individual_quote_data(self, data):
        """单股行情数据回调"""
        try:
            if data:
                self.stats['total_received'] += len(data)

                for symbol, quote_data in data.items():
                    processed_data = self.process_quote_data(symbol, quote_data)
                    if processed_data:
                        self.data_queue.put(processed_data, timeout=0.1)

        except Exception as e:
            self.logger.error(f"处理单股行情数据失败: {e}")

    def process_quote_data(self, symbol, quote_data):
        """处理行情数据"""
        try:
            # 数据格式标准化
            if isinstance(quote_data, dict):
                minute_bar = {
                    "symbol": symbol,
                    "frame": quote_data.get('time', datetime.now().isoformat()),
                    "open": float(quote_data.get('open', 0)),
                    "high": float(quote_data.get('high', 0)),
                    "low": float(quote_data.get('low', 0)),
                    "close": float(quote_data.get('close', 0)),
                    "vol": float(quote_data.get('volume', 0)),
                    "amount": float(quote_data.get('amount', 0)),
                    "timestamp": datetime.now().timestamp()
                }
            elif isinstance(quote_data, list) and len(quote_data) >= 6:
                minute_bar = {
                    "symbol": symbol,
                    "frame": str(quote_data[0]) if quote_data[0] else datetime.now().isoformat(),
                    "open": float(quote_data[1]) if len(quote_data) > 1 else 0,
                    "high": float(quote_data[2]) if len(quote_data) > 2 else 0,
                    "low": float(quote_data[3]) if len(quote_data) > 3 else 0,
                    "close": float(quote_data[4]) if len(quote_data) > 4 else 0,
                    "vol": float(quote_data[5]) if len(quote_data) > 5 else 0,
                    "amount": float(quote_data[6]) if len(quote_data) > 6 else 0,
                    "timestamp": datetime.now().timestamp()
                }
            else:
                return None

            # 数据质量检查
            if self.quality_check and not self.validate_data_quality(minute_bar):
                self.stats['data_quality_errors'] += 1
                return None

            # 更新缓存
            self.update_data_cache(symbol, minute_bar)

            return minute_bar

        except Exception as e:
            self.logger.debug(f"处理{symbol}数据失败: {e}")
            return None

    def validate_data_quality(self, minute_bar):
        """数据质量检查"""
        try:
            symbol = minute_bar['symbol']

            # 基本数据完整性检查
            if not all([minute_bar['open'], minute_bar['high'], minute_bar['low'], minute_bar['close']]):
                return False

            # 价格逻辑检查
            if minute_bar['high'] < minute_bar['low']:
                return False

            if not (minute_bar['low'] <= minute_bar['open'] <= minute_bar['high']):
                return False

            if not (minute_bar['low'] <= minute_bar['close'] <= minute_bar['high']):
                return False

            # 价格变动幅度检查（与缓存中的历史数据比较）
            if symbol in self.data_cache and len(self.data_cache[symbol]) > 0:
                last_data = self.data_cache[symbol][-1]
                last_close = last_data.get('close', 0)

                if last_close > 0:
                    price_change = abs(minute_bar['close'] - last_close) / last_close
                    if price_change > self.price_change_threshold:
                        self.logger.warning(f"{symbol}价格变动异常: {price_change:.2%}")
                        return False

            return True

        except Exception as e:
            self.logger.debug(f"数据质量检查失败: {e}")
            return True  # 检查失败时默认通过

    def update_data_cache(self, symbol, minute_bar):
        """更新数据缓存"""
        try:
            cache = self.data_cache[symbol]
            cache.append(minute_bar)

            # 限制缓存大小
            while len(cache) > self.cache_size:
                cache.popleft()

            self.stats['total_cached'] += 1

        except Exception as e:
            self.logger.debug(f"更新缓存失败: {e}")

    def start_data_processing_threads(self):
        """启动数据处理线程"""
        # 批量发布线程
        publish_thread = threading.Thread(target=self.batch_publish_worker, daemon=True)
        publish_thread.start()

        # 数据清理线程
        cleanup_thread = threading.Thread(target=self.data_cleanup_worker, daemon=True)
        cleanup_thread.start()

    def batch_publish_worker(self):
        """批量发布工作线程"""
        batch_data = []
        last_publish_time = time.time()

        while True:
            try:
                # 从队列获取数据
                try:
                    data = self.data_queue.get(timeout=0.1)
                    batch_data.append(data)
                except queue.Empty:
                    pass

                current_time = time.time()

                # 批量发布条件：达到批量大小或超时
                if (len(batch_data) >= self.batch_size or
                    (batch_data and current_time - last_publish_time >= self.batch_timeout)):

                    if batch_data:
                        self.batch_publish_to_redis(batch_data)
                        batch_data.clear()
                        last_publish_time = current_time

                if not self.is_running:
                    break

            except Exception as e:
                self.logger.error(f"批量发布工作线程错误: {e}")
                time.sleep(1)

    def batch_publish_to_redis(self, batch_data):
        """批量发布到Redis"""
        try:
            pipe = self.redis_client.pipeline()

            for minute_bar in batch_data:
                # 构造Redis键和值
                date_str = datetime.now().strftime('%Y-%m-%d')
                key = f"minute_bar:{minute_bar['symbol']}:{date_str}"
                value = json.dumps(minute_bar, ensure_ascii=False, default=str)

                # 添加到管道
                pipe.lpush(key, value)
                pipe.expire(key, 86400 * 7)  # 7天过期
                pipe.lpush("minute_bar_queue", value)

            # 执行批量操作
            pipe.execute()

            self.stats['total_published'] += len(batch_data)
            self.stats['last_publish_time'] = datetime.now()

        except Exception as e:
            self.stats['publish_errors'] += len(batch_data)
            self.logger.error(f"批量发布到Redis失败: {e}")

    def data_cleanup_worker(self):
        """数据清理工作线程"""
        while True:
            try:
                if self.is_running:
                    # 清理过期缓存
                    current_time = time.time()
                    for symbol in list(self.data_cache.keys()):
                        cache = self.data_cache[symbol]
                        # 清理超过1小时的数据
                        while cache and current_time - cache[0].get('timestamp', 0) > 3600:
                            cache.popleft()

                        # 如果缓存为空，删除该股票的缓存
                        if not cache:
                            del self.data_cache[symbol]

                time.sleep(300)  # 每5分钟清理一次

            except Exception as e:
                self.logger.error(f"数据清理工作线程错误: {e}")
                time.sleep(60)

    def start_monitoring_threads(self):
        """启动监控线程"""
        # 性能监控线程
        monitor_thread = threading.Thread(target=self.performance_monitor, daemon=True)
        monitor_thread.start()

        # 系统资源监控线程
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
                    receive_rate = self.stats['total_received'] / max(runtime_seconds, 1)
                    publish_rate = self.stats['total_published'] / max(runtime_seconds, 1)

                    # 计算成功率
                    total_processed = self.stats['total_received']
                    success_rate = (total_processed - self.stats['data_quality_errors']) / max(total_processed, 1) * 100

                    # 队列状态
                    queue_size = self.data_queue.qsize()
                    cache_count = len(self.data_cache)

                    # 打印性能报告
                    self.logger.info("=" * 60)
                    self.logger.info("性能监控报告")
                    self.logger.info("=" * 60)
                    self.logger.info(f"运行时间: {runtime}")
                    self.logger.info(f"订阅股票: {self.stats['subscription_count']}只")
                    self.logger.info(f"接收数据: {self.stats['total_received']}条 ({receive_rate:.1f}/秒)")
                    self.logger.info(f"发布数据: {self.stats['total_published']}条 ({publish_rate:.1f}/秒)")
                    self.logger.info(f"缓存数据: {self.stats['total_cached']}条")
                    self.logger.info(f"数据质量: {success_rate:.1f}%")
                    self.logger.info(f"发布错误: {self.stats['publish_errors']}次")
                    self.logger.info(f"队列大小: {queue_size}")
                    self.logger.info(f"缓存股票: {cache_count}只")
                    self.logger.info(f"最后发布: {self.stats['last_publish_time']}")
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

                    # 网络连接数
                    connections = len(process.connections())

                    # 如果资源使用过高，记录警告
                    if cpu_percent > 80:
                        self.logger.warning(f"CPU使用率过高: {cpu_percent:.1f}%")

                    if memory_mb > 1000:  # 超过1GB
                        self.logger.warning(f"内存使用过高: {memory_mb:.1f}MB")

                    if self.data_queue.qsize() > 5000:
                        self.logger.warning(f"数据队列积压: {self.data_queue.qsize()}")

                    # 定期记录资源使用情况
                    if int(time.time()) % 300 == 0:  # 每5分钟记录一次
                        self.logger.info(f"资源使用 - CPU: {cpu_percent:.1f}%, 内存: {memory_mb:.1f}MB, "
                                       f"连接数: {connections}, 系统CPU: {system_cpu:.1f}%, "
                                       f"系统内存: {system_memory:.1f}%")

                time.sleep(30)  # 每30秒检查一次

            except Exception as e:
                self.logger.error(f"资源监控错误: {e}")
                time.sleep(60)

    def keep_running(self):
        """保持程序运行"""
        try:
            self.logger.info("增强版QMT订阅器正在运行...")
            self.logger.info("按 Ctrl+C 停止程序")

            # 启动QMT数据接收
            xtdata.run()

        except KeyboardInterrupt:
            self.logger.info("收到停止信号")
            self.stop_subscription()
        except Exception as e:
            self.logger.error(f"运行时错误: {e}")
            self.stop_subscription()

    def stop_subscription(self):
        """停止订阅"""
        try:
            self.logger.info("正在停止增强版QMT订阅器...")
            self.is_running = False

            # 停止QMT订阅
            xtdata.unsubscribe_quote()

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
                self.batch_publish_to_redis(remaining_data)

            # 打印最终统计
            self.print_final_stats()

            self.logger.info("增强版QMT订阅器已停止")

        except Exception as e:
            self.logger.error(f"停止订阅时出错: {e}")

    def print_final_stats(self):
        """打印最终统计信息"""
        try:
            runtime = datetime.now() - self.stats['start_time']

            self.logger.info("=" * 60)
            self.logger.info("最终统计报告")
            self.logger.info("=" * 60)
            self.logger.info(f"总运行时间: {runtime}")
            self.logger.info(f"订阅股票数: {self.stats['subscription_count']}")
            self.logger.info(f"接收数据总数: {self.stats['total_received']}")
            self.logger.info(f"发布数据总数: {self.stats['total_published']}")
            self.logger.info(f"缓存数据总数: {self.stats['total_cached']}")
            self.logger.info(f"数据质量错误: {self.stats['data_quality_errors']}")
            self.logger.info(f"发布错误次数: {self.stats['publish_errors']}")

            if runtime.total_seconds() > 0:
                avg_receive_rate = self.stats['total_received'] / runtime.total_seconds()
                avg_publish_rate = self.stats['total_published'] / runtime.total_seconds()
                self.logger.info(f"平均接收速率: {avg_receive_rate:.2f} 条/秒")
                self.logger.info(f"平均发布速率: {avg_publish_rate:.2f} 条/秒")

            self.logger.info("=" * 60)

        except Exception as e:
            self.logger.error(f"打印统计信息失败: {e}")

    def get_performance_metrics(self):
        """获取性能指标（供外部调用）"""
        runtime = datetime.now() - self.stats['start_time']
        runtime_seconds = runtime.total_seconds()

        return {
            'runtime_seconds': runtime_seconds,
            'subscription_count': self.stats['subscription_count'],
            'total_received': self.stats['total_received'],
            'total_published': self.stats['total_published'],
            'total_cached': self.stats['total_cached'],
            'data_quality_errors': self.stats['data_quality_errors'],
            'publish_errors': self.stats['publish_errors'],
            'queue_size': self.data_queue.qsize(),
            'cache_count': len(self.data_cache),
            'receive_rate': self.stats['total_received'] / max(runtime_seconds, 1),
            'publish_rate': self.stats['total_published'] / max(runtime_seconds, 1),
            'success_rate': (self.stats['total_received'] - self.stats['data_quality_errors']) / max(self.stats['total_received'], 1) * 100
        }
