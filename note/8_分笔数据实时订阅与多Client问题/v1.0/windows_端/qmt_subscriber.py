#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
QMT分钟线数据订阅器 - Windows端

从QMT实时订阅分钟线数据，发送到远程Redis服务器
"""

import json
import redis
from datetime import datetime
from xtquant import xtdata
import threading
import logging
import time
import traceback


class QMTMinuteSubscriber:
    """QMT分钟线数据订阅器 - Windows端"""

    def __init__(self, config):
        """
        初始化订阅器

        Args:
            config (dict): 配置字典
        """
        # 连接远程Redis
        self.redis_host = '8.217.201.221'
        self.redis_port = 16379
        self.redis_password = 'quantide666'
        self.redis_client = redis.StrictRedis(
            host=config.get('redis', {}).get('host', self.redis_host),
            port=config.get('redis', {}).get('port', self.redis_port),
            password=config.get('redis', {}).get('password', self.redis_password),
            db=config.get('redis', {}).get('db', 0),
            decode_responses=True,
            socket_connect_timeout=5,
            socket_timeout=5,
            retry_on_timeout=True
        )

        self.qmt_config = config.get('qmt', {})
        self.stock_list = []  # 将在初始化时动态获取
        self.logger = logging.getLogger(__name__)
        self.is_running = False
        self.publish_interval = config.get('system', {}).get('publish_interval', 1)
        self.max_retry_times = config.get('system', {}).get('max_retry_times', 3)

        # 统计信息
        self.stats = {
            'total_received': 0,
            'total_published': 0,
            'publish_errors': 0,
            'last_publish_time': None
        }

    def _generate_common_stock_list(self):
        """
        生成常用股票代码列表

        Returns:
            list: 股票代码列表
        """
        stock_list = []

        # 深圳市场股票代码 (000001-003999)
        # 主板 000001-000999
        for i in range(1, 1000):
            stock_list.append(f"{i:06d}.SZ")

        # 中小板 002001-002999
        for i in range(2001, 3000):
            stock_list.append(f"{i:06d}.SZ")

        # 创业板 300001-300999
        for i in range(300001, 301000):
            stock_list.append(f"{i:06d}.SZ")

        # 上海市场股票代码
        # 主板 600000-603999
        for i in range(600000, 604000):
            stock_list.append(f"{i:06d}.SH")

        # 科创板 688000-688999
        for i in range(688000, 689000):
            stock_list.append(f"{i:06d}.SH")

        # 常用指数
        indexes = [
            "000001.SH",  # 上证指数
            "399001.SZ",  # 深证成指
            "399006.SZ",  # 创业板指
            "000300.SH",  # 沪深300
            "000905.SH",  # 中证500
            "000852.SH",  # 中证1000
        ]
        stock_list.extend(indexes)

        self.logger.info(f"生成股票代码: 深圳{1000+999+1000}只, 上海{4000+1000}只, 指数{len(indexes)}只")
        return stock_list

    def get_all_stock_list(self):
        """
        获取全市场股票列表

        Returns:
            list: 股票代码列表
        """
        try:
            from xtquant import xtdata

            stock_list_mode = self.qmt_config.get('stock_list_mode', 'all')
            self.logger.info(f"股票列表获取模式: {stock_list_mode}")

            if stock_list_mode == 'config':
                # 从配置文件获取手动指定的股票列表
                stock_list = self.qmt_config.get('manual_stock_list', [])
                self.logger.info(f"从配置获取股票列表: {len(stock_list)}只")
                return stock_list

            elif stock_list_mode == 'file':
                # 从文件读取股票列表
                stock_list_file = self.qmt_config.get('stock_list_file', 'stock_list.txt')
                try:
                    with open(stock_list_file, 'r', encoding='utf-8') as f:
                        stock_list = [line.strip() for line in f.readlines() if line.strip()]
                    self.logger.info(f"从文件{stock_list_file}获取股票列表: {len(stock_list)}只")
                    return stock_list
                except FileNotFoundError:
                    self.logger.error(f"股票列表文件{stock_list_file}不存在，切换到全市场模式")
                    stock_list_mode = 'all'

            if stock_list_mode == 'all':
                # 获取全市场股票列表 - 使用预定义列表方法
                self.logger.info("使用预定义股票列表（因为QMT sector API可能不可用）")

                # 生成常用股票代码列表
                all_stocks = self._generate_common_stock_list()

                self.logger.info(f"生成股票列表: {len(all_stocks)}只")

                # 验证股票代码有效性（可选，但会比较慢）
                if self.qmt_config.get('validate_stocks', False):
                    self.logger.info("开始验证股票代码有效性...")
                    valid_stocks = []
                    for i, stock in enumerate(all_stocks):
                        if i % 100 == 0:
                            self.logger.info(f"验证进度: {i}/{len(all_stocks)}")
                        try:
                            stock_info = xtdata.get_instrument_detail(stock)
                            if stock_info and stock_info.get('InstrumentName'):
                                valid_stocks.append(stock)
                        except:
                            pass
                    all_stocks = valid_stocks
                    self.logger.info(f"验证完成，有效股票: {len(all_stocks)}只")

                # 应用订阅数量限制
                max_count = self.qmt_config.get('max_subscribe_count', 0)
                if max_count > 0 and len(all_stocks) > max_count:
                    self.logger.warning(f"股票数量{len(all_stocks)}超过限制{max_count}，将截取前{max_count}只")
                    all_stocks = all_stocks[:max_count]

                self.logger.info(f"最终获取股票列表: {len(all_stocks)}只")
                return all_stocks

        except Exception as e:
            self.logger.error(f"获取股票列表失败: {e}")
            # 返回默认股票列表
            default_stocks = ["000001.SZ", "000002.SZ", "600519.SH", "000858.SZ"]
            self.logger.warning(f"使用默认股票列表: {default_stocks}")
            return default_stocks

    def apply_stock_filters(self, stock_list):
        """
        应用股票过滤规则

        Args:
            stock_list (list): 原始股票列表

        Returns:
            list: 过滤后的股票列表
        """
        try:
            from xtquant import xtdata

            filters = self.qmt_config.get('filters', {})
            exclude_st = filters.get('exclude_st', False)
            exclude_suspended = filters.get('exclude_suspended', True)
            min_market_cap = filters.get('min_market_cap', 0)

            filtered_stocks = []

            for stock in stock_list:
                try:
                    # 获取股票基本信息
                    stock_info = xtdata.get_instrument_detail(stock)
                    if not stock_info:
                        continue

                    stock_name = stock_info.get('InstrumentName', '')

                    # ST股票过滤
                    if exclude_st and ('ST' in stock_name or '*ST' in stock_name):
                        continue

                    # 停牌股票过滤
                    if exclude_suspended:
                        # 获取最新行情判断是否停牌
                        latest_data = xtdata.get_full_tick([stock])
                        if latest_data and stock in latest_data:
                            tick_data = latest_data[stock]
                            # 如果没有最新价或成交量为0，可能是停牌
                            if not tick_data.get('lastPrice') or tick_data.get('volume', 0) == 0:
                                continue

                    # 市值过滤 (这里简化处理，实际可能需要更复杂的逻辑)
                    if min_market_cap > 0:
                        # 可以通过其他接口获取市值信息进行过滤
                        pass

                    filtered_stocks.append(stock)

                except Exception as e:
                    self.logger.debug(f"过滤股票{stock}时出错: {e}")
                    # 出错时保留该股票
                    filtered_stocks.append(stock)

            self.logger.info(f"股票过滤完成: {len(stock_list)} -> {len(filtered_stocks)}")
            return filtered_stocks

        except Exception as e:
            self.logger.error(f"应用股票过滤规则失败: {e}")
            return stock_list

    def test_connections(self):
        """测试连接"""
        try:
            # 测试Redis连接
            self.redis_client.ping()
            self.logger.info("Redis连接测试成功")

            # 测试QMT连接
            try:
                # 检查QMT是否已启动
                from xtquant import xtdata

                # 尝试连接QMT（connect()返回连接对象，不是错误代码）
                connect_result = xtdata.connect()
                self.logger.info(f"QMT连接对象: {type(connect_result)}")

                # 通过获取股票信息来验证连接是否正常
                if connect_result is None:
                    self.logger.error("QMT连接失败，返回None")
                    return False

                # 测试获取股票基本信息
                test_symbol = "000001.SZ"  # 使用平安银行作为测试股票
                try:
                    stock_info = xtdata.get_instrument_detail(test_symbol)
                    if stock_info and len(stock_info) > 0:
                        self.logger.info(f"QMT股票信息获取成功 - {test_symbol}: {stock_info.get('InstrumentName', 'N/A')}")
                    else:
                        self.logger.warning("QMT股票信息获取为空，但连接正常")
                except Exception as e:
                    self.logger.warning(f"QMT股票信息获取异常: {e}")

                # 测试获取股票列表（这是我们实际需要的功能）
                # 尝试不同的sector名称格式
                sector_formats = [
                    ('深圳A股', ['SZA股', 'SZ A股', '深圳A股', 'SZSE']),
                    ('上海A股', ['SHA股', 'SH A股', '上海A股', 'SSE'])
                ]

                for market_name, formats in sector_formats:
                    found = False
                    for sector_format in formats:
                        try:
                            stocks = xtdata.get_stock_list_in_sector(sector_format)
                            if stocks and len(stocks) > 0:
                                self.logger.info(f"QMT股票列表获取成功 - {market_name}({sector_format}): {len(stocks)}只")
                                found = True
                                break
                        except Exception as e:
                            self.logger.debug(f"尝试{sector_format}失败: {e}")

                    if not found:
                        self.logger.warning(f"QMT {market_name}列表获取失败，尝试了所有格式")

                # 测试获取最新行情数据（可选，非交易时间可能失败）
                try:
                    latest_data = xtdata.get_full_tick([test_symbol])
                    if latest_data and test_symbol in latest_data:
                        tick_data = latest_data[test_symbol]
                        self.logger.info(f"QMT行情测试成功 - {test_symbol}: 最新价={tick_data.get('lastPrice', 'N/A')}")
                    else:
                        self.logger.info("QMT行情数据为空（可能是非交易时间）")
                except Exception as e:
                    self.logger.info(f"QMT行情获取异常（可能是非交易时间）: {e}")

                self.logger.info("QMT连接测试完成 - 基础功能正常")

            except ImportError as e:
                self.logger.error(f"QMT模块导入失败: {e}")
                self.logger.error("请确保已安装xtquant库: pip install xtquant")
                return False
            except Exception as e:
                self.logger.error(f"QMT连接测试失败: {e}")
                self.logger.error("请检查QMT是否已启动并登录")
                return False

            return True
        except Exception as e:
            self.logger.error(f"连接测试失败: {e}")
            return False

    def start_subscription(self):
        """启动数据订阅"""
        try:
            # 测试连接
            if not self.test_connections():
                self.logger.error("连接测试失败，无法启动订阅")
                return

            # 获取股票列表
            self.logger.info("正在获取股票列表...")
            self.stock_list = self.get_all_stock_list()

            if not self.stock_list:
                self.logger.error("未能获取到任何股票代码，无法启动订阅")
                return

            self.logger.info(f"开始订阅{len(self.stock_list)}只股票的分钟线数据")

            # 显示前10只股票作为示例
            sample_stocks = self.stock_list[:10]
            self.logger.info(f"股票列表示例: {sample_stocks}...")
            if len(self.stock_list) > 10:
                self.logger.info(f"还有{len(self.stock_list) - 10}只股票未显示")

            # 订阅分钟线数据 - 根据QMT API文档的正确方式
            self.logger.info("尝试订阅QMT实时数据...")

            # QMT的正确订阅方式（根据API测试结果）
            # 注意：subscribe_quote只能订阅单个股票，需要循环订阅
            self.logger.info("QMT API只支持单个股票订阅，开始逐个订阅...")

            subscription_count = 0
            failed_count = 0

            # 限制订阅数量，避免过多订阅
            max_subscriptions = min(len(self.stock_list), 100)  # 最多订阅100只股票

            for i, stock_code in enumerate(self.stock_list[:max_subscriptions]):
                try:
                    # 使用正确的API调用方式
                    seq = xtdata.subscribe_quote(
                        stock_code=stock_code,  # 单个股票代码
                        period='1m',           # 1分钟周期
                        callback=self.on_minute_data  # 回调函数
                    )

                    if seq is not None:
                        subscription_count += 1
                        if subscription_count <= 5:  # 只显示前5个成功的订阅
                            self.logger.info(f"订阅成功: {stock_code}, 序列号: {seq}")
                    else:
                        failed_count += 1

                    # 每订阅10只股票暂停一下，避免频率过高
                    if (i + 1) % 10 == 0:
                        time.sleep(0.1)
                        self.logger.info(f"订阅进度: {i+1}/{max_subscriptions}")

                except Exception as e:
                    failed_count += 1
                    if failed_count <= 3:  # 只显示前3个失败的详细信息
                        self.logger.warning(f"订阅失败: {stock_code}, 错误: {e}")

            self.logger.info(f"订阅完成: 成功{subscription_count}只, 失败{failed_count}只")

            if subscription_count == 0:
                # 如果所有订阅都失败，改用轮询方式
                self.logger.error("所有股票订阅都失败，切换到轮询模式...")
                self.start_polling_mode()
                return
            elif subscription_count < max_subscriptions * 0.3:  # 成功率低于30%才切换
                # 如果成功率太低，也切换到轮询模式
                self.logger.warning(f"订阅成功率较低({subscription_count}/{max_subscriptions})，切换到轮询模式...")
                self.start_polling_mode()
                return

            # 订阅成功，启动实时数据接收
            self.logger.info(f"QMT实时订阅模式启动成功，共订阅{subscription_count}只股票")
            self.is_running = True

            # 启动统计信息打印线程
            stats_thread = threading.Thread(target=self.print_stats, daemon=True)
            stats_thread.start()

            # 启动数据接收
            xtdata.run()

        except Exception as e:
            self.logger.error(f"订阅数据失败: {e}")
            self.logger.error(traceback.format_exc())

    def on_minute_data(self, data):
        """
        分钟线数据回调函数

        Args:
            data (dict): QMT返回的数据字典
        """
        try:
            # 先打印数据结构，了解QMT返回的格式
            if self.stats['total_received'] < 3:  # 只打印前3次，避免日志过多
                self.logger.info(f"QMT数据格式: {type(data)}")
                self.logger.info(f"QMT数据内容: {data}")

            # 处理数据
            if isinstance(data, dict):
                self.stats['total_received'] += len(data)

                for symbol, bar_data in data.items():
                    self.process_bar_data(symbol, bar_data)

            elif isinstance(data, list):
                self.stats['total_received'] += len(data)

                # 如果是列表格式，每个元素可能包含股票代码和数据
                for item in data:
                    if isinstance(item, dict) and 'symbol' in item:
                        symbol = item.get('symbol', 'UNKNOWN')
                        self.process_bar_data(symbol, item)
                    else:
                        self.logger.warning(f"未知的数据项格式: {type(item)}, 内容: {item}")
            else:
                self.logger.warning(f"未知的数据格式: {type(data)}, 内容: {data}")

        except Exception as e:
            self.logger.error(f"处理分钟线数据失败: {e}")
            self.logger.error(traceback.format_exc())

    def process_bar_data(self, symbol, bar_data):
        """
        处理单个股票的K线数据

        Args:
            symbol (str): 股票代码
            bar_data: K线数据（可能是字典或列表）
        """
        try:
            if isinstance(bar_data, dict):
                # 字典格式
                minute_bar = {
                    "symbol": symbol,
                    "frame": bar_data.get('time', datetime.now().isoformat()),
                    "open": float(bar_data.get('open', 0)),
                    "high": float(bar_data.get('high', 0)),
                    "low": float(bar_data.get('low', 0)),
                    "close": float(bar_data.get('close', 0)),
                    "vol": float(bar_data.get('volume', 0)),
                    "amount": float(bar_data.get('amount', 0))
                }

            elif isinstance(bar_data, list) and len(bar_data) >= 6:
                # 列表格式，假设顺序为: [time, open, high, low, close, volume, amount]
                minute_bar = {
                    "symbol": symbol,
                    "frame": str(bar_data[0]) if bar_data[0] else datetime.now().isoformat(),
                    "open": float(bar_data[1]) if len(bar_data) > 1 else 0,
                    "high": float(bar_data[2]) if len(bar_data) > 2 else 0,
                    "low": float(bar_data[3]) if len(bar_data) > 3 else 0,
                    "close": float(bar_data[4]) if len(bar_data) > 4 else 0,
                    "vol": float(bar_data[5]) if len(bar_data) > 5 else 0,
                    "amount": float(bar_data[6]) if len(bar_data) > 6 else 0
                }

            else:
                self.logger.warning(f"未知的K线数据格式: {type(bar_data)}, 内容: {bar_data}")
                return

            # 发布到Redis
            self.publish_to_redis(minute_bar)

        except Exception as e:
            self.logger.error(f"处理K线数据失败 - {symbol}: {e}")
            self.logger.debug(f"问题数据: {bar_data}")

    def publish_to_redis(self, minute_bar):
        """
        发布数据到远程Redis

        Args:
            minute_bar (dict): 分钟线数据
        """
        retry_count = 0

        while retry_count < self.max_retry_times:
            try:
                # 使用List结构存储当日分钟线数据
                date_str = datetime.now().strftime('%Y-%m-%d')
                key = f"minute_bar:{minute_bar['symbol']}:{date_str}"
                value = json.dumps(minute_bar, ensure_ascii=False, default=str)

                # 推送到当日数据队列
                self.redis_client.lpush(key, value)

                # 设置过期时间（7天）
                self.redis_client.expire(key, 86400 * 7)

                # 同时推送到消费队列
                self.redis_client.lpush("minute_bar_queue", value)

                # 更新统计信息
                self.stats['total_published'] += 1
                self.stats['last_publish_time'] = datetime.now()

                self.logger.debug(f"发布分钟线数据: {minute_bar['symbol']} - {minute_bar['frame']}")
                break

            except Exception as e:
                retry_count += 1
                self.stats['publish_errors'] += 1
                self.logger.error(f"发布数据到Redis失败 (重试 {retry_count}/{self.max_retry_times}): {e}")

                if retry_count < self.max_retry_times:
                    time.sleep(1)  # 等待1秒后重试
                    self.reconnect_redis()
                else:
                    self.logger.error(f"发布数据最终失败: {minute_bar['symbol']}")

    def reconnect_redis(self):
        """Redis重连机制"""
        try:
            self.redis_client = redis.StrictRedis(
                host=self.redis_host,
                port=self.redis_port,
                password=self.redis_password,
                db=0,
                decode_responses=True,
                socket_connect_timeout=5,
                socket_timeout=5,
                retry_on_timeout=True
            )
            self.redis_client.ping()
            self.logger.info("Redis重连成功")
        except Exception as e:
            self.logger.error(f"Redis重连失败: {e}")

    def print_stats(self):
        """打印统计信息"""
        while self.is_running:
            try:
                time.sleep(60)  # 每分钟打印一次
                self.logger.info(
                    f"统计信息 - 接收: {self.stats['total_received']}, "
                    f"发布: {self.stats['total_published']}, "
                    f"错误: {self.stats['publish_errors']}, "
                    f"最后发布: {self.stats['last_publish_time']}"
                )
            except Exception as e:
                self.logger.error(f"打印统计信息失败: {e}")

    def start_polling_mode(self):
        """启动轮询模式获取数据"""
        self.logger.info("启动轮询模式...")
        self.is_running = True

        # 启动统计信息打印线程
        stats_thread = threading.Thread(target=self.print_stats, daemon=True)
        stats_thread.start()

        # 启动轮询线程
        polling_thread = threading.Thread(target=self.polling_data, daemon=True)
        polling_thread.start()

        # 保持主线程运行
        try:
            while self.is_running:
                time.sleep(1)
        except KeyboardInterrupt:
            self.logger.info("收到停止信号")
            self.stop_subscription()

    def polling_data(self):
        """轮询获取数据"""
        self.logger.info("开始轮询获取分钟线数据...")

        while self.is_running:
            try:
                # 每分钟获取一次数据
                current_time = datetime.now()

                # 只在交易时间内获取数据
                if self.is_trading_time(current_time):
                    # 分批获取数据，避免一次性获取太多
                    batch_size = 50
                    for i in range(0, len(self.stock_list), batch_size):
                        if not self.is_running:
                            break

                        batch_stocks = self.stock_list[i:i+batch_size]
                        self.get_batch_minute_data(batch_stocks)

                        # 批次间稍作延迟
                        time.sleep(1)

                # 等待下一分钟
                time.sleep(60)

            except Exception as e:
                self.logger.error(f"轮询获取数据失败: {e}")
                time.sleep(10)  # 出错后等待10秒再重试

    def is_trading_time(self, current_time):
        """判断是否为交易时间"""
        # 简单的交易时间判断（工作日 9:30-15:00）
        weekday = current_time.weekday()
        if weekday >= 5:  # 周末
            return False

        hour = current_time.hour
        minute = current_time.minute

        # 上午 9:30-11:30
        if (hour == 9 and minute >= 30) or (hour == 10) or (hour == 11 and minute <= 30):
            return True

        # 下午 13:00-15:00
        if (hour == 13) or (hour == 14) or (hour == 15 and minute == 0):
            return True

        return False

    def get_batch_minute_data(self, stock_list):
        """批量获取分钟线数据"""
        try:
            # 获取当前时间的分钟线数据
            end_time = datetime.now().strftime('%Y%m%d%H%M%S')
            start_time = (datetime.now().replace(second=0, microsecond=0)).strftime('%Y%m%d%H%M%S')

            # 使用get_market_data_ex获取数据（参考您的示例代码格式）
            data = xtdata.get_market_data_ex(
                [],  # 第一个参数：空列表
                stock_list,  # 第二个参数：股票代码列表
                '1m',  # 第三个参数：周期
                start_time,  # 第四个参数：开始时间
                end_time  # 第五个参数：结束时间
            )

            if data:
                for symbol, df in data.items():
                    if not df.empty:
                        # 转换为我们需要的格式
                        latest_row = df.iloc[-1]  # 获取最新的一行数据

                        minute_bar = {
                            "symbol": symbol,
                            "frame": latest_row.name.strftime('%Y-%m-%d %H:%M:%S'),  # 时间索引
                            "open": float(latest_row.get('open', 0)),
                            "high": float(latest_row.get('high', 0)),
                            "low": float(latest_row.get('low', 0)),
                            "close": float(latest_row.get('close', 0)),
                            "vol": float(latest_row.get('volume', 0)),
                            "amount": float(latest_row.get('amount', 0))
                        }

                        # 发布到Redis
                        self.publish_to_redis(minute_bar)
                        self.stats['total_received'] += 1

        except Exception as e:
            self.logger.error(f"批量获取分钟线数据失败: {e}")

    def stop_subscription(self):
        """停止订阅"""
        self.is_running = False
        self.logger.info("停止QMT数据订阅")
