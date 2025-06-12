#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Mac端市场数据查询SDK

提供分钟线和日线数据查询功能
"""

import pandas as pd
import redis
from clickhouse_driver import Client
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
import json
import logging


class MacMarketDataSDK:
    """Mac端市场数据查询SDK"""

    def __init__(self, config):
        """
        初始化SDK

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
            decode_responses=True
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

        self.logger = logging.getLogger(__name__)

    def get_minute_bars(self, symbol: str, start_time: str, end_time: str,
                       period: str = '1min') -> pd.DataFrame:
        """
        获取分钟线数据

        Args:
            symbol: 股票代码
            start_time: 开始时间 (YYYY-MM-DD HH:MM:SS)
            end_time: 结束时间 (YYYY-MM-DD HH:MM:SS)
            period: 时间周期 ('1min', '5min', '30min')

        Returns:
            pandas.DataFrame: 分钟线数据
        """
        try:
            # 根据周期选择表名
            table_map = {
                '1min': 'minute_bars',
                '5min': 'minute_bars_5min',
                '30min': 'minute_bars_30min'
            }

            table_name = table_map.get(period, 'minute_bars')

            query = f"""
            SELECT
                symbol,
                frame,
                open,
                high,
                low,
                close,
                vol,
                amount
            FROM {table_name}
            WHERE symbol = %(symbol)s
              AND frame >= %(start_time)s
              AND frame <= %(end_time)s
            ORDER BY frame
            """

            result = self.clickhouse_client.execute(
                query,
                {
                    'symbol': symbol,
                    'start_time': start_time,
                    'end_time': end_time
                }
            )

            # 转换为DataFrame
            columns = ['symbol', 'frame', 'open', 'high', 'low', 'close', 'vol', 'amount']
            df = pd.DataFrame(result, columns=columns)
            if not df.empty:
                df.set_index('frame', inplace=True)

            self.logger.info(f"查询到{len(df)}条{period}数据: {symbol}")
            return df

        except Exception as e:
            self.logger.error(f"查询分钟线数据失败: {e}")
            return pd.DataFrame()

    def get_daily_bars(self, symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        获取日线数据

        Args:
            symbol: 股票代码
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)

        Returns:
            pandas.DataFrame: 日线数据
        """
        try:
            query = """
            SELECT
                symbol,
                frame,
                open,
                high,
                low,
                close,
                vol,
                amount,
                adjust,
                st,
                limit_up,
                limit_down
            FROM daily_bars
            WHERE symbol = %(symbol)s
              AND frame >= %(start_date)s
              AND frame <= %(end_date)s
            ORDER BY frame
            """

            result = self.clickhouse_client.execute(
                query,
                {
                    'symbol': symbol,
                    'start_date': start_date,
                    'end_date': end_date
                }
            )

            # 转换为DataFrame
            columns = ['symbol', 'frame', 'open', 'high', 'low', 'close', 'vol', 'amount',
                      'adjust', 'st', 'limit_up', 'limit_down']
            df = pd.DataFrame(result, columns=columns)
            if not df.empty:
                df.set_index('frame', inplace=True)

            self.logger.info(f"查询到{len(df)}条日线数据: {symbol}")
            return df

        except Exception as e:
            self.logger.error(f"查询日线数据失败: {e}")
            return pd.DataFrame()

    def get_realtime_data(self, symbol: str, date: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        获取实时数据（从Redis）

        Args:
            symbol: 股票代码
            date: 日期 (YYYY-MM-DD)，默认为今天

        Returns:
            List[Dict]: 实时分钟线数据列表
        """
        try:
            if date is None:
                date = datetime.now().strftime('%Y-%m-%d')

            key = f"minute_bar:{symbol}:{date}"

            # 从Redis获取当日数据
            data_list = self.redis_client.lrange(key, 0, -1)

            result = []
            for data in data_list:
                minute_bar = json.loads(data)
                result.append(minute_bar)

            # 按时间排序
            result.sort(key=lambda x: x['frame'])

            self.logger.info(f"获取到{len(result)}条实时数据: {symbol}")
            return result

        except Exception as e:
            self.logger.error(f"获取实时数据失败: {e}")
            return []

    def get_latest_price(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        获取最新价格

        Args:
            symbol: 股票代码

        Returns:
            Dict: 最新价格信息
        """
        try:
            date = datetime.now().strftime('%Y-%m-%d')
            key = f"minute_bar:{symbol}:{date}"

            # 获取最新一条数据
            latest_data = self.redis_client.lindex(key, 0)

            if latest_data:
                result = json.loads(latest_data)
                self.logger.debug(f"获取最新价格: {symbol} - {result['close']}")
                return result
            else:
                self.logger.warning(f"未找到最新价格数据: {symbol}")
                return None

        except Exception as e:
            self.logger.error(f"获取最新价格失败: {e}")
            return None

    def get_market_overview(self, symbols: List[str] = None) -> Dict[str, Dict[str, Any]]:
        """
        获取市场概览

        Args:
            symbols: 股票代码列表，如果为None则获取所有有数据的股票

        Returns:
            Dict: 市场概览数据
        """
        try:
            result = {}

            if symbols is None:
                # 如果没有指定股票列表，从Redis获取所有有数据的股票
                symbols = self.get_available_symbols()
                # 限制返回数量，避免过多
                if len(symbols) > 20:
                    symbols = symbols[:20]

            for symbol in symbols:
                latest_price = self.get_latest_price(symbol)
                if latest_price:
                    result[symbol] = {
                        'latest_price': latest_price['close'],
                        'timestamp': latest_price['frame'],
                        'volume': latest_price['vol'],
                        'amount': latest_price['amount']
                    }

            self.logger.info(f"获取市场概览: {len(result)}只股票")
            return result

        except Exception as e:
            self.logger.error(f"获取市场概览失败: {e}")
            return {}

    def get_available_symbols(self) -> List[str]:
        """
        获取所有有数据的股票代码

        Returns:
            List[str]: 股票代码列表
        """
        try:
            date = datetime.now().strftime('%Y-%m-%d')

            # 从Redis获取所有当日有数据的股票代码
            pattern = f"minute_bar:*:{date}"
            keys = self.redis_client.keys(pattern)

            symbols = []
            for key in keys:
                # 解析key格式: minute_bar:symbol:date
                parts = key.split(':')
                if len(parts) == 3:
                    symbol = parts[1]
                    symbols.append(symbol)

            # 去重并排序
            symbols = sorted(list(set(symbols)))

            self.logger.info(f"发现{len(symbols)}只有数据的股票")
            return symbols

        except Exception as e:
            self.logger.error(f"获取可用股票代码失败: {e}")
            return []

    def search_symbols(self, keyword: str) -> List[str]:
        """
        搜索股票代码

        Args:
            keyword: 搜索关键词（股票代码或名称的一部分）

        Returns:
            List[str]: 匹配的股票代码列表
        """
        try:
            # 从ClickHouse查询匹配的股票代码
            query = """
            SELECT DISTINCT symbol
            FROM minute_bars
            WHERE symbol LIKE %(keyword)s
            ORDER BY symbol
            LIMIT 100
            """

            result = self.clickhouse_client.execute(
                query,
                {'keyword': f'%{keyword.upper()}%'}
            )

            symbols = [row[0] for row in result]
            self.logger.info(f"搜索关键词'{keyword}'找到{len(symbols)}只股票")
            return symbols

        except Exception as e:
            self.logger.error(f"搜索股票代码失败: {e}")
            return []

    def validate_symbol(self, symbol: str) -> bool:
        """
        验证股票代码是否有效（是否有数据）

        Args:
            symbol: 股票代码

        Returns:
            bool: 是否有效
        """
        try:
            # 检查ClickHouse中是否有该股票的数据
            query = """
            SELECT count() as count
            FROM minute_bars
            WHERE symbol = %(symbol)s
            LIMIT 1
            """

            result = self.clickhouse_client.execute(
                query,
                {'symbol': symbol}
            )

            has_data = result[0][0] > 0 if result else False

            if not has_data:
                # 如果ClickHouse中没有，检查Redis中是否有当日数据
                date = datetime.now().strftime('%Y-%m-%d')
                key = f"minute_bar:{symbol}:{date}"
                has_data = self.redis_client.exists(key)

            return has_data

        except Exception as e:
            self.logger.error(f"验证股票代码{symbol}失败: {e}")
            return False

    def get_data_statistics(self) -> Dict[str, Any]:
        """
        获取数据统计信息

        Returns:
            Dict: 统计信息
        """
        try:
            stats = {}

            # 分钟线数据统计
            minute_query = """
            SELECT
                count() as total_count,
                uniq(symbol) as symbol_count,
                min(frame) as earliest_time,
                max(frame) as latest_time
            FROM minute_bars
            """
            minute_result = self.clickhouse_client.execute(minute_query)
            if minute_result:
                stats['minute_bars'] = {
                    'total_count': minute_result[0][0],
                    'symbol_count': minute_result[0][1],
                    'earliest_time': minute_result[0][2],
                    'latest_time': minute_result[0][3]
                }

            # 今日数据统计
            today = datetime.now().strftime('%Y-%m-%d')
            today_query = """
            SELECT
                symbol,
                count() as count
            FROM minute_bars
            WHERE toDate(frame) = %(today)s
            GROUP BY symbol
            ORDER BY count DESC
            """
            today_result = self.clickhouse_client.execute(today_query, {'today': today})
            stats['today_data'] = {symbol: count for symbol, count in today_result}

            return stats

        except Exception as e:
            self.logger.error(f"获取数据统计失败: {e}")
            return {}
