#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
ClickHouse处理模块

提供ClickHouse连接和操作功能。
"""

import datetime
import pandas as pd
from typing import Dict, List, Any, Union
from clickhouse_driver import Client

from config import CLICKHOUSE_CONFIG
from logger import logger
from exceptions import ClickHouseConnectionError, ClickHouseOperationError
from utils import retry


class ClickHouseHandler:
    """ClickHouse处理类，提供ClickHouse连接和操作功能"""

    def __init__(self, config: Dict = None):
        """
        初始化ClickHouse处理类

        Args:
            config (Dict, optional): ClickHouse配置. 默认为None，使用配置文件中的值.
        """
        self.config = config or CLICKHOUSE_CONFIG
        self.client = None
        self.database = self.config.get('database', 'RealTime_DailyLine_DB')
        self.table = self.config.get('table', 'day_bar')
        self.connect()

    @retry(exceptions=(Exception,))
    def connect(self) -> None:
        """
        连接ClickHouse服务器

        Raises:
            ClickHouseConnectionError: 连接ClickHouse服务器失败时抛出
        """
        try:
            self.client = Client(
                host=self.config.get('host', 'localhost'),
                port=self.config.get('port', 9000),
                user=self.config.get('user', 'default'),
                password=self.config.get('password', ''),
                database=self.database
            )

            # 测试连接
            self.client.execute('SELECT 1')
            logger.info("ClickHouse连接成功")

            # 确保数据库存在
            self._ensure_database_exists()

            # 确保表存在
            self._ensure_table_exists()

        except Exception as e:
            logger.error(f"ClickHouse连接失败: {e}")
            raise ClickHouseConnectionError(f"ClickHouse连接失败: {e}")

    def _ensure_database_exists(self) -> None:
        """
        确保数据库存在，如果不存在则创建

        Raises:
            ClickHouseOperationError: 创建数据库失败时抛出
        """
        try:
            # 检查数据库是否存在
            result = self.client.execute(
                "SELECT name FROM system.databases WHERE name = %(database)s",
                {'database': self.database}
            )

            if not result:
                # 创建数据库
                self.client.execute(f"CREATE DATABASE IF NOT EXISTS {self.database}")
                logger.info(f"已创建数据库 {self.database}")
        except Exception as e:
            logger.error(f"确保数据库存在失败: {e}")
            raise ClickHouseOperationError(f"确保数据库存在失败: {e}")

    def _ensure_table_exists(self) -> None:
        """
        确保表存在，如果不存在则创建

        Raises:
            ClickHouseOperationError: 创建表失败时抛出
        """
        try:
            # 创建表
            self.client.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.database}.{self.table} (
                    symbol String,
                    trade_date Date,
                    open Float64,
                    high Float64,
                    low Float64,
                    close Float64,
                    vol Float64,
                    amount Float64,
                    adjust Float64,
                    pre_close Float64,
                    change Float64,
                    pct_chg Float64,
                    is_st UInt8,
                    limit_up Float64,
                    limit_down Float64
                ) ENGINE = MergeTree()
                ORDER BY (symbol, trade_date)
            """)
            logger.info(f"已确保表 {self.database}.{self.table} 存在")
        except Exception as e:
            logger.error(f"确保表存在失败: {e}")
            raise ClickHouseOperationError(f"确保表存在失败: {e}")

    def check_connection(self) -> bool:
        """
        检查ClickHouse连接状态

        Returns:
            bool: 连接状态，True表示连接正常，False表示连接异常
        """
        try:
            if self.client is None:
                self.connect()
                return True

            self.client.execute('SELECT 1')
            return True
        except Exception as e:
            logger.error(f"ClickHouse连接检查失败: {e}")
            return False

    @retry(exceptions=(Exception,))
    def insert_data(self, data: Union[Dict, List[Dict], pd.DataFrame]) -> None:
        """
        插入数据到ClickHouse表

        Args:
            data (Union[Dict, List[Dict], pd.DataFrame]): 要插入的数据，可以是字典、字典列表或DataFrame

        Raises:
            ClickHouseOperationError: 插入数据失败时抛出
        """
        try:
            if not self.check_connection():
                self.connect()

            # 将数据转换为DataFrame
            if isinstance(data, dict):
                df = pd.DataFrame([data])
            elif isinstance(data, list):
                df = pd.DataFrame(data)
            else:
                df = data

            # 转换为ClickHouse可接受的格式
            records = df.to_dict('records')

            # 插入数据
            self.client.execute(
                f"INSERT INTO {self.database}.{self.table} VALUES",
                records
            )

            logger.info(f"已插入 {len(records)} 条数据到ClickHouse表 {self.database}.{self.table}")
        except Exception as e:
            logger.error(f"插入数据到ClickHouse表失败: {e}")
            raise ClickHouseOperationError(f"插入数据到ClickHouse表失败: {e}")

    def get_latest_trade_date(self, symbol: str = None) -> str:
        """
        获取ClickHouse中最新的交易日期

        Args:
            symbol (str, optional): 股票代码. 默认为None，表示查询所有股票.

        Returns:
            str: 最新交易日期，格式为'YYYYMMDD'，如果没有数据则返回None
        """
        try:
            if not self.check_connection():
                self.connect()

            # 构建查询SQL
            query = f"SELECT MAX(trade_date) as latest_date FROM {self.database}.{self.table}"
            if symbol:
                query += f" WHERE symbol = '{symbol}'"

            # 执行查询
            result = self.client.execute(query)

            # 解析结果
            if result and result[0][0]:
                latest_date = result[0][0]
                # 将日期转换为字符串格式'YYYYMMDD'
                return latest_date.strftime('%Y%m%d')
            else:
                logger.warning(f"ClickHouse中没有找到交易日期数据")
                return None

        except Exception as e:
            logger.error(f"获取ClickHouse最新交易日期失败: {e}")
            return None

    def get_earliest_trade_date(self, symbol: str = None) -> str:
        """
        获取ClickHouse中最早的交易日期

        Args:
            symbol (str, optional): 股票代码. 默认为None，表示查询所有股票.

        Returns:
            str: 最早交易日期，格式为'YYYYMMDD'，如果没有数据则返回None
        """
        try:
            if not self.check_connection():
                self.connect()

            # 构建查询SQL
            query = f"SELECT MIN(trade_date) as earliest_date FROM {self.database}.{self.table}"
            if symbol:
                query += f" WHERE symbol = '{symbol}'"

            # 执行查询
            result = self.client.execute(query)

            # 解析结果
            if result and result[0][0]:
                earliest_date = result[0][0]
                # 将日期转换为字符串格式'YYYYMMDD'
                return earliest_date.strftime('%Y%m%d')
            else:
                logger.warning(f"ClickHouse中没有找到交易日期数据")
                return None

        except Exception as e:
            logger.error(f"获取ClickHouse最早交易日期失败: {e}")
            return None

    def get_date_range(self) -> tuple:
        """
        获取ClickHouse中的日期范围

        Returns:
            tuple: (earliest_date, latest_date)，格式为('YYYYMMDD', 'YYYYMMDD')
                  如果没有数据则返回(None, None)
        """
        earliest_date = self.get_earliest_trade_date()
        latest_date = self.get_latest_trade_date()
        return (earliest_date, latest_date)

    def get_existing_symbols_for_date(self, trade_date: str) -> List[str]:
        """
        获取指定交易日期在ClickHouse中已存在的股票代码列表

        Args:
            trade_date (str): 交易日期，格式为'YYYYMMDD'

        Returns:
            List[str]: 股票代码列表
        """
        try:
            if not self.check_connection():
                self.connect()

            # 将日期字符串转换为日期对象
            date_obj = datetime.datetime.strptime(trade_date, '%Y%m%d').date()

            # 构建查询SQL
            query = f"SELECT DISTINCT symbol FROM {self.database}.{self.table} WHERE trade_date = %(date)s"

            # 执行查询
            result = self.client.execute(query, {'date': date_obj})

            # 解析结果
            symbols = [row[0] for row in result]

            logger.info(f"日期 {trade_date} 在ClickHouse中已有 {len(symbols)} 个股票的数据")
            return symbols

        except Exception as e:
            logger.error(f"获取ClickHouse已存在股票代码失败: {e}")
            return []

    def get_symbol_count_for_date(self, trade_date: str) -> int:
        """
        获取指定交易日期在ClickHouse中的股票数量

        Args:
            trade_date (str): 交易日期，格式为'YYYYMMDD'

        Returns:
            int: 股票数量
        """
        try:
            if not self.check_connection():
                self.connect()

            # 将日期字符串转换为日期对象
            date_obj = datetime.datetime.strptime(trade_date, '%Y%m%d').date()

            # 构建查询SQL
            query = f"SELECT COUNT(DISTINCT symbol) FROM {self.database}.{self.table} WHERE trade_date = %(date)s"

            # 执行查询
            result = self.client.execute(query, {'date': date_obj})

            # 解析结果
            count = result[0][0]

            logger.info(f"日期 {trade_date} 在ClickHouse中共有 {count} 个股票的数据")
            return count

        except Exception as e:
            logger.error(f"获取ClickHouse股票数量失败: {e}")
            return 0

    def check_date_data_completeness(self, trade_date: str, expected_count: int = None) -> bool:
        """
        检查指定交易日期的数据是否完整

        Args:
            trade_date (str): 交易日期，格式为'YYYYMMDD'
            expected_count (int, optional): 预期的股票数量. 默认为None，表示使用系统中最大的股票数量.

        Returns:
            bool: 数据是否完整
        """
        try:
            if not self.check_connection():
                self.connect()

            # 获取当前日期的股票数量
            current_count = self.get_symbol_count_for_date(trade_date)

            # 如果没有指定预期数量，则获取系统中最大的股票数量
            if expected_count is None:
                # 构建查询SQL
                query = f"SELECT MAX(cnt) FROM (SELECT COUNT(DISTINCT symbol) as cnt FROM {self.database}.{self.table} GROUP BY trade_date)"

                # 执行查询
                result = self.client.execute(query)

                # 解析结果
                expected_count = result[0][0] if result and result[0][0] else 0

            # 如果预期数量为0，则认为数据完整
            if expected_count == 0:
                return True

            # 计算完整度
            completeness = current_count / expected_count

            # 如果完整度大于等于0.95，则认为数据完整
            is_complete = completeness >= 0.95

            logger.info(f"日期 {trade_date} 数据完整度: {completeness:.2%} ({current_count}/{expected_count}), 是否完整: {is_complete}")
            return is_complete

        except Exception as e:
            logger.error(f"检查数据完整性失败: {e}")
            return False

    def get_incomplete_dates(self, start_date: str = None, end_date: str = None, expected_count: int = None) -> List[str]:
        """
        获取指定日期范围内不完整的日期列表

        Args:
            start_date (str, optional): 开始日期，格式为'YYYYMMDD'. 默认为None，表示最早日期.
            end_date (str, optional): 结束日期，格式为'YYYYMMDD'. 默认为None，表示最新日期.
            expected_count (int, optional): 预期的股票数量. 默认为None，表示使用系统中最大的股票数量.

        Returns:
            List[str]: 不完整的日期列表，格式为['YYYYMMDD', 'YYYYMMDD', ...]
        """
        try:
            if not self.check_connection():
                self.connect()

            # 如果没有指定日期范围，则使用系统中的最早和最新日期
            if start_date is None or end_date is None:
                earliest_date, latest_date = self.get_date_range()
                start_date = start_date or earliest_date
                end_date = end_date or latest_date

            if not start_date or not end_date:
                logger.warning("没有指定日期范围，且系统中没有数据")
                return []

            # 获取日期范围内的所有日期
            start_date_obj = datetime.datetime.strptime(start_date, '%Y%m%d').date()
            end_date_obj = datetime.datetime.strptime(end_date, '%Y%m%d').date()

            # 构建查询SQL，获取日期范围内的所有日期
            query = f"SELECT DISTINCT trade_date FROM {self.database}.{self.table} WHERE trade_date BETWEEN %(start_date)s AND %(end_date)s ORDER BY trade_date"

            # 执行查询
            result = self.client.execute(query, {'start_date': start_date_obj, 'end_date': end_date_obj})

            # 解析结果
            dates = [row[0].strftime('%Y%m%d') for row in result]

            # 检查每个日期的数据是否完整
            incomplete_dates = []
            for date in dates:
                if not self.check_date_data_completeness(date, expected_count):
                    incomplete_dates.append(date)

            logger.info(f"日期范围 {start_date} - {end_date} 内共有 {len(incomplete_dates)} 个不完整的日期")
            return incomplete_dates

        except Exception as e:
            logger.error(f"获取不完整日期列表失败: {e}")
            return []

    def close(self) -> None:
        """关闭ClickHouse连接"""
        if self.client:
            # ClickHouse Python驱动没有显式的close方法，设置为None即可
            self.client = None
            logger.info("ClickHouse连接已关闭")


# 创建默认ClickHouse处理器实例
clickhouse_handler = ClickHouseHandler()
