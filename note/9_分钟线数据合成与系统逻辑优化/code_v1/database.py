# -*- coding: utf-8 -*-
"""
数据库连接工具类
"""
import redis
import clickhouse_connect
import json
from datetime import datetime
from typing import List, Optional
from config import REDIS_CONFIG, CLICKHOUSE_CONFIG, REDIS_QUEUES, CLICKHOUSE_TABLES
from models import BarData, TickData
from trading_time_validator import TradingTimeValidator
import logging


class RedisManager:
    """Redis连接管理器"""

    def __init__(self):
        self.client = redis.Redis(**REDIS_CONFIG)
        self.trading_validator = TradingTimeValidator()
        self.logger = logging.getLogger(__name__)

    def publish_tick_data(self, tick_data: TickData):
        """发布分笔数据到Redis"""
        data = tick_data.model_dump_json()
        self.client.lpush(REDIS_QUEUES['whole_quote_data'], data)

    def publish_bar_data(self, bar_data: BarData, period: int, is_historical: bool = False):
        """
        发布分钟线数据到Redis

        Args:
            bar_data: 分钟线数据
            period: 周期（分钟）
            is_historical: 是否为历史数据
        """
        # 验证数据是否在交易时间内
        bar_dict = {
            'frame': bar_data.frame,
            'symbol': bar_data.symbol,
            'open': bar_data.open,
            'high': bar_data.high,
            'low': bar_data.low,
            'close': bar_data.close,
            'vol': bar_data.vol,
            'amount': bar_data.amount
        }

        if not self.trading_validator.validate_bar_data(bar_dict):
            self.logger.debug(f"拒绝发布非交易时间数据: {bar_data.symbol} at {bar_data.frame}")
            return

        data = bar_data.model_dump_json()

        if is_historical:
            # 历史数据：发布到队列供Mac端消费存储到ClickHouse
            queue_name = f"bar_data_{period}min"
            self.client.lpush(REDIS_QUEUES[queue_name], data)
        else:
            # 当日合成数据：存储在Redis中（用于实时查询）
            current_data_key = f"current_bar_data_{period}min"
            self.client.lpush(current_data_key, data)
            # 设置过期时间为第二天凌晨3点
            self.client.expire(current_data_key, 86400)  # 24小时

    def consume_tick_data(self, timeout: int = 1) -> Optional[TickData]:
        """消费分笔数据"""
        result = self.client.brpop(REDIS_QUEUES['whole_quote_data'], timeout=timeout)
        if result:
            data = json.loads(result[1])
            return TickData(**data)
        return None

    def consume_bar_data(self, period: int, timeout: int = 1) -> Optional[BarData]:
        """消费分钟线数据"""
        queue_name = f"bar_data_{period}min"
        result = self.client.brpop(REDIS_QUEUES[queue_name], timeout=timeout)
        if result:
            data = json.loads(result[1])
            return BarData(**data)
        return None

    def get_current_bar_data(self, period: int, symbol: str = None) -> List[BarData]:
        """获取当日分钟线数据（从Redis当日数据存储中获取）"""
        current_data_key = f"current_bar_data_{period}min"
        data_list = self.client.lrange(current_data_key, 0, -1)

        bars = []
        for data in data_list:
            try:
                bar_data = BarData(**json.loads(data))
                # 如果指定了symbol，进行过滤
                if symbol is None or bar_data.symbol == symbol:
                    bars.append(bar_data)
            except Exception as e:
                self.logger.error(f"解析当日数据失败: {e}")
                continue

        # 按时间排序
        sorted_bars = sorted(bars, key=lambda x: x.frame)
        return sorted_bars

    def clear_all_queues(self):
        """清空所有队列"""
        for queue_name in REDIS_QUEUES.values():
            self.client.delete(queue_name)

    def get_queue_length(self, queue_name: str) -> int:
        """获取队列长度"""
        return self.client.llen(queue_name)

    def get_system_info(self) -> dict:
        """获取Redis系统信息"""
        info = {}
        for name, queue in REDIS_QUEUES.items():
            info[name] = self.client.llen(queue)
        return info


class ClickHouseManager:
    """ClickHouse连接管理器"""

    def __init__(self):
        self.client = clickhouse_connect.get_client(**CLICKHOUSE_CONFIG)
        self.trading_validator = TradingTimeValidator()
        self.logger = logging.getLogger(__name__)
        self._create_tables()

    def _create_tables(self):
        """创建ClickHouse表"""
        for table_name in CLICKHOUSE_TABLES.values():
            create_sql = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                symbol String,
                frame DateTime,
                open Float64,
                high Float64,
                low Float64,
                close Float64,
                vol Float64,
                amount Float64
            ) ENGINE = MergeTree()
            ORDER BY (symbol, frame)
            """
            self.client.command(create_sql)

    def insert_bar_data(self, bar_data_list: List[BarData], period: int):
        """插入历史分钟线数据（Mac端专用，只处理已验证的历史数据）"""
        table_name = CLICKHOUSE_TABLES[f'data_bar_for_{period}min']

        valid_data = []

        for bar in bar_data_list:
            # Mac端接收的数据已经过Windows端和Mac端的交易时间验证
            # 这里直接插入，因为数据来源是历史数据队列
            valid_data.append([
                bar.symbol,
                bar.frame,
                bar.open,
                bar.high,
                bar.low,
                bar.close,
                bar.vol,
                bar.amount
            ])

        if valid_data:
            self.client.insert(table_name, valid_data)
            self.logger.info(f"插入 {len(valid_data)} 条历史数据到 {table_name}")

    def query_bar_data(self, symbol: str, start_time: datetime, end_time: datetime, period: int) -> List[BarData]:
        """查询历史分钟线数据"""
        table_name = CLICKHOUSE_TABLES[f'data_bar_for_{period}min']

        query_sql = f"""
        SELECT symbol, frame, open, high, low, close, vol, amount
        FROM {table_name}
        WHERE symbol = %(symbol)s
        AND frame >= %(start_time)s
        AND frame <= %(end_time)s
        ORDER BY frame
        """

        result = self.client.query(query_sql, {
            'symbol': symbol,
            'start_time': start_time,
            'end_time': end_time
        })

        bars = []
        for row in result.result_rows:
            bars.append(BarData(
                symbol=row[0],
                frame=row[1],
                open=row[2],
                high=row[3],
                low=row[4],
                close=row[5],
                vol=row[6],
                amount=row[7]
            ))

        return bars

    def get_table_count(self, period: int) -> int:
        """获取表记录数"""
        table_name = CLICKHOUSE_TABLES[f'data_bar_for_{period}min']
        result = self.client.query(f"SELECT COUNT(*) FROM {table_name}")
        return result.result_rows[0][0] if result.result_rows else 0

    def get_system_info(self) -> dict:
        """获取ClickHouse系统信息"""
        info = {}
        for period in [1, 5, 15, 30]:
            count = self.get_table_count(period)
            info[f'{period}min_count'] = count
        return info
