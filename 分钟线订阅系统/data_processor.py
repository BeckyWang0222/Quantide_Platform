# -*- coding: utf-8 -*-
"""
数据处理和合成工具类
"""
# import pandas as pd  # 暂时不使用
from datetime import datetime, timedelta
from typing import List, Dict
from collections import defaultdict
from models import TickData, BarData
from trading_time_validator import TradingTimeValidator
import logging


class BarDataSynthesizer:
    """分钟线数据合成器"""

    def __init__(self):
        # 存储各个股票的分笔数据缓存
        self.tick_cache: Dict[str, List[TickData]] = defaultdict(list)
        # 存储各个周期的分钟线缓存
        self.bar_cache: Dict[int, Dict[str, List[BarData]]] = {
            1: defaultdict(list),
            5: defaultdict(list),
            15: defaultdict(list),
            30: defaultdict(list)
        }
        # 交易时间验证器
        self.trading_validator = TradingTimeValidator()
        self.logger = logging.getLogger(__name__)

        # 数据统计
        self.stats = {
            'total_ticks': 0,
            'valid_ticks': 0,
            'filtered_ticks': 0,
            'total_bars': 0,
            'valid_bars': 0,
            'filtered_bars': 0
        }

    def add_tick_data(self, tick_data: TickData):
        """添加分笔数据（仅处理交易时间内的数据）"""
        self.stats['total_ticks'] += 1

        # 验证是否为交易时间内的数据
        tick_dict = {
            'time': tick_data.time,
            'symbol': tick_data.symbol,
            'price': tick_data.price,
            'volume': tick_data.volume,
            'amount': tick_data.amount
        }

        if not self.trading_validator.validate_tick_data(tick_dict):
            self.stats['filtered_ticks'] += 1
            self.logger.debug(f"过滤非交易时间分笔数据: {tick_data.symbol} at {tick_data.time}")
            return

        self.stats['valid_ticks'] += 1
        self.tick_cache[tick_data.symbol].append(tick_data)

        # 合成1分钟线
        bar_1min = self._synthesize_1min_bar(tick_data.symbol)
        if bar_1min:
            # 验证合成的1分钟线是否在交易时间内
            bar_dict = {
                'frame': bar_1min.frame,
                'symbol': bar_1min.symbol,
                'open': bar_1min.open,
                'high': bar_1min.high,
                'low': bar_1min.low,
                'close': bar_1min.close,
                'vol': bar_1min.vol,
                'amount': bar_1min.amount
            }

            if self.trading_validator.validate_bar_data(bar_dict):
                self.bar_cache[1][tick_data.symbol].append(bar_1min)
                self.stats['valid_bars'] += 1

                # 基于1分钟线合成其他周期
                for period in [5, 15, 30]:
                    bar = self._synthesize_multi_min_bar(tick_data.symbol, period)
                    if bar:
                        # 验证多周期分钟线
                        multi_bar_dict = {
                            'frame': bar.frame,
                            'symbol': bar.symbol,
                            'open': bar.open,
                            'high': bar.high,
                            'low': bar.low,
                            'close': bar.close,
                            'vol': bar.vol,
                            'amount': bar.amount
                        }

                        if self.trading_validator.validate_bar_data(multi_bar_dict):
                            self.bar_cache[period][tick_data.symbol].append(bar)
                            self.stats['valid_bars'] += 1
                        else:
                            self.stats['filtered_bars'] += 1
            else:
                self.stats['filtered_bars'] += 1

    def _synthesize_1min_bar(self, symbol: str) -> BarData:
        """合成1分钟线"""
        ticks = self.tick_cache[symbol]
        if not ticks:
            return None

        # 获取当前分钟的开始时间
        current_time = ticks[-1].time
        minute_start = current_time.replace(second=0, microsecond=0)
        minute_end = minute_start + timedelta(minutes=1)

        # 筛选当前分钟的分笔数据
        minute_ticks = [
            tick for tick in ticks
            if minute_start <= tick.time < minute_end
        ]

        if not minute_ticks:
            return None

        # 计算OHLCV
        prices = [tick.price for tick in minute_ticks]
        volumes = [tick.volume for tick in minute_ticks]
        amounts = [tick.amount for tick in minute_ticks]

        bar_data = BarData(
            symbol=symbol,
            frame=minute_start,
            open=prices[0],
            high=max(prices),
            low=min(prices),
            close=prices[-1],
            vol=sum(volumes),
            amount=sum(amounts)
        )

        # 清理已处理的分笔数据
        self.tick_cache[symbol] = [
            tick for tick in ticks
            if tick.time >= minute_end
        ]

        return bar_data

    def _synthesize_multi_min_bar(self, symbol: str, period: int) -> BarData:
        """合成多分钟线（5分钟、15分钟、30分钟）"""
        bars_1min = self.bar_cache[1][symbol]
        if not bars_1min:
            return None

        # 获取当前周期的开始时间
        current_time = bars_1min[-1].frame
        period_start = self._get_period_start(current_time, period)
        period_end = period_start + timedelta(minutes=period)

        # 筛选当前周期的1分钟线数据
        period_bars = [
            bar for bar in bars_1min
            if period_start <= bar.frame < period_end
        ]

        if len(period_bars) == 0:  # 没有数据
            return None

        # 合成多分钟线
        bar_data = BarData(
            symbol=symbol,
            frame=period_start,
            open=period_bars[0].open,
            high=max(bar.high for bar in period_bars),
            low=min(bar.low for bar in period_bars),
            close=period_bars[-1].close,
            vol=sum(bar.vol for bar in period_bars),
            amount=sum(bar.amount for bar in period_bars)
        )

        return bar_data

    def _get_period_start(self, current_time: datetime, period: int) -> datetime:
        """获取周期开始时间"""
        minute = current_time.minute
        period_minute = (minute // period) * period
        return current_time.replace(minute=period_minute, second=0, microsecond=0)

    def get_latest_bars(self, symbol: str, period: int, count: int = 100) -> List[BarData]:
        """获取最新的分钟线数据"""
        if symbol not in self.bar_cache[period]:
            return []

        bars = self.bar_cache[period][symbol]
        return bars[-count:] if len(bars) > count else bars

    def clear_cache(self, symbol: str = None):
        """清理缓存"""
        if symbol:
            if symbol in self.tick_cache:
                del self.tick_cache[symbol]
            for period in self.bar_cache:
                if symbol in self.bar_cache[period]:
                    del self.bar_cache[period][symbol]
        else:
            self.tick_cache.clear()
            for period in self.bar_cache:
                self.bar_cache[period].clear()

    def get_cache_info(self) -> dict:
        """获取缓存信息"""
        info = {
            'tick_cache_symbols': len(self.tick_cache),
            'tick_cache_total': sum(len(ticks) for ticks in self.tick_cache.values())
        }

        for period in [1, 5, 15, 30]:
            info[f'bar_{period}min_symbols'] = len(self.bar_cache[period])
            info[f'bar_{period}min_total'] = sum(
                len(bars) for bars in self.bar_cache[period].values()
            )

        # 添加数据质量统计
        info.update(self.stats)

        # 计算过滤率
        if self.stats['total_ticks'] > 0:
            info['tick_filter_rate'] = self.stats['filtered_ticks'] / self.stats['total_ticks']
        else:
            info['tick_filter_rate'] = 0

        if self.stats['total_bars'] > 0:
            info['bar_filter_rate'] = self.stats['filtered_bars'] / self.stats['total_bars']
        else:
            info['bar_filter_rate'] = 0

        return info


class DataMerger:
    """数据合并器 - 合并Redis当日数据和ClickHouse历史数据"""

    @staticmethod
    def merge_bar_data(redis_data: List[BarData], clickhouse_data: List[BarData]) -> List[BarData]:
        """合并分钟线数据"""
        # 合并数据并按时间排序
        all_data = redis_data + clickhouse_data

        # 去重（以frame和symbol为键）
        unique_data = {}
        for bar in all_data:
            key = (bar.symbol, bar.frame)
            unique_data[key] = bar

        # 按时间排序
        merged_data = list(unique_data.values())
        merged_data.sort(key=lambda x: x.frame)

        return merged_data
