# -*- coding: utf-8 -*-
"""
QMT历史数据获取器
用于从QMT获取历史分钟线数据
"""
import sys
import os
import time
import threading
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import uuid
import logging

from models import BarData, HistoricalDataRequest, HistoricalDataResponse
from database import RedisManager
from trading_time_validator import TradingTimeValidator


class QMTHistoricalFetcher:
    """QMT历史数据获取器"""

    def __init__(self):
        self.redis_manager = RedisManager()
        self.trading_validator = TradingTimeValidator()
        self.is_fetching = False
        self.current_task = None
        self.logger = logging.getLogger(__name__)

        # 任务状态跟踪
        self.task_status = {}

        # 默认股票列表
        self.default_symbols = [
            "000001.SZ", "000002.SZ", "000858.SZ", "000725.SZ", "000776.SZ",
            "600000.SH", "600036.SH", "600519.SH", "600887.SH", "601318.SH",
            "600276.SH", "601166.SH", "000858.SZ", "002415.SZ", "300059.SZ"
        ]

    def fetch_historical_data(self, request: HistoricalDataRequest) -> HistoricalDataResponse:
        """
        获取历史数据

        Args:
            request: 历史数据请求

        Returns:
            HistoricalDataResponse: 响应结果
        """
        try:
            # 生成任务ID
            task_id = str(uuid.uuid4())

            # 验证参数
            if request.start_time >= request.end_time:
                return HistoricalDataResponse(
                    success=False,
                    message="开始时间必须早于结束时间",
                    task_id=task_id
                )

            # 使用默认股票列表如果未指定
            symbols = request.symbols if request.symbols else self.default_symbols

            # 初始化任务状态
            self.task_status[task_id] = {
                "start_time": datetime.now(),
                "request": request,
                "symbols": symbols,
                "total_symbols": len(symbols),
                "processed_symbols": 0,
                "total_records": 0,
                "filtered_records": 0,
                "status": "running",
                "message": "任务已启动"
            }

            # 启动后台任务
            thread = threading.Thread(
                target=self._fetch_data_background,
                args=(task_id, request, symbols),
                daemon=True
            )
            thread.start()

            return HistoricalDataResponse(
                success=True,
                message="历史数据获取任务已启动",
                task_id=task_id,
                total_symbols=len(symbols),
                processed_symbols=0,
                total_records=0,
                start_time=request.start_time,
                end_time=request.end_time
            )

        except Exception as e:
            self.logger.error(f"启动历史数据获取任务失败: {e}")
            return HistoricalDataResponse(
                success=False,
                message=f"启动任务失败: {str(e)}",
                task_id=""
            )

    def _fetch_data_background(self, task_id: str, request: HistoricalDataRequest, symbols: List[str]):
        """后台获取数据"""
        try:
            self.is_fetching = True
            self.current_task = task_id

            total_records = 0
            filtered_records = 0
            processed_symbols = 0

            for symbol in symbols:
                try:
                    # 更新任务状态
                    self.task_status[task_id]["processed_symbols"] = processed_symbols
                    self.task_status[task_id]["message"] = f"正在处理 {symbol}"

                    # 获取各个周期的数据
                    for period in request.periods:
                        bars = self._fetch_symbol_data(symbol, request.start_time, request.end_time, period)

                        # 过滤交易时间内的数据并发布到Redis（标记为历史数据）
                        for bar in bars:
                            bar_dict = {
                                'frame': bar.frame,
                                'symbol': bar.symbol,
                                'open': bar.open,
                                'high': bar.high,
                                'low': bar.low,
                                'close': bar.close,
                                'vol': bar.vol,
                                'amount': bar.amount
                            }

                            # 验证是否为交易时间内的数据
                            if self.trading_validator.validate_bar_data(bar_dict):
                                # 明确标记为历史数据，发布到队列供Mac端消费
                                self.redis_manager.publish_bar_data(bar, period, is_historical=True)
                                total_records += 1
                            else:
                                filtered_records += 1
                                self.logger.debug(f"过滤非交易时间历史数据: {symbol} {bar.frame}")

                    processed_symbols += 1

                    # 更新进度
                    self.task_status[task_id]["processed_symbols"] = processed_symbols
                    self.task_status[task_id]["total_records"] = total_records
                    self.task_status[task_id]["filtered_records"] = filtered_records

                    # 避免请求过于频繁
                    time.sleep(0.1)

                except Exception as e:
                    self.logger.error(f"获取 {symbol} 数据失败: {e}")
                    continue

            # 任务完成
            self.task_status[task_id]["status"] = "completed"
            self.task_status[task_id]["message"] = f"任务完成，共处理 {total_records} 条有效记录，过滤 {filtered_records} 条非交易时间记录"
            self.task_status[task_id]["end_time"] = datetime.now()

            self.logger.info(f"历史数据获取任务 {task_id} 完成，共获取 {total_records} 条有效记录，过滤 {filtered_records} 条记录")

        except Exception as e:
            self.task_status[task_id]["status"] = "error"
            self.task_status[task_id]["message"] = f"任务执行错误: {str(e)}"
            self.logger.error(f"历史数据获取任务 {task_id} 失败: {e}")
        finally:
            self.is_fetching = False
            self.current_task = None

    def _fetch_symbol_data(self, symbol: str, start_time: datetime, end_time: datetime, period: int) -> List[BarData]:
        """
        获取单个股票的历史数据

        Args:
            symbol: 股票代码
            start_time: 开始时间
            end_time: 结束时间
            period: 周期（分钟）

        Returns:
            List[BarData]: 分钟线数据列表
        """
        try:
            # 这里应该调用真实的QMT API
            # 由于QMT API需要实际环境，这里提供模拟数据
            return self._simulate_qmt_historical_data(symbol, start_time, end_time, period)

        except Exception as e:
            self.logger.error(f"获取 {symbol} 历史数据失败: {e}")
            return []

    def _simulate_qmt_historical_data(self, symbol: str, start_time: datetime, end_time: datetime, period: int) -> List[BarData]:
        """
        模拟QMT历史数据（实际使用时替换为真实的QMT API调用）

        实际QMT API调用示例：
        ```python
        from xtquant import xtdata

        # 连接QMT
        xtdata.connect()

        # 获取历史分钟线数据
        data = xtdata.get_market_data(
            stock_list=[symbol],
            period=f'{period}m',
            start_time=start_time.strftime('%Y%m%d%H%M%S'),
            end_time=end_time.strftime('%Y%m%d%H%M%S'),
            fields=['time', 'open', 'high', 'low', 'close', 'volume', 'amount']
        )
        ```
        """
        import random

        bars = []
        current_time = start_time
        base_price = random.uniform(10.0, 50.0)

        while current_time <= end_time:
            # 跳过非交易时间
            if not self._is_trading_time(current_time):
                current_time += timedelta(minutes=period)
                continue

            # 生成模拟数据
            price_change = random.uniform(-0.5, 0.5)
            open_price = base_price + price_change
            high_price = open_price + random.uniform(0, 1.0)
            low_price = open_price - random.uniform(0, 1.0)
            close_price = open_price + random.uniform(-0.5, 0.5)
            volume = random.randint(1000, 100000)
            amount = volume * close_price

            bar = BarData(
                symbol=symbol,
                frame=current_time,
                open=round(open_price, 2),
                high=round(high_price, 2),
                low=round(low_price, 2),
                close=round(close_price, 2),
                vol=volume,
                amount=round(amount, 2)
            )

            bars.append(bar)
            base_price = close_price  # 下一根K线的基准价格
            current_time += timedelta(minutes=period)

        return bars

    def _is_trading_time(self, dt: datetime) -> bool:
        """判断是否为交易时间（使用统一的验证器）"""
        return self.trading_validator.is_trading_time(dt)

    def get_task_status(self, task_id: str) -> Optional[Dict]:
        """获取任务状态"""
        return self.task_status.get(task_id)

    def get_all_tasks(self) -> Dict:
        """获取所有任务状态"""
        return self.task_status

    def cancel_task(self, task_id: str) -> bool:
        """取消任务"""
        if task_id in self.task_status:
            self.task_status[task_id]["status"] = "cancelled"
            self.task_status[task_id]["message"] = "任务已取消"
            return True
        return False

    def cleanup_old_tasks(self, hours: int = 24):
        """清理旧任务记录"""
        cutoff_time = datetime.now() - timedelta(hours=hours)

        to_remove = []
        for task_id, task_info in self.task_status.items():
            if task_info.get("start_time", datetime.now()) < cutoff_time:
                to_remove.append(task_id)

        for task_id in to_remove:
            del self.task_status[task_id]

        return len(to_remove)
