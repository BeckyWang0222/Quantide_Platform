# -*- coding: utf-8 -*-
"""
数据模型定义
"""
from datetime import datetime
from typing import Optional
from pydantic import BaseModel


class TickData(BaseModel):
    """分笔数据模型"""
    symbol: str
    time: datetime
    price: float
    volume: int
    amount: float
    bid_price: Optional[float] = None
    ask_price: Optional[float] = None
    bid_volume: Optional[int] = None
    ask_volume: Optional[int] = None

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class BarData(BaseModel):
    """分钟线数据模型"""
    symbol: str
    frame: datetime
    open: float
    high: float
    low: float
    close: float
    vol: float
    amount: float

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class SystemStatus(BaseModel):
    """系统状态模型"""
    service_name: str
    status: str  # running, stopped, error
    last_update: datetime
    message: str
    data_count: int = 0

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class QueryRequest(BaseModel):
    """查询请求模型"""
    symbol: str
    start_time: datetime
    end_time: datetime
    period: int  # 1, 5, 15, 30


class QueryResponse(BaseModel):
    """查询响应模型"""
    success: bool
    message: str
    data: list[BarData] = []
    total_count: int = 0


class HistoricalDataRequest(BaseModel):
    """历史数据请求模型"""
    start_time: datetime
    end_time: datetime
    symbols: list[str] = []  # 股票代码列表，空列表表示所有股票
    periods: list[int] = [1, 5, 15, 30]  # 需要的周期


class HistoricalDataResponse(BaseModel):
    """历史数据响应模型"""
    success: bool
    message: str
    task_id: str = ""
    total_symbols: int = 0
    processed_symbols: int = 0
    total_records: int = 0
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
