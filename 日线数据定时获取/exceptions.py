#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
异常处理模块

定义系统中使用的自定义异常类。
"""


class DayBarFetcherError(Exception):
    """日线数据获取系统基础异常类"""
    pass


class ConfigError(DayBarFetcherError):
    """配置错误异常"""
    pass


class TushareAPIError(DayBarFetcherError):
    """Tushare API调用异常"""
    pass


class DataFetchError(DayBarFetcherError):
    """数据获取异常"""
    pass


class DataProcessError(DayBarFetcherError):
    """数据处理异常"""
    pass


class RedisConnectionError(DayBarFetcherError):
    """Redis连接异常"""
    pass


class RedisOperationError(DayBarFetcherError):
    """Redis操作异常"""
    pass


class ClickHouseConnectionError(DayBarFetcherError):
    """ClickHouse连接异常"""
    pass


class ClickHouseOperationError(DayBarFetcherError):
    """ClickHouse操作异常"""
    pass


class SchedulerError(DayBarFetcherError):
    """调度器异常"""
    pass


class MonitorError(DayBarFetcherError):
    """监控报警异常"""
    pass
