#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
工具函数模块

提供系统中使用的各种工具函数。
"""

import time
import datetime
import functools
import pandas as pd
import requests
import tushare as ts
from typing import List, Dict, Any, Callable, Optional, Union

from logger import logger
from exceptions import DataFetchError, TushareAPIError
from config_loader import TUSHARE_CONFIG, SCHEDULER_CONFIG

# 初始化Tushare API
ts.set_token(TUSHARE_CONFIG.token)
pro = ts.pro_api()


def retry(max_retries: int = None, retry_interval: int = None,
          exceptions: tuple = (Exception,)) -> Callable:
    """
    重试装饰器

    Args:
        max_retries (int, optional): 最大重试次数. 默认为None，使用配置文件中的值.
        retry_interval (int, optional): 重试间隔(秒). 默认为None，使用配置文件中的值.
        exceptions (tuple, optional): 需要捕获的异常类型. 默认为(Exception,).

    Returns:
        Callable: 装饰器函数
    """
    if max_retries is None:
        max_retries = SCHEDULER_CONFIG.max_retries

    if retry_interval is None:
        retry_interval = SCHEDULER_CONFIG.retry_interval

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            retries = 0
            while retries <= max_retries:
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    retries += 1
                    if retries > max_retries:
                        logger.error(f"函数 {func.__name__} 执行失败，已达到最大重试次数: {e}")
                        raise

                    logger.warning(f"函数 {func.__name__} 执行失败，正在进行第 {retries} 次重试: {e}")
                    time.sleep(retry_interval)

        return wrapper

    return decorator


def is_trade_day(date: Union[str, datetime.date] = None) -> bool:
    """
    判断给定日期是否为交易日

    Args:
        date (Union[str, datetime.date], optional): 日期，格式为'YYYYMMDD'或datetime.date对象. 默认为None，表示当天.

    Returns:
        bool: 是否为交易日
    """
    if date is None:
        date = datetime.date.today().strftime('%Y%m%d')
    elif isinstance(date, datetime.date):
        date = date.strftime('%Y%m%d')

    try:
        # 调用Tushare API获取交易日历
        df = pro.trade_cal(exchange='SSE', start_date=date, end_date=date, is_open='1')

        # 判断是否为交易日
        return not df.empty

    except Exception as e:
        logger.error(f"判断交易日失败: {e}")
        # 发生异常时，默认为交易日，确保数据获取不会因为判断失败而中断
        return True


def get_last_trade_day(date: Union[str, datetime.date] = None) -> str:
    """
    获取给定日期的上一个交易日

    Args:
        date (Union[str, datetime.date], optional): 日期，格式为'YYYYMMDD'或datetime.date对象. 默认为None，表示当天.

    Returns:
        str: 上一个交易日，格式为'YYYYMMDD'
    """
    if date is None:
        date = datetime.date.today()
    elif isinstance(date, str):
        date = datetime.datetime.strptime(date, '%Y%m%d').date()

    # 获取前30天的日期范围，确保能找到上一个交易日
    end_date = date.strftime('%Y%m%d')
    start_date = (date - datetime.timedelta(days=30)).strftime('%Y%m%d')

    try:
        # 调用Tushare API获取交易日历
        df = pro.trade_cal(exchange='SSE', start_date=start_date, end_date=end_date, is_open='1')

        if df.empty:
            logger.warning(f"未找到交易日，返回当前日期: {end_date}")
            return end_date

        # 按日期排序
        df = df.sort_values(by='cal_date', ascending=False)

        # 获取小于当前日期的最大交易日
        last_trade_days = df[df['cal_date'] < end_date]

        if last_trade_days.empty:
            logger.warning(f"未找到上一个交易日，返回当前日期: {end_date}")
            return end_date

        return last_trade_days.iloc[0]['cal_date']

    except Exception as e:
        logger.error(f"获取上一个交易日失败: {e}")
        # 发生异常时，返回前一天日期
        return (date - datetime.timedelta(days=1)).strftime('%Y%m%d')
