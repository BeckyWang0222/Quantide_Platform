#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
主程序入口

日线数据定时获取系统的主程序入口。
"""

import sys
import time
import signal
import argparse
import datetime
import pandas as pd
from typing import Dict, List, Any, Union, Optional

from config_loader import config
from logger import logger
from scheduler import scheduler
from data_fetcher import data_fetcher
from data_processor import data_processor
from redis_handler import redis_handler
from clickhouse_handler import clickhouse_handler
from utils import is_trade_day, get_last_trade_day


def signal_handler(sig, frame):
    """
    信号处理函数

    Args:
        sig: 信号
        frame: 帧
    """
    logger.info("接收到终止信号，正在关闭系统...")

    # 关闭调度器
    scheduler.shutdown()

    # 关闭Redis连接
    redis_handler.close()

    # 关闭ClickHouse连接
    clickhouse_handler.close()

    logger.info("系统已安全关闭")
    sys.exit(0)


def parse_args():
    """
    解析命令行参数

    Returns:
        argparse.Namespace: 解析后的参数
    """
    parser = argparse.ArgumentParser(description='日线数据定时获取系统')

    # 添加子命令
    subparsers = parser.add_subparsers(dest='command', help='命令')

    # 启动命令
    start_parser = subparsers.add_parser('start', help='启动系统')

    # 获取当日数据命令
    daily_parser = subparsers.add_parser('daily', help='获取当日数据')
    daily_parser.add_argument('--batch-size', type=int, default=1000, help='批量获取的股票数量，默认为1000')

    # 获取历史数据命令
    history_parser = subparsers.add_parser('history', help='获取历史数据')
    history_parser.add_argument('--days', type=int, default=7, help='获取的天数，默认为7天')
    history_parser.add_argument('--start', type=str, help='开始日期，格式为YYYYMMDD')
    history_parser.add_argument('--end', type=str, help='结束日期，格式为YYYYMMDD')
    history_parser.add_argument('--force', action='store_true', help='强制获取所有数据，不跳过已存在的数据')
    history_parser.add_argument('--batch-size', type=int, default=1000, help='批量获取的股票数量，默认为1000')

    # 更新股票列表命令
    stock_list_parser = subparsers.add_parser('stock_list', help='更新股票列表')

    # 更新交易日历命令
    trade_cal_parser = subparsers.add_parser('trade_cal', help='更新交易日历')

    # 检查连接命令
    check_parser = subparsers.add_parser('check', help='检查连接')
    check_parser.add_argument('--redis', action='store_true', help='检查Redis连接')
    check_parser.add_argument('--clickhouse', action='store_true', help='检查ClickHouse连接')

    # 显示数据范围命令
    subparsers.add_parser('info', help='显示ClickHouse中的数据信息')

    # 检查并补充不完整数据命令
    complete_parser = subparsers.add_parser('complete', help='检查并补充不完整的数据')
    complete_parser.add_argument('--date', type=str, help='指定日期，格式为YYYYMMDD')
    complete_parser.add_argument('--start', type=str, help='开始日期，格式为YYYYMMDD')
    complete_parser.add_argument('--end', type=str, help='结束日期，格式为YYYYMMDD')
    complete_parser.add_argument('--batch-size', type=int, default=100, help='批量获取的股票数量，默认为100')

    return parser.parse_args()


def start_system():
    """启动系统"""
    logger.info("正在启动日线数据定时获取系统...")

    # 注册信号处理函数
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        # 启动调度器
        scheduler.start()

        logger.info("系统已启动，按Ctrl+C终止")

        # 保持主线程运行
        while True:
            time.sleep(1)

    except KeyboardInterrupt:
        logger.info("接收到键盘中断，正在关闭系统...")
        scheduler.shutdown()
        redis_handler.close()
        clickhouse_handler.close()
        logger.info("系统已安全关闭")

    except Exception as e:
        logger.error(f"系统运行异常: {e}")
        scheduler.shutdown()
        redis_handler.close()
        clickhouse_handler.close()
        logger.error("系统已关闭")
        sys.exit(1)


def fetch_daily_data(batch_size: int = 1000):
    """
    获取当日数据

    Args:
        batch_size (int, optional): 批量获取的股票数量. 默认为1000.
    """
    logger.info("手动获取当日数据")

    # 检查是否为交易日
    if not is_trade_day():
        logger.info("今天不是交易日，是否继续获取？(y/n)")
        choice = input().strip().lower()
        if choice != 'y':
            logger.info("已取消获取当日数据")
            return

    try:
        # 获取当日数据，使用分批获取
        df = data_fetcher.get_daily_data(batch_size=batch_size)

        if df.empty:
            logger.warning("获取当日数据为空")
            return

        # 处理数据并直接存储到ClickHouse
        data_processor.process_and_store(df)

        # 检查数据是否完整
        trade_date = datetime.date.today().strftime('%Y%m%d')
        is_complete = data_fetcher.check_and_complete_date_data(trade_date=trade_date, batch_size=batch_size)

        if not is_complete:
            logger.warning(f"日期 {trade_date} 的数据不完整，已尝试补充")

        logger.info("当日数据获取完成")

    except Exception as e:
        logger.error(f"获取当日数据失败: {e}")


def fetch_historical_data(days=7, start_date=None, end_date=None, skip_existing=True, batch_size=1000):
    """
    获取历史数据

    Args:
        days (int, optional): 获取的天数. 默认为7.
        start_date (str, optional): 开始日期，格式为'YYYYMMDD'. 默认为None.
        end_date (str, optional): 结束日期，格式为'YYYYMMDD'. 默认为None.
        skip_existing (bool, optional): 是否跳过已存在的数据. 默认为True.
        batch_size (int, optional): 批量获取的股票数量. 默认为1000.
    """
    logger.info("手动获取历史数据")

    try:
        # 计算日期范围
        if start_date and end_date:
            logger.info(f"获取指定日期范围的历史数据: {start_date} - {end_date}")
        else:
            end_date = datetime.date.today() - datetime.timedelta(days=1)
            start_date = end_date - datetime.timedelta(days=days-1)

            # 格式化日期
            start_date = start_date.strftime('%Y%m%d')
            end_date = end_date.strftime('%Y%m%d')

            logger.info(f"获取最近 {days} 天的历史数据: {start_date} - {end_date}")

        # 获取历史数据，跳过已存在的数据
        df = data_fetcher.get_historical_daily_data(start_date, end_date, skip_existing=skip_existing)

        if df.empty:
            logger.info("没有需要获取的新数据")
            return

        # 直接存储到ClickHouse
        data_processor.process_and_store(df)

        # 检查数据是否完整
        for date in pd.date_range(start=start_date, end=end_date, freq='D'):
            date_str = date.strftime('%Y%m%d')
            # 检查是否为交易日
            if is_trade_day(date_str):
                is_complete = data_fetcher.check_and_complete_date_data(
                    trade_date=date_str,
                    batch_size=batch_size
                )
                if not is_complete:
                    logger.warning(f"日期 {date_str} 的数据不完整，已尝试补充")

        logger.info("历史数据获取完成")

    except Exception as e:
        logger.error(f"获取历史数据失败: {e}")


def update_stock_list():
    """更新股票列表"""
    logger.info("手动更新股票列表")

    try:
        # 强制更新股票列表
        df = data_fetcher.get_stock_list(force_update=True)

        if df.empty:
            logger.warning("更新股票列表为空")
            return

        logger.info(f"股票列表更新完成，共 {len(df)} 条记录")

    except Exception as e:
        logger.error(f"更新股票列表失败: {e}")


def update_trade_calendar():
    """更新交易日历"""
    logger.info("手动更新交易日历")

    try:
        # 获取当年的交易日历
        today = datetime.date.today()
        start_date = today.replace(month=1, day=1).strftime('%Y%m%d')
        end_date = today.replace(month=12, day=31).strftime('%Y%m%d')

        df = data_fetcher.get_trade_calendar(start_date, end_date)

        if df.empty:
            logger.warning("更新交易日历为空")
            return

        logger.info(f"交易日历更新完成，共 {len(df)} 条记录")

    except Exception as e:
        logger.error(f"更新交易日历失败: {e}")


def check_connections(check_redis=True, check_clickhouse=True):
    """
    检查连接

    Args:
        check_redis (bool, optional): 是否检查Redis连接. 默认为True.
        check_clickhouse (bool, optional): 是否检查ClickHouse连接. 默认为True.
    """
    logger.info("手动检查连接")

    if check_redis:
        try:
            logger.info("检查Redis连接...")
            result = redis_handler.check_connection()

            if result:
                logger.info("Redis连接正常")
            else:
                logger.error("Redis连接异常")

        except Exception as e:
            logger.error(f"检查Redis连接失败: {e}")

    if check_clickhouse:
        try:
            logger.info("检查ClickHouse连接...")
            result = clickhouse_handler.check_connection()

            if result:
                logger.info("ClickHouse连接正常")
            else:
                logger.error("ClickHouse连接异常")

        except Exception as e:
            logger.error(f"检查ClickHouse连接失败: {e}")


def show_clickhouse_data_range():
    """显示ClickHouse中的数据时间范围"""
    try:
        from clickhouse_handler import clickhouse_handler
        earliest_date, latest_date = clickhouse_handler.get_date_range()

        if earliest_date and latest_date:
            logger.info("=" * 50)
            logger.info(f"ClickHouse中已有数据的时间范围: {earliest_date} - {latest_date}")

            # 获取数据总量
            if clickhouse_handler.check_connection():
                try:
                    query = f"SELECT COUNT(*) FROM {clickhouse_handler.database}.{clickhouse_handler.table}"
                    result = clickhouse_handler.client.execute(query)
                    count = result[0][0]
                    logger.info(f"ClickHouse中共有 {count} 条数据记录")
                except Exception as e:
                    logger.error(f"获取数据总量失败: {e}")

            logger.info("=" * 50)
        else:
            logger.info("=" * 50)
            logger.info("ClickHouse中暂无数据")
            logger.info("=" * 50)

    except Exception as e:
        logger.error(f"显示ClickHouse数据范围失败: {e}")


def main():
    """主函数"""
    args = parse_args()

    if args.command == 'start':
        start_system()

    elif args.command == 'daily':
        # 如果complete命令中指定了batch_size，则使用该值
        batch_size = getattr(args, 'batch_size', 1000)
        fetch_daily_data(batch_size=batch_size)
        # 显示ClickHouse中的数据时间范围
        show_clickhouse_data_range()

    elif args.command == 'history':
        # 如果指定了--force参数，则不跳过已存在的数据
        skip_existing = not args.force
        batch_size = args.batch_size

        if args.start and args.end:
            fetch_historical_data(
                start_date=args.start,
                end_date=args.end,
                skip_existing=skip_existing,
                batch_size=batch_size
            )
        else:
            fetch_historical_data(
                days=args.days,
                skip_existing=skip_existing,
                batch_size=batch_size
            )

        # 显示ClickHouse中的数据时间范围
        show_clickhouse_data_range()

    elif args.command == 'stock_list':
        update_stock_list()

    elif args.command == 'trade_cal':
        update_trade_calendar()

    elif args.command == 'check':
        check_connections(check_redis=args.redis, check_clickhouse=args.clickhouse)
        # 显示ClickHouse中的数据时间范围
        if args.clickhouse or not (args.redis or args.clickhouse):
            show_clickhouse_data_range()

    elif args.command == 'info':
        # 显示ClickHouse中的数据信息
        show_clickhouse_data_range()

    elif args.command == 'complete':
        # 检查并补充不完整的数据
        if args.date:
            # 补充指定日期的数据
            data_fetcher.check_and_complete_date_data(
                trade_date=args.date,
                batch_size=args.batch_size
            )
        elif args.start and args.end:
            # 补充指定日期范围的数据
            data_fetcher.check_and_complete_date_range(
                start_date=args.start,
                end_date=args.end,
                batch_size=args.batch_size
            )
        else:
            # 补充所有不完整的数据
            data_fetcher.check_and_complete_date_range(
                batch_size=args.batch_size
            )

        # 显示ClickHouse中的数据信息
        show_clickhouse_data_range()

    else:
        logger.info("请指定命令，使用 -h 查看帮助")


if __name__ == '__main__':
    main()
