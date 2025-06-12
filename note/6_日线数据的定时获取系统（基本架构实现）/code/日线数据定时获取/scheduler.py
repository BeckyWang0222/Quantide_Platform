#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
调度器模块

提供定时任务调度功能。
"""

import datetime
import traceback
from typing import Dict, List, Any, Union, Optional, Callable
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.jobstores.memory import MemoryJobStore
from apscheduler.executors.pool import ThreadPoolExecutor

from config import SCHEDULER_CONFIG
from logger import logger
from exceptions import SchedulerError
from monitor import monitor
from data_fetcher import data_fetcher
from data_processor import data_processor
from redis_handler import redis_handler
from clickhouse_handler import clickhouse_handler
from utils import is_trade_day, get_last_trade_day


class Scheduler:
    """调度器类，提供定时任务调度功能"""

    def __init__(self, config: Dict = None):
        """
        初始化调度器类

        Args:
            config (Dict, optional): 调度器配置. 默认为None，使用配置文件中的值.
        """
        self.config = config or SCHEDULER_CONFIG
        self.scheduler = None
        self.initialize()

    def initialize(self) -> None:
        """
        初始化调度器

        Raises:
            SchedulerError: 初始化调度器失败时抛出
        """
        try:
            # 获取作业存储配置
            job_stores_config = self.config.get('job_stores', {'default': {'type': 'memory'}})
            job_stores = {}
            for name, config in job_stores_config.items():
                store_type = config.get('type')
                if store_type == 'memory':
                    job_stores[name] = MemoryJobStore()
                # 可以添加其他类型的作业存储

            # 获取执行器配置
            executors_config = self.config.get('executors', {'default': {'type': 'threadpool', 'max_workers': 10}})
            executors = {}
            for name, config in executors_config.items():
                executor_type = config.get('type')
                if executor_type == 'threadpool':
                    max_workers = config.get('max_workers', 10)
                    executors[name] = ThreadPoolExecutor(max_workers)
                # 可以添加其他类型的执行器

            # 获取作业默认配置
            job_defaults = self.config.get('job_defaults', {
                'coalesce': False,
                'max_instances': 3
            })

            # 创建调度器
            self.scheduler = BackgroundScheduler(
                jobstores=job_stores,
                executors=executors,
                job_defaults=job_defaults
            )

            logger.info("调度器初始化成功")

        except Exception as e:
            logger.error(f"初始化调度器失败: {e}")
            raise SchedulerError(f"初始化调度器失败: {e}")

    def start(self) -> None:
        """
        启动调度器

        Raises:
            SchedulerError: 启动调度器失败时抛出
        """
        try:
            if self.scheduler is None:
                self.initialize()

            # 添加定时任务
            self._add_jobs()

            # 启动调度器
            self.scheduler.start()
            logger.info("调度器已启动")

        except Exception as e:
            logger.error(f"启动调度器失败: {e}")
            raise SchedulerError(f"启动调度器失败: {e}")

    def shutdown(self) -> None:
        """
        关闭调度器

        Raises:
            SchedulerError: 关闭调度器失败时抛出
        """
        try:
            if self.scheduler is not None:
                self.scheduler.shutdown()
                self.scheduler = None
                logger.info("调度器已关闭")

        except Exception as e:
            logger.error(f"关闭调度器失败: {e}")
            raise SchedulerError(f"关闭调度器失败: {e}")

    def _add_jobs(self) -> None:
        """
        添加定时任务

        Raises:
            SchedulerError: 添加定时任务失败时抛出
        """
        try:
            # 获取定时任务配置
            daily_data_time = self.config.get('daily_data_time', '15:30:00')
            historical_data_time = self.config.get('historical_data_time', '01:00:00')
            stock_list_update_time = self.config.get('stock_list_update_time', '00:10:00')
            trade_cal_update_time = self.config.get('trade_cal_update_time', '00:20:00')
            redis_check_time = self.config.get('redis_check_time', '07:00:00')
            clickhouse_check_time = self.config.get('clickhouse_check_time', '07:10:00')

            # 解析时间
            daily_time_parts = daily_data_time.split(':')
            historical_time_parts = historical_data_time.split(':')
            stock_list_time_parts = stock_list_update_time.split(':')
            trade_cal_time_parts = trade_cal_update_time.split(':')
            redis_check_time_parts = redis_check_time.split(':')
            clickhouse_check_time_parts = clickhouse_check_time.split(':')

            # 添加每天获取当日数据的任务
            self.scheduler.add_job(
                self._job_wrapper(self._fetch_daily_data),
                CronTrigger(
                    hour=daily_time_parts[0],
                    minute=daily_time_parts[1],
                    second=daily_time_parts[2]
                ),
                id='fetch_daily_data',
                name='获取当日数据'
            )

            # 添加每天获取历史数据的任务
            self.scheduler.add_job(
                self._job_wrapper(self._fetch_historical_data),
                CronTrigger(
                    hour=historical_time_parts[0],
                    minute=historical_time_parts[1],
                    second=historical_time_parts[2]
                ),
                id='fetch_historical_data',
                name='获取历史数据'
            )

            # 添加每月更新股票列表的任务
            self.scheduler.add_job(
                self._job_wrapper(self._update_stock_list),
                CronTrigger(
                    day='1',
                    hour=stock_list_time_parts[0],
                    minute=stock_list_time_parts[1],
                    second=stock_list_time_parts[2]
                ),
                id='update_stock_list',
                name='更新股票列表'
            )

            # 添加每月更新交易日历的任务
            self.scheduler.add_job(
                self._job_wrapper(self._update_trade_calendar),
                CronTrigger(
                    day='1',
                    hour=trade_cal_time_parts[0],
                    minute=trade_cal_time_parts[1],
                    second=trade_cal_time_parts[2]
                ),
                id='update_trade_calendar',
                name='更新交易日历'
            )

            # 添加每周检查Redis连接的任务
            self.scheduler.add_job(
                self._job_wrapper(self._check_redis_connection),
                CronTrigger(
                    day_of_week='mon',
                    hour=redis_check_time_parts[0],
                    minute=redis_check_time_parts[1],
                    second=redis_check_time_parts[2]
                ),
                id='check_redis_connection',
                name='检查Redis连接'
            )

            # 添加每周检查ClickHouse连接的任务
            self.scheduler.add_job(
                self._job_wrapper(self._check_clickhouse_connection),
                CronTrigger(
                    day_of_week='mon',
                    hour=clickhouse_check_time_parts[0],
                    minute=clickhouse_check_time_parts[1],
                    second=clickhouse_check_time_parts[2]
                ),
                id='check_clickhouse_connection',
                name='检查ClickHouse连接'
            )

            logger.info("已添加所有定时任务")

        except Exception as e:
            logger.error(f"添加定时任务失败: {e}")
            raise SchedulerError(f"添加定时任务失败: {e}")

    def _job_wrapper(self, job_func: Callable) -> Callable:
        """
        作业包装器，用于捕获作业执行过程中的异常

        Args:
            job_func (Callable): 作业函数

        Returns:
            Callable: 包装后的作业函数
        """
        def wrapper(*args, **kwargs):
            job_id = kwargs.get('job_id', job_func.__name__)
            try:
                logger.info(f"开始执行作业: {job_id}")
                result = job_func(*args, **kwargs)
                logger.info(f"作业执行完成: {job_id}")
                return result
            except Exception as e:
                logger.error(f"作业执行失败: {job_id}, 异常: {e}")
                logger.error(traceback.format_exc())
                # 发送报警
                monitor.alert_scheduler_failure(job_id, e)
                raise

        return wrapper

    def _fetch_daily_data(self, batch_size: int = 1000) -> bool:
        """
        获取当日数据

        Args:
            batch_size (int, optional): 批量获取的股票数量. 默认为1000.

        Returns:
            bool: 获取结果，True表示成功，False表示失败
        """
        # 检查是否为交易日
        if not is_trade_day():
            logger.info("今天不是交易日，跳过获取当日数据")
            return False

        try:
            logger.info("开始获取当日数据")

            # 获取当日数据，使用分批获取
            df = data_fetcher.get_daily_data(batch_size=batch_size)

            if df.empty:
                logger.warning("获取当日数据为空")
                return False

            # 处理数据并直接存储到ClickHouse
            result = data_processor.process_and_store(df)

            # 检查数据是否完整
            trade_date = datetime.date.today().strftime('%Y%m%d')
            is_complete = data_fetcher.check_and_complete_date_data(trade_date=trade_date, batch_size=batch_size)

            if not is_complete:
                logger.warning(f"日期 {trade_date} 的数据不完整，已尝试补充")

            logger.info("当日数据获取完成")
            return result

        except Exception as e:
            logger.error(f"获取当日数据失败: {e}")
            # 发送报警
            monitor.alert_data_fetch_failure("当日数据", e)
            raise

    def _fetch_historical_data(self, days: int = 7, skip_existing: bool = True) -> bool:
        """
        获取历史数据

        Args:
            days (int, optional): 获取的天数. 默认为7.
            skip_existing (bool, optional): 是否跳过已存在的数据. 默认为True.

        Returns:
            bool: 获取结果，True表示成功，False表示失败
        """
        try:
            logger.info(f"开始获取历史数据，天数: {days}, 跳过已存在数据: {skip_existing}")

            # 计算日期范围
            end_date = datetime.date.today() - datetime.timedelta(days=1)
            start_date = end_date - datetime.timedelta(days=days-1)

            # 格式化日期
            start_date_str = start_date.strftime('%Y%m%d')
            end_date_str = end_date.strftime('%Y%m%d')

            # 获取历史数据，跳过已存在的数据
            df = data_fetcher.get_historical_daily_data(start_date_str, end_date_str, skip_existing=skip_existing)

            if df.empty:
                logger.info("没有需要获取的新数据")
                return True

            # 直接存储到ClickHouse
            result = data_processor.process_and_store(df)

            logger.info("历史数据获取完成")
            return result

        except Exception as e:
            logger.error(f"获取历史数据失败: {e}")
            # 发送报警
            monitor.alert_data_fetch_failure("历史数据", e)
            raise

    def _update_stock_list(self) -> bool:
        """
        更新股票列表

        Returns:
            bool: 更新结果，True表示成功，False表示失败
        """
        try:
            logger.info("开始更新股票列表")

            # 强制更新股票列表
            df = data_fetcher.get_stock_list(force_update=True)

            if df.empty:
                logger.warning("更新股票列表为空")
                return False

            logger.info(f"股票列表更新完成，共 {len(df)} 条记录")
            return True

        except Exception as e:
            logger.error(f"更新股票列表失败: {e}")
            # 发送报警
            monitor.alert_data_fetch_failure("股票列表", e)
            raise

    def _update_trade_calendar(self) -> bool:
        """
        更新交易日历

        Returns:
            bool: 更新结果，True表示成功，False表示失败
        """
        try:
            logger.info("开始更新交易日历")

            # 获取当年的交易日历
            today = datetime.date.today()
            start_date = today.replace(month=1, day=1).strftime('%Y%m%d')
            end_date = today.replace(month=12, day=31).strftime('%Y%m%d')

            df = data_fetcher.get_trade_calendar(start_date, end_date)

            if df.empty:
                logger.warning("更新交易日历为空")
                return False

            logger.info(f"交易日历更新完成，共 {len(df)} 条记录")
            return True

        except Exception as e:
            logger.error(f"更新交易日历失败: {e}")
            # 发送报警
            monitor.alert_data_fetch_failure("交易日历", e)
            raise

    def _check_redis_connection(self) -> bool:
        """
        检查Redis连接

        Returns:
            bool: 检查结果，True表示成功，False表示失败
        """
        try:
            logger.info("开始检查Redis连接")

            # 检查Redis连接
            result = redis_handler.check_connection()

            if result:
                logger.info("Redis连接正常")
            else:
                logger.error("Redis连接异常")
                # 发送报警
                monitor.alert_connection_failure("Redis", Exception("Redis连接异常"))

            return result

        except Exception as e:
            logger.error(f"检查Redis连接失败: {e}")
            # 发送报警
            monitor.alert_connection_failure("Redis", e)
            raise

    def _check_clickhouse_connection(self) -> bool:
        """
        检查ClickHouse连接

        Returns:
            bool: 检查结果，True表示成功，False表示失败
        """
        try:
            logger.info("开始检查ClickHouse连接")

            # 检查ClickHouse连接
            result = clickhouse_handler.check_connection()

            if result:
                logger.info("ClickHouse连接正常")
            else:
                logger.error("ClickHouse连接异常")
                # 发送报警
                monitor.alert_connection_failure("ClickHouse", Exception("ClickHouse连接异常"))

            return result

        except Exception as e:
            logger.error(f"检查ClickHouse连接失败: {e}")
            # 发送报警
            monitor.alert_connection_failure("ClickHouse", e)
            raise


# 创建默认调度器实例
scheduler = Scheduler()
