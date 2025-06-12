#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
数据获取模块

从Tushare获取日线数据。
"""

import datetime
import pandas as pd
import requests
import tushare as ts
from typing import Dict, List, Any, Union, Optional, Tuple

from config import TUSHARE_CONFIG
from logger import logger
from exceptions import DataFetchError, TushareAPIError
from utils import retry, is_trade_day, get_last_trade_day


class DataFetcher:
    """数据获取类，从Tushare获取日线数据"""

    def __init__(self, config: Dict = None):
        """
        初始化数据获取类

        Args:
            config (Dict, optional): Tushare配置. 默认为None，使用配置文件中的值.
        """
        self.config = config or TUSHARE_CONFIG
        self.token = self.config.get('token')

        # 初始化Tushare API
        ts.set_token(self.token)
        self.pro = ts.pro_api()
        logger.info("Tushare API初始化成功")

        # 股票列表缓存
        self.stock_list_cache = None
        self.stock_list_cache_date = None

    @retry(exceptions=(Exception, TushareAPIError))
    def _call_tushare_api(self, api_name: str, params: Dict = None, fields: str = None) -> pd.DataFrame:
        """
        调用Tushare API

        Args:
            api_name (str): API名称
            params (Dict, optional): API参数. 默认为None.
            fields (str, optional): 返回字段. 默认为None.

        Returns:
            pd.DataFrame: API返回的数据

        Raises:
            TushareAPIError: API调用失败时抛出
        """
        try:
            # 获取API方法
            api_method = getattr(self.pro, api_name)

            # 调用API
            if params:
                df = api_method(**params)
            else:
                df = api_method()

            # 检查返回结果
            if df is None or df.empty:
                logger.warning(f"Tushare API返回空数据: {api_name}")
                return pd.DataFrame()

            return df

        except AttributeError as e:
            error_msg = f"Tushare API方法不存在: {api_name}, 错误: {e}"
            logger.error(error_msg)
            raise TushareAPIError(error_msg)
        except Exception as e:
            error_msg = f"Tushare API调用异常: {e}"
            logger.error(error_msg)
            raise TushareAPIError(error_msg)

    def get_stock_list(self, force_update: bool = False) -> pd.DataFrame:
        """
        获取股票列表

        Args:
            force_update (bool, optional): 是否强制更新缓存. 默认为False.

        Returns:
            pd.DataFrame: 股票列表数据
        """
        today = datetime.date.today()

        # 如果缓存不存在或者已过期或者强制更新，则重新获取
        if (self.stock_list_cache is None or
            self.stock_list_cache_date is None or
            self.stock_list_cache_date != today or
            force_update):

            logger.info("获取股票列表...")

            # 获取股票基本信息
            df = self._call_tushare_api(
                api_name='stock_basic',
                params={
                    'exchange': '',
                    'list_status': 'L'  # 上市状态: L上市 D退市 P暂停上市
                },
                fields='ts_code,symbol,name,area,industry,list_date,market,exchange,is_hs'
            )

            if not df.empty:
                # 更新缓存
                self.stock_list_cache = df
                self.stock_list_cache_date = today
                logger.info(f"获取股票列表成功，共 {len(df)} 条记录")
            else:
                logger.warning("获取股票列表为空")
                # 如果获取失败但缓存存在，则使用缓存
                if self.stock_list_cache is not None:
                    logger.info("使用缓存的股票列表")
                    df = self.stock_list_cache
        else:
            logger.info("使用缓存的股票列表")
            df = self.stock_list_cache

        return df

    def get_trade_calendar(self, start_date: str = None, end_date: str = None) -> pd.DataFrame:
        """
        获取交易日历

        Args:
            start_date (str, optional): 开始日期，格式为'YYYYMMDD'. 默认为None，表示当年初.
            end_date (str, optional): 结束日期，格式为'YYYYMMDD'. 默认为None，表示当年底.

        Returns:
            pd.DataFrame: 交易日历数据
        """
        # 设置默认日期范围
        if start_date is None:
            start_date = datetime.date.today().replace(month=1, day=1).strftime('%Y%m%d')

        if end_date is None:
            end_date = datetime.date.today().replace(month=12, day=31).strftime('%Y%m%d')

        logger.info(f"获取交易日历，日期范围: {start_date} - {end_date}")

        # 获取交易日历
        df = self._call_tushare_api(
            api_name='trade_cal',
            params={
                'exchange': 'SSE',
                'start_date': start_date,
                'end_date': end_date
            },
            fields='exchange,cal_date,is_open,pretrade_date'
        )

        if not df.empty:
            logger.info(f"获取交易日历成功，共 {len(df)} 条记录")
        else:
            logger.warning("获取交易日历为空")

        return df

    def get_daily_data(self, trade_date: str = None, ts_code: str = None, batch_size: int = 1000) -> pd.DataFrame:
        """
        获取日线数据

        Args:
            trade_date (str, optional): 交易日期，格式为'YYYYMMDD'. 默认为None，表示最近一个交易日.
            ts_code (str, optional): 股票代码，如'000001.SZ'. 默认为None，表示获取所有股票.
            batch_size (int, optional): 批量获取的股票数量. 默认为1000.

        Returns:
            pd.DataFrame: 日线数据
        """
        # 如果未指定日期，则获取最近一个交易日
        if trade_date is None:
            if is_trade_day():
                trade_date = datetime.date.today().strftime('%Y%m%d')
            else:
                trade_date = get_last_trade_day()

        logger.info(f"获取日线数据，日期: {trade_date}, 股票代码: {ts_code or '所有'}")

        # 如果指定了股票代码，则直接获取
        if ts_code:
            # 构建API参数
            params = {'trade_date': trade_date, 'ts_code': ts_code}

            # 获取日线数据
            df = self._call_tushare_api(
                api_name='daily',
                params=params,
                fields='ts_code,trade_date,open,high,low,close,pre_close,change,pct_chg,vol,amount'
            )

            if not df.empty:
                # 获取涨跌停和ST数据
                df = self._enrich_daily_data(df)
                logger.info(f"获取日线数据成功，共 {len(df)} 条记录")
            else:
                logger.warning(f"获取日线数据为空，日期: {trade_date}, 股票代码: {ts_code}")

            return df

        # 如果没有指定股票代码，则获取所有股票列表，并分批获取
        stock_list = self.get_stock_list()

        if stock_list.empty:
            logger.warning("获取股票列表为空，无法获取日线数据")
            return pd.DataFrame()

        # 获取所有股票代码
        all_ts_codes = stock_list['ts_code'].tolist()

        # 检查是否需要跳过已存在的股票
        from clickhouse_handler import clickhouse_handler
        existing_symbols = clickhouse_handler.get_existing_symbols_for_date(trade_date)

        # 过滤掉已存在的股票
        if existing_symbols:
            ts_codes_to_fetch = [code for code in all_ts_codes if code not in existing_symbols]
            logger.info(f"日期 {trade_date} 已有 {len(existing_symbols)} 个股票的数据，需要获取 {len(ts_codes_to_fetch)} 个股票的数据")
        else:
            ts_codes_to_fetch = all_ts_codes
            logger.info(f"日期 {trade_date} 没有已存在的数据，需要获取 {len(ts_codes_to_fetch)} 个股票的数据")

        # 如果没有需要获取的股票，则返回空DataFrame
        if not ts_codes_to_fetch:
            logger.info(f"日期 {trade_date} 所有股票的数据已存在，无需获取")
            return pd.DataFrame()

        # 分批获取数据
        all_dfs = []
        for i in range(0, len(ts_codes_to_fetch), batch_size):
            batch_codes = ts_codes_to_fetch[i:i+batch_size]
            batch_codes_str = ','.join(batch_codes)

            logger.info(f"获取日线数据，日期: {trade_date}, 批次: {i//batch_size + 1}/{(len(ts_codes_to_fetch) + batch_size - 1)//batch_size}, 股票数量: {len(batch_codes)}")

            # 构建API参数
            params = {'trade_date': trade_date, 'ts_code': batch_codes_str}

            # 获取日线数据
            try:
                batch_df = self._call_tushare_api(
                    api_name='daily',
                    params=params,
                    fields='ts_code,trade_date,open,high,low,close,pre_close,change,pct_chg,vol,amount'
                )

                if not batch_df.empty:
                    all_dfs.append(batch_df)
                    logger.info(f"批次 {i//batch_size + 1} 获取成功，共 {len(batch_df)} 条记录")
                else:
                    logger.warning(f"批次 {i//batch_size + 1} 获取为空")

            except Exception as e:
                logger.error(f"批次 {i//batch_size + 1} 获取失败: {e}")
                # 继续获取下一批次，不中断整个过程

        # 合并所有批次的数据
        if all_dfs:
            df = pd.concat(all_dfs, ignore_index=True)

            # 获取涨跌停和ST数据
            df = self._enrich_daily_data(df)
            logger.info(f"获取日线数据成功，共 {len(df)} 条记录")
        else:
            logger.warning(f"获取日线数据为空，日期: {trade_date}")
            df = pd.DataFrame()

        return df

    def get_historical_daily_data(self, start_date: str, end_date: str, ts_code: str = None, skip_existing: bool = True) -> pd.DataFrame:
        """
        获取历史日线数据

        Args:
            start_date (str): 开始日期，格式为'YYYYMMDD'
            end_date (str): 结束日期，格式为'YYYYMMDD'
            ts_code (str, optional): 股票代码，如'000001.SZ'. 默认为None，表示获取所有股票.
            skip_existing (bool, optional): 是否跳过已存在的数据. 默认为True.

        Returns:
            pd.DataFrame: 历史日线数据
        """
        logger.info(f"获取历史日线数据，日期范围: {start_date} - {end_date}, 股票代码: {ts_code or '所有'}")

        # 如果需要跳过已存在的数据，则先检查ClickHouse中的数据日期范围
        if skip_existing:
            from clickhouse_handler import clickhouse_handler
            earliest_date, latest_date = clickhouse_handler.get_date_range()

            if earliest_date and latest_date:
                logger.info(f"ClickHouse中已有数据的日期范围: {earliest_date} - {latest_date}")

                # 检查请求的日期范围是否完全在已有数据范围内
                if start_date >= earliest_date and end_date <= latest_date:
                    logger.info(f"请求的日期范围 {start_date} - {end_date} 完全在已有数据范围内，无需获取历史数据")
                    return pd.DataFrame()

                # 检查请求的日期范围是否部分在已有数据范围内
                if (start_date <= latest_date and end_date > latest_date) or (start_date < earliest_date and end_date >= earliest_date):
                    # 分割日期范围，只获取不在已有范围内的部分
                    date_ranges = []

                    # 检查开始日期是否早于已有数据的最早日期
                    if start_date < earliest_date:
                        date_ranges.append((start_date, (datetime.datetime.strptime(earliest_date, '%Y%m%d') - datetime.timedelta(days=1)).strftime('%Y%m%d')))

                    # 检查结束日期是否晚于已有数据的最新日期
                    if end_date > latest_date:
                        date_ranges.append(((datetime.datetime.strptime(latest_date, '%Y%m%d') + datetime.timedelta(days=1)).strftime('%Y%m%d'), end_date))

                    logger.info(f"将获取不在已有数据范围内的日期: {date_ranges}")

                    # 获取每个日期范围的数据并合并
                    all_dfs = []
                    for range_start, range_end in date_ranges:
                        # 构建API参数
                        params = {
                            'start_date': range_start,
                            'end_date': range_end
                        }
                        if ts_code:
                            params['ts_code'] = ts_code

                        # 获取历史日线数据
                        range_df = self._call_tushare_api(
                            api_name='daily',
                            params=params,
                            fields='ts_code,trade_date,open,high,low,close,pre_close,change,pct_chg,vol,amount'
                        )

                        if not range_df.empty:
                            all_dfs.append(range_df)

                    # 合并所有数据
                    if all_dfs:
                        df = pd.concat(all_dfs, ignore_index=True)
                    else:
                        df = pd.DataFrame()

                    if not df.empty:
                        # 获取涨跌停和ST数据
                        df = self._enrich_historical_daily_data(df)
                        logger.info(f"获取历史日线数据成功，共 {len(df)} 条记录")
                    else:
                        logger.info("没有需要获取的新数据")

                    return df

        # 如果不需要跳过已存在的数据，或者没有已有数据，或者请求的日期范围完全不在已有数据范围内
        # 构建API参数
        params = {
            'start_date': start_date,
            'end_date': end_date
        }
        if ts_code:
            params['ts_code'] = ts_code

        # 获取历史日线数据
        df = self._call_tushare_api(
            api_name='daily',
            params=params,
            fields='ts_code,trade_date,open,high,low,close,pre_close,change,pct_chg,vol,amount'
        )

        if not df.empty:
            # 如果需要跳过已存在的数据，则按日期过滤
            if skip_existing and not ts_code:
                # 按日期分组处理数据
                filtered_dfs = []
                for date, group in df.groupby('trade_date'):
                    date_str = date.strftime('%Y%m%d') if isinstance(date, datetime.date) else date
                    # 获取该日期已存在的股票代码
                    existing_symbols = clickhouse_handler.get_existing_symbols_for_date(date_str)
                    if existing_symbols:
                        # 过滤掉已存在的股票数据
                        filtered_group = group[~group['ts_code'].isin(existing_symbols)]
                        if not filtered_group.empty:
                            filtered_dfs.append(filtered_group)
                            logger.info(f"日期 {date_str} 过滤后剩余 {len(filtered_group)} 条记录")
                    else:
                        # 该日期没有数据，全部保留
                        filtered_dfs.append(group)

                if filtered_dfs:
                    df = pd.concat(filtered_dfs, ignore_index=True)
                else:
                    df = pd.DataFrame()

            if not df.empty:
                # 获取涨跌停和ST数据
                df = self._enrich_historical_daily_data(df)
                logger.info(f"获取历史日线数据成功，共 {len(df)} 条记录")
            else:
                logger.info("过滤后没有需要获取的新数据")
        else:
            logger.warning(f"获取历史日线数据为空，日期范围: {start_date} - {end_date}")

        return df

    def _enrich_daily_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        丰富日线数据，添加涨跌停和ST信息

        Args:
            df (pd.DataFrame): 原始日线数据

        Returns:
            pd.DataFrame: 丰富后的日线数据
        """
        if df.empty:
            return df

        # 获取交易日期
        trade_date = df['trade_date'].iloc[0]

        try:
            # 获取涨跌停价格
            limit_df = self._call_tushare_api(
                api_name='limit_list',
                params={'trade_date': trade_date},
                fields='ts_code,trade_date,name,close,up_limit,down_limit'
            )

            # 获取ST股票信息
            namechange_df = self._call_tushare_api(
                api_name='namechange',
                params={'start_date': trade_date, 'end_date': trade_date},
                fields='ts_code,name,start_date,end_date,change_reason'
            )

            # 处理ST信息
            st_codes = []
            if not namechange_df.empty:
                # 筛选出ST股票
                st_df = namechange_df[namechange_df['change_reason'].str.contains('ST', na=False)]
                st_codes = st_df['ts_code'].tolist() if not st_df.empty else []

            # 添加涨跌停和ST信息
            if not limit_df.empty:
                # 合并涨跌停信息
                df = pd.merge(df, limit_df[['ts_code', 'up_limit', 'down_limit']],
                             on='ts_code', how='left')
            else:
                # 如果没有涨跌停数据，则添加空列
                df['up_limit'] = None
                df['down_limit'] = None

            # 添加ST标志
            df['is_st'] = df['ts_code'].isin(st_codes).astype(int)

            # 添加复权因子（这里简化处理，实际应该从adj_factor接口获取）
            df['adjust'] = 1.0

            # 重命名列名以符合ClickHouse表结构
            df = df.rename(columns={
                'ts_code': 'symbol',
                'trade_date': 'trade_date',
                'up_limit': 'limit_up',
                'down_limit': 'limit_down'
            })

            # 转换日期格式
            df['trade_date'] = pd.to_datetime(df['trade_date']).dt.date

            return df

        except Exception as e:
            logger.error(f"丰富日线数据失败: {e}")
            # 如果丰富数据失败，则返回原始数据
            return df

    def _enrich_historical_daily_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        丰富历史日线数据，添加涨跌停和ST信息

        Args:
            df (pd.DataFrame): 原始历史日线数据

        Returns:
            pd.DataFrame: 丰富后的历史日线数据
        """
        if df.empty:
            return df

        # 获取日期范围
        start_date = df['trade_date'].min()
        end_date = df['trade_date'].max()

        try:
            # 获取涨跌停价格
            limit_df = self._call_tushare_api(
                api_name='limit_list',
                params={'start_date': start_date, 'end_date': end_date},
                fields='ts_code,trade_date,name,close,up_limit,down_limit'
            )

            # 获取ST股票信息
            namechange_df = self._call_tushare_api(
                api_name='namechange',
                params={'start_date': start_date, 'end_date': end_date},
                fields='ts_code,name,start_date,end_date,change_reason'
            )

            # 处理ST信息
            st_info = {}
            if not namechange_df.empty:
                # 筛选出ST股票
                st_df = namechange_df[namechange_df['change_reason'].str.contains('ST', na=False)]

                # 构建ST信息字典
                for _, row in st_df.iterrows():
                    ts_code = row['ts_code']
                    start = row['start_date']
                    end = row['end_date'] if pd.notna(row['end_date']) else end_date

                    # 将日期范围内的所有交易日标记为ST
                    date_range = pd.date_range(start=start, end=end)
                    for date in date_range:
                        date_str = date.strftime('%Y%m%d')
                        key = f"{ts_code}_{date_str}"
                        st_info[key] = 1

            # 添加涨跌停信息
            if not limit_df.empty:
                # 将涨跌停信息转换为字典，方便查找
                limit_info = {}
                for _, row in limit_df.iterrows():
                    ts_code = row['ts_code']
                    trade_date = row['trade_date']
                    key = f"{ts_code}_{trade_date}"
                    limit_info[key] = {
                        'up_limit': row['up_limit'],
                        'down_limit': row['down_limit']
                    }

                # 添加涨跌停和ST信息
                df['limit_up'] = None
                df['limit_down'] = None
                df['is_st'] = 0

                for i, row in df.iterrows():
                    ts_code = row['ts_code']
                    trade_date = row['trade_date']
                    key = f"{ts_code}_{trade_date}"

                    # 添加涨跌停信息
                    if key in limit_info:
                        df.at[i, 'limit_up'] = limit_info[key]['up_limit']
                        df.at[i, 'limit_down'] = limit_info[key]['down_limit']

                    # 添加ST信息
                    if key in st_info:
                        df.at[i, 'is_st'] = st_info[key]
            else:
                # 如果没有涨跌停数据，则添加空列
                df['limit_up'] = None
                df['limit_down'] = None
                df['is_st'] = 0

            # 添加复权因子（这里简化处理，实际应该从adj_factor接口获取）
            df['adjust'] = 1.0

            # 重命名列名以符合ClickHouse表结构
            df = df.rename(columns={
                'ts_code': 'symbol',
                'trade_date': 'trade_date'
            })

            # 转换日期格式
            df['trade_date'] = pd.to_datetime(df['trade_date']).dt.date

            return df

        except Exception as e:
            logger.error(f"丰富历史日线数据失败: {e}")
            # 如果丰富数据失败，则返回原始数据
            return df

    def check_and_complete_date_data(self, trade_date: str = None, expected_count: int = None, batch_size: int = 100) -> bool:
        """
        检查并补充指定日期的不完整数据

        Args:
            trade_date (str, optional): 交易日期，格式为'YYYYMMDD'. 默认为None，表示最近一个交易日.
            expected_count (int, optional): 预期的股票数量. 默认为None，表示使用系统中最大的股票数量.
            batch_size (int, optional): 批量获取的股票数量. 默认为100.

        Returns:
            bool: 是否成功补充数据
        """
        # 如果未指定日期，则获取最近一个交易日
        if trade_date is None:
            if is_trade_day():
                trade_date = datetime.date.today().strftime('%Y%m%d')
            else:
                trade_date = get_last_trade_day()

        logger.info(f"检查并补充日期 {trade_date} 的数据")

        # 检查数据是否完整
        from clickhouse_handler import clickhouse_handler
        is_complete = clickhouse_handler.check_date_data_completeness(trade_date, expected_count)

        if is_complete:
            logger.info(f"日期 {trade_date} 的数据已完整，无需补充")
            return True

        # 获取已存在的股票代码
        existing_symbols = clickhouse_handler.get_existing_symbols_for_date(trade_date)

        # 获取所有股票列表
        stock_list = self.get_stock_list()

        if stock_list.empty:
            logger.warning("获取股票列表为空，无法补充数据")
            return False

        # 获取所有股票代码
        all_ts_codes = stock_list['ts_code'].tolist()

        # 过滤出需要获取的股票代码
        ts_codes_to_fetch = [code for code in all_ts_codes if code not in existing_symbols]

        if not ts_codes_to_fetch:
            logger.warning(f"日期 {trade_date} 已有所有股票的数据，但数据不完整，可能是由于股票数量变化")
            return False

        logger.info(f"日期 {trade_date} 需要补充 {len(ts_codes_to_fetch)} 个股票的数据")

        # 分批获取数据
        success = False
        for i in range(0, len(ts_codes_to_fetch), batch_size):
            batch_codes = ts_codes_to_fetch[i:i+batch_size]
            batch_codes_str = ','.join(batch_codes)

            logger.info(f"补充日期 {trade_date} 的数据，批次: {i//batch_size + 1}/{(len(ts_codes_to_fetch) + batch_size - 1)//batch_size}, 股票数量: {len(batch_codes)}")

            try:
                # 获取日线数据
                batch_df = self.get_daily_data(trade_date=trade_date, ts_code=batch_codes_str)

                if not batch_df.empty:
                    # 处理数据并存储到ClickHouse
                    from data_processor import data_processor
                    data_processor.process_and_store(batch_df)

                    logger.info(f"批次 {i//batch_size + 1} 补充成功，共 {len(batch_df)} 条记录")
                    success = True
                else:
                    logger.warning(f"批次 {i//batch_size + 1} 获取为空")

            except Exception as e:
                logger.error(f"批次 {i//batch_size + 1} 补充失败: {e}")
                # 继续获取下一批次，不中断整个过程

        # 再次检查数据是否完整
        is_complete = clickhouse_handler.check_date_data_completeness(trade_date, expected_count)

        if is_complete:
            logger.info(f"日期 {trade_date} 的数据已补充完整")
        else:
            logger.warning(f"日期 {trade_date} 的数据补充后仍不完整")

        return success

    def check_and_complete_date_range(self, start_date: str = None, end_date: str = None, expected_count: int = None, batch_size: int = 100) -> bool:
        """
        检查并补充指定日期范围内的不完整数据

        Args:
            start_date (str, optional): 开始日期，格式为'YYYYMMDD'. 默认为None，表示最早日期.
            end_date (str, optional): 结束日期，格式为'YYYYMMDD'. 默认为None，表示最新日期.
            expected_count (int, optional): 预期的股票数量. 默认为None，表示使用系统中最大的股票数量.
            batch_size (int, optional): 批量获取的股票数量. 默认为100.

        Returns:
            bool: 是否成功补充数据
        """
        logger.info(f"检查并补充日期范围 {start_date or '最早'} - {end_date or '最新'} 的数据")

        # 获取不完整的日期列表
        from clickhouse_handler import clickhouse_handler
        incomplete_dates = clickhouse_handler.get_incomplete_dates(start_date, end_date, expected_count)

        if not incomplete_dates:
            logger.info("所有日期的数据都已完整，无需补充")
            return True

        logger.info(f"共有 {len(incomplete_dates)} 个日期的数据不完整: {incomplete_dates}")

        # 补充每个不完整日期的数据
        success = True
        for date in incomplete_dates:
            if not self.check_and_complete_date_data(date, expected_count, batch_size):
                success = False

        return success


# 创建默认数据获取器实例
data_fetcher = DataFetcher()
