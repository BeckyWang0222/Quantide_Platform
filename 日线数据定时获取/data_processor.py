#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
数据处理模块

处理从Tushare获取的日线数据，并发布到Redis。
"""

import pandas as pd
import json
from typing import Dict, List, Any, Union, Optional
from fast_json import dumps as fastjson_dumps

from logger import logger
from exceptions import DataProcessError
from redis_handler import redis_handler
from clickhouse_handler import clickhouse_handler


class DataProcessor:
    """数据处理类，处理日线数据并发布到Redis"""
    
    def __init__(self):
        """初始化数据处理类"""
        pass
    
    def process_and_publish(self, data: pd.DataFrame, batch_size: int = 1000) -> bool:
        """
        处理数据并发布到Redis
        
        Args:
            data (pd.DataFrame): 要处理的数据
            batch_size (int, optional): 批处理大小. 默认为1000.
        
        Returns:
            bool: 处理结果，True表示成功，False表示失败
        """
        if data.empty:
            logger.warning("没有数据需要处理")
            return False
        
        try:
            logger.info(f"开始处理 {len(data)} 条数据")
            
            # 确保数据类型正确
            data = self._ensure_data_types(data)
            
            # 按批次处理数据
            total_records = len(data)
            for i in range(0, total_records, batch_size):
                batch_data = data.iloc[i:i+batch_size]
                
                # 转换为JSON格式
                json_data = self._convert_to_json(batch_data)
                
                # 发布到Redis
                redis_handler.publish_batch_data(json_data)
                
                logger.info(f"已处理并发布 {i+len(batch_data)}/{total_records} 条数据")
            
            logger.info(f"数据处理完成，共 {total_records} 条记录")
            return True
        
        except Exception as e:
            logger.error(f"数据处理失败: {e}")
            raise DataProcessError(f"数据处理失败: {e}")
    
    def process_and_store(self, data: pd.DataFrame, batch_size: int = 1000) -> bool:
        """
        处理数据并直接存储到ClickHouse
        
        Args:
            data (pd.DataFrame): 要处理的数据
            batch_size (int, optional): 批处理大小. 默认为1000.
        
        Returns:
            bool: 处理结果，True表示成功，False表示失败
        """
        if data.empty:
            logger.warning("没有数据需要处理")
            return False
        
        try:
            logger.info(f"开始处理并存储 {len(data)} 条数据")
            
            # 确保数据类型正确
            data = self._ensure_data_types(data)
            
            # 按批次处理数据
            total_records = len(data)
            for i in range(0, total_records, batch_size):
                batch_data = data.iloc[i:i+batch_size]
                
                # 直接存储到ClickHouse
                clickhouse_handler.insert_data(batch_data)
                
                logger.info(f"已处理并存储 {i+len(batch_data)}/{total_records} 条数据")
            
            logger.info(f"数据处理和存储完成，共 {total_records} 条记录")
            return True
        
        except Exception as e:
            logger.error(f"数据处理和存储失败: {e}")
            raise DataProcessError(f"数据处理和存储失败: {e}")
    
    def _ensure_data_types(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        确保数据类型正确
        
        Args:
            data (pd.DataFrame): 原始数据
        
        Returns:
            pd.DataFrame: 处理后的数据
        """
        # 定义列的数据类型
        type_dict = {
            'symbol': str,
            'trade_date': 'datetime64[ns]',
            'open': float,
            'high': float,
            'low': float,
            'close': float,
            'pre_close': float,
            'change': float,
            'pct_chg': float,
            'vol': float,
            'amount': float,
            'adjust': float,
            'limit_up': float,
            'limit_down': float,
            'is_st': int
        }
        
        # 转换数据类型
        for col, dtype in type_dict.items():
            if col in data.columns:
                try:
                    if col == 'trade_date' and data[col].dtype != 'datetime64[ns]':
                        data[col] = pd.to_datetime(data[col])
                    else:
                        data[col] = data[col].astype(dtype)
                except Exception as e:
                    logger.warning(f"转换列 {col} 的数据类型失败: {e}")
        
        return data
    
    def _convert_to_json(self, data: pd.DataFrame) -> List[str]:
        """
        将DataFrame转换为JSON格式
        
        Args:
            data (pd.DataFrame): 要转换的数据
        
        Returns:
            List[str]: JSON字符串列表
        """
        # 将日期转换为字符串
        if 'trade_date' in data.columns and data['trade_date'].dtype == 'datetime64[ns]':
            data = data.copy()
            data['trade_date'] = data['trade_date'].dt.strftime('%Y-%m-%d')
        
        # 转换为字典列表
        records = data.to_dict('records')
        
        # 使用fastjson转换为JSON字符串
        json_data = [fastjson_dumps(record) for record in records]
        
        return json_data


# 创建默认数据处理器实例
data_processor = DataProcessor()
