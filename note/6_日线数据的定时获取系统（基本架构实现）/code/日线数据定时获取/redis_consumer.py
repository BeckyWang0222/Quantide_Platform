#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Redis消费者模块

从Redis队列中读取数据并存入ClickHouse。
"""

import json
import time
import pandas as pd
from typing import Dict, List, Any

from logger import logger
from redis_handler import redis_handler
from clickhouse_handler import clickhouse_handler
from exceptions import RedisOperationError, ClickHouseOperationError


def consume_and_store(batch_size: int = 100, sleep_time: int = 1):
    """
    从Redis队列中消费数据并存入ClickHouse
    
    Args:
        batch_size (int, optional): 批处理大小. 默认为100.
        sleep_time (int, optional): 队列为空时的休眠时间(秒). 默认为1.
    """
    logger.info("开始从Redis队列消费数据并存入ClickHouse")
    
    while True:
        try:
            # 从Redis队列中获取数据
            data_list = []
            for _ in range(batch_size):
                # 使用LPOP命令从队列左侧弹出一个元素
                data = redis_handler.client.lpop(redis_handler.queue_name)
                if data is None:
                    break
                
                # 解析JSON数据
                if isinstance(data, bytes):
                    data = data.decode('utf-8')
                
                data_dict = json.loads(data)
                data_list.append(data_dict)
            
            # 如果没有数据，则休眠一段时间
            if not data_list:
                time.sleep(sleep_time)
                continue
            
            # 将数据转换为DataFrame
            df = pd.DataFrame(data_list)
            
            # 将数据存入ClickHouse
            clickhouse_handler.insert_data(df)
            
            logger.info(f"已从Redis队列消费并存入ClickHouse {len(data_list)} 条数据")
        
        except Exception as e:
            logger.error(f"消费数据并存入ClickHouse失败: {e}")
            time.sleep(sleep_time)


if __name__ == "__main__":
    consume_and_store()