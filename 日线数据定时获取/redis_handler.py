#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Redis处理模块

提供Redis连接和操作功能。
"""

import json
import redis
from typing import Dict, List, Any, Union

from config_loader import REDIS_CONFIG
from logger import logger
from exceptions import RedisConnectionError, RedisOperationError
from utils import retry


class RedisHandler:
    """Redis处理类，提供Redis连接和操作功能"""

    def __init__(self, config: Dict = None):
        """
        初始化Redis处理类

        Args:
            config (Dict, optional): Redis配置. 默认为None，使用配置文件中的值.
        """
        self.config = config or REDIS_CONFIG
        self.client = None
        self.queue_name = self.config.queue
        self.connect()

    @retry(exceptions=(redis.RedisError,))
    def connect(self) -> None:
        """
        连接Redis服务器

        Raises:
            RedisConnectionError: 连接Redis服务器失败时抛出
        """
        try:
            self.client = redis.Redis(
                host=self.config.host,
                port=self.config.port,
                password=self.config.password,
                db=self.config.db,
                decode_responses=self.config.decode_responses
            )
            # 测试连接
            self.client.ping()
            logger.info("Redis连接成功")
        except redis.RedisError as e:
            logger.error(f"Redis连接失败: {e}")
            raise RedisConnectionError(f"Redis连接失败: {e}")

    def check_connection(self) -> bool:
        """
        检查Redis连接状态

        Returns:
            bool: 连接状态，True表示连接正常，False表示连接异常
        """
        try:
            if self.client is None:
                self.connect()
                return True

            self.client.ping()
            return True
        except Exception as e:
            logger.error(f"Redis连接检查失败: {e}")
            return False

    @retry(exceptions=(redis.RedisError,))
    def publish_data(self, data: Union[Dict, List, str]) -> None:
        """
        发布数据到Redis队列

        Args:
            data (Union[Dict, List, str]): 要发布的数据，可以是字典、列表或字符串

        Raises:
            RedisOperationError: 发布数据失败时抛出
        """
        try:
            if not self.check_connection():
                self.connect()

            # 如果数据不是字符串，则转换为JSON字符串
            if not isinstance(data, str):
                data = json.dumps(data, ensure_ascii=False)

            # 发布数据到队列
            self.client.rpush(self.queue_name, data)
            logger.debug(f"数据已发布到Redis队列 {self.queue_name}")
        except Exception as e:
            logger.error(f"发布数据到Redis队列失败: {e}")
            raise RedisOperationError(f"发布数据到Redis队列失败: {e}")

    @retry(exceptions=(redis.RedisError,))
    def publish_batch_data(self, data_list: List[Union[Dict, List, str]]) -> None:
        """
        批量发布数据到Redis队列

        Args:
            data_list (List[Union[Dict, List, str]]): 要发布的数据列表

        Raises:
            RedisOperationError: 批量发布数据失败时抛出
        """
        try:
            if not self.check_connection():
                self.connect()

            # 将数据转换为JSON字符串
            json_data_list = []
            for data in data_list:
                if not isinstance(data, str):
                    json_data_list.append(json.dumps(data, ensure_ascii=False))
                else:
                    json_data_list.append(data)

            # 使用管道批量发布数据
            with self.client.pipeline() as pipe:
                for data in json_data_list:
                    pipe.rpush(self.queue_name, data)
                pipe.execute()

            logger.info(f"已批量发布 {len(data_list)} 条数据到Redis队列 {self.queue_name}")
        except Exception as e:
            logger.error(f"批量发布数据到Redis队列失败: {e}")
            raise RedisOperationError(f"批量发布数据到Redis队列失败: {e}")

    def close(self) -> None:
        """关闭Redis连接"""
        if self.client:
            self.client.close()
            self.client = None
            logger.info("Redis连接已关闭")


# 创建默认Redis处理器实例
redis_handler = RedisHandler()
