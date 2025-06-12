#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Redis处理模块单元测试
"""

import unittest
import json
from unittest.mock import patch, MagicMock

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from redis_handler import RedisHandler
from exceptions import RedisConnectionError, RedisOperationError


class TestRedisHandler(unittest.TestCase):
    """Redis处理模块单元测试类"""
    
    def setUp(self):
        """测试前的准备工作"""
        # 使用测试配置
        self.test_config = {
            'host': 'localhost',
            'port': 6379,
            'password': 'test_password',
            'db': 0,
            'decode_responses': True,
            'queue': 'test_queue'
        }
        
        # 创建Redis处理器实例
        with patch('redis_handler.redis.Redis') as mock_redis:
            mock_client = MagicMock()
            mock_redis.return_value = mock_client
            self.redis_handler = RedisHandler(self.test_config)
            self.mock_client = mock_client
    
    def test_connect_success(self):
        """测试连接Redis成功"""
        # 重新创建Redis处理器实例，模拟连接成功
        with patch('redis_handler.redis.Redis') as mock_redis:
            mock_client = MagicMock()
            mock_redis.return_value = mock_client
            
            redis_handler = RedisHandler(self.test_config)
            
            # 验证Redis客户端被正确创建
            mock_redis.assert_called_once_with(
                host=self.test_config['host'],
                port=self.test_config['port'],
                password=self.test_config['password'],
                db=self.test_config['db'],
                decode_responses=self.test_config['decode_responses']
            )
            
            # 验证ping方法被调用
            mock_client.ping.assert_called_once()
    
    def test_connect_failure(self):
        """测试连接Redis失败"""
        # 重新创建Redis处理器实例，模拟连接失败
        with patch('redis_handler.redis.Redis') as mock_redis:
            mock_client = MagicMock()
            mock_client.ping.side_effect = Exception("Connection failed")
            mock_redis.return_value = mock_client
            
            # 应该抛出异常
            with self.assertRaises(RedisConnectionError):
                redis_handler = RedisHandler(self.test_config)
    
    def test_check_connection_success(self):
        """测试检查Redis连接成功"""
        # 模拟ping方法返回成功
        self.mock_client.ping.return_value = True
        
        # 检查连接
        result = self.redis_handler.check_connection()
        
        # 验证结果
        self.assertTrue(result)
        self.mock_client.ping.assert_called_once()
    
    def test_check_connection_failure(self):
        """测试检查Redis连接失败"""
        # 模拟ping方法抛出异常
        self.mock_client.ping.side_effect = Exception("Connection failed")
        
        # 检查连接
        result = self.redis_handler.check_connection()
        
        # 验证结果
        self.assertFalse(result)
    
    def test_publish_data_dict(self):
        """测试发布字典数据"""
        # 准备测试数据
        test_data = {'symbol': '000001', 'close': 10.5}
        
        # 发布数据
        self.redis_handler.publish_data(test_data)
        
        # 验证rpush方法被调用
        self.mock_client.rpush.assert_called_once()
        args, kwargs = self.mock_client.rpush.call_args
        self.assertEqual(args[0], self.test_config['queue'])
        self.assertEqual(json.loads(args[1]), test_data)
    
    def test_publish_data_string(self):
        """测试发布字符串数据"""
        # 准备测试数据
        test_data = '{"symbol": "000001", "close": 10.5}'
        
        # 发布数据
        self.redis_handler.publish_data(test_data)
        
        # 验证rpush方法被调用
        self.mock_client.rpush.assert_called_once()
        args, kwargs = self.mock_client.rpush.call_args
        self.assertEqual(args[0], self.test_config['queue'])
        self.assertEqual(args[1], test_data)
    
    def test_publish_batch_data(self):
        """测试批量发布数据"""
        # 准备测试数据
        test_data_list = [
            {'symbol': '000001', 'close': 10.5},
            {'symbol': '000002', 'close': 20.5}
        ]
        
        # 模拟管道
        mock_pipeline = MagicMock()
        self.mock_client.pipeline.return_value.__enter__.return_value = mock_pipeline
        
        # 批量发布数据
        self.redis_handler.publish_batch_data(test_data_list)
        
        # 验证管道被调用
        self.mock_client.pipeline.assert_called_once()
        
        # 验证rpush方法被调用两次
        self.assertEqual(mock_pipeline.rpush.call_count, 2)
        
        # 验证execute方法被调用
        mock_pipeline.execute.assert_called_once()
    
    def test_close(self):
        """测试关闭Redis连接"""
        # 关闭连接
        self.redis_handler.close()
        
        # 验证close方法被调用
        self.mock_client.close.assert_called_once()
        
        # 验证client被设置为None
        self.assertIsNone(self.redis_handler.client)


if __name__ == '__main__':
    unittest.main()
