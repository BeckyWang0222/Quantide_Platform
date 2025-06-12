#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
ClickHouse处理模块单元测试
"""

import unittest
import pandas as pd
from unittest.mock import patch, MagicMock

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from clickhouse_handler import ClickHouseHandler
from exceptions import ClickHouseConnectionError, ClickHouseOperationError


class TestClickHouseHandler(unittest.TestCase):
    """ClickHouse处理模块单元测试类"""
    
    def setUp(self):
        """测试前的准备工作"""
        # 使用测试配置
        self.test_config = {
            'host': 'localhost',
            'port': 9000,
            'user': 'default',
            'password': '',
            'database': 'test_db',
            'table': 'test_table'
        }
        
        # 创建ClickHouse处理器实例
        with patch('clickhouse_handler.Client') as mock_client_class:
            mock_client = MagicMock()
            mock_client_class.return_value = mock_client
            self.clickhouse_handler = ClickHouseHandler(self.test_config)
            self.mock_client = mock_client
    
    def test_connect_success(self):
        """测试连接ClickHouse成功"""
        # 重新创建ClickHouse处理器实例，模拟连接成功
        with patch('clickhouse_handler.Client') as mock_client_class:
            mock_client = MagicMock()
            mock_client_class.return_value = mock_client
            
            clickhouse_handler = ClickHouseHandler(self.test_config)
            
            # 验证Client被正确创建
            mock_client_class.assert_called_once_with(
                host=self.test_config['host'],
                port=self.test_config['port'],
                user=self.test_config['user'],
                password=self.test_config['password'],
                database=self.test_config['database']
            )
            
            # 验证execute方法被调用
            mock_client.execute.assert_called()
    
    def test_connect_failure(self):
        """测试连接ClickHouse失败"""
        # 重新创建ClickHouse处理器实例，模拟连接失败
        with patch('clickhouse_handler.Client') as mock_client_class:
            mock_client = MagicMock()
            mock_client.execute.side_effect = Exception("Connection failed")
            mock_client_class.return_value = mock_client
            
            # 应该抛出异常
            with self.assertRaises(ClickHouseConnectionError):
                clickhouse_handler = ClickHouseHandler(self.test_config)
    
    def test_ensure_database_exists_already_exists(self):
        """测试确保数据库存在（已存在）"""
        # 模拟数据库已存在
        self.mock_client.execute.return_value = [['test_db']]
        
        # 调用方法
        self.clickhouse_handler._ensure_database_exists()
        
        # 验证execute方法被调用
        self.mock_client.execute.assert_called()
    
    def test_ensure_database_exists_create_new(self):
        """测试确保数据库存在（需要创建）"""
        # 模拟数据库不存在
        self.mock_client.execute.side_effect = [[], None]
        
        # 调用方法
        self.clickhouse_handler._ensure_database_exists()
        
        # 验证execute方法被调用两次
        self.assertEqual(self.mock_client.execute.call_count, 2)
    
    def test_ensure_table_exists(self):
        """测试确保表存在"""
        # 调用方法
        self.clickhouse_handler._ensure_table_exists()
        
        # 验证execute方法被调用
        self.mock_client.execute.assert_called()
    
    def test_check_connection_success(self):
        """测试检查ClickHouse连接成功"""
        # 模拟execute方法返回成功
        self.mock_client.execute.return_value = [[1]]
        
        # 检查连接
        result = self.clickhouse_handler.check_connection()
        
        # 验证结果
        self.assertTrue(result)
        self.mock_client.execute.assert_called_with('SELECT 1')
    
    def test_check_connection_failure(self):
        """测试检查ClickHouse连接失败"""
        # 模拟execute方法抛出异常
        self.mock_client.execute.side_effect = Exception("Connection failed")
        
        # 检查连接
        result = self.clickhouse_handler.check_connection()
        
        # 验证结果
        self.assertFalse(result)
    
    def test_insert_data_dict(self):
        """测试插入字典数据"""
        # 准备测试数据
        test_data = {'symbol': '000001', 'trade_date': '2023-01-01', 'close': 10.5}
        
        # 插入数据
        self.clickhouse_handler.insert_data(test_data)
        
        # 验证execute方法被调用
        self.mock_client.execute.assert_called()
    
    def test_insert_data_dataframe(self):
        """测试插入DataFrame数据"""
        # 准备测试数据
        test_df = pd.DataFrame({
            'symbol': ['000001', '000002'],
            'trade_date': ['2023-01-01', '2023-01-01'],
            'close': [10.5, 20.5]
        })
        
        # 插入数据
        self.clickhouse_handler.insert_data(test_df)
        
        # 验证execute方法被调用
        self.mock_client.execute.assert_called()
    
    def test_close(self):
        """测试关闭ClickHouse连接"""
        # 关闭连接
        self.clickhouse_handler.close()
        
        # 验证client被设置为None
        self.assertIsNone(self.clickhouse_handler.client)


if __name__ == '__main__':
    unittest.main()
