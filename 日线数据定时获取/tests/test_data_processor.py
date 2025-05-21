#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
数据处理模块单元测试
"""

import unittest
import pandas as pd
from unittest.mock import patch, MagicMock

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data_processor import DataProcessor
from exceptions import DataProcessError


class TestDataProcessor(unittest.TestCase):
    """数据处理模块单元测试类"""
    
    def setUp(self):
        """测试前的准备工作"""
        self.data_processor = DataProcessor()
        
        # 准备测试数据
        self.test_df = pd.DataFrame({
            'symbol': ['000001.SZ', '000002.SZ'],
            'trade_date': ['2023-01-01', '2023-01-01'],
            'open': [10.0, 20.0],
            'high': [11.0, 21.0],
            'low': [9.0, 19.0],
            'close': [10.5, 20.5],
            'pre_close': [10.0, 20.0],
            'change': [0.5, 0.5],
            'pct_chg': [5.0, 2.5],
            'vol': [1000, 2000],
            'amount': [10000, 40000],
            'adjust': [1.0, 1.0],
            'limit_up': [11.0, 22.0],
            'limit_down': [9.0, 18.0],
            'is_st': [0, 0]
        })
    
    def test_ensure_data_types(self):
        """测试确保数据类型正确"""
        # 准备测试数据，包含不同类型的数据
        test_df = pd.DataFrame({
            'symbol': ['000001', '000002'],
            'trade_date': ['2023-01-01', '2023-01-02'],
            'open': ['10.0', '20.0'],  # 字符串类型
            'high': [11.0, 21.0],
            'low': [9.0, 19.0],
            'close': [10.5, 20.5],
            'is_st': ['0', '0']  # 字符串类型
        })
        
        # 处理数据类型
        result = self.data_processor._ensure_data_types(test_df)
        
        # 验证结果
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(result['symbol'].dtype, 'object')  # 字符串类型
        self.assertEqual(result['open'].dtype, 'float64')  # 浮点数类型
        self.assertEqual(result['is_st'].dtype, 'int64')  # 整数类型
    
    @patch('data_processor.fastjson_dumps')
    def test_convert_to_json(self, mock_fastjson_dumps):
        """测试转换为JSON格式"""
        # 模拟fastjson_dumps返回JSON字符串
        mock_fastjson_dumps.side_effect = lambda x: f"JSON:{x}"
        
        # 转换为JSON
        result = self.data_processor._convert_to_json(self.test_df)
        
        # 验证结果
        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 2)
        self.assertEqual(mock_fastjson_dumps.call_count, 2)
    
    @patch('data_processor.redis_handler')
    def test_process_and_publish_success(self, mock_redis_handler):
        """测试处理数据并发布到Redis成功"""
        # 处理数据并发布
        result = self.data_processor.process_and_publish(self.test_df)
        
        # 验证结果
        self.assertTrue(result)
        mock_redis_handler.publish_batch_data.assert_called_once()
    
    @patch('data_processor.redis_handler')
    def test_process_and_publish_empty_data(self, mock_redis_handler):
        """测试处理空数据并发布到Redis"""
        # 准备空数据
        empty_df = pd.DataFrame()
        
        # 处理数据并发布
        result = self.data_processor.process_and_publish(empty_df)
        
        # 验证结果
        self.assertFalse(result)
        mock_redis_handler.publish_batch_data.assert_not_called()
    
    @patch('data_processor.redis_handler')
    def test_process_and_publish_error(self, mock_redis_handler):
        """测试处理数据并发布到Redis失败"""
        # 模拟发布数据失败
        mock_redis_handler.publish_batch_data.side_effect = Exception("Publish failed")
        
        # 处理数据并发布，应该抛出异常
        with self.assertRaises(DataProcessError):
            self.data_processor.process_and_publish(self.test_df)
    
    @patch('data_processor.clickhouse_handler')
    def test_process_and_store_success(self, mock_clickhouse_handler):
        """测试处理数据并存储到ClickHouse成功"""
        # 处理数据并存储
        result = self.data_processor.process_and_store(self.test_df)
        
        # 验证结果
        self.assertTrue(result)
        mock_clickhouse_handler.insert_data.assert_called_once()
    
    @patch('data_processor.clickhouse_handler')
    def test_process_and_store_empty_data(self, mock_clickhouse_handler):
        """测试处理空数据并存储到ClickHouse"""
        # 准备空数据
        empty_df = pd.DataFrame()
        
        # 处理数据并存储
        result = self.data_processor.process_and_store(empty_df)
        
        # 验证结果
        self.assertFalse(result)
        mock_clickhouse_handler.insert_data.assert_not_called()
    
    @patch('data_processor.clickhouse_handler')
    def test_process_and_store_error(self, mock_clickhouse_handler):
        """测试处理数据并存储到ClickHouse失败"""
        # 模拟存储数据失败
        mock_clickhouse_handler.insert_data.side_effect = Exception("Insert failed")
        
        # 处理数据并存储，应该抛出异常
        with self.assertRaises(DataProcessError):
            self.data_processor.process_and_store(self.test_df)


if __name__ == '__main__':
    unittest.main()
