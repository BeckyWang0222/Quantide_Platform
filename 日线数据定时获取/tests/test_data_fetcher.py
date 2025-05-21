#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
数据获取模块单元测试
"""

import unittest
import pandas as pd
from unittest.mock import patch, MagicMock

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data_fetcher import DataFetcher
from exceptions import TushareAPIError


class TestDataFetcher(unittest.TestCase):
    """数据获取模块单元测试类"""
    
    def setUp(self):
        """测试前的准备工作"""
        self.data_fetcher = DataFetcher()
    
    @patch('data_fetcher.requests.post')
    def test_call_tushare_api_success(self, mock_post):
        """测试调用Tushare API成功"""
        # 模拟API返回结果
        mock_response = MagicMock()
        mock_response.json.return_value = {
            'code': 0,
            'msg': '',
            'data': {
                'fields': ['ts_code', 'trade_date', 'open', 'high', 'low', 'close'],
                'items': [
                    ['000001.SZ', '20230101', 10.0, 11.0, 9.0, 10.5]
                ]
            }
        }
        mock_post.return_value = mock_response
        
        # 调用API
        result = self.data_fetcher._call_tushare_api('daily', {'ts_code': '000001.SZ'})
        
        # 验证结果
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 1)
        self.assertEqual(result.iloc[0]['ts_code'], '000001.SZ')
    
    @patch('data_fetcher.requests.post')
    def test_call_tushare_api_error(self, mock_post):
        """测试调用Tushare API失败"""
        # 模拟API返回错误
        mock_response = MagicMock()
        mock_response.json.return_value = {
            'code': 1,
            'msg': 'API调用失败'
        }
        mock_post.return_value = mock_response
        
        # 调用API，应该抛出异常
        with self.assertRaises(TushareAPIError):
            self.data_fetcher._call_tushare_api('daily', {'ts_code': '000001.SZ'})
    
    @patch('data_fetcher.DataFetcher._call_tushare_api')
    def test_get_stock_list(self, mock_call_api):
        """测试获取股票列表"""
        # 模拟API返回结果
        mock_call_api.return_value = pd.DataFrame({
            'ts_code': ['000001.SZ', '000002.SZ'],
            'symbol': ['000001', '000002'],
            'name': ['平安银行', '万科A'],
            'area': ['深圳', '深圳'],
            'industry': ['银行', '房地产'],
            'list_date': ['19910403', '19910129']
        })
        
        # 获取股票列表
        result = self.data_fetcher.get_stock_list(force_update=True)
        
        # 验证结果
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 2)
        self.assertEqual(result.iloc[0]['name'], '平安银行')
    
    @patch('data_fetcher.DataFetcher._call_tushare_api')
    def test_get_daily_data(self, mock_call_api):
        """测试获取日线数据"""
        # 模拟API返回结果
        mock_call_api.return_value = pd.DataFrame({
            'ts_code': ['000001.SZ', '000002.SZ'],
            'trade_date': ['20230101', '20230101'],
            'open': [10.0, 20.0],
            'high': [11.0, 21.0],
            'low': [9.0, 19.0],
            'close': [10.5, 20.5],
            'pre_close': [10.0, 20.0],
            'change': [0.5, 0.5],
            'pct_chg': [5.0, 2.5],
            'vol': [1000, 2000],
            'amount': [10000, 40000]
        })
        
        # 获取日线数据
        result = self.data_fetcher.get_daily_data(trade_date='20230101')
        
        # 验证结果
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 2)
        self.assertEqual(result.iloc[0]['close'], 10.5)
    
    @patch('data_fetcher.DataFetcher._call_tushare_api')
    def test_get_historical_daily_data(self, mock_call_api):
        """测试获取历史日线数据"""
        # 模拟API返回结果
        mock_call_api.return_value = pd.DataFrame({
            'ts_code': ['000001.SZ', '000001.SZ'],
            'trade_date': ['20230101', '20230102'],
            'open': [10.0, 10.5],
            'high': [11.0, 11.5],
            'low': [9.0, 9.5],
            'close': [10.5, 11.0],
            'pre_close': [10.0, 10.5],
            'change': [0.5, 0.5],
            'pct_chg': [5.0, 4.76],
            'vol': [1000, 1100],
            'amount': [10000, 11000]
        })
        
        # 获取历史日线数据
        result = self.data_fetcher.get_historical_daily_data('20230101', '20230102')
        
        # 验证结果
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 2)
        self.assertEqual(result.iloc[1]['close'], 11.0)


if __name__ == '__main__':
    unittest.main()
