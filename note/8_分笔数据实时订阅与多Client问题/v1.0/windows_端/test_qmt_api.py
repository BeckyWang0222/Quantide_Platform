#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
QMT API测试脚本
用于测试正确的API调用方式
"""

from xtquant import xtdata
import inspect

def test_qmt_api():
    """测试QMT API的正确调用方式"""
    
    print("=== QMT API 测试 ===")
    
    # 连接QMT
    try:
        connect_result = xtdata.connect()
        print(f"QMT连接成功: {type(connect_result)}")
    except Exception as e:
        print(f"QMT连接失败: {e}")
        return
    
    # 查看subscribe_quote函数的签名
    try:
        sig = inspect.signature(xtdata.subscribe_quote)
        print(f"subscribe_quote函数签名: {sig}")
        
        # 获取参数信息
        params = sig.parameters
        print("参数列表:")
        for name, param in params.items():
            print(f"  {name}: {param}")
            
    except Exception as e:
        print(f"获取函数签名失败: {e}")
    
    # 查看帮助文档
    try:
        help_text = help(xtdata.subscribe_quote)
        print(f"帮助文档: {help_text}")
    except Exception as e:
        print(f"获取帮助文档失败: {e}")
    
    # 测试简单的订阅调用
    test_stocks = ['000001.SZ', '000002.SZ']
    
    def test_callback(data):
        print(f"收到数据: {data}")
    
    print("\n=== 测试不同的API调用方式 ===")
    
    # 方法1: 位置参数
    try:
        print("测试方法1: 位置参数")
        seq = xtdata.subscribe_quote(test_stocks, '1m', test_callback)
        print(f"方法1成功，序列号: {seq}")
        return seq
    except Exception as e:
        print(f"方法1失败: {e}")
    
    # 方法2: 关键字参数 stock_code
    try:
        print("测试方法2: stock_code参数")
        seq = xtdata.subscribe_quote(stock_code=test_stocks, period='1m', callback=test_callback)
        print(f"方法2成功，序列号: {seq}")
        return seq
    except Exception as e:
        print(f"方法2失败: {e}")
    
    # 方法3: 关键字参数 codes
    try:
        print("测试方法3: codes参数")
        seq = xtdata.subscribe_quote(codes=test_stocks, period='1m', callback=test_callback)
        print(f"方法3成功，序列号: {seq}")
        return seq
    except Exception as e:
        print(f"方法3失败: {e}")
    
    # 方法4: 关键字参数 stock_list
    try:
        print("测试方法4: stock_list参数")
        seq = xtdata.subscribe_quote(stock_list=test_stocks, period='1m', callback=test_callback)
        print(f"方法4成功，序列号: {seq}")
        return seq
    except Exception as e:
        print(f"方法4失败: {e}")
    
    print("所有方法都失败了")
    return None

if __name__ == "__main__":
    test_qmt_api()
