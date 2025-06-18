# -*- coding: utf-8 -*-
"""
系统配置文件
"""

# Redis配置
REDIS_CONFIG = {
    'host': ,
    'port': ,
    'db': 0,
    'decode_responses': True
}

# ClickHouse配置
CLICKHOUSE_CONFIG = {
    'host': ,
    'port': ,
    'user': 'default',
    'password': ,
    'database': 'v1'
}

# Redis队列名称
REDIS_QUEUES = {
    'whole_quote_data': 'whole_quote_data',
    'bar_data_1min': 'bar_data_1min',
    'bar_data_5min': 'bar_data_5min',
    'bar_data_15min': 'bar_data_15min',
    'bar_data_30min': 'bar_data_30min'
}

# ClickHouse表名
CLICKHOUSE_TABLES = {
    'data_bar_for_1min': 'data_bar_for_1min',
    'data_bar_for_5min': 'data_bar_for_5min',
    'data_bar_for_15min': 'data_bar_for_15min',
    'data_bar_for_30min': 'data_bar_for_30min'
}

# 分钟线周期
BAR_PERIODS = [1, 5, 15, 30]

# 交易时间配置
TRADING_HOURS = {
    'morning_start': '09:30:00',
    'morning_end': '11:30:00',
    'afternoon_start': '13:00:00',
    'afternoon_end': '15:00:00'
}

# 数据清理时间
DATA_CLEANUP_TIME = '02:00:00'

# Web服务端口配置
WEB_PORTS = {
    'windows': 8001,
    'mac': 8002,
    'client': 8003
}
