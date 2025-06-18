#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
配置文件模块

包含系统所需的所有配置参数，包括Redis、ClickHouse、Tushare以及定时器和调度器的配置。
"""

import os
import yaml
import logging
from pathlib import Path

# 项目根目录
BASE_DIR = Path(__file__).resolve().parent

# 配置文件路径
CONFIG_FILE = os.path.join(BASE_DIR, 'config.yaml')

# 默认配置
DEFAULT_CONFIG = {
    # Redis配置
    'redis': {
        'host': 'localhost',
        'port': 6379,
        'password': '',
        'db': 0,
        'decode_responses': True,
        'queue': 'day_bar_queue'
    },
    
    # ClickHouse配置
    'clickhouse': {
        'host': 'localhost',
        'port': 9000,
        'database': 'RealTime_DailyLine_DB',
        'table': 'day_bar',
        'user': 'default',
        'password': ''
    },
    
    # Tushare配置
    'tushare': {
        'token': 'bd02f68c6c42a536dd9b005228af5454e175a5812380585a7d2b1ab9',
        'api_url': 'https://tushare.citydata.club'
    },
    
    # 定时器配置
    'scheduler': {
        # 当日数据获取时间（每个交易日收盘后）
        'daily_data_time': '15:30:00',
        # 历史数据获取时间（每天凌晨）
        'historical_data_time': '02:00:00',
        # 股票列表更新时间（每月第一天）
        'stock_list_update_time': '00:10:00',
        # 交易日历更新时间（每月第一天）
        'trade_cal_update_time': '00:20:00',
        # Redis连接检查时间（每周一）
        'redis_check_time': '07:00:00',
        # ClickHouse连接检查时间（每周一）
        'clickhouse_check_time': '07:10:00',
        # 重试次数
        'max_retries': 3,
        # 重试间隔（秒）
        'retry_interval': 60,
        # 作业存储
        'job_stores': {
            'default': {
                'type': 'memory'
            }
        },
        # 执行器
        'executors': {
            'default': {
                'type': 'threadpool',
                'max_workers': 10
            }
        },
        # 作业默认配置
        'job_defaults': {
            'coalesce': False,
            'max_instances': 3
        }
    },
    
    # 日志配置
    'logging': {
        'level': 'INFO',
        'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        'file': 'logs/app.log',
        'max_bytes': 10485760,  # 10MB
        'backup_count': 5
    },
    
    # 监控报警配置
    'monitor': {
        'enabled': True,
        'email': {
            'enabled': True,
            'smtp_server': 'smtp.example.com',
            'smtp_port': 587,
            'sender': 'alert@example.com',
            'password': 'password',
            'recipients': ['admin@example.com']
        }
    }
}


def load_config():
    """
    加载配置文件，如果配置文件不存在则创建默认配置文件
    
    Returns:
        dict: 配置字典
    """
    if not os.path.exists(CONFIG_FILE):
        # 创建配置文件目录（如果不存在）
        os.makedirs(os.path.dirname(CONFIG_FILE), exist_ok=True)
        
        # 写入默认配置
        with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
            yaml.dump(DEFAULT_CONFIG, f, default_flow_style=False, allow_unicode=True)
        
        return DEFAULT_CONFIG
    
    # 读取配置文件
    try:
        with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        return config
    except Exception as e:
        logging.error(f"加载配置文件失败: {e}")
        return DEFAULT_CONFIG


# 加载配置
config = load_config()

# 导出配置变量，方便其他模块使用
REDIS_CONFIG = config['redis']
CLICKHOUSE_CONFIG = config['clickhouse']
TUSHARE_CONFIG = config['tushare']
SCHEDULER_CONFIG = config['scheduler']
LOGGING_CONFIG = config['logging']
MONITOR_CONFIG = config['monitor']
