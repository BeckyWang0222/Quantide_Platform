#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
配置加载模块

加载和解析配置文件。
"""

import os
import yaml
from typing import Dict, Any

from config import Model

# 配置文件路径
CONFIG_FILE = os.path.join(os.path.dirname(__file__), 'config.yaml')

def load_config() -> Model:
    """
    加载配置文件
    
    Returns:
        Model: 配置模型对象
    """
    try:
        with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
            config_dict = yaml.safe_load(f)
        
        # 使用Pydantic模型解析配置
        config = Model(**config_dict)
        return config
    
    except Exception as e:
        raise Exception(f"加载配置文件失败: {e}")

# 加载配置
config = load_config()

# 导出各模块配置
REDIS_CONFIG = config.redis
CLICKHOUSE_CONFIG = config.clickhouse
TUSHARE_CONFIG = config.tushare
SCHEDULER_CONFIG = config.scheduler
LOGGING_CONFIG = config.logging
MONITOR_CONFIG = config.monitor
