#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
日志模块

提供日志记录功能，包括控制台输出和文件输出。
"""

import os
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

# 导入配置
from config import Model
import yaml

# 加载配置
CONFIG_FILE = os.path.join(os.path.dirname(__file__), 'config.yaml')
with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
    config_dict = yaml.safe_load(f)
    config = Model(**config_dict)
    LOGGING_CONFIG = config.logging


def setup_logger(name=None):
    """
    设置日志记录器

    Args:
        name (str, optional): 日志记录器名称. 默认为None，使用root logger.

    Returns:
        logging.Logger: 日志记录器实例
    """
    # 获取日志配置
    log_level = getattr(logging, LOGGING_CONFIG.level)
    log_format = LOGGING_CONFIG.format
    log_file = LOGGING_CONFIG.file
    max_bytes = LOGGING_CONFIG.max_bytes
    backup_count = LOGGING_CONFIG.backup_count

    # 创建日志记录器
    logger = logging.getLogger(name)
    logger.setLevel(log_level)

    # 清除已有的处理器
    if logger.handlers:
        logger.handlers.clear()

    # 创建格式化器
    formatter = logging.Formatter(log_format)

    # 创建控制台处理器
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # 创建文件处理器
    log_file_path = Path(log_file)

    # 确保日志目录存在
    os.makedirs(log_file_path.parent, exist_ok=True)

    file_handler = RotatingFileHandler(
        log_file_path,
        maxBytes=max_bytes,
        backupCount=backup_count,
        encoding='utf-8'
    )
    file_handler.setLevel(log_level)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger


# 创建默认日志记录器
logger = setup_logger('day_bar_fetcher')
