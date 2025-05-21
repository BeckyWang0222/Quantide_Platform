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

from config import LOGGING_CONFIG


def setup_logger(name=None):
    """
    设置日志记录器
    
    Args:
        name (str, optional): 日志记录器名称. 默认为None，使用root logger.
    
    Returns:
        logging.Logger: 日志记录器实例
    """
    # 获取日志配置
    log_level = getattr(logging, LOGGING_CONFIG.get('level', 'INFO'))
    log_format = LOGGING_CONFIG.get('format', '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    log_file = LOGGING_CONFIG.get('file', 'logs/app.log')
    max_bytes = LOGGING_CONFIG.get('max_bytes', 10485760)  # 默认10MB
    backup_count = LOGGING_CONFIG.get('backup_count', 5)
    
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
