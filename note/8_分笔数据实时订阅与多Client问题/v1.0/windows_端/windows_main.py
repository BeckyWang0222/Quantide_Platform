#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Windows端主程序

启动QMT分钟线数据订阅服务
"""

import yaml
import logging
import logging.handlers
import os
import sys
import signal
from qmt_subscriber import QMTMinuteSubscriber


def setup_logging(config):
    """
    配置日志系统
    
    Args:
        config (dict): 配置字典
    """
    log_config = config.get('logging', {})
    
    # 创建日志目录
    log_dir = 'logs'
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    log_file = os.path.join(log_dir, log_config.get('file', 'qmt_subscriber.log'))
    
    # 配置日志格式
    formatter = logging.Formatter(
        log_config.get('format', '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    )
    
    # 文件处理器（支持日志轮转）
    file_handler = logging.handlers.RotatingFileHandler(
        log_file,
        maxBytes=log_config.get('max_bytes', 10*1024*1024),  # 10MB
        backupCount=log_config.get('backup_count', 5)
    )
    file_handler.setFormatter(formatter)
    
    # 控制台处理器
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    
    # 配置根日志器
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, log_config.get('level', 'INFO').upper()))
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)
    
    return root_logger


def load_config():
    """
    加载配置文件
    
    Returns:
        dict: 配置字典
    """
    config_file = 'windows_config.yaml'
    
    if not os.path.exists(config_file):
        print(f"错误: 配置文件 {config_file} 不存在")
        sys.exit(1)
    
    try:
        with open(config_file, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        return config
    except Exception as e:
        print(f"错误: 加载配置文件失败 - {e}")
        sys.exit(1)


def signal_handler(signum, frame):
    """信号处理器"""
    logger = logging.getLogger(__name__)
    logger.info(f"接收到信号 {signum}，准备退出...")
    sys.exit(0)


def main():
    """主函数"""
    # 注册信号处理器
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        # 加载配置
        config = load_config()
        
        # 配置日志
        logger = setup_logging(config)
        logger.info("=" * 50)
        logger.info("启动QMT分钟线数据订阅服务")
        logger.info("=" * 50)
        
        # 打印配置信息
        logger.info(f"Redis服务器: {config.get('redis', {}).get('host')}:{config.get('redis', {}).get('port')}")
        logger.info(f"股票数量: {len(config.get('qmt', {}).get('stock_list', []))}")
        logger.info(f"日志级别: {config.get('logging', {}).get('level')}")
        
        # 创建订阅器
        subscriber = QMTMinuteSubscriber(config)
        
        # 启动订阅
        logger.info("正在启动QMT数据订阅...")
        subscriber.start_subscription()
        
    except KeyboardInterrupt:
        logger.info("用户中断，停止服务")
    except Exception as e:
        logger.error(f"服务异常: {e}")
        logger.error("详细错误信息:", exc_info=True)
    finally:
        logger.info("QMT分钟线数据订阅服务已停止")


if __name__ == "__main__":
    main()
