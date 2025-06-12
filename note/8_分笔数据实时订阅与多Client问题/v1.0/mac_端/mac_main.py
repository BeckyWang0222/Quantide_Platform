#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Mac端主程序

提供数据消费、Web服务等功能的统一入口
"""

import yaml
import logging
import logging.handlers
import os
import sys
import signal
import threading
import argparse
from mac_data_consumer import MacDataConsumer
from web_interface import WebInterface
from clickhouse_driver import Client


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
    
    log_file = os.path.join(log_dir, log_config.get('file', 'mac_system.log'))
    
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
    config_file = 'mac_config.yaml'
    
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


def init_clickhouse(config):
    """
    初始化ClickHouse数据库
    
    Args:
        config (dict): 配置字典
    """
    try:
        ch_config = config.get('clickhouse', {})
        client = Client(
            host=ch_config.get('host', 'localhost'),
            port=ch_config.get('port', 9000),
            user=ch_config.get('user', 'default'),
            password=ch_config.get('password', '')
        )
        
        # 读取初始化SQL文件
        sql_file = config.get('database', {}).get('init_sql_file', 'init_clickhouse.sql')
        if os.path.exists(sql_file):
            with open(sql_file, 'r', encoding='utf-8') as f:
                sql_content = f.read()
            
            # 执行SQL语句
            for statement in sql_content.split(';'):
                statement = statement.strip()
                if statement:
                    client.execute(statement)
            
            print("ClickHouse数据库初始化成功")
        else:
            print(f"警告: SQL初始化文件 {sql_file} 不存在")
            
    except Exception as e:
        print(f"ClickHouse初始化失败: {e}")
        sys.exit(1)


def signal_handler(signum, frame):
    """信号处理器"""
    logger = logging.getLogger(__name__)
    logger.info(f"接收到信号 {signum}，准备退出...")
    sys.exit(0)


def run_consumer(config):
    """运行数据消费器"""
    logger = logging.getLogger(__name__)
    logger.info("启动数据消费器模式")
    
    consumer = MacDataConsumer(config)
    num_workers = config.get('system', {}).get('consumer_threads', 4)
    
    try:
        consumer.start_consuming(num_workers)
    except KeyboardInterrupt:
        logger.info("用户中断，停止消费器")
        consumer.stop_consuming()


def run_web(config):
    """运行Web服务"""
    logger = logging.getLogger(__name__)
    logger.info("启动Web服务模式")
    
    web_interface = WebInterface(config)
    
    try:
        web_interface.run()
    except KeyboardInterrupt:
        logger.info("用户中断，停止Web服务")


def run_all(config):
    """运行所有服务"""
    logger = logging.getLogger(__name__)
    logger.info("启动完整服务模式")
    
    # 启动数据消费器线程
    consumer = MacDataConsumer(config)
    num_workers = config.get('system', {}).get('consumer_threads', 4)
    
    consumer_thread = threading.Thread(
        target=consumer.start_consuming,
        args=(num_workers,),
        daemon=True
    )
    consumer_thread.start()
    
    # 启动Web服务
    web_interface = WebInterface(config)
    
    try:
        web_interface.run()
    except KeyboardInterrupt:
        logger.info("用户中断，停止所有服务")
        consumer.stop_consuming()


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='Mac端分钟线数据处理系统')
    parser.add_argument('--mode', choices=['consumer', 'web', 'all'], default='all',
                       help='运行模式: consumer(数据消费), web(Web服务), all(全部)')
    parser.add_argument('--init-db', action='store_true',
                       help='初始化ClickHouse数据库')
    
    args = parser.parse_args()
    
    # 注册信号处理器
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        # 加载配置
        config = load_config()
        
        # 配置日志
        logger = setup_logging(config)
        logger.info("=" * 50)
        logger.info("启动Mac端分钟线数据处理系统")
        logger.info("=" * 50)
        
        # 初始化数据库
        if args.init_db:
            logger.info("初始化ClickHouse数据库...")
            init_clickhouse(config)
        
        # 打印配置信息
        logger.info(f"Redis服务器: {config.get('redis', {}).get('host')}:{config.get('redis', {}).get('port')}")
        logger.info(f"ClickHouse: {config.get('clickhouse', {}).get('host')}:{config.get('clickhouse', {}).get('port')}")
        logger.info(f"运行模式: {args.mode}")
        
        # 根据模式启动相应服务
        if args.mode == 'consumer':
            run_consumer(config)
        elif args.mode == 'web':
            run_web(config)
        elif args.mode == 'all':
            run_all(config)
        
    except Exception as e:
        logger.error(f"系统异常: {e}")
        logger.error("详细错误信息:", exc_info=True)
    finally:
        logger.info("Mac端分钟线数据处理系统已停止")


if __name__ == "__main__":
    main()
