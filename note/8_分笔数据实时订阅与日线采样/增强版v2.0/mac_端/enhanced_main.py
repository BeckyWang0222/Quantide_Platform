#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
增强版数据消费主程序 - Mac端

基于最佳实践的高性能数据消费和存储系统
"""

import yaml
import logging
import sys
import os
from datetime import datetime
from enhanced_data_consumer import EnhancedDataConsumer


def setup_logging(log_level='INFO'):
    """设置日志配置"""
    # 创建logs目录
    log_dir = 'logs'
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    # 日志文件名包含日期
    log_filename = os.path.join(log_dir, f'mac_enhanced_{datetime.now().strftime("%Y%m%d")}.log')
    
    # 配置日志格式
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    
    # 配置日志处理器
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format=log_format,
        handlers=[
            logging.FileHandler(log_filename, encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    # 设置第三方库日志级别
    logging.getLogger('redis').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('clickhouse_driver').setLevel(logging.WARNING)


def load_config(config_file='enhanced_mac_config.yaml'):
    """加载配置文件"""
    try:
        with open(config_file, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        return config
    except FileNotFoundError:
        print(f"配置文件 {config_file} 不存在，使用默认配置")
        return get_default_config()
    except Exception as e:
        print(f"加载配置文件失败: {e}")
        return get_default_config()


def get_default_config():
    """获取默认配置"""
    return {
        'redis': {
            'host': '8.217.201.221',
            'port': 16379,
            'password': 'quantide666',
            'db': 0
        },
        'clickhouse': {
            'host': 'localhost',
            'port': 9000,
            'user': 'default',
            'password': '',
            'database': 'default'
        },
        'data': {
            'quality_check': True,
            'retention_days': 30
        },
        'system': {
            'batch_size': 1000,
            'batch_timeout': 5.0,
            'worker_count': 4,
            'max_retry_times': 3,
            'log_level': 'INFO'
        }
    }


def print_banner():
    """打印程序横幅"""
    banner = """
    ╔══════════════════════════════════════════════════════════════╗
    ║                增强版数据消费系统 v2.0 - Mac端                 ║
    ║                                                              ║
    ║  基于最佳实践的高性能股票数据消费与存储系统                     ║
    ║                                                              ║
    ║  主要特性:                                                    ║
    ║  • 多线程批量数据处理                                         ║
    ║  • 数据质量检查和评分                                         ║
    ║  • ClickHouse高性能存储                                      ║
    ║  • 实时监控和资源管理                                         ║
    ║  • 自动数据清理和维护                                         ║
    ║                                                              ║
    ╚══════════════════════════════════════════════════════════════╝
    """
    print(banner)


def print_config_info(config):
    """打印配置信息"""
    print("=" * 60)
    print("Mac端系统配置信息")
    print("=" * 60)
    print(f"Redis服务器: {config['redis']['host']}:{config['redis']['port']}")
    print(f"ClickHouse服务器: {config['clickhouse']['host']}:{config['clickhouse']['port']}")
    print(f"批量大小: {config['system']['batch_size']}")
    print(f"批量超时: {config['system']['batch_timeout']}秒")
    print(f"工作线程数: {config['system']['worker_count']}")
    print(f"数据质量检查: {'启用' if config['data']['quality_check'] else '禁用'}")
    print(f"数据保留天数: {config['data']['retention_days']}")
    print(f"日志级别: {config['system']['log_level']}")
    print("=" * 60)


def main():
    """主函数"""
    try:
        # 打印横幅
        print_banner()
        
        # 加载配置
        print("正在加载配置...")
        config = load_config()
        
        # 设置日志
        setup_logging(config['system']['log_level'])
        logger = logging.getLogger(__name__)
        
        # 打印配置信息
        print_config_info(config)
        
        # 创建增强版数据消费器
        logger.info("正在初始化增强版数据消费器...")
        consumer = EnhancedDataConsumer(config)
        
        # 启动数据消费
        logger.info("正在启动增强版数据消费...")
        success = consumer.start_enhanced_consumption()
        
        if not success:
            logger.error("启动数据消费失败")
            sys.exit(1)
        
    except KeyboardInterrupt:
        print("\n收到停止信号，正在退出...")
        if 'consumer' in locals():
            consumer.stop_consumption()
        sys.exit(0)
    except Exception as e:
        print(f"程序运行出错: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
