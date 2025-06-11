#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
QMT增强版数据订阅主程序 - Windows端

基于最佳实践的高性能股票数据订阅系统
"""

import yaml
import logging
import sys
import os
from datetime import datetime
from qmt_enhanced_subscriber import QMTEnhancedSubscriber


def setup_logging(log_level='INFO'):
    """设置日志配置"""
    # 创建logs目录
    log_dir = 'logs'
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    # 日志文件名包含日期
    log_filename = os.path.join(log_dir, f'qmt_enhanced_{datetime.now().strftime("%Y%m%d")}.log')
    
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


def load_config(config_file='enhanced_config.yaml'):
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
        'qmt': {
            'subscription_mode': 'whole_quote',  # whole_quote 或 individual
            'max_subscribe_count': 1000,
            'cache_size': 100,
            'quality_check': True,
            'price_change_threshold': 0.2,
            'validate_stocks': False
        },
        'system': {
            'batch_size': 100,
            'batch_timeout': 1.0,
            'max_retry_times': 3,
            'retry_delay': 1,
            'log_level': 'INFO'
        }
    }


def print_banner():
    """打印程序横幅"""
    banner = """
    ╔══════════════════════════════════════════════════════════════╗
    ║                QMT增强版数据订阅系统 v2.0                      ║
    ║                                                              ║
    ║  基于QMT最佳实践的高性能股票数据实时订阅与处理系统              ║
    ║                                                              ║
    ║  主要特性:                                                    ║
    ║  • 全推行情订阅 (subscribe_whole_quote)                       ║
    ║  • 数据质量检查和缓存机制                                      ║
    ║  • 批量处理和性能优化                                         ║
    ║  • 实时监控和资源管理                                         ║
    ║  • 完善的错误处理和恢复                                       ║
    ║                                                              ║
    ╚══════════════════════════════════════════════════════════════╝
    """
    print(banner)


def print_config_info(config):
    """打印配置信息"""
    print("=" * 60)
    print("系统配置信息")
    print("=" * 60)
    print(f"Redis服务器: {config['redis']['host']}:{config['redis']['port']}")
    print(f"订阅模式: {config['qmt']['subscription_mode']}")
    print(f"最大订阅数: {config['qmt']['max_subscribe_count']}")
    print(f"批量大小: {config['system']['batch_size']}")
    print(f"批量超时: {config['system']['batch_timeout']}秒")
    print(f"数据质量检查: {'启用' if config['qmt']['quality_check'] else '禁用'}")
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
        
        # 创建增强版订阅器
        logger.info("正在初始化QMT增强版订阅器...")
        subscriber = QMTEnhancedSubscriber(config)
        
        # 启动订阅
        logger.info("正在启动增强版数据订阅...")
        success = subscriber.start_enhanced_subscription()
        
        if not success:
            logger.error("启动订阅失败")
            sys.exit(1)
        
    except KeyboardInterrupt:
        print("\n收到停止信号，正在退出...")
        sys.exit(0)
    except Exception as e:
        print(f"程序运行出错: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
