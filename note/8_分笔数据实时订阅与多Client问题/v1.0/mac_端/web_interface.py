#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Web可视化界面

提供股票数据查询和图表展示的Web界面
"""

from flask import Flask, render_template, request, jsonify
from flask_cors import CORS
import json
from datetime import datetime, timedelta
import pandas as pd
import logging
from mac_market_data_sdk import MacMarketDataSDK


class WebInterface:
    """Web可视化界面"""

    def __init__(self, config):
        """
        初始化Web界面

        Args:
            config (dict): 配置字典
        """
        self.config = config
        self.sdk = MacMarketDataSDK(config)

        # 创建Flask应用
        web_config = config.get('web', {})
        self.app = Flask(
            __name__,
            template_folder=web_config.get('template_folder', 'templates'),
            static_folder=web_config.get('static_folder', 'static')
        )
        CORS(self.app)

        self.logger = logging.getLogger(__name__)
        self.setup_routes()

    def setup_routes(self):
        """设置路由"""

        @self.app.route('/')
        def index():
            """主页"""
            return render_template('index.html')

        @self.app.route('/api/query_data', methods=['POST'])
        def query_data():
            """查询历史数据API"""
            try:
                data = request.get_json()
                symbol = data.get('symbol')
                start_date = data.get('start_date')
                end_date = data.get('end_date')
                period = data.get('period', '1min')

                self.logger.info(f"查询请求: {symbol}, {start_date} - {end_date}, {period}")

                # 查询数据
                if period == 'daily':
                    df = self.sdk.get_daily_bars(symbol, start_date, end_date)
                else:
                    start_time = f"{start_date} 09:30:00"
                    end_time = f"{end_date} 15:00:00"
                    df = self.sdk.get_minute_bars(symbol, start_time, end_time, period)

                # 转换为前端需要的格式
                result = self.format_chart_data(df)

                return jsonify({
                    'success': True,
                    'data': result,
                    'message': f'查询到{len(df)}条数据'
                })

            except Exception as e:
                self.logger.error(f"查询数据失败: {e}")
                return jsonify({
                    'success': False,
                    'error': str(e)
                })

        @self.app.route('/api/realtime_data', methods=['POST'])
        def realtime_data():
            """获取实时数据API"""
            try:
                data = request.get_json()
                symbol = data.get('symbol')

                self.logger.info(f"实时数据请求: {symbol}")

                # 获取实时数据
                realtime_info = self.sdk.get_latest_price(symbol)

                if realtime_info:
                    return jsonify({
                        'success': True,
                        'data': {
                            'symbol': realtime_info['symbol'],
                            'price': realtime_info['close'],
                            'volume': realtime_info['vol'],
                            'amount': realtime_info['amount'],
                            'timestamp': realtime_info['frame']
                        }
                    })
                else:
                    return jsonify({
                        'success': False,
                        'error': '未找到实时数据'
                    })

            except Exception as e:
                self.logger.error(f"获取实时数据失败: {e}")
                return jsonify({
                    'success': False,
                    'error': str(e)
                })

        @self.app.route('/api/market_overview', methods=['GET'])
        def market_overview():
            """市场概览API"""
            try:
                # 获取所有有数据的股票概览（限制数量）
                overview = self.sdk.get_market_overview()

                return jsonify({
                    'success': True,
                    'data': overview
                })

            except Exception as e:
                self.logger.error(f"获取市场概览失败: {e}")
                return jsonify({
                    'success': False,
                    'error': str(e)
                })

        @self.app.route('/api/search_symbols', methods=['POST'])
        def search_symbols():
            """搜索股票代码API"""
            try:
                data = request.get_json()
                keyword = data.get('keyword', '')

                if not keyword:
                    return jsonify({
                        'success': False,
                        'error': '请输入搜索关键词'
                    })

                symbols = self.sdk.search_symbols(keyword)

                return jsonify({
                    'success': True,
                    'data': symbols
                })

            except Exception as e:
                self.logger.error(f"搜索股票代码失败: {e}")
                return jsonify({
                    'success': False,
                    'error': str(e)
                })

        @self.app.route('/api/validate_symbol', methods=['POST'])
        def validate_symbol():
            """验证股票代码API"""
            try:
                data = request.get_json()
                symbol = data.get('symbol', '')

                if not symbol:
                    return jsonify({
                        'success': False,
                        'error': '请输入股票代码'
                    })

                is_valid = self.sdk.validate_symbol(symbol.upper())

                return jsonify({
                    'success': True,
                    'data': {
                        'symbol': symbol.upper(),
                        'is_valid': is_valid,
                        'message': '股票代码有效' if is_valid else '股票代码无效或暂无数据'
                    }
                })

            except Exception as e:
                self.logger.error(f"验证股票代码失败: {e}")
                return jsonify({
                    'success': False,
                    'error': str(e)
                })

        @self.app.route('/api/available_symbols', methods=['GET'])
        def available_symbols():
            """获取所有可用股票代码API"""
            try:
                symbols = self.sdk.get_available_symbols()

                return jsonify({
                    'success': True,
                    'data': symbols,
                    'count': len(symbols)
                })

            except Exception as e:
                self.logger.error(f"获取可用股票代码失败: {e}")
                return jsonify({
                    'success': False,
                    'error': str(e)
                })

        @self.app.route('/api/statistics', methods=['GET'])
        def statistics():
            """数据统计API"""
            try:
                stats = self.sdk.get_data_statistics()

                return jsonify({
                    'success': True,
                    'data': stats
                })

            except Exception as e:
                self.logger.error(f"获取统计信息失败: {e}")
                return jsonify({
                    'success': False,
                    'error': str(e)
                })

    def format_chart_data(self, df):
        """
        格式化图表数据

        Args:
            df (pd.DataFrame): 数据DataFrame

        Returns:
            dict: 格式化后的图表数据
        """
        if df.empty:
            return {'kline': [], 'volume': [], 'dates': []}

        # K线数据格式: [open, close, low, high]
        kline_data = []
        volume_data = []
        dates = []

        for index, row in df.iterrows():
            # 处理时间格式
            if isinstance(index, str):
                dates.append(index)
            else:
                dates.append(index.strftime('%Y-%m-%d %H:%M:%S'))

            kline_data.append([
                float(row['open']),
                float(row['close']),
                float(row['low']),
                float(row['high'])
            ])
            volume_data.append(float(row['vol']))

        return {
            'kline': kline_data,
            'volume': volume_data,
            'dates': dates
        }

    def run(self, host=None, port=None, debug=None):
        """
        启动Web服务

        Args:
            host (str): 主机地址
            port (int): 端口号
            debug (bool): 调试模式
        """
        web_config = self.config.get('web', {})

        host = host or web_config.get('host', '0.0.0.0')
        port = port or web_config.get('port', 5000)
        debug = debug or web_config.get('debug', True)

        self.logger.info(f"启动Web服务: http://{host}:{port}")
        self.app.run(host=host, port=port, debug=debug)
