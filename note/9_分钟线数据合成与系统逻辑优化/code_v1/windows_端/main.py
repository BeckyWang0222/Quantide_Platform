# -*- coding: utf-8 -*-
"""
Windows端主程序 - 数据生产者
功能：
1. 从QMT实时订阅分笔数据
2. 合成分钟线数据
3. 发布数据到Redis
4. 提供Web管理界面
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import asyncio
import threading
import time
from datetime import datetime, time as dt_time
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
import uvicorn
import webbrowser

from config import WEB_PORTS, TRADING_HOURS, DATA_CLEANUP_TIME
from database import RedisManager
from data_processor import BarDataSynthesizer
from models import TickData, SystemStatus, HistoricalDataRequest, HistoricalDataResponse
from qmt_historical_fetcher import QMTHistoricalFetcher
from trading_time_validator import TradingTimeValidator


class WindowsDataService:
    """Windows端数据服务"""

    def __init__(self):
        self.redis_manager = RedisManager()
        self.synthesizer = BarDataSynthesizer()
        self.historical_fetcher = QMTHistoricalFetcher()
        self.trading_validator = TradingTimeValidator()
        self.is_running = False
        self.is_trading_time = False
        self.status = SystemStatus(
            service_name="Windows数据生产服务",
            status="stopped",
            last_update=datetime.now(),
            message="服务未启动"
        )

    def start_service(self):
        """启动服务"""
        self.is_running = True
        self.status.status = "running"
        self.status.message = "服务正在运行"
        self.status.last_update = datetime.now()

        # 启动数据订阅线程
        threading.Thread(target=self._data_subscription_loop, daemon=True).start()
        # 启动时间检查线程
        threading.Thread(target=self._time_check_loop, daemon=True).start()

    def stop_service(self):
        """停止服务"""
        self.is_running = False
        self.status.status = "stopped"
        self.status.message = "服务已停止"
        self.status.last_update = datetime.now()

    def _data_subscription_loop(self):
        """数据订阅循环"""
        while self.is_running:
            try:
                if self.is_trading_time:
                    # 模拟从QMT订阅分笔数据
                    tick_data = self._simulate_qmt_tick_data()
                    if tick_data:
                        # 验证分笔数据是否在交易时间内
                        tick_dict = {
                            'time': tick_data.time,
                            'symbol': tick_data.symbol,
                            'price': tick_data.price,
                            'volume': tick_data.volume,
                            'amount': tick_data.amount
                        }

                        if self.trading_validator.validate_tick_data(tick_dict):
                            # 发布分笔数据到Redis
                            self.redis_manager.publish_tick_data(tick_data)

                            # 合成分钟线数据
                            self.synthesizer.add_tick_data(tick_data)

                            # 发布合成的分钟线数据（当日数据）
                            for period in [1, 5, 15, 30]:
                                bars = self.synthesizer.get_latest_bars(tick_data.symbol, period, 1)
                                if bars:
                                    # 当日合成数据，is_historical=False
                                    self.redis_manager.publish_bar_data(bars[-1], period, is_historical=False)

                            self.status.data_count += 1
                            self.status.last_update = datetime.now()
                        else:
                            # 记录被过滤的数据
                            print(f"过滤非交易时间分笔数据: {tick_data.symbol} at {tick_data.time}")

                time.sleep(1)  # 1秒间隔

            except Exception as e:
                self.status.status = "error"
                self.status.message = f"数据订阅错误: {str(e)}"
                self.status.last_update = datetime.now()
                time.sleep(5)

    def _time_check_loop(self):
        """时间检查循环"""
        while self.is_running:
            try:
                current_time = datetime.now().time()

                # 检查是否为交易时间
                morning_start = dt_time.fromisoformat(TRADING_HOURS['morning_start'])
                morning_end = dt_time.fromisoformat(TRADING_HOURS['morning_end'])
                afternoon_start = dt_time.fromisoformat(TRADING_HOURS['afternoon_start'])
                afternoon_end = dt_time.fromisoformat(TRADING_HOURS['afternoon_end'])

                self.is_trading_time = (
                    (morning_start <= current_time <= morning_end) or
                    (afternoon_start <= current_time <= afternoon_end)
                )

                # 检查是否为凌晨2点（数据清理时间）
                cleanup_time = dt_time.fromisoformat(DATA_CLEANUP_TIME)
                if current_time.hour == cleanup_time.hour and current_time.minute == cleanup_time.minute:
                    self._handle_cleanup_time()

                time.sleep(60)  # 每分钟检查一次

            except Exception as e:
                print(f"时间检查错误: {e}")
                time.sleep(60)

    def _handle_cleanup_time(self):
        """处理凌晨2点的特殊逻辑"""
        try:
            # 模拟从QMT订阅前一天的分钟线数据
            yesterday_bars = self._simulate_qmt_historical_data()

            # 发布到Redis
            for bar_data, period in yesterday_bars:
                self.redis_manager.publish_bar_data(bar_data, period)

            self.status.message = "已完成前一天历史数据订阅"
            self.status.last_update = datetime.now()

        except Exception as e:
            self.status.status = "error"
            self.status.message = f"历史数据订阅错误: {str(e)}"
            self.status.last_update = datetime.now()

    def _simulate_qmt_tick_data(self) -> TickData:
        """模拟QMT分笔数据（实际应用中替换为真实的QMT API调用）"""
        import random

        # 只在交易时间内生成数据
        current_time = datetime.now()
        if not self.trading_validator.is_trading_time(current_time):
            return None

        symbols = ["000001.SZ", "000002.SZ", "600000.SH", "600036.SH"]
        symbol = random.choice(symbols)

        return TickData(
            symbol=symbol,
            time=current_time,
            price=round(random.uniform(10.0, 50.0), 2),
            volume=random.randint(100, 10000),
            amount=round(random.uniform(1000, 500000), 2)
        )

    def _simulate_qmt_historical_data(self):
        """模拟QMT历史数据订阅"""
        # 这里应该是真实的QMT历史数据API调用
        # 返回前一天的分钟线数据
        return []

    def get_status(self) -> SystemStatus:
        """获取服务状态"""
        return self.status

    def get_system_info(self) -> dict:
        """获取系统信息"""
        redis_info = self.redis_manager.get_system_info()
        cache_info = self.synthesizer.get_cache_info()

        return {
            "service_status": self.status.model_dump(),
            "redis_info": redis_info,
            "cache_info": cache_info,
            "is_trading_time": self.is_trading_time
        }


# 创建FastAPI应用
app = FastAPI(title="Windows端数据生产服务")
service = WindowsDataService()

@app.on_event("startup")
async def startup_event():
    """启动事件"""
    service.start_service()

@app.on_event("shutdown")
async def shutdown_event():
    """关闭事件"""
    service.stop_service()

@app.get("/", response_class=HTMLResponse)
async def get_dashboard():
    """获取仪表板页面"""
    return """
    <!DOCTYPE html>
    <html lang="zh-CN">
    <head>
        <title>Windows端数据生产服务 - 量化交易系统</title>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
        <link href="https://cdn.jsdelivr.net/npm/bootstrap-icons@1.10.0/font/bootstrap-icons.css" rel="stylesheet">
        <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
        <style>
            :root {
                --primary-color: #2563eb;
                --success-color: #059669;
                --warning-color: #d97706;
                --danger-color: #dc2626;
                --info-color: #0891b2;
                --dark-color: #1f2937;
                --light-bg: #f8fafc;
                --card-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1);
            }

            body {
                background-color: var(--light-bg);
                font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            }

            .navbar {
                background: linear-gradient(135deg, var(--primary-color), var(--info-color));
                box-shadow: var(--card-shadow);
            }

            .card {
                border: none;
                border-radius: 12px;
                box-shadow: var(--card-shadow);
                transition: transform 0.2s ease, box-shadow 0.2s ease;
                background: white;
            }

            .card:hover {
                transform: translateY(-2px);
                box-shadow: 0 8px 25px -5px rgba(0, 0, 0, 0.1);
            }

            .card-header {
                background: linear-gradient(135deg, #f8fafc, #e2e8f0);
                border-bottom: 2px solid var(--primary-color);
                border-radius: 12px 12px 0 0 !important;
                padding: 1rem 1.5rem;
            }

            .card-header h5 {
                margin: 0;
                color: var(--dark-color);
                font-weight: 600;
                display: flex;
                align-items: center;
                gap: 0.5rem;
            }

            .status-badge {
                font-size: 0.875rem;
                padding: 0.5rem 1rem;
                border-radius: 50px;
                font-weight: 500;
            }

            .metric-item {
                display: flex;
                justify-content: space-between;
                align-items: center;
                padding: 0.75rem 0;
                border-bottom: 1px solid #e5e7eb;
            }

            .metric-item:last-child {
                border-bottom: none;
            }

            .metric-label {
                color: #6b7280;
                font-weight: 500;
            }

            .metric-value {
                color: var(--dark-color);
                font-weight: 600;
            }

            .form-control, .form-select {
                border-radius: 8px;
                border: 2px solid #e5e7eb;
                transition: border-color 0.2s ease;
            }

            .form-control:focus, .form-select:focus {
                border-color: var(--primary-color);
                box-shadow: 0 0 0 0.2rem rgba(37, 99, 235, 0.25);
            }

            .btn {
                border-radius: 8px;
                font-weight: 500;
                padding: 0.75rem 1.5rem;
                transition: all 0.2s ease;
            }

            .btn-primary {
                background: linear-gradient(135deg, var(--primary-color), var(--info-color));
                border: none;
            }

            .btn-primary:hover {
                transform: translateY(-1px);
                box-shadow: 0 4px 12px rgba(37, 99, 235, 0.4);
            }

            .checkbox-group {
                display: flex;
                flex-wrap: wrap;
                gap: 1rem;
                padding: 1rem;
                background: #f8fafc;
                border-radius: 8px;
                border: 2px solid #e5e7eb;
            }

            .checkbox-item {
                display: flex;
                align-items: center;
                gap: 0.5rem;
            }

            .checkbox-item input[type="checkbox"] {
                width: 1.2rem;
                height: 1.2rem;
                accent-color: var(--primary-color);
            }

            .loading-spinner {
                display: inline-block;
                width: 1rem;
                height: 1rem;
                border: 2px solid #e5e7eb;
                border-radius: 50%;
                border-top-color: var(--primary-color);
                animation: spin 1s ease-in-out infinite;
            }

            @keyframes spin {
                to { transform: rotate(360deg); }
            }

            .alert {
                border-radius: 8px;
                border: none;
            }

            .table {
                border-radius: 8px;
                overflow: hidden;
            }

            .table thead th {
                background: var(--primary-color);
                color: white;
                border: none;
                font-weight: 600;
            }

            .progress {
                height: 8px;
                border-radius: 4px;
                background: #e5e7eb;
            }

            .progress-bar {
                border-radius: 4px;
            }
        </style>
    </head>
    <body>
        <!-- 导航栏 -->
        <nav class="navbar navbar-dark">
            <div class="container">
                <span class="navbar-brand mb-0 h1">
                    <i class="bi bi-pc-display"></i>
                    Windows端数据生产服务
                </span>
                <span class="navbar-text">
                    <i class="bi bi-clock"></i>
                    <span id="current-time"></span>
                </span>
            </div>
        </nav>

            <!-- 系统概览 -->
            <div class="row mb-4">
                <div class="col-12">
                    <div class="card">
                        <div class="card-header">
                            <h5><i class="bi bi-speedometer2"></i> 系统概览</h5>
                        </div>
                        <div class="card-body">
                            <div class="row text-center">
                                <div class="col-md-3">
                                    <div class="metric-card">
                                        <i class="bi bi-play-circle text-success" style="font-size: 2rem;"></i>
                                        <h6 class="mt-2">服务状态</h6>
                                        <span id="service-status-badge" class="status-badge bg-secondary">加载中</span>
                                    </div>
                                </div>
                                <div class="col-md-3">
                                    <div class="metric-card">
                                        <i class="bi bi-clock text-info" style="font-size: 2rem;"></i>
                                        <h6 class="mt-2">交易时间</h6>
                                        <span id="trading-time-badge" class="status-badge bg-secondary">检查中</span>
                                    </div>
                                </div>
                                <div class="col-md-3">
                                    <div class="metric-card">
                                        <i class="bi bi-database text-warning" style="font-size: 2rem;"></i>
                                        <h6 class="mt-2">数据计数</h6>
                                        <span id="data-count" class="metric-value">0</span>
                                    </div>
                                </div>
                                <div class="col-md-3">
                                    <div class="metric-card">
                                        <i class="bi bi-arrow-clockwise text-primary" style="font-size: 2rem;"></i>
                                        <h6 class="mt-2">最后更新</h6>
                                        <span id="last-update" class="metric-value">--</span>
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            </div>

            <!-- 详细状态 -->
            <div class="row mb-4">
                <div class="col-lg-6">
                    <div class="card h-100">
                        <div class="card-header">
                            <h5><i class="bi bi-gear"></i> 服务详细状态</h5>
                        </div>
                        <div class="card-body" id="service-status">
                            <div class="text-center">
                                <div class="loading-spinner"></div>
                                <p class="mt-2 text-muted">加载中...</p>
                            </div>
                        </div>
                    </div>
                </div>
                <div class="col-lg-6">
                    <div class="card h-100">
                        <div class="card-header">
                            <h5><i class="bi bi-hdd-network"></i> Redis队列状态</h5>
                        </div>
                        <div class="card-body" id="redis-status">
                            <div class="text-center">
                                <div class="loading-spinner"></div>
                                <p class="mt-2 text-muted">加载中...</p>
                            </div>
                        </div>
                    </div>
                </div>
            </div>

            <div class="row mb-4">
                <div class="col-lg-6">
                    <div class="card h-100">
                        <div class="card-header">
                            <h5><i class="bi bi-memory"></i> 数据缓存状态</h5>
                        </div>
                        <div class="card-body" id="cache-status">
                            <div class="text-center">
                                <div class="loading-spinner"></div>
                                <p class="mt-2 text-muted">加载中...</p>
                            </div>
                        </div>
                    </div>
                </div>
                <div class="col-lg-6">
                    <div class="card h-100">
                        <div class="card-header">
                            <h5><i class="bi bi-download"></i> 历史数据获取</h5>
                        </div>
                        <div class="card-body">
                            <form id="historicalForm">
                                <div class="row">
                                    <div class="col-md-6">
                                        <div class="mb-3">
                                            <label for="startTime" class="form-label">
                                                <i class="bi bi-calendar-event"></i> 开始时间
                                            </label>
                                            <input type="datetime-local" class="form-control" id="startTime" required>
                                        </div>
                                    </div>
                                    <div class="col-md-6">
                                        <div class="mb-3">
                                            <label for="endTime" class="form-label">
                                                <i class="bi bi-calendar-check"></i> 结束时间
                                            </label>
                                            <input type="datetime-local" class="form-control" id="endTime" required>
                                        </div>
                                    </div>
                                </div>
                                <div class="mb-3">
                                    <label for="symbols" class="form-label">
                                        <i class="bi bi-list-ul"></i> 股票代码（可选，多个用逗号分隔）
                                    </label>
                                    <input type="text" class="form-control" id="symbols"
                                           placeholder="例如：000001.SZ,600000.SH">
                                </div>
                                <div class="mb-3">
                                    <label class="form-label">
                                        <i class="bi bi-clock-history"></i> 周期选择
                                    </label>
                                    <div class="checkbox-group">
                                        <div class="checkbox-item">
                                            <input type="checkbox" id="period1" value="1" checked>
                                            <label for="period1">1分钟</label>
                                        </div>
                                        <div class="checkbox-item">
                                            <input type="checkbox" id="period5" value="5" checked>
                                            <label for="period5">5分钟</label>
                                        </div>
                                        <div class="checkbox-item">
                                            <input type="checkbox" id="period15" value="15" checked>
                                            <label for="period15">15分钟</label>
                                        </div>
                                        <div class="checkbox-item">
                                            <input type="checkbox" id="period30" value="30" checked>
                                            <label for="period30">30分钟</label>
                                        </div>
                                    </div>
                                </div>
                                <button type="submit" class="btn btn-primary w-100">
                                    <i class="bi bi-play-fill"></i> 开始获取历史数据
                                </button>
                            </form>
                            <div id="taskStatus" class="mt-3" style="display: none;"></div>
                        </div>
                    </div>
                </div>
            </div>

            <!-- 任务状态 -->
            <div class="row">
                <div class="col-12">
                    <div class="card">
                        <div class="card-header">
                            <h5><i class="bi bi-list-task"></i> 历史数据获取任务</h5>
                        </div>
                        <div class="card-body" id="tasks-status">
                            <div class="text-center text-muted">
                                <i class="bi bi-inbox" style="font-size: 3rem;"></i>
                                <p class="mt-2">暂无任务</p>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </div>

        <script>
            let currentTaskId = null;

            // 更新当前时间
            function updateCurrentTime() {
                const now = new Date();
                document.getElementById('current-time').textContent = now.toLocaleString('zh-CN');
            }

            function updateStatus() {
                fetch('/api/status')
                    .then(response => response.json())
                    .then(data => {
                        const status = data.service_status;

                        // 更新概览卡片
                        updateOverviewCards(status, data);

                        // 更新详细服务状态
                        updateServiceStatus(status);

                        // 更新Redis状态
                        updateRedisStatus(data.redis_info);

                        // 更新缓存状态
                        updateCacheStatus(data.cache_info);
                    })
                    .catch(error => {
                        console.error('Error:', error);
                        showErrorState();
                    });
            }

            function updateOverviewCards(status, data) {
                // 服务状态徽章
                const statusBadge = document.getElementById('service-status-badge');
                statusBadge.className = `status-badge bg-${status.status === 'running' ? 'success' : 'danger'}`;
                statusBadge.textContent = status.status === 'running' ? '运行中' : '已停止';

                // 交易时间徽章
                const tradingBadge = document.getElementById('trading-time-badge');
                tradingBadge.className = `status-badge bg-${data.is_trading_time ? 'success' : 'secondary'}`;
                tradingBadge.textContent = data.is_trading_time ? '交易时间' : '非交易时间';

                // 数据计数
                document.getElementById('data-count').textContent = status.data_count || 0;

                // 最后更新时间
                const lastUpdate = new Date(status.last_update).toLocaleString('zh-CN');
                document.getElementById('last-update').textContent = lastUpdate;
            }

            function updateServiceStatus(status) {
                const serviceStatus = document.getElementById('service-status');
                serviceStatus.innerHTML = `
                    <div class="metric-item">
                        <span class="metric-label">运行状态</span>
                        <span class="badge bg-${status.status === 'running' ? 'success' : 'danger'}">
                            <i class="bi bi-${status.status === 'running' ? 'check-circle' : 'x-circle'}"></i>
                            ${status.status === 'running' ? '运行中' : '已停止'}
                        </span>
                    </div>
                    <div class="metric-item">
                        <span class="metric-label">状态消息</span>
                        <span class="metric-value">${status.message}</span>
                    </div>
                    <div class="metric-item">
                        <span class="metric-label">处理数据量</span>
                        <span class="metric-value">
                            <i class="bi bi-graph-up"></i> ${status.data_count || 0} 条
                        </span>
                    </div>
                    <div class="metric-item">
                        <span class="metric-label">最后更新</span>
                        <span class="metric-value">${new Date(status.last_update).toLocaleString('zh-CN')}</span>
                    </div>
                `;
            }

            function updateRedisStatus(redisInfo) {
                const redisStatus = document.getElementById('redis-status');
                let html = '';

                for (const [key, value] of Object.entries(redisInfo)) {
                    const icon = getRedisIcon(key);
                    html += `
                        <div class="metric-item">
                            <span class="metric-label">
                                <i class="bi bi-${icon}"></i> ${formatRedisKey(key)}
                            </span>
                            <span class="metric-value">${value}</span>
                        </div>
                    `;
                }

                redisStatus.innerHTML = html || '<p class="text-muted">暂无数据</p>';
            }

            function updateCacheStatus(cacheInfo) {
                const cacheStatus = document.getElementById('cache-status');
                let html = '';

                for (const [key, value] of Object.entries(cacheInfo)) {
                    const icon = getCacheIcon(key);
                    let displayValue = value;

                    if (key.includes('filter_rate')) {
                        displayValue = `${(value * 100).toFixed(2)}%`;
                    }

                    html += `
                        <div class="metric-item">
                            <span class="metric-label">
                                <i class="bi bi-${icon}"></i> ${formatCacheKey(key)}
                            </span>
                            <span class="metric-value">${displayValue}</span>
                        </div>
                    `;
                }

                cacheStatus.innerHTML = html || '<p class="text-muted">暂无数据</p>';
            }

            function getRedisIcon(key) {
                if (key.includes('queue')) return 'list-ul';
                if (key.includes('data')) return 'database';
                return 'hdd-network';
            }

            function getCacheIcon(key) {
                if (key.includes('tick')) return 'clock';
                if (key.includes('bar')) return 'bar-chart';
                if (key.includes('rate')) return 'percent';
                return 'memory';
            }

            function formatRedisKey(key) {
                const keyMap = {
                    'whole_quote_data': '分笔数据队列',
                    'bar_data_1min': '1分钟线队列',
                    'bar_data_5min': '5分钟线队列',
                    'bar_data_15min': '15分钟线队列',
                    'bar_data_30min': '30分钟线队列'
                };
                return keyMap[key] || key;
            }

            function formatCacheKey(key) {
                const keyMap = {
                    'tick_cache_symbols': 'Tick缓存股票数',
                    'tick_cache_total': 'Tick缓存总数',
                    'bar_1min_symbols': '1分钟线股票数',
                    'bar_1min_total': '1分钟线总数',
                    'bar_5min_symbols': '5分钟线股票数',
                    'bar_5min_total': '5分钟线总数',
                    'bar_15min_symbols': '15分钟线股票数',
                    'bar_15min_total': '15分钟线总数',
                    'bar_30min_symbols': '30分钟线股票数',
                    'bar_30min_total': '30分钟线总数',
                    'tick_filter_rate': 'Tick过滤率',
                    'bar_filter_rate': '分钟线过滤率'
                };
                return keyMap[key] || key;
            }

            function showErrorState() {
                const elements = ['service-status', 'redis-status', 'cache-status'];
                elements.forEach(id => {
                    document.getElementById(id).innerHTML = `
                        <div class="text-center text-danger">
                            <i class="bi bi-exclamation-triangle" style="font-size: 2rem;"></i>
                            <p class="mt-2">连接失败</p>
                        </div>
                    `;
                });
            }

            function updateTasks() {
                fetch('/api/all-tasks')
                    .then(response => response.json())
                    .then(result => {
                        if (result.success) {
                            const tasksStatus = document.getElementById('tasks-status');
                            const tasks = result.data;

                            if (Object.keys(tasks).length === 0) {
                                tasksStatus.innerHTML = `
                                    <div class="text-center text-muted">
                                        <i class="bi bi-inbox" style="font-size: 3rem;"></i>
                                        <p class="mt-2">暂无任务</p>
                                    </div>
                                `;
                                return;
                            }

                            let tasksHtml = `
                                <div class="table-responsive">
                                    <table class="table table-hover">
                                        <thead>
                                            <tr>
                                                <th><i class="bi bi-hash"></i> 任务ID</th>
                                                <th><i class="bi bi-flag"></i> 状态</th>
                                                <th><i class="bi bi-bar-chart"></i> 进度</th>
                                                <th><i class="bi bi-graph-up"></i> 记录数</th>
                                                <th><i class="bi bi-chat-text"></i> 消息</th>
                                                <th><i class="bi bi-clock"></i> 开始时间</th>
                                            </tr>
                                        </thead>
                                        <tbody>
                            `;

                            for (const [taskId, taskInfo] of Object.entries(tasks)) {
                                const progress = taskInfo.total_symbols > 0 ?
                                    Math.round((taskInfo.processed_symbols / taskInfo.total_symbols) * 100) : 0;

                                const statusClass = getTaskStatusClass(taskInfo.status);
                                const statusIcon = getTaskStatusIcon(taskInfo.status);

                                tasksHtml += `
                                    <tr>
                                        <td>
                                            <code>${taskId.substring(0, 8)}...</code>
                                        </td>
                                        <td>
                                            <span class="badge ${statusClass}">
                                                <i class="bi bi-${statusIcon}"></i>
                                                ${getTaskStatusText(taskInfo.status)}
                                            </span>
                                        </td>
                                        <td>
                                            <div class="d-flex align-items-center">
                                                <div class="progress flex-grow-1 me-2" style="height: 20px;">
                                                    <div class="progress-bar ${getProgressBarClass(taskInfo.status)}"
                                                         style="width: ${progress}%">
                                                        ${progress}%
                                                    </div>
                                                </div>
                                                <small class="text-muted">${taskInfo.processed_symbols}/${taskInfo.total_symbols}</small>
                                            </div>
                                        </td>
                                        <td>
                                            <span class="badge bg-info">
                                                <i class="bi bi-database"></i>
                                                ${taskInfo.total_records || 0}
                                            </span>
                                        </td>
                                        <td>
                                            <small class="text-muted">${taskInfo.message}</small>
                                        </td>
                                        <td>
                                            <small>${new Date(taskInfo.start_time).toLocaleString('zh-CN')}</small>
                                        </td>
                                    </tr>
                                `;
                            }

                            tasksHtml += '</tbody></table></div>';
                            tasksStatus.innerHTML = tasksHtml;
                        }
                    })
                    .catch(error => {
                        console.error('Error:', error);
                        document.getElementById('tasks-status').innerHTML = `
                            <div class="text-center text-danger">
                                <i class="bi bi-exclamation-triangle" style="font-size: 2rem;"></i>
                                <p class="mt-2">加载任务失败</p>
                            </div>
                        `;
                    });
            }

            function getTaskStatusClass(status) {
                const statusMap = {
                    'running': 'bg-warning',
                    'completed': 'bg-success',
                    'error': 'bg-danger',
                    'pending': 'bg-secondary'
                };
                return statusMap[status] || 'bg-secondary';
            }

            function getTaskStatusIcon(status) {
                const iconMap = {
                    'running': 'arrow-clockwise',
                    'completed': 'check-circle',
                    'error': 'x-circle',
                    'pending': 'clock'
                };
                return iconMap[status] || 'question-circle';
            }

            function getTaskStatusText(status) {
                const textMap = {
                    'running': '运行中',
                    'completed': '已完成',
                    'error': '错误',
                    'pending': '等待中'
                };
                return textMap[status] || status;
            }

            function getProgressBarClass(status) {
                const classMap = {
                    'running': 'bg-warning',
                    'completed': 'bg-success',
                    'error': 'bg-danger',
                    'pending': 'bg-secondary'
                };
                return classMap[status] || 'bg-secondary';
            }

            // 历史数据获取表单提交
            document.addEventListener('DOMContentLoaded', function() {
                // 设置默认时间
                const now = new Date();
                const yesterday = new Date(now.getTime() - 24 * 60 * 60 * 1000);

                document.getElementById('startTime').value = formatDateTime(yesterday);
                document.getElementById('endTime').value = formatDateTime(now);

                // 表单提交事件
                document.getElementById('historicalForm').addEventListener('submit', function(e) {
                    e.preventDefault();

                    const startTime = document.getElementById('startTime').value;
                    const endTime = document.getElementById('endTime').value;
                    const symbolsInput = document.getElementById('symbols').value;

                    // 获取选中的周期
                    const periods = [];
                    if (document.getElementById('period1').checked) periods.push(1);
                    if (document.getElementById('period5').checked) periods.push(5);
                    if (document.getElementById('period15').checked) periods.push(15);
                    if (document.getElementById('period30').checked) periods.push(30);

                    // 解析股票代码
                    const symbols = symbolsInput ? symbolsInput.split(',').map(s => s.trim()).filter(s => s) : [];

                    const requestData = {
                        start_time: startTime,  // 直接使用datetime-local的值
                        end_time: endTime,      // 直接使用datetime-local的值
                        symbols: symbols,
                        periods: periods
                    };

                    // 显示任务状态
                    const taskStatus = document.getElementById('taskStatus');
                    taskStatus.style.display = 'block';
                    taskStatus.innerHTML = '<div class="alert alert-info">正在启动历史数据获取任务...</div>';

                    // 发送请求
                    fetch('/api/fetch-historical', {
                        method: 'POST',
                        headers: {
                            'Content-Type': 'application/json',
                        },
                        body: JSON.stringify(requestData)
                    })
                    .then(response => response.json())
                    .then(data => {
                        if (data.success) {
                            currentTaskId = data.task_id;
                            taskStatus.innerHTML = `
                                <div class="alert alert-success border-0">
                                    <div class="d-flex align-items-center">
                                        <i class="bi bi-check-circle-fill me-2" style="font-size: 1.5rem;"></i>
                                        <div>
                                            <h6 class="alert-heading mb-1">任务已启动</h6>
                                            <div class="small">
                                                <strong>任务ID:</strong> <code>${data.task_id.substring(0, 8)}...</code><br>
                                                <strong>股票数量:</strong> ${data.total_symbols} 只<br>
                                                <strong>时间范围:</strong> ${startTime} 至 ${endTime}
                                            </div>
                                        </div>
                                    </div>
                                </div>
                            `;

                            // 开始监控任务进度
                            monitorTask(data.task_id);
                        } else {
                            taskStatus.innerHTML = `
                                <div class="alert alert-danger border-0">
                                    <div class="d-flex align-items-center">
                                        <i class="bi bi-x-circle-fill me-2" style="font-size: 1.5rem;"></i>
                                        <div>
                                            <h6 class="alert-heading mb-1">任务启动失败</h6>
                                            <div class="small">${data.message}</div>
                                        </div>
                                    </div>
                                </div>
                            `;
                        }
                    })
                    .catch(error => {
                        taskStatus.innerHTML = `
                            <div class="alert alert-danger border-0">
                                <div class="d-flex align-items-center">
                                    <i class="bi bi-exclamation-triangle-fill me-2" style="font-size: 1.5rem;"></i>
                                    <div>
                                        <h6 class="alert-heading mb-1">请求失败</h6>
                                        <div class="small">${error.message}</div>
                                    </div>
                                </div>
                            </div>
                        `;
                    });
                });
            });

            function formatDateTime(date) {
                return date.toISOString().slice(0, 16);
            }

            function monitorTask(taskId) {
                const interval = setInterval(() => {
                    fetch(`/api/task-status/${taskId}`)
                        .then(response => response.json())
                        .then(result => {
                            if (result.success) {
                                const taskInfo = result.data;
                                const progress = taskInfo.total_symbols > 0 ?
                                    Math.round((taskInfo.processed_symbols / taskInfo.total_symbols) * 100) : 0;

                                const taskStatus = document.getElementById('taskStatus');

                                if (taskInfo.status === 'running') {
                                    taskStatus.innerHTML = `
                                        <div class="alert alert-info border-0">
                                            <div class="d-flex align-items-center mb-3">
                                                <div class="loading-spinner me-2"></div>
                                                <div>
                                                    <h6 class="alert-heading mb-1">任务进行中</h6>
                                                    <div class="small text-muted">${taskInfo.message}</div>
                                                </div>
                                            </div>
                                            <div class="row g-3">
                                                <div class="col-md-6">
                                                    <div class="d-flex justify-content-between align-items-center mb-1">
                                                        <span class="small">处理进度</span>
                                                        <span class="small text-muted">${taskInfo.processed_symbols}/${taskInfo.total_symbols}</span>
                                                    </div>
                                                    <div class="progress" style="height: 8px;">
                                                        <div class="progress-bar bg-info" style="width: ${progress}%"></div>
                                                    </div>
                                                </div>
                                                <div class="col-md-6">
                                                    <div class="text-center">
                                                        <div class="h5 mb-0">${taskInfo.total_records || 0}</div>
                                                        <div class="small text-muted">已获取记录数</div>
                                                    </div>
                                                </div>
                                            </div>
                                        </div>
                                    `;
                                } else if (taskInfo.status === 'completed') {
                                    clearInterval(interval);
                                    taskStatus.innerHTML = `
                                        <div class="alert alert-success border-0">
                                            <div class="d-flex align-items-center">
                                                <i class="bi bi-check-circle-fill me-2" style="font-size: 1.5rem;"></i>
                                                <div>
                                                    <h6 class="alert-heading mb-1">任务完成</h6>
                                                    <div class="small">
                                                        <strong>处理记录:</strong> ${taskInfo.total_records} 条<br>
                                                        <strong>最终状态:</strong> ${taskInfo.message}
                                                    </div>
                                                </div>
                                            </div>
                                        </div>
                                    `;
                                } else if (taskInfo.status === 'error') {
                                    clearInterval(interval);
                                    taskStatus.innerHTML = `
                                        <div class="alert alert-danger border-0">
                                            <div class="d-flex align-items-center">
                                                <i class="bi bi-x-circle-fill me-2" style="font-size: 1.5rem;"></i>
                                                <div>
                                                    <h6 class="alert-heading mb-1">任务失败</h6>
                                                    <div class="small">
                                                        <strong>错误信息:</strong> ${taskInfo.message}<br>
                                                        <strong>处理记录:</strong> ${taskInfo.total_records || 0} 条
                                                    </div>
                                                </div>
                                            </div>
                                        </div>
                                    `;
                                }
                            }
                        })
                        .catch(error => {
                            console.error('监控任务失败:', error);
                            clearInterval(interval);
                            document.getElementById('taskStatus').innerHTML = `
                                <div class="alert alert-warning border-0">
                                    <div class="d-flex align-items-center">
                                        <i class="bi bi-exclamation-triangle-fill me-2" style="font-size: 1.5rem;"></i>
                                        <div>
                                            <h6 class="alert-heading mb-1">监控中断</h6>
                                            <div class="small">无法获取任务状态，请刷新页面查看</div>
                                        </div>
                                    </div>
                                </div>
                            `;
                        });
                }, 2000); // 每2秒检查一次
            }

            // 初始化
            document.addEventListener('DOMContentLoaded', function() {
                // 更新当前时间
                updateCurrentTime();
                setInterval(updateCurrentTime, 1000);

                // 初始加载状态
                updateStatus();
                updateTasks();

                // 定时更新
                setInterval(updateStatus, 5000);
                setInterval(updateTasks, 3000);
            });
        </script>

        <!-- Bootstrap JS -->
        <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/js/bootstrap.bundle.min.js"></script>
    </body>
    </html>
    """

@app.get("/api/status")
async def get_status():
    """获取系统状态API"""
    return service.get_system_info()


@app.post("/api/fetch-historical")
async def fetch_historical_data(request: Request):
    """获取历史数据API"""
    try:
        data = await request.json()

        # 解析请求参数
        start_time_str = data.get('start_time')
        end_time_str = data.get('end_time')

        # 处理时间字符串，移除Z后缀并解析
        if start_time_str.endswith('Z'):
            start_time_str = start_time_str[:-1]
        if end_time_str.endswith('Z'):
            end_time_str = end_time_str[:-1]

        historical_request = HistoricalDataRequest(
            start_time=datetime.fromisoformat(start_time_str),
            end_time=datetime.fromisoformat(end_time_str),
            symbols=data.get('symbols', []),
            periods=data.get('periods', [1, 5, 15, 30])
        )

        # 执行历史数据获取
        response = service.historical_fetcher.fetch_historical_data(historical_request)

        # 手动序列化响应，处理datetime对象
        response_data = {
            "success": response.success,
            "message": response.message,
            "task_id": response.task_id,
            "total_symbols": response.total_symbols,
            "processed_symbols": response.processed_symbols,
            "total_records": response.total_records,
            "start_time": response.start_time.isoformat() if response.start_time else None,
            "end_time": response.end_time.isoformat() if response.end_time else None
        }

        return JSONResponse(content=response_data)

    except Exception as e:
        return JSONResponse(content={
            "success": False,
            "message": f"请求处理错误: {str(e)}",
            "task_id": ""
        })


@app.get("/api/task-status/{task_id}")
async def get_task_status(task_id: str):
    """获取任务状态API"""
    try:
        status = service.historical_fetcher.get_task_status(task_id)
        if status:
            # 创建一个新的状态字典，安全地处理datetime对象
            safe_status = {}
            for key, value in status.items():
                if key == 'start_time' and value:
                    safe_status[key] = value.isoformat()
                elif key == 'end_time' and value:
                    safe_status[key] = value.isoformat()
                elif key == 'request' and value:
                    # 处理request对象
                    if hasattr(value, 'start_time'):
                        safe_status[key] = {
                            'start_time': value.start_time.isoformat(),
                            'end_time': value.end_time.isoformat(),
                            'symbols': value.symbols,
                            'periods': value.periods
                        }
                    else:
                        safe_status[key] = value
                else:
                    safe_status[key] = value

            return JSONResponse(content={
                "success": True,
                "data": safe_status
            })
        else:
            return JSONResponse(content={
                "success": False,
                "message": "任务不存在"
            })
    except Exception as e:
        return JSONResponse(content={
            "success": False,
            "message": f"获取任务状态失败: {str(e)}"
        })


@app.get("/api/all-tasks")
async def get_all_tasks():
    """获取所有任务状态API"""
    try:
        tasks = service.historical_fetcher.get_all_tasks()

        # 安全地转换datetime对象为字符串
        serialized_tasks = {}
        for task_id, task_info in tasks.items():
            serialized_info = {}
            for key, value in task_info.items():
                if key == 'start_time' and value:
                    serialized_info[key] = value.isoformat()
                elif key == 'end_time' and value:
                    serialized_info[key] = value.isoformat()
                elif key == 'request' and value:
                    # 处理request对象
                    if hasattr(value, 'start_time'):
                        serialized_info[key] = {
                            'start_time': value.start_time.isoformat(),
                            'end_time': value.end_time.isoformat(),
                            'symbols': value.symbols,
                            'periods': value.periods
                        }
                    else:
                        serialized_info[key] = value
                else:
                    serialized_info[key] = value
            serialized_tasks[task_id] = serialized_info

        return JSONResponse(content={
            "success": True,
            "data": serialized_tasks
        })
    except Exception as e:
        return JSONResponse(content={
            "success": False,
            "message": f"获取任务列表失败: {str(e)}"
        })





if __name__ == "__main__":
    # 自动打开浏览器
    webbrowser.open(f"http://localhost:{WEB_PORTS['windows']}")

    # 启动Web服务
    uvicorn.run(app, host="0.0.0.0", port=WEB_PORTS['windows'])
