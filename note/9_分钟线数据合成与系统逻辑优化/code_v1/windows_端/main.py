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
    <html>
    <head>
        <title>Windows端数据生产服务</title>
        <meta charset="utf-8">
        <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/css/bootstrap.min.css" rel="stylesheet">
        <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    </head>
    <body>
        <div class="container mt-4">
            <h1>Windows端数据生产服务</h1>
            <div class="row">
                <div class="col-md-6">
                    <div class="card">
                        <div class="card-header">
                            <h5>服务状态</h5>
                        </div>
                        <div class="card-body" id="service-status">
                            加载中...
                        </div>
                    </div>
                </div>
                <div class="col-md-6">
                    <div class="card">
                        <div class="card-header">
                            <h5>Redis队列状态</h5>
                        </div>
                        <div class="card-body" id="redis-status">
                            加载中...
                        </div>
                    </div>
                </div>
            </div>
            <div class="row mt-4">
                <div class="col-md-6">
                    <div class="card">
                        <div class="card-header">
                            <h5>数据缓存状态</h5>
                        </div>
                        <div class="card-body" id="cache-status">
                            加载中...
                        </div>
                    </div>
                </div>
                <div class="col-md-6">
                    <div class="card">
                        <div class="card-header">
                            <h5>历史数据获取</h5>
                        </div>
                        <div class="card-body">
                            <form id="historicalForm">
                                <div class="mb-3">
                                    <label for="startTime" class="form-label">开始时间</label>
                                    <input type="datetime-local" class="form-control" id="startTime" required>
                                </div>
                                <div class="mb-3">
                                    <label for="endTime" class="form-label">结束时间</label>
                                    <input type="datetime-local" class="form-control" id="endTime" required>
                                </div>
                                <div class="mb-3">
                                    <label for="symbols" class="form-label">股票代码（可选，多个用逗号分隔）</label>
                                    <input type="text" class="form-control" id="symbols" placeholder="000001.SZ,600000.SH">
                                </div>
                                <div class="mb-3">
                                    <label class="form-label">周期选择</label>
                                    <div>
                                        <input type="checkbox" id="period1" value="1" checked>
                                        <label for="period1">1分钟</label>
                                        <input type="checkbox" id="period5" value="5" checked>
                                        <label for="period5">5分钟</label>
                                        <input type="checkbox" id="period15" value="15" checked>
                                        <label for="period15">15分钟</label>
                                        <input type="checkbox" id="period30" value="30" checked>
                                        <label for="period30">30分钟</label>
                                    </div>
                                </div>
                                <button type="submit" class="btn btn-primary">开始获取</button>
                            </form>
                            <div id="taskStatus" class="mt-3" style="display: none;"></div>
                        </div>
                    </div>
                </div>
            </div>
            <div class="row mt-4">
                <div class="col-12">
                    <div class="card">
                        <div class="card-header">
                            <h5>历史数据获取任务</h5>
                        </div>
                        <div class="card-body" id="tasks-status">
                            暂无任务
                        </div>
                    </div>
                </div>
            </div>
        </div>

        <script>
            let currentTaskId = null;

            function updateStatus() {
                fetch('/api/status')
                    .then(response => response.json())
                    .then(data => {
                        // 更新服务状态
                        const serviceStatus = document.getElementById('service-status');
                        const status = data.service_status;
                        serviceStatus.innerHTML = `
                            <p><strong>状态:</strong> <span class="badge bg-${status.status === 'running' ? 'success' : 'danger'}">${status.status}</span></p>
                            <p><strong>消息:</strong> ${status.message}</p>
                            <p><strong>数据计数:</strong> ${status.data_count}</p>
                            <p><strong>最后更新:</strong> ${status.last_update}</p>
                            <p><strong>交易时间:</strong> <span class="badge bg-${data.is_trading_time ? 'success' : 'secondary'}">${data.is_trading_time ? '是' : '否'}</span></p>
                        `;

                        // 更新Redis状态
                        const redisStatus = document.getElementById('redis-status');
                        let redisHtml = '';
                        for (const [key, value] of Object.entries(data.redis_info)) {
                            redisHtml += `<p><strong>${key}:</strong> ${value}</p>`;
                        }
                        redisStatus.innerHTML = redisHtml;

                        // 更新缓存状态
                        const cacheStatus = document.getElementById('cache-status');
                        let cacheHtml = '';
                        for (const [key, value] of Object.entries(data.cache_info)) {
                            if (key.includes('filter_rate')) {
                                const percentage = (value * 100).toFixed(2);
                                cacheHtml += `<p><strong>${key}:</strong> ${percentage}%</p>`;
                            } else {
                                cacheHtml += `<p><strong>${key}:</strong> ${value}</p>`;
                            }
                        }
                        cacheStatus.innerHTML = cacheHtml;
                    })
                    .catch(error => {
                        console.error('Error:', error);
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
                                tasksStatus.innerHTML = '暂无任务';
                                return;
                            }

                            let tasksHtml = '<div class="table-responsive"><table class="table table-sm"><thead><tr><th>任务ID</th><th>状态</th><th>进度</th><th>消息</th><th>开始时间</th></tr></thead><tbody>';

                            for (const [taskId, taskInfo] of Object.entries(tasks)) {
                                const progress = taskInfo.total_symbols > 0 ?
                                    Math.round((taskInfo.processed_symbols / taskInfo.total_symbols) * 100) : 0;

                                tasksHtml += `
                                    <tr>
                                        <td>${taskId.substring(0, 8)}...</td>
                                        <td><span class="badge bg-${taskInfo.status === 'completed' ? 'success' : taskInfo.status === 'error' ? 'danger' : 'warning'}">${taskInfo.status}</span></td>
                                        <td>${taskInfo.processed_symbols}/${taskInfo.total_symbols} (${progress}%)</td>
                                        <td>${taskInfo.message}</td>
                                        <td>${new Date(taskInfo.start_time).toLocaleString()}</td>
                                    </tr>
                                `;
                            }

                            tasksHtml += '</tbody></table></div>';
                            tasksStatus.innerHTML = tasksHtml;
                        }
                    })
                    .catch(error => {
                        console.error('Error:', error);
                    });
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
                                <div class="alert alert-success">
                                    <strong>任务已启动</strong><br>
                                    任务ID: ${data.task_id}<br>
                                    股票数量: ${data.total_symbols}<br>
                                    时间范围: ${startTime} 至 ${endTime}
                                </div>
                            `;

                            // 开始监控任务进度
                            monitorTask(data.task_id);
                        } else {
                            taskStatus.innerHTML = `<div class="alert alert-danger">任务启动失败: ${data.message}</div>`;
                        }
                    })
                    .catch(error => {
                        taskStatus.innerHTML = `<div class="alert alert-danger">请求失败: ${error.message}</div>`;
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
                                taskStatus.innerHTML = `
                                    <div class="alert alert-info">
                                        <strong>任务进行中</strong><br>
                                        状态: ${taskInfo.status}<br>
                                        进度: ${taskInfo.processed_symbols}/${taskInfo.total_symbols} (${progress}%)<br>
                                        记录数: ${taskInfo.total_records}<br>
                                        消息: ${taskInfo.message}
                                    </div>
                                `;

                                if (taskInfo.status === 'completed' || taskInfo.status === 'error') {
                                    clearInterval(interval);
                                    const alertClass = taskInfo.status === 'completed' ? 'alert-success' : 'alert-danger';
                                    taskStatus.innerHTML = `
                                        <div class="alert ${alertClass}">
                                            <strong>任务${taskInfo.status === 'completed' ? '完成' : '失败'}</strong><br>
                                            最终状态: ${taskInfo.status}<br>
                                            处理记录: ${taskInfo.total_records}<br>
                                            消息: ${taskInfo.message}
                                        </div>
                                    `;
                                }
                            }
                        })
                        .catch(error => {
                            console.error('监控任务失败:', error);
                            clearInterval(interval);
                        });
                }, 2000); // 每2秒检查一次
            }

            // 初始加载和定时更新
            updateStatus();
            updateTasks();
            setInterval(updateStatus, 5000);
            setInterval(updateTasks, 3000);
        </script>
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
