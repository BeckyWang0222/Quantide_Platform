# -*- coding: utf-8 -*-
"""
Mac端主程序 - 数据处理者
功能：
1. 从Redis消费历史分钟线数据
2. 存储数据到ClickHouse
3. 清理Redis队列
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
import uvicorn
import webbrowser

from config import WEB_PORTS, DATA_CLEANUP_TIME
from database import RedisManager, ClickHouseManager
from models import SystemStatus
from trading_time_validator import TradingTimeValidator


class MacDataService:
    """Mac端数据服务"""

    def __init__(self):
        self.redis_manager = RedisManager()
        self.clickhouse_manager = ClickHouseManager()
        self.trading_validator = TradingTimeValidator()
        self.is_running = False
        self.is_processing = False
        self.status = SystemStatus(
            service_name="Mac数据处理服务",
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

        # 启动数据处理线程
        threading.Thread(target=self._data_processing_loop, daemon=True).start()
        # 启动时间检查线程
        threading.Thread(target=self._time_check_loop, daemon=True).start()

    def stop_service(self):
        """停止服务"""
        self.is_running = False
        self.status.status = "stopped"
        self.status.message = "服务已停止"
        self.status.last_update = datetime.now()

    def _data_processing_loop(self):
        """数据处理循环"""
        while self.is_running:
            try:
                # 持续处理各个周期的分钟线数据
                processed_any = False
                for period in [1, 5, 15, 30]:
                    count = self._process_bar_data(period)
                    if count > 0:
                        processed_any = True

                if processed_any:
                    self.status.last_update = datetime.now()
                    self.status.message = "正在处理数据"
                elif not self.is_processing:
                    self.status.message = "等待数据"

                # 如果是历史数据处理模式，处理间隔更短
                sleep_time = 2 if self.is_processing else 10
                time.sleep(sleep_time)

            except Exception as e:
                self.status.status = "error"
                self.status.message = f"数据处理错误: {str(e)}"
                self.status.last_update = datetime.now()
                time.sleep(30)

    def _time_check_loop(self):
        """时间检查循环"""
        while self.is_running:
            try:
                current_time = datetime.now().time()

                # 检查是否为凌晨2点（开始处理历史数据）
                cleanup_time = dt_time.fromisoformat(DATA_CLEANUP_TIME)
                if (current_time.hour == cleanup_time.hour and
                    current_time.minute >= cleanup_time.minute and
                    current_time.minute < cleanup_time.minute + 30):  # 30分钟处理窗口

                    if not self.is_processing:
                        self._start_historical_data_processing()
                else:
                    if self.is_processing:
                        self._stop_historical_data_processing()

                time.sleep(60)  # 每分钟检查一次

            except Exception as e:
                print(f"时间检查错误: {e}")
                time.sleep(60)

    def _start_historical_data_processing(self):
        """开始历史数据处理"""
        self.is_processing = True
        self.status.message = "正在处理历史数据"
        self.status.last_update = datetime.now()

        try:
            # 处理前一天的历史数据
            total_processed = 0

            for period in [1, 5, 15, 30]:
                processed_count = self._process_historical_bar_data(period)
                total_processed += processed_count

            # 清理Redis队列
            self.redis_manager.clear_all_queues()

            self.status.data_count = total_processed
            self.status.message = f"历史数据处理完成，共处理 {total_processed} 条记录"
            self.status.last_update = datetime.now()

        except Exception as e:
            self.status.status = "error"
            self.status.message = f"历史数据处理错误: {str(e)}"
            self.status.last_update = datetime.now()

    def _stop_historical_data_processing(self):
        """停止历史数据处理"""
        self.is_processing = False
        self.status.message = "等待下次处理时间"
        self.status.last_update = datetime.now()

    def _process_bar_data(self, period: int) -> int:
        """处理历史分钟线数据，返回处理的数据条数"""
        try:
            bar_data_list = []

            # 批量消费Redis队列中的历史分钟线数据
            for _ in range(100):  # 每次最多处理100条
                bar_data = self.redis_manager.consume_bar_data(period, timeout=1)
                if bar_data:
                    # 验证是否为交易时间内的数据
                    bar_dict = {
                        'frame': bar_data.frame,
                        'symbol': bar_data.symbol,
                        'open': bar_data.open,
                        'high': bar_data.high,
                        'low': bar_data.low,
                        'close': bar_data.close,
                        'vol': bar_data.vol,
                        'amount': bar_data.amount
                    }

                    # 只处理交易时间内的历史数据
                    if self.trading_validator.validate_bar_data(bar_dict):
                        bar_data_list.append(bar_data)
                    else:
                        print(f"Mac端过滤非交易时间历史数据: {bar_data.symbol} at {bar_data.frame}")
                else:
                    break

            # 批量插入ClickHouse（只存储历史数据）
            if bar_data_list:
                self.clickhouse_manager.insert_bar_data(bar_data_list, period)
                self.status.data_count += len(bar_data_list)
                return len(bar_data_list)

            return 0

        except Exception as e:
            print(f"处理{period}分钟线历史数据错误: {e}")
            return 0

    def _process_historical_bar_data(self, period: int) -> int:
        """处理历史分钟线数据"""
        try:
            bar_data_list = []
            processed_count = 0

            # 消费所有历史数据
            while True:
                bar_data = self.redis_manager.consume_bar_data(period, timeout=1)
                if bar_data:
                    bar_data_list.append(bar_data)

                    # 批量插入（每1000条插入一次）
                    if len(bar_data_list) >= 1000:
                        self.clickhouse_manager.insert_bar_data(bar_data_list, period)
                        processed_count += len(bar_data_list)
                        bar_data_list = []
                else:
                    break

            # 插入剩余数据
            if bar_data_list:
                self.clickhouse_manager.insert_bar_data(bar_data_list, period)
                processed_count += len(bar_data_list)

            return processed_count

        except Exception as e:
            print(f"处理{period}分钟线历史数据错误: {e}")
            return 0

    def get_status(self) -> SystemStatus:
        """获取服务状态"""
        return self.status

    def start_manual_processing(self):
        """手动启动历史数据处理"""
        if not self.is_processing:
            self.is_processing = True
            self.status.message = "手动启动历史数据处理"
            self.status.last_update = datetime.now()
            return True
        return False

    def stop_manual_processing(self):
        """停止手动历史数据处理"""
        if self.is_processing:
            self.is_processing = False
            self.status.message = "已停止历史数据处理"
            self.status.last_update = datetime.now()
            return True
        return False

    def get_system_info(self) -> dict:
        """获取系统信息"""
        redis_info = self.redis_manager.get_system_info()
        clickhouse_info = self.clickhouse_manager.get_system_info()

        return {
            "service_status": self.status.model_dump(),
            "redis_info": redis_info,
            "clickhouse_info": clickhouse_info,
            "is_processing": self.is_processing
        }


# 创建FastAPI应用
app = FastAPI(title="Mac端数据处理服务")
service = MacDataService()

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
        <title>Mac端数据处理服务</title>
        <meta charset="utf-8">
        <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/css/bootstrap.min.css" rel="stylesheet">
        <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    </head>
    <body>
        <div class="container mt-4">
            <h1>Mac端数据处理服务</h1>
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
                <div class="col-md-8">
                    <div class="card">
                        <div class="card-header">
                            <h5>ClickHouse存储状态</h5>
                        </div>
                        <div class="card-body" id="clickhouse-status">
                            加载中...
                        </div>
                    </div>
                </div>
                <div class="col-md-4">
                    <div class="card">
                        <div class="card-header">
                            <h5>历史数据处理控制</h5>
                        </div>
                        <div class="card-body">
                            <p>手动控制历史数据处理进程</p>
                            <button id="startProcessing" class="btn btn-success me-2">启动处理</button>
                            <button id="stopProcessing" class="btn btn-danger">停止处理</button>
                            <div id="processingStatus" class="mt-3"></div>
                        </div>
                    </div>
                </div>
            </div>
        </div>

        <script>
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
                            <p><strong>正在处理:</strong> <span class="badge bg-${data.is_processing ? 'warning' : 'secondary'}">${data.is_processing ? '是' : '否'}</span></p>
                        `;

                        // 更新Redis状态
                        const redisStatus = document.getElementById('redis-status');
                        let redisHtml = '';
                        for (const [key, value] of Object.entries(data.redis_info)) {
                            redisHtml += `<p><strong>${key}:</strong> ${value}</p>`;
                        }
                        redisStatus.innerHTML = redisHtml;

                        // 更新ClickHouse状态
                        const clickhouseStatus = document.getElementById('clickhouse-status');
                        let clickhouseHtml = '';
                        for (const [key, value] of Object.entries(data.clickhouse_info)) {
                            clickhouseHtml += `<p><strong>${key}:</strong> ${value}</p>`;
                        }
                        clickhouseStatus.innerHTML = clickhouseHtml;

                        // 更新按钮状态
                        const startBtn = document.getElementById('startProcessing');
                        const stopBtn = document.getElementById('stopProcessing');

                        if (data.is_processing) {
                            startBtn.disabled = true;
                            stopBtn.disabled = false;
                        } else {
                            startBtn.disabled = false;
                            stopBtn.disabled = true;
                        }
                    })
                    .catch(error => {
                        console.error('Error:', error);
                    });
            }

            function controlProcessing(action) {
                const statusDiv = document.getElementById('processingStatus');
                statusDiv.innerHTML = '<div class="alert alert-info">正在执行操作...</div>';

                fetch(`/api/${action}-processing`, {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                    }
                })
                .then(response => response.json())
                .then(data => {
                    const alertClass = data.success ? 'alert-success' : 'alert-danger';
                    statusDiv.innerHTML = `<div class="alert ${alertClass}">${data.message}</div>`;

                    // 立即更新状态
                    setTimeout(updateStatus, 500);

                    // 3秒后清除状态消息
                    setTimeout(() => {
                        statusDiv.innerHTML = '';
                    }, 3000);
                })
                .catch(error => {
                    statusDiv.innerHTML = `<div class="alert alert-danger">操作失败: ${error.message}</div>`;
                });
            }

            // 页面加载完成后绑定事件
            document.addEventListener('DOMContentLoaded', function() {
                document.getElementById('startProcessing').addEventListener('click', function() {
                    controlProcessing('start');
                });

                document.getElementById('stopProcessing').addEventListener('click', function() {
                    controlProcessing('stop');
                });
            });

            // 初始加载和定时更新
            updateStatus();
            setInterval(updateStatus, 5000);
        </script>
    </body>
    </html>
    """

@app.get("/api/status")
async def get_status():
    """获取系统状态API"""
    return service.get_system_info()


@app.post("/api/start-processing")
async def start_processing():
    """手动启动历史数据处理API"""
    try:
        success = service.start_manual_processing()
        return JSONResponse(content={
            "success": success,
            "message": "历史数据处理已启动" if success else "历史数据处理已在运行中"
        })
    except Exception as e:
        return JSONResponse(content={
            "success": False,
            "message": f"启动失败: {str(e)}"
        })


@app.post("/api/stop-processing")
async def stop_processing():
    """停止历史数据处理API"""
    try:
        success = service.stop_manual_processing()
        return JSONResponse(content={
            "success": success,
            "message": "历史数据处理已停止" if success else "历史数据处理未在运行"
        })
    except Exception as e:
        return JSONResponse(content={
            "success": False,
            "message": f"停止失败: {str(e)}"
        })


if __name__ == "__main__":
    # 自动打开浏览器
    webbrowser.open(f"http://localhost:{WEB_PORTS['mac']}")

    # 启动Web服务
    uvicorn.run(app, host="0.0.0.0", port=WEB_PORTS['mac'])
