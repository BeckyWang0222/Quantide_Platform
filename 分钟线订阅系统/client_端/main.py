# -*- coding: utf-8 -*-
"""
Client端主程序 - 数据消费者
功能：
1. 查询当日分钟线数据（从Redis）
2. 查询历史分钟线数据（从ClickHouse）
3. 合并查询结果
4. 提供Web查询界面
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime, date
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
import uvicorn

from config import WEB_PORTS
from database import RedisManager, ClickHouseManager
from data_processor import DataMerger
from models import QueryResponse


class ClientDataService:
    """Client端数据服务"""

    def __init__(self):
        try:
            self.redis_manager = RedisManager()
            self.clickhouse_manager = ClickHouseManager()
            self.data_merger = DataMerger()
            print("✓ Client端服务初始化成功")
        except Exception as e:
            print(f"✗ Client端服务初始化失败: {e}")
            raise

    def query_bar_data(self, symbol: str, start_time: datetime, end_time: datetime, period: int) -> QueryResponse:
        """
        查询分钟线数据

        按照系统架构：
        1. 如果查询的分钟线数据是当日的，则直接从Redis中读取合成的分钟线数据
        2. 如果查询的分钟线数据是历史的，则直接从ClickHouse中读取
        3. 如果查询的分钟线数据是既有当日的又有历史的，则合并数据返回给Client
        """
        try:
            today = date.today()
            start_date = start_time.date()
            end_date = end_time.date()

            redis_data = []
            clickhouse_data = []

            # 1. 查询当日数据（从Redis读取）
            if end_date >= today:
                redis_data = self.redis_manager.get_current_bar_data(period, symbol)
                # 过滤时间范围
                redis_data = [bar for bar in redis_data if start_time <= bar.frame <= end_time]

            # 2. 查询历史数据（从ClickHouse读取）
            if start_date < today:
                # 避免与当日数据重复，历史数据查询到今天之前
                hist_end_time = min(end_time, datetime.combine(today, datetime.min.time()))
                if start_time < hist_end_time:
                    clickhouse_data = self.clickhouse_manager.query_bar_data(
                        symbol, start_time, hist_end_time, period
                    )

            # 3. 合并数据
            merged_data = self.data_merger.merge_bar_data(redis_data, clickhouse_data)

            return QueryResponse(
                success=True,
                message=f"当日数据: {len(redis_data)} 条，历史数据: {len(clickhouse_data)} 条，合并后: {len(merged_data)} 条",
                data=merged_data,
                total_count=len(merged_data)
            )

        except Exception as e:
            return QueryResponse(
                success=False,
                message=f"查询失败: {str(e)}",
                data=[],
                total_count=0
            )

    def get_available_symbols(self) -> list:
        """获取可用的股票代码"""
        # 这里可以从Redis或ClickHouse获取实际的股票列表
        # 暂时返回模拟数据
        return ["000001.SZ", "000002.SZ", "600000.SH", "600036.SH"]


# 创建FastAPI应用
app = FastAPI(title="Client端数据查询服务")

# 全局服务实例
service = None

def get_service():
    """获取服务实例"""
    global service
    if service is None:
        service = ClientDataService()
    return service

@app.get("/", response_class=HTMLResponse)
async def get_query_page():
    """获取查询页面"""
    try:
        current_service = get_service()
        symbols = current_service.get_available_symbols()
        symbol_options = ""
        for symbol in symbols:
            symbol_options += f'<option value="{symbol}">{symbol}</option>'

        # 构建HTML页面
        html_template = """<!DOCTYPE html>
<html>
<head>
    <title>分钟线数据查询系统</title>
    <meta charset="utf-8">
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/css/bootstrap.min.css" rel="stylesheet">
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        .time-input-group {
            position: relative;
        }
        .quick-time-buttons {
            margin-top: 8px;
        }
        .quick-time-buttons .btn {
            font-size: 0.75rem;
            padding: 0.25rem 0.5rem;
        }
        input[type="date"], input[type="number"] {
            font-family: monospace;
        }
        .time-input-group input[type="number"] {
            text-align: center;
        }
        .time-input-group .col-3 input {
            padding: 0.375rem 0.25rem;
        }
    </style>
</head>
<body>
    <div class="container mt-4">
        <h1 class="mb-4">分钟线数据查询系统</h1>

        <!-- 系统状态 -->
        <div class="alert alert-success mb-4">
            <h5>系统状态</h5>
            <p>✅ Client端服务正常运行</p>
            <p>✅ 可用股票数量: """ + str(len(symbols)) + """</p>
            <button class="btn btn-sm btn-info" onclick="checkDataStatus()">检查数据状态</button>
            <div id="dataStatus" class="mt-2"></div>
        </div>

        <!-- 查询表单 -->
        <div class="card mb-4">
            <div class="card-header">
                <h5>数据查询</h5>
                <small class="text-muted">时间格式为24小时制，例如：2024-01-01 09:30 表示上午9点30分</small>
            </div>
            <div class="card-body">
                <form id="queryForm">
                    <div class="row">
                        <div class="col-md-3">
                            <label for="symbol" class="form-label">股票代码</label>
                            <select class="form-select" id="symbol" name="symbol" required>
                                <option value="">请选择股票</option>
                                """ + symbol_options + """
                            </select>
                        </div>
                        <div class="col-md-2">
                            <label for="period" class="form-label">周期</label>
                            <select class="form-select" id="period" name="period" required>
                                <option value="1">1分钟</option>
                                <option value="5">5分钟</option>
                                <option value="15">15分钟</option>
                                <option value="30">30分钟</option>
                            </select>
                        </div>
                        <div class="col-md-3">
                            <div class="time-input-group">
                                <label for="start_time" class="form-label">开始时间 (24小时制)</label>
                                <div class="row g-1">
                                    <div class="col-6">
                                        <input type="date" class="form-control" id="start_date" required>
                                    </div>
                                    <div class="col-3">
                                        <input type="number" class="form-control" id="start_hour" min="0" max="23" placeholder="时" required>
                                    </div>
                                    <div class="col-3">
                                        <input type="number" class="form-control" id="start_minute" min="0" max="59" placeholder="分" required>
                                    </div>
                                </div>
                            </div>
                        </div>
                        <div class="col-md-3">
                            <div class="time-input-group">
                                <label for="end_time" class="form-label">结束时间 (24小时制)</label>
                                <div class="row g-1">
                                    <div class="col-6">
                                        <input type="date" class="form-control" id="end_date" required>
                                    </div>
                                    <div class="col-3">
                                        <input type="number" class="form-control" id="end_hour" min="0" max="23" placeholder="时" required>
                                    </div>
                                    <div class="col-3">
                                        <input type="number" class="form-control" id="end_minute" min="0" max="59" placeholder="分" required>
                                    </div>
                                </div>
                            </div>
                        </div>
                        <div class="col-md-1 d-flex align-items-end">
                            <button type="submit" class="btn btn-primary">查询</button>
                        </div>
                    </div>

                    <!-- 快捷时间选择 -->
                    <div class="row mt-3">
                        <div class="col-12">
                            <label class="form-label">快捷时间选择：</label>
                            <div class="quick-time-buttons">
                                <button type="button" class="btn btn-sm btn-outline-primary me-2" onclick="setTodayTrading()">
                                    今日交易时间 (09:30-15:00)
                                </button>
                                <button type="button" class="btn btn-sm btn-outline-secondary me-2" onclick="setLast24Hours()">
                                    最近24小时
                                </button>
                                <button type="button" class="btn btn-sm btn-outline-info me-2" onclick="setThisWeek()">
                                    最近7天
                                </button>
                                <button type="button" class="btn btn-sm btn-outline-success" onclick="setCurrentHour()">
                                    当前小时
                                </button>
                            </div>
                        </div>
                    </div>
                </form>
            </div>
        </div>

        <!-- 查询结果 -->
        <div class="card mb-4">
            <div class="card-header">
                <h5>查询结果</h5>
            </div>
            <div class="card-body">
                <div id="queryStatus" class="alert alert-info" style="display: none;"></div>
                <div id="chartContainer" style="height: 400px;">
                    <canvas id="priceChart"></canvas>
                </div>
            </div>
        </div>

        <!-- 数据表格 -->
        <div class="card">
            <div class="card-header">
                <h5>数据详情</h5>
            </div>
            <div class="card-body">
                <div class="table-responsive">
                    <table class="table table-striped" id="dataTable">
                        <thead>
                            <tr>
                                <th>时间</th>
                                <th>开盘价</th>
                                <th>最高价</th>
                                <th>最低价</th>
                                <th>收盘价</th>
                                <th>成交量</th>
                                <th>成交额</th>
                            </tr>
                        </thead>
                        <tbody id="dataTableBody">
                            <tr>
                                <td colspan="7" class="text-center text-muted">请选择查询条件并点击查询按钮</td>
                            </tr>
                        </tbody>
                    </table>
                </div>
            </div>
        </div>
    </div>

    <script>
        let priceChart = null;

        // 初始化默认时间
        document.addEventListener('DOMContentLoaded', function() {
            const now = new Date();
            const today = formatDate(now);

            // 设置默认查询时间为今天的交易时间
            document.getElementById('start_date').value = today;
            document.getElementById('start_hour').value = '09';
            document.getElementById('start_minute').value = '30';

            document.getElementById('end_date').value = today;
            document.getElementById('end_hour').value = '15';
            document.getElementById('end_minute').value = '00';
        });

        // 日期格式化函数
        function formatDate(date) {
            const year = date.getFullYear();
            const month = String(date.getMonth() + 1).padStart(2, '0');
            const day = String(date.getDate()).padStart(2, '0');
            return `${year}-${month}-${day}`;
        }

        // 从分离的输入框获取完整的日期时间
        function getDateTime(dateId, hourId, minuteId) {
            const date = document.getElementById(dateId).value;
            const hour = String(document.getElementById(hourId).value).padStart(2, '0');
            const minute = String(document.getElementById(minuteId).value).padStart(2, '0');
            return `${date}T${hour}:${minute}`;
        }

        // 设置分离的时间输入框
        function setDateTime(dateId, hourId, minuteId, dateTime) {
            const date = new Date(dateTime);
            document.getElementById(dateId).value = formatDate(date);
            document.getElementById(hourId).value = String(date.getHours()).padStart(2, '0');
            document.getElementById(minuteId).value = String(date.getMinutes()).padStart(2, '0');
        }

        // 快捷时间设置函数
        function setTodayTrading() {
            const today = new Date();
            setDateTime('start_date', 'start_hour', 'start_minute',
                       new Date(today.getFullYear(), today.getMonth(), today.getDate(), 9, 30));
            setDateTime('end_date', 'end_hour', 'end_minute',
                       new Date(today.getFullYear(), today.getMonth(), today.getDate(), 15, 0));
        }

        function setLast24Hours() {
            const now = new Date();
            const yesterday = new Date(now.getTime() - 24 * 60 * 60 * 1000);

            setDateTime('start_date', 'start_hour', 'start_minute', yesterday);
            setDateTime('end_date', 'end_hour', 'end_minute', now);
        }

        function setThisWeek() {
            const now = new Date();
            const weekStart = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000);

            setDateTime('start_date', 'start_hour', 'start_minute', weekStart);
            setDateTime('end_date', 'end_hour', 'end_minute', now);
        }

        function setCurrentHour() {
            const now = new Date();
            const hourStart = new Date(now.getFullYear(), now.getMonth(), now.getDate(), now.getHours(), 0);
            const hourEnd = new Date(now.getFullYear(), now.getMonth(), now.getDate(), now.getHours(), 59);

            setDateTime('start_date', 'start_hour', 'start_minute', hourStart);
            setDateTime('end_date', 'end_hour', 'end_minute', hourEnd);
        }

        // 检查数据状态
        function checkDataStatus() {
            const statusDiv = document.getElementById('dataStatus');
            statusDiv.innerHTML = '<div class="spinner-border spinner-border-sm" role="status"></div> 检查中...';

            fetch('/api/data-status')
                .then(response => response.json())
                .then(data => {
                    if (data.success) {
                        let html = '<div class="row mt-2">';
                        html += '<div class="col-md-6">';
                        html += '<h6>Redis当日数据:</h6>';
                        for (const [period, count] of Object.entries(data.redis_current_data)) {
                            html += `<small>${period}: ${count} 条</small><br>`;
                        }
                        html += '</div>';
                        html += '<div class="col-md-6">';
                        html += '<h6>ClickHouse历史数据:</h6>';
                        for (const [period, count] of Object.entries(data.clickhouse_historical_data)) {
                            html += `<small>${period}: ${count} 条</small><br>`;
                        }
                        html += '</div>';
                        html += '</div>';
                        statusDiv.innerHTML = html;
                    } else {
                        statusDiv.innerHTML = '<div class="text-danger">检查失败: ' + data.error + '</div>';
                    }
                })
                .catch(error => {
                    statusDiv.innerHTML = '<div class="text-danger">检查错误: ' + error.message + '</div>';
                });
        }

        // 时间调试
        function checkTimeDebug() {
            const debugDiv = document.getElementById('timeDebug');
            debugDiv.innerHTML = '<div class="spinner-border spinner-border-sm" role="status"></div> 调试中...';

            fetch('/api/time-debug')
                .then(response => response.json())
                .then(data => {
                    if (data.success) {
                        let html = '<div class="alert alert-info">';
                        html += '<h6>时间调试信息:</h6>';
                        html += '<p><strong>当前服务器时间:</strong> ' + data.current_time + '</p>';
                        html += '<h6>Redis数据时间样本:</h6>';
                        for (const [period, samples] of Object.entries(data.time_samples)) {
                            html += '<p><strong>' + period + ':</strong></p>';
                            if (samples.length > 0) {
                                samples.forEach((sample, index) => {
                                    html += '<small>' + (index + 1) + '. ' + sample.symbol + ' - ' + sample.frame + ' - 收盘价: ' + sample.close + '</small><br>';
                                });
                            } else {
                                html += '<small>无数据</small><br>';
                            }
                        }
                        html += '</div>';
                        debugDiv.innerHTML = html;
                    } else {
                        debugDiv.innerHTML = '<div class="text-danger">调试失败: ' + data.error + '</div>';
                    }
                })
                .catch(error => {
                    debugDiv.innerHTML = '<div class="text-danger">调试错误: ' + error.message + '</div>';
                });
        }

        // 全面诊断
        function fullDebug() {
            const debugDiv = document.getElementById('timeDebug');
            debugDiv.innerHTML = '<div class="spinner-border spinner-border-sm" role="status"></div> 正在进行全面诊断...';

            fetch('/api/full-debug')
                .then(response => response.json())
                .then(data => {
                    if (data.success) {
                        const info = data.debug_info;
                        let html = '<div class="alert alert-warning">';
                        html += '<h6>🔍 全面诊断报告:</h6>';

                        // Redis连接和键信息
                        html += '<h6>🔗 Redis连接状态:</h6>';
                        html += '<p><strong>连接:</strong> ' + info.redis_connection + '</p>';
                        html += '<p><strong>所有键:</strong> ' + (info.redis_all_keys ? info.redis_all_keys.join(', ') : '无') + '</p>';

                        // Redis数据状态
                        html += '<h6>📊 Redis数据状态:</h6>';
                        for (const [period, count] of Object.entries(info.redis_data)) {
                            if (period.includes('count')) {
                                html += '<p><strong>' + period + ':</strong> ' + count + ' 条</p>';
                            }
                        }

                        // ClickHouse数据状态
                        html += '<h6>🗄️ ClickHouse数据状态:</h6>';
                        for (const [period, count] of Object.entries(info.clickhouse_data)) {
                            html += '<p><strong>' + period + ':</strong> ' + count + ' 条</p>';
                        }

                        // 查询测试结果
                        html += '<h6>🧪 查询测试结果:</h6>';

                        // Redis测试结果
                        if (info.redis_test && !info.redis_test.error) {
                            html += '<p><strong>Redis测试:</strong></p>';
                            for (const [period, result] of Object.entries(info.redis_test)) {
                                if (period !== 'error') {
                                    html += '<p>&nbsp;&nbsp;' + period + ': ' + result.total_count + ' 条数据</p>';
                                    if (result.sample_times && result.sample_times.length > 0) {
                                        html += '<p>&nbsp;&nbsp;&nbsp;&nbsp;时间样本: ' + result.sample_times.join(', ') + '</p>';
                                    }
                                }
                            }
                        } else if (info.redis_test && info.redis_test.error) {
                            html += '<p><strong>Redis测试错误:</strong> ' + info.redis_test.error + '</p>';
                        }

                        // ClickHouse测试结果
                        if (info.clickhouse_test) {
                            html += '<p><strong>ClickHouse测试:</strong> ' + info.clickhouse_test.total_count + ' 条数据</p>';
                        }

                        // 完整查询测试结果
                        if (info.full_query_test) {
                            html += '<p><strong>完整查询测试:</strong> ' + info.full_query_test.message + '</p>';
                        }

                        html += '</div>';
                        debugDiv.innerHTML = html;
                    } else {
                        debugDiv.innerHTML = '<div class="text-danger">诊断失败: ' + data.error + '<br><pre>' + data.traceback + '</pre></div>';
                    }
                })
                .catch(error => {
                    debugDiv.innerHTML = '<div class="text-danger">诊断错误: ' + error.message + '</div>';
                });
        }

        // 查询表单提交
        document.getElementById('queryForm').addEventListener('submit', function(e) {
            e.preventDefault();

            const formData = new FormData(e.target);
            const queryData = {
                symbol: formData.get('symbol'),
                period: parseInt(formData.get('period')),
                start_time: getDateTime('start_date', 'start_hour', 'start_minute'),
                end_time: getDateTime('end_date', 'end_hour', 'end_minute')
            };

            // 24小时制时间格式，不需要转换

            // 显示查询状态
            const statusDiv = document.getElementById('queryStatus');
            statusDiv.style.display = 'block';
            statusDiv.className = 'alert alert-info';
            statusDiv.textContent = '正在查询数据...';

            // 发送查询请求
            fetch('/api/query', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify(queryData)
            })
            .then(response => response.json())
            .then(data => {
                if (data.success) {
                    statusDiv.className = 'alert alert-success';
                    statusDiv.textContent = `查询成功，共找到 ${data.total_count} 条记录`;

                    // 更新图表和表格
                    updateChart(data.data);
                    updateTable(data.data);
                } else {
                    statusDiv.className = 'alert alert-danger';
                    statusDiv.textContent = `查询失败: ${data.message}`;
                }
            })
            .catch(error => {
                statusDiv.className = 'alert alert-danger';
                statusDiv.textContent = `查询错误: ${error.message}`;
            });
        });

        function updateChart(data) {
            const ctx = document.getElementById('priceChart').getContext('2d');

            if (priceChart) {
                priceChart.destroy();
            }

            if (data.length === 0) {
                return;
            }

            const labels = data.map(item => new Date(item.frame).toLocaleString());
            const prices = data.map(item => item.close);

            priceChart = new Chart(ctx, {
                type: 'line',
                data: {
                    labels: labels,
                    datasets: [{
                        label: '收盘价',
                        data: prices,
                        borderColor: 'rgb(75, 192, 192)',
                        backgroundColor: 'rgba(75, 192, 192, 0.2)',
                        tension: 0.1
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    scales: {
                        y: {
                            beginAtZero: false
                        }
                    }
                }
            });
        }

        function updateTable(data) {
            const tbody = document.getElementById('dataTableBody');
            tbody.innerHTML = '';

            if (data.length === 0) {
                const row = tbody.insertRow();
                row.innerHTML = '<td colspan="7" class="text-center text-muted">没有找到数据</td>';
                return;
            }

            data.forEach(item => {
                const row = tbody.insertRow();
                row.innerHTML = `
                    <td>${new Date(item.frame).toLocaleString()}</td>
                    <td>${item.open.toFixed(2)}</td>
                    <td>${item.high.toFixed(2)}</td>
                    <td>${item.low.toFixed(2)}</td>
                    <td>${item.close.toFixed(2)}</td>
                    <td>${item.vol.toFixed(0)}</td>
                    <td>${item.amount.toFixed(2)}</td>
                `;
            });
        }
    </script>
</body>
</html>"""

        return html_template

    except Exception as e:
        # 如果获取股票列表失败，返回错误页面
        error_html = f"""<!DOCTYPE html>
<html>
<head>
    <title>Client端错误</title>
    <meta charset="utf-8">
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/css/bootstrap.min.css" rel="stylesheet">
</head>
<body>
    <div class="container mt-4">
        <div class="alert alert-danger">
            <h4>Client端服务错误</h4>
            <p>无法初始化Client端服务: {str(e)}</p>
            <p>请检查Redis和ClickHouse连接配置</p>
        </div>
    </div>
</body>
</html>"""
        return error_html

@app.post("/api/query")
async def query_data(request: Request):
    """查询数据API"""
    try:
        data = await request.json()

        # 解析请求参数
        symbol = data.get('symbol')
        period = data.get('period')
        start_time_str = data.get('start_time')
        end_time_str = data.get('end_time')

        # 解析时间（24小时制格式：YYYY-MM-DDTHH:MM）
        start_time = datetime.fromisoformat(start_time_str)
        end_time = datetime.fromisoformat(end_time_str)

        # 执行查询
        current_service = get_service()
        result = current_service.query_bar_data(symbol, start_time, end_time, period)

        # 手动序列化数据，确保datetime正确转换
        data_list = []
        for bar in result.data:
            data_list.append({
                "symbol": bar.symbol,
                "frame": bar.frame.isoformat(),
                "open": float(bar.open),
                "high": float(bar.high),
                "low": float(bar.low),
                "close": float(bar.close),
                "vol": float(bar.vol),
                "amount": float(bar.amount)
            })

        return JSONResponse(content={
            "success": result.success,
            "message": result.message,
            "total_count": result.total_count,
            "data": data_list
        })

    except Exception as e:
        return JSONResponse(content={
            "success": False,
            "message": f"查询失败: {str(e)}",
            "total_count": 0,
            "data": []
        })

@app.get("/api/symbols")
async def get_symbols():
    """获取可用股票代码API"""
    current_service = get_service()
    return current_service.get_available_symbols()

@app.get("/api/data-status")
async def get_data_status():
    """获取数据状态API"""
    try:
        current_service = get_service()

        # 检查Redis当日数据状态
        redis_status = {}
        for period in [1, 5, 15, 30]:
            current_data_key = f"current_bar_data_{period}min"
            count = current_service.redis_manager.client.llen(current_data_key)
            redis_status[f"{period}min"] = count

        # 检查ClickHouse历史数据状态
        clickhouse_status = {}
        for period in [1, 5, 15, 30]:
            count = current_service.clickhouse_manager.get_table_count(period)
            clickhouse_status[f"{period}min"] = count

        return {
            "success": True,
            "redis_current_data": redis_status,
            "clickhouse_historical_data": clickhouse_status,
            "check_time": datetime.now().isoformat()
        }

    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }




if __name__ == "__main__":
    print("=" * 50)
    print("Client端数据查询服务")
    print("=" * 50)

    try:
        # 创建服务实例以测试连接
        print("正在初始化服务...")
        test_service = ClientDataService()
        print("✓ 服务初始化成功")

        port = WEB_PORTS['client']
        print(f"✓ 服务将在端口 {port} 启动")
        print(f"✓ 访问地址: http://localhost:{port}")
        print(f"✓ 请手动在浏览器中打开上述地址")
        print("=" * 50)

        # 启动Web服务
        uvicorn.run(app, host="0.0.0.0", port=port)

    except Exception as e:
        print(f"✗ 服务启动失败: {e}")
        print("请检查Redis和ClickHouse连接配置")
        exit(1)
