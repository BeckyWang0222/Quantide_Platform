# 量化交易系统 - 分钟线数据合成与系统逻辑优化

## 系统概述

本系统是一个完整的量化交易数据处理系统，严格按照架构要求实现数据分离存储：

- **Windows端**：数据生产者，负责从QMT订阅分笔数据并合成分钟线数据，**不直接与ClickHouse交互**
- **Mac端**：数据处理者，负责将Redis队列中的历史数据存储到ClickHouse
- **Client端**：数据消费者，查询当日数据从Redis，查询历史数据从ClickHouse

## 系统架构
![9_0](https://github.com/user-attachments/assets/feb579e4-4e28-455b-a193-6777a83b8663)

```
QMT数据源 → Windows端 → Redis → Mac端 → ClickHouse
                    ↓
                Client端 ← Redis(当日) + ClickHouse(历史)
```

### 关键架构原则
- ✅ **Windows端与ClickHouse没有直接关系**
- ✅ **当日合成的分钟线数据存储在Redis中**
- ✅ **历史订阅的分钟线数据存储在ClickHouse中**
- ✅ **所有数据必须是交易时间内的数据**

## 功能特性

### Windows端功能
- 实时订阅QMT分笔数据
- 自动合成1分钟、5分钟、15分钟、30分钟线数据
- **当日合成数据**：存储到Redis（`current_bar_data_{period}min`）
- **历史数据**：发布到Redis队列（`bar_data_{period}min`）
- 交易时间自动控制
- 凌晨2点自动订阅前一天历史数据
- **🆕 历史数据获取功能**：支持时间参数控制的历史分钟线数据获取
- **🆕 交易时间验证**：确保所有数据都在交易时间内
- **❌ 不直接与ClickHouse交互**
- Web管理界面监控系统状态

### Mac端功能
- **专门处理历史数据**：只消费Redis队列中的历史分钟线数据
- 批量存储历史数据到ClickHouse
- 凌晨2点自动处理历史数据
- 自动清理Redis历史数据队列
- **🆕 手动控制历史数据处理**：支持手动启动/停止历史数据处理
- **🆕 交易时间过滤**：只存储交易时间内的历史数据到ClickHouse
- **❌ 不处理当日数据**：当日数据保留在Redis中
- Web管理界面监控存储状态

### Client端功能
- **当日数据查询**：从Redis的`current_bar_data_{period}min`获取
- **历史数据查询**：从ClickHouse的`data_bar_for_{period}min`表获取
- **智能数据合并**：自动合并当日和历史数据，避免重复
- **🆕 交易时间过滤**：查询结果自动过滤非交易时间数据
- 图表可视化展示
- 支持多个客户端同时使用

## 安装和配置

### 1. 环境要求
- Python 3.8+
- Redis服务器
- ClickHouse服务器

### 2. 安装依赖
```bash
pip install -r requirements.txt
```

### 3. 配置修改
编辑 `config.py` 文件，修改Redis和ClickHouse连接配置：

```python
# Redis配置（无需密码认证）
REDIS_CONFIG = {
    'host': '8.217.201.221',
    'port': 16379,
    'db': 0,
    'decode_responses': True
}

# ClickHouse配置
CLICKHOUSE_CONFIG = {
    'host': 'localhost',
    'port': 9000,
    'user': 'default',
    'password': '',
    'database': 'v1'
}
```

## 启动系统

### 一键启动
```bash
python start_all.py
```

### 分别启动
```bash
cd windows_端 && python main.py  # 端口8001
cd mac_端 && python main.py      # 端口8002
cd client_端 && python main.py   # 端口8003
```

### 访问界面
- Windows端：http://localhost:8001
- Mac端：http://localhost:8002
- Client端：http://localhost:8003

## 数据格式

### 分笔数据格式
```python
{
    "symbol": "000001.SZ",
    "time": "2024-01-01T09:30:00",
    "price": 10.50,
    "volume": 1000,
    "amount": 10500.0
}
```

### 分钟线数据格式
```python
{
    "symbol": "000001.SZ",
    "frame": "2024-01-01T09:30:00",
    "open": 10.50,
    "high": 10.60,
    "low": 10.45,
    "close": 10.55,
    "vol": 10000.0,
    "amount": 105000.0
}
```

## 测试系统
```bash
python test_system.py
```

## 注意事项
- 确保Redis和ClickHouse服务正常运行
- Windows端与ClickHouse没有直接关系
- 当日数据存储在Redis，历史数据存储在ClickHouse
- 所有数据必须是交易时间内的数据
