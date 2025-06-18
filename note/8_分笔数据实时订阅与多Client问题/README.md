# 分钟线数据实时订阅与日线采样系统 v2.0

## 🚀 v2.0 增强版特性

### 核心改进
- **全推行情订阅**：使用 `subscribe_whole_quote` API，支持全市场实时数据订阅
- **数据质量检查**：智能数据质量评分和过滤机制
- **批量处理优化**：多线程批量数据处理，显著提升性能
- **实时监控系统**：完善的性能监控和资源管理
- **自动数据清理**：智能数据保留和清理策略

### 性能提升
- **10x 数据处理速度**：批量处理 + 多线程优化
- **智能缓存机制**：减少重复计算和网络请求
- **资源自适应**：根据系统资源自动调整处理策略
- **错误自恢复**：完善的重试和恢复机制

### 新增功能
- **增强版订阅器**：`qmt_enhanced_subscriber.py` - 基于最佳实践的高性能订阅
- **增强版消费器**：`enhanced_data_consumer.py` - 多线程批量数据处理
- **性能监控面板**：实时系统资源和性能监控
- **数据质量评分**：智能数据质量检查和评分系统

## 项目概述

本项目是一个跨平台的股票分钟线数据实时订阅与处理系统，通过Windows电脑上的QMT接口实时获取股票分钟线数据，经过远程Redis消息队列缓冲，最终在Mac电脑上通过ClickHouse数据库进行存储和查询。同时提供Web可视化界面，支持多种时间周期的数据访问和图表展示。

**v2.0版本基于QMT最佳实践进行了全面优化，性能和稳定性大幅提升。**

## 系统架构

```
Windows电脑(QMT) → 远程Redis(8.217.201.221:16379) → Mac电脑(ClickHouse) → Web可视化界面
       ↓                    ↓                           ↓                ↓
   实时数据订阅          跨平台数据缓冲              持久化存储与查询      图表展示与交互
```

## 核心功能

- **全市场数据订阅**：Windows电脑通过QMT接口订阅全市场股票分钟线数据
- **智能股票列表管理**：支持全市场、文件配置、手动指定三种股票列表模式
- **跨平台数据传输**：通过远程Redis消息队列实现Windows到Mac的数据传输
- **多周期数据支持**：支持1分钟、5分钟、30分钟、日线等多种时间周期
- **物化视图优化**：利用ClickHouse物化视图实现数据聚合和查询优化
- **任意股票查询**：支持查询任意股票代码的历史和实时数据
- **Web可视化界面**：提供简洁易用的Web查询和图表展示界面
- **股票代码搜索**：支持股票代码搜索、验证和可用代码浏览
- **灵活查询SDK**：提供Python SDK支持程序化数据访问

## 项目结构

```
note/8_分钟线数据实时订阅与日线采样/
├── README.md                           # 项目说明文档
├── 8_分钟线数据实时订阅与日线采样.md      # 技术方案文档
├── 8_代码说明.md                       # 代码详细说明文档
├── windows_端/                         # Windows端代码
│   ├── windows_config.yaml            # 基础版配置文件
│   ├── qmt_subscriber.py              # 基础版QMT数据订阅器
│   ├── windows_main.py                # 基础版主程序
│   ├── enhanced_config.yaml           # 🆕 增强版配置文件
│   ├── qmt_enhanced_subscriber.py     # 🆕 增强版QMT订阅器
│   ├── enhanced_main.py               # 🆕 增强版主程序
│   ├── enhanced_requirements.txt      # 🆕 增强版依赖包
│   ├── start_enhanced.bat             # 🆕 增强版启动脚本
│   ├── test_qmt_api.py                # QMT API测试脚本
│   ├── requirements.txt               # 基础版依赖包
│   └── start.bat                      # 基础版启动脚本
└── mac_端/                            # Mac端代码
    ├── mac_config.yaml                # 基础版配置文件
    ├── mac_data_consumer.py           # 基础版数据消费器
    ├── enhanced_mac_config.yaml       # 🆕 增强版配置文件
    ├── enhanced_data_consumer.py      # 🆕 增强版数据消费器
    ├── enhanced_main.py               # 🆕 增强版主程序
    ├── mac_market_data_sdk.py         # 市场数据查询SDK
    ├── web_interface.py               # Web可视化界面
    ├── init_clickhouse.sql            # ClickHouse初始化SQL
    ├── mac_main.py                    # 基础版主程序
    ├── requirements.txt               # 依赖包
    ├── start.sh                       # 启动脚本
    └── templates/
        └── index.html                 # Web界面模板
```

## 快速开始

### 🚀 推荐：使用增强版 v2.0

#### Windows端增强版部署

```bash
# 进入Windows端目录
cd windows_端/

# 安装增强版依赖包
pip install -r enhanced_requirements.txt

# 修改增强版配置文件
# 编辑 enhanced_config.yaml，设置订阅模式和Redis连接信息

# 启动增强版服务
start_enhanced.bat
```

#### Mac端增强版部署

```bash
# 进入Mac端目录
cd mac_端/

# 安装依赖包
pip3 install -r requirements.txt

# 安装ClickHouse (使用Homebrew)
brew install clickhouse

# 启动ClickHouse服务
brew services start clickhouse

# 修改增强版配置文件
# 编辑 enhanced_mac_config.yaml，设置Redis和ClickHouse连接信息

# 启动增强版服务
python3 enhanced_main.py
```

### 环境要求

**Windows端：**
- Windows 10/11
- Python 3.8+
- QMT量化交易平台
- 网络连接到Redis服务器

**Mac端：**
- macOS 10.15+
- Python 3.8+
- ClickHouse数据库
- 网络连接到Redis服务器

### 基础版安装步骤

#### 1. Windows端部署

```bash
# 进入Windows端目录
cd windows_端/

# 安装依赖包
pip install -r requirements.txt

# 修改配置文件
# 编辑 windows_config.yaml，设置股票列表和Redis连接信息

# 启动服务
start.bat
```

#### 2. Mac端部署

```bash
# 进入Mac端目录
cd mac_端/

# 安装依赖包
pip3 install -r requirements.txt

# 安装ClickHouse (使用Homebrew)
brew install clickhouse

# 启动ClickHouse服务
brew services start clickhouse

# 修改配置文件
# 编辑 mac_config.yaml，设置Redis和ClickHouse连接信息

# 启动服务
chmod +x start.sh
./start.sh
```

### 使用说明

#### Windows端使用

1. 确保QMT量化交易平台已安装并登录
2. 修改 `windows_config.yaml` 中的股票列表
3. 运行 `start.bat` 启动数据订阅服务
4. 查看日志文件确认数据正常发送到Redis

#### Mac端使用

1. 确保ClickHouse服务正常运行
2. 运行启动脚本，选择运行模式：
   - 完整模式：数据消费 + Web服务
   - 仅数据消费：只消费Redis数据存储到ClickHouse
   - 仅Web服务：只提供Web查询界面

3. 访问Web界面：http://localhost:5000

#### Web界面功能

- **智能股票查询**：支持任意股票代码查询，带搜索建议功能
- **股票代码验证**：验证股票代码是否有效和可查询
- **可用代码浏览**：查看所有系统中有数据的股票代码
- **K线图表**：显示股票价格走势的专业K线图
- **成交量图表**：显示对应的成交量柱状图
- **实时数据**：获取最新的股票价格信息
- **系统统计**：查看数据库中的数据统计信息
- **响应式设计**：适配不同设备和屏幕尺寸

## 配置说明

### Windows端配置 (windows_config.yaml)

```yaml
redis:
  host:     # Redis服务器地址
  port:             # Redis端口
  password: null         # Redis密码
  db: 0                  # Redis数据库编号

qmt:
  stock_list_mode: "all" # 股票列表模式: "all"(全市场), "file"(文件), "config"(配置)

  # 全市场模式配置
  markets:               # 订阅的市场
    - "SZ"              # 深圳市场
    - "SH"              # 上海市场
  stock_types:           # 股票类型
    - "stock"           # 股票
    - "index"           # 指数
  max_subscribe_count: 3000  # 最大订阅数量限制

  # 手动配置模式 (当mode为"config"时使用)
  manual_stock_list:     # 手动指定的股票列表
    - "000001.SZ"
    - "000002.SZ"
    - "600519.SH"

system:
  batch_size: 100        # 批量处理大小
  publish_interval: 1    # 发布间隔(秒)
```

### Mac端配置 (mac_config.yaml)

```yaml
redis:
  host: 8.217.201.221    # Redis服务器地址
  port: 16379            # Redis端口

clickhouse:
  host: localhost        # ClickHouse地址
  port: 9000            # ClickHouse端口
  database: market_data  # 数据库名称

web:
  host: 0.0.0.0         # Web服务地址
  port: 5000            # Web服务端口

system:
  batch_size: 1000      # 批量插入大小
  consumer_threads: 4   # 消费线程数
```

## 数据结构

### 分钟线数据结构
```python
{
    "symbol": "000001.SZ",           # 股票代码
    "frame": "2024-01-01 09:30:00",  # 时间戳
    "open": 10.50,                   # 开盘价
    "high": 10.80,                   # 最高价
    "low": 10.40,                    # 最低价
    "close": 10.70,                  # 收盘价
    "vol": 1000000,                  # 成交量
    "amount": 10700000               # 成交额
}
```

### 日线数据结构
```python
{
    "symbol": "000001.SZ",           # 股票代码
    "frame": "2024-01-01",           # 交易日期
    "open": 10.50,                   # 开盘价
    "high": 10.80,                   # 最高价
    "low": 10.40,                    # 最低价
    "close": 10.70,                  # 收盘价
    "vol": 100000000,               # 成交量
    "amount": 1070000000,            # 成交额
    "adjust": 1.0,                   # 复权因子
    "st": false,                     # 是否ST股票
    "limit_up": 11.55,               # 涨停价
    "limit_down": 9.45               # 跌停价
}
```

## 故障排除

### 常见问题

1. **Redis连接失败**
   - 检查网络连接
   - 确认Redis服务器地址和端口
   - 检查防火墙设置

2. **ClickHouse连接失败**
   - 确认ClickHouse服务已启动
   - 检查端口9000是否被占用
   - 查看ClickHouse日志

3. **QMT数据订阅失败**
   - 确认QMT已登录
   - 检查股票代码格式
   - 查看QMT API文档

4. **Web界面无法访问**
   - 检查端口5000是否被占用
   - 确认Flask服务已启动
   - 查看浏览器控制台错误

### 日志查看

- Windows端日志：`windows_端/logs/qmt_subscriber.log`
- Mac端日志：`mac_端/logs/mac_system.log`

## 技术支持

如有问题，请查看：
1. 项目技术方案文档：`8_分钟线数据实时订阅与日线采样.md`
2. 代码详细说明：`8_代码说明.md`
3. 系统日志文件

## 许可证

本项目仅供学习和研究使用。
