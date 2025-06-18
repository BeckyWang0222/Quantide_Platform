# 日线数据定时获取系统

本系统用于定时从Tushare获取日线数据，通过Redis发布到消息队列，最终存入ClickHouse数据库。

## 功能特点

- 定时获取当日数据和历史数据
- 定期更新股票列表和交易日历
- 定期检查Redis和ClickHouse连接
- 完善的日志记录和异常处理
- 监控报警功能
- 单元测试覆盖
- 智能跳过已有数据，避免重复获取
- 检查数据完整性并自动补充不完整数据
- 分批获取数据，解决Tushare API调用限制问题

## 系统架构

系统由以下模块组成：

- **配置模块**：管理系统配置
- **数据获取模块**：从Tushare获取数据，支持分批获取和数据完整性检查
- **数据处理模块**：处理数据并发布到Redis或直接存储到ClickHouse
- **Redis处理模块**：管理Redis连接和操作
- **ClickHouse处理模块**：管理ClickHouse连接和操作，支持数据完整性检查
- **调度器模块**：管理定时任务，确保数据获取的可靠性
- **日志模块**：记录系统日志
- **监控报警模块**：监控系统运行状态并发送报警
- **异常处理模块**：定义和处理系统异常
- **工具模块**：提供通用工具函数

## 安装依赖

```bash
pip install -r requirements.txt
```

## 配置说明

系统配置存储在`config.yaml`文件中，包括以下配置项：

- **Redis配置**：Redis连接信息和队列名称
- **ClickHouse配置**：ClickHouse连接信息和表名
- **Tushare配置**：Tushare API Token和URL
- **调度器配置**：定时任务配置
- **日志配置**：日志级别和文件配置
- **监控报警配置**：报警方式和接收者配置

## 使用方法

### 启动系统

```bash
python main.py start
```

### 手动获取当日数据

```bash
# 获取当日数据
python main.py daily

# 使用分批获取，每批100个股票
python main.py daily --batch-size 100
```

### 手动获取历史数据

```bash
# 获取最近7天的历史数据
python main.py history --days 7

# 获取指定日期范围的历史数据
python main.py history --start 20230101 --end 20230107

# 强制获取所有数据，不跳过已存在的数据
python main.py history --days 7 --force

# 使用分批获取，每批100个股票
python main.py history --days 7 --batch-size 100
```

### 检查并补充不完整的数据

```bash
# 检查并补充所有不完整的数据
python main.py complete

# 检查并补充指定日期的数据
python main.py complete --date 20230101

# 检查并补充指定日期范围的数据
python main.py complete --start 20230101 --end 20230107

# 使用分批获取，每批100个股票
python main.py complete --batch-size 100
```

### 显示数据信息

```bash
# 显示ClickHouse中的数据信息（时间范围和记录数量）
python main.py info
```

### 手动更新股票列表

```bash
python main.py stock_list
```

### 手动更新交易日历

```bash
python main.py trade_cal
```

### 手动检查连接

```bash
# 检查所有连接
python main.py check

# 只检查Redis连接
python main.py check --redis

# 只检查ClickHouse连接
python main.py check --clickhouse
```

### 完成Redis到Clickhouse的数据传输

方法一（已默认实现）：
```python
# 在 main.py 的 fetch_daily_data 和 fetch_historical_data 函数中
# 直接使用：
data_processor.process_and_store(df)
```

方法二（需要创建消费者程序）：
```python
# 创建 redis_consumer.py 文件，然后运行：
python redis_consumer.py
```

## 定时任务说明

系统包含以下定时任务：

- **获取当日数据**：每个交易日收盘后（默认15:30）获取当日数据，支持分批获取和数据完整性检查
- **获取历史数据**：每天凌晨（默认01:00）获取历史数据，智能跳过已有数据
- **更新股票列表**：每月第一天（默认00:10）更新股票列表
- **更新交易日历**：每月第一天（默认00:20）更新交易日历
- **检查Redis连接**：每周一（默认07:00）检查Redis连接
- **检查ClickHouse连接**：每周一（默认07:10）检查ClickHouse连接

系统会自动检查数据完整性，并在需要时补充不完整的数据，确保每一天的日线数据都是完整的。

## 数据流程

1. 从Tushare获取日线数据（支持分批获取）
2. 处理数据，添加涨跌停和ST信息
3. 检查数据完整性，必要时补充不完整数据
4. 直接存储到ClickHouse或通过Redis中转
   - 直接存储：使用`data_processor.process_and_store(df)`
   - Redis中转：使用`data_processor.process_and_publish(df)`发布到Redis队列，然后通过消费者程序从Redis队列读取并存入ClickHouse

## 单元测试

运行单元测试：

```bash
cd tests
python -m unittest discover
```

## 日志和监控

- 日志文件存储在`logs`目录下
- 系统异常会通过邮件发送报警

## 依赖库

- pandas：数据处理
- redis：Redis客户端
- clickhouse-driver：ClickHouse客户端
- requests：HTTP请求
- pyyaml：YAML配置文件解析
- apscheduler：任务调度
- fastjson：JSON处理
- tushare：Tushare API客户端

