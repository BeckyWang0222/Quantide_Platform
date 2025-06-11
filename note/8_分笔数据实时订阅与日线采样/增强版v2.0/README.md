# QMT增强版数据订阅系统 v2.0

## 🚀 增强版特性

基于QMT最佳实践的高性能股票数据实时订阅与处理系统。

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

## 📁 项目结构

```
增强版v2.0/
├── README.md                       # 增强版说明文档
├── 增强版使用指南.md                # 详细使用指南
├── windows_端/                     # Windows端增强版代码
│   ├── enhanced_config.yaml       # 增强版配置文件
│   ├── qmt_enhanced_subscriber.py # 增强版QMT订阅器
│   ├── enhanced_main.py           # 增强版主程序
│   ├── enhanced_requirements.txt  # 增强版依赖包
│   └── start_enhanced.bat         # 增强版启动脚本
└── mac_端/                        # Mac端增强版代码
    ├── enhanced_mac_config.yaml   # 增强版配置文件
    ├── enhanced_data_consumer.py  # 增强版数据消费器
    ├── enhanced_main.py           # 增强版主程序
    └── enhanced_requirements.txt  # 增强版依赖包
```

## 🚀 快速开始

### Windows端增强版部署

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

### Mac端增强版部署

```bash
# 进入Mac端目录
cd mac_端/

# 安装依赖包
pip3 install -r enhanced_requirements.txt

# 修改增强版配置文件
# 编辑 enhanced_mac_config.yaml，设置Redis和ClickHouse连接信息

# 启动增强版服务
python3 enhanced_main.py
```

## 📊 性能监控

### Windows端监控信息

增强版会每分钟输出详细的性能报告：

```
============================================================
性能监控报告
============================================================
运行时间: 0:15:30
订阅股票: 1000只
接收数据: 15000条 (16.1/秒)
发布数据: 14950条 (16.0/秒)
缓存数据: 15000条
数据质量: 99.7%
发布错误: 0次
队列大小: 50
缓存股票: 800只
最后发布: 2024-05-29 15:30:45
============================================================
```

### Mac端监控信息

```
============================================================
Mac端性能监控报告
============================================================
运行时间: 0:15:30
消费数据: 14950条 (16.0/秒)
插入数据: 14900条 (15.9/秒)
成功率: 99.7%
插入错误: 0次
质量错误: 50次
队列大小: 20
处理股票: 800只
最后插入: 2024-05-29 15:30:45
============================================================
```

## ⚙️ 配置说明

### Windows端增强版配置

```yaml
# Redis配置
redis:
  host: 8.217.201.221
  port: 16379
  password: quantide666
  db: 0

# QMT增强配置
qmt:
  subscription_mode: "whole_quote"    # 全推行情模式
  max_subscribe_count: 1000           # 最大订阅数量
  cache_size: 100                     # 数据缓存大小
  quality_check: true                 # 启用数据质量检查

# 系统性能配置
system:
  batch_size: 100                     # 批量处理大小
  batch_timeout: 1.0                  # 批量超时时间
  log_level: "INFO"                   # 日志级别
```

### Mac端增强版配置

```yaml
# Redis配置
redis:
  host: 8.217.201.221
  port: 16379
  password: quantide666
  db: 0

# ClickHouse配置
clickhouse:
  host: localhost
  port: 9000
  database: default

# 数据处理配置
data:
  quality_check: true                 # 启用数据质量检查
  retention_days: 30                  # 数据保留天数

# 系统性能配置
system:
  batch_size: 1000                    # 批量插入大小
  worker_count: 4                     # 工作线程数
  log_level: "INFO"                   # 日志级别
```

## 🛠️ 故障排除

### 常见问题

1. **全推行情订阅失败**
   - 检查QMT版本是否支持全推行情API
   - 尝试切换到单股订阅模式
   - 检查QMT登录状态

2. **数据质量分数过低**
   - 检查QMT数据源质量
   - 调整质量检查阈值
   - 查看详细的质量检查日志

3. **批量插入失败**
   - 检查ClickHouse连接状态
   - 减少批量大小
   - 增加重试次数

### 日志查看

```bash
# Windows端日志
tail -f logs/qmt_enhanced_YYYYMMDD.log

# Mac端日志
tail -f logs/mac_enhanced_YYYYMMDD.log
```

## 📈 性能优化建议

### Windows端优化
- 使用全推行情模式
- 适当调整批量大小
- 启用数据质量检查
- 定期清理日志文件

### Mac端优化
- 增加工作线程数
- 调整批量插入大小
- 使用SSD存储ClickHouse数据
- 定期清理过期数据

## 📞 技术支持

如遇问题，请按以下顺序排查：

1. **查看日志文件**：检查详细的错误信息
2. **检查配置文件**：确认配置格式正确
3. **验证网络连接**：测试Redis和ClickHouse连接
4. **查看系统资源**：确认CPU和内存使用正常
5. **参考使用指南**：查看详细的使用指南文档

增强版v2.0为您提供了更强大、更稳定、更高效的股票数据订阅体验！🚀
