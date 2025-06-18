# 日线与分钟线实时订阅系统

该项目为 **《21 天驯化 AI 打工仔：开发量化交易系统》** 系列推文的配套代码。

### 🌟 欢迎关注 Quantide 全网平台～  
- 🔗 匡醍量化/大富翁量化网站：[点击进入](https://www.jieyu.ai)
- 📕 小红书👌：@[Quantide](https://www.xiaohongshu.com/discovery/item/651addf1000000001f0074b5?source=webshare&xhsshare=pc_web&xsec_token=ABeAHOCQcco8m84uBGCeT8LddQx97KMlv_yNl2fQg9gdo=&xsec_source=pc_share)（点击头像直达）  
- 📃 微信公众号：搜索「[量化风云](https://mp.weixin.qq.com/template/article/1750215662/index.html)」
- 🎬 微信视频号：@量化风云

### 📚 系列推文链接
- [（一）实现 Redis 消息队列 和 从 Tushare 获取 OHLC 数据](https://mp.weixin.qq.com/s/CK4n-sqeSCQ7ymR09e92ig)
- [（二）ClickHouse 数据库的基本实现](https://mp.weixin.qq.com/s/blG9mpCGwtxE8Mqh-oRiqg)
- [（三）数据库的优化设计与分钟线的获取](https://mp.weixin.qq.com/s/XKHznc40vFLAhAtHR-hwKw)
- [（四）通用的数据交换格式 SQEP 与性能测试](https://mp.weixin.qq.com/s/8cU1uNMknCVaDuVeXIdHYg)
- [（五） SQEP 的性能再优化](https://mp.weixin.qq.com/s/1YMfaaekdk0eRK1F1oV-mw)
- [（六） 日线数据的定时获取系统（基本架构实现）](https://mp.weixin.qq.com/s/igl1Ay7mQbYcy5sFbdXWOw)
- [（七）日线数据定时获取系统（字段修复与完善）](https://mp.weixin.qq.com/s/T719nCS6cZOUpDca6Wz7dA)
- [（八）QMT实时分笔数据订阅系统与多 Client 问题](https://mp.weixin.qq.com/s/fTkzKuKLqOpQUieCuKvREQ)
- [（九）系统逻辑优化与分钟线数据合成](https://mp.weixin.qq.com/s/kOEH_9oVrWsq-iitmmG6PQ)

## 📁 项目文件夹架构

```
Quantide_Platform/
├── README.md                           # 项目总体说明文档
├── requirements.txt                    # 项目依赖包列表
├── images/                            # 项目相关图片资源
│   └── 分钟线架构.png                  # 分钟线系统架构图
│
├── note/                              # 系列推文对应的笔记和文档
│   ├── 21天驯化AI.md                   # 系列总览文档
│   ├── 1_Redis消息队列与Tushare数据获取/
│   ├── 2_ClickHouse数据库/
│   ├── 3_数据库的优化设计/
│   ├── 4_Symbol编码的性能测试/
│   ├── 5_SQEP再优化/
│   ├── 6_日线数据的定时获取系统（基本架构实现）/
│   ├── 7_日线数据的定时获取系统（字段修复）/
│   ├── 8_分笔数据实时订阅与多Client问题/
│   ├── 8_分钟线数据实时订阅与日线采样/
│   └── 9_分钟线数据合成与系统逻辑优化/
│
├── 日线数据实时订阅系统/                # 日线数据获取系统（推文六、七）
│   ├── README.md                      # 系统说明文档
│   ├── main.py                        # 主程序入口
│   ├── config.py                      # 配置文件
│   ├── scheduler.py                   # 定时调度器
│   ├── data_fetcher.py                # 数据获取模块
│   ├── data_processor.py              # 数据处理模块
│   ├── redis_handler.py               # Redis操作模块
│   ├── clickhouse_handler.py          # ClickHouse操作模块
│   ├── monitor.py                     # 系统监控模块
│   ├── logger.py                      # 日志管理模块
│   ├── utils.py                       # 工具函数
│   ├── requirements.txt               # 依赖包列表
│   ├── logs/                          # 日志文件目录
│   └── tests/                         # 测试文件目录
│
└──  分钟线订阅系统/                     # 分钟线数据订阅系统（推文八、九）
    ├── README.md                      # 系统说明文档
    ├── config.py                      # 配置文件
    ├── models.py                      # 数据模型
    ├── data_processor.py              # 数据处理器
    ├── database.py                    # 数据库操作
    ├── trading_time_validator.py      # 交易时间验证
    ├── qmt_historical_fetcher.py      # QMT历史数据获取
    ├── start_all.py                   # 启动脚本
    ├── test_system.py                 # 系统测试
    ├── requirements.txt               # 依赖包列表
    ├── windows_端/                    # Windows端组件
    ├── mac_端/                        # Mac端组件
    └── client_端/                     # 客户端组件
```

每个系统都有独立的`README.md`文档，包含详细的安装、配置和使用说明。

---
### 本地代码地址说明
- 日线实时订阅系统：
    - 代码 以及 Clickhouse 存储的机器：APPLE PRO
    - 代码路径：/Users/quantide/workspace/WBQ/quantide/日线数据定时获取
    - 运行环境：`cd /Users/quantide/workspace/WBQ/quantide/`,`source .venv/bin/activate`就能启动 quantide 环境
- 分钟线实时订阅系统：
    - Windows 端
        - Redis 的机器是 Windows （QMT）
        - 代码路径：`C:\wbq\mycode`
        - 监控：http://localhost:8001
    - Mac 端
        - Clickhouse 存储的机器：APPLE PRO
        - 代码路径：`cd /Users/quantide/workspace/WBQ/quantide/分钟线数据实时订阅与日线采样/mac_端`
        - 监控：http://localhost:8002
