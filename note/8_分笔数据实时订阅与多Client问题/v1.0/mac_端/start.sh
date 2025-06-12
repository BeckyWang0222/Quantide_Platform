#!/bin/bash

echo "========================================"
echo "   Mac端分钟线数据处理系统启动脚本"
echo "========================================"

# 检查Python环境
if ! command -v python3 &> /dev/null; then
    echo "错误: 未找到Python3环境，请先安装Python3"
    exit 1
fi

# 检查配置文件
if [ ! -f "mac_config.yaml" ]; then
    echo "错误: 配置文件 mac_config.yaml 不存在"
    exit 1
fi

# 安装依赖包
echo "正在安装依赖包..."
pip3 install -r requirements.txt

# 检查ClickHouse连接
echo "检查ClickHouse连接..."
python3 -c "
from clickhouse_driver import Client
try:
    client = Client(host='localhost', port=9000)
    client.execute('SELECT 1')
    print('ClickHouse连接成功')
except Exception as e:
    print(f'ClickHouse连接失败: {e}')
    print('请确保ClickHouse服务已启动')
    exit(1)
"

if [ $? -ne 0 ]; then
    exit 1
fi

# 初始化数据库
echo "初始化ClickHouse数据库..."
python3 mac_main.py --init-db

# 启动服务
echo "启动Mac端分钟线数据处理系统..."
echo "可选模式:"
echo "  1. 完整模式 (数据消费 + Web服务)"
echo "  2. 仅数据消费"
echo "  3. 仅Web服务"
echo ""

read -p "请选择模式 (1-3): " choice

case $choice in
    1)
        echo "启动完整模式..."
        python3 mac_main.py --mode all
        ;;
    2)
        echo "启动数据消费模式..."
        python3 mac_main.py --mode consumer
        ;;
    3)
        echo "启动Web服务模式..."
        python3 mac_main.py --mode web
        ;;
    *)
        echo "无效选择，启动完整模式..."
        python3 mac_main.py --mode all
        ;;
esac
