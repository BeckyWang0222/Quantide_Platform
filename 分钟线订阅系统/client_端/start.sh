#!/bin/bash

echo "========================================"
echo "   量化交易系统 - Client端数据查询服务"
echo "========================================"
echo

cd "$(dirname "$0")"
cd ..

echo "检查Python环境..."
if ! command -v python3 &> /dev/null; then
    echo "错误: 未找到Python3，请先安装Python 3.8+"
    echo "Linux: sudo apt install python3 python3-pip"
    echo "macOS: brew install python"
    exit 1
fi

echo "Python版本: $(python3 --version)"

echo "检查依赖包..."
if ! python3 -c "import fastapi" &> /dev/null; then
    echo "正在安装依赖包..."
    pip3 install -r requirements.txt
    if [ $? -ne 0 ]; then
        echo "尝试使用国内镜像源..."
        pip3 install -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple/
        if [ $? -ne 0 ]; then
            echo "错误: 依赖包安装失败"
            exit 1
        fi
    fi
fi

echo "测试Redis连接..."
python3 -c "
import sys
sys.path.append('.')
try:
    from database import RedisManager
    redis_manager = RedisManager()
    redis_manager.get_system_info()
    print('✓ Redis连接正常')
except Exception as e:
    print(f'⚠ Redis连接失败: {e}')
    print('请检查Redis服务器配置')
"

echo
echo "启动Client端数据查询服务..."
echo "服务地址: http://localhost:8003"
echo "按 Ctrl+C 停止服务"
echo

cd client_端
python3 main.py

echo
echo "服务已停止"
