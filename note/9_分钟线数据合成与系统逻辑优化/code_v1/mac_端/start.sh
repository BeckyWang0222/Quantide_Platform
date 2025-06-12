#!/bin/bash

echo "========================================"
echo "   量化交易系统 - Mac端数据处理服务"
echo "========================================"
echo

cd "$(dirname "$0")"
cd ..

echo "检查Python环境..."
if ! command -v python3 &> /dev/null; then
    echo "错误: 未找到Python3，请先安装Python 3.8+"
    echo "可以使用: brew install python"
    exit 1
fi

echo "Python版本: $(python3 --version)"

echo "检查依赖包..."
if ! python3 -c "import fastapi" &> /dev/null; then
    echo "正在安装依赖包..."
    pip3 install -r requirements.txt
    if [ $? -ne 0 ]; then
        echo "错误: 依赖包安装失败"
        exit 1
    fi
fi

echo "测试系统连接..."
python3 test_system.py
if [ $? -ne 0 ]; then
    echo "警告: 系统测试未完全通过，但继续启动服务"
fi

echo
echo "启动Mac端数据处理服务..."
echo "服务地址: http://localhost:8002"
echo "按 Ctrl+C 停止服务"
echo

cd mac_端
python3 main.py

echo
echo "服务已停止"
