@echo off
echo ========================================
echo    量化交易系统 - Windows端数据生产服务
echo ========================================
echo.

cd /d "%~dp0"
cd ..

echo 检查Python环境...
python --version >nul 2>&1
if errorlevel 1 (
    echo 错误: 未找到Python，请先安装Python 3.8+
    pause
    exit /b 1
)

echo 检查依赖包...
pip show fastapi >nul 2>&1
if errorlevel 1 (
    echo 正在安装依赖包...
    pip install -r requirements.txt
    if errorlevel 1 (
        echo 错误: 依赖包安装失败
        pause
        exit /b 1
    )
)

echo 测试系统连接...
python test_system.py
if errorlevel 1 (
    echo 警告: 系统测试未完全通过，但继续启动服务
)

echo.
echo 启动Windows端数据生产服务...
echo 服务地址: http://localhost:8001
echo 按 Ctrl+C 停止服务
echo.

cd windows_端
python main.py

echo.
echo 服务已停止
pause
