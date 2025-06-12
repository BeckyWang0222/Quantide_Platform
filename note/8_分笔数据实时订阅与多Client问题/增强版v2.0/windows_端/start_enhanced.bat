@echo off
chcp 65001 >nul
title QMT增强版数据订阅系统

echo.
echo ╔══════════════════════════════════════════════════════════════╗
echo ║                QMT增强版数据订阅系统启动脚本                    ║
echo ║                                                              ║
echo ║  基于QMT最佳实践的高性能股票数据实时订阅系统                    ║
echo ╚══════════════════════════════════════════════════════════════╝
echo.

:: 检查Python环境
echo [1/5] 检查Python环境...
python --version >nul 2>&1
if errorlevel 1 (
    echo ❌ Python未安装或未添加到PATH
    echo 请安装Python 3.8+并添加到系统PATH
    pause
    exit /b 1
)
echo ✅ Python环境正常

:: 检查虚拟环境
echo.
echo [2/5] 检查虚拟环境...
if not exist "venv" (
    echo 📦 创建虚拟环境...
    python -m venv venv
    if errorlevel 1 (
        echo ❌ 创建虚拟环境失败
        pause
        exit /b 1
    )
)

:: 激活虚拟环境
echo 🔄 激活虚拟环境...
call venv\Scripts\activate.bat
if errorlevel 1 (
    echo ❌ 激活虚拟环境失败
    pause
    exit /b 1
)
echo ✅ 虚拟环境已激活

:: 安装依赖
echo.
echo [3/5] 检查并安装依赖包...
if not exist "venv\Lib\site-packages\redis" (
    echo 📦 安装依赖包...
    pip install -r enhanced_requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple/
    if errorlevel 1 (
        echo ❌ 安装依赖包失败
        pause
        exit /b 1
    )
) else (
    echo ✅ 依赖包已安装
)

:: 检查配置文件
echo.
echo [4/5] 检查配置文件...
if not exist "enhanced_config.yaml" (
    echo ❌ 配置文件 enhanced_config.yaml 不存在
    echo 请确保配置文件存在
    pause
    exit /b 1
)
echo ✅ 配置文件正常

:: 启动程序
echo.
echo [5/5] 启动QMT增强版数据订阅系统...
echo.
echo 🚀 正在启动，请稍候...
echo 💡 按 Ctrl+C 可以停止程序
echo.

python enhanced_main.py

:: 程序结束处理
echo.
echo 📊 程序已结束
echo 📝 日志文件保存在 logs/ 目录中
echo.
pause
