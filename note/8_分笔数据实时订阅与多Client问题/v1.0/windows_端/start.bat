@echo off
echo ========================================
echo    QMT分钟线数据订阅服务启动脚本
echo ========================================

REM 检查Python环境
python --version >nul 2>&1
if errorlevel 1 (
    echo 错误: 未找到Python环境，请先安装Python
    pause
    exit /b 1
)

REM 检查配置文件
if not exist "windows_config.yaml" (
    echo 错误: 配置文件 windows_config.yaml 不存在
    pause
    exit /b 1
)

REM 安装依赖包
echo 正在安装依赖包...
pip install -r requirements.txt

REM 启动服务
echo 启动QMT分钟线数据订阅服务...
python windows_main.py

pause
