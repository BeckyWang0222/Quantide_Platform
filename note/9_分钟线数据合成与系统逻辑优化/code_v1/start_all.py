# -*- coding: utf-8 -*-
"""
一键启动所有服务的脚本
"""
import subprocess
import sys
import time
import webbrowser
from config import WEB_PORTS


def start_service(service_name, script_path, port):
    """启动单个服务"""
    print(f"正在启动 {service_name}...")
    try:
        # 启动服务进程
        process = subprocess.Popen([
            sys.executable, script_path
        ], cwd=f"{service_name}")
        
        print(f"✓ {service_name} 启动成功 (PID: {process.pid})")
        print(f"  访问地址: http://localhost:{port}")
        
        return process
    except Exception as e:
        print(f"✗ {service_name} 启动失败: {e}")
        return None


def main():
    """主函数"""
    print("=" * 60)
    print("量化交易系统 - 一键启动脚本")
    print("=" * 60)
    
    # 检查依赖
    print("检查系统依赖...")
    try:
        import fastapi
        import redis
        import clickhouse_connect
        print("✓ 所有依赖已安装")
    except ImportError as e:
        print(f"✗ 缺少依赖: {e}")
        print("请运行: pip install -r requirements.txt")
        return
    
    # 启动各个服务
    services = [
        ("windows_端", "windows_端/main.py", WEB_PORTS['windows']),
        ("mac_端", "mac_端/main.py", WEB_PORTS['mac']),
        ("client_端", "client_端/main.py", WEB_PORTS['client'])
    ]
    
    processes = []
    
    for service_name, script_path, port in services:
        process = start_service(service_name, script_path, port)
        if process:
            processes.append((service_name, process, port))
        time.sleep(2)  # 等待2秒再启动下一个服务
    
    if not processes:
        print("✗ 没有服务启动成功")
        return
    
    print("\n" + "=" * 60)
    print("所有服务启动完成！")
    print("=" * 60)
    
    for service_name, process, port in processes:
        print(f"{service_name}: http://localhost:{port}")
    
    print("\n提示:")
    print("- Windows端: 数据生产和管理")
    print("- Mac端: 数据处理和存储")
    print("- Client端: 数据查询和展示")
    
    # 自动打开浏览器
    print("\n正在打开浏览器...")
    time.sleep(3)
    
    for service_name, process, port in processes:
        webbrowser.open(f"http://localhost:{port}")
        time.sleep(1)
    
    print("\n按 Ctrl+C 停止所有服务")
    
    try:
        # 等待用户中断
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n正在停止所有服务...")
        for service_name, process, port in processes:
            try:
                process.terminate()
                print(f"✓ {service_name} 已停止")
            except:
                print(f"✗ {service_name} 停止失败")
        
        print("所有服务已停止")


if __name__ == "__main__":
    main()
