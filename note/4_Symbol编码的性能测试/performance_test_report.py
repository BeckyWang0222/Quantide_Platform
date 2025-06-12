# 查询性能可视化
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import time
from tqdm import tqdm
import seaborn as sns
from matplotlib.ticker import PercentFormatter
import random
import pandas as pd
import numpy as np
import redis
import json
from datetime import datetime, timedelta
import uuid
import csv
import io
from clickhouse_driver import Client

from matplotlib import font_manager
font_path = '/Volumes/share/data/WBQ/temp/SimHei.ttf'  # 替换为SimHei.ttf的实际路径
font_manager.fontManager.addfont(font_path)
plt.rcParams['font.family'] = 'SimHei'

CLICKHOUSE_HOST = "localhost"
CLICKHOUSE_PORT = 9000
CLICKHOUSE_DB = "test_data"
CLICKHOUSE_TABLE = "stock_data"

clickhouse_client = Client(host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT, database=CLICKHOUSE_DB)

def visualize_performance_results(results):
    """将性能测试结果可视化为图表"""
    if not results:
        print("没有测试结果可供可视化")
        return
    
    # 设置图表风格
    plt.figure(figsize=(20, 15))
    
    # 1. 查询时间对比 - 条形图
    plt.subplot(2, 2, 1)
    test_names = [r['name'] for r in results]
    str_times = [r['avg_str_time'] for r in results]
    int_times = [r['avg_int_time'] for r in results]
    
    x = np.arange(len(test_names))
    width = 0.35
    
    plt.bar(x - width/2, str_times, width, label='字符串查询 (symbol)', color='#3498db', 
            yerr=[r['std_str_time'] for r in results], capsize=5)
    plt.bar(x + width/2, int_times, width, label='整数查询 (symbol_int)', color='#e74c3c', 
            yerr=[r['std_int_time'] for r in results], capsize=5)
    
    plt.xlabel('查询类型', fontsize=12)
    plt.ylabel('平均查询时间 (秒)', fontsize=12)
    plt.title('不同查询类型的平均执行时间对比', fontsize=14, fontweight='bold')
    plt.xticks(x, [name if len(name) < 15 else name[:12] + '...' for name in test_names], rotation=45, ha='right')
    plt.legend(fontsize=10)
    plt.grid(True, linestyle='--', alpha=0.7)
    
    # 2. 性能提升百分比 - 水平条形图
    plt.subplot(2, 2, 2)
    improvements = [r['improvement'] for r in results]
    colors = ['#2ecc71' if imp > 0 else '#e74c3c' for imp in improvements]
    
    y_pos = np.arange(len(test_names))
    plt.barh(y_pos, improvements, color=colors)
    plt.axvline(x=0, color='black', linestyle='-', alpha=0.7)
    plt.yticks(y_pos, [name if len(name) < 15 else name[:12] + '...' for name in test_names])
    plt.xlabel('性能提升 (%)', fontsize=12)
    plt.title('整数编码相对于字符串的性能提升', fontsize=14, fontweight='bold')
    plt.grid(True, linestyle='--', alpha=0.7)
    
    # 添加数值标签
    for i, v in enumerate(improvements):
        plt.text(v + (1 if v >= 0 else -1), i, f"{v:.1f}%", 
                 va='center', fontweight='bold', color='black')
    
    # 3. 查询时间分布 - 箱线图
    plt.subplot(2, 2, 3)
    
    # 准备数据
    data_to_plot = []
    labels = []
    
    for r in results:
        data_to_plot.append(r['str_times'])
        data_to_plot.append(r['int_times'])
        labels.append(f"{r['name']} (str)")
        labels.append(f"{r['name']} (int)")
    
    # 绘制箱线图
    box = plt.boxplot(data_to_plot, patch_artist=True, labels=labels)
    
    # 设置颜色
    colors = []
    for i in range(len(data_to_plot)):
        if i % 2 == 0:  # 字符串查询
            colors.append('#3498db')
        else:  # 整数查询
            colors.append('#e74c3c')
    
    for patch, color in zip(box['boxes'], colors):
        patch.set_facecolor(color)
    
    plt.xticks(rotation=90)
    plt.ylabel('查询时间 (秒)', fontsize=12)
    plt.title('查询时间分布', fontsize=14, fontweight='bold')
    plt.grid(True, linestyle='--', alpha=0.7)
    
    # 4. 平均性能对比 - 饼图
    plt.subplot(2, 2, 4)
    
    # 计算平均性能提升
    avg_improvement = sum(improvements) / len(improvements)
    
    # 创建饼图数据
    if avg_improvement > 0:
        # 整数查询更快
        labels = ['整数查询更快', '字符串查询']
        sizes = [avg_improvement, 100 - avg_improvement]
        colors = ['#2ecc71', '#3498db']
        title = f'平均而言，整数查询比字符串查询快 {avg_improvement:.1f}%'
    else:
        # 字符串查询更快
        labels = ['字符串查询更快', '整数查询']
        sizes = [-avg_improvement, 100 + avg_improvement]
        colors = ['#3498db', '#e74c3c']
        title = f'平均而言，字符串查询比整数查询快 {-avg_improvement:.1f}%'
    
    plt.pie(sizes, labels=labels, colors=colors, autopct='%1.1f%%', 
            startangle=90, explode=(0.1, 0), shadow=True)
    plt.axis('equal')
    plt.title(title, fontsize=14, fontweight='bold')
    
    # 调整布局并保存
    plt.tight_layout()
    plt.savefig('query_performance_comparison.png', dpi=300, bbox_inches='tight')
    plt.show()
    
    print("性能测试结果图表已保存为 'query_performance_comparison.png'")
    
    # 创建详细的性能报告
    create_performance_report(results)

def create_performance_report(results):
    """创建详细的性能测试报告"""
    # 创建DataFrame
    report_data = []
    
    for r in results:
        report_data.append({
            '查询类型': r['name'],
            '字符串查询平均时间(秒)': r['avg_str_time'],
            '整数查询平均时间(秒)': r['avg_int_time'],
            '字符串查询标准差': r['std_str_time'],
            '整数查询标准差': r['std_int_time'],
            '性能提升(%)': r['improvement'],
            '字符串查询最小时间': min(r['str_times']),
            '字符串查询最大时间': max(r['str_times']),
            '整数查询最小时间': min(r['int_times']),
            '整数查询最大时间': max(r['int_times']),
        })
    
    df = pd.DataFrame(report_data)
    
    # 计算总体统计
    avg_str_time = df['字符串查询平均时间(秒)'].mean()
    avg_int_time = df['整数查询平均时间(秒)'].mean()
    avg_improvement = df['性能提升(%)'].mean()
    
    # 打印报告
    print("\n===== 性能测试详细报告 =====")
    print(f"测试场景数量: {len(results)}")
    print(f"总体平均字符串查询时间: {avg_str_time:.6f} 秒")
    print(f"总体平均整数查询时间: {avg_int_time:.6f} 秒")
    print(f"总体平均性能提升: {avg_improvement:.2f}%")
    
    # 打印每个场景的详细信息
    print("\n各场景详细数据:")
    print(df.to_string(index=False))
    
    # 保存报告到CSV
    df.to_csv('performance_test_report.csv', index=False)
    print("\n详细报告已保存到 'performance_test_report.csv'")

def run_performance_test(test_cases, num_iterations=5):
    """
    运行性能测试
    
    Args:
        test_cases: 测试用例列表，每个测试用例是一个字典，包含name, str_query和int_query
        num_iterations: 每个测试用例重复执行的次数
    
    Returns:
        测试结果列表
    """
    results = []
    
    for test_idx, test_case in enumerate(test_cases):
        str_query = test_case['str_query']
        int_query = test_case['int_query']
        test_name = test_case['name']
        
        print(f"\n测试 {test_idx+1}/{len(test_cases)}: {test_name}")
        
        str_times = []
        int_times = []
        
        for i in range(num_iterations):
            try:
                # 清除缓存
                if i == 0:  # 只在第一次迭代时清除缓存
                    try:
                        clickhouse_client.execute("SYSTEM DROP MARK CACHE")
                        clickhouse_client.execute("SYSTEM DROP UNCOMPRESSED CACHE")
                    except:
                        pass  # 忽略清除缓存的错误
                
                # 测试字符串查询
                start_time = time.time()
                clickhouse_client.execute(str_query)
                str_time = time.time() - start_time
                str_times.append(str_time)
                
                # 测试整数查询
                start_time = time.time()
                clickhouse_client.execute(int_query)
                int_time = time.time() - start_time
                int_times.append(int_time)
                
                print(f"  迭代 {i+1}/{num_iterations}: 字符串 {str_time:.6f}秒, 整数 {int_time:.6f}秒")
            except Exception as e:
                print(f"  迭代 {i+1}/{num_iterations} 出错: {str(e)}")
                print(f"  字符串查询: {str_query}")
                print(f"  整数查询: {int_query}")
                continue
        
        if not str_times or not int_times:
            print(f"  测试 {test_name} 失败，跳过")
            continue
        
        # 计算平均时间和标准差
        avg_str_time = sum(str_times) / len(str_times)
        avg_int_time = sum(int_times) / len(int_times)
        std_str_time = (sum((t - avg_str_time) ** 2 for t in str_times) / len(str_times)) ** 0.5
        std_int_time = (sum((t - avg_int_time) ** 2 for t in int_times) / len(int_times)) ** 0.5
        
        improvement = (avg_str_time - avg_int_time) / avg_str_time * 100
        
        print(f"  平均: 字符串 {avg_str_time:.6f}±{std_str_time:.6f}秒, 整数 {avg_int_time:.6f}±{std_int_time:.6f}秒")
        print(f"  性能提升: {improvement:.2f}%")
        
        results.append({
            'name': test_name,
            'str_times': str_times,
            'int_times': int_times,
            'avg_str_time': avg_str_time,
            'avg_int_time': avg_int_time,
            'std_str_time': std_str_time,
            'std_int_time': std_int_time,
            'improvement': improvement
        })
    
    return results

# 定义多种查询场景的测试用例
def create_test_cases():
    """创建多种查询场景的测试用例"""
    # 获取一些随机的股票代码用于测试
    symbols = clickhouse_client.execute("""
    SELECT DISTINCT symbol FROM stock_data 
    ORDER BY rand() 
    LIMIT 20
    """)
    
    symbol_ints = []
    for symbol_tuple in symbols:
        symbol = symbol_tuple[0]
        code, exchange = symbol.split('.')
        if exchange.upper() == 'SH':
            prefix = '1'
        elif exchange.upper() == 'SZ':
            prefix = '2'
        else:
            continue
        symbol_ints.append(int(prefix + code))
    
    # 确保我们有足够的股票代码
    if len(symbols) < 10 or len(symbol_ints) < 10:
        print("警告: 没有足够的股票代码用于测试")
        return []
    
    # 创建测试用例
    test_cases = [
        # 1. 单条记录精确查询
        {
            'name': '单条记录精确查询',
            'str_query': f"SELECT * FROM stock_data WHERE symbol = '{symbols[0][0]}' LIMIT 1000",
            'int_query': f"SELECT * FROM stock_data_with_int WHERE symbol_int = {symbol_ints[0]} LIMIT 1000"
        },
        
        # 2. 日期范围查询
        {
            'name': '日期范围查询',
            'str_query': f"SELECT * FROM stock_data WHERE symbol = '{symbols[1][0]}' AND frame BETWEEN '2016-01-01' AND '2016-12-31'",
            'int_query': f"SELECT * FROM stock_data_with_int WHERE symbol_int = {symbol_ints[1]} AND frame BETWEEN '2016-01-01' AND '2016-12-31'"
        },
        
        # 3. 批量查询 (IN条件)
        {
            'name': '批量查询 (5个股票)',
            'str_query': f"SELECT * FROM stock_data WHERE symbol IN ('{symbols[0][0]}', '{symbols[1][0]}', '{symbols[2][0]}', '{symbols[3][0]}', '{symbols[4][0]}') LIMIT 1000",
            'int_query': f"SELECT * FROM stock_data_with_int WHERE symbol_int IN ({symbol_ints[0]}, {symbol_ints[1]}, {symbol_ints[2]}, {symbol_ints[3]}, {symbol_ints[4]}) LIMIT 1000"
        },
        
        # 4. 聚合查询 (AVG)
        {
            'name': '聚合查询 (AVG)',
            'str_query': f"SELECT AVG(close) FROM stock_data WHERE symbol = '{symbols[2][0]}' GROUP BY toYYYYMM(frame)",
            'int_query': f"SELECT AVG(close) FROM stock_data_with_int WHERE symbol_int = {symbol_ints[2]} GROUP BY toYYYYMM(frame)"
        },
        
        # 5. 排序查询
        {
            'name': '排序查询',
            'str_query': f"SELECT * FROM stock_data WHERE symbol = '{symbols[3][0]}' ORDER BY frame DESC LIMIT 1000",
            'int_query': f"SELECT * FROM stock_data_with_int WHERE symbol_int = {symbol_ints[3]} ORDER BY frame DESC LIMIT 1000"
        },
        
        # 6. 复杂条件查询
        {
            'name': '复杂条件查询',
            'str_query': f"SELECT * FROM stock_data WHERE symbol = '{symbols[4][0]}' AND close > open AND vol > 1000000 LIMIT 1000",
            'int_query': f"SELECT * FROM stock_data_with_int WHERE symbol_int = {symbol_ints[4]} AND close > open AND vol > 1000000 LIMIT 1000"
        },
        
        # 7. JOIN查询
        {
            'name': 'JOIN查询',
            'str_query': f"""
            SELECT a.symbol, a.frame, a.close, b.close as prev_close
            FROM stock_data a
            LEFT JOIN stock_data b ON a.symbol = b.symbol AND b.frame = addDays(a.frame, -1)
            WHERE a.symbol = '{symbols[5][0]}'
            LIMIT 1000
            """,
            'int_query': f"""
            SELECT a.symbol_int, a.frame, a.close, b.close as prev_close
            FROM stock_data_with_int a
            LEFT JOIN stock_data_with_int b ON a.symbol_int = b.symbol_int AND b.frame = addDays(a.frame, -1)
            WHERE a.symbol_int = {symbol_ints[5]}
            LIMIT 1000
            """
        },
        
        # 8. 大批量查询 (更多股票)
        {
            'name': '大批量查询 (10个股票)',
            'str_query': "SELECT * FROM stock_data WHERE symbol IN (" + ", ".join([f"'{s[0]}'" for s in symbols[:10]]) + ") LIMIT 5000",
            'int_query': "SELECT * FROM stock_data_with_int WHERE symbol_int IN (" + ", ".join([str(s) for s in symbol_ints[:10]]) + ") LIMIT 5000"
        },
        
        # 9. 聚合查询 (COUNT)
        {
            'name': '聚合查询 (COUNT)',
            'str_query': f"SELECT COUNT(*) FROM stock_data WHERE symbol = '{symbols[6][0]}' GROUP BY toYear(frame)",
            'int_query': f"SELECT COUNT(*) FROM stock_data_with_int WHERE symbol_int = {symbol_ints[6]} GROUP BY toYear(frame)"
        },
        
        # 10. 复杂聚合查询
        {
            'name': '复杂聚合查询',
            'str_query': f"""
            SELECT 
                toYear(frame) AS year,
                AVG(close) AS avg_close,
                MAX(high) AS max_high,
                MIN(low) AS min_low,
                SUM(vol) AS total_vol
            FROM stock_data 
            WHERE symbol = '{symbols[7][0]}'
            GROUP BY year
            ORDER BY year
            """,
            'int_query': f"""
            SELECT 
                toYear(frame) AS year,
                AVG(close) AS avg_close,
                MAX(high) AS max_high,
                MIN(low) AS min_low,
                SUM(vol) AS total_vol
            FROM stock_data_with_int 
            WHERE symbol_int = {symbol_ints[7]}
            GROUP BY year
            ORDER BY year
            """
        }
    ]
    
    return test_cases


# 创建测试用例
test_cases = create_test_cases()
    
if not test_cases:
    print("无法创建测试用例，请检查数据库连接和表结构")
    exit()
    
# 运行性能测试
print(f"开始运行 {len(test_cases)} 个测试用例，每个用例重复 5 次...")
results = run_performance_test(test_cases, num_iterations=5)
    
if not results:
    print("测试失败，没有结果可供分析")
    exit()
    
# 可视化结果
visualize_performance_results(results)