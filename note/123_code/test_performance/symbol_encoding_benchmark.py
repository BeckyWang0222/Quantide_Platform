import time
import random
import pandas as pd
import numpy as np
import sqlite3
import matplotlib.pyplot as plt
from typing import List, Tuple
import os

from matplotlib import font_manager
font_path = 'SimHei.ttf'  # 替换为SimHei.ttf的实际路径
font_manager.fontManager.addfont(font_path)
plt.rcParams['font.family'] = 'SimHei'

class SymbolEncodingBenchmark:
    """股票代码编码方式性能测试"""
    
    def __init__(self, db_path="symbol_benchmark.db"):
        """初始化基准测试
        
        Args:
            db_path: SQLite数据库文件路径
        """
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.cursor = self.conn.cursor()
        
        # 创建测试表
        self._create_tables()
        
    def _create_tables(self):
        """创建测试表"""
        # 字符串格式表
        self.cursor.execute("""
        CREATE TABLE IF NOT EXISTS bar_day_str (
            symbol TEXT,
            frame TEXT,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            vol REAL,
            amount REAL,
            adjust REAL,
            PRIMARY KEY (symbol, frame)
        )
        """)
        
        # 整型编码表
        self.cursor.execute("""
        CREATE TABLE IF NOT EXISTS bar_day_int (
            symbol INTEGER,
            frame TEXT,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            vol REAL,
            amount REAL,
            adjust REAL,
            PRIMARY KEY (symbol, frame)
        )
        """)
        
        # 创建索引
        self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_str_symbol ON bar_day_str (symbol)")
        self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_int_symbol ON bar_day_int (symbol)")
        
        self.conn.commit()
    
    @staticmethod
    def encode_symbol(symbol: str) -> int:
        """将字符串格式的股票代码转换为整型编码"""
        code, exchange = symbol.split('.')
        code = code.lstrip('0')  # 移除前导零
        if not code:
            code = '0'
            
        if exchange.upper() == 'SH':
            prefix = '1'
        elif exchange.upper() == 'SZ':
            prefix = '2'
        else:
            raise ValueError(f"不支持的交易所: {exchange}")
            
        return int(prefix + code)
    
    def generate_test_data(self, num_symbols: int, days_per_symbol: int) -> pd.DataFrame:
        """生成测试数据
        
        Args:
            num_symbols: 股票数量
            days_per_symbol: 每只股票的交易日数量
            
        Returns:
            包含测试数据的DataFrame
        """
        # 生成股票代码
        sh_symbols = [f"{str(i).zfill(6)}.SH" for i in range(num_symbols // 2)]
        sz_symbols = [f"{str(i).zfill(6)}.SZ" for i in range(num_symbols // 2)]
        symbols = sh_symbols + sz_symbols
        
        # 生成日期范围
        start_date = pd.Timestamp('2020-01-01')
        dates = [start_date + pd.Timedelta(days=i) for i in range(days_per_symbol)]
        
        # 生成数据
        data = []
        for symbol in symbols:
            for date in dates:
                open_price = random.uniform(10, 100)
                high = open_price * random.uniform(1, 1.1)
                low = open_price * random.uniform(0.9, 1)
                close = random.uniform(low, high)
                
                data.append({
                    'symbol': symbol,
                    'frame': date.strftime('%Y-%m-%d'),
                    'open': open_price,
                    'high': high,
                    'low': low,
                    'close': close,
                    'vol': random.uniform(10000, 1000000),
                    'amount': random.uniform(1000000, 100000000),
                    'adjust': random.uniform(0.8, 1.2)
                })
        
        return pd.DataFrame(data)
    
    def load_test_data(self, df: pd.DataFrame):
        """加载测试数据到数据库
        
        Args:
            df: 包含测试数据的DataFrame
        """
        # 清空表
        self.cursor.execute("DELETE FROM bar_day_str")
        self.cursor.execute("DELETE FROM bar_day_int")
        
        # 插入字符串格式数据
        for _, row in df.iterrows():
            self.cursor.execute(
                "INSERT INTO bar_day_str VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    row['symbol'],
                    row['frame'],
                    row['open'],
                    row['high'],
                    row['low'],
                    row['close'],
                    row['vol'],
                    row['amount'],
                    row['adjust']
                )
            )
        
        # 插入整型编码数据
        for _, row in df.iterrows():
            encoded_symbol = self.encode_symbol(row['symbol'])
            self.cursor.execute(
                "INSERT INTO bar_day_int VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    encoded_symbol,
                    row['frame'],
                    row['open'],
                    row['high'],
                    row['low'],
                    row['close'],
                    row['vol'],
                    row['amount'],
                    row['adjust']
                )
            )
        
        self.conn.commit()
    
    def run_query_benchmark(self, num_queries: int) -> Tuple[List[float], List[float]]:
        """运行查询基准测试
        
        Args:
            num_queries: 查询次数
            
        Returns:
            字符串格式和整型编码的查询时间列表
        """
        # 获取所有股票代码
        self.cursor.execute("SELECT DISTINCT symbol FROM bar_day_str")
        str_symbols = [row[0] for row in self.cursor.fetchall()]
        
        self.cursor.execute("SELECT DISTINCT symbol FROM bar_day_int")
        int_symbols = [row[0] for row in self.cursor.fetchall()]
        
        # 运行查询测试
        str_times = []
        int_times = []
        
        for _ in range(num_queries):
            # 随机选择一个股票代码
            str_symbol = random.choice(str_symbols)
            int_symbol = self.encode_symbol(str_symbol)
            
            # 测试字符串格式查询
            start_time = time.time()
            self.cursor.execute(
                "SELECT * FROM bar_day_str WHERE symbol = ?",
                (str_symbol,)
            )
            results = self.cursor.fetchall()
            str_times.append(time.time() - start_time)
            
            # 测试整型编码查询
            start_time = time.time()
            self.cursor.execute(
                "SELECT * FROM bar_day_int WHERE symbol = ?",
                (int_symbol,)
            )
            results = self.cursor.fetchall()
            int_times.append(time.time() - start_time)
        
        return str_times, int_times
    
    def run_range_query_benchmark(self, num_queries: int) -> Tuple[List[float], List[float]]:
        """运行范围查询基准测试
        
        Args:
            num_queries: 查询次数
            
        Returns:
            字符串格式和整型编码的查询时间列表
        """
        # 获取所有交易所
        exchanges = ['SH', 'SZ']
        
        # 运行查询测试
        str_times = []
        int_times = []
        
        for _ in range(num_queries):
            # 随机选择一个交易所
            exchange = random.choice(exchanges)
            
            # 测试字符串格式查询
            start_time = time.time()
            self.cursor.execute(
                "SELECT * FROM bar_day_str WHERE symbol LIKE ?",
                (f"%.{exchange}",)
            )
            results = self.cursor.fetchall()
            str_times.append(time.time() - start_time)
            
            # 测试整型编码查询
            prefix = 1 if exchange == 'SH' else 2
            start_time = time.time()
            self.cursor.execute(
                "SELECT * FROM bar_day_int WHERE symbol >= ? AND symbol < ?",
                (prefix * 1000000, (prefix + 1) * 1000000)
            )
            results = self.cursor.fetchall()
            int_times.append(time.time() - start_time)
        
        return str_times, int_times
    
    def run_full_benchmark(self, data_sizes: List[int], days_per_symbol: int = 252, num_queries: int = 100):
        """运行完整基准测试
        
        Args:
            data_sizes: 测试的股票数量列表
            days_per_symbol: 每只股票的交易日数量
            num_queries: 每次测试的查询次数
        """
        results = {
            'data_size': [],
            'str_query_avg': [],
            'int_query_avg': [],
            'str_range_avg': [],
            'int_range_avg': []
        }
        
        for size in data_sizes:
            print(f"测试数据量: {size}只股票 × {days_per_symbol}天 = {size * days_per_symbol}条记录")
            
            # 生成并加载测试数据
            df = self.generate_test_data(size, days_per_symbol)
            self.load_test_data(df)
            
            # 运行查询测试
            str_times, int_times = self.run_query_benchmark(num_queries)
            str_range_times, int_range_times = self.run_range_query_benchmark(num_queries)
            
            # 记录结果
            results['data_size'].append(size * days_per_symbol)
            results['str_query_avg'].append(np.mean(str_times) * 1000)  # 转换为毫秒
            results['int_query_avg'].append(np.mean(int_times) * 1000)
            results['str_range_avg'].append(np.mean(str_range_times) * 1000)
            results['int_range_avg'].append(np.mean(int_range_times) * 1000)
            
            print(f"  单条查询 - 字符串格式: {results['str_query_avg'][-1]:.2f}ms, 整型编码: {results['int_query_avg'][-1]:.2f}ms")
            print(f"  范围查询 - 字符串格式: {results['str_range_avg'][-1]:.2f}ms, 整型编码: {results['int_range_avg'][-1]:.2f}ms")
            print(f"  性能提升 - 单条查询: {(results['str_query_avg'][-1] / results['int_query_avg'][-1]):.2f}x, 范围查询: {(results['str_range_avg'][-1] / results['int_range_avg'][-1]):.2f}x")
            print()
        
        # 绘制结果图表
        self._plot_results(results)
        
        return results
    
    def _plot_results(self, results: dict):
        """绘制测试结果图表
        
        Args:
            results: 测试结果字典
        """
        plt.figure(figsize=(15, 10))
        
        # 单条查询性能对比
        plt.subplot(2, 2, 1)
        plt.plot(results['data_size'], results['str_query_avg'], 'o-', label='字符串格式')
        plt.plot(results['data_size'], results['int_query_avg'], 'o-', label='整型编码')
        plt.title('单条查询性能对比')
        plt.xlabel('数据量 (记录数)')
        plt.ylabel('平均查询时间 (毫秒)')
        plt.legend()
        plt.grid(True)
        
        # 范围查询性能对比
        plt.subplot(2, 2, 2)
        plt.plot(results['data_size'], results['str_range_avg'], 'o-', label='字符串格式')
        plt.plot(results['data_size'], results['int_range_avg'], 'o-', label='整型编码')
        plt.title('范围查询性能对比')
        plt.xlabel('数据量 (记录数)')
        plt.ylabel('平均查询时间 (毫秒)')
        plt.legend()
        plt.grid(True)
        
        # 性能提升比例
        plt.subplot(2, 2, 3)
        speedup_query = [s / i for s, i in zip(results['str_query_avg'], results['int_query_avg'])]
        speedup_range = [s / i for s, i in zip(results['str_range_avg'], results['int_range_avg'])]
        plt.plot(results['data_size'], speedup_query, 'o-', label='单条查询')
        plt.plot(results['data_size'], speedup_range, 'o-', label='范围查询')
        plt.title('整型编码性能提升比例')
        plt.xlabel('数据量 (记录数)')
        plt.ylabel('性能提升 (倍)')
        plt.legend()
        plt.grid(True)
        
        # 查询时间与数据量的关系
        plt.subplot(2, 2, 4)
        plt.loglog(results['data_size'], results['str_query_avg'], 'o-', label='字符串-单条')
        plt.loglog(results['data_size'], results['int_query_avg'], 'o-', label='整型-单条')
        plt.loglog(results['data_size'], results['str_range_avg'], 'o-', label='字符串-范围')
        plt.loglog(results['data_size'], results['int_range_avg'], 'o-', label='整型-范围')
        plt.title('查询时间与数据量关系 (对数坐标)')
        plt.xlabel('数据量 (记录数)')
        plt.ylabel('平均查询时间 (毫秒)')
        plt.legend()
        plt.grid(True)
        
        plt.tight_layout()
        plt.savefig('symbol_encoding_benchmark.png')
        plt.close()
    
    def cleanup(self):
        """清理测试资源"""
        self.conn.close()
        if os.path.exists(self.db_path):
            os.remove(self.db_path)


if __name__ == "__main__":
    # 运行基准测试
    benchmark = SymbolEncodingBenchmark()
    
    # 测试不同数据量
    data_sizes = [100, 500, 1000, 2000, 5000]
    results = benchmark.run_full_benchmark(data_sizes)
    
    # 输出总结
    print("测试总结:")
    print(f"数据量范围: {min(results['data_size'])} - {max(results['data_size'])}条记录")
    print(f"单条查询平均性能提升: {np.mean([s / i for s, i in zip(results['str_query_avg'], results['int_query_avg'])]):.2f}倍")
    print(f"范围查询平均性能提升: {np.mean([s / i for s, i in zip(results['str_range_avg'], results['int_range_avg'])]):.2f}倍")
    
    # 清理资源
    benchmark.cleanup()
    
    print("\n测试完成，结果已保存到 symbol_encoding_benchmark.png")