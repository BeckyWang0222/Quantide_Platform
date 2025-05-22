import time
import json
import csv
import io
import random
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from typing import List, Dict, Any, Tuple
import os

from matplotlib import font_manager
font_path = 'SimHei.ttf'  # 替换为SimHei.ttf的实际路径
font_manager.fontManager.addfont(font_path)
plt.rcParams['font.family'] = 'SimHei'

class DataFormatBenchmark:
    """SQEP数据格式性能测试：JSON vs CSV"""
    
    def __init__(self):
        """初始化基准测试"""
        # 定义SQEP-BAR-DAY字段顺序（CSV格式需要）
        self.field_order = [
            'symbol', 'frame', 'open', 'high', 'low', 
            'close', 'vol', 'amount', 'adjust'
        ]
    
    def generate_test_data(self, num_records: int) -> List[Dict[str, Any]]:
        """生成测试数据
        
        Args:
            num_records: 记录数量
            
        Returns:
            包含测试数据的记录列表
        """
        data = []
        
        # 生成股票代码 - 确保至少有1只股票
        num_symbols = max(1, min(num_records // 252, 5000))
        symbols = []
        for i in range(num_symbols):
            exchange = 'SH' if i % 2 == 0 else 'SZ'
            symbols.append(f"{str(i).zfill(6)}.{exchange}")
        
        # 生成日期范围 - 确保至少有1天
        days_needed = max(1, num_records // len(symbols))
        start_date = pd.Timestamp('2020-01-01')
        dates = [start_date + pd.Timedelta(days=i) for i in range(min(days_needed, 365))]
        
        # 生成数据
        for symbol in symbols:
            for date in dates:
                if len(data) >= num_records:
                    break
                    
                open_price = random.uniform(10, 100)
                high = open_price * random.uniform(1, 1.1)
                low = open_price * random.uniform(0.9, 1)
                close = random.uniform(low, high)
                
                data.append({
                    'symbol': symbol,
                    'frame': date.strftime('%Y-%m-%d'),
                    'open': round(open_price, 2),
                    'high': round(high, 2),
                    'low': round(low, 2),
                    'close': round(close, 2),
                    'vol': round(random.uniform(10000, 1000000), 0),
                    'amount': round(random.uniform(1000000, 100000000), 0),
                    'adjust': round(random.uniform(0.8, 1.2), 4)
                })
        
        return data[:num_records]
    
    def encode_json(self, data: List[Dict[str, Any]]) -> str:
        """将数据编码为JSON格式
        
        Args:
            data: 记录列表
            
        Returns:
            JSON字符串
        """
        return json.dumps({
            "timestamp": pd.Timestamp.now().isoformat(),
            "source": "benchmark",
            "data_type": "SQEP-BAR-DAY",
            "records": data
        })
    
    def decode_json(self, json_str: str) -> List[Dict[str, Any]]:
        """将JSON字符串解码为数据
        
        Args:
            json_str: JSON字符串
            
        Returns:
            记录列表
        """
        data = json.loads(json_str)
        return data["records"]
    
    def encode_csv(self, data: List[Dict[str, Any]]) -> str:
        """将数据编码为CSV格式
        
        Args:
            data: 记录列表
            
        Returns:
            CSV字符串
        """
        output = io.StringIO()
        writer = csv.writer(output)
        
        # 写入元数据行
        writer.writerow([
            pd.Timestamp.now().isoformat(),
            "benchmark",
            "SQEP-BAR-DAY",
            len(data)
        ])
        
        # 写入数据行
        for record in data:
            row = [record[field] for field in self.field_order]
            writer.writerow(row)
        
        return output.getvalue()
    
    def decode_csv(self, csv_str: str) -> List[Dict[str, Any]]:
        """将CSV字符串解码为数据
        
        Args:
            csv_str: CSV字符串
            
        Returns:
            记录列表
        """
        input_file = io.StringIO(csv_str)
        reader = csv.reader(input_file)
        
        # 读取元数据行
        metadata = next(reader)
        timestamp, source, data_type, num_records = metadata
        
        # 读取数据行
        records = []
        for row in reader:
            record = {field: value for field, value in zip(self.field_order, row)}
            
            # 转换数据类型
            record['open'] = float(record['open'])
            record['high'] = float(record['high'])
            record['low'] = float(record['low'])
            record['close'] = float(record['close'])
            record['vol'] = float(record['vol'])
            record['amount'] = float(record['amount'])
            record['adjust'] = float(record['adjust'])
            
            records.append(record)
        
        return records
    
    def run_encoding_benchmark(self, data: List[Dict[str, Any]], num_iterations: int = 100) -> Tuple[float, float]:
        """运行编码基准测试
        
        Args:
            data: 测试数据
            num_iterations: 迭代次数
            
        Returns:
            JSON和CSV的平均编码时间（毫秒）
        """
        json_times = []
        csv_times = []
        
        for _ in range(num_iterations):
            # 测试JSON编码
            start_time = time.time()
            json_str = self.encode_json(data)
            json_times.append(time.time() - start_time)
            
            # 测试CSV编码
            start_time = time.time()
            csv_str = self.encode_csv(data)
            csv_times.append(time.time() - start_time)
        
        # 计算平均时间（毫秒）
        json_avg = np.mean(json_times) * 1000
        csv_avg = np.mean(csv_times) * 1000
        
        return json_avg, csv_avg
    
    def run_decoding_benchmark(self, data: List[Dict[str, Any]], num_iterations: int = 100) -> Tuple[float, float]:
        """运行解码基准测试
        
        Args:
            data: 测试数据
            num_iterations: 迭代次数
            
        Returns:
            JSON和CSV的平均解码时间（毫秒）
        """
        # 先编码数据
        json_str = self.encode_json(data)
        csv_str = self.encode_csv(data)
        
        json_times = []
        csv_times = []
        
        for _ in range(num_iterations):
            # 测试JSON解码
            start_time = time.time()
            self.decode_json(json_str)
            json_times.append(time.time() - start_time)
            
            # 测试CSV解码
            start_time = time.time()
            self.decode_csv(csv_str)
            csv_times.append(time.time() - start_time)
        
        # 计算平均时间（毫秒）
        json_avg = np.mean(json_times) * 1000
        csv_avg = np.mean(csv_times) * 1000
        
        return json_avg, csv_avg
    
    def measure_size(self, data: List[Dict[str, Any]]) -> Tuple[int, int]:
        """测量编码后的数据大小
        
        Args:
            data: 测试数据
            
        Returns:
            JSON和CSV的字节大小
        """
        json_str = self.encode_json(data)
        csv_str = self.encode_csv(data)
        
        return len(json_str.encode('utf-8')), len(csv_str.encode('utf-8'))
    
    def run_full_benchmark(self, data_sizes: List[int], num_iterations: int = 100):
        """运行完整基准测试
        
        Args:
            data_sizes: 测试的记录数量列表
            num_iterations: 每次测试的迭代次数
        """
        results = {
            'data_size': [],
            'json_encode_time': [],
            'csv_encode_time': [],
            'json_decode_time': [],
            'csv_decode_time': [],
            'json_size': [],
            'csv_size': []
        }
        
        for size in data_sizes:
            print(f"测试数据量: {size}条记录")
            
            # 生成测试数据
            data = self.generate_test_data(size)
            
            # 运行编码测试
            json_encode_time, csv_encode_time = self.run_encoding_benchmark(data, num_iterations)
            
            # 运行解码测试
            json_decode_time, csv_decode_time = self.run_decoding_benchmark(data, num_iterations)
            
            # 测量数据大小
            json_size, csv_size = self.measure_size(data)
            
            # 记录结果
            results['data_size'].append(size)
            results['json_encode_time'].append(json_encode_time)
            results['csv_encode_time'].append(csv_encode_time)
            results['json_decode_time'].append(json_decode_time)
            results['csv_decode_time'].append(csv_decode_time)
            results['json_size'].append(json_size)
            results['csv_size'].append(csv_size)
            
            print(f"  编码时间 - JSON: {json_encode_time:.2f}ms, CSV: {csv_encode_time:.2f}ms")
            print(f"  解码时间 - JSON: {json_decode_time:.2f}ms, CSV: {csv_decode_time:.2f}ms")
            print(f"  数据大小 - JSON: {json_size/1024:.2f}KB, CSV: {csv_size/1024:.2f}KB")
            print(f"  性能比较 - 编码: JSON/CSV = {json_encode_time/csv_encode_time:.2f}x, 解码: JSON/CSV = {json_decode_time/csv_decode_time:.2f}x")
            print(f"  大小比较 - JSON/CSV = {json_size/csv_size:.2f}x")
            print()
        
        # 绘制结果图表
        self._plot_results(results)
        
        return results
    
    def _plot_results(self, results: dict):
        """绘制测试结果图表
        
        Args:
            results: 测试结果字典
        """
        plt.figure(figsize=(15, 12))
        
        # 编码时间对比
        plt.subplot(3, 2, 1)
        plt.plot(results['data_size'], results['json_encode_time'], 'o-', label='JSON')
        plt.plot(results['data_size'], results['csv_encode_time'], 'o-', label='CSV')
        plt.title('编码时间对比')
        plt.xlabel('数据量 (记录数)')
        plt.ylabel('平均编码时间 (毫秒)')
        plt.legend()
        plt.grid(True)
        
        # 解码时间对比
        plt.subplot(3, 2, 2)
        plt.plot(results['data_size'], results['json_decode_time'], 'o-', label='JSON')
        plt.plot(results['data_size'], results['csv_decode_time'], 'o-', label='CSV')
        plt.title('解码时间对比')
        plt.xlabel('数据量 (记录数)')
        plt.ylabel('平均解码时间 (毫秒)')
        plt.legend()
        plt.grid(True)
        
        # 数据大小对比
        plt.subplot(3, 2, 3)
        plt.plot(results['data_size'], [s/1024 for s in results['json_size']], 'o-', label='JSON')
        plt.plot(results['data_size'], [s/1024 for s in results['csv_size']], 'o-', label='CSV')
        plt.title('数据大小对比')
        plt.xlabel('数据量 (记录数)')
        plt.ylabel('数据大小 (KB)')
        plt.legend()
        plt.grid(True)
        
        # 性能比率
        plt.subplot(3, 2, 4)
        encode_ratio = [j/c for j, c in zip(results['json_encode_time'], results['csv_encode_time'])]
        decode_ratio = [j/c for j, c in zip(results['json_decode_time'], results['csv_decode_time'])]
        size_ratio = [j/c for j, c in zip(results['json_size'], results['csv_size'])]
        
        plt.plot(results['data_size'], encode_ratio, 'o-', label='编码时间比 (JSON/CSV)')
        plt.plot(results['data_size'], decode_ratio, 'o-', label='解码时间比 (JSON/CSV)')
        plt.plot(results['data_size'], size_ratio, 'o-', label='大小比 (JSON/CSV)')
        plt.axhline(y=1, color='r', linestyle='--')
        plt.title('JSON/CSV 性能比率')
        plt.xlabel('数据量 (记录数)')
        plt.ylabel('比率 (JSON/CSV)')
        plt.legend()
        plt.grid(True)
        
        # 编码+解码总时间
        plt.subplot(3, 2, 5)
        json_total = [e + d for e, d in zip(results['json_encode_time'], results['json_decode_time'])]
        csv_total = [e + d for e, d in zip(results['csv_encode_time'], results['csv_decode_time'])]
        plt.plot(results['data_size'], json_total, 'o-', label='JSON')
        plt.plot(results['data_size'], csv_total, 'o-', label='CSV')
        plt.title('总处理时间 (编码+解码)')
        plt.xlabel('数据量 (记录数)')
        plt.ylabel('总时间 (毫秒)')
        plt.legend()
        plt.grid(True)
        
        # 对数坐标下的性能
        plt.subplot(3, 2, 6)
        plt.loglog(results['data_size'], results['json_encode_time'], 'o-', label='JSON编码')
        plt.loglog(results['data_size'], results['csv_encode_time'], 'o-', label='CSV编码')
        plt.loglog(results['data_size'], results['json_decode_time'], 'o-', label='JSON解码')
        plt.loglog(results['data_size'], results['csv_decode_time'], 'o-', label='CSV解码')
        plt.title('性能随数据量变化 (对数坐标)')
        plt.xlabel('数据量 (记录数)')
        plt.ylabel('时间 (毫秒)')
        plt.legend()
        plt.grid(True)
        
        plt.tight_layout()
        plt.savefig('data_format_benchmark.png')
        plt.close()


if __name__ == "__main__":
    # 运行基准测试
    benchmark = DataFormatBenchmark()
    
    # 测试不同数据量
    data_sizes = [100, 500, 1000, 5000, 10000, 50000]
    results = benchmark.run_full_benchmark(data_sizes)
    
    # 输出总结
    print("测试总结:")
    print(f"数据量范围: {min(results['data_size'])} - {max(results['data_size'])}条记录")
    
    # 计算平均比率
    avg_encode_ratio = np.mean([j/c for j, c in zip(results['json_encode_time'], results['csv_encode_time'])])
    avg_decode_ratio = np.mean([j/c for j, c in zip(results['json_decode_time'], results['csv_decode_time'])])
    avg_size_ratio = np.mean([j/c for j, c in zip(results['json_size'], results['csv_size'])])
    
    print(f"编码时间比率 (JSON/CSV): {avg_encode_ratio:.2f}x")
    print(f"解码时间比率 (JSON/CSV): {avg_decode_ratio:.2f}x")
    print(f"数据大小比率 (JSON/CSV): {avg_size_ratio:.2f}x")
    
    print("\n测试完成，结果已保存到 data_format_benchmark.png")