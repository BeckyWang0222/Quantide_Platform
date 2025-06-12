import json
import fast_json
import csv
import time
import io
import os
import redis
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from typing import List, Dict, Any, Tuple
from datetime import datetime, timedelta

from matplotlib import font_manager
font_path = '/Volumes/share/data/WBQ/temp/SimHei.ttf'  # 替换为SimHei.ttf的实际路径
font_manager.fontManager.addfont(font_path)
plt.rcParams['font.family'] = 'SimHei'

# 配置参数
REDIS_HOST = "localhost"
REDIS_PORT = 6379
REDIS_PASSWORD = "quantide666"  # Redis密码
REDIS_DB = 0
REDIS_QUEUE_JSON = 'benchmark:json:batch_size'
REDIS_QUEUE_CSV = 'benchmark:csv:batch_size'

# 总数据量
TOTAL_RECORDS = 1000000  # 减少数据量，避免测试时间过长

class BatchSizeBenchmark:
    """不同批处理大小下JSON和CSV格式性能对比基准测试"""

    def __init__(self):
        """初始化基准测试"""
        # 设置字段顺序，按照要求的数据结构
        self.field_order = ['symbol', 'frame', 'open', 'high', 'low', 'close', 'vol', 'amount', 'adjust']
        self.redis_client = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PASSWORD, decode_responses=True)

        # 确保输出目录存在
        os.makedirs('results', exist_ok=True)

    def generate_dataframe(self, num_records: int) -> pd.DataFrame:
        """生成测试用的pandas DataFrame，符合指定的数据结构

        Args:
            num_records: 记录数量

        Returns:
            pandas DataFrame
        """
        # 生成股票代码（1或2开头的六位数整数）
        # 生成1或2作为第一位
        first_digits = np.random.choice([1, 2], num_records)
        # 生成剩余5位数字（范围00000-99999）
        remaining_digits = np.random.randint(0, 100000, num_records)
        # 组合成六位数整数
        symbols = first_digits * 100000 + remaining_digits

        # 生成交易日期（datetime.date类型）
        base_date = datetime.now().date()
        frame_dates = []
        for i in range(num_records):
            # 随机生成过去100天内的日期
            days_ago = np.random.randint(0, 100)
            date = base_date - timedelta(days=days_ago)
            frame_dates.append(date)

        # 生成基础价格
        base_prices = np.random.uniform(10, 100, num_records)

        # 生成高低价格
        high_prices = base_prices * (1 + np.random.uniform(0, 0.05, num_records))
        low_prices = base_prices * (1 - np.random.uniform(0, 0.05, num_records))

        # 生成开盘和收盘价格
        open_prices = np.random.uniform(low_prices, high_prices)
        close_prices = np.random.uniform(low_prices, high_prices)

        # 生成成交量和成交额
        volumes = np.random.uniform(10000, 1000000, num_records)
        amounts = np.random.uniform(100000, 10000000, num_records)

        # 生成复权因子
        adjust_factors = np.random.uniform(0.9, 1.1, num_records)

        # 创建DataFrame，确保数据类型符合要求
        df = pd.DataFrame({
            'symbol': symbols.astype(np.int32),
            'frame': frame_dates,
            'open': np.round(open_prices, 2).astype(np.float64),
            'high': np.round(high_prices, 2).astype(np.float64),
            'low': np.round(low_prices, 2).astype(np.float64),
            'close': np.round(close_prices, 2).astype(np.float64),
            'vol': np.round(volumes, 0).astype(np.float64),
            'amount': np.round(amounts, 0).astype(np.float64),
            'adjust': np.round(adjust_factors, 4).astype(np.float64)
        })

        return df

    def serialize_json(self, df: pd.DataFrame) -> str:
        """JSON序列化（使用fast_json，带key，无元数据）

        Args:
            df: pandas DataFrame

        Returns:
            JSON字符串
        """
        # 创建DataFrame的副本，避免修改原始数据
        df_copy = df.copy()

        # 将日期转换为字符串
        if 'frame' in df_copy.columns:
            df_copy['frame'] = df_copy['frame'].astype(str)

        # 转换DataFrame为记录列表
        records = df_copy.to_dict('records')

        # 直接序列化记录列表，不添加元数据
        return fast_json.dumps(records)

    def deserialize_json(self, json_str: str) -> pd.DataFrame:
        """JSON反序列化（使用fast_json，带key，无元数据）

        Args:
            json_str: JSON字符串

        Returns:
            pandas DataFrame
        """
        print("反序列化开始")
        # 直接解析为记录列表
        records = fast_json.loads(json_str)
        df = pd.DataFrame(records)

        # 将字符串日期转换回datetime.date对象
        if 'frame' in df.columns:
            df['frame'] = pd.to_datetime(df['frame']).dt.date
        print("反序列化结束")

        return df

    def serialize_csv(self, df: pd.DataFrame) -> str:
        """CSV序列化（使用pandas，无key，无元数据）

        Args:
            df: pandas DataFrame

        Returns:
            CSV字符串
        """
        # 确保列顺序一致
        if not df.empty:
            df = df[self.field_order]

        # 转换为CSV字符串（不包含索引和列名）
        output = io.StringIO()
        df.to_csv(output, index=False, header=False)
        return output.getvalue()

    def deserialize_csv(self, csv_str: str) -> pd.DataFrame:
        """CSV反序列化（使用pandas，无key，无元数据）

        Args:
            csv_str: CSV字符串

        Returns:
            pandas DataFrame
        """
        # 读取CSV字符串到DataFrame（无列名）
        df = pd.read_csv(io.StringIO(csv_str), header=None, names=self.field_order)
        return df

    def benchmark_redis_operations(self, df: pd.DataFrame, batch_size: int, iterations: int = 1) -> Dict[str, float]:
        """Redis操作性能测试

        Args:
            df: pandas DataFrame
            batch_size: 批处理大小
            iterations: 迭代次数

        Returns:
            测试结果
        """
        results = {
            'json_push_time': 0,
            'csv_push_time': 0,
            'json_pop_time': 0,
            'csv_pop_time': 0
        }

        # 清空队列
        self.redis_client.delete(REDIS_QUEUE_JSON)
        self.redis_client.delete(REDIS_QUEUE_CSV)

        # 准备批次数据
        # dfs = []
        # for i in range(0, len(df), batch_size):
        #     dfs.append(df.iloc[i:i+batch_size])

        # JSON LPUSH测试
        json_push_times = []
        for _ in range(iterations):
            self.redis_client.delete(REDIS_QUEUE_JSON)
            start_time = time.time()
            for i in range(0, len(df), batch_size):
                # json_str = self.serialize_json(df.iloc[i:i+batch_size])
                # json_str = fast_json.dump(df.iloc[i:i+batch_size])
                json_str = df.iloc[i:i+batch_size].to_json()
                self.redis_client.lpush(REDIS_QUEUE_JSON, json_str)
                
                if i % 1000 == 0:
                    print(f"已推送json {i} 条记录")
                    # print(time.time() - start_time)

            json_push_times.append(time.time() - start_time)

        results['json_push_time'] = sum(json_push_times) / iterations * 1000  # 毫秒

        # CSV LPUSH测试
        csv_push_times = []
        for _ in range(iterations):
            self.redis_client.delete(REDIS_QUEUE_CSV)
            start_time = time.time()

            for i in range(0, len(df), batch_size):
                # 直接序列化DataFrame为CSV，无元数据行
                csv_str = self.serialize_csv(df.iloc[i:i+batch_size])
                #csv_str = df.iloc[i:i+batch_size].to_csv
                self.redis_client.lpush(REDIS_QUEUE_CSV, csv_str)
                if i % 1000 == 0:
                    print(f"已推送csv {i} 条记录")

            csv_push_times.append(time.time() - start_time)

        results['csv_push_time'] = sum(csv_push_times) / iterations * 1000  # 毫秒

        # JSON RPOP测试
        json_pop_times = []
        for _ in range(iterations):
            # 确保队列有数据
            if self.redis_client.llen(REDIS_QUEUE_JSON) == 0:
                for i in range(0, len(df), batch_size):
                    # json_str = self.serialize_json(df.iloc[i:i+batch_size])
                    json_str = df.iloc[i:i+batch_size].to_json()
                    self.redis_client.lpush(REDIS_QUEUE_JSON, json_str)
                    if i % 1000 == 0:
                        print(f"已消费json {i} 条记录")

            start_time = time.time()

            while self.redis_client.llen(REDIS_QUEUE_JSON) > 0:
                json_str = self.redis_client.rpop(REDIS_QUEUE_JSON)
                if json_str:
                    self.deserialize_json(json_str)

            json_pop_times.append(time.time() - start_time)

        results['json_pop_time'] = sum(json_pop_times) / iterations * 1000  # 毫秒

        # CSV RPOP测试
        csv_pop_times = []
        for _ in range(iterations):
            # 确保队列有数据
            if self.redis_client.llen(REDIS_QUEUE_CSV) == 0:
                for i in range(0, len(df), batch_size):
                    # 直接序列化DataFrame为CSV，无元数据行
                    csv_str = self.serialize_csv(df.iloc[i:i+batch_size])
                    # csv_str = df.iloc[i:i+batch_size].to_csv()
                    self.redis_client.lpush(REDIS_QUEUE_CSV, csv_str)
                    if i % 1000 == 0:
                        print(f"已消费csv {i} 条记录")

            start_time = time.time()

            while self.redis_client.llen(REDIS_QUEUE_CSV) > 0:
                csv_str = self.redis_client.rpop(REDIS_QUEUE_CSV)
                if csv_str:
                    self.deserialize_csv(csv_str)

            csv_pop_times.append(time.time() - start_time)

        results['csv_pop_time'] = sum(csv_pop_times) / iterations * 1000  # 毫秒

        return results

    def run_batch_size_benchmark(self, batch_sizes: List[int], iterations: int = 1):
        """运行不同批处理大小的基准测试

        Args:
            batch_sizes: 测试的批处理大小列表
            iterations: 每次测试的迭代次数
        """
        results = {
            'batch_size': [],
            'json_push_time': [],
            'csv_push_time': [],
            'json_pop_time': [],
            'csv_pop_time': [],
            'json_push_ops': [],  # 每秒操作数
            'csv_push_ops': [],
            'json_pop_ops': [],
            'csv_pop_ops': []
        }

        # 生成测试数据
        print(f"正在生成{TOTAL_RECORDS}条记录的DataFrame...")
        df = self.generate_dataframe(TOTAL_RECORDS)
        print(f"成功生成DataFrame，形状: {df.shape}")

        for batch_size in batch_sizes:
            print(f"\n测试批处理大小: {batch_size}")

            # Redis操作测试
            redis_results = self.benchmark_redis_operations(df, batch_size, iterations)

            # 计算每秒操作数
            json_push_ops = TOTAL_RECORDS / (redis_results['json_push_time'] / 1000)  # 每秒记录数
            csv_push_ops = TOTAL_RECORDS / (redis_results['csv_push_time'] / 1000)
            json_pop_ops = TOTAL_RECORDS / (redis_results['json_pop_time'] / 1000)
            csv_pop_ops = TOTAL_RECORDS / (redis_results['csv_pop_time'] / 1000)

            print(f"  Redis LPUSH时间 - JSON: {redis_results['json_push_time']:.2f}ms, CSV: {redis_results['csv_push_time']:.2f}ms")
            print(f"  Redis RPOP+反序列化时间 - JSON: {redis_results['json_pop_time']:.2f}ms, CSV: {redis_results['csv_pop_time']:.2f}ms")
            print(f"  每秒操作数 - JSON LPUSH: {json_push_ops:.2f} ops/s, CSV LPUSH: {csv_push_ops:.2f} ops/s")
            print(f"  每秒操作数 - JSON RPOP: {json_pop_ops:.2f} ops/s, CSV RPOP: {csv_pop_ops:.2f} ops/s")

            # 计算比率
            push_ratio = redis_results['json_push_time'] / redis_results['csv_push_time']
            pop_ratio = redis_results['json_pop_time'] / redis_results['csv_pop_time']

            print(f"  Redis比较 - LPUSH: JSON/CSV = {push_ratio:.2f}x, RPOP+反序列化: JSON/CSV = {pop_ratio:.2f}x")

            # 记录结果
            results['batch_size'].append(batch_size)
            results['json_push_time'].append(redis_results['json_push_time'])
            results['csv_push_time'].append(redis_results['csv_push_time'])
            results['json_pop_time'].append(redis_results['json_pop_time'])
            results['csv_pop_time'].append(redis_results['csv_pop_time'])
            results['json_push_ops'].append(json_push_ops)
            results['csv_push_ops'].append(csv_push_ops)
            results['json_pop_ops'].append(json_pop_ops)
            results['csv_pop_ops'].append(csv_pop_ops)

        # 绘制结果图表
        self._plot_results(results)

        return results

    def _plot_results(self, results):
        """绘制结果图表

        Args:
            results: 测试结果
        """
        # 创建DataFrame
        df = pd.DataFrame(results)

        # 保存为CSV
        df.to_csv('results/batch_size_benchmark.csv', index=False)

        # 绘制Redis操作时间对比图
        plt.figure(figsize=(15, 10))

        plt.subplot(2, 2, 1)
        plt.plot(results['batch_size'], results['json_push_time'], 'b-', label='JSON')
        plt.plot(results['batch_size'], results['csv_push_time'], 'r-', label='CSV')
        plt.xlabel('批处理大小')
        plt.ylabel('时间（毫秒）')
        plt.title('Redis LPUSH时间对比')
        plt.legend()
        plt.grid(True)
        plt.xscale('log')  # 使用对数刻度

        plt.subplot(2, 2, 2)
        plt.plot(results['batch_size'], results['json_pop_time'], 'b-', label='JSON')
        plt.plot(results['batch_size'], results['csv_pop_time'], 'r-', label='CSV')
        plt.xlabel('批处理大小')
        plt.ylabel('时间（毫秒）')
        plt.title('Redis RPOP+反序列化时间对比')
        plt.legend()
        plt.grid(True)
        plt.xscale('log')  # 使用对数刻度

        plt.subplot(2, 2, 3)
        plt.plot(results['batch_size'], [j/c for j, c in zip(results['json_push_time'], results['csv_push_time'])], 'g-')
        plt.xlabel('批处理大小')
        plt.ylabel('比率（JSON/CSV）')
        plt.title('Redis LPUSH时间比率（JSON/CSV）')
        plt.axhline(y=1, color='r', linestyle='--')
        plt.grid(True)
        plt.xscale('log')  # 使用对数刻度

        plt.subplot(2, 2, 4)
        plt.plot(results['batch_size'], [j/c for j, c in zip(results['json_pop_time'], results['csv_pop_time'])], 'g-')
        plt.xlabel('批处理大小')
        plt.ylabel('比率（JSON/CSV）')
        plt.title('Redis RPOP+反序列化时间比率（JSON/CSV）')
        plt.axhline(y=1, color='r', linestyle='--')
        plt.grid(True)
        plt.xscale('log')  # 使用对数刻度

        plt.tight_layout()
        plt.savefig('results/batch_size_time_benchmark.png')

        # 绘制每秒操作数对比图
        plt.figure(figsize=(15, 10))

        plt.subplot(2, 2, 1)
        plt.plot(results['batch_size'], results['json_push_ops'], 'b-', label='JSON')
        plt.plot(results['batch_size'], results['csv_push_ops'], 'r-', label='CSV')
        plt.xlabel('批处理大小')
        plt.ylabel('每秒操作数')
        plt.title('Redis LPUSH每秒操作数对比')
        plt.legend()
        plt.grid(True)
        plt.xscale('log')  # 使用对数刻度

        plt.subplot(2, 2, 2)
        plt.plot(results['batch_size'], results['json_pop_ops'], 'b-', label='JSON')
        plt.plot(results['batch_size'], results['csv_pop_ops'], 'r-', label='CSV')
        plt.xlabel('批处理大小')
        plt.ylabel('每秒操作数')
        plt.title('Redis RPOP+反序列化每秒操作数对比')
        plt.legend()
        plt.grid(True)
        plt.xscale('log')  # 使用对数刻度

        plt.subplot(2, 2, 3)
        plt.plot(results['batch_size'], [j/c for j, c in zip(results['json_push_ops'], results['csv_push_ops'])], 'g-')
        plt.xlabel('批处理大小')
        plt.ylabel('比率（JSON/CSV）')
        plt.title('Redis LPUSH每秒操作数比率（JSON/CSV）')
        plt.axhline(y=1, color='r', linestyle='--')
        plt.grid(True)
        plt.xscale('log')  # 使用对数刻度

        plt.subplot(2, 2, 4)
        plt.plot(results['batch_size'], [j/c for j, c in zip(results['json_pop_ops'], results['csv_pop_ops'])], 'g-')
        plt.xlabel('批处理大小')
        plt.ylabel('比率（JSON/CSV）')
        plt.title('Redis RPOP+反序列化每秒操作数比率（JSON/CSV）')
        plt.axhline(y=1, color='r', linestyle='--')
        plt.grid(True)
        plt.xscale('log')  # 使用对数刻度

        plt.tight_layout()
        plt.savefig('results/batch_size_ops_benchmark.png')

        # 绘制批处理大小对性能的影响
        plt.figure(figsize=(15, 10))

        # 标准化数据（相对于最大值）
        max_json_push_ops = max(results['json_push_ops'])
        max_csv_push_ops = max(results['csv_push_ops'])
        max_json_pop_ops = max(results['json_pop_ops'])
        max_csv_pop_ops = max(results['csv_pop_ops'])

        norm_json_push_ops = [x/max_json_push_ops for x in results['json_push_ops']]
        norm_csv_push_ops = [x/max_csv_push_ops for x in results['csv_push_ops']]
        norm_json_pop_ops = [x/max_json_pop_ops for x in results['json_pop_ops']]
        norm_csv_pop_ops = [x/max_csv_pop_ops for x in results['csv_pop_ops']]

        plt.subplot(2, 1, 1)
        plt.plot(results['batch_size'], norm_json_push_ops, 'b-', label='JSON LPUSH')
        plt.plot(results['batch_size'], norm_csv_push_ops, 'r-', label='CSV LPUSH')
        plt.plot(results['batch_size'], norm_json_pop_ops, 'b--', label='JSON RPOP')
        plt.plot(results['batch_size'], norm_csv_pop_ops, 'r--', label='CSV RPOP')
        plt.xlabel('批处理大小')
        plt.ylabel('标准化性能（相对于最大值）')
        plt.title('批处理大小对性能的影响（标准化）')
        plt.legend()
        plt.grid(True)
        plt.xscale('log')  # 使用对数刻度

        # 找出每种操作的最佳批处理大小
        best_json_push = results['batch_size'][results['json_push_ops'].index(max_json_push_ops)]
        best_csv_push = results['batch_size'][results['csv_push_ops'].index(max_csv_push_ops)]
        best_json_pop = results['batch_size'][results['json_pop_ops'].index(max_json_pop_ops)]
        best_csv_pop = results['batch_size'][results['csv_pop_ops'].index(max_csv_pop_ops)]

        # 绘制条形图
        plt.subplot(2, 1, 2)
        x = np.arange(4)
        best_sizes = [best_json_push, best_csv_push, best_json_pop, best_csv_pop]
        plt.bar(x, best_sizes)
        plt.xticks(x, ['JSON LPUSH', 'CSV LPUSH', 'JSON RPOP', 'CSV RPOP'])
        plt.ylabel('最佳批处理大小')
        plt.title('各操作的最佳批处理大小')
        for i, v in enumerate(best_sizes):
            plt.text(i, v + 0.1, str(v), ha='center')

        plt.tight_layout()
        plt.savefig('results/batch_size_optimal_benchmark.png')


# 主函数
if __name__ == "__main__":
    # 设置中文字体
    try:
        import matplotlib
        matplotlib.rcParams['font.sans-serif'] = ['SimHei', 'DejaVu Sans', 'Arial', 'sans-serif']
        matplotlib.rcParams['axes.unicode_minus'] = False  # 解决负号显示问题
    except Exception as e:
        print(f"设置中文字体失败: {e}")
        print("图表中的中文可能无法正确显示")

    # 创建基准测试实例
    benchmark = BatchSizeBenchmark()

    # 测试不同批处理大小
    batch_sizes = [1, 10, 100, 1000, 10000]

    # 运行基准测试
    benchmark.run_batch_size_benchmark(batch_sizes, iterations=1)

    print("基准测试完成，结果已保存到results目录")
