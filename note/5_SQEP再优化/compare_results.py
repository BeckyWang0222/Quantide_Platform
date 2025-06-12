import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

from matplotlib import font_manager
font_path = '/Volumes/share/data/WBQ/temp/SimHei.ttf'  # 替换为SimHei.ttf的实际路径
font_manager.fontManager.addfont(font_path)
plt.rcParams['font.family'] = 'SimHei'

# 读取两个测试结果
with_meta = pd.read_csv('results/pandas_benchmark.csv')
no_meta = pd.read_csv('results/no_metadata_benchmark.csv')

# 打印比较结果
print('有元数据行 vs 无元数据行比较：\n')
for col in with_meta.columns[1:]:
    if col in no_meta.columns:
        ratio = with_meta[col].mean() / no_meta[col].mean()
        print(f'{col}比率（有/无）: {ratio:.2f}x')

# 创建比较图表
plt.figure(figsize=(15, 10))

# 序列化时间比较
plt.subplot(2, 3, 1)
plt.plot(with_meta['data_size'], with_meta['json_serialize_time'], 'b-', label='JSON(有元数据)')
plt.plot(no_meta['data_size'], no_meta['json_serialize_time'], 'b--', label='JSON(无元数据)')
plt.plot(with_meta['data_size'], with_meta['csv_serialize_time'], 'r-', label='CSV(有元数据)')
plt.plot(no_meta['data_size'], no_meta['csv_serialize_time'], 'r--', label='CSV(无元数据)')
plt.xlabel('数据量（记录数）')
plt.ylabel('时间（毫秒）')
plt.title('序列化时间比较')
plt.legend()
plt.grid(True)

# 反序列化时间比较
plt.subplot(2, 3, 2)
plt.plot(with_meta['data_size'], with_meta['json_deserialize_time'], 'b-', label='JSON(有元数据)')
plt.plot(no_meta['data_size'], no_meta['json_deserialize_time'], 'b--', label='JSON(无元数据)')
plt.plot(with_meta['data_size'], with_meta['csv_deserialize_time'], 'r-', label='CSV(有元数据)')
plt.plot(no_meta['data_size'], no_meta['csv_deserialize_time'], 'r--', label='CSV(无元数据)')
plt.xlabel('数据量（记录数）')
plt.ylabel('时间（毫秒）')
plt.title('反序列化时间比较')
plt.legend()
plt.grid(True)

# 数据大小比较
plt.subplot(2, 3, 3)
plt.plot(with_meta['data_size'], [s/1024 for s in with_meta['json_size']], 'b-', label='JSON(有元数据)')
plt.plot(no_meta['data_size'], [s/1024 for s in no_meta['json_size']], 'b--', label='JSON(无元数据)')
plt.plot(with_meta['data_size'], [s/1024 for s in with_meta['csv_size']], 'r-', label='CSV(有元数据)')
plt.plot(no_meta['data_size'], [s/1024 for s in no_meta['csv_size']], 'r--', label='CSV(无元数据)')
plt.xlabel('数据量（记录数）')
plt.ylabel('大小（KB）')
plt.title('数据大小比较')
plt.legend()
plt.grid(True)

# Redis LPUSH时间比较
plt.subplot(2, 3, 4)
plt.plot(with_meta['data_size'], with_meta['json_push_time'], 'b-', label='JSON(有元数据)')
plt.plot(no_meta['data_size'], no_meta['json_push_time'], 'b--', label='JSON(无元数据)')
plt.plot(with_meta['data_size'], with_meta['csv_push_time'], 'r-', label='CSV(有元数据)')
plt.plot(no_meta['data_size'], no_meta['csv_push_time'], 'r--', label='CSV(无元数据)')
plt.xlabel('数据量（记录数）')
plt.ylabel('时间（毫秒）')
plt.title('Redis LPUSH时间比较')
plt.legend()
plt.grid(True)

# Redis RPOP+反序列化时间比较
plt.subplot(2, 3, 5)
plt.plot(with_meta['data_size'], with_meta['json_pop_time'], 'b-', label='JSON(有元数据)')
plt.plot(no_meta['data_size'], no_meta['json_pop_time'], 'b--', label='JSON(无元数据)')
plt.plot(with_meta['data_size'], with_meta['csv_pop_time'], 'r-', label='CSV(有元数据)')
plt.plot(no_meta['data_size'], no_meta['csv_pop_time'], 'r--', label='CSV(无元数据)')
plt.xlabel('数据量（记录数）')
plt.ylabel('时间（毫秒）')
plt.title('Redis RPOP+反序列化时间比较')
plt.legend()
plt.grid(True)

# 计算性能提升比例
plt.subplot(2, 3, 6)
# 计算每个指标的有元数据/无元数据比率
metrics = ['json_serialize_time', 'csv_serialize_time', 'json_deserialize_time', 'csv_deserialize_time', 
           'json_push_time', 'csv_push_time', 'json_pop_time', 'csv_pop_time']
labels = ['JSON序列化', 'CSV序列化', 'JSON反序列化', 'CSV反序列化', 
          'JSON LPUSH', 'CSV LPUSH', 'JSON RPOP', 'CSV RPOP']
ratios = []

for metric in metrics:
    with_meta_avg = with_meta[metric].mean()
    no_meta_avg = no_meta[metric].mean()
    ratios.append(with_meta_avg / no_meta_avg)

# 绘制条形图
x = np.arange(len(labels))
plt.bar(x, ratios)
plt.axhline(y=1, color='r', linestyle='--')
plt.xticks(x, labels, rotation=45)
plt.ylabel('比率（有元数据/无元数据）')
plt.title('元数据行对性能的影响')
for i, v in enumerate(ratios):
    plt.text(i, v + 0.05, f'{v:.2f}x', ha='center')

plt.tight_layout()
plt.savefig('results/metadata_comparison.png')
plt.show()

print("\n比较结果已保存到 results/metadata_comparison.png")
