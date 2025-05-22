import redis
import json
from clickhouse_driver import Client
from datetime import datetime

# 配置参数
REDIS_HOST = "localhost"
REDIS_PORT = 6379
REDIS_QUEUE_NAME = "tushare_data_queue"

CLICKHOUSE_HOST = "localhost"
CLICKHOUSE_PORT = 9000
CLICKHOUSE_DB = "default"

# 初始化 Redis 和 ClickHouse 客户端
redis_client = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
clickhouse_client = Client(host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT, database=CLICKHOUSE_DB)

def insert_to_clickhouse(data):
    """将数据插入到 ClickHouse"""
    query = """
    INSERT INTO daily_data (ts_code, trade_date, open, high, low, close, vol, adj_factor)
    VALUES
    """
    values = []
    for record in data["ohlc_data"]:
        adj_factor_record = next((adj for adj in data["adj_factor"] if adj["trade_date"] == record["trade_date"]), None)
        adj_factor = adj_factor_record["adj_factor"] if adj_factor_record else None
        values.append((
            record["ts_code"],
            datetime.strptime(record["trade_date"], "%Y%m%d").date(),
            record["open"],
            record["high"],
            record["low"],
            record["close"],
            record["vol"],
            adj_factor
        ))
    clickhouse_client.execute(query, values)

def consume_data():
    """数据消费主函数"""
    print("启动数据消费者，等待队列数据...")
    while True:
        try:
            # 阻塞式获取队列数据
            result = redis_client.brpop(REDIS_QUEUE_NAME, timeout=1)
            if result is None:
                # 如果没有获取到数据，说明队列为空，退出循环
                print("Redis 队列为空，停止消费数据。")
                break
            _, json_data = result
            data_package = json.loads(json_data)
            insert_to_clickhouse(data_package)
            print(f"成功插入数据: {len(data_package['ohlc_data'])} 条")
        except Exception as e:
            print(f"数据处理异常: {str(e)}")
            continue

if __name__ == "__main__":
    consume_data()
