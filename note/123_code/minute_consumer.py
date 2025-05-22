import redis
import json
from clickhouse_driver import Client
from datetime import datetime

# 配置参数
REDIS_HOST = "localhost"
REDIS_PORT = 6379
REDIS_QUEUE_NAME = "qmt_minute_queue"

CLICKHOUSE_HOST = "localhost"
CLICKHOUSE_PORT = 9000
CLICKHOUSE_DB = "default"

# 初始化客户端连接
redis_client = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
clickhouse_client = Client(host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT, database=CLICKHOUSE_DB)

def insert_to_clickhouse(data):
    """将分钟线数据插入到 ClickHouse"""
    query = """
    INSERT INTO minute_data 
    (ts_code, trade_time, open, high, low, close, vol, amount)
    VALUES
    """
    values = []
    for record in data["minute_data"]:
        values.append((
            record["ts_code"],
            datetime.strptime(record["trade_time"], "%Y-%m-%d %H:%M:%S"),
            record["open"],
            record["high"],
            record["low"],
            record["close"],
            record["vol"],
            record["amount"]
        ))
    
    if values:
        clickhouse_client.execute(query, values)

def consume_minute_data():
    """消费分钟线数据"""
    print("启动分钟线数据消费者，等待队列数据...")
    while True:
        try:
            result = redis_client.brpop(REDIS_QUEUE_NAME, timeout=1)
            if result is None:
                print("Redis 队列为空，停止消费数据。")
                break

            _, json_data = result
            data_package = json.loads(json_data)
            insert_to_clickhouse(data_package)
            print(f"成功插入分钟线数据: {data_package['ts_code']} - {data_package['trade_date']}")
        
        except Exception as e:
            print(f"数据处理异常: {str(e)}")
            continue

if __name__ == "__main__":
    consume_minute_data()