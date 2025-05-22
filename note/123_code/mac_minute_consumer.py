import redis
import json
from clickhouse_driver import Client
from datetime import datetime
import time

# Redis配置
REDIS_HOST = "localhost"  # 本地Redis或Windows的IP
REDIS_PORT = 6379
REDIS_QUEUE_NAME = "qmt_minute_queue"
REDIS_PASSWORD = None  # 如果有密码，请设置

# ClickHouse配置
CLICKHOUSE_HOST = "localhost"
CLICKHOUSE_PORT = 9000
CLICKHOUSE_DB = "default"
CLICKHOUSE_USER = "default"
CLICKHOUSE_PASSWORD = ""

def setup_redis_client():
    """初始化Redis客户端"""
    return redis.StrictRedis(
        host=REDIS_HOST, 
        port=REDIS_PORT, 
        password=REDIS_PASSWORD,
        decode_responses=True
    )

def setup_clickhouse_client():
    """初始化ClickHouse客户端"""
    return Client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        database=CLICKHOUSE_DB,
        user=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD
    )

def insert_to_clickhouse(client, data):
    """将分钟线数据插入到ClickHouse"""
    if not data["minute_data"]:
        print("没有数据需要插入")
        return 0
    
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
        client.execute(query, values)
        return len(values)
    return 0

def main():
    """主函数"""
    # 初始化客户端
    redis_client = setup_redis_client()
    clickhouse_client = setup_clickhouse_client()
    
    print("启动分钟线数据消费者，等待队列数据...")
    
    try:
        while True:
            # 尝试从Redis获取数据
            result = redis_client.brpop(REDIS_QUEUE_NAME, timeout=1)
            
            if result is None:
                print("Redis队列为空，等待新数据...")
                time.sleep(5)  # 等待5秒再次尝试
                continue
            
            # 解析数据
            _, json_data = result
            data_package = json.loads(json_data)
            
            # 插入ClickHouse
            inserted_count = insert_to_clickhouse(clickhouse_client, data_package)
            print(f"成功插入分钟线数据: {data_package['ts_code']} - {data_package['trade_date']} ({inserted_count}条)")
    
    except KeyboardInterrupt:
        print("程序被手动中断")
    
    except Exception as e:
        print(f"程序执行异常: {str(e)}")
    
    finally:
        print("程序执行完毕")

if __name__ == "__main__":
    main()