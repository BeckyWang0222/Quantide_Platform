import redis
import json
from clickhouse_driver import Client
from datetime import datetime
from typing import Dict, List, Any

# 配置参数
REDIS_HOST = "8.217.201.221"
REDIS_PORT = 16379
REDIS_PASSWORD = "quantide666"  # 添加Redis密码
REDIS_QUEUE_NAME = "sqep_bar_day_queue"

CLICKHOUSE_HOST = "localhost"
CLICKHOUSE_PORT = 9000
CLICKHOUSE_DB = "default"

# 初始化 Redis 和 ClickHouse 客户端
redis_client = redis.StrictRedis(
    host=REDIS_HOST, 
    port=REDIS_PORT, 
    password=REDIS_PASSWORD,  # 使用密码进行身份验证
    decode_responses=True
)
clickhouse_client = Client(host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT, database=CLICKHOUSE_DB)

def create_sqep_table_if_not_exists():
    """创建SQEP-BAR-DAY表（如果不存在）"""
    query = """
    CREATE TABLE IF NOT EXISTS sqep_bar_day (
        symbol Int32,
        frame Date,
        open Float64,
        high Float64,
        low Float64,
        close Float64,
        vol Float64,
        amount Float64,
        adjust Float64,
        st UInt8 DEFAULT 0,
        buy_limit Float64 DEFAULT 0,
        sell_limit Float64 DEFAULT 0
    ) ENGINE = MergeTree()
    PARTITION BY toYYYYMM(frame)
    ORDER BY (symbol, frame);
    """
    clickhouse_client.execute(query)
    print("已确保SQEP-BAR-DAY表存在")

def decode_symbol(encoded_symbol: int) -> str:
    """将整型编码的股票代码转换回字符串格式
    
    Args:
        encoded_symbol: 整型编码的股票代码，如 2000001
        
    Returns:
        字符串格式的股票代码，如 '000001.SZ'
    """
    encoded_str = str(encoded_symbol)
    prefix = encoded_str[0]
    code = encoded_str[1:]
    
    # 补齐6位数字
    code = code.zfill(6)
    
    if prefix == '1':
        exchange = 'SH'
    elif prefix == '2':
        exchange = 'SZ'
    else:
        raise ValueError(f"不支持的交易所前缀: {prefix}")
        
    return f"{code}.{exchange}"

def insert_to_clickhouse(data_package: Dict[str, Any]):
    """将SQEP-BAR-DAY数据插入到ClickHouse
    
    Args:
        data_package: 包含SQEP-BAR-DAY记录的数据包
    """
    records = data_package["records"]
    if not records:
        return 0
    
    # 准备插入数据
    values = []
    for record in records:
        # 准备基本字段
        row = (
            record["symbol"],
            datetime.fromisoformat(record["frame"]).date(),
            record["open"],
            record["high"],
            record["low"],
            record["close"],
            record["vol"],
            record["amount"],
            record["adjust"],
            int(record.get("st", False)),
            record.get("buy_limit", 0.0),
            record.get("sell_limit", 0.0)
        )
        values.append(row)
    
    # 执行插入
    query = """
    INSERT INTO sqep_bar_day (
        symbol, frame, open, high, low, close, vol, amount, adjust, st, buy_limit, sell_limit
    ) VALUES
    """
    
    clickhouse_client.execute(query, values)
    return len(values)

def consume_sqep_data():
    """消费SQEP-BAR-DAY数据"""
    # 确保表存在
    create_sqep_table_if_not_exists()
    
    print("启动SQEP-BAR-DAY数据消费者，等待队列数据...")
    while True:
        try:
            # 阻塞式获取队列数据
            result = redis_client.brpop(REDIS_QUEUE_NAME, timeout=1)
            if result is None:
                # 如果没有获取到数据，说明队列为空，退出循环
                print("Redis队列为空，停止消费数据。")
                break
            
            _, json_data = result
            data_package = json.loads(json_data)
            
            # 检查数据类型
            if data_package.get("data_type") != "SQEP-BAR-DAY":
                print(f"跳过非SQEP-BAR-DAY数据: {data_package.get('data_type')}")
                continue
            
            # 插入数据
            inserted_count = insert_to_clickhouse(data_package)
            
            # 获取第一条记录的股票代码用于显示
            if data_package["records"]:
                first_symbol = data_package["records"][0]["symbol"]
                symbol_str = decode_symbol(first_symbol)
                print(f"成功插入SQEP-BAR-DAY数据: {symbol_str} ({inserted_count}条)")
            else:
                print("数据包中没有记录")
                
        except Exception as e:
            print(f"数据处理异常: {str(e)}")
            continue

def query_sqep_data(symbol: str, start_date: str, end_date: str):
    """查询SQEP-BAR-DAY数据
    
    Args:
        symbol: 股票代码，如 '000001.SZ'
        start_date: 开始日期，格式为YYYY-MM-DD
        end_date: 结束日期，格式为YYYY-MM-DD
        
    Returns:
        查询结果列表
    """
    # 编码股票代码
    code, exchange = symbol.split('.')
    code = code.lstrip('0')
    if not code:
        code = '0'
        
    if exchange.upper() == 'SH':
        prefix = '1'
    elif exchange.upper() == 'SZ':
        prefix = '2'
    else:
        raise ValueError(f"不支持的交易所: {exchange}")
        
    encoded_symbol = int(prefix + code)
    
    # 执行查询
    query = f"""
    SELECT 
        symbol, frame, open, high, low, close, vol, amount, adjust, 
        st, buy_limit, sell_limit
    FROM sqep_bar_day
    WHERE symbol = {encoded_symbol} AND frame BETWEEN '{start_date}' AND '{end_date}'
    ORDER BY frame
    """
    
    result = clickhouse_client.execute(query)
    
    # 转换结果
    columns = [
        'symbol', 'frame', 'open', 'high', 'low', 'close', 'vol', 
        'amount', 'adjust', 'st', 'buy_limit', 'sell_limit'
    ]
    
    return [dict(zip(columns, row)) for row in result]

if __name__ == "__main__":
    consume_sqep_data()