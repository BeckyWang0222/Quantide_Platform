import redis
import tushare as ts
import json
from datetime import datetime

# Tushare和Redis配置
TUSHARE_TOKEN = "bd02f68c6c42a536dd9b005228af5454e175a5812380585a7d2b1ab9"
REDIS_HOST = "localhost"
REDIS_PORT = 6379
REDIS_QUEUE_NAME = "tushare_data_queue"

# 初始化连接
pro = ts.pro_api(TUSHARE_TOKEN)
redis_client = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

def fetch_ohlc_daily_data(ts_code: str, start_date: str, end_date: str) -> list:
    try:
        df = pro.daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
        return df[['ts_code', 'trade_date', 'open', 'high', 'low', 'close', 'vol']].to_dict('records')
    except Exception as e:
        print(f"OHLC数据获取失败: {str(e)}")
        return []

def fetch_adj_factor(ts_code: str, start_date: str, end_date: str) -> list:
    try:
        df = pro.adj_factor(ts_code=ts_code, start_date=start_date, end_date=end_date)
        return df[['ts_code', 'trade_date', 'adj_factor']].to_dict('records')
    except Exception as e:
        print(f"复权因子获取失败: {str(e)}")
        return []
def produce_data(ts_code_list: list, date_range: tuple):
    start_date, end_date = date_range
    for ts_code in ts_code_list:
        data_package = {
            "timestamp": datetime.now().isoformat(),
            "ts_code": ts_code,
            "ohlc_data": fetch_ohlc_daily_data(ts_code, start_date, end_date),
            "adj_factor": fetch_adj_factor(ts_code, start_date, end_date)
        }
        redis_client.lpush(REDIS_QUEUE_NAME, json.dumps(data_package))
        print(f"已推送数据: {ts_code} - {start_date}至{end_date}")

if __name__ == "__main__":
    STOCK_CODES = ["000001.SZ", "600519.SH"]
    DATE_RANGE = ("20230101", "20231231")
    produce_data(STOCK_CODES, DATE_RANGE)
