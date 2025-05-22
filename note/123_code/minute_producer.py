import redis
import json
from datetime import datetime, timedelta
from xtquant.xtdata import (
    subscribe_quote,
    get_local_data,
    get_trading_dates,
    download_history_data
)

# Redis配置
REDIS_HOST = "localhost"
REDIS_PORT = 6379
REDIS_QUEUE_NAME = "qmt_minute_queue"

# 初始化Redis连接
redis_client = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

def download_minute_data(stock_list, start_date, end_date):
    """下载分钟线历史数据"""
    try:
        download_history_data(stock_list, 'min1', start_date, end_date)
        print(f"历史数据下载完成: {start_date} 至 {end_date}")
    except Exception as e:
        print(f"历史数据下载失败: {str(e)}")

def fetch_minute_data(stock_code: str, start_date: str, end_date: str) -> list:
    """获取分钟线数据"""
    try:
        # 获取分钟线数据，包含字段：time, open, high, low, close, volume, amount
        df = get_local_data(stock_code, 'min1', start_date, end_date)
        if df is None or len(df) == 0:
            return []
        
        # 转换数据格式
        records = []
        for time, row in df.iterrows():
            records.append({
                "ts_code": stock_code,
                "trade_time": time.strftime("%Y-%m-%d %H:%M:%S"),
                "open": float(row['open']),
                "high": float(row['high']),
                "low": float(row['low']),
                "close": float(row['close']),
                "vol": float(row['volume']),
                "amount": float(row['amount'])
            })
        return records
    except Exception as e:
        print(f"分钟线数据获取失败 {stock_code}: {str(e)}")
        return []

def produce_minute_data(stock_list: list, start_date: str, end_date: str):
    """生产分钟线数据"""
    # 首先下载历史数据
    download_minute_data(stock_list, start_date, end_date)
    
    # 获取交易日列表
    trading_dates = get_trading_dates(start_date, end_date)
    
    for trade_date in trading_dates:
        date_str = trade_date.strftime("%Y%m%d")
        for stock_code in stock_list:
            # 获取当天的分钟线数据
            minute_data = fetch_minute_data(
                stock_code, 
                date_str, 
                date_str
            )
            
            if minute_data:
                data_package = {
                    "timestamp": datetime.now().isoformat(),
                    "ts_code": stock_code,
                    "trade_date": date_str,
                    "minute_data": minute_data
                }
                redis_client.lpush(REDIS_QUEUE_NAME, json.dumps(data_package))
                print(f"已推送分钟线数据: {stock_code} - {date_str}")

if __name__ == "__main__":
    STOCK_LIST = ["000001.SZ", "600519.SH"]  # 股票代码列表
    START_DATE = "20230101"  # 起始日期
    END_DATE = "20231231"    # 结束日期
    
    produce_minute_data(STOCK_LIST, START_DATE, END_DATE)