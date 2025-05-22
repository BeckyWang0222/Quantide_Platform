import redis
import json
from datetime import datetime
from xtquant.xtdata import (
    init,
    download_history_data,
    get_local_data,
    get_trading_dates,
    close
)

# Redis配置 - 使用Mac的IP地址
REDIS_HOST = "Mac的IP地址"
REDIS_PORT = 6379
REDIS_QUEUE_NAME = "qmt_minute_queue"
REDIS_PASSWORD = None  # 如果有密码，请设置

def setup_redis_client():
    """初始化Redis客户端"""
    return redis.StrictRedis(
        host=REDIS_HOST, 
        port=REDIS_PORT, 
        password=REDIS_PASSWORD,
        decode_responses=True
    )

def fetch_minute_data(stock_code, date_str):
    """获取指定日期的分钟线数据"""
    try:
        # 获取分钟线数据
        df = get_local_data(stock_code, 'min1', date_str, date_str)
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
        print(f"获取分钟线数据失败 {stock_code} {date_str}: {str(e)}")
        return []

def main():
    """主函数"""
    # 初始化QMT接口
    init()
    
    # 初始化Redis客户端
    redis_client = setup_redis_client()
    
    try:
        # 配置参数
        stock_list = ["000001.SZ", "600519.SH"]
        start_date = "20230101"
        end_date = "20230131"
        
        # 下载历史数据
        print(f"开始下载历史数据: {start_date} 至 {end_date}")
        download_history_data(stock_list, 'min1', start_date, end_date)
        print("历史数据下载完成")
        
        # 获取交易日列表
        trading_dates = get_trading_dates(start_date, end_date)
        
        # 按日期和股票代码获取分钟线数据并发送到Redis
        for trade_date in trading_dates:
            date_str = trade_date.strftime("%Y%m%d")
            print(f"处理日期: {date_str}")
            
            for stock_code in stock_list:
                minute_data = fetch_minute_data(stock_code, date_str)
                
                if minute_data:
                    # 封装数据
                    data_package = {
                        "timestamp": datetime.now().isoformat(),
                        "ts_code": stock_code,
                        "trade_date": date_str,
                        "minute_data": minute_data
                    }
                    
                    # 发送到Redis
                    redis_client.lpush(REDIS_QUEUE_NAME, json.dumps(data_package))
                    print(f"已推送分钟线数据: {stock_code} - {date_str} ({len(minute_data)}条)")
    
    except Exception as e:
        print(f"程序执行异常: {str(e)}")
    
    finally:
        # 关闭QMT接口
        close()
        print("程序执行完毕")

if __name__ == "__main__":
    main()