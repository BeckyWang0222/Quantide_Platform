import redis
import tushare as ts
import json
from datetime import datetime
from typing import List, Dict, Tuple, Any, Union

# Tushare和Redis配置
TUSHARE_TOKEN = "bd02f68c6c42a536dd9b005228af5454e175a5812380585a7d2b1ab9"
REDIS_HOST = "8.217.201.221"
REDIS_PORT = 16379
REDIS_PASSWORD = "quantide666"  # 添加Redis密码
REDIS_QUEUE_NAME = "sqep_bar_day_queue"

# 初始化连接
pro = ts.pro_api(TUSHARE_TOKEN)
redis_client = redis.StrictRedis(
    host=REDIS_HOST, 
    port=REDIS_PORT, 
    password=REDIS_PASSWORD,  # 使用密码进行身份验证
    decode_responses=True
)

def encode_symbol(symbol: str) -> int:
    """将字符串格式的股票代码转换为整型编码
    
    Args:
        symbol: 股票代码，如 '000001.SZ' 或 '600519.SH'
        
    Returns:
        整型编码的股票代码，如 2000001 或 1600519
    """
    code, exchange = symbol.split('.')
    code = code.lstrip('0')  # 移除前导零，但保留至少一位数字
    if not code:
        code = '0'
        
    if exchange.upper() == 'SH':
        prefix = '1'
    elif exchange.upper() == 'SZ':
        prefix = '2'
    else:
        raise ValueError(f"不支持的交易所: {exchange}")
        
    return int(prefix + code)

def fetch_daily_data(ts_code: str, start_date: str, end_date: str) -> List[Dict[str, Any]]:
    """获取日线数据并转换为SQEP-BAR-DAY格式
    
    Args:
        ts_code: 股票代码
        start_date: 开始日期，格式为YYYYMMDD
        end_date: 结束日期，格式为YYYYMMDD
        
    Returns:
        SQEP-BAR-DAY格式的数据列表
    """
    try:
        # 获取OHLC数据
        df_daily = pro.daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
        
        # 获取复权因子
        df_adj = pro.adj_factor(ts_code=ts_code, start_date=start_date, end_date=end_date)
        adj_dict = {row['trade_date']: row['adj_factor'] for _, row in df_adj.iterrows()}
        
        # 获取涨跌停价格（如果有高级API权限）
        try:
            df_limit = pro.limit_list(ts_code=ts_code, start_date=start_date, end_date=end_date)
            limit_dict = {row['trade_date']: (row['up_limit'], row['down_limit']) 
                         for _, row in df_limit.iterrows()}
        except:
            limit_dict = {}
        
        # 获取ST状态（如果有高级API权限）
        try:
            df_namechange = pro.namechange(ts_code=ts_code, start_date=start_date, end_date=end_date)
            st_dict = {row['start_date']: '*' in row['name'] or 'ST' in row['name'] 
                      for _, row in df_namechange.iterrows()}
        except:
            st_dict = {}
        
        # 转换为SQEP-BAR-DAY格式
        sqep_data = []
        for _, row in df_daily.iterrows():
            trade_date = row['trade_date']
            
            # 转换日期格式
            frame = datetime.strptime(trade_date, '%Y%m%d').date().isoformat()
            
            # 转换股票代码
            symbol = encode_symbol(ts_code)
            
            # 创建基本SQEP记录
            sqep_record = {
                'symbol': symbol,
                'frame': frame,
                'open': float(row['open']),
                'high': float(row['high']),
                'low': float(row['low']),
                'close': float(row['close']),
                'vol': float(row['vol']),
                'amount': float(row.get('amount', 0)),
                'adjust': float(adj_dict.get(trade_date, 1.0))
            }
            
            # 添加可选字段（如果存在）
            if trade_date in limit_dict:
                sqep_record['buy_limit'] = float(limit_dict[trade_date][0])
                sqep_record['sell_limit'] = float(limit_dict[trade_date][1])
                
            if trade_date in st_dict:
                sqep_record['st'] = st_dict[trade_date]
                
            sqep_data.append(sqep_record)
            
        return sqep_data
    
    except Exception as e:
        print(f"获取日线数据失败: {str(e)}")
        return []

def produce_sqep_data(ts_code_list: List[str], date_range: Tuple[str, str]):
    """生产SQEP-BAR-DAY数据并推送到Redis
    
    Args:
        ts_code_list: 股票代码列表
        date_range: 日期范围元组 (start_date, end_date)
    """
    start_date, end_date = date_range
    
    for ts_code in ts_code_list:
        # 获取并转换数据
        sqep_data = fetch_daily_data(ts_code, start_date, end_date)
        
        if not sqep_data:
            print(f"未获取到 {ts_code} 的数据")
            continue
        
        # 创建数据包
        data_package = {
            "timestamp": datetime.now().isoformat(),
            "source": "tushare",
            "data_type": "SQEP-BAR-DAY",
            "records": sqep_data
        }
        
        # 推送到Redis
        redis_client.lpush(REDIS_QUEUE_NAME, json.dumps(data_package))
        print(f"已推送SQEP-BAR-DAY数据: {ts_code} - {start_date}至{end_date} ({len(sqep_data)}条)")

if __name__ == "__main__":
    # 示例参数
    STOCK_CODES = ["000001.SZ", "600519.SH"]
    DATE_RANGE = ("20230101", "20231231")
    
    produce_sqep_data(STOCK_CODES, DATE_RANGE)