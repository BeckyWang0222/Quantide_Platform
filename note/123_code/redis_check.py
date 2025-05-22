import redis
import logging

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# 创建 Redis 客户端
client = redis.StrictRedis(
    host='localhost',
    port=6379,
    db=0,
    socket_connect_timeout=5  # 连接超时时间设置为 5 秒
)

# 检查连接是否成功
try:
    response = client.ping()
    if response:
        logging.info("成功连接到 Redis")
    else:
        logging.warning("Redis 服务器未正确响应 PING 命令")
except redis.ConnectionError:
    logging.error("连接 Redis 时出现连接错误")
except redis.TimeoutError:
    logging.error("连接 Redis 超时，请检查网络或服务器状态")
except Exception as e:
    logging.error(f"连接 Redis 时出现未知错误: {str(e)}")
