-- ClickHouse数据库初始化SQL脚本

-- 创建数据库
CREATE DATABASE IF NOT EXISTS market_data;

-- 使用数据库
USE market_data;

-- 创建日线数据表
CREATE TABLE IF NOT EXISTS daily_bars (
    symbol String,
    frame Date,
    open Float64,
    high Float64,
    low Float64,
    close Float64,
    vol Float64,
    amount Float64,
    adjust Float64,
    st Bool,
    limit_up Float64,
    limit_down Float64,
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(frame)
ORDER BY (symbol, frame)
SETTINGS index_granularity = 8192;

-- 创建分钟线数据表
CREATE TABLE IF NOT EXISTS minute_bars (
    symbol String,
    frame DateTime,
    open Float64,
    high Float64,
    low Float64,
    close Float64,
    vol Float64,
    amount Float64,
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(frame)
ORDER BY (symbol, frame)
SETTINGS index_granularity = 8192;

-- 创建5分钟线聚合表
CREATE TABLE IF NOT EXISTS minute_bars_5min (
    symbol String,
    frame DateTime,
    open Float64,
    high Float64,
    low Float64,
    close Float64,
    vol Float64,
    amount Float64,
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(frame)
ORDER BY (symbol, frame)
SETTINGS index_granularity = 8192;

-- 创建5分钟线物化视图
CREATE MATERIALIZED VIEW IF NOT EXISTS minute_bars_5min_mv
TO minute_bars_5min
AS SELECT
    symbol,
    toStartOfInterval(frame, INTERVAL 5 MINUTE) as frame,
    argMin(open, frame) as open,
    max(high) as high,
    min(low) as low,
    argMax(close, frame) as close,
    sum(vol) as vol,
    sum(amount) as amount,
    now() as created_at
FROM minute_bars
GROUP BY symbol, frame;

-- 创建30分钟线聚合表
CREATE TABLE IF NOT EXISTS minute_bars_30min (
    symbol String,
    frame DateTime,
    open Float64,
    high Float64,
    low Float64,
    close Float64,
    vol Float64,
    amount Float64,
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(frame)
ORDER BY (symbol, frame)
SETTINGS index_granularity = 8192;

-- 创建30分钟线物化视图
CREATE MATERIALIZED VIEW IF NOT EXISTS minute_bars_30min_mv
TO minute_bars_30min
AS SELECT
    symbol,
    toStartOfInterval(frame, INTERVAL 30 MINUTE) as frame,
    argMin(open, frame) as open,
    max(high) as high,
    min(low) as low,
    argMax(close, frame) as close,
    sum(vol) as vol,
    sum(amount) as amount,
    now() as created_at
FROM minute_bars
GROUP BY symbol, frame;
