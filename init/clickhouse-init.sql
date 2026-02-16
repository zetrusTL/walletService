-- ClickHouse initialization script for page views aggregation
-- This script creates tables and materialized views for real-time page view aggregation

-- 1. Raw events table
CREATE TABLE IF NOT EXISTS page_views_raw
(
    event_date    Date DEFAULT today(),
    event_time    DateTime64(3, 'UTC'),
    page_id       String,
    user_id       String,
    duration_ms   UInt32,
    user_agent    String,
    ip_address    IPv6,
    region        LowCardinality(String),
    is_bounce     UInt8,
    kafka_offset  Int64,
    kafka_partition Int32,
    processed_time DateTime DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, page_id, user_id)
TTL event_date + INTERVAL 30 DAY
SETTINGS index_granularity = 8192;

-- 2. Minute aggregation table (AggregatingMergeTree)
CREATE TABLE IF NOT EXISTS page_views_agg_minute
(
    window_start  DateTime,
    page_id       String,
    view_count    AggregateFunction(sum, UInt64),
    total_duration AggregateFunction(sum, UInt64),
    unique_users  AggregateFunction(uniq, String),
    bounce_count  AggregateFunction(sum, UInt8)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(window_start)
ORDER BY (window_start, page_id)
TTL window_start + INTERVAL 7 DAY;

-- 3. Hour aggregation table (SummingMergeTree)
CREATE TABLE IF NOT EXISTS page_views_agg_hour
(
    window_start  DateTime,
    page_id       String,
    view_count    UInt64,
    avg_duration  Float32,
    unique_users  UInt64,
    bounce_rate   Float32
) ENGINE = SummingMergeTree()
ORDER BY (window_start, page_id);

-- 4. Processing errors table
CREATE TABLE IF NOT EXISTS processing_errors
(
    error_time    DateTime,
    raw_message   String,
    error_reason  String,
    kafka_offset  Int64,
    kafka_partition Int32
) ENGINE = MergeTree()
ORDER BY (error_time);

-- Materialized view: raw -> minute aggregation
CREATE MATERIALIZED VIEW IF NOT EXISTS page_views_raw_to_minute
TO page_views_agg_minute
AS
SELECT
    toStartOfMinute(event_time) AS window_start,
    page_id,
    sumState(toUInt64(1)) as view_count,
    sumState(toUInt64(duration_ms)) as total_duration,
    uniqState(user_id) as unique_users,
    sumState(is_bounce) as bounce_count
FROM page_views_raw
GROUP BY window_start, page_id;

-- Materialized view: minute -> hour aggregation
-- Note: This MV reads from AggregatingMergeTree, so we need to use -Merge functions
CREATE MATERIALIZED VIEW IF NOT EXISTS page_views_minute_to_hour
TO page_views_agg_hour
AS
SELECT
    toStartOfHour(window_start) AS window_start,
    page_id,
    sumMerge(view_count) as view_count,
    if(sumMerge(view_count) > 0, toFloat32(sumMerge(total_duration)) / toFloat32(sumMerge(view_count)), 0) as avg_duration,
    uniqMerge(unique_users) as unique_users,
    if(sumMerge(view_count) > 0, toFloat32(sumMerge(bounce_count)) * 100.0 / toFloat32(sumMerge(view_count)), 0) as bounce_rate
FROM page_views_agg_minute
GROUP BY window_start, page_id;
