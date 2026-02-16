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
-- Store raw sums for correct merge; use page_views_agg_hour_read view for avg_duration/bounce_rate
DROP VIEW IF EXISTS page_views_minute_to_hour;
DROP TABLE IF EXISTS page_views_agg_hour;
CREATE TABLE page_views_agg_hour
(
    window_start   DateTime,
    page_id        String,
    view_count     UInt64,
    total_duration UInt64,
    bounce_count   UInt64,
    unique_users   UInt64
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
-- AggregatingMergeTree stores states; must use -Merge to aggregate before inserting into SummingMergeTree
CREATE MATERIALIZED VIEW page_views_minute_to_hour
TO page_views_agg_hour
AS
SELECT
    toStartOfHour(window_start) AS window_start,
    page_id,
    sumMerge(view_count) AS view_count,
    sumMerge(total_duration) AS total_duration,
    sumMerge(bounce_count) AS bounce_count,
    uniqMerge(unique_users) AS unique_users
FROM page_views_agg_minute
GROUP BY window_start, page_id;

-- View for querying with computed avg_duration and bounce_rate (Float32)
CREATE OR REPLACE VIEW page_views_agg_hour_read AS
SELECT
    window_start,
    page_id,
    view_count,
    total_duration,
    if(view_count > 0, toFloat32(total_duration) / toFloat32(view_count), toFloat32(0)) AS avg_duration,
    unique_users,
    if(view_count > 0, toFloat32(bounce_count) * 100.0 / toFloat32(view_count), toFloat32(0)) AS bounce_rate
FROM page_views_agg_hour;
