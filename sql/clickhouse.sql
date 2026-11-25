CREATE DATABASE IF NOT EXISTS cinema;

CREATE TABLE IF NOT EXISTS cinema.raw_events (
    event_id UUID,
    user_id String,
    movie_id String,
    event_type LowCardinality(String),
    timestamp DateTime64(3, 'UTC'),
    device_type LowCardinality(String),
    session_id String,
    progress_seconds Nullable(Int32),
    event_date Date MATERIALIZED toDate(timestamp)
) ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (event_date, user_id, event_id);

CREATE TABLE IF NOT EXISTS cinema.daily_metrics (
    day Date,
    metric LowCardinality(String),
    dimension String,
    value Float64,
    computed_at DateTime64(3, 'UTC'),
    version UInt64
) ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMM(day)
ORDER BY (day, metric, dimension);

CREATE TABLE IF NOT EXISTS cinema.cohort_retention (
    cohort_date Date,
    day_offset UInt8,
    cohort_size UInt64,
    returned UInt64,
    retention_percent Float64,
    computed_at DateTime64(3, 'UTC'),
    version UInt64
) ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMM(cohort_date)
ORDER BY (cohort_date, day_offset);

CREATE TABLE IF NOT EXISTS cinema.kafka_events (
    event_id UUID,
    user_id String,
    movie_id String,
    event_type String,
    timestamp DateTime64(3, 'UTC'),
    device_type String,
    session_id String,
    progress_seconds Nullable(Int32)
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka1:9092,kafka2:9092,kafka3:9092',
    kafka_topic_list = 'movie-events',
    kafka_group_name = 'cinema-clickhouse-v1',
    kafka_format = 'AvroConfluent',
    format_avro_schema_registry_url = 'http://schema-registry:8081',
    kafka_num_consumers = 3,
    kafka_auto_offset_reset = 'earliest',
    kafka_thread_per_consumer = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS cinema.kafka_to_raw
TO cinema.raw_events AS
SELECT event_id, user_id, movie_id, event_type, timestamp,
       device_type, session_id, progress_seconds
FROM cinema.kafka_events;
