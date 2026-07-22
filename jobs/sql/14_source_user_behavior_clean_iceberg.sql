CREATE TABLE user_behavior_source (
    event_id STRING,
    user_id STRING,
    product_id STRING,
    event_type STRING,
    event_time STRING,
    channel STRING,
    device_type STRING,
    page_id STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'user_behavior_clean',
    'properties.bootstrap.servers' = 'kafka:29092',
    'properties.group.id' = 'chapter9-iceberg-clean-v1',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
);
