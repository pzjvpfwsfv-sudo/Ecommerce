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
    'topic' = 'user_behavior_events',
    'properties.bootstrap.servers' = 'kafka:29092',
    'properties.group.id' = '__AUDIT_GROUP_ID__',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json'
);
