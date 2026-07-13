CREATE CATALOG lakehouse WITH (
    'type' = 'iceberg',
    'catalog-type' = 'hive',
    'uri' = 'thrift://hive-metastore:9083',
    'warehouse' = 's3a://warehouse/iceberg',
    'property-version' = '1'
);

CREATE DATABASE IF NOT EXISTS lakehouse.analytics;

CREATE TABLE IF NOT EXISTS lakehouse.analytics.user_behavior_detail (
    event_id STRING,
    user_id STRING,
    product_id STRING,
    event_type STRING,
    event_time STRING,
    channel STRING,
    device_type STRING,
    page_id STRING
);
