CREATE CATALOG lakehouse_local WITH (
    'type' = 'iceberg',
    'catalog-type' = 'hadoop',
    'warehouse' = 'file:///workspace/tmp/iceberg-warehouse',
    'property-version' = '1'
);

CREATE DATABASE IF NOT EXISTS lakehouse_local.analytics;

CREATE TABLE IF NOT EXISTS lakehouse_local.analytics.user_behavior_detail (
    event_id STRING,
    user_id STRING,
    product_id STRING,
    event_type STRING,
    event_time STRING,
    channel STRING,
    device_type STRING,
    page_id STRING
);
