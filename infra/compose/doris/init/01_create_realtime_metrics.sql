CREATE DATABASE IF NOT EXISTS analytics;

USE analytics;

CREATE TABLE IF NOT EXISTS realtime_metrics (
    metric_name VARCHAR(32) NOT NULL,
    metric_value BIGINT NOT NULL,
    updated_at DATETIME NOT NULL
)
UNIQUE KEY(metric_name)
DISTRIBUTED BY HASH(metric_name) BUCKETS 1
PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
);
