CREATE TABLE doris_metrics_sink (
    metric_name STRING,
    metric_value BIGINT,
    updated_at TIMESTAMP(3)
) WITH (
    'connector' = 'doris',
    'fenodes' = 'doris-fe:8030',
    'table.identifier' = 'analytics.realtime_metrics',
    'username' = 'root',
    'password' = '',
    'sink.label-prefix' = 'chapter4_metrics',
    'sink.enable.batch-mode' = 'true',
    'sink.buffer-flush.max-rows' = '10000',
    'sink.buffer-flush.interval' = '3s',
    'sink.enable-2pc' = 'false',
    'sink.properties.format' = 'json'
);
