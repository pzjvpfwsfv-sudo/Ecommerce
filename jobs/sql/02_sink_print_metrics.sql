CREATE TABLE metrics_print_sink (
    metric_name STRING,
    metric_value BIGINT
) WITH (
    'connector' = 'print'
);
