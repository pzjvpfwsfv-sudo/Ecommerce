INSERT INTO doris_metrics_sink
SELECT
    'pv' AS metric_name,
    COUNT(*) AS metric_value,
    CURRENT_TIMESTAMP AS updated_at
FROM user_behavior_source
UNION ALL
SELECT
    'uv' AS metric_name,
    COUNT(DISTINCT user_id) AS metric_value,
    CURRENT_TIMESTAMP AS updated_at
FROM user_behavior_source;
