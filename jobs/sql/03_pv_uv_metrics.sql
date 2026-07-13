INSERT INTO metrics_print_sink
SELECT 'pv' AS metric_name, COUNT(*) AS pv
FROM user_behavior_source
UNION ALL
SELECT 'uv' AS metric_name, COUNT(DISTINCT user_id) AS uv
FROM user_behavior_source;
