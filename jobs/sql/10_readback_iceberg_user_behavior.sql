SET 'execution.runtime-mode' = 'batch';
SET 'sql-client.execution.result-mode' = 'TABLEAU';

SELECT COUNT(*) AS event_count FROM lakehouse.analytics.user_behavior_detail;
