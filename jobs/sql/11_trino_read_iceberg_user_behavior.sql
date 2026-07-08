SELECT COUNT(*) AS event_count
FROM lakehouse.analytics.user_behavior_detail;

SELECT event_type, COUNT(*) AS event_count
FROM lakehouse.analytics.user_behavior_detail
GROUP BY event_type
ORDER BY event_count DESC, event_type ASC;
