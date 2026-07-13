INSERT INTO lakehouse_local.analytics.user_behavior_detail
SELECT
    event_id,
    user_id,
    product_id,
    event_type,
    event_time,
    channel,
    device_type,
    page_id
FROM user_behavior_source;
