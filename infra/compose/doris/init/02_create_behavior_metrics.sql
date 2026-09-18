CREATE DATABASE IF NOT EXISTS analytics;

USE analytics;

CREATE TABLE IF NOT EXISTS behavior_metric_publications (
    metric_run_id VARCHAR(96) NOT NULL,
    dataset_id VARCHAR(64) NOT NULL,
    metric_version VARCHAR(32) NOT NULL,
    data_scope VARCHAR(64) NOT NULL,
    source_snapshot_id LARGEINT NOT NULL,
    source_event_count BIGINT NOT NULL,
    window_start DATE NOT NULL,
    window_end DATE NOT NULL,
    calculated_at DATETIME(3) NOT NULL,
    published_at DATETIME(3) NOT NULL,
    overview_row_count BIGINT NOT NULL,
    overview_sha256 CHAR(64) NOT NULL,
    funnel_row_count BIGINT NOT NULL,
    funnel_sha256 CHAR(64) NOT NULL,
    dimension_row_count BIGINT NOT NULL,
    dimension_sha256 CHAR(64) NOT NULL,
    quality_row_count BIGINT NOT NULL,
    quality_sha256 CHAR(64) NOT NULL,
    status VARCHAR(16) NOT NULL
)
UNIQUE KEY(metric_run_id)
DISTRIBUTED BY HASH(metric_run_id) BUCKETS 1
PROPERTIES ("replication_allocation" = "tag.location.default: 1");

CREATE TABLE IF NOT EXISTS behavior_overview_metrics (
    metric_run_id VARCHAR(96) NOT NULL,
    dataset_id VARCHAR(64) NOT NULL,
    metric_version VARCHAR(32) NOT NULL,
    window_type VARCHAR(8) NOT NULL,
    window_start DATE NOT NULL,
    window_end DATE NOT NULL,
    event_count BIGINT NOT NULL,
    view_count BIGINT NOT NULL,
    cart_count BIGINT NOT NULL,
    purchase_count BIGINT NOT NULL,
    unique_user_count BIGINT NOT NULL,
    session_count BIGINT NOT NULL,
    product_count BIGINT NOT NULL,
    purchase_amount_proxy DECIMAL(38,2) NOT NULL
)
UNIQUE KEY(metric_run_id, window_type, window_start)
DISTRIBUTED BY HASH(metric_run_id) BUCKETS 1
PROPERTIES ("replication_allocation" = "tag.location.default: 1");

CREATE TABLE IF NOT EXISTS behavior_funnel_metrics (
    metric_run_id VARCHAR(96) NOT NULL,
    dataset_id VARCHAR(64) NOT NULL,
    metric_version VARCHAR(32) NOT NULL,
    window_type VARCHAR(8) NOT NULL,
    window_start DATE NOT NULL,
    window_end DATE NOT NULL,
    missing_session_event_count BIGINT NOT NULL,
    view_sessions BIGINT NOT NULL,
    view_to_cart_sessions BIGINT NOT NULL,
    completed_sessions BIGINT NOT NULL
)
UNIQUE KEY(metric_run_id, window_type, window_start)
DISTRIBUTED BY HASH(metric_run_id) BUCKETS 1
PROPERTIES ("replication_allocation" = "tag.location.default: 1");

CREATE TABLE IF NOT EXISTS behavior_dimension_metrics (
    metric_run_id VARCHAR(96) NOT NULL,
    dataset_id VARCHAR(64) NOT NULL,
    metric_version VARCHAR(32) NOT NULL,
    window_type VARCHAR(8) NOT NULL,
    window_start DATE NOT NULL,
    window_end DATE NOT NULL,
    dimension_type VARCHAR(16) NOT NULL,
    dimension_id VARCHAR(256) NOT NULL,
    dimension_name VARCHAR(512) NULL,
    is_unknown BOOLEAN NOT NULL,
    view_count BIGINT NOT NULL,
    cart_count BIGINT NOT NULL,
    purchase_count BIGINT NOT NULL,
    unique_user_count BIGINT NOT NULL,
    purchase_amount_proxy DECIMAL(38,2) NOT NULL
)
UNIQUE KEY(metric_run_id, window_type, window_start, dimension_type, dimension_id)
DISTRIBUTED BY HASH(metric_run_id) BUCKETS 1
PROPERTIES ("replication_allocation" = "tag.location.default: 1");

CREATE TABLE IF NOT EXISTS behavior_quality_metrics (
    metric_run_id VARCHAR(96) NOT NULL,
    dataset_id VARCHAR(64) NOT NULL,
    metric_version VARCHAR(32) NOT NULL,
    window_type VARCHAR(8) NOT NULL,
    window_start DATE NOT NULL,
    window_end DATE NOT NULL,
    source_event_count BIGINT NOT NULL,
    clean_event_count BIGINT NOT NULL,
    late_event_count BIGINT NOT NULL,
    clean_event_rate DECIMAL(18,8) NULL,
    late_event_rate DECIMAL(18,8) NULL,
    distinct_event_count BIGINT NOT NULL,
    duplicate_event_count BIGINT NOT NULL,
    missing_session_count BIGINT NOT NULL,
    unknown_category_count BIGINT NOT NULL,
    unknown_brand_count BIGINT NOT NULL,
    invalid_event_type_count BIGINT NOT NULL,
    empty_key_id_count BIGINT NOT NULL,
    invalid_price_count BIGINT NOT NULL,
    invalid_derived_date_count BIGINT NOT NULL,
    overview_event_count BIGINT NOT NULL,
    reconciliation_status VARCHAR(32) NOT NULL
)
UNIQUE KEY(metric_run_id, window_type, window_start)
DISTRIBUTED BY HASH(metric_run_id) BUCKETS 1
PROPERTIES ("replication_allocation" = "tag.location.default: 1");
