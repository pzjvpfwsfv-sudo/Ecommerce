CREATE DATABASE IF NOT EXISTS analytics;

USE analytics;

CREATE TABLE IF NOT EXISTS order_metric_publications (
    metric_run_id VARCHAR(128) NOT NULL,
    dataset_id VARCHAR(64) NOT NULL,
    metric_version VARCHAR(32) NOT NULL,
    source_bundle_sha256 CHAR(64) NOT NULL,
    source_snapshots_json VARCHAR(4096) NOT NULL,
    curated_snapshots_json VARCHAR(4096) NOT NULL,
    implementation_revision CHAR(40) NOT NULL,
    source_order_count BIGINT NOT NULL,
    window_start DATE NOT NULL,
    window_end DATE NOT NULL,
    calculated_at DATETIME(3) NOT NULL,
    published_at DATETIME(3) NOT NULL,
    overview_row_count BIGINT NOT NULL,
    overview_sha256 CHAR(64) NOT NULL,
    delivery_row_count BIGINT NOT NULL,
    delivery_sha256 CHAR(64) NOT NULL,
    payment_row_count BIGINT NOT NULL,
    payment_sha256 CHAR(64) NOT NULL,
    ranking_row_count BIGINT NOT NULL,
    ranking_sha256 CHAR(64) NOT NULL,
    review_row_count BIGINT NOT NULL,
    review_sha256 CHAR(64) NOT NULL,
    quality_row_count BIGINT NOT NULL,
    quality_sha256 CHAR(64) NOT NULL,
    status VARCHAR(16) NOT NULL
)
UNIQUE KEY(metric_run_id)
DISTRIBUTED BY HASH(metric_run_id) BUCKETS 1
PROPERTIES ("replication_allocation" = "tag.location.default: 1");

CREATE TABLE IF NOT EXISTS order_metric_overview (
    metric_run_id VARCHAR(128) NOT NULL,
    window_type VARCHAR(8) NOT NULL,
    window_start DATE NOT NULL,
    dataset_id VARCHAR(64) NOT NULL,
    metric_version VARCHAR(32) NOT NULL,
    window_end DATE NOT NULL,
    order_count BIGINT NOT NULL,
    delivered_order_count BIGINT NOT NULL,
    canceled_order_count BIGINT NOT NULL,
    unavailable_order_count BIGINT NOT NULL,
    status_eligible_order_count BIGINT NOT NULL,
    status_excluded_order_count BIGINT NOT NULL,
    delivered_rate DECIMAL(18,6) NULL,
    canceled_rate DECIMAL(18,6) NULL,
    unique_customer_count BIGINT NOT NULL,
    repeat_customer_count BIGINT NOT NULL,
    repeat_customer_rate DECIMAL(18,6) NULL,
    item_row_count BIGINT NOT NULL,
    item_value_sum DECIMAL(38,2) NOT NULL,
    freight_value_sum DECIMAL(38,2) NOT NULL,
    payment_value_sum DECIMAL(38,2) NOT NULL,
    items_per_order_avg DECIMAL(18,6) NULL
)
UNIQUE KEY(metric_run_id, window_type, window_start)
DISTRIBUTED BY HASH(metric_run_id) BUCKETS 1
PROPERTIES ("replication_allocation" = "tag.location.default: 1");

CREATE TABLE IF NOT EXISTS order_metric_delivery (
    metric_run_id VARCHAR(128) NOT NULL,
    window_type VARCHAR(8) NOT NULL,
    window_start DATE NOT NULL,
    dataset_id VARCHAR(64) NOT NULL,
    metric_version VARCHAR(32) NOT NULL,
    window_end DATE NOT NULL,
    delivery_eligible_order_count BIGINT NOT NULL,
    delivery_excluded_order_count BIGINT NOT NULL,
    delivery_days_avg DECIMAL(18,6) NULL,
    delivery_days_p50 DECIMAL(18,6) NULL,
    delivery_days_p90 DECIMAL(18,6) NULL,
    late_delivery_order_count BIGINT NOT NULL,
    late_delivery_eligible_order_count BIGINT NOT NULL,
    late_delivery_excluded_order_count BIGINT NOT NULL,
    late_delivery_rate DECIMAL(18,6) NULL
)
UNIQUE KEY(metric_run_id, window_type, window_start)
DISTRIBUTED BY HASH(metric_run_id) BUCKETS 1
PROPERTIES ("replication_allocation" = "tag.location.default: 1");

CREATE TABLE IF NOT EXISTS order_metric_payment (
    metric_run_id VARCHAR(128) NOT NULL,
    window_type VARCHAR(8) NOT NULL,
    window_start DATE NOT NULL,
    payment_type VARCHAR(64) NOT NULL,
    dataset_id VARCHAR(64) NOT NULL,
    metric_version VARCHAR(32) NOT NULL,
    window_end DATE NOT NULL,
    is_all BOOLEAN NOT NULL,
    global_order_count BIGINT NOT NULL,
    payment_order_count BIGINT NOT NULL,
    payment_row_count BIGINT NOT NULL,
    installment_order_count BIGINT NOT NULL,
    payment_type_order_count BIGINT NOT NULL,
    payment_type_value_sum DECIMAL(38,2) NOT NULL
)
UNIQUE KEY(metric_run_id, window_type, window_start, payment_type)
DISTRIBUTED BY HASH(metric_run_id) BUCKETS 1
PROPERTIES ("replication_allocation" = "tag.location.default: 1");

CREATE TABLE IF NOT EXISTS order_metric_ranking (
    metric_run_id VARCHAR(128) NOT NULL,
    window_type VARCHAR(8) NOT NULL,
    window_start DATE NOT NULL,
    dimension_type VARCHAR(32) NOT NULL,
    dimension_id VARCHAR(256) NOT NULL,
    dataset_id VARCHAR(64) NOT NULL,
    metric_version VARCHAR(32) NOT NULL,
    window_end DATE NOT NULL,
    dimension_name VARCHAR(512) NOT NULL,
    is_unknown BOOLEAN NOT NULL,
    ranking_order_count BIGINT NOT NULL,
    ranking_item_row_count BIGINT NULL,
    ranking_customer_count BIGINT NOT NULL,
    ranking_item_value_sum DECIMAL(38,2) NULL,
    ranking_freight_value_sum DECIMAL(38,2) NULL,
    ranking_payment_value_sum DECIMAL(38,2) NULL,
    ranking_late_delivery_order_count BIGINT NULL,
    ranking_late_delivery_eligible_order_count BIGINT NULL,
    ranking_late_delivery_rate DECIMAL(18,6) NULL,
    payment_value_is_additive BOOLEAN NULL
)
UNIQUE KEY(metric_run_id, window_type, window_start, dimension_type, dimension_id)
DISTRIBUTED BY HASH(metric_run_id) BUCKETS 1
PROPERTIES ("replication_allocation" = "tag.location.default: 1");

CREATE TABLE IF NOT EXISTS order_metric_review (
    metric_run_id VARCHAR(128) NOT NULL,
    window_type VARCHAR(8) NOT NULL,
    window_start DATE NOT NULL,
    dataset_id VARCHAR(64) NOT NULL,
    metric_version VARCHAR(32) NOT NULL,
    window_end DATE NOT NULL,
    review_row_count BIGINT NOT NULL,
    reviewed_order_count BIGINT NOT NULL,
    all_order_count BIGINT NOT NULL,
    review_coverage_rate DECIMAL(18,6) NULL,
    review_score_avg DECIMAL(18,6) NULL,
    low_score_order_count BIGINT NOT NULL,
    low_score_rate DECIMAL(18,6) NULL,
    multi_review_order_count BIGINT NOT NULL
)
UNIQUE KEY(metric_run_id, window_type, window_start)
DISTRIBUTED BY HASH(metric_run_id) BUCKETS 1
PROPERTIES ("replication_allocation" = "tag.location.default: 1");

CREATE TABLE IF NOT EXISTS order_metric_quality (
    metric_run_id VARCHAR(128) NOT NULL,
    dataset_id VARCHAR(64) NOT NULL,
    metric_version VARCHAR(32) NOT NULL,
    window_type VARCHAR(8) NOT NULL,
    window_start DATE NOT NULL,
    window_end DATE NOT NULL,
    source_row_count BIGINT NOT NULL,
    iceberg_row_count BIGINT NOT NULL,
    duplicate_key_count BIGINT NOT NULL,
    orphan_key_count BIGINT NOT NULL,
    invalid_value_count BIGINT NOT NULL,
    temporal_anomaly_count BIGINT NOT NULL,
    amount_comparable_order_count BIGINT NOT NULL,
    amount_reconciled_order_count BIGINT NOT NULL,
    amount_mismatch_order_count BIGINT NOT NULL,
    amount_reconciliation_rate DECIMAL(18,6) NULL,
    payment_item_freight_abs_difference_avg DECIMAL(18,6) NULL,
    payment_item_freight_abs_difference_p50 DECIMAL(18,6) NULL,
    payment_item_freight_abs_difference_p90 DECIMAL(18,6) NULL,
    raw_row_counts_json VARCHAR(4096) NOT NULL,
    normalized_row_counts_json VARCHAR(4096) NOT NULL,
    iceberg_row_counts_json VARCHAR(4096) NOT NULL,
    normalized_sha256_json VARCHAR(8192) NOT NULL,
    source_snapshots_json VARCHAR(4096) NOT NULL,
    curated_snapshots_json VARCHAR(4096) NOT NULL,
    fact_reconciliations_json VARCHAR(8192) NOT NULL,
    reportable_quality_json VARCHAR(8192) NOT NULL,
    reconciliation_status VARCHAR(16) NOT NULL
)
UNIQUE KEY(metric_run_id)
DISTRIBUTED BY HASH(metric_run_id) BUCKETS 1
PROPERTIES ("replication_allocation" = "tag.location.default: 1");
