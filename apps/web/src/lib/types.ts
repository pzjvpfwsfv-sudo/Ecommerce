export interface MetricMeta {
  dataset_id: string;
  metric_version: string;
  metric_run_id: string;
  window_start: string;
  window_end: string;
  calculated_at: string;
  warnings: string[];
}

export interface OrderMeta extends MetricMeta {
  dataset_id: "olist-brazilian-ecommerce-v2";
  metric_version: "orders-v1";
  source_bundle_sha256: string;
  source_snapshots: Record<string, string>;
  curated_snapshots: Record<string, string>;
  source_timezone: "unspecified";
  source_currency: string | null;
  source_order_count: number;
  implementation_revision: string;
}

export interface BehaviorMeta extends MetricMeta {
  dataset_id: "rees46-multicategory";
  metric_version: "behavior-v1";
  source_snapshot_id: string;
  source_table: string;
  replay_first_at: string | null;
  replay_last_at: string | null;
  data_scope: "g2c-correctness-subset" | "stable-user-2pct-full";
  source_event_count: number;
}

export interface MetricResponse<T, M extends MetricMeta = MetricMeta> {
  meta: M;
  data: T;
}

export interface WindowPoint { window_type: string; window_start: string; window_end: string; }

export interface OrderOverviewPoint extends WindowPoint {
  order_count: number;
  delivered_order_count: number;
  canceled_order_count: number;
  unavailable_order_count: number;
  status_eligible_order_count: number;
  status_excluded_order_count: number;
  delivered_rate: string | null;
  canceled_rate: string | null;
  unique_customer_count: number;
  repeat_customer_count: number;
  repeat_customer_rate: string | null;
  item_row_count: number;
  item_value_sum: string;
  freight_value_sum: string;
  payment_value_sum: string;
  items_per_order_avg: string | null;
}

export interface OrderDeliveryPoint extends WindowPoint {
  delivery_eligible_order_count: number;
  delivery_excluded_order_count: number;
  delivery_days_avg: string | null;
  delivery_days_p50: string | null;
  delivery_days_p90: string | null;
  late_delivery_order_count: number;
  late_delivery_eligible_order_count: number;
  late_delivery_excluded_order_count: number;
  late_delivery_rate: string | null;
}

export interface OrderPaymentPoint extends WindowPoint {
  payment_type: string;
  is_all: boolean;
  global_order_count: number;
  payment_order_count: number;
  payment_row_count: number;
  installment_order_count: number;
  payment_value_sum: string;
}

export type OrderDimension = "product" | "category" | "seller" | "customer_state" | "seller_state";
export type OrderSort = "order_count" | "item_value" | "freight_value" | "payment_value" | "late_rate";

export interface OrderRankingPoint extends WindowPoint {
  dimension_type: OrderDimension;
  dimension_id: string;
  dimension_name: string;
  is_unknown: boolean;
  ranking_order_count: number;
  ranking_item_row_count: number | null;
  ranking_customer_count: number;
  ranking_item_value_sum: string | null;
  ranking_freight_value_sum: string | null;
  ranking_payment_value_sum: string | null;
  ranking_late_delivery_order_count: number | null;
  ranking_late_delivery_eligible_order_count: number | null;
  ranking_late_delivery_rate: string | null;
  payment_value_is_additive: boolean | null;
}

export interface OrderReviewPoint extends WindowPoint {
  review_row_count: number;
  reviewed_order_count: number;
  all_order_count: number;
  review_coverage_rate: string | null;
  review_score_avg: string | null;
  low_score_order_count: number;
  low_score_rate: string | null;
  multi_review_order_count: number;
}

export interface OrderQuality {
  window_type: "FULL";
  window_start: string;
  window_end: string;
  source_row_count: number;
  iceberg_row_count: number;
  duplicate_key_count: number;
  orphan_key_count: number;
  invalid_value_count: number;
  temporal_anomaly_count: number;
  amount_comparable_order_count: number;
  amount_reconciled_order_count: number;
  amount_mismatch_order_count: number;
  amount_reconciliation_rate: string | null;
  raw_row_counts: Record<string, number>;
  normalized_row_counts: Record<string, number>;
  iceberg_row_counts: Record<string, number>;
  normalized_sha256: Record<string, string>;
  source_snapshots: Record<string, string>;
  curated_snapshots: Record<string, string>;
  fact_reconciliations: Record<string, number>;
  reportable_quality: Record<string, number>;
  reconciliation_status: "PASS";
}

export interface PublicationData {
  published_at: string;
  status: "PUBLISHED";
  [key: string]: string | number;
}

export interface BehaviorOverviewPoint extends WindowPoint {
  event_count: number;
  view_count: number;
  cart_count: number;
  purchase_count: number;
  unique_user_count: number;
  session_count: number;
  product_count: number;
  purchase_amount_proxy: string | null;
}

export interface BehaviorFunnelPoint extends WindowPoint {
  missing_session_event_count: number;
  view_sessions: number;
  view_to_cart_sessions: number;
  completed_sessions: number;
  view_to_cart_rate: string | null;
  cart_to_purchase_rate: string | null;
  full_conversion_rate: string | null;
}

export type BehaviorDimension = "product" | "category" | "brand";
export type BehaviorSort = "views" | "carts" | "purchases" | "users" | "amount";

export interface BehaviorRankingPoint extends WindowPoint {
  dimension_type: BehaviorDimension;
  dimension_id: string;
  dimension_name: string | null;
  is_unknown: boolean;
  view_count: number;
  cart_count: number;
  purchase_count: number;
  unique_user_count: number;
  purchase_amount_proxy: string | null;
}

export interface BehaviorQuality {
  window_type: "FULL";
  window_start: string;
  window_end: string;
  source_event_count: number;
  clean_event_count: number;
  late_event_count: number;
  clean_event_rate: string | null;
  late_event_rate: string | null;
  distinct_event_count: number;
  duplicate_event_count: number;
  missing_session_count: number;
  unknown_category_count: number;
  unknown_brand_count: number;
  invalid_event_type_count: number;
  empty_key_id_count: number;
  invalid_price_count: number;
  invalid_derived_date_count: number;
  overview_event_count: number;
  reconciliation_status: "PASS";
}

export interface MetricDefinition {
  metric_name: string;
  display_name: string;
  formula: string;
  numerator: string;
  denominator: string | null;
  source_fields: string[];
  allowed_windows: string[];
  additive: boolean;
  null_policy: string;
  limitations: string[];
  forbidden_claims: string[];
  exclusions?: string[];
}

export interface DefinitionsResponse {
  domain: "orders" | "behavior";
  dataset_id: string;
  metric_version: string;
  definitions: MetricDefinition[];
}
