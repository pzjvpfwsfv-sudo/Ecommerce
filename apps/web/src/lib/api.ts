import { ApiError, jsonRequest } from "./http";
import type {
  BehaviorDimension, BehaviorFunnelPoint, BehaviorMeta, BehaviorOverviewPoint,
  BehaviorQuality, BehaviorRankingPoint, BehaviorSort, DefinitionsResponse,
  MetricMeta, MetricResponse, OrderDeliveryPoint, OrderDimension, OrderMeta,
  OrderOverviewPoint, OrderPaymentPoint, OrderQuality, OrderRankingPoint,
  OrderReviewPoint, OrderSort, PublicationData,
} from "./types";

export interface OrderQuery {
  window: "day" | "month" | "full";
  startDate?: string;
  endDate?: string;
  signal?: AbortSignal;
}

export interface BehaviorQuery {
  window: "day" | "full";
  signal?: AbortSignal;
}

export interface OrderRankingQuery extends OrderQuery {
  dimension: OrderDimension;
  sortBy: OrderSort;
  limit: number;
}

export interface BehaviorRankingQuery extends BehaviorQuery {
  dimension: BehaviorDimension;
  sortBy: BehaviorSort;
  limit: number;
}

const identities = {
  orders: { dataset: "olist-brazilian-ecommerce-v2", version: "orders-v1" },
  behavior: { dataset: "rees46-multicategory", version: "behavior-v1" },
} as const;

const orderSorts: Record<OrderDimension, readonly OrderSort[]> = {
  product: ["order_count", "item_value", "freight_value"],
  category: ["order_count", "item_value", "freight_value"],
  seller: ["order_count", "item_value", "freight_value"],
  customer_state: ["order_count", "payment_value", "late_rate"],
  seller_state: ["order_count", "payment_value", "late_rate"],
};

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function checkMeta(value: unknown, domain: "orders" | "behavior"): asserts value is MetricMeta {
  const identity = identities[domain];
  if (!isRecord(value) ||
      value.dataset_id !== identity.dataset || value.metric_version !== identity.version ||
      typeof value.metric_run_id !== "string" || !value.metric_run_id.startsWith(`${identity.version}-`) ||
      typeof value.window_start !== "string" || typeof value.window_end !== "string" ||
      typeof value.calculated_at !== "string" ||
      !Array.isArray(value.warnings) || !value.warnings.every((item) => typeof item === "string")) {
    throw new ApiError(502, "metric meta missing or mismatched");
  }
  if (domain === "orders" &&
      (typeof value.source_order_count !== "number" ||
       !isRecord(value.source_snapshots) || !isRecord(value.curated_snapshots) ||
       !(typeof value.source_currency === "string" || value.source_currency === null))) {
    throw new ApiError(502, "order meta missing");
  }
  if (domain === "behavior" &&
      (!Number.isInteger(value.source_event_count) ||
       typeof value.source_snapshot_id !== "string" ||
       typeof value.source_table !== "string" ||
       !/^real_behavior_detail_v1(?:_[a-z0-9][a-z0-9_]{0,31})?$/.test(value.source_table) ||
       !((value.replay_first_at === null && value.replay_last_at === null) ||
         (typeof value.replay_first_at === "string" && typeof value.replay_last_at === "string")) ||
       !["g2c-correctness-subset", "stable-user-2pct-full"].includes(String(value.data_scope)))) {
    throw new ApiError(502, "behavior meta missing");
  }
}

async function metric<T>(
  path: string, domain: "orders" | "behavior", signal?: AbortSignal,
  shape: "array" | "object" = "array",
): Promise<MetricResponse<T>> {
  const response = await jsonRequest<unknown>(path, { signal });
  if (!isRecord(response)) throw new ApiError(502, "metric response missing");
  checkMeta(response.meta, domain);
  if (shape === "array" ? !Array.isArray(response.data) : !isRecord(response.data)) {
    throw new ApiError(502, "metric data shape invalid");
  }
  return response as unknown as MetricResponse<T>;
}

function orderParams(query: OrderQuery): URLSearchParams {
  if (query.window === "full" && (query.startDate || query.endDate)) {
    throw new Error("full window does not accept dates");
  }
  if (Boolean(query.startDate) !== Boolean(query.endDate)) {
    throw new Error("startDate and endDate must be provided together");
  }
  if (query.startDate && query.endDate && query.startDate > query.endDate) {
    throw new Error("startDate must not be after endDate");
  }
  const params = new URLSearchParams({ window: query.window });
  if (query.startDate && query.endDate) {
    params.set("start_date", query.startDate);
    params.set("end_date", query.endDate);
  }
  return params;
}

function behaviorParams(query: BehaviorQuery): URLSearchParams {
  if (query.window !== "day" && query.window !== "full") throw new Error("invalid behavior window");
  return new URLSearchParams({ window: query.window });
}

const orderList = async <T>(route: string, query: OrderQuery) =>
  metric<T[]>(`/api/v1/orders/${route}?${orderParams(query)}`, "orders", query.signal) as Promise<MetricResponse<T[], OrderMeta>>;

const behaviorList = async <T>(route: string, query: BehaviorQuery) =>
  metric<T[]>(`/api/v1/behavior/${route}?${behaviorParams(query)}`, "behavior", query.signal) as Promise<MetricResponse<T[], BehaviorMeta>>;

export const getOrderOverview = (query: OrderQuery) => orderList<OrderOverviewPoint>("overview", query);
export const getOrderDelivery = (query: OrderQuery) => orderList<OrderDeliveryPoint>("delivery", query);
export const getOrderPayments = (query: OrderQuery) => orderList<OrderPaymentPoint>("payments", query);
export const getOrderReviews = (query: OrderQuery) => orderList<OrderReviewPoint>("reviews", query);

export async function getOrderRankings(query: OrderRankingQuery) {
  if (!orderSorts[query.dimension].includes(query.sortBy) ||
      !Number.isInteger(query.limit) || query.limit < 1 || query.limit > 100) {
    throw new Error("unsupported order ranking sort or limit");
  }
  const params = orderParams(query);
  params.set("dimension", query.dimension);
  params.set("sort_by", query.sortBy);
  params.set("limit", String(query.limit));
  return metric<OrderRankingPoint[]>(`/api/v1/orders/rankings?${params}`, "orders", query.signal) as Promise<MetricResponse<OrderRankingPoint[], OrderMeta>>;
}

export const getOrderQuality = (signal?: AbortSignal) =>
  metric<OrderQuality>("/api/v1/orders/quality", "orders", signal, "object") as Promise<MetricResponse<OrderQuality, OrderMeta>>;

export const getBehaviorOverview = (query: BehaviorQuery) => behaviorList<BehaviorOverviewPoint>("overview", query);
export const getBehaviorFunnel = (query: BehaviorQuery) => behaviorList<BehaviorFunnelPoint>("funnel", query);

export async function getBehaviorRankings(query: BehaviorRankingQuery) {
  if (!(["product", "category", "brand"] as string[]).includes(query.dimension) ||
      !(["views", "carts", "purchases", "users", "amount"] as string[]).includes(query.sortBy) ||
      !Number.isInteger(query.limit) || query.limit < 1 || query.limit > 100) {
    throw new Error("unsupported behavior ranking query");
  }
  const params = behaviorParams(query);
  params.set("dimension", query.dimension);
  params.set("sort_by", query.sortBy);
  params.set("limit", String(query.limit));
  return metric<BehaviorRankingPoint[]>(`/api/v1/behavior/rankings?${params}`, "behavior", query.signal) as Promise<MetricResponse<BehaviorRankingPoint[], BehaviorMeta>>;
}

export const getBehaviorQuality = (signal?: AbortSignal) =>
  metric<BehaviorQuality>("/api/v1/behavior/quality", "behavior", signal, "object") as Promise<MetricResponse<BehaviorQuality, BehaviorMeta>>;

export function getPublication(domain: "orders", signal?: AbortSignal): Promise<MetricResponse<PublicationData, OrderMeta>>;
export function getPublication(domain: "behavior", signal?: AbortSignal): Promise<MetricResponse<PublicationData, BehaviorMeta>>;
export function getPublication(domain: "orders" | "behavior", signal?: AbortSignal) {
  return metric<PublicationData>(`/api/v1/${domain}/publication`, domain, signal, "object");
}

export async function getDefinitions(domain: "orders" | "behavior", signal?: AbortSignal): Promise<DefinitionsResponse> {
  const identity = identities[domain];
  const response = await jsonRequest<unknown>(
    `/api/v1/metrics/definitions?domain=${domain}&version=${identity.version}`, { signal },
  );
  if (!isRecord(response) || response.domain !== domain ||
      response.dataset_id !== identity.dataset || response.metric_version !== identity.version ||
      !Array.isArray(response.definitions)) {
    throw new ApiError(502, "metric definitions identity mismatch");
  }
  return response as unknown as DefinitionsResponse;
}

export async function fetchSameRun<T extends { meta: MetricMeta }, U extends { meta: MetricMeta }>(
  first: Promise<T>, second: Promise<U>,
): Promise<[T, U]> {
  const result = await Promise.all([first, second] as const);
  const [a, b] = result;
  if (a.meta.dataset_id !== b.meta.dataset_id ||
      a.meta.metric_version !== b.meta.metric_version ||
      a.meta.metric_run_id !== b.meta.metric_run_id ||
      (a.meta.metric_version === "behavior-v1" && (
        (a.meta as BehaviorMeta).source_table !== (b.meta as BehaviorMeta).source_table ||
        (a.meta as BehaviorMeta).source_snapshot_id !== (b.meta as BehaviorMeta).source_snapshot_id
      ))) {
    throw new Error("metric run mismatch");
  }
  return result;
}
