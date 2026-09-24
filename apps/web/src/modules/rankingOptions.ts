import type { EChartsOption } from "echarts";

import { formatCount, formatRate, formatValue } from "../lib/format";
import type {
  BehaviorDimension, BehaviorRankingPoint, BehaviorSort, OrderDimension,
  OrderRankingPoint, OrderSort,
} from "../lib/types";

export const orderDimensions: Record<OrderDimension, string> = {
  product: "商品", category: "品类", seller: "卖家", customer_state: "买家地域", seller_state: "卖家地域",
};
export const behaviorDimensions: Record<BehaviorDimension, string> = {
  product: "商品", category: "品类", brand: "品牌",
};
export const orderSorts: Record<OrderDimension, readonly OrderSort[]> = {
  product: ["order_count", "item_value", "freight_value"],
  category: ["order_count", "item_value", "freight_value"],
  seller: ["order_count", "item_value", "freight_value"],
  customer_state: ["order_count", "payment_value", "late_rate"],
  seller_state: ["order_count", "payment_value", "late_rate"],
};
export const orderSortLabels: Record<OrderSort, string> = {
  order_count: "订单数", item_value: "商品值", freight_value: "运费值",
  payment_value: "支付值", late_rate: "晚到率",
};
export const behaviorSortLabels: Record<BehaviorSort, string> = {
  views: "浏览数", carts: "加购数", purchases: "购买数", users: "用户数", amount: "购买金额代理值",
};

export function rankingName(row: OrderRankingPoint | BehaviorRankingPoint): string {
  return row.is_unknown ? `未识别维度（${row.dimension_id}）` : row.dimension_name || row.dimension_id;
}

export function orderValue(row: OrderRankingPoint, sort: OrderSort, currency: string | null): string {
  if (sort === "order_count") return formatCount(row.ranking_order_count);
  if (sort === "item_value") return row.ranking_item_value_sum === null ? "不适用" : formatValue(row.ranking_item_value_sum, currency);
  if (sort === "freight_value") return row.ranking_freight_value_sum === null ? "不适用" : formatValue(row.ranking_freight_value_sum, currency);
  if (sort === "payment_value") return row.ranking_payment_value_sum === null ? "不适用" : formatValue(row.ranking_payment_value_sum, currency);
  return formatRate(row.ranking_late_delivery_rate, row.ranking_late_delivery_order_count ?? undefined,
    row.ranking_late_delivery_eligible_order_count ?? undefined);
}

export function behaviorValue(row: BehaviorRankingPoint, sort: BehaviorSort): string {
  if (sort === "views") return formatCount(row.view_count);
  if (sort === "carts") return formatCount(row.cart_count);
  if (sort === "purchases") return formatCount(row.purchase_count);
  if (sort === "users") return formatCount(row.unique_user_count);
  return row.purchase_amount_proxy === null ? "不适用" : formatValue(row.purchase_amount_proxy, null);
}

function orderNumber(row: OrderRankingPoint, sort: OrderSort): number | null {
  if (sort === "order_count") return row.ranking_order_count;
  if (sort === "item_value") return row.ranking_item_value_sum === null ? null : Number(row.ranking_item_value_sum);
  if (sort === "freight_value") return row.ranking_freight_value_sum === null ? null : Number(row.ranking_freight_value_sum);
  if (sort === "payment_value") return row.ranking_payment_value_sum === null ? null : Number(row.ranking_payment_value_sum);
  return row.ranking_late_delivery_rate === null ? null : Number(row.ranking_late_delivery_rate) * 100;
}

function behaviorNumber(row: BehaviorRankingPoint, sort: BehaviorSort): number | null {
  if (sort === "views") return row.view_count;
  if (sort === "carts") return row.cart_count;
  if (sort === "purchases") return row.purchase_count;
  if (sort === "users") return row.unique_user_count;
  return row.purchase_amount_proxy === null ? null : Number(row.purchase_amount_proxy);
}

export function rankingChart(
  rows: OrderRankingPoint[] | BehaviorRankingPoint[],
  sort: OrderSort | BehaviorSort,
  currency: string | null,
): EChartsOption {
  const reversed = [...rows].reverse();
  const order = sort === "order_count" || sort === "item_value" || sort === "freight_value" ||
    sort === "payment_value" || sort === "late_rate";
  const name = order ? orderSortLabels[sort as OrderSort] : behaviorSortLabels[sort as BehaviorSort];
  const text = (row: OrderRankingPoint | BehaviorRankingPoint) => order
    ? orderValue(row as OrderRankingPoint, sort as OrderSort, currency)
    : behaviorValue(row as BehaviorRankingPoint, sort as BehaviorSort);
  const numeric = (row: OrderRankingPoint | BehaviorRankingPoint) => order
    ? orderNumber(row as OrderRankingPoint, sort as OrderSort)
    : behaviorNumber(row as BehaviorRankingPoint, sort as BehaviorSort);
  return {
    color: ["#2d7566"],
    grid: { left: 145, right: 30, top: 18, bottom: 28 },
    tooltip: { trigger: "item", renderMode: "richText", formatter: (params) => {
      const index = (params as { dataIndex: number }).dataIndex;
      const row = reversed[index];
      return row ? `${rankingName(row)}\n${name}: ${text(row)}` : "";
    } },
    xAxis: { type: "value", splitLine: { lineStyle: { color: "#e7eee8" } } },
    yAxis: { type: "category", data: reversed.map(rankingName), axisLabel: { width: 125, overflow: "truncate" } },
    series: [{ type: "bar", barMaxWidth: 19, data: reversed.map(numeric) }],
  };
}
