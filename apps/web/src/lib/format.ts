export function formatCount(value: number): string {
  return new Intl.NumberFormat("zh-CN", { maximumFractionDigits: 0 }).format(value);
}

export function formatValue(value: string, currency: string | null): string {
  const [integer, decimals] = value.split(".");
  const grouped = integer.replace(/\B(?=(\d{3})+(?!\d))/g, ",");
  const formatted = decimals === undefined ? grouped : `${grouped}.${decimals}`;
  return currency ? `${formatted} ${currency}` : formatted;
}

export function formatRate(value: string | null, numerator?: number, denominator?: number): string {
  if (value === null) return "不适用";
  const rate = `${(Number(value) * 100).toFixed(2)}%`;
  return numerator === undefined || denominator === undefined
    ? rate : `${rate}（${formatCount(numerator)}/${formatCount(denominator)}）`;
}

export function formatDateTime(value: string): string {
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? value : date.toLocaleString("zh-CN", { hour12: false });
}
