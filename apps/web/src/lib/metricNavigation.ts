export function metricLinkParam<T extends string>(
  hash: string, route: string, key: string, allowed: readonly T[],
): T | null {
  const [path, search] = hash.slice(1).split("?", 2);
  if (path !== route || !search) return null;
  const value = new URLSearchParams(search.split("#", 1)[0]).get(key);
  return value && allowed.includes(value as T) ? value as T : null;
}
