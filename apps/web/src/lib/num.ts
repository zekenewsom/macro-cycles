export const fmt = (n?: number, d = 2) =>
  (n ?? 0).toLocaleString(undefined, { maximumFractionDigits: d, minimumFractionDigits: d });
export const pct = (n?: number, d = 1) => `${((n ?? 0) * 100).toFixed(d)}%`;

