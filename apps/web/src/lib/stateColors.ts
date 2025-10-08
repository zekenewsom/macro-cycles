export function stateColor(label: string): string {
  const l = label || "";
  if (l.includes("Trend-Up")) return "rgba(0,200,0,0.10)";
  if (l.includes("Trend-Down")) return "rgba(200,0,0,0.10)";
  if (l.includes("Range")) return "rgba(150,150,150,0.08)";
  return "rgba(150,150,150,0.06)";
}

export function lineColor(label: string): string {
  const l = label || "";
  if (l.includes("Trend-Up")) return "rgba(0,160,0,1)";
  if (l.includes("Trend-Down")) return "rgba(200,0,0,1)";
  if (l.includes("Range")) return "rgba(120,120,120,1)";
  return "rgba(120,120,120,1)";
}

