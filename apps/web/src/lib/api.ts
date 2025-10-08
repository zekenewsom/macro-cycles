export const API = process.env.NEXT_PUBLIC_API_BASE ?? "http://localhost:8000";

export async function j<T>(path: string): Promise<T> {
  const r = await fetch(`${API}${path}`, { cache: "no-store" });
  if (!r.ok) throw new Error(`API ${path} failed`);
  return r.json();
}

export async function getDriversPillars(){ return j("/drivers/pillars"); }
export async function getIndicatorHeatmap(pillar: string){ return j(`/drivers/indicator-heatmap?pillar=${encodeURIComponent(pillar)}`); }
export async function getContributions(){ return j("/drivers/contributions"); }
