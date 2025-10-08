"use client";
import dynamic from "next/dynamic";
import * as React from "react";
import { Dialog, DialogContent, DialogHeader, DialogTitle } from "@/components/ui/dialog";

const Plot = dynamic(() => import("react-plotly.js"), { ssr: false });

async function fetchJSON(path: string) {
  const base = process.env.NEXT_PUBLIC_API_BASE ?? "http://localhost:8000";
  const r = await fetch(`${base}${path}`, { cache: "no-store" });
  return r.json();
}

export default function PillarDeepDive({ pillar, open, onOpenChange }: { pillar: string | null; open: boolean; onOpenChange: (v: boolean) => void }) {
  const [series, setSeries] = React.useState<{ date: string; z: number }[]>([]);
  const [movers, setMovers] = React.useState<{ id: string; label: string; delta: number; z_after: number }[]>([]);
  const [mini, setMini] = React.useState<{ label: string; delta: number }[]>([]);
  const [loading, setLoading] = React.useState(false);

  React.useEffect(() => {
    const run = async () => {
      if (!pillar || !open) return;
      setLoading(true);
      try {
        const [s, m, mw] = await Promise.all([
          fetchJSON(`/drivers/pillar-series?pillar=${encodeURIComponent(pillar)}&years=15`),
          fetchJSON(`/overview/movers?top_k=15&pillar=${encodeURIComponent(pillar)}`),
          fetchJSON(`/drivers/pillar-indicator-contrib?pillar=${encodeURIComponent(pillar)}&window=1&top_k=12`),
        ]);
        setSeries(s?.points ?? []);
        const rows = (m?.gainers ?? []).concat(m?.losers ?? []).sort((a: any, b: any) => Math.abs(b.delta) - Math.abs(a.delta)).slice(0, 12);
        setMovers(rows);
        setMini((mw?.items ?? []).map((r: any) => ({ label: r.label || r.series_id, delta: r.delta })));
      } catch (_) {
        setSeries([]);
        setMovers([]);
      } finally {
        setLoading(false);
      }
    };
    run();
  }, [pillar, open]);

  const tr = [
    {
      x: series.map((d) => d.date),
      y: series.map((d) => d.z),
      mode: "lines",
      type: "scatter",
      name: "z",
    } as any,
  ];
  const miniTr = [
    {
      type: "bar",
      x: mini.map((d) => d.label),
      y: mini.map((d) => d.delta),
      marker: { color: mini.map((d) => (d.delta >= 0 ? "#16a34a" : "#dc2626")) },
      hovertemplate: "%{x}: Δz=%{y:.2f}<extra></extra>",
    } as any,
  ];

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-3xl">
        <DialogHeader>
          <DialogTitle>{pillar?.toUpperCase()} — Deep Dive</DialogTitle>
        </DialogHeader>
        {loading ? (
          <div className="p-4 text-sm text-muted-foreground">Loading…</div>
        ) : (
          <div className="space-y-4">
            <div className="border rounded-md">
              <Plot
                data={tr}
                layout={{ autosize: true, margin: { l: 40, r: 10, t: 10, b: 40 }, showlegend: false, yaxis: { zeroline: false } }}
                useResizeHandler
                style={{ width: "100%", height: 260 }}
                config={{ displaylogo: false }}
              />
            </div>
            <div className="border rounded-md p-2">
              <div className="text-sm font-medium mb-1">Mini Waterfall — Indicator Δz (1m)</div>
              <Plot
                data={miniTr}
                layout={{ autosize: true, margin: { l: 40, r: 10, t: 10, b: 80 }, showlegend: false, xaxis: { tickangle: -30 } }}
                useResizeHandler
                style={{ width: "100%", height: 240 }}
                config={{ displaylogo: false }}
              />
            </div>
            <div>
              <div className="text-sm font-medium mb-2">Top Movers (Δz this month)</div>
              <div className="text-xs overflow-auto">
                <table className="w-full">
                  <thead>
                    <tr>
                      <th className="text-left">Series</th>
                      <th className="text-left">Label</th>
                      <th className="text-right">Δ z</th>
                      <th className="text-right">z</th>
                    </tr>
                  </thead>
                  <tbody>
                    {movers.map((m: any) => (
                      <tr key={m.id} className="border-t border-border/50">
                        <td className="py-1">{m.id}</td>
                        <td>{m.label}</td>
                        <td className="text-right">{(m.delta ?? 0).toFixed(2)}</td>
                        <td className="text-right">{(m.z_after ?? 0).toFixed(2)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
                {!movers.length && (
                  <div className="text-muted-foreground py-3">No movers found for this pillar.</div>
                )}
              </div>
            </div>
          </div>
        )}
      </DialogContent>
    </Dialog>
  );
}
