"use client";
import dynamic from "next/dynamic";
import * as React from "react";
import { Dialog, DialogContent, DialogHeader, DialogTitle } from "@/components/ui/dialog";

const Plot = dynamic(() => import("react-plotly.js"), { ssr: false });

type Item = { pillar: string; weight: number; delta: number; contribution: number };

export default function Waterfall({ items }: { items: Item[] }) {
  const x = items.map((i) => i.pillar);
  const y = items.map((i) => i.contribution);
  const [open, setOpen] = React.useState(false);
  const [pillar, setPillar] = React.useState<string>("");
  const [loading, setLoading] = React.useState(false);
  const [movers, setMovers] = React.useState<{ id: string; label: string; delta: number; z_after: number }[]>([]);

  async function loadMovers(p: string) {
    setLoading(true);
    try {
      const base = process.env.NEXT_PUBLIC_API_BASE || "http://localhost:8000";
      const r = await fetch(`${base}/overview/movers?top_k=15&pillar=${encodeURIComponent(p)}`, { cache: "no-store" });
      const j = await r.json();
      let rows = (j?.gainers ?? []).concat(j?.losers ?? [])
        .sort((a: any, b: any) => Math.abs(b.delta) - Math.abs(a.delta))
        .slice(0, 12);
      // Fallback to pillar-indicator-contrib if empty
      if (!rows.length) {
        const rr = await fetch(`${base}/drivers/pillar-indicator-contrib?pillar=${encodeURIComponent(p)}&window=1&top_k=12`, { cache: "no-store" });
        const jj = await rr.json();
        rows = (jj?.items ?? []).map((it: any) => ({ id: it.series_id, label: it.label || it.series_id, delta: it.delta, z_after: it.z_after }));
      }
      setMovers(rows);
    } catch (e) {
      setMovers([]);
    } finally {
      setLoading(false);
    }
  }

  return (
    <>
      <Plot
        data={[{ type: "bar", x, y }]}
        layout={{ autosize: true, margin: { l: 40, r: 20, t: 10, b: 40 }, title: "Contributions (weight × Δpillar)" }}
        useResizeHandler
        style={{ width: "100%", height: 320 }}
        config={{ displaylogo: false }}
        onClick={(ev: any) => {
          const p = ev?.points?.[0]?.x;
          if (typeof p === "string") {
            setPillar(p);
            setOpen(true);
            loadMovers(p);
          }
        }}
      />

      <Dialog open={open} onOpenChange={setOpen}>
        <DialogContent className="max-w-2xl">
          <DialogHeader>
            <DialogTitle>Top moving indicators — {pillar}</DialogTitle>
          </DialogHeader>
          {loading ? (
            <div className="p-4 text-sm text-muted-foreground">Loading…</div>
          ) : movers.length ? (
            <div className="p-2 text-sm overflow-auto">
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
              <div className="text-xs text-muted-foreground mt-2">Tip: open the Data page to inspect any series.</div>
            </div>
          ) : (
            <div className="p-4 text-sm">No movers found for this pillar.</div>
          )}
        </DialogContent>
      </Dialog>
    </>
  );
}
