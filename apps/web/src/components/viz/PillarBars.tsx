"use client";
import dynamic from "next/dynamic";
import * as React from "react";

const Plot = dynamic(() => import("react-plotly.js"), { ssr: false });

type Pillar = { pillar: string; z: number; momentum_3m?: number | null };

export default function PillarBars({ pillars, onClick }: { pillars: Pillar[]; onClick?: (pillar: string) => void }) {
  const labels = pillars.map((p) => (p.pillar || "").toUpperCase());
  const z = pillars.map((p) => (typeof p.z === "number" ? p.z : null));
  const mom = pillars.map((p) => (typeof p.momentum_3m === "number" ? p.momentum_3m : 0));
  const colors = mom.map((m) => (m > 0 ? "#16a34a" : m < 0 ? "#dc2626" : "#6b7280"));

  return (
    <Plot
      data={[
        {
          type: "bar",
          x: z,
          y: labels,
          orientation: "h",
          marker: { color: colors },
          hovertemplate: "%{y}: z=%{x:.2f}<extra></extra>",
        } as any,
      ]}
      layout={{
        bargap: 0.25,
        autosize: true,
        margin: { l: 80, r: 10, t: 10, b: 30 },
        xaxis: { zeroline: true, zerolinecolor: "#9ca3af", showspikes: false },
        yaxis: { automargin: true },
        showlegend: false,
      }}
      useResizeHandler
      style={{ width: "100%", height: Math.max(220, labels.length * 28) }}
      config={{ displaylogo: false }}
      onClick={(ev: any) => {
        const p = ev?.points?.[0]?.y;
        if (typeof p === "string") {
          const id = p.toLowerCase();
          onClick?.(id);
        }
      }}
    />
  );
}

