"use client";
import dynamic from "next/dynamic";
import type { CompositePoint } from "@/lib/types";

// Avoid SSR errors from plotly accessing `self`/`window`
const Plot = dynamic(() => import("react-plotly.js"), { ssr: false });

export default function CompositeChart({ data }: { data: CompositePoint[] }) {
  const x = data.map(d => d.date);
  const y = data.map(d => d.z);
  return (
    <Plot
      data={[{ x, y, type: "scatter", mode: "lines", name: "Composite (z)" }]}
      layout={{
        autosize: true,
        margin: { l: 40, r: 20, t: 10, b: 40 },
        xaxis: {
          rangeslider: { visible: true },
          rangeselector: { buttons: [
            { step: "year", stepmode: "backward", count: 1, label: "1y" },
            { step: "year", stepmode: "backward", count: 5, label: "5y" },
            { step: "all", label: "Max" },
          ]},
        },
        showlegend: false,
      }}
      useResizeHandler
      style={{ width: "100%", height: 420 }}
      config={{ displaylogo: false, responsive: true }}
    />
  );
}
