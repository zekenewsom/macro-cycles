"use client";
import dynamic from "next/dynamic";
import * as React from "react";

const Plot = dynamic(() => import("react-plotly.js"), { ssr: false });

type MacroDialProps = {
  value: number | null | undefined;
  delta?: number | null | undefined;
  regime?: string | null | undefined;
  min?: number;
  max?: number;
  height?: number;
};

export default function MacroDial({ value, delta, regime, min = -2.5, max = 2.5, height = 220 }: MacroDialProps) {
  const v = typeof value === "number" ? value : 0;
  const d = typeof delta === "number" ? delta : 0;
  const title = regime ? `${regime}` : "Composite";
  const suffix = d ? ` (Δ ${d > 0 ? "+" : ""}${d.toFixed(2)})` : "";
  // Color zones: red (-inf..-1), amber (-1..0), yellow (0..0.5), green (0.5..inf)
  const steps = [
    { range: [min, -1], color: "#ef4444" },
    { range: [-1, 0], color: "#f59e0b" },
    { range: [0, 0.5], color: "#fde047" },
    { range: [0.5, max], color: "#22c55e" },
  ];
  return (
    <Plot
      data={[
        {
          type: "indicator",
          mode: "gauge+number",
          value: v,
          title: { text: `${title}${suffix}`, font: { size: 16 } },
          number: { valueformat: ".2f" },
          gauge: {
            axis: { range: [min, max] },
            bar: { color: "#111827" },
            steps,
          },
        } as any,
      ]}
      layout={{ autosize: true, margin: { l: 10, r: 10, t: 6, b: 6 } }}
      useResizeHandler
      style={{ width: "100%", height }}
      config={{ displaylogo: false }}
    />
  );
}
