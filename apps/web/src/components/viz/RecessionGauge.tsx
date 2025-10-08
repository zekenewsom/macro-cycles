"use client";
import dynamic from "next/dynamic";
import * as React from "react";

const Plot = dynamic(() => import("react-plotly.js"), { ssr: false });

export default function RecessionGauge({ prob }: { prob: number | null | undefined }) {
  if (prob == null || isNaN(Number(prob))) {
    return (
      <div className="p-4 text-sm text-muted-foreground">Recession risk model unavailable.</div>
    );
  }
  const p = Math.max(0, Math.min(1, Number(prob)));
  return (
    <Plot
      data={[
        {
          type: "indicator",
          mode: "gauge+number",
          value: p * 100,
          number: { suffix: "%", valueformat: ".0f" },
          title: { text: "Recession Risk (12m)" },
          gauge: {
            axis: { range: [0, 100] },
            bar: { color: p >= 0.5 ? "#b91c1c" : p >= 0.3 ? "#f59e0b" : "#059669" },
            steps: [
              { range: [0, 30], color: "#d1fae5" },
              { range: [30, 50], color: "#fef3c7" },
              { range: [50, 100], color: "#fee2e2" },
            ],
          },
        } as any,
      ]}
      layout={{ autosize: true, margin: { l: 20, r: 20, t: 10, b: 10 } }}
      useResizeHandler
      style={{ width: "100%", height: 220 }}
      config={{ displaylogo: false }}
    />
  );
}

