"use client";
import { Card } from "@/components/ui/card";
import dynamic from "next/dynamic";
const Plot = dynamic(() => import("react-plotly.js"), { ssr: false });

export function KPI({ label, value, delta, spark }: { label: string; value: string; delta?: number; spark?: number[] }) {
  const deltaStr =
    delta === undefined ? "" : delta >= 0 ? `+${delta.toFixed(2)}` : delta.toFixed(2);
  return (
    <Card className="p-4">
      <div className="text-xs text-muted-foreground">{label}</div>
      <div className="flex items-end justify-between">
        <div className="text-3xl font-semibold">{value}</div>
        {spark && (
          <Plot
            data={[{ y: spark, type: "scatter", mode: "lines", line: { width: 1 } }]}
            layout={{
              autosize: true,
              margin: { l: 0, r: 0, t: 0, b: 0 },
              xaxis: { visible: false },
              yaxis: { visible: false },
            }}
            useResizeHandler
            style={{ width: 120, height: 40 }}
            config={{ displaylogo: false }}
          />
        )}
      </div>
      {delta !== undefined && <div className="text-xs mt-1">{deltaStr} since last month</div>}
    </Card>
  );
}
