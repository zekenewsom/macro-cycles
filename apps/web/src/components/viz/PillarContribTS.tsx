"use client";
import dynamic from "next/dynamic";
const Plot = dynamic(() => import("react-plotly.js"), { ssr: false });

export default function PillarContribTS({
  series,
}: {
  series: { pillar: string; points: { date: string; value: number }[] }[];
}) {
  const traces = series.map((s) => ({
    x: s.points.map((p) => p.date),
    y: s.points.map((p) => p.value),
    type: "scatter",
    mode: "lines",
    name: s.pillar,
  }));
  return (
    <Plot
      data={traces}
      layout={{ autosize: true, margin: { l: 40, r: 20, t: 10, b: 40 }, legend: { orientation: "h" } }}
      useResizeHandler
      style={{ width: "100%", height: 360 }}
      config={{ displaylogo: false }}
    />
  );
}

