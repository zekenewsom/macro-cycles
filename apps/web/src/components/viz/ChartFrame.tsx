"use client";
import { useState } from "react";
import dynamic from "next/dynamic";
const Plot = dynamic(() => import("react-plotly.js"), { ssr: false });
import { Button } from "@/components/ui/button";
import { Download, ZoomOut } from "lucide-react";

type ChartFrameProps = {
  title: string;
  subtitle?: string;
  traces: any[];
  layout?: Partial<any>;
  height?: number;
  filename?: string;
  rightControls?: React.ReactNode;
};

export default function ChartFrame({
  title,
  subtitle,
  traces,
  layout,
  height = 420,
  filename = "chart",
  rightControls,
}: ChartFrameProps) {
  const [rev, setRev] = useState(0);

  return (
    <div className="p-4">
      <div className="flex items-center justify-between mb-3">
        <div>
          {subtitle && <div className="text-sm text-muted-foreground">{subtitle}</div>}
          <div className="text-lg font-semibold">{title}</div>
        </div>
        <div className="flex items-center gap-2">
          {rightControls}
          <Button variant="outline" size="sm" onClick={() => setRev((r) => r + 1)} title="Reset view">
            <ZoomOut className="h-4 w-4" />
          </Button>
          <Button
            variant="outline"
            size="sm"
            title="Export PNG"
            onClick={() => {
              const link = document.querySelector<HTMLAnchorElement>(
                ".js-plotly-plot .modebar-btn[data-title='Download plot as a png']",
              );
              (link as any)?.click?.();
            }}
          >
            <Download className="h-4 w-4" />
          </Button>
        </div>
      </div>

      <Plot
        key={rev}
        data={traces}
        layout={{
          autosize: true,
          margin: { l: 48, r: 16, t: 8, b: 40 },
          xaxis: {
            rangeslider: { visible: true },
            rangeselector: {
              buttons: [
                { step: "year", stepmode: "backward", count: 1, label: "1y" },
                { step: "year", stepmode: "backward", count: 5, label: "5y" },
                { step: "all", label: "Max" },
              ],
            },
            showspikes: true,
            spikemode: "across",
            spikethickness: 1,
          },
          yaxis: { zeroline: false, showspikes: true, spikemode: "across", spikethickness: 1 },
          showlegend: true,
          ...layout,
        }}
        useResizeHandler
        style={{ width: "100%", height }}
        config={{ displaylogo: false, responsive: true, modeBarButtonsToRemove: ["lasso2d", "select2d"] }}
      />
    </div>
  );
}
