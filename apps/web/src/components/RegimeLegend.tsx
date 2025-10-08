"use client";
import { stateColor } from "@/lib/stateColors";

export default function RegimeLegend({ labels = ["Trend-Down", "Range", "Trend-Up"] }: { labels?: string[] }) {
  return (
    <div className="flex gap-2 items-center flex-wrap text-xs">
      {labels.map((l) => (
        <span key={l} className="inline-flex items-center gap-1">
          <span className="inline-block w-3 h-3 rounded" style={{ backgroundColor: stateColor(l) }} />
          <span className="text-muted-foreground">{l}</span>
        </span>
      ))}
    </div>
  );
}

