"use client";
export default function DashboardLegend() {
  return (
    <div className="text-xs text-muted-foreground space-y-1">
      <div>
        <span className="font-medium text-foreground">Dial:</span> color bands reflect composite z-score — red (≤ -1), amber (-1–0), yellow (0–0.5), green (≥ 0.5).
      </div>
      <div>
        <span className="font-medium text-foreground">Pillars:</span> bar color shows 3m momentum — green (rising), red (falling), gray (flat).
      </div>
      <div>
        <span className="font-medium text-foreground">Key Market States:</span> state based on price vs. 50D MA; vol bucket from 20D return std quantiles.
      </div>
    </div>
  );
}

