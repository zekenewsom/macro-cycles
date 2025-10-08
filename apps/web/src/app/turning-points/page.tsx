import { Card } from "@/components/ui/card";
import ChartFrame from "@/components/viz/ChartFrame";
import { buildRegimeShapes } from "@/lib/regimes";
import RegimeLegend from "@/components/RegimeLegend";
import { j } from "@/lib/api";

export const dynamic = "force-dynamic";

export default async function Page() {
  const [comp, track] = await Promise.all([
    j<{ series: { date: string; z: number }[] }>("/overview/composite"),
    j<{ labels: { date: string; label: string }[] }>("/turning-points/track?name=business"),
  ]);
  const x = comp.series.map((d) => d.date);
  const y = comp.series.map((d) => d.z);
  const shapes = buildRegimeShapes(track.labels ?? []);
  // compute HMM state characteristics (mean and volatility of z)
  const stats: { label: string; mean: number; vol: number; count: number }[] = [];
  if ((track.labels ?? []).length) {
    const byDate: Record<string, string> = Object.fromEntries((track.labels ?? []).map((l: any) => [l.date, l.label]));
    const groups: Record<string, number[]> = {};
    for (const pt of comp.series) {
      const lab = byDate[pt.date];
      if (!lab) continue;
      (groups[lab] = groups[lab] || []).push(pt.z);
    }
    for (const [lab, arr] of Object.entries(groups)) {
      if (!arr.length) continue;
      const m = arr.reduce((a, b) => a + b, 0) / arr.length;
      const v = Math.sqrt(arr.reduce((a, b) => a + Math.pow(b - m, 2), 0) / Math.max(arr.length - 1, 1));
      stats.push({ label: lab, mean: m, vol: v, count: arr.length });
    }
    stats.sort((a, b) => b.mean - a.mean);
  }
  // compute spans
  const spans: { label: string; start: string; end: string }[] = [];
  const labels = track.labels ?? [];
  if (labels.length) {
    let start = labels[0].date;
    let curr = labels[0].label;
    for (let i = 1; i <= labels.length; i++) {
      const l = labels[i]?.label;
      if (l !== curr) {
        spans.push({ label: curr, start, end: labels[i - 1].date });
        start = labels[i]?.date;
        curr = l;
      }
    }
  }
  return (
    <div className="p-6 space-y-6">
      <Card>
        <ChartFrame
          title="Turning Points — Business Regime Bands"
          traces={[{ x, y, type: "scatter", mode: "lines", name: "Composite (z)" }]}
          layout={{ shapes }}
          height={460}
        />
      </Card>
      <RegimeLegend />
      <Card className="p-4">
        <div className="font-medium mb-2">Regime Spans</div>
        <div className="text-xs">
          {spans.length === 0 ? (
            <div>No bands available.</div>
          ) : (
            <table className="w-full">
              <thead>
                <tr>
                  <th className="text-left">Label</th>
                  <th className="text-left">Start</th>
                  <th className="text-left">End</th>
                </tr>
              </thead>
              <tbody>
                {spans.slice(-12).map((s, i) => (
                  <tr key={i} className="border-t border-border/50">
                    <td className="py-1">{s.label}</td>
                    <td>{s.start}</td>
                    <td>{s.end}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>
      </Card>
      <Card className="p-4">
        <div className="font-medium mb-2">HMM State Characteristics</div>
        <div className="text-xs">
          {stats.length === 0 ? (
            <div>No HMM state info found.</div>
          ) : (
            <table className="w-full">
              <thead>
                <tr>
                  <th className="text-left">State</th>
                  <th className="text-right">Mean z</th>
                  <th className="text-right">Volatility (σ)</th>
                  <th className="text-right">Observations</th>
                </tr>
              </thead>
              <tbody>
                {stats.map((s) => (
                  <tr key={s.label} className="border-top border-border/50">
                    <td className="py-1">{s.label}</td>
                    <td className="text-right">{s.mean.toFixed(2)}</td>
                    <td className="text-right">{s.vol.toFixed(2)}</td>
                    <td className="text-right">{s.count}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>
      </Card>
    </div>
  );
}
