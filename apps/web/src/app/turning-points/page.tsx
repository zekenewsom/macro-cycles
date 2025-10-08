import { Card } from "@/components/ui/card";
import ChartFrame from "@/components/viz/ChartFrame";
import { buildRegimeShapes } from "@/lib/regimes";
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
    </div>
  );
}

