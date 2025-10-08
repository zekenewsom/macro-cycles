import { Card } from "@/components/ui/card";
import PillarContribTS from "@/components/viz/PillarContribTS";
import { j } from "@/lib/api";

export const dynamic = "force-dynamic";

// client chart component imported above

export default async function Page() {
  const [snapRes, tserRes] = await Promise.allSettled([
    j<{ items: any[] }>("/explain/indicator-snapshot?top_k=12"),
    j<{ series: any[] }>("/explain/pillar-contrib-timeseries"),
  ]);
  const snap = snapRes.status === "fulfilled" ? snapRes.value : { items: [] };
  const tser = tserRes.status === "fulfilled" ? tserRes.value : { series: [] };

  return (
    <div className="p-6 space-y-6">
      <Card className="p-4">
        <div className="font-medium mb-2">Pillar Contributions to Composite (time series)</div>
        <PillarContribTS series={tser.series ?? []} />
      </Card>
      <Card className="p-4">
        <div className="font-medium mb-2">Top Indicator Drivers (latest)</div>
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-3">
          {(snap.items ?? []).map((r: any) => (
            <Card key={r.series_id} className="p-3">
              <div className="text-xs text-muted-foreground">{String(r.pillar ?? "").toUpperCase()}</div>
              <div className="text-sm font-medium">{r.label}</div>
              <div className="text-xs">
                z {Number(r.indicator_z).toFixed(2)} • est. comp {Number(r.composite_contrib_est).toFixed(3)}
              </div>
            </Card>
          ))}
        </div>
      </Card>
    </div>
  );
}
