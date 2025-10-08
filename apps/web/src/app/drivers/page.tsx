import { Card } from "@/components/ui/card";
import ChartFrame from "@/components/viz/ChartFrame";
import DataGrid from "@/components/viz/DataGrid";
import { j } from "@/lib/api";

export const dynamic = "force-dynamic";

export default async function Page({ searchParams }: { searchParams: Promise<{ p?: string }> }) {
  const sp = await searchParams;
  const pSel = sp?.p ?? "growth";

  const [pillarsResp, heat, contrib, snap] = await Promise.all([
    j<any>("/drivers/pillars"),
    j<any>(`/drivers/indicator-heatmap?pillar=${pSel}&last_months=120`),
    j<any>("/drivers/contributions"),
    j<any>("/explain/indicator-snapshot?top_k=30"),
  ]);

  const heatTrace = [
    {
      z: heat.matrix,
      x: heat.dates,
      y: heat.series_labels ?? heat.series,
      type: "heatmap",
      zsmooth: false,
      showscale: true,
      zmin: -2.5,
      zmax: 2.5,
      colorscale: "RdBu",
      reversescale: true,
      hovertemplate: "%{y}<br>%{x}<br>z=%{z:.2f}<extra></extra>",
    } as any,
  ];

  const contribTrace = [
    {
      type: "bar",
      x: (contrib.items ?? []).map((i: any) => i.pillar),
      y: (contrib.items ?? []).map((i: any) => i.contribution),
      text: (contrib.items ?? []).map((i: any) => (i.delta ?? 0).toFixed(2)),
      textposition: "auto",
      hovertemplate: "%{x}: %{y:.3f}<extra></extra>",
    },
  ];

  return (
    <div className="p-6 space-y-6">
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
        {(pillarsResp.pillars ?? []).map((p: any) => (
          <a key={p.pillar} href={`/drivers?p=${p.pillar}`}>
            <Card className={`p-4 ${p.pillar === pSel ? "ring-2 ring-primary" : ""}`}>
              <div className="text-xs text-muted-foreground">{String(p.pillar).toUpperCase()}</div>
              <div className="text-3xl font-semibold">{p.z != null ? Number(p.z).toFixed(2) : "—"}</div>
              {p.diffusion != null && (
                <div className="text-xs">diffusion {Math.round(Number(p.diffusion) * 100)}%</div>
              )}
            </Card>
          </a>
        ))}
      </div>

      <Card>
        <ChartFrame title={`Indicator Heatmap — ${pSel}`} traces={heatTrace as any} height={520} />
      </Card>

      <Card>
        <ChartFrame title="Composite Contributions (weight × Δpillar)" traces={contribTrace as any} height={360} />
      </Card>

      <DataGrid
        title="Top Indicator Drivers (latest)"
        data={snap.items ?? []}
        columns={[
          { header: "Pillar", accessorKey: "pillar" },
          { header: "Indicator", accessorKey: "label" },
          { header: "z", accessorKey: "indicator_z" },
          { header: "Est. Composite", accessorKey: "composite_contrib_est" },
          { header: "Series ID", accessorKey: "series_id" },
          { header: "Source", accessorKey: "source" },
        ]}
      />
    </div>
  );
}
