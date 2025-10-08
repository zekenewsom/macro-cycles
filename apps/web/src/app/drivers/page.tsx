import { Card } from "@/components/ui/card";
import Heatmap from "@/components/Heatmap";
import Waterfall from "@/components/Waterfall";
import { getDriversPillars, getIndicatorHeatmap, getContributions } from "@/lib/api";

export const dynamic = "force-dynamic";

export default async function Page(){
  const pillarsResp = await getDriversPillars();
  const firstPillar = (pillarsResp.pillars?.[0]?.pillar) ?? "growth";
  const [heat, contrib] = await Promise.all([
    getIndicatorHeatmap(firstPillar),
    getContributions()
  ]);

  return (
    <div className="p-6 space-y-6">
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
        {(pillarsResp.pillars ?? []).map((p:any)=> (
          <Card key={p.pillar} className="p-4">
            <div className="text-xs text-[--muted-foreground]">{String(p.pillar).toUpperCase()}</div>
            <div className="text-3xl font-semibold">{p.z != null ? Number(p.z).toFixed(2) : "—"}</div>
            {p.diffusion != null && (
              <div className="text-xs">diffusion {Math.round(Number(p.diffusion)*100)}%</div>
            )}
          </Card>
        ))}
      </div>

      <Card className="p-4">
        <div className="font-medium mb-2">Indicator Heatmap — {firstPillar}</div>
        <Heatmap dates={heat.dates ?? []} series={heat.series ?? []} matrix={heat.matrix ?? []} />
      </Card>

      <Card className="p-4">
        <Waterfall items={contrib.items ?? []} />
      </Card>
    </div>
  );
}

