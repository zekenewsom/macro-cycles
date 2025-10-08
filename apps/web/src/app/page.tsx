import ChartFrame from "@/components/viz/ChartFrame";
import { KPI } from "@/components/viz/KPI";
import { buildRegimeShapes } from "@/lib/regimes";
import { Card } from "@/components/ui/card";
import Link from "next/link";
import { j } from "@/lib/api";
import type { CompositeResponse, PillarsResponse, MoversResponse, MarketRegimesResp } from "@/lib/types";
import Waterfall from "@/components/Waterfall";

export const dynamic = "force-dynamic";

export default async function Page() {
  const [comp, pillars, movers, regimes, bizTrack, contrib] = await Promise.all([
    j<CompositeResponse>("/overview/composite"),
    j<PillarsResponse>("/overview/pillars"),
    j<MoversResponse>("/overview/movers"),
    j<MarketRegimesResp>("/market/regimes?tickers=SPX,UST2Y,UST10Y,TWEXB,GOLD,BTC"),
    j<any>("/turning-points/track?name=business"),
    j<any>("/drivers/contributions"),
  ]);

  const x = comp.series.map((d) => d.date);
  const y = comp.series.map((d) => d.z);
  const shapes = buildRegimeShapes(bizTrack.labels ?? []);
  const last = comp.series.at(-1)!;

  return (
    <div className="p-6 space-y-6">
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
        <KPI label="Composite (z)" value={last?.z?.toFixed?.(2) ?? "0.00"} delta={(last?.z ?? 0) - (comp.series.at(-2)?.z ?? 0)} spark={y.slice(-60)} />
        <KPI label="Regime" value={last?.regime ?? "N/A"} />
        <KPI label="Data Vintage" value={new Date(comp.meta.data_vintage).toLocaleString()} />
      </div>

      <Card>
        <ChartFrame
          title="Composite Cycle Score"
          subtitle="with business HMM regime bands"
          traces={[{ x, y, type: "scatter", mode: "lines", name: "Composite (z)" }]}
          layout={{ shapes }}
          height={460}
          filename="composite"
        />
      </Card>

      <div className="grid grid-cols-1 md:grid-cols-3 lg:grid-cols-5 gap-4">
        {pillars.pillars.map((p) => (
          <Link href={`/drivers?p=${p.pillar}`} key={p.pillar}>
            <Card className="p-4 hover:shadow-lg transition-shadow cursor-pointer">
              <div className="text-xs text-muted-foreground">{p.pillar.toUpperCase()}</div>
              <div className="text-3xl font-semibold">{p.z.toFixed(2)}</div>
              <div className="text-xs">3m mom {p.momentum_3m.toFixed(2)} • diffusion {(p.diffusion * 100).toFixed(0)}%</div>
            </Card>
          </Link>
        ))}
      </div>

      <Card className="p-4">
        <div className="font-medium mb-2">Market Regimes</div>
        <div className="grid grid-cols-1 md:grid-cols-3 lg:grid-cols-6 gap-3">
          {regimes.assets.map((a) => (
            <Card key={a.ticker} className="p-3">
              <div className="text-xs text-muted-foreground">{a.ticker}</div>
              <div className="text-sm">{a.label}</div>
            </Card>
          ))}
        </div>
      </Card>

      <Card className="p-4">
        <div className="font-medium mb-2">Weekly Movers</div>
        <div className="text-xs overflow-auto">
          <pre>{JSON.stringify(movers, null, 2)}</pre>
        </div>
      </Card>

      <Card className="p-4">
        <div className="font-medium mb-2">What Changed — Composite Contributions</div>
        <Waterfall items={contrib.items ?? []} />
      </Card>
    </div>
  );
}
