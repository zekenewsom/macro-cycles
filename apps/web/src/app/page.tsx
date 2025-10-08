import Link from "next/link";
import { Card } from "@/components/ui/card";
import CompositeChart from "@/components/CompositeChart";
import { j } from "@/lib/api";
import type { CompositeResponse, PillarsResponse, MoversResponse, MarketRegimesResp } from "@/lib/types";

export const dynamic = "force-dynamic";

export default async function Page() {
  const [comp, pillars, movers, regimes] = await Promise.all([
    j<CompositeResponse>("/overview/composite"),
    j<PillarsResponse>("/overview/pillars"),
    j<MoversResponse>("/overview/movers"),
    j<MarketRegimesResp>("/market/regimes?tickers=SPX,UST2Y,UST10Y,TWEXB,GOLD,BTC"),
  ]);

  return (
    <div className="p-6 space-y-6">
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
        <Card className="p-4">Composite: {comp.series.at(-1)?.z.toFixed(2)}</Card>
        <Card className="p-4">Regime: {comp.series.at(-1)?.regime}</Card>
        <Card className="p-4">Data Vintage: {comp.meta.data_vintage}</Card>
      </div>

      <Card className="p-4">
        <CompositeChart data={comp.series} />
      </Card>

      <div className="grid grid-cols-1 md:grid-cols-3 lg:grid-cols-5 gap-4">
        {pillars.pillars.map((p) => (
          <Link key={p.pillar} href="/drivers">
            <Card className="p-4 hover:bg-accent cursor-pointer">
              <div className="text-sm text-[--muted-foreground]">{p.pillar.toUpperCase()}</div>
              <div className="text-3xl font-semibold">{p.z.toFixed(2)}</div>
              <div className="text-xs">3m mom: {p.momentum_3m.toFixed(2)} • diffusion {(p.diffusion * 100).toFixed(0)}%</div>
            </Card>
          </Link>
        ))}
      </div>

      <Card className="p-4">
        <div className="font-medium mb-2">Market Regimes</div>
        <div className="grid grid-cols-1 md:grid-cols-3 lg:grid-cols-6 gap-3">
          {regimes.assets.map((a) => (
            <Card key={a.ticker} className="p-3">
              <div className="text-xs text-[--muted-foreground]">{a.ticker}</div>
              <div className="text-sm">{a.label}</div>
            </Card>
          ))}
        </div>
      </Card>

      <Card className="p-4">
        <div className="font-medium mb-2">Weekly Movers</div>
        <pre className="text-xs">{JSON.stringify(movers, null, 2)}</pre>
      </Card>
    </div>
  );
}
