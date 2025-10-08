import ChartFrame from "@/components/viz/ChartFrame";
import { KPI } from "@/components/viz/KPI";
import { buildRegimeShapes } from "@/lib/regimes";
import { Card } from "@/components/ui/card";
import Link from "next/link";
import { j } from "@/lib/api";
import type { CompositeResponse, PillarsResponse, MoversResponse, MarketRegimesResp } from "@/lib/types";
import Waterfall from "@/components/Waterfall";
import MacroDial from "@/components/viz/MacroDial";
import ExecutivePillars from "@/components/ExecutivePillars";
import RecessionGauge from "@/components/viz/RecessionGauge";
import CompositeMethodToggle from "@/components/CompositeMethodToggle";
import HorizonToggle from "@/components/HorizonToggle";
import DashboardLegend from "@/components/DashboardLegend";

export const dynamic = "force-dynamic";

export default async function Page(props: any) {
  const sp = props?.searchParams && typeof props.searchParams.then === "function" ? await props.searchParams : (props?.searchParams ?? {});
  const method = (sp?.method ?? "weighted");
  const chg = (sp?.chg ?? "1m") as string;
  const months = chg === "6m" ? 6 : chg === "3m" ? 3 : 1;
  const [comp, pillars, movers, regimes, bizTrack, contrib, recess] = await Promise.all([
    j<CompositeResponse>(`/overview/composite?method=${method}`),
    j<PillarsResponse>("/overview/pillars"),
    j<MoversResponse>("/overview/movers"),
    j<MarketRegimesResp>("/market/regimes?tickers=SPX,UST2Y,UST10Y,TWEXB,GOLD,BTC"),
    j<any>("/turning-points/track?name=business"),
    j<any>(`/drivers/contributions?horizon_months=${months}`),
    j<any>("/risk/recession"),
  ]);

  const x = comp.series.map((d) => d.date);
  const y = comp.series.map((d) => d.z);
  const shapes = buildRegimeShapes(bizTrack.labels ?? []);
  const last = comp.series.at(-1)!;

  const delta = (last?.z ?? 0) - (comp.series.at(-2)?.z ?? 0);
  const narrative = `The macro environment appears to be in a ${last?.regime ?? "—"} phase. Composite is ${last?.z?.toFixed?.(2) ?? "—"} (${delta > 0 ? "+" : ""}${delta.toFixed(2)} m/m).`;

  return (
    <div className="p-6 space-y-6">
      <Card>
        <ChartFrame
          title="Composite Cycle Score"
          subtitle={`Method: ${method} • with business HMM regime bands`}
          traces={[{ x, y, type: "scatter", mode: "lines", name: "Composite (z)" }]}
          layout={{ shapes }}
          height={420}
          filename="composite"
        />
      </Card>
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4 items-stretch">
        <Card className="p-4 col-span-1 lg:col-span-1">
          <div className="text-sm text-muted-foreground mb-1">Composite</div>
          <MacroDial value={last?.z} delta={delta} regime={last?.regime} />
          <div className="text-sm mt-2">{narrative}</div>
        </Card>
        <Card className="p-2 col-span-1 lg:col-span-2">
          <div className="p-2 pb-0 flex items-center justify-between">
            <div className="text-sm text-muted-foreground">Pillars — z and 3m momentum</div>
            <CompositeMethodToggle />
          </div>
          <ExecutivePillars pillars={pillars.pillars as any} />
        </Card>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
        <Card className="p-4 col-span-1 lg:col-span-2">
          <div className="flex items-start justify-between gap-4 mb-2">
            <div className="font-medium">What Changed — Composite Contributions ({chg})</div>
            {/* Inline movers (small screens show, large screens optional) */}
            <div className="text-xs text-muted-foreground md:hidden">
              <div className="font-medium text-foreground mb-1">Top Movers</div>
              <div className="grid grid-cols-2 gap-x-4 gap-y-1 max-h-28 overflow-auto">
                {[...(movers.gainers ?? []), ...(movers.losers ?? [])]
                  .sort((a: any, b: any) => Math.abs(b.delta) - Math.abs(a.delta))
                  .slice(0, 8)
                  .map((m: any) => (
                    <div key={`${m.id}-${m.delta}`} className="flex items-center justify-between">
                      <span className="truncate" title={m.label || m.id}>{m.label || m.id}</span>
                      <span className={`ml-2 tabular-nums ${m.delta >= 0 ? "text-green-600" : "text-red-600"}`}>
                        {(m.delta > 0 ? "+" : "") + (m.delta ?? 0).toFixed(2)}
                      </span>
                    </div>
                  ))}
              </div>
            </div>
          </div>
          <div className="flex items-center justify-between mb-2">
            <div className="text-xs text-muted-foreground">Contributions computed as weight × Δpillar over {chg}</div>
            <div className="hidden md:block"><HorizonToggle /></div>
          </div>
          <Waterfall items={contrib.items ?? []} />
        </Card>
        <Card className="p-4 col-span-1">
          <div className="font-medium mb-2">Recession Risk</div>
          <RecessionGauge prob={recess?.prob} />
          <div className="mt-4">
            <div className="text-sm font-medium mb-2">Key Market States</div>
            {(!regimes.assets || !regimes.assets.length) && (
              <div className="text-xs text-muted-foreground mb-2">No market state data found. Ensure parquet files exist under <code>data/raw/market</code>.</div>
            )}
            <div className="grid grid-cols-2 gap-2">
              {regimes.assets.map((a) => (
                <Card key={a.ticker} className="p-2 space-y-1">
                  <div className="text-xs text-muted-foreground">{a.ticker}</div>
                  <div className="text-sm">{a.label}</div>
                  {Array.isArray(a.probas) && (
                    <div className="flex gap-1 text-[10px]">
                      {a.probas.map((p: any) => (
                        <span key={p.state} className="px-1.5 py-0.5 rounded bg-muted">
                          {p.state.replace("trend_","")}: {(Number(p.p) * 100).toFixed(0)}%
                        </span>
                      ))}
                    </div>
                  )}
                </Card>
              ))}
            </div>
          </div>
          <div className="mt-4">
            <DashboardLegend />
          </div>
        </Card>
      </div>

      
    </div>
  );
}
