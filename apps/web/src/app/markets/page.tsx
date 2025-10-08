import { Card } from "@/components/ui/card";
import { KPI } from "@/components/viz/KPI";
import { j } from "@/lib/api";

export const dynamic = "force-dynamic";

export default async function Page() {
  const ids = ["SPX", "UST2Y", "UST10Y", "TWEXB", "GOLD", "BTC"];
  const res = await j<{ items: { id: string; last?: number; ma50?: number; spark?: number[]; warning?: string }[] }>(
    "/market/summary?ids=" + ids.join(","),
  );
  const map = Object.fromEntries(res.items.map((i) => [i.id, i] as const));
  return (
    <div className="p-6 space-y-6">
      <div className="grid grid-cols-1 md:grid-cols-3 lg:grid-cols-6 gap-4">
        {ids.map((id) => {
          const it = map[id] || ({} as any);
          const label = id === "SPX" ? "S&P 500" : id;
          const val = it.last != null ? it.last.toFixed(2) : "—";
          const delta = it.ma50 != null && it.last != null ? it.last - it.ma50 : undefined;
          return <KPI key={id} label={label} value={String(val)} delta={delta} spark={it.spark} />;
        })}
      </div>
      <Card className="p-4">
        <div className="text-sm text-muted-foreground">Market overview tiles with last vs MA and sparklines</div>
      </Card>
    </div>
  );
}

