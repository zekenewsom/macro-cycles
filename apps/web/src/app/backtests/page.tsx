import { Card } from "@/components/ui/card";
import ChartFrame from "@/components/viz/ChartFrame";

export const dynamic = "force-dynamic";

async function j(path:string){
  const r = await fetch(`${process.env.NEXT_PUBLIC_API_BASE}${path}`, { cache:"no-store" });
  return r.json();
}

export default async function Page({ searchParams }:{ searchParams: Promise<{ method?: string, ticker?: string, cost_bps?: string }> }){
  const sp = await searchParams;
  const method = sp?.method ?? "weighted";
  const ticker = sp?.ticker ?? "SPX";
  const cost_bps = sp?.cost_bps ?? "10";
  const res = await j(`/backtest/composite-threshold?ticker=${ticker}&method=${method}&cost_bps=${cost_bps}`);
  const ser = res.series ?? [];
  const eqBH = ser.reduce((acc:any[], cur:any, i:number)=>{ const prev = acc[i-1]?.y ?? 1; return [...acc, {x: cur.date, y: prev * (1 + (cur.ret ?? 0))}];}, []);
  const eqSig= ser.reduce((acc:any[], cur:any, i:number)=>{ const prev = acc[i-1]?.y ?? 1; return [...acc, {x: cur.date, y: prev * (1 + (cur.ret_sig ?? 0))}];}, []);
  return (
    <div className="p-6 space-y-6">
      <Card className="p-4">
        <form className="grid grid-cols-1 md:grid-cols-5 gap-2">
          <select name="method" defaultValue={method} className="bg-transparent border rounded-md px-3 py-2">
            <option value="weighted">Weighted</option>
            <option value="median">Median</option>
            <option value="trimmed">Trimmed</option>
            <option value="diffusion">Diffusion</option>
          </select>
          <input name="ticker" defaultValue={ticker} className="bg-transparent border rounded-md px-3 py-2" />
          <input name="cost_bps" defaultValue={cost_bps} className="bg-transparent border rounded-md px-3 py-2" />
          <button className="px-3 py-2 border rounded-md" type="submit">Run</button>
        </form>
      </Card>

      <Card className="p-4">
        <div className="font-medium mb-2">Vintage-clean Strategy (Composite z&gt;0)</div>
        <ChartFrame
          title={`${ticker}: Buy&Hold vs Strategy`}
          traces={[
            { x: eqBH.map(p=>p.x), y: eqBH.map(p=>p.y), type:"scatter", mode:"lines", name:"Buy & Hold" },
            { x: eqSig.map(p=>p.x), y: eqSig.map(p=>p.y), type:"scatter", mode:"lines", name:"Strategy" },
          ]}
          height={420}
        />
        <pre className="text-xs mt-2">{JSON.stringify(res.metrics ?? {}, null, 2)}</pre>
      </Card>
    </div>
  );
}
