import { Card } from "@/components/ui/card";
import ChartFrame from "@/components/viz/ChartFrame";

export const dynamic = "force-dynamic";

function ribbonShapes(labels: { date: string; label: string }[], mode: "heur" | "hmm") {
  const shapes: any[] = [];
  if (!labels?.length) return shapes;
  let start = labels[0].date,
    curr = labels[0].label;
  const color = (lab: string) => {
    if (mode === "hmm") {
      if (lab.includes("Trend-Up")) return "rgba(0,200,0,0.10)";
      if (lab.includes("Trend-Down")) return "rgba(200,0,0,0.10)";
      return "rgba(150,150,150,0.08)";
    }
    if (lab.includes("Trend-Up")) return "rgba(0,200,0,0.10)";
    if (lab.includes("Trend-Down")) return "rgba(200,0,0,0.10)";
    return "rgba(150,150,150,0.06)";
  };
  for (let i = 1; i <= labels.length; i++) {
    const l = labels[i]?.label,
      d = labels[i]?.date;
    if (l !== curr) {
      shapes.push({ type: "rect", xref: "x", yref: "paper", x0: start, x1: labels[i - 1]?.date, y0: 0, y1: 1, line: { width: 0 }, fillcolor: color(curr) });
      start = d as any;
      curr = l as any;
    }
  }
  return shapes;
}

export default async function Page(props: any) {
  const sp = props?.searchParams && typeof props.searchParams.then === "function" ? await props.searchParams : (props?.searchParams ?? {});
  const ids = (sp?.tickers ?? "SPX,UST2Y,UST10Y,TWEXB,GOLD,BTC").toUpperCase();
  const mode = sp?.mode === "hmm" ? "hmm" : "auto";
  const [hist, hmm, mats] = await Promise.all([
    fetch(`${process.env.NEXT_PUBLIC_API_BASE}/market/regimes/history?tickers=${ids}&mode=${mode}`, { cache: "no-store" }).then((r) => r.json()),
    fetch(`${process.env.NEXT_PUBLIC_API_BASE}/market/regimes/hmm?tickers=${ids}`, { cache: "no-store" }).then((r) => r.json()),
    fetch(`${process.env.NEXT_PUBLIC_API_BASE}/market/regimes/${mode === "hmm" ? "hmm/" : ""}transition-matrix?tickers=${ids}`, { cache: "no-store" }).then((r) => r.json()),
  ]);

  const hs = Object.fromEntries((hist.assets ?? []).map((a: any) => [a.ticker, a]));
  const hinfo = Object.fromEntries((hmm.items ?? []).map((x: any) => [x.ticker, x]));
  const tm = Object.fromEntries((mats.items ?? []).map((m: any) => [m.ticker, m]));

  const tickers = ids.split(",");

  return (
    <div className="p-6 space-y-6">
      <Card className="p-4">
        <form className="flex gap-2">
          <select name="mode" defaultValue={mode} className="bg-transparent border rounded-md px-3 py-2">
            <option value="auto">Auto (prefer HMM)</option>
            <option value="hmm">HMM only</option>
          </select>
          <input name="tickers" defaultValue={ids} className="bg-transparent border rounded-md px-3 py-2" />
          <button className="px-3 py-2 border rounded-md" type="submit">
            Apply
          </button>
        </form>
      </Card>

      <Card className="p-4">
        <div className="font-medium mb-2">Regime Ribbons</div>
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
          {tickers.map((id) => {
            const labels = hs[id]?.points ?? [];
            const x = labels.map((p: any) => p.date);
            return (
              <Card key={id} className="p-2">
                <ChartFrame
                  title={id}
                  traces={[{ x, y: x.map(() => 0), type: "scatter", mode: "lines", name: "", line: { width: 0.5 } } as any]}
                  layout={{ shapes: ribbonShapes(labels, mode === "hmm" ? "hmm" : "heur"), yaxis: { visible: false }, showlegend: false } as any}
                  height={140}
                />
              </Card>
            );
          })}
        </div>
      </Card>

      {mode === "hmm" && (
        <Card className="p-4">
          <div className="font-medium mb-2">State Probabilities (HMM)</div>
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
            {tickers.map((id) => {
              const it = hinfo[id];
              if (!it?.probs?.length) return (
                <Card key={id} className="p-2">
                  <div className="p-2 text-sm">{id}: no HMM</div>
                </Card>
              );
              const traces = it.probs.map((s: any) => ({
                x: s.points.map((p: any) => p.date),
                y: s.points.map((p: any) => p.p),
                stackgroup: "one",
                type: "scatter",
                mode: "lines",
                name: s.state,
              }));
              return (
                <Card key={id} className="p-2">
                  <ChartFrame title={`${id} — P(states)`} traces={traces as any} height={200} />
                </Card>
              );
            })}
          </div>
        </Card>
      )}

      <Card className="p-4">
        <div className="font-medium mb-2">Transition Matrices (last ~2y)</div>
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
          {tickers.map((id) => {
            const m = tm[id];
            return (
              <ChartFrame
                key={id}
                title={id}
                traces={[
                  { z: m?.matrix ?? [], x: m?.states ?? [], y: m?.states ?? [], type: "heatmap", hovertemplate: "%{y}→%{x}: %{z:.2f}<extra></extra>" } as any,
                ]}
                height={320}
              />
            );
          })}
        </div>
      </Card>
    </div>
  );
}
