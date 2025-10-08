export function buildRegimeShapes(labels: { date: string; label: string }[], yMin = -10, yMax = 10) {
  const shapes: any[] = [];
  const colors: Record<string, string> = {
    Expansion: "rgba(0,200,0,0.08)",
    "Early-Recovery": "rgba(0,150,200,0.08)",
    "Late-Cycle": "rgba(255,165,0,0.10)",
    Contraction: "rgba(200,0,0,0.10)",
    Tight: "rgba(200,0,0,0.08)",
    Easing: "rgba(0,150,200,0.08)",
    Flood: "rgba(0,200,0,0.08)",
    Neutral: "rgba(150,150,150,0.06)",
    Disinflation: "rgba(0,150,200,0.08)",
    Reflation: "rgba(255,165,0,0.10)",
    Overheat: "rgba(200,0,0,0.10)",
    Stable: "rgba(150,150,150,0.06)",
  };
  if (!labels?.length) return shapes;
  let start = labels[0]?.date;
  let curr = labels[0]?.label;
  for (let i = 1; i <= labels.length; i++) {
    const l = labels[i]?.label;
    const d = labels[i]?.date;
    if (l !== curr) {
      shapes.push({
        type: "rect",
        xref: "x",
        yref: "paper",
        x0: start,
        x1: labels[i - 1]?.date,
        y0: 0,
        y1: 1,
        fillcolor: colors[curr] ?? "rgba(150,150,150,0.05)",
        line: { width: 0 },
      });
      start = d;
      curr = l;
    }
  }
  return shapes;
}

