"use client";
import dynamic from "next/dynamic";

const Plot = dynamic(() => import("react-plotly.js"), { ssr: false });

export default function Waterfall({ items }:{items:{pillar:string, weight:number, delta:number, contribution:number}[]}){
  const x = items.map(i=>i.pillar);
  const y = items.map(i=>i.contribution);
  return (
    <Plot
      data={[{ type:"bar", x, y }]}
      layout={{ autosize:true, margin:{l:40,r:20,t:10,b:40}, title:"Contributions (weight × Δpillar)" }}
      useResizeHandler style={{width:"100%", height:320}} config={{displaylogo:false}}
    />
  );
}

