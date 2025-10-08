"use client";
import dynamic from "next/dynamic";

const Plot = dynamic(() => import("react-plotly.js"), { ssr: false });

export default function Heatmap({ dates, series, matrix }:{dates:string[]; series:string[]; matrix:(number|null)[][]}){
  return (
    <Plot
      data={[{ z: matrix, x: dates, y: series, type: "heatmap", zsmooth: false, showscale: true }]}
      layout={{ autosize: true, margin: {l:120,r:20,t:10,b:40}, yaxis:{autorange:"reversed"} }}
      useResizeHandler style={{width:"100%", height:500}} config={{displaylogo:false}}
    />
  );
}

