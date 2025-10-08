"use client";
import * as React from "react";
import PillarBars from "@/components/viz/PillarBars";
import PillarDeepDive from "@/components/PillarDeepDive";

type Pillar = { pillar: string; z: number; momentum_3m?: number | null };

export default function ExecutivePillars({ pillars }: { pillars: Pillar[] }) {
  const [open, setOpen] = React.useState(false);
  const [sel, setSel] = React.useState<string | null>(null);
  return (
    <>
      <PillarBars
        pillars={pillars}
        onClick={(p) => {
          setSel(p);
          setOpen(true);
        }}
      />
      <PillarDeepDive pillar={sel} open={open} onOpenChange={setOpen} />
    </>
  );
}

