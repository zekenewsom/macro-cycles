"use client";
import { useRouter, useSearchParams } from "next/navigation";
import * as React from "react";
import { Button } from "@/components/ui/button";

const OPTS = [
  { id: "1m", label: "1m", months: 1 },
  { id: "3m", label: "3m", months: 3 },
  { id: "6m", label: "6m", months: 6 },
];

export default function HorizonToggle() {
  const router = useRouter();
  const sp = useSearchParams();
  const current = sp.get("chg") || "1m";
  const onSelect = (id: string) => {
    const params = new URLSearchParams(sp?.toString() || "");
    params.set("chg", id);
    router.replace(`/?${params.toString()}`);
    router.refresh();
  };
  return (
    <div className="flex gap-1">
      {OPTS.map((o) => (
        <Button key={o.id} size="sm" variant={current === o.id ? "default" : "outline"} onClick={() => onSelect(o.id)}>
          {o.label}
        </Button>
      ))}
    </div>
  );
}

