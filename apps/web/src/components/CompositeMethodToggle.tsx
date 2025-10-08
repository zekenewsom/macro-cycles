"use client";
import { useRouter, useSearchParams } from "next/navigation";
import * as React from "react";
import { Button } from "@/components/ui/button";

const METHODS = [
  { id: "weighted", label: "Weighted" },
  { id: "median", label: "Median" },
  { id: "trimmed", label: "Trimmed" },
  { id: "diffusion", label: "Diffusion" },
];

export default function CompositeMethodToggle() {
  const router = useRouter();
  const sp = useSearchParams();
  const current = sp.get("method") || "weighted";

  const onSelect = (id: string) => {
    const params = new URLSearchParams(sp?.toString() || "");
    params.set("method", id);
    router.replace(`/?${params.toString()}`);
    router.refresh();
  };

  return (
    <div className="flex flex-wrap gap-2">
      {METHODS.map((m) => (
        <Button
          key={m.id}
          size="sm"
          variant={current === m.id ? "default" : "outline"}
          onClick={() => onSelect(m.id)}
        >
          {m.label}
        </Button>
      ))}
    </div>
  );
}

