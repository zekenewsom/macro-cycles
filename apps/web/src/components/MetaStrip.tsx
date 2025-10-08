"use client";
import * as React from "react";
import { API } from "@/lib/api";

export default function MetaStrip() {
  const [vintage, setVintage] = React.useState<string>("");
  const [version, setVersion] = React.useState<string>("v0");
  const [warnings, setWarnings] = React.useState<string[]>([]);
  React.useEffect(() => {
    (async () => {
      try {
        const r = await fetch(`${API}/overview/composite`, { cache: "no-store" });
        const j = await r.json();
        setVintage(j?.meta?.data_vintage || "");
        setVersion(j?.meta?.config_version || "v0");
      } catch {}
      try {
        const r2 = await fetch(`${API}/meta/dq`, { cache: "no-store" });
        const j2 = await r2.json();
        setWarnings(j2?.meta?.warnings || []);
      } catch {}
    })();
  }, []);
  return (
    <div className="w-full border-b bg-muted/30 text-xs">
      <div className="mx-auto max-w-screen-2xl px-4 py-1 flex items-center gap-3">
        <div>Data vintage: <span className="font-medium">{vintage}</span></div>
        <div>Config: <span className="font-medium">{version}</span></div>
        {warnings?.length ? (
          <div className="text-amber-500">Warnings: {warnings.slice(0, 2).join(" • ")}{warnings.length > 2 ? ` (+${warnings.length - 2})` : ""}</div>
        ) : null}
      </div>
    </div>
  );
}

