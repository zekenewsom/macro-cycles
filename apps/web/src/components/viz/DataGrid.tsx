"use client";
import * as React from "react";
import { useReactTable, getCoreRowModel, flexRender, ColumnDef } from "@tanstack/react-table";
import { Card } from "@/components/ui/card";
import { Button } from "@/components/ui/button";

export default function DataGrid<T>({ data, columns, title }: { data: T[]; columns: ColumnDef<T, any>[]; title?: string }) {
  const table = useReactTable({ data, columns, getCoreRowModel: getCoreRowModel() });
  const toCSV = () => {
    const header = columns.map((c: any) => (c.header as string) ?? "").join(",");
    const rows = (data as any[])
      .map((d: any) => columns.map((c: any) => JSON.stringify(d[c.accessorKey] ?? "")).join(","))
      .join("\n");
    const blob = new Blob([header + "\n" + rows], { type: "text/csv;charset=utf-8;" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = (title || "data") + ".csv";
    a.click();
  };

  return (
    <Card className="p-4">
      <div className="flex items-center justify-between mb-2">
        <div className="font-medium">{title}</div>
        <Button size="sm" variant="outline" onClick={toCSV}>
          Export CSV
        </Button>
      </div>
      <div className="overflow-auto">
        <table className="w-full text-sm">
          <thead className="text-muted-foreground">
            {table.getHeaderGroups().map((hg) => (
              <tr key={hg.id}>
                {hg.headers.map((h) => (
                  <th key={h.id} className="px-2 py-1 text-left">
                    {h.isPlaceholder ? null : flexRender(h.column.columnDef.header, h.getContext())}
                  </th>
                ))}
              </tr>
            ))}
          </thead>
          <tbody>
            {table.getRowModel().rows.map((r) => (
              <tr key={r.id} className="border-t border-border/50">
                {r.getVisibleCells().map((c) => (
                  <td key={c.id} className="px-2 py-1">
                    {flexRender(c.column.columnDef.cell, c.getContext())}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </Card>
  );
}

