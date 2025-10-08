"use client";
import * as React from "react";
import { useRouter } from "next/navigation";
import { Command } from "cmdk";
import { Dialog, DialogContent } from "@/components/ui/dialog";

export default function CommandPalette() {
  const [open, setOpen] = React.useState(false);
  const router = useRouter();

  React.useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === "k") {
        e.preventDefault();
        setOpen((o) => !o);
      }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, []);

  const go = (path: string) => {
    setOpen(false);
    router.push(path);
  };

  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <DialogContent className="p-0 overflow-hidden">
        <Command label="Command Menu">
          <Command.Input placeholder="Search pages…" />
          <Command.List>
            <Command.Empty>No results.</Command.Empty>
            <Command.Group heading="Pages">
              <Command.Item onSelect={() => go("/")}>Overview</Command.Item>
              <Command.Item onSelect={() => go("/drivers")}>Drivers</Command.Item>
              <Command.Item onSelect={() => go("/turning-points")}>Turning Points</Command.Item>
              <Command.Item onSelect={() => go("/explain")}>Explain</Command.Item>
              <Command.Item onSelect={() => go("/data")}>Data Browser</Command.Item>
              <Command.Item onSelect={() => go("/note")}>Monthly Note</Command.Item>
            </Command.Group>
          </Command.List>
        </Command>
      </DialogContent>
    </Dialog>
  );
}

