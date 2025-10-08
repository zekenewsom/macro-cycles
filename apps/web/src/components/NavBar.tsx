"use client";
import Link from "next/link";
import { usePathname, useRouter } from "next/navigation";
import { cn } from "@/lib/utils";
import { Button } from "@/components/ui/button";
import { Sheet, SheetContent, SheetTrigger } from "@/components/ui/sheet";
import { Menu, Command } from "lucide-react";
import * as React from "react";

const items = [
  { href: "/", label: "Overview" },
  { href: "/drivers", label: "Drivers" },
  { href: "/explain", label: "Explain" },
  { href: "/markets", label: "Markets" },
  { href: "/turning-points", label: "Turning Points" },
  { href: "/data", label: "Data" },
  { href: "/note", label: "Monthly Note" },
];

export default function NavBar() {
  const pathname = usePathname() || "/";
  const router = useRouter();
  const [open, setOpen] = React.useState(false);

  React.useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === "k") {
        e.preventDefault();
        // Layout-level CommandPalette handles toggling; hint only here
      }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, []);

  const NavLinks = ({ onNavigate }: { onNavigate?: () => void }) => (
    <ul className="flex items-center gap-2 text-sm">
      {items.map((it) => {
        const active = it.href === "/" ? pathname === "/" : pathname.startsWith(it.href);
        return (
          <li key={it.href}>
            <Link
              href={it.href}
              onClick={() => onNavigate?.()}
              className={cn(
                "px-3 py-1.5 rounded-md",
                "transition-colors",
                active
                  ? "text-primary bg-primary/10 ring-1 ring-primary/50"
                  : "text-foreground/80 hover:text-foreground hover:bg-muted/60"
              )}
            >
              {it.label}
            </Link>
          </li>
        );
      })}
    </ul>
  );

  return (
    <div className="sticky top-0 z-40 backdrop-blur supports-[backdrop-filter]:bg-background/70 border-b">
      <div className="mx-auto max-w-screen-2xl px-4">
        <div className="grid h-14 grid-cols-[1fr_auto_1fr] items-center gap-3">
          {/* Brand (left) */}
          <div className="flex items-center">
            <Link href="/" className="font-semibold tracking-tight text-lg">
              Macro Cycles
            </Link>
          </div>

          {/* Centered desktop nav */}
          <nav className="hidden md:block justify-self-center">
            <NavLinks />
          </nav>

          {/* Actions (right) */}
          <div className="justify-self-end hidden md:flex items-center gap-2">
            <Button
              variant="outline"
              size="sm"
              title="Command Palette (⌘K)"
              onClick={() => {
                const ev = new KeyboardEvent("keydown", { key: "k", metaKey: true });
                window.dispatchEvent(ev);
              }}
            >
              <Command className="h-4 w-4 mr-2" />
              ⌘K
            </Button>
          </div>

          {/* Mobile menu (overrides right area on small screens) */}
          <div className="md:hidden col-start-3 justify-self-end">
            <Sheet open={open} onOpenChange={setOpen}>
              <SheetTrigger asChild>
                <Button variant="outline" size="sm">
                  <Menu className="h-4 w-4" />
                </Button>
              </SheetTrigger>
              <SheetContent side="left" className="w-64">
                <div className="mt-8">
                  <NavLinks onNavigate={() => setOpen(false)} />
                </div>
              </SheetContent>
            </Sheet>
          </div>
        </div>
      </div>
      <div className="h-[1px] w-full bg-gradient-to-r from-transparent via-primary/30 to-transparent" />
    </div>
  );
}
