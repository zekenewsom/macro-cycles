# Macro Cycles — Agent Plan & Progress

This file tracks scope, plan, progress, and how to run things during development.

## Project Structure
- `apps/api`: FastAPI service (`apps/api/main.py`).
- `apps/web`: Next.js App Router UI (Tailwind + shadcn/ui + Plotly).
- `orchestration/flows`: Prefect flows (ingest/build/explain/regimes/note).
- `libs/py`: Shared Python utilities and transforms.
- `config/*.yaml`: Sources, indicator pipelines, pillar weights.
- `data/`: Parquet lake (raw, indicators, pillars, composite, regimes, artifacts, vintages).
- `db/`: DuckDB catalog (local only).

## Current Status (2025‑10‑08)
Delivered
- Explainability: indicator snapshot, pillar contribution TS, probit placeholder; endpoints + UI page.
- Monthly Note: generated markdown with composite, regimes, movers, breaks.
- Data endpoints: `/data/catalog`, `/data/series` (supports `vintage=YYYY-MM-DD`).
- Market endpoints: `/market/summary` (KPIs + sparkline), `/market/assets` (TS).
- Regimes flow: HMM over composite + per‑pillar → `data/regimes/` and `/turning-points/track` endpoint.
- Composite variants: weighted z (primary), median, trimmed mean, diffusion composite.
- Indicator robustness: frequency canonicalization (M/W/D), DQ gates (`data/_dq/*.json`, `/meta/dq`).
- Web pages: Overview (waterfall), Drivers, Explain, Markets, Turning Points, Data, Monthly Note.
- Global UI: centered navbar (mobile sheet), ⌘K palette, meta strip (data vintage + warnings).

In progress / Next
- ALFRED vintages (flow exists; needs backtest UI options).
- New free sources: BEA, BLS, H.4.1, Treasury, CoinGecko adapters.
- Recession probit (proper fit + coefficients + marginal effects artifact).
- Market regimes: MA/vol/HMM hybrids + transitions.
- Data Browser filters and drill (pillar/freq/source/tags; right metadata pane).
- Pydantic response models; time‑frame toggle for waterfall; toasts for `meta.warnings`.
- Tests (contract/transform/DQ) and README troubleshooting.

## How to Run
- Install: `make install` (Python 3.12, Poetry, Node 18+)
- Pipeline: `make pipeline` (ingest → indicators → pillars → composite → regimes → explain → note)
- Vintage (optional): `make alfred`
- API: `make api` → http://127.0.0.1:8000
- Web: `make web` → http://localhost:3000

## Notable Endpoints
- Overview: `/overview/{composite,pillars,movers}`, `/market/regimes`
- Explain: `/explain/{indicator-snapshot,pillar-contrib-timeseries,probit-effects}`
- Note: `/note/monthly`
- Turning points: `/turning-points/track?name=business`
- Data: `/data/{catalog,series?series_id=…&vintage=YYYY-MM-DD}`
- Markets: `/market/{summary,assets?ids=…}`
- Meta: `/meta/dq`

## Coding Conventions
- Python: PEP8, typed where useful, thin endpoints; push logic into `libs/py`.
- YAML configs drive pipelines; avoid hardcoded lists in code.
- No demo fallbacks — return empty arrays + `meta.warnings`.

## PRs & Commits
- Conventional Commits; include scope, Makefile steps, screenshots (web) or sample JSON (API), and config changes.
