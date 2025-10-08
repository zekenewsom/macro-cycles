# Macro Cycles

Local, YAML‑driven macro/market pipelines with a FastAPI backend and a Next.js UI. Data is stored as Parquet under `data/` and queried via DuckDB. No demo fallbacks — if data is missing, API responses include `meta.warnings` and return empty arrays.

## Quickstart
- Prereqs: Python 3.12, Poetry, Node 18+, npm.
- Clone, then from repo root:
  - `make install` (Poetry + web deps)
  - `cp .env.example .env` and set `FRED_API_KEY` (optional but recommended)
  - `make pipeline` (ingest → indicators → pillars → composite)
  - Terminal A: `make api` → http://127.0.0.1:8000
  - Terminal B: `make web` → http://localhost:3000

## Make Targets
- `make install`: Install backend and web dependencies.
- `make ingest`: Prefect flows pull FRED + yfinance → `data/raw/`.
- `make build-indicators`: Apply pipelines + rolling z → `data/indicators/`.
- `make build-pillars`: Aggregate pillar z + diffusion → `data/pillars/pillars.parquet`.
- `make build-composite`: Weighted composite + contributions → `data/composite/*.parquet`.
- `make pipeline`: Runs the above in order.
- `make api`: FastAPI with reload.
- `make web`: Next.js dev server.

## API Overview (local)
- Health: `GET /health`
- Overview:
  - `GET /overview/composite` (reads `data/composite/composite.parquet`)
  - `GET /overview/pillars` (latest per pillar; computes `momentum_3m` on the fly)
  - `GET /overview/movers` (latest z‑delta per indicator; ignores non‑data files)
  - `GET /market/regimes?tickers=SPX,UST10Y,GOLD,...` (MA/vol heuristic + sparkline)
- Drivers/Data Browser:
  - `GET /drivers/pillars`
  - `GET /drivers/indicator-heatmap?pillar=growth`
  - `GET /drivers/contributions`

## Configuration
- `config/sources.yaml` — external series (FRED ids, yfinance tickers).
- `config/indicators.yaml` — indicator definitions: `series_id`, `label`, `pillar`, `pipeline` (e.g., `pct_change`, `diff`, `ema`) and z‑score settings.
- `config/pillars.yaml` — list of pillar names.
- `config/composite.yaml` — `pillar_weights` for composite and contributions.

## Project Layout
- `apps/api`: FastAPI service (`apps/api/main.py`).
- `apps/web`: Next.js app (App Router, Tailwind, shadcn/ui, Plotly).
- `orchestration/flows`: Prefect flows for ingest/build.
- `libs/py`: utilities (`indicators.py`).
- `data/`: Parquet lake; safe to delete/regenerate.
- `db/`: DuckDB catalog (local only).

## Notes & Troubleshooting
- Pydantic/Prefect warnings during flow startup are harmless.
- `aiosqlite CancelledError` when a Prefect temp server shuts down can be ignored.
- API never returns synthetic data; check `meta.warnings` for missing parquet hints.
- If movers 500s, ensure indicators exist and that `data/indicators/_catalog.parquet` is ignored (the API already uses `union_by_name=true` and filters nulls).

## Contributing
See `AGENTS.md` for contributor guidelines, coding style, and PR conventions.

