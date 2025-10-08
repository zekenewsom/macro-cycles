# Repository Guidelines

## Project Structure
- `apps/api`: FastAPI service (`apps/api/main.py`).
- `apps/web`: Next.js App Router UI (Tailwind + shadcn/ui + Plotly).
- `orchestration/flows`: Prefect flows for ingest/build (`ingest_sources.py`, `build_indicators.py`, `build_pillars.py`, `build_composite.py`).
- `libs/py`: Shared Python utilities (e.g., `indicators.py`).
- `config/*.yaml`: Source list, indicator pipelines, pillar weights.
- `data/`: Parquet lake (`raw/`, `indicators/`, `pillars/`, `composite/`).
- `db/`: DuckDB catalog (local only).

## Build & Dev Commands
- Prereqs: Python 3.12, Poetry, Node 18+.
- Install: `make install` (Poetry + web dependencies).
- Pipeline (local-only): `make pipeline` → ingest (FRED/yf) → indicators → pillars → composite.
- API (reload): `make api` → http://127.0.0.1:8000
- Web (Next.js): `make web` → http://localhost:3000
- Key endpoints: `/overview/{composite,pillars,movers,market/regimes}`, `/drivers/{pillars,indicator-heatmap,contributions}`.

## Coding Style & Conventions
- Python: PEP8, 4 spaces, `snake_case` modules, `PascalCase` classes, typed where useful.
- Keep endpoints thin; prefer pure functions/utilities in `libs/py`.
- YAML configs drive pipelines; avoid hard-coding series lists in code.

## Data & Safety
- No demo fallbacks. If data is missing, endpoints return empty arrays with `meta.warnings`.
- Secrets: copy `.env.example` → `.env`; set `FRED_API_KEY`. Do not commit secrets.
- Parquet under `data/` is disposable; regenerate via `make pipeline`.

## Testing & Verification
- Smoke checks: `curl /health`, `/overview/*`, and `/drivers/*` while API runs.
- Python tests (optional): place `tests/test_*.py`; run `poetry run pytest -q`.

## Commits & PRs
- Conventional Commits (e.g., `feat(api): drivers heatmap` / `fix(flow): null-safe contributions`).
- PRs include: scope, run steps (Makefile targets), screenshots (web) or sample JSON (API), and config changes.
