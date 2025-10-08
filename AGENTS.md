# Macro Cycles — Agent Plan & Progress

This doc tracks scope, plan, progress, and how to run during development.

## Project Structure
- `apps/api`: FastAPI service (`apps/api/main.py`).
- `apps/web`: Next.js App Router UI (Tailwind + shadcn/ui + Plotly).
- `orchestration/flows`: Prefect flows (ingest/build/pillars/composite/regimes/explain/note).
- `libs/py`: Shared Python utilities and transforms.
- `config/*.yaml`: Sources, indicator pipelines, pillar weights, pillars list.
- `data/`: Parquet lake (raw, indicators, pillars, composite, regimes, artifacts, vintages, _dq).
- `db/`: DuckDB catalog (local only).

## Current Status (2025‑10‑08)
Delivered (highlights)
- Markets (HMM): ribbons (history), transitions (+stationary), price overlay, mode toggle; `/market/price-series`, `/market/regimes/hmm`, `/market/regimes/hmm/transition-matrix`, `/market/regimes/transition-matrix`.
- Composite: fixed normalized weighted z (handles missing pillars by date), median/trimmed/diffusion variants; `/overview/composite?method=`.
- Contributions: correct per‑pillar Δ using pillar’s own last two points; “What changed” waterfall uses weight × Δpillar.
- Drivers/Heatmap: monthly end‑of‑month alignment; corrected matrix orientation; human series labels; `last_months` trim.
- Waterfall drill‑down: clicking a pillar opens a dialog of top movers (API: `/overview/movers?pillar=`).
- Data Browser: filters (pillar/freq/source), pagination, metadata pane, vintage “as‑of” marker.
- DQ: producer flow writes `data/_dq/dq.json`; `/meta/dq` flattens and surfaces stale/few‑rows warnings.
- Pydantic: response models on Markets endpoints for clearer API docs.
- UI consistency: Regime legend component; global state colors.

In progress / Next
- Overview polish: 1m/3m/6m “What changed” toggle; composite method legend; show movers inline.
- Markets drill: stationary π chips under matrices; asset side drawer with spans & performance.
- Turning Points v2: server spans endpoint (done) + time‑in‑state bar, use new spans API throughout.
- Global: time‑range control (10y/20y/max) persisted in QS; annotations overlay.
- Sources: BEA, BLS, H.4.1, Treasury minimal adapters; harden CoinGecko.
- Tests: composite weighting and contributions; heatmap contract; markets endpoints (stationary); README troubleshooting.

## How to Run
- Install: `make install` (Python 3.12, Poetry, Node 18+)
- Pipeline: `make pipeline` (ingest → indicators → pillars → composite → market HMM → regimes → explain → note)
- DQ producer: `make dq`
- ALFRED vintages (optional): `make alfred`
- API: `make api` → http://127.0.0.1:8000
- Web: `make web` → http://localhost:3000

## Notable Endpoints
- Overview: `/overview/{composite?method=weighted|median|trimmed|diffusion,pillars,movers}`
- Drivers: `/drivers/{pillars,indicator-heatmap?pillar=&last_months=120,contributions}`
- Data: `/data/{catalog,series?series_id=…&vintage=YYYY-MM-DD}`
- Markets:
  - `/market/{summary,assets}`
  - `/market/price-series?ticker=SPX&downsample=1500`
  - `/market/regimes/history?tickers=&mode=auto|hmm|heur`
  - `/market/regimes/{hmm/}transition-matrix?tickers=…` (includes `stationary`)
- Turning points: `/turning-points/{track?name=business,spans?name=business&max_items=12}`
- Explain: `/explain/{indicator-snapshot,pillar-contrib-timeseries,probit-effects}`
- Note: `/note/monthly`
- Meta: `/meta/dq`

## Coding Conventions
- Python: PEP8, typed where useful, thin endpoints; push logic into `libs/py`.
- YAML configs drive pipelines; avoid hardcoded lists in code.
- No demo fallbacks — return empty arrays + `meta.warnings`.

## PRs & Commits
- Conventional Commits; include scope, Makefile steps, screenshots (web) or sample JSON (API), and config changes.
