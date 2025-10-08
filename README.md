# Macro Cycles

Local, YAML‑driven macro + markets pipelines with a FastAPI backend and a Next.js UI. Data is stored as Parquet under `data/` and queried via DuckDB. No demo fallbacks — if data is missing, API responses include `meta.warnings` and return empty arrays.

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
- `make pipeline`: ingest → indicators → pillars → composite → market HMM → regimes → explain → note.
- `make api`: FastAPI with reload.
- `make web`: Next.js dev server.
- `make alfred`: Fetch ALFRED vintages (optional; requires `FRED_API_KEY`).
- `make build-market-hmm`: Fit per‑asset HMM regimes (downstream of `ingest`).
- `make dq`: Write `data/_dq/dq.json` data‑quality snapshot.

## API Overview (local)
- Health: `GET /health`
- Overview:
  - `GET /overview/composite?method=weighted|median|trimmed|diffusion` (reads `data/composite/composite.parquet`)
  - `GET /overview/pillars` (latest per pillar; computes `momentum_3m` on the fly)
  - `GET /overview/movers` (latest z‑delta per indicator; ignores non‑data files)
- Drivers/Data Browser:
  - `GET /drivers/pillars`
  - `GET /drivers/indicator-heatmap?pillar=growth&last_months=120` (monthly EOM alignment, series labels returned)
  - `GET /drivers/contributions`
  - `GET /data/catalog` (series registry; pillar, freq, coverage)
  - `GET /data/series?series_id=INDPRO[&vintage=YYYY-MM-DD]`
- Explainability & Note:
  - `GET /explain/indicator-snapshot`
  - `GET /explain/pillar-contrib-timeseries`
  - `GET /explain/probit-effects`
  - `GET /note/monthly`
- Turning Points:
  - `GET /turning-points/track?name=business` (HMM regime bands)
  - `GET /turning-points/spans?name=business&max_items=12` (consecutive label runs)
- Markets:
  - `GET /market/summary?ids=SPX,UST2Y,UST10Y,TWEXB,GOLD,BTC`
  - `GET /market/assets?ids=…`
  - `GET /market/price-series?ticker=SPX&downsample=1500`
  - `GET /market/regimes/history?tickers=SPX,...&mode=auto|hmm|heur`
  - `GET /market/regimes/hmm` (labels + stacked probabilities)
  - `GET /market/regimes/{hmm/}transition-matrix?tickers=…` (includes `stationary`)

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

## UI Pages (dev)
- `/` Overview — composite chart with HMM bands; KPIs; “What changed” waterfall; composite method toggle.
- `/drivers` — pillar selector; indicator heatmap (monthly EOM, proper orientation); contributions; drivers grid.
- `/explain` — pillar contributions time series; top indicator tiles.
- `/markets` — HMM ribbons with price overlay; stacked probabilities; transition matrices with stationary π; legend.
- `/turning-points` — composite with regime bands; recent regime spans table; legend.
- `/data` — catalog with filters (pillar/freq/source) + pagination; series preview with vintage “as‑of” marker.
- `/note` — Monthly Macro Note (markdown).

## Notes & Troubleshooting
- Pydantic/Prefect warnings during flow startup are harmless.
- `aiosqlite CancelledError` when a Prefect temp server shuts down can be ignored.
- Next.js 15: some routes need `await props.searchParams` — these are wired where needed (`/`, `/data`, `/markets`).
- Polars string literals: when building categorical labels in expressions use `pl.lit("text")` (fixed in markets heuristic).
- Composite (weighted) nulls: normalized weighting is implemented to avoid null propagation when a date misses some pillars.
- Contributions delta: computed per pillar using that pillar’s own last two observations (not global last row).
- Heatmap orientation/alignment: API returns S×D (series × dates) matrix with monthly EOM alignment; UI uses diverging palette and correct orientation.
- HMM artifacts: require sufficient raw history (min_rows ~300 in the HMM flow). Use `make ingest` (full yfinance history) then `make build-market-hmm`.
- If ALFRED vintages are requested in `/data/series` with `vintage=…` but not present, the endpoint returns latest values. Provide `FRED_API_KEY` and run `make alfred`.

## Roadmap (active)
- Data breadth: BEA, BLS, H.4.1, Treasury, CoinGecko adapters with retries and caching.
- Transforms: butterworth, kalman_smoother (added), seasonal adjust proxies, compose steps in YAML.
- Composite toggles: return/publish weighted/median/trimmed/diffusion and enable UI toggle.
- Regimes: finalized recession probit with coefficients + marginal effects; market regime transitions.
- API: Pydantic models; series vintage‑as‑of (done for value path).
- UI: Data Browser filters + right metadata pane; time‑frame switch for waterfall; toasts for `meta.warnings`.

## Contributing
See `AGENTS.md` for contributor guidelines, coding style, and PR conventions.
