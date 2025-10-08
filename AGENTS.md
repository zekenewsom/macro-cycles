# Repository Guidelines

## Project Structure & Module Organization
- `apps/api`: FastAPI service entrypoint (`apps/api/main.py`).
- `apps/web`: Frontend scaffold (deps only for now).
- `orchestration/flows`: Prefect flows (e.g., `ingest_demo.py`).
- `config/sources.yaml`: Data source config (FRED, yfinance, CoinGecko).
- `data/` and `db/`: Local artifacts (Parquet) and DuckDB catalog.
- `libs/py`, `libs/ts`: Shared library placeholders.
- `tests/`: Test root (add `test_*.py` here).

## Build, Test, and Development Commands
- Install deps: `poetry install` (Python `>=3.12,<3.14`).
- Activate env: `poetry shell` (optional; otherwise use `poetry run ...`).
- Run API (dev): `poetry run uvicorn apps.api.main:app --reload` → visit `/health`.
- Seed demo data: `poetry run python orchestration/flows/ingest_demo.py`.
- Example check: `curl http://127.0.0.1:8000/health`.

## Coding Style & Naming Conventions
- Python: PEP 8, 4‑space indent, type hints where helpful, module/file `snake_case`, classes `PascalCase`, constants `UPPER_CASE`.
- Imports: stdlib → third‑party → local, separated by blank lines.
- Keep API routes small and typed; prefer pure functions in `libs/py`.

## Testing Guidelines
- Framework: pytest (not yet configured). Place tests under `tests/` as `test_*.py`.
- Run: `poetry run pytest` (once added). Aim to cover flows, data transforms, and API endpoints.
- For API, prefer `TestClient` from FastAPI to validate routes (health, overview endpoints).

## Commit & Pull Request Guidelines
- Commits: follow Conventional Commits (e.g., `feat(api): add pillars endpoint`, `fix(flow): handle empty parquet`).
- PRs: include purpose, key changes, run instructions, and any screenshots (web) or sample `curl`/JSON (API). Link issues where applicable.
- Keep PRs focused; update docs/config when behavior changes.

## Security & Configuration Tips
- Copy `.env.example` → `.env`; set `FRED_API_KEY` and optionally `DUCKDB_PATH` (defaults to `db/catalog.duckdb`). Never commit secrets.
- `data/` and `db/` are local; safe to regenerate. Review `config/sources.yaml` before running external pulls.

