SHELL := /bin/bash

.DEFAULT_GOAL := help

help:
	@echo "Macro Cycles — common commands"
	@echo "make install         # poetry + web deps"
	@echo "make ingest          # FRED + yfinance → data/raw"
	@echo "make build-indicators# indicators → data/indicators"
	@echo "make build-pillars   # pillars → data/pillars"
	@echo "make build-composite # composite + contributions → data/composite"
	@echo "make pipeline        # ingest + all builds + regimes + explain + note"
	@echo "make explain         # generate explainability artifacts"
	@echo "make monthly-note    # generate Monthly Macro Note (md)"
	@echo "make api             # run FastAPI (reload)"
	@echo "make web             # run Next.js dev server"

install:
	poetry install
	cd apps/web && npm install

ingest:
	poetry run python -m orchestration.flows.ingest_sources
	poetry run python -c "from orchestration.flows.ingest_sources import ingest_coingecko; ingest_coingecko()"

build-indicators:
	poetry run python -m orchestration.flows.build_indicators

build-pillars:
	poetry run python -m orchestration.flows.build_pillars

build-composite:
	poetry run python -m orchestration.flows.build_composite

pipeline: ingest build-indicators build-pillars build-composite build-market-hmm build-regimes explain monthly-note

explain:
	poetry run python -m orchestration.flows.explainability

monthly-note:
	poetry run python -m orchestration.flows.monthly_note

build-regimes:
	poetry run python -m orchestration.flows.build_regimes

# Build per-asset HMM regimes
build-market-hmm:
	poetry run python -m orchestration.flows.market_hmm

# (HMM step is included in pipeline above)

api:
	poetry run uvicorn apps.api.main:app --reload --port 8000

web:
	cd apps/web && npm run dev

alfred:
	poetry run python -m orchestration.flows.ingest_sources ingest_alfred

dq:
	poetry run python -m orchestration.flows.dq_write
