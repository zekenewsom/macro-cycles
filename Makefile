SHELL := /bin/bash

.DEFAULT_GOAL := help

help:
	@echo "Macro Cycles — common commands"
	@echo "make install         # poetry + web deps"
	@echo "make ingest          # FRED + yfinance → data/raw"
	@echo "make build-indicators# indicators → data/indicators"
	@echo "make build-pillars   # pillars → data/pillars"
	@echo "make build-composite # composite + contributions → data/composite"
	@echo "make pipeline        # ingest + all builds"
	@echo "make api             # run FastAPI (reload)"
	@echo "make web             # run Next.js dev server"

install:
	poetry install
	cd apps/web && npm install

ingest:
	poetry run python -m orchestration.flows.ingest_sources

build-indicators:
	poetry run python -m orchestration.flows.build_indicators

build-pillars:
	poetry run python -m orchestration.flows.build_pillars

build-composite:
	poetry run python -m orchestration.flows.build_composite

pipeline: ingest build-indicators build-pillars build-composite

api:
	poetry run uvicorn apps.api.main:app --reload --port 8000

web:
	cd apps/web && npm run dev
