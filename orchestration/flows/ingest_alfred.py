import os, sys, time, requests, polars as pl
from datetime import datetime
from prefect import flow, task

# Ensure project root is on sys.path when running as a script
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir, os.pardir))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from libs.py.indicators import load_yaml
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

DATA_VINT = "data/vintages/fred"  # stacked vintages: one parquet per series


def fred_key():
    return os.getenv("FRED_API_KEY", "")


def fred(url, params):
    params = {**params, "api_key": fred_key(), "file_type": "json"}
    key = params.get("api_key", "")
    if not key:
        raise RuntimeError("FRED_API_KEY not set. Export FRED_API_KEY or add it to your .env file.")
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    return r.json()


@task
def ensure_dirs():
    os.makedirs(DATA_VINT, exist_ok=True)


@task
def list_vintage_enabled():
    cfg = load_yaml("config/sources.yaml")
    fred_cfg = (cfg.get("fred") or {})
    series = [s for s in (fred_cfg.get("series") or []) if s.get("vintage")]
    return series


@task
def fetch_vintage_dates(series_id: str):
    url = "https://api.stlouisfed.org/fred/series/vintagedates"
    j = fred(url, {"series_id": series_id})
    return j.get("vintage_dates", [])


@task
def fetch_snapshot(series_id: str, vintage_date: str):
    # Observations as published on vintage_date
    url = "https://api.stlouisfed.org/fred/series/observations"
    j = fred(
        url,
        {
            "series_id": series_id,
            "realtime_start": vintage_date,
            "realtime_end": vintage_date,
            "observation_start": "1900-01-01",
        },
    )
    rows = []
    for obs in j.get("observations", []):
        v = obs.get("value")
        if v in (".", None):
            continue
        rows.append({"date": obs["date"], "value": float(v), "vintage": vintage_date})
    if not rows:
        return None
    df = pl.DataFrame(rows).with_columns(pl.col("date").str.strptime(pl.Date, strict=False))
    return df


@task
def write_series(series_id: str, df_list: list[pl.DataFrame]):
    if not df_list:
        return
    out = pl.concat([d for d in df_list if d is not None]).sort(["vintage", "date"])  # type: ignore
    out.write_parquet(os.path.join(DATA_VINT, f"{series_id}.parquet"))


@flow(name="ingest_alfred")
def run(max_vintages: int = 24, sleep_ms: int = 120):
    ensure_dirs()
    todo = list_vintage_enabled()
    for s in todo:
        sid = s["id"]
        vintages = fetch_vintage_dates(sid)
        vintages = vintages[-max_vintages:] if max_vintages > 0 else vintages
        chunks = []
        for vd in vintages:
            chunks.append(fetch_snapshot(sid, vd))
            time.sleep(sleep_ms / 1000.0)
        write_series(sid, chunks)


if __name__ == "__main__":
    run()
