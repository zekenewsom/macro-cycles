import os
from datetime import datetime, UTC
from typing import List, Dict, Any

import polars as pl
import pandas as pd
from prefect import flow, task, get_run_logger
from tenacity import retry, stop_after_attempt, wait_fixed
from dotenv import load_dotenv
import requests

from libs.py.config_loader import load_sources_config, get_retry_config, get_cache_config


@task
def ensure_dirs(cache: Dict[str, Any]):
    root = cache.get("parquet_root", "data/raw")
    os.makedirs(root, exist_ok=True)
    os.makedirs(os.path.join(root, "fred"), exist_ok=True)
    os.makedirs(os.path.join(root, "market"), exist_ok=True)
    os.makedirs("db", exist_ok=True)


def _retry_decorator(retry_cfg: Dict[str, Any]):
    max_attempts = int(retry_cfg.get("max_attempts", 3))
    backoff_seconds = int(retry_cfg.get("backoff_seconds", 2))
    return retry(stop=stop_after_attempt(max_attempts), wait=wait_fixed(backoff_seconds))


def _write_parquet(df: pl.DataFrame, path: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.write_parquet(path)


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _fred_client(api_key_env: str):
    from fredapi import Fred
    key = os.getenv(api_key_env)
    if not key:
        raise RuntimeError(f"Missing {api_key_env} in environment")
    return Fred(api_key=key)

def _fred_client_vintage(api_key_env: str):
    # Same client; we'll use fredapi's all releases endpoint
    from fredapi import Fred
    key = os.getenv(api_key_env)
    if not key:
        raise RuntimeError(f"Missing {api_key_env} in environment")
    return Fred(api_key=key)


def _yf_module():
    import yfinance as yf
    return yf

def _cg_prices(asset_id: str):
    url = f"https://api.coingecko.com/api/v3/coins/{asset_id}/market_chart"
    params = {"vs_currency": "usd", "days": "max"}
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    return r.json().get("prices", [])


def _fred_fetch_series(client, series_id: str) -> pd.Series:
    return client.get_series(series_id)

def _fred_fetch_all_releases(client, series_id: str) -> pd.DataFrame:
    # Returns df with columns: date, value, realtime_start, realtime_end
    return client.get_series_all_releases(series_id)


def _yf_fetch(symbol: str) -> pd.DataFrame:
    yf = _yf_module()
    # Be explicit; avoid multi-threading quirky returns
    return yf.download(symbol, progress=False, auto_adjust=False, actions=False, threads=False)


def _fred_to_pl(series: pd.Series, series_id: str) -> pl.DataFrame:
    df = series.to_frame(name="value").reset_index()
    df.columns = ["date", "value"]
    pdf = pd.DataFrame({
        "date": pd.to_datetime(df["date"]).dt.tz_localize(None),
        "value": pd.to_numeric(df["value"], errors="coerce"),
        "series_id": series_id,
        "source": "fred",
        "retrieved_at": _now_iso(),
    })
    return pl.from_pandas(pdf)


def _yf_to_pl(df: pd.DataFrame, symbol: str, alias: str) -> pl.DataFrame:
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return pl.DataFrame({"date": [], "value": [], "series_id": [], "source": [], "retrieved_at": [], "symbol": []})

    # Handle possible MultiIndex columns when yfinance returns multiple symbols
    close_series = None
    if isinstance(df.columns, pd.MultiIndex):
        for candidate in ("Adj Close", "Close"):
            key = (candidate, symbol)
            if key in df.columns:
                close_series = df[key]
                break
    else:
        for candidate in ("Adj Close", "Close"):
            if candidate in df.columns:
                close_series = df[candidate]
                break

    if close_series is None:
        # Fallback: try first numeric column
        num_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
        if num_cols:
            close_series = df[num_cols[0]]
        else:
            return pl.DataFrame({"date": [], "value": [], "series_id": [], "source": [], "retrieved_at": [], "symbol": []})

    pdf = pd.DataFrame({
        "date": pd.to_datetime(df.index).tz_localize(None),
        "value": pd.to_numeric(close_series, errors="coerce").values,
        "symbol": symbol,
        "series_id": alias or symbol,
        "source": "yfinance",
        "retrieved_at": _now_iso(),
    })
    return pl.from_pandas(pdf)


@task
def ingest_fred_series(series: Dict[str, Any], cache: Dict[str, Any], retry_cfg: Dict[str, Any]):
    logger = get_run_logger()
    parquet_root = cache.get("parquet_root", "data/raw")
    api_key_env = series.get("api_key_env")  # Not used per-series; using top-level
    fred_api_env = api_key_env or "FRED_API_KEY"
    client = _fred_client(fred_api_env)

    series_id = series["id"]
    @_retry_decorator(retry_cfg)
    def _fetch():
        return _fred_fetch_series(client, series_id)

    try:
        s = _fetch()
    except Exception as e:
        logger.warning(f"Skipping FRED series {series_id}: {e}")
        return ""
    df = _fred_to_pl(s, series_id)
    if df.is_empty():
        logger.warning(f"No data for FRED series {series_id}")
        return ""
    path = os.path.join(parquet_root, "fred", f"{series_id}.parquet")
    _write_parquet(df, path)
    logger.info(f"Wrote FRED series {series_id} -> {path}")
    return path


@task
def ingest_alfred_series(series: Dict[str, Any], cache: Dict[str, Any], retry_cfg: Dict[str, Any]):
    """Fetch all vintages for a FRED series and write data/vintage/fred/<id>.parquet
    Columns: date, value, vintage
    """
    logger = get_run_logger()
    parquet_root = cache.get("parquet_root", "data/raw")
    api_key_env = series.get("api_key_env") or "FRED_API_KEY"
    client = _fred_client_vintage(api_key_env)
    series_id = series["id"]

    @_retry_decorator(retry_cfg)
    def _fetch():
        return _fred_fetch_all_releases(client, series_id)

    try:
        df = _fetch()
    except Exception as e:
        logger.warning(f"Skipping ALFRED series {series_id}: {e}")
        return ""
    if df is None or df.empty:
        logger.warning(f"No vintage data for {series_id}")
        return ""
    # Normalize columns
    pdf = pd.DataFrame({
        "date": pd.to_datetime(df["date"]).dt.tz_localize(None),
        "value": pd.to_numeric(df["value"], errors="coerce"),
        "vintage": pd.to_datetime(df["realtime_start"]).dt.tz_localize(None),
        "series_id": series_id,
        "source": "alfred",
        "retrieved_at": _now_iso(),
    })
    pl_df = pl.from_pandas(pdf).select(["date", "value", "vintage"]).drop_nulls(subset=["date", "vintage"]).sort(["date", "vintage"])  # type: ignore
    out_path = os.path.join("data", "vintage", "fred", f"{series_id}.parquet")
    _write_parquet(pl_df, out_path)
    logger.info(f"Wrote ALFRED vintages {series_id} -> {out_path}")
    return out_path


@task
def ingest_yf_ticker(item: Dict[str, Any], cache: Dict[str, Any], retry_cfg: Dict[str, Any]):
    logger = get_run_logger()
    parquet_root = cache.get("parquet_root", "data/raw")
    symbol = item.get("symbol") or item.get("ticker")
    alias = item.get("alias") or symbol

    @_retry_decorator(retry_cfg)
    def _fetch():
        return _yf_fetch(symbol)

    try:
        df = _fetch()
    except Exception as e:
        logger.warning(f"Skipping yfinance {symbol}: {e}")
        return ""
    pl_df = _yf_to_pl(df, symbol, alias)
    path = os.path.join(parquet_root, "market", f"{alias}.parquet")
    _write_parquet(pl_df, path)
    logger.info(f"Wrote yfinance {symbol} as {alias} -> {path}")
    return path


@task
def ingest_coingecko_asset(item: Dict[str, Any], cache: Dict[str, Any], retry_cfg: Dict[str, Any]):
    logger = get_run_logger()
    parquet_root = cache.get("parquet_root", "data/raw")
    asset_id = item.get("id")
    alias = item.get("alias") or asset_id

    @_retry_decorator(retry_cfg)
    def _fetch():
        return _cg_prices(asset_id)

    try:
        prices = _fetch()
    except Exception as e:
        logger.warning(f"Skipping CoinGecko {asset_id}: {e}")
        return ""
    if not prices:
        return ""
    # prices: [ [ms, price], ... ]
    pdf = pd.DataFrame(prices, columns=["ms", "value"])  # type: ignore
    pdf["date"] = pd.to_datetime(pdf["ms"], unit="ms").dt.tz_localize(None)
    pdf = pdf[["date", "value"]]
    pl_df = pl.from_pandas(pdf)
    path = os.path.join(parquet_root, "market", f"{alias}.parquet")
    _write_parquet(pl_df, path)
    logger.info(f"Wrote CoinGecko {asset_id} as {alias} -> {path}")
    return path


@flow
def ingest_fred(force: bool = False):
    load_dotenv()
    cfg = load_sources_config()
    retry_cfg = get_retry_config(cfg)
    cache = get_cache_config(cfg)
    ensure_dirs(cache)

    fred_cfg = cfg.get("fred", {})
    # allow top-level api_key_env
    api_key_env = fred_cfg.get("api_key_env", "FRED_API_KEY")
    series_list = fred_cfg.get("series", [])

    results = []
    for s in series_list:
        s = {**s, "api_key_env": api_key_env}
        results.append(ingest_fred_series(s, cache, retry_cfg))
    return results


@flow
def ingest_yf(force: bool = False):
    load_dotenv()
    cfg = load_sources_config()
    retry_cfg = get_retry_config(cfg)
    cache = get_cache_config(cfg)
    ensure_dirs(cache)

    yf_cfg = cfg.get("yfinance", {})
    tickers: List[Dict[str, Any]] = yf_cfg.get("tickers", [])

    results = []
    for item in tickers:
        results.append(ingest_yf_ticker(item, cache, retry_cfg))
    return results


@flow
def ingest_alfred(force: bool = False):
    load_dotenv()
    cfg = load_sources_config()
    retry_cfg = get_retry_config(cfg)
    cache = get_cache_config(cfg)
    ensure_dirs(cache)
    vint_dir = os.path.join(cache.get("parquet_root", "data/raw"), "fred")
    os.makedirs(os.path.join("data", "vintage", "fred"), exist_ok=True)

    alfred_cfg = cfg.get("alfred", {})
    api_key_env = alfred_cfg.get("api_key_env", "FRED_API_KEY")
    series_list: List[Dict[str, Any]] = alfred_cfg.get("series", [])

    results = []
    for s in series_list:
        s = {**s, "api_key_env": api_key_env}
        results.append(ingest_alfred_series(s, cache, retry_cfg))
    return results


@flow
def ingest_coingecko(force: bool = False):
    cfg = load_sources_config()
    retry_cfg = get_retry_config(cfg)
    cache = get_cache_config(cfg)
    ensure_dirs(cache)
    cg_cfg = cfg.get("coingecko", {})
    assets = cg_cfg.get("assets", [])
    results = []
    for it in assets:
        results.append(ingest_coingecko_asset(it, cache, retry_cfg))
    return results


if __name__ == "__main__":
    # Run both flows for convenience
    ingest_fred()
    ingest_yf()

def main():
    """Console entry point: run both FRED and yfinance ingests."""
    ingest_fred()
    ingest_yf()
    ingest_alfred()
    ingest_coingecko()
