import os
import polars as pl
from prefect import flow, task

from libs.py.indicators import load_indicator_configs, apply_pipeline, rolling_z, canonicalize_freq
import json, datetime

RAW = "data/raw"
OUT = "data/indicators"


@task
def ensure_dirs():
    os.makedirs(OUT, exist_ok=True)


@task
def list_series():
    cfg, comp_cfg, pillars_cfg = load_indicator_configs()
    indicators = cfg.get("indicators", [])
    zcfg = (cfg.get("standardization", {}) or {}).get("zscore", {"window_years": 10, "clamp": 4.0})
    return indicators, zcfg


@task
def build_one(ind_def: dict, zcfg: dict):
    sid = ind_def["series_id"]
    fp = None
    for folder in ["fred", "market", "coingecko", ""]:
        candidate = os.path.join(RAW, folder, f"{sid}.parquet")
        if os.path.exists(candidate):
            fp = candidate
            break
    if not fp:
        return None
    raw = pl.read_parquet(fp)
    # DQ: percent NaN before dropping
    total_rows = int(raw.height)
    nan_rows = int(raw.filter(pl.col("value").is_null()).height) if total_rows else 0
    df = raw
    if not set(["date", "value"]).issubset(df.columns):
        return None
    df = df.select(["date", "value"]).drop_nulls().sort("date")
    # canonicalize frequency
    target_freq = ind_def.get("freq", "M")
    df = canonicalize_freq(df, target_freq)
    transforms = ind_def.get("pipeline", [])
    inv = ind_def.get("invert", False)
    if transforms:
        pipe = [{"kind": t} if isinstance(t, str) else t for t in transforms]
        df = apply_pipeline(df, pipe)
    if inv:
        df = df.with_columns((-pl.col("value")).alias("value"))
    df = rolling_z(
        df,
        value_col="value",
        window_years=int(zcfg.get("window_years", 10)),
        clamp=float(zcfg.get("clamp", 4.0)),
        freq=ind_def.get("freq", "M"),
    )
    out = df.with_columns(
        [
            pl.lit(sid).alias("series_id"),
            pl.lit(ind_def.get("label", sid)).alias("label"),
            pl.lit(ind_def.get("pillar")).alias("pillar"),
        ]
    )
    out_fp = os.path.join(OUT, f"{sid}.parquet")
    out.write_parquet(out_fp)
    # DQ gate: write JSON with counts and staleness
    try:
        dq_dir = os.path.join("data", "_dq")
        os.makedirs(dq_dir, exist_ok=True)
        last_date = out.select(pl.col("date").max()).item() if out.height else None
        staleness = None
        if last_date is not None:
            staleness = (datetime.date.today() - last_date.date()).days if hasattr(last_date, "date") else None
        dq = {
            "series_id": sid,
            "rows": int(out.height),
            "last_date": str(last_date) if last_date is not None else None,
            "staleness_days": staleness,
            "path": out_fp,
            "rows_raw": total_rows,
            "pct_nan_raw": (nan_rows / total_rows) if total_rows else 0.0,
        }
        with open(os.path.join(dq_dir, f"{sid}.json"), "w") as f:
            json.dump(dq, f)
    except Exception:
        pass
    return sid


@flow(name="build_indicators")
def build_indicators():
    ensure_dirs()
    indicators, zcfg = list_series()
    built = []
    for ind in indicators:
        sid = build_one(ind, zcfg)
        if sid:
            built.append(sid)
    if built:
        pl.DataFrame({"series_id": built}).write_parquet(os.path.join(OUT, "_catalog.parquet"))
    return built


if __name__ == "__main__":
    build_indicators()
