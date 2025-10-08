import os
import polars as pl
from prefect import flow, task

from libs.py.indicators import load_indicator_configs, apply_pipeline, rolling_z

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
    df = pl.read_parquet(fp)
    if not set(["date", "value"]).issubset(df.columns):
        return None
    df = df.select(["date", "value"]).drop_nulls().sort("date")
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
    out.write_parquet(os.path.join(OUT, f"{sid}.parquet"))
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

