import os
from typing import Dict, Any, List

import polars as pl
from prefect import flow, task

from libs.py.config_loader import load_sources_config, load_yaml


def _ensure_dirs():
    os.makedirs("data/indicators", exist_ok=True)
    os.makedirs("data/pillars", exist_ok=True)
    os.makedirs("data/composite", exist_ok=True)


def _safe_z(df: pl.DataFrame, value_col: str = "value") -> pl.Series:
    vals = df[value_col]
    mu = vals.mean()
    sigma = vals.std(ddof=1)
    if sigma is None or sigma == 0 or not pl.Series([sigma]).is_finite().all():
        return pl.Series([0.0] * len(df)).alias("z")
    return ((vals - mu) / sigma).alias("z")


@task
def build_indicators_from_raw() -> List[str]:
    _ensure_dirs()
    out_paths: List[str] = []
    # FRED
    fred_root = os.path.join("data", "raw", "fred")
    if os.path.isdir(fred_root):
        for fn in os.listdir(fred_root):
            if not fn.endswith(".parquet"):
                continue
            p = os.path.join(fred_root, fn)
            df = pl.read_parquet(p).sort("date")
            z = _safe_z(df)
            out = df.with_columns(z).select(["date", "series_id", "value", "source", "retrieved_at", "z"])  # type: ignore
            series_id = out["series_id"][0]
            dest = os.path.join("data", "indicators", f"{series_id}.parquet")
            out.write_parquet(dest)
            out_paths.append(dest)
    # yfinance (market)
    market_root = os.path.join("data", "raw", "market")
    if os.path.isdir(market_root):
        for fn in os.listdir(market_root):
            if not fn.endswith(".parquet"):
                continue
            p = os.path.join(market_root, fn)
            df = pl.read_parquet(p).sort("date")
            z = _safe_z(df)
            out = df.with_columns(z).select(["date", "series_id", "value", "source", "retrieved_at", "z"])  # type: ignore
            series_id = out["series_id"][0]
            dest = os.path.join("data", "indicators", f"{series_id}.parquet")
            out.write_parquet(dest)
            out_paths.append(dest)
    return out_paths


@task
def build_pillars_from_indicators(cfg: Dict[str, Any]) -> str:
    _ensure_dirs()
    sources = cfg.get("fred", {}).get("series", [])
    # Map series_id -> pillar
    by_pillar: Dict[str, List[str]] = {}
    for s in sources:
        sid = s.get("id")
        pillar = s.get("pillar")
        if not sid or not pillar:
            continue
        by_pillar.setdefault(pillar, []).append(sid)

    frames: List[pl.DataFrame] = []
    for pillar, sids in by_pillar.items():
        long_rows: List[pl.DataFrame] = []
        for sid in sids:
            path = os.path.join("data", "indicators", f"{sid}.parquet")
            if not os.path.exists(path):
                continue
            df = pl.read_parquet(path).select(["date", "z"]).with_columns(
                pl.lit(sid).alias("series_id"),
                pl.lit(pillar).alias("pillar"),
            )
            long_rows.append(df)
        if not long_rows:
            continue
        long = pl.concat(long_rows).sort(["date"])  # date, series_id, pillar, z
        # diffusion: share of positive z per date
        diff = long.select(
            ["date", (pl.col("z") > 0).cast(pl.Float64).alias("pos"), pl.col("pillar")]
        ).group_by(["date", "pillar"]).agg(pl.mean("pos").alias("diffusion"))

        # pillar z: mean z across components per date
        z_mean = long.group_by(["date", "pillar"]).agg(pl.mean("z").alias("z"))
        merged = z_mean.join(diff, on=["date", "pillar"], how="left").sort("date")
        # momentum_3m: difference vs 3 steps back
        merged = merged.with_columns(
            (pl.col("z") - pl.col("z").shift(3)).alias("momentum_3m")
        )
        frames.append(merged)

    if frames:
        pillars = pl.concat(frames).sort(["date", "pillar"])
        dest = os.path.join("data", "pillars", "pillars.parquet")
        pillars.write_parquet(dest)
        return dest
    else:
        # nothing to write
        return ""


@task
def build_composite_from_pillars(cfg: Dict[str, Any]) -> str:
    _ensure_dirs()
    pillars_path = os.path.join("data", "pillars", "pillars.parquet")
    if not os.path.exists(pillars_path):
        return ""
    df = pl.read_parquet(pillars_path)
    weights = (cfg.get("composite", {}) or {}).get("weights", {})
    if not weights:
        # equal weights by pillar present in data
        unique_pillars = (
            df.select(pl.col("pillar").unique().alias("pillar"))
            .get_column("pillar")
            .to_list()
        )
        if not unique_pillars:
            return ""
        w = 1.0 / float(len(unique_pillars))
        weights = {p: w for p in unique_pillars}

    # compute weighted sum per date
    # merge weights into df
    w_df = pl.DataFrame({"pillar": list(weights.keys()), "w": list(weights.values())})
    merged = df.join(w_df, on="pillar", how="left").fill_null(0.0)
    comp = (
        merged.group_by("date")
        .agg(((pl.col("z") * pl.col("w")).sum()).alias("z"))
        .sort("date")
    )
    comp = comp.with_columns(
        pl.col("z").alias("score"),
        (pl.col("z").abs() / 2).alias("confidence"),
        pl.when(pl.col("z") > 0).then(pl.lit("Expansion")).otherwise(pl.lit("Contraction")).alias("regime"),
    )
    dest = os.path.join("data", "composite", "composite.parquet")
    comp.select(["date", "score", "z", "regime", "confidence"]).write_parquet(dest)
    return dest


@flow
def build_indicators():
    return build_indicators_from_raw()


@flow
def build_pillars():
    cfg = load_sources_config()
    return build_pillars_from_indicators(cfg)


@flow
def build_composite():
    # combine both sources.yaml and composite.yaml if present
    cfg: Dict[str, Any] = {}
    cfg.update(load_sources_config())
    comp_cfg_path = os.path.join("config", "composite.yaml")
    if os.path.exists(comp_cfg_path):
        comp_cfg = load_yaml(comp_cfg_path)
        if isinstance(comp_cfg, dict):
            cfg.update(comp_cfg)
    return build_composite_from_pillars(cfg)


@flow
def build_all():
    build_indicators()
    build_pillars()
    build_composite()


if __name__ == "__main__":
    build_all()
