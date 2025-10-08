import os
import polars as pl
from prefect import flow, task
from libs.py.indicators import load_yaml

IND = "data/indicators"
OUT = "data/pillars"


@task
def ensure_dirs():
    os.makedirs(OUT, exist_ok=True)


@task
def load_pillar_cfg():
    return load_yaml("config/pillars.yaml") or {}


@task
def build_pillars(cfg: dict):
    parts = []
    if not os.path.isdir(IND):
        return ""
    for fn in os.listdir(IND):
        if fn.endswith(".parquet") and not fn.startswith("_"):
            df = pl.read_parquet(os.path.join(IND, fn)).select(["date", "z", "series_id", "pillar"]).drop_nulls()
            parts.append(df)
    if not parts:
        return ""
    allz = pl.concat(parts).sort(["pillar", "date"])  # date, z, series_id, pillar
    by_pillar = (
        allz.group_by(["pillar", "date"]).agg([
            pl.col("z").mean().alias("z"),
            pl.len().alias("count"),
            (pl.col("z") > 0).cast(pl.Float64).mean().alias("diffusion"),
        ])
    ).sort(["pillar", "date"]) 
    dest = os.path.join(OUT, "pillars.parquet")
    by_pillar.write_parquet(dest)
    return dest


@flow(name="build_pillars")
def run():
    ensure_dirs()
    cfg = load_pillar_cfg()
    return build_pillars(cfg)


if __name__ == "__main__":
    run()
