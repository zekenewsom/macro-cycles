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


@task
def validate(cfg: dict):
    # Ensure each pillar in config has weight and at least N indicators present in indicators parquet
    comp_cfg = load_yaml("config/composite.yaml") or {}
    weights = comp_cfg.get("pillar_weights", {})
    pillars_list = [p.get("name") for p in (cfg.get("pillars", []) or [])]
    missing_weights = [p for p in pillars_list if p not in weights]
    if missing_weights:
        raise RuntimeError(f"Missing composite weights for pillars: {missing_weights}")
    # Count indicators per pillar from indicators parquet
    ind_dir = IND
    counts = {}
    if os.path.isdir(ind_dir):
        for fn in os.listdir(ind_dir):
            if fn.endswith(".parquet") and not fn.startswith("_"):
                try:
                    df = pl.read_parquet(os.path.join(ind_dir, fn), n_rows=1)
                    pillar = df.get_column("pillar").to_list()[0] if df.height and "pillar" in df.columns else None
                    if pillar:
                        counts[pillar] = counts.get(pillar, 0) + 1
                except Exception:
                    pass
    too_few = [p for p in pillars_list if counts.get(p, 0) < 1]
    if too_few:
        raise RuntimeError(f"Pillars with too few indicators: {too_few}")


@flow(name="build_pillars")
def run():
    ensure_dirs()
    cfg = load_pillar_cfg()
    validate(cfg)
    return build_pillars(cfg)


if __name__ == "__main__":
    run()
