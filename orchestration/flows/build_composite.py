import os
import polars as pl
from prefect import flow, task
from libs.py.indicators import load_yaml

PILLARS = "data/pillars/pillars.parquet"
OUT = "data/composite"


@task
def ensure_dirs():
    os.makedirs(OUT, exist_ok=True)


@task
def load_comp_cfg():
    return load_yaml("config/composite.yaml") or {}


@task
def build_composite(cfg: dict):
    if not os.path.exists(PILLARS):
        return ""
    weights = cfg.get("pillar_weights", {})
    df = pl.read_parquet(PILLARS)
    wide = df.pivot(values="z", index="date", on="pillar").sort("date")
    cols = [k for k in weights.keys() if k in wide.columns]
    if not cols:
        return ""
    comp = wide.select(pl.col("date"), sum([pl.col(c) * float(weights[c]) for c in cols]).alias("z"))
    comp = comp.with_columns([
        pl.when(pl.col("z") > 0.75)
        .then(pl.lit("Expansion"))
        .when(pl.col("z") < -0.75)
        .then(pl.lit("Contraction"))
        .when(pl.col("z") > 0)
        .then(pl.lit("Late-Cycle"))
        .otherwise(pl.lit("Early-Recovery"))
        .alias("regime"),
        (pl.col("z").abs() / 2).clip(0, 1).alias("confidence"),
        pl.col("z").alias("score"),
    ])
    dest = os.path.join(OUT, "composite.parquet")
    comp.write_parquet(dest)
    return dest


@task
def build_contributions(cfg: dict):
    if not os.path.exists(PILLARS):
        return ""
    weights = cfg.get("pillar_weights", {})
    df = pl.read_parquet(PILLARS)
    wide = df.pivot(values="z", index="date", on="pillar").sort("date")
    # 1-row delta (assumes approximately monthly data)
    deltas = (
        wide.select([pl.col("date")] + [(pl.col(c) - pl.col(c).shift(1)).alias(c) for c in wide.columns if c != "date"])  # type: ignore
        .sort("date")
    )
    last = deltas.tail(1)
    rows = []
    if last.height:
        last_dict = last.to_dicts()[0]
        for k, w in weights.items():
            if k not in last.columns:
                continue
            raw = last_dict.get(k, 0.0)
            try:
                delta = float(0.0 if raw is None else raw)
            except Exception:
                delta = 0.0
            rows.append({"pillar": k, "delta": delta, "weight": float(w), "contribution": float(w) * delta})
    dest = os.path.join(OUT, "contributions.parquet")
    pl.DataFrame(rows).write_parquet(dest)
    return dest


@flow(name="build_composite")
def run():
    ensure_dirs()
    cfg = load_comp_cfg()
    build_composite(cfg)
    build_contributions(cfg)


if __name__ == "__main__":
    run()
