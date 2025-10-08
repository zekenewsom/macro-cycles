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
    # validate weights cover all pillars present
    present = set(df.select("pillar").unique()["pillar"].to_list())
    wkeys = set(weights.keys())
    missing = sorted(list(present - wkeys))
    extras = sorted(list(wkeys - present))
    if missing:
        raise ValueError(f"Missing pillar_weights for pillars: {', '.join(missing)}")
    if extras:
        print(f"[build_composite] Warning: extra pillar_weights not in data: {', '.join(extras)}")
    wide = df.pivot(values="z", index="date", on="pillar").sort("date")
    cols = [k for k in weights.keys() if k in wide.columns]
    if not cols:
        return ""
    # normalized weighted average across available (non-null) pillars per date
    weighted_terms = [
        pl.when(pl.col(c).is_not_null()).then(pl.col(c) * float(weights[c])).otherwise(0.0) for c in cols
    ]
    present_w = [pl.when(pl.col(c).is_not_null()).then(float(weights[c])).otherwise(0.0) for c in cols]
    comp = wide.select(
        [
            pl.col("date"),
            (pl.sum_horizontal(weighted_terms) / pl.sum_horizontal(present_w)).alias("z_weighted"),
        ]
    )
    # median z across pillars
    med = wide.select(["date"] + cols).with_columns(
        pl.concat_list([pl.col(c) for c in cols]).alias("arr")
    ).select(["date", pl.col("arr").list.median().alias("z_median")])
    comp = comp.join(med, on="date", how="left")
    # trimmed mean (drop top/bottom 1 value if >=3 pillars)
    tmean = wide.select(["date"] + cols).with_columns(
        pl.concat_list([pl.col(c) for c in cols]).alias("arr")
    ).select([
        "date",
        pl.when(pl.col("arr").list.len() >= 3)
        .then(pl.col("arr").list.sort().list.slice(1, pl.len() - 2).list.mean())
        .otherwise(pl.col("arr").list.mean())
        .alias("z_trimmed_mean"),
    ])
    comp = comp.join(tmean, on="date", how="left")
    # diffusion-only composite from pillars diffusion
    pil = pl.read_parquet(PILLARS).select(["date", "pillar", "diffusion"]).pivot(values="diffusion", index="date", on="pillar")
    dcols = [c for c in pil.columns if c != "date"]
    diffc = pil.select(["date", (sum([pl.col(c) for c in dcols]) / pl.lit(len(dcols))).alias("diffusion_composite")])
    comp = comp.join(diffc, on="date", how="left")
    # primary z as weighted
    comp = comp.with_columns([pl.col("z_weighted").alias("z")])
    # add regime/confidence/score based on z
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
    # validate weights cover all pillars present
    present = set(df.select("pillar").unique()["pillar"].to_list())
    wkeys = set(weights.keys())
    missing = sorted(list(present - wkeys))
    extras = sorted(list(wkeys - present))
    if missing:
        raise ValueError(f"Missing pillar_weights for pillars: {', '.join(missing)}")
    if extras:
        print(f"[build_contributions] Warning: extra pillar_weights not in data: {', '.join(extras)}")
    wide = df.pivot(values="z", index="date", on="pillar").sort("date")
    # per-pillar delta based on its own last two observations
    rows = []
    piv = df.sort(["pillar", "date"]).with_columns(
        pl.col("z").shift(1).over("pillar").alias("z_prev"),
        pl.max("date").over("pillar").alias("max_date"),
    )
    last_rows = piv.filter(pl.col("date") == pl.col("max_date"))
    for r in last_rows.to_dicts():
        k = r.get("pillar")
        if k not in weights:
            continue
        z_last = r.get("z")
        z_prev = r.get("z_prev")
        if z_last is None or z_prev is None:
            delta = 0.0
        else:
            try:
                delta = float(z_last) - float(z_prev)
            except Exception:
                delta = 0.0
        w = float(weights.get(k, 0.0))
        rows.append({"pillar": k, "delta": delta, "weight": w, "contribution": w * delta})
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
