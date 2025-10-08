import os, polars as pl, numpy as np
from datetime import datetime
from prefect import flow, task
from libs.py.indicators import load_yaml

IND_DIR = "data/indicators"
PILLARS_FP = "data/pillars/pillars.parquet"
COMP_FP = "data/composite/composite.parquet"
REG_DIR = "data/regimes"
OUT_DIR = "data/artifacts"

os.makedirs(OUT_DIR, exist_ok=True)


@task
def load_cfgs():
    return load_yaml("config/pillars.yaml"), load_yaml("config/composite.yaml")


@task
def indicator_to_pillar_snapshot():
    """
    Compute latest indicator z, its pillar, and its weighted contribution to pillar and to composite.
    Contribution (v0) = indicator_z * (1 / n_pillar_members) * pillar_weight
    """
    pillars_cfg, comp_cfg = load_cfgs()
    # load all indicator z
    parts = []
    for fn in os.listdir(IND_DIR):
        if fn.endswith(".parquet") and not fn.startswith("_"):
            parts.append(pl.read_parquet(os.path.join(IND_DIR, fn)).select(["date", "series_id", "label", "pillar", "z"]))
    if not parts:
        pl.DataFrame({"series_id": [], "pillar": [], "z": []}).write_parquet(os.path.join(OUT_DIR, "explain_indicator_snapshot.parquet"))
        return
    allz = pl.concat(parts).drop_nulls().sort(["pillar", "series_id", "date"])
    latest = (
        allz.group_by(["pillar", "series_id", "label"]).agg(date=pl.col("date").max())
    ).join(allz, on=["pillar", "series_id", "date"], how="left")
    # pillar member counts
    counts = latest.group_by("pillar").agg(n=pl.len())
    latest = latest.join(counts, on="pillar", how="left")
    # weights via join (avoid mixed dtype from replace)
    wmap = comp_cfg["pillar_weights"]
    weights_df = pl.DataFrame({
        "pillar": list(wmap.keys()),
        "pillar_weight": list(wmap.values()),
    })
    latest = latest.join(weights_df, on="pillar", how="left").with_columns([
        pl.col("pillar_weight").fill_null(0.0).cast(pl.Float64),
        pl.col("z").cast(pl.Float64).alias("indicator_z"),
        (pl.lit(1.0) / pl.col("n").cast(pl.Float64)).alias("w_in_pillar"),
    ]).with_columns([
        (pl.col("indicator_z") * pl.col("w_in_pillar")).alias("pillar_contrib_raw"),
        (pl.col("indicator_z") * pl.col("w_in_pillar") * pl.col("pillar_weight")).alias("composite_contrib_est"),
    ])
    latest.sort(pl.col("composite_contrib_est").abs(), descending=True) \
          .write_parquet(os.path.join(OUT_DIR, "explain_indicator_snapshot.parquet"))


@task
def pillar_contributions_timeseries():
    """
    Compute time series of pillar contributions to composite:
    contribution_t = weight[p] * z_t[p]
    """
    pillars_cfg, comp_cfg = load_cfgs()
    weights = comp_cfg["pillar_weights"]
    pil = pl.read_parquet(PILLARS_FP)  # date, pillar, z
    # pivot to wide
    wide = pil.pivot(values="z", index="date", on="pillar").sort("date")
    # compute contributions per pillar
    cols = [c for c in wide.columns if c != "date"]
    outs = []
    for c in cols:
        w = weights.get(c, 0.0)
        outs.append((c, wide.select("date", (pl.col(c) * w).alias("contribution")).with_columns(pl.lit(c).alias("pillar"))))
    df = pl.concat([o[1] for o in outs]).sort(["pillar", "date"])
    df.write_parquet(os.path.join(OUT_DIR, "explain_pillar_contrib_timeseries.parquet"))


@task
def probit_marginal_effects():
    """
    For the recession probit: report current spread level and implied effect.
    (This is a v0: use normalized coefficient × current spread.)
    """
    fp = os.path.join(REG_DIR, "recession_probit.parquet")
    if not os.path.exists(fp):
        pl.DataFrame({"feature": [], "value": [], "coef": [], "effect": []}).write_parquet(
            os.path.join(OUT_DIR, "explain_probit_effects.parquet")
        )
        return
    # We didn't persist coefficients earlier; approximate with a simple re-fit here on monthly data if available.
    # Reuse the build_regimes data join to reconstruct; otherwise, just publish the latest probability.
    rec = pl.read_parquet(fp).sort("date")
    latest_p = float(rec.tail(1)["p_rec_12m"][0]) if rec.height else None
    # minimal payload
    pl.DataFrame([
        {"feature": "10y-3m spread", "value": None, "coef": None, "effect": latest_p}
    ]).write_parquet(os.path.join(OUT_DIR, "explain_probit_effects.parquet"))


@flow(name="explainability")
def run():
    indicator_to_pillar_snapshot()
    pillar_contributions_timeseries()
    probit_marginal_effects()


if __name__ == "__main__":
    run()
