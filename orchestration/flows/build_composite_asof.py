import os, sys, polars as pl
from datetime import date
from prefect import flow, task

# Ensure project root is on sys.path when running as a script
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir, os.pardir))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from libs.py.asof_utils import month_ends, expanding_z_no_lookahead

IND_DIR  = "data/indicators"         # latest transformed indicators (value + label + pillar)
VINT_DIR = "data/vintages/fred"      # stacked vintages (date,value,vintage) for raw
OUT_DIR  = "data/composite_asof"     # new: as-of snapshots
PILLAR_OUT = "data/pillars_asof"
os.makedirs(OUT_DIR, exist_ok=True)
os.makedirs(PILLAR_OUT, exist_ok=True)

DEFAULT_WEIGHTS = {
  "growth": 0.15, "labor": 0.10, "inflation": 0.10, "policy":0.10,
  "liquidity":0.10, "credit":0.10, "housing":0.10, "external":0.10,
  "profits":0.07, "sentiment":0.08
}


def _list_indicator_files():
    return [f for f in os.listdir(IND_DIR) if f.endswith(".parquet") and not f.startswith("_")]


def _read_indicator(series_id: str) -> pl.DataFrame:
    fp = os.path.join(IND_DIR, f"{series_id}.parquet")
    return pl.read_parquet(fp).select(["date", "value", "label", "pillar", "series_id"]).drop_nulls().sort("date")


def _try_vintage(series_id: str) -> pl.DataFrame | None:
    fp = os.path.join(VINT_DIR, f"{series_id}.parquet")
    if not os.path.exists(fp):
        return None
    v = pl.read_parquet(fp)
    # normalize dtypes for safe comparisons
    if "vintage" in v.columns:
        v = v.with_columns(pl.col("vintage").str.strptime(pl.Date, strict=False))
    if "date" in v.columns:
        v = v.with_columns(pl.col("date").cast(pl.Date))
    return v


def _asof_series(series_id: str, all_month_ends: list[date]) -> pl.DataFrame:
    ind = _read_indicator(series_id)
    pillar = ind["pillar"][0]
    label = ind["label"][0]
    vint = _try_vintage(series_id)
    out_rows: list[dict] = []
    for me in all_month_ends:
        if vint is not None:
            vd = vint.filter(pl.col("vintage") <= pl.lit(me)).sort(["vintage", "date"])  # type: ignore
            if vd.height == 0:
                out_rows.append({"date": me, "series_id": series_id, "pillar": pillar, "label": label, "value_asof": None})
                continue
            last_vint = vd["vintage"][-1]
            snap = vd.filter(pl.col("vintage") == pl.lit(last_vint)).filter(pl.col("date") <= pl.lit(me)).sort("date")
            val = snap["value"][snap.height - 1] if snap.height else None
        else:
            snap = ind.filter(pl.col("date") <= pl.lit(me)).sort("date")
            val = snap["value"][snap.height - 1] if snap.height else None
        out_rows.append({"date": me, "series_id": series_id, "pillar": pillar, "label": label, "value_asof": val})
    # Enforce schema to avoid inference issues when first rows contain nulls
    return pl.DataFrame(out_rows, schema={
        "date": pl.Date,
        "series_id": pl.Utf8,
        "pillar": pl.Utf8,
        "label": pl.Utf8,
        "value_asof": pl.Float64,
    })


def _pillar_agg(df: pl.DataFrame, method: str):
    if method == "median":
        return df.select([pl.col("z").median().alias("z")])
    if method == "trimmed":
        # simple trimmed mean: drop one low/high if possible
        n = df.height
        if n > 2:
            return df.select([pl.col("z").sort().slice(1, n - 2).mean().alias("z")])
        return df.select([pl.col("z").mean().alias("z")])
    if method == "diffusion":
        return df.select([(pl.col("z") > 0).cast(pl.Float64).mean().alias("z")])
    return df.select([pl.col("z").mean().alias("z")])


@task
def build_asof(method: str = "weighted", start: str = "1980-01-01"):
    ids = [f[:-8] for f in _list_indicator_files()]
    from datetime import date as _date
    try:
        start_dt = _date.fromisoformat(start)
    except Exception:
        start_dt = _date(1980, 1, 1)
    all_me = month_ends(start_dt, _date.today())
    long = pl.concat([_asof_series(sid, all_me) for sid in ids], how="diagonal")
    # Compute expanding z without look-ahead using window expressions per series
    min_periods = 36
    base = (
        long.drop_nulls(subset=["value_asof"]).rename({"value_asof": "value"}).sort(["series_id", "date"])
    )
    zs = base.with_columns([
        # cumulative counts/sums per series
        (pl.col("value").cum_count().over("series_id") - 1).alias("_n_prev"),
        (pl.col("value").cum_sum().over("series_id") - pl.col("value")).alias("_sum_prev"),
        (((pl.col("value") ** 2).cum_sum().over("series_id")) - (pl.col("value") ** 2)).alias("_sum2_prev"),
    ]).with_columns([
        # guards against div by zero
        pl.when(pl.col("_n_prev") > 0).then(pl.col("_sum_prev") / pl.col("_n_prev")).otherwise(None).alias("_mu"),
        pl.when(pl.col("_n_prev") > 1)
        .then((pl.col("_sum2_prev") - (pl.col("_sum_prev") ** 2) / pl.col("_n_prev")) / (pl.col("_n_prev") - 1))
        .otherwise(None)
        .alias("_var"),
    ]).with_columns([
        pl.when(pl.col("_n_prev") >= min_periods)
        .then((pl.col("value") - pl.col("_mu")) / pl.col("_var").sqrt())
        .otherwise(None)
        .alias("z"),
    ]).drop(["_n_prev", "_sum_prev", "_sum2_prev", "_mu", "_var"])
    # pillar aggregation per date
    if method == "diffusion":
        agg_expr = (pl.col("z") > 0).cast(pl.Float64).mean().alias("z")
    elif method == "median":
        agg_expr = pl.col("z").median().alias("z")
    else:
        agg_expr = pl.col("z").mean().alias("z")
    pil = zs.group_by(["date", "pillar"]).agg(agg_expr).sort(["date", "pillar"])
    os.makedirs(PILLAR_OUT, exist_ok=True)
    pil.write_parquet(os.path.join(PILLAR_OUT, f"pillars_{method}.parquet"))
    dfw = pl.DataFrame([{"pillar": k, "w": v} for k, v in DEFAULT_WEIGHTS.items()])
    comp = (
        pil.join(dfw, on="pillar", how="left")
        .with_columns(pl.col("w").fill_null(0.0))
        .group_by("date")
        .agg((pl.col("z") * pl.col("w")).sum().alias("z"))
        .sort("date")
    )
    os.makedirs(OUT_DIR, exist_ok=True)
    comp.write_parquet(os.path.join(OUT_DIR, f"composite_{method}.parquet"))


@flow(name="build_composite_asof")
def run(methods: list[str] = ("weighted", "median", "trimmed", "diffusion"), start: str = "1980-01-01"):
    for m in methods:
        build_asof.with_options(name=f"build_asof_{m}")(method=m, start=start)


if __name__ == "__main__":
    run()
