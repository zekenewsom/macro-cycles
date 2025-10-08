import os, polars as pl
import numpy as np
from datetime import date
from prefect import flow, task
import statsmodels.api as sm

VINT = "data/vintages/fred"
OUT = "data/risk"
os.makedirs(OUT, exist_ok=True)

@task
def fit_probit(horizon_m: int = 12):
    for sid in ("T10Y3M", "USREC"):
        if not os.path.exists(os.path.join(VINT, f"{sid}.parquet")):
            raise FileNotFoundError(f"missing vintages for {sid}")
    spr = pl.read_parquet(os.path.join(VINT, "T10Y3M.parquet")).with_columns(pl.col("date").cast(pl.Date))
    rec = pl.read_parquet(os.path.join(VINT, "USREC.parquet")).with_columns(pl.col("date").cast(pl.Date))
    s = spr.group_by("date").agg(pl.col("value").last()).rename({"value": "spread"})
    r = rec.group_by("date").agg(pl.col("value").last()).rename({"value": "rec"})
    df = s.join(r, on="date", how="inner").sort("date")
    df = df.with_columns(pl.col("rec").shift(-horizon_m).alias("rec_fwd")).drop_nulls()
    y = df["rec_fwd"].to_numpy()
    X = sm.add_constant(df["spread"].to_numpy())
    model = sm.Probit(y, X, missing="drop")
    res = model.fit(disp=False)
    np.save(os.path.join(OUT, f"probit_{horizon_m}m_coef.npy"), res.params)
    pl.DataFrame({"horizon_m": [horizon_m], "llf": [res.llf], "n": [int(res.nobs)]}).write_parquet(os.path.join(OUT, f"probit_{horizon_m}m_meta.parquet"))


@task
def predict_asof(asof: str, horizon_m: int = 12):
    coefs_fp = os.path.join(OUT, f"probit_{horizon_m}m_coef.npy")
    if not os.path.exists(coefs_fp):
        return {"asof": asof, "p": None}
    coefs = np.load(coefs_fp)
    sp = pl.read_parquet(os.path.join(VINT, "T10Y3M.parquet")).with_columns(pl.col("date").cast(pl.Date))
    sp = sp.filter(pl.col("vintage") <= pl.lit(asof)).sort(["vintage", "date"])  # type: ignore
    if sp.height == 0:
        return {"asof": asof, "p": None}
    last_v = sp["vintage"][sp.height - 1]
    snap = sp.filter(pl.col("vintage") == pl.lit(last_v)).filter(pl.col("date") <= pl.lit(asof)).sort("date")
    if snap.height == 0:
        return {"asof": asof, "p": None}
    x = float(snap["value"][snap.height - 1])
    z = coefs[0] + coefs[1] * x
    p = 1 / (1 + np.exp(-z))
    return {"asof": asof, "p": float(p), "spread": x}


@flow(name="recession_probit")
def run_fit_and_predict(asof: str | None = None, horizon_m: int = 12):
    fit_probit(horizon_m)
    if asof:
        return predict_asof(asof, horizon_m)


if __name__ == "__main__":
    run_fit_and_predict(asof="2020-12-31")

