import os
import polars as pl
from prefect import flow, task
from hmmlearn.hmm import GaussianHMM
import numpy as np
import pandas as pd
import json

COMP_FP = "data/composite/composite.parquet"
OUT_DIR = "data/regimes"
PILLARS_FP = "data/pillars/pillars.parquet"

os.makedirs(OUT_DIR, exist_ok=True)


@task
def _fit_hmm(series: list[float], n_states: int = 3):
    X = np.array(series, dtype=float).reshape(-1, 1)
    model = GaussianHMM(n_components=n_states, covariance_type="diag", n_iter=200, random_state=0)
    model.fit(X)
    states = model.predict(X)
    probs = model.predict_proba(X)
    return states, probs


@flow(name="build_regimes")
def run():
    if os.path.exists(COMP_FP):
        df = pl.read_parquet(COMP_FP).select(["date", "z"]).drop_nulls().sort("date")
        if df.height >= 50:
            z = df["z"].to_list()
            states, probs = _fit_hmm(z)
            state_means = {}
            for s in set(states):
                vals = [z[i] for i in range(len(z)) if states[i] == s]
                state_means[s] = float(np.mean(vals)) if vals else 0.0
            mapping = {}
            for rank, (s, _) in enumerate(sorted(state_means.items(), key=lambda x: x[1])):
                mapping[s] = ["Contraction", "Early-Recovery", "Expansion"][min(rank, 2)]
            labels = [mapping[s] for s in states]
            p_assigned = [float(probs[i, states[i]]) for i in range(len(states))]
            out = pl.DataFrame({"date": df["date"], "label": labels, "p": p_assigned})
            out.write_parquet(os.path.join(OUT_DIR, "business.parquet"))
    # pillar regimes
    if os.path.exists(PILLARS_FP):
        pil = pl.read_parquet(PILLARS_FP).select(["date", "pillar", "z"]).drop_nulls().sort(["pillar", "date"])
        for pillar in pil.select("pillar").unique().to_series().to_list():
            sub = pil.filter(pl.col("pillar") == pillar).select(["date", "z"]).sort("date")
            if sub.height < 50:
                continue
            z = sub["z"].to_list()
            states, probs = _fit_hmm(z)
            state_means = {}
            for s in set(states):
                vals = [z[i] for i in range(len(z)) if states[i] == s]
                state_means[s] = float(np.mean(vals)) if vals else 0.0
            mapping = {}
            for rank, (s, _) in enumerate(sorted(state_means.items(), key=lambda x: x[1])):
                mapping[s] = ["Contraction", "Early-Recovery", "Expansion"][min(rank, 2)]
            labels = [mapping[s] for s in states]
            p_assigned = [float(probs[i, states[i]]) for i in range(len(states))]
            out = pl.DataFrame({"date": sub["date"], "label": labels, "p": p_assigned})
            out.write_parquet(os.path.join(OUT_DIR, f"pillar_{pillar}.parquet"))

    # Recession probit (10y-3m)
    # Requires FRED daily yields DGS10 and DGS3MO and USREC monthly indicator
    d10 = os.path.join("data", "raw", "fred", "DGS10.parquet")
    d3m = os.path.join("data", "raw", "fred", "DGS3MO.parquet")
    rec = os.path.join("data", "raw", "fred", "USREC.parquet")
    try:
        if os.path.exists(d10) and os.path.exists(d3m) and os.path.exists(rec):
            f10 = pl.read_parquet(d10).select(["date", "value"]).rename({"value": "y10"})
            f3 = pl.read_parquet(d3m).select(["date", "value"]).rename({"value": "y3m"})
            # Resample to monthly end-of-month
            def to_monthly(df: pl.DataFrame, col: str):
                return (
                    df.group_by_dynamic("date", every="1mo", period="1mo", closed="left", label="right")
                    .agg(pl.col(col).last().alias(col))
                    .drop_nulls()
                )
            m10 = to_monthly(f10, "y10")
            m3 = to_monthly(f3, "y3m")
            m = m10.join(m3, on="date", how="inner").with_columns((pl.col("y10") - pl.col("y3m")).alias("spread")).drop_nulls()
            r = pl.read_parquet(rec).select(["date", "value"]).rename({"value": "usrec"})
            r = to_monthly(r, "usrec").with_columns(pl.col("usrec").cast(pl.Int64))
            dfm = m.join(r, on="date", how="left").fill_null(0).sort("date")
            # Build target: recession 12m ahead
            pdf = dfm.to_pandas()
            pdf["y"] = pdf["usrec"].shift(-12)  # recession 12m ahead
            pdf = pdf.dropna(subset=["y", "spread"]).copy()
            pdf["y"] = (pdf["y"] > 0).astype(int)
            # Fit logit
            import statsmodels.api as sm  # type: ignore
            X = sm.add_constant(pdf[["spread"]])
            model = sm.Logit(pdf["y"], X).fit(disp=False)
            coefs = {"intercept": float(model.params["const"]), "spread": float(model.params["spread"]) }
            # Predict probabilities across history
            X_all = sm.add_constant(dfm.to_pandas()[["spread"]].fillna(method="ffill").fillna(method="bfill"))
            p = model.predict(X_all)
            out = dfm.select(["date", "spread"])  # type: ignore
            out = out.with_columns(pl.Series(name="p_rec_12m", values=p.values))
            out.write_parquet(os.path.join(OUT_DIR, "recession_probit.parquet"))
            # write coefficients artifact
            os.makedirs("data/artifacts", exist_ok=True)
            with open(os.path.join("data/artifacts", "recession_probit_coefs.json"), "w") as f:
                json.dump(coefs, f)
    except Exception:
        pass


if __name__ == "__main__":
    run()
