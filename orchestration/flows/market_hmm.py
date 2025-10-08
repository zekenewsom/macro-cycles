import os, polars as pl, numpy as np
from typing import List, Dict
from prefect import flow, task
from hmmlearn.hmm import GaussianHMM

RAW_MKT = "data/raw/market"
OUT_DIR = "data/regimes"
os.makedirs(OUT_DIR, exist_ok=True)


def _features_from_prices(df: pl.DataFrame) -> pl.DataFrame:
    # df: date,value (close/yield)
    df = df.sort("date").with_columns([
        pl.col("value").pct_change().alias("ret1"),
    ])
    # 5d / 20d momentum & 20d vol (realized) + 20d mean return
    df = df.with_columns([
        pl.col("value").pct_change(n=5).alias("ret5"),
        pl.col("value").pct_change(n=20).alias("ret20"),
        pl.col("ret1").rolling_std(window_size=20).alias("vol20"),
        pl.col("ret1").rolling_mean(window_size=20).alias("mean20"),
    ]).drop_nulls()
    return df


def _fit_hmm(X: np.ndarray, n_states: int = 3, cov_type: str = "full", seed: int = 42):
    model = GaussianHMM(n_components=n_states, covariance_type=cov_type, n_iter=500, random_state=seed)
    model.fit(X)
    # order states by mean 1d return (ascending)
    means = model.means_[:, 0]  # dimension 0 corresponds to ret1 by construction
    order = np.argsort(means)
    remap = {old: i for i, old in enumerate(order)}
    # posterior probs & states
    proba = model.predict_proba(X)[:, order]  # reorder columns
    states_raw = model.predict(X)
    states = np.vectorize(remap.get)(states_raw)
    means_ord = means[order]
    return model, states, proba, means_ord


def _state_labels(means_ord: np.ndarray) -> List[str]:
    # 0 = lowest mean ret → Trend-Down, 1 = middle → Range, 2 = highest → Trend-Up
    n = len(means_ord)
    base = ["Trend-Down", "Range", "Trend-Up", "Blowoff"]
    return base[:n]


def _transition_matrix_from_states(states: np.ndarray, n: int):
    C = np.zeros((n, n), dtype=int)
    for a, b in zip(states[:-1], states[1:]):
        C[a, b] += 1
    row_sum = C.sum(axis=1, keepdims=True)
    row_sum[row_sum == 0] = 1
    P = C / row_sum
    return P.tolist()


@task
def list_assets() -> List[str]:
    if not os.path.isdir(RAW_MKT):
        return []
    return sorted([fn.replace(".parquet", "") for fn in os.listdir(RAW_MKT) if fn.endswith(".parquet") and not fn.startswith("_")])


@task
def build_one(ticker: str, n_states: int = 3, min_rows: int = 300) -> Dict:
    fp = os.path.join(RAW_MKT, f"{ticker}.parquet")
    if not os.path.exists(fp):
        return {"ticker": ticker, "ok": False, "reason": "no_file"}
    px = pl.read_parquet(fp).select(["date", "value"]).drop_nulls().sort("date")
    if px.height < min_rows:
        return {"ticker": ticker, "ok": False, "reason": f"too_short({px.height})"}
    f = _features_from_prices(px)
    X = np.column_stack(
        [
            f["ret1"].to_numpy(),
            f["ret5"].to_numpy(),
            f["ret20"].to_numpy(),
            f["vol20"].to_numpy(),
            f["mean20"].to_numpy(),
        ]
    )
    # standardize columns for stability
    mu = np.nanmean(X, axis=0)
    sd = np.nanstd(X, axis=0)
    sd[sd == 0] = 1
    Xz = (X - mu) / sd
    model, states, proba, means = _fit_hmm(Xz, n_states=n_states)
    labels = _state_labels(means)
    lbl = [labels[s] for s in states]
    out = f.select(["date"]).with_columns([
        pl.Series("state", states),
        pl.Series("label", lbl),
    ])
    # add probabilities p0..p{k-1}
    for i in range(proba.shape[1]):
        out = out.with_columns(pl.Series(f"p{i}", proba[:, i]))
    # persist
    out_fp = os.path.join(OUT_DIR, f"market_{ticker}_hmm.parquet")
    out.write_parquet(out_fp)
    # transition matrix
    trans = _transition_matrix_from_states(states, n_states)
    return {"ticker": ticker, "ok": True, "n": int(out.height), "states": labels, "trans": trans}


@flow(name="market_hmm")
def run(n_states: int = 3):
    os.makedirs(OUT_DIR, exist_ok=True)
    results: list[Dict] = []
    assets = list_assets()
    for t in assets:
        res = build_one(t, n_states=n_states)
        results.append(res)
    # write a small catalog
    ok = [r for r in results if r.get("ok")]
    if ok:
        pl.DataFrame(ok).write_parquet(os.path.join(OUT_DIR, "_market_hmm_catalog.parquet"))
    return ok


if __name__ == "__main__":
    run()
