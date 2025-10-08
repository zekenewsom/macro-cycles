import os, polars as pl, numpy as np
from prefect import flow, task
from hmmlearn.hmm import GaussianHMM

IN_DIR = "data/composite_asof"
OUT = "data/regimes"
os.makedirs(OUT, exist_ok=True)

@task
def hmm_business(method: str = "weighted", n_states: int = 3):
    fp = os.path.join(IN_DIR, f"composite_{method}.parquet")
    if not os.path.exists(fp):
        raise FileNotFoundError(fp)
    df = pl.read_parquet(fp).drop_nulls().sort("date")
    df = df.with_columns(pl.col("z").diff().rolling_mean(window_size=3).alias("dz3"))
    X = np.column_stack([df["z"].to_numpy(), (df["dz3"].fill_null(0)).to_numpy()])
    model = GaussianHMM(n_components=n_states, n_iter=500, covariance_type="full", random_state=42)
    model.fit(X)
    means = model.means_[:, 0]
    order = np.argsort(means)
    proba = model.predict_proba(X)[:, order]
    states = model.predict(X)
    remap = {old: i for i, old in enumerate(order)}
    states = np.vectorize(remap.get)(states)
    labels = ["Contraction", "Neutral", "Expansion"][:n_states]
    out = df.select(["date"]).with_columns(
        [
            pl.Series("state", states),
            pl.Series("label", [labels[int(s)] for s in states]),
            *[pl.Series(f"p{i}", proba[:, i]) for i in range(proba.shape[1])],
        ]
    )
    out.write_parquet(os.path.join(OUT, f"business_{method}_hmm.parquet"))


@flow(name="business_hmm")
def run(methods: list[str] = ("weighted", "median", "trimmed", "diffusion")):
    for m in methods:
        hmm_business(m)


if __name__ == "__main__":
    run()

