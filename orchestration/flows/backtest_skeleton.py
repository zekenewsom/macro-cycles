import os, polars as pl
from datetime import date
from prefect import flow, task

# Inputs
SPX_FP = "data/raw/market/SPX.parquet"           # must have date,value (Close)
COMP_FP = "data/composite/composite.parquet"     # uses latest; for strict vintage, rebuild composite from vintages later
OUT = "data/backtests"

@task
def ensure_dirs():
    os.makedirs(OUT, exist_ok=True)


def month_end(df: pl.DataFrame, col: str = "date"):
    """Return month-end sampled rows using the last observation in each month.
    Uses group_by_dynamic on the date column to avoid duplicate 'date' columns.
    """
    return (
        df.sort(col)
        .group_by_dynamic(col, every="1mo", period="1mo", closed="left", label="right")
        .agg(pl.all().last())
        .drop_nulls(subset=[col])
    )


@task
def load_price_series():
    if not os.path.exists(SPX_FP):
        raise FileNotFoundError("SPX parquet missing")
    px = pl.read_parquet(SPX_FP).select(["date", "value"]).rename({"value": "close"}).sort("date")
    mpx = month_end(px)
    mret = mpx.with_columns(pl.col("close").pct_change().alias("ret"))
    return mret


@task
def load_signal():
    # v0: use composite z > 0 at month-end (note: not vintage-safe yet)
    comp = pl.read_parquet(COMP_FP).select(["date", "z"]).sort("date")
    mcomp = month_end(comp)
    sig = mcomp.select(["date", (pl.col("z") > 0).cast(pl.Int8).alias("signal")])
    return sig


@task
def join_and_backtest(pr: pl.DataFrame, sig: pl.DataFrame):
    df = pr.join(sig, on="date", how="inner").sort("date")
    df = df.with_columns(pl.col("signal").shift(1).fill_null(0).alias("signal_lag"))  # trade next month
    df = df.with_columns((pl.col("ret") * pl.col("signal_lag")).alias("ret_sig"))
    # write series
    out = df
    out.write_parquet(os.path.join(OUT, "spx_signal_v0.parquet"))
    # metrics
    def sharpe(x):
        import numpy as np
        r = x.drop_nulls().to_numpy()
        return float((np.mean(r) / (np.std(r) + 1e-12)) * (12 ** 0.5)) if r.size else 0.0
    import numpy as np
    eq_bh = np.cumprod(1 + df["ret"].fill_null(0).to_numpy()) if df.height else np.array([1.0])
    eq_sig = np.cumprod(1 + df["ret_sig"].fill_null(0).to_numpy()) if df.height else np.array([1.0])
    metrics = {
        "n_months": int(df.height),
        "buyhold_total_return": float(eq_bh[-1] - 1) if df.height else 0.0,
        "strategy_total_return": float(eq_sig[-1] - 1) if df.height else 0.0,
        "buyhold_sharpe": sharpe(df["ret"]),
        "strategy_sharpe": sharpe(df["ret_sig"]),
    }
    pl.DataFrame([metrics]).write_parquet(os.path.join(OUT, "spx_signal_v0_metrics.parquet"))
    return True


@flow(name="backtest_skeleton")
def run():
    ensure_dirs()
    pr = load_price_series()
    sig = load_signal()
    join_and_backtest(pr, sig)


if __name__ == "__main__":
    run()
