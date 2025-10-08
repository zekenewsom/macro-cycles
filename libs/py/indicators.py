import os
from typing import Dict, List, Tuple
import polars as pl
import pandas as pd

try:
    from statsmodels.tsa.filters.hp_filter import hpfilter  # type: ignore
except Exception:
    hpfilter = None  # fallback
try:
    from statsmodels.tsa.filters.cf_filter import cffilter  # type: ignore
except Exception:
    cffilter = None
try:
    from statsmodels.tsa.statespace.structural import UnobservedComponents  # type: ignore
except Exception:
    UnobservedComponents = None

CONFIG_DIR = os.getenv("CONFIG_DIR", "config")


def load_yaml(fp: str) -> dict:
    import yaml
    with open(fp, "r") as f:
        return yaml.safe_load(f)


def rolling_z(
    df: pl.DataFrame,
    value_col: str = "value",
    window_years: int = 10,
    freq: str = "M",
    clamp: float = 4.0,
) -> pl.DataFrame:
    # approximate rolling window length from freq
    n = {"D": 252 * window_years, "W": 52 * window_years, "M": 12 * window_years, "Q": 4 * window_years}.get(
        freq, 120
    )
    out = (
        df.with_columns(
            [
                pl.col(value_col).rolling_mean(window_size=n).alias("mu"),
                pl.col(value_col).rolling_std(window_size=n).alias("sigma"),
            ]
        )
        .with_columns(
            [
                pl.when(pl.col("sigma") > 0)
                .then((pl.col(value_col) - pl.col("mu")) / pl.col("sigma"))
                .otherwise(None)
                .alias("z")
            ]
        )
        .with_columns([pl.col("z").clip(-clamp, clamp)])
    )
    return out


def ema(df: pl.DataFrame, value_col: str = "value", span: int = 3) -> pl.DataFrame:
    alpha = 2 / (span + 1)
    return df.with_columns(pl.col(value_col).ewm_mean(alpha=alpha, adjust=False).alias(value_col))


def pct_change(df: pl.DataFrame, periods: int = 12, value_col: str = "value") -> pl.DataFrame:
    return df.with_columns((pl.col(value_col) / pl.col(value_col).shift(periods) - 1).alias(value_col))


def diff(df: pl.DataFrame, periods: int = 1, value_col: str = "value") -> pl.DataFrame:
    return df.with_columns((pl.col(value_col) - pl.col(value_col).shift(periods)).alias(value_col))


def annualize_3m(df: pl.DataFrame, value_col: str = "value") -> pl.DataFrame:
    # 3m rate annualized ≈ 4 * 3m change
    return df.with_columns((pl.col(value_col) * 4).alias(value_col))


def apply_pipeline(df: pl.DataFrame, pipeline: List[dict]) -> pl.DataFrame:
    out = df.sort("date")
    for step in pipeline:
        kind = step["kind"]
        if kind == "pct_change":
            out = pct_change(out, periods=int(step.get("periods", 12)))
        elif kind == "diff":
            out = diff(out, periods=int(step.get("periods", 1)))
        elif kind == "rolling_rate":
            if step.get("annualize", True):
                out = annualize_3m(out)
        elif kind == "ema":
            out = ema(out, span=int(step.get("span", 3)))
        elif kind == "winsorize":
            q = float(step.get("q", 0.01))
            # compute quantiles and clip
            vals = out.select(pl.col("value")).to_series()
            lo = float(vals.quantile(q)) if vals.len() else None
            hi = float(vals.quantile(1 - q)) if vals.len() else None
            if lo is not None and hi is not None:
                out = out.with_columns(pl.col("value").clip(lo, hi))
        elif kind == "hp_filter":
            lam = float(step.get("lambda", 129600))
            component = step.get("component", "cycle")  # or 'trend'
            if hpfilter is not None and out.height:
                s = pd.Series(out.get_column("value").to_list())
                cycle, trend = hpfilter(s, lamb=lam)
                out = out.with_columns(
                    pl.Series("value", (cycle if component == "cycle" else trend).values)
                )
        elif kind == "butterworth":
            # approximate with Christiano-Fitzgerald filter if available
            if cffilter is not None and out.height:
                s = pd.Series(out.get_column("value").to_list())
                cl, ch = cffilter(s, low=6, high=32)  # defaults for business cycle
                component = step.get("component", "cycle")
                val = ch if component == "trend" else cl
                out = out.with_columns(pl.Series("value", val.values))
            else:
                # fallback to ema
                out = ema(out, span=int(step.get("span", 3)))
        elif kind == "kalman_smoother":
            # local level model via statsmodels UCM
            if UnobservedComponents is not None and out.height:
                s = pd.Series(out.get_column("value").to_list())
                try:
                    mod = UnobservedComponents(s, level='llevel')
                    res = mod.fit(disp=False)
                    smooth = res.level_smoothed
                    out = out.with_columns(pl.Series("value", pd.Series(smooth).values))
                except Exception:
                    out = ema(out, span=int(step.get("span", 3)))
        elif kind == "seasonal_adjust_proxy":
            # simple proxy: subtract 12m rolling mean (needs monthly df)
            out = out.with_columns(
                (pl.col("value") - pl.col("value").rolling_mean(window_size=int(step.get("window", 12)))).alias("value")
            )
        elif kind == "join":
            # compose operation with another series, e.g., deflate
            op = step.get("op", "deflate")
            other = step.get("with")
            if other:
                fp = f"data/indicators/{other}.parquet"
                if os.path.exists(fp):
                    j = pl.read_parquet(fp).select(["date", "value"]).rename({"value": "other_val"})
                    out = out.join(j, on="date", how="left")
                    if op == "deflate":
                        # divide by other series, avoid division by zero
                        out = out.with_columns(
                            pl.when(pl.col("other_val").abs() > 1e-12)
                            .then(pl.col("value") / pl.col("other_val"))
                            .otherwise(pl.lit(None))
                            .alias("value")
                        )
                    out = out.drop("other_val")
        else:
            raise ValueError(f"unknown transform: {kind}")
    return out


def canonicalize_freq(df: pl.DataFrame, freq: str) -> pl.DataFrame:
    df = df.sort("date")
    if freq == "M":
        # end-of-month last value
        return (
            df.group_by_dynamic("date", every="1mo", period="1mo", closed="left", label="right")
            .agg([pl.col("value").last().alias("value")])
            .drop_nulls()
        )
    elif freq == "W":
        # weekly last value
        return (
            df.group_by_dynamic("date", every="1w", period="1w", closed="right", label="right")
            .agg([pl.col("value").last().alias("value")])
            .drop_nulls()
        )
    elif freq == "D":
        return df
    else:
        return df


def load_indicator_configs() -> Tuple[dict, dict, dict]:
    ind_cfg = load_yaml(os.path.join(CONFIG_DIR, "indicators.yaml"))
    comp_cfg = load_yaml(os.path.join(CONFIG_DIR, "composite.yaml"))
    pillars_cfg = load_yaml(os.path.join(CONFIG_DIR, "pillars.yaml"))
    return ind_cfg or {}, comp_cfg or {}, pillars_cfg or {}
