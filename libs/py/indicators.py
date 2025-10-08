import os
from typing import Dict, List, Tuple
import polars as pl

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
        else:
            raise ValueError(f"unknown transform: {kind}")
    return out


def load_indicator_configs() -> Tuple[dict, dict, dict]:
    ind_cfg = load_yaml(os.path.join(CONFIG_DIR, "indicators.yaml"))
    comp_cfg = load_yaml(os.path.join(CONFIG_DIR, "composite.yaml"))
    pillars_cfg = load_yaml(os.path.join(CONFIG_DIR, "pillars.yaml"))
    return ind_cfg or {}, comp_cfg or {}, pillars_cfg or {}

