import polars as pl
from datetime import date, timedelta

def month_ends(start: date, end: date):
    cur = date(start.year, start.month, 1)
    acc = []
    while cur <= end:
        if cur.month == 12:
            nxt = date(cur.year + 1, 1, 1)
        else:
            nxt = date(cur.year, cur.month + 1, 1)
        acc.append(nxt - timedelta(days=1))
        cur = nxt
    return acc


def expanding_z_no_lookahead(df: pl.DataFrame, val_col: str = "value", sort_col: str = "date", min_periods: int = 36) -> pl.DataFrame:
    """
    df sorted by date; compute expanding mean/std up to t-1, then z_t = (x_t - mu_{t-1})/sigma_{t-1}.
    If not enough history, returns null.
    """
    d = df.sort(sort_col)
    d = d.with_columns([
        pl.col(val_col).cumcount().alias("_idx"),
        pl.col(val_col).cumsum().alias("_csum"),
        (pl.col(val_col) ** 2).cumsum().alias("_csum2"),
    ])
    d = d.with_columns([
        (pl.col("_csum") - pl.col(val_col)).alias("_sum_prev"),
        (pl.col("_csum2") - pl.col(val_col) ** 2).alias("_sum2_prev"),
        pl.col("_idx").alias("_n_prev"),
    ])
    mu = pl.col("_sum_prev") / pl.col("_n_prev").clip(lower_bound=1)
    var = (pl.col("_sum2_prev") - (pl.col("_sum_prev") ** 2) / pl.col("_n_prev").clip(lower_bound=1)) / (pl.col("_n_prev") - 1).clip(lower_bound=1)
    z = (pl.col(val_col) - mu) / pl.sqrt(var)
    out = d.with_columns(pl.when(pl.col("_n_prev") >= min_periods).then(z).otherwise(pl.lit(None)).alias("z"))
    return out.drop(["_idx", "_csum", "_csum2", "_sum_prev", "_sum2_prev", "_n_prev"])

