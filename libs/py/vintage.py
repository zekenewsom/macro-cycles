import polars as pl

def asof_snapshot(df: pl.DataFrame, asof: str) -> pl.DataFrame:
    """
    df has columns: date, value, vintage (date string)
    Return the last vintage <= asof for each 'date'.
    """
    dd = (
        df.select([
            pl.col("date").cast(pl.Date),
            pl.col("value").alias("value"),
            pl.col("vintage").str.strptime(pl.Date, strict=False).alias("vintage"),
        ])
        .filter(pl.col("vintage") <= pl.lit(asof))
        .sort(["date", "vintage"])
    )
    if dd.is_empty():
        return dd.select(["date", "value", "vintage"]) if set(["date","value","vintage"]).issubset(set(dd.columns)) else dd
    last = dd.group_by("date").agg(pl.col("vintage").max().alias("vintage"))
    out = last.join(dd, on=["date", "vintage"], how="left").sort("date")
    return out.select(["date", "value", "vintage"])  # type: ignore

