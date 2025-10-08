import os, polars as pl
from prefect import flow, task
from datetime import datetime

@task
def ensure_dirs():
    os.makedirs("data/raw", exist_ok=True)

@task
def write_demo_series():
    # this will be replaced by real FRED/yfinance pulls later
    df = pl.DataFrame({"series_id":["DEMO"]*5,
                       "date":pl.date_range(low=datetime(2020,1,1), high=datetime(2020,5,1), interval="1mo"),
                       "value":[1,2,3,2,4],
                       "source":["demo"]*5})
    df.write_parquet("data/raw/demo_series.parquet")

@flow
def ingest_demo():
    ensure_dirs()
    write_demo_series()

if __name__ == "__main__":
    ingest_demo()
