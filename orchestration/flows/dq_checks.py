import os, polars as pl, json
from datetime import date
from prefect import flow, task

RAW_DIRS = ["data/raw/fred", "data/raw/market", "data/vintages/fred"]
OUT = "data/_dq"

@task
def ensure_dirs():
    os.makedirs(OUT, exist_ok=True)


def _dq_one_parquet(fp: str):
    try:
        df = pl.read_parquet(fp)
        if "date" in df.columns:
            dmin = df["date"].min()
            dmax = df["date"].max()
            n = df.height
            return {
                "path": fp,
                "rows": n,
                "date_min": str(dmin),
                "date_max": str(dmax),
                "staleness_days": (date.today() - dmax).days if hasattr(dmax, "year") else None,
            }
        else:
            return {"path": fp, "rows": df.height}
    except Exception as e:
        return {"path": fp, "error": str(e)}


@task
def scan_and_write():
    reports = []
    for root in RAW_DIRS:
        if not os.path.isdir(root):
            continue
        for fn in os.listdir(root):
            if not fn.endswith(".parquet"):
                continue
            reports.append(_dq_one_parquet(os.path.join(root, fn)))
    stale = [r for r in reports if isinstance(r.get("staleness_days"), int) and r["staleness_days"] > 45]
    res = {"generated_at": date.today().isoformat(), "count": len(reports), "stale_over_45d": stale, "items": reports}
    with open(os.path.join(OUT, "dq.json"), "w") as f:
        json.dump(res, f, indent=2)
    return res


@flow(name="dq_checks")
def run():
    ensure_dirs()
    return scan_and_write()


if __name__ == "__main__":
    run()

