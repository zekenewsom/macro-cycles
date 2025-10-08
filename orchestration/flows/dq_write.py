import os, json
import polars as pl
from datetime import date
from prefect import flow

OUT = "data/_dq"
SRC = [("data/raw/fred", "fred"), ("data/raw/market", "market"), ("data/indicators", "indicator")]


@flow(name="dq_write")
def run():
    os.makedirs(OUT, exist_ok=True)
    reports: list[dict] = []
    for folder, tag in SRC:
        if not os.path.isdir(folder):
            continue
        for fn in os.listdir(folder):
            if not fn.endswith(".parquet") or fn.startswith("_"):
                continue
            fp = os.path.join(folder, fn)
            try:
                df = pl.read_parquet(fp)
                dmin = str(df["date"].min()) if "date" in df.columns and df.height else None
                dmax = str(df["date"].max()) if "date" in df.columns and df.height else None
                staleness = None
                if dmax:
                    try:
                        staleness = (date.today() - df["date"].max()).days  # type: ignore
                    except Exception:
                        pass
                rep = {
                    "source": tag,
                    "path": fp,
                    "rows": int(df.height),
                    "date_min": dmin,
                    "date_max": dmax,
                    "staleness_days": staleness,
                }
                reports.append(rep)
            except Exception as e:
                reports.append({"source": tag, "path": fp, "error": str(e)})
    with open(os.path.join(OUT, "dq.json"), "w") as f:
        json.dump({"generated_at": date.today().isoformat(), "items": reports}, f, indent=2)


if __name__ == "__main__":
    run()

