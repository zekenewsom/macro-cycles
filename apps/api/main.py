from fastapi import FastAPI, Query
from datetime import datetime, timedelta
import duckdb, os, json
import yaml
from typing import List
import polars as pl

app = FastAPI(title="Macro Cycles Local API", version="0.1.0")
DB_PATH = os.getenv("DUCKDB_PATH", "db/catalog.duckdb")

def connect():
    os.makedirs("db", exist_ok=True)
    return duckdb.connect(DB_PATH)

def load_sources_mapping():
    try:
        with open(os.path.join("config", "sources.yaml"), "r") as f:
            cfg = yaml.safe_load(f) or {}
        series = (cfg.get("fred", {}) or {}).get("series", [])
        mapping = {}
        for s in series:
            sid = s.get("id")
            if sid:
                mapping[sid] = {"name": s.get("name", sid), "pillar": s.get("pillar")}
        return mapping
    except Exception:
        return {}

def meta_with_warnings(extra: dict | None = None, warnings: List[str] | None = None):
    m = {"data_vintage": datetime.utcnow().isoformat(), "config_version": "v0", "git_hash": "local"}
    if extra:
        m.update(extra)
    if warnings:
        m["warnings"] = warnings
    return m

@app.get("/health")
def health():
    return {"status": "ok", "ts": datetime.utcnow().isoformat()}

@app.get("/overview/composite")
def composite(window_years: int = 40):
    con = connect()
    warnings: List[str] = []
    if not (os.path.exists("data/composite") and len(os.listdir("data/composite")) > 0):
        warnings.append("Composite parquet not found under data/composite; run build_pipeline.")
        return {"series": [], "meta": meta_with_warnings(warnings=warnings)}
    res = con.sql(
        """
        SELECT 
            date,
            COALESCE(score, 0.0) AS score,
            COALESCE(z, 0.0)     AS z,
            regime,
            COALESCE(confidence, 0.0) AS confidence
        FROM read_parquet('data/composite/composite.parquet')
        WHERE date >= DATE '1970-01-01' AND z IS NOT NULL
        ORDER BY date
        """
    ).fetchall()
    series = [{"date": str(r[0]), "score": float(r[1]), "z": float(r[2]),
               "regime": r[3], "confidence": float(r[4])} for r in res]
    return {"series": series, "meta": meta_with_warnings(warnings=warnings)}

@app.get("/overview/pillars")
def pillars():
    warnings: List[str] = []
    if not (os.path.exists("data/pillars") and len(os.listdir("data/pillars")) > 0):
        warnings.append("Pillars parquet not found under data/pillars; run build_pipeline.")
        return {"pillars": [], "meta": meta_with_warnings(warnings=warnings)}
    con = connect()
    rows = con.sql(
        """
        WITH base AS (
            SELECT
                pillar,
                date,
                z,
                diffusion,
                (z - LAG(z, 3) OVER (PARTITION BY pillar ORDER BY date)) AS momentum_3m
            FROM read_parquet('data/pillars/pillars.parquet')
        ), s AS (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY pillar ORDER BY date DESC) AS rn
            FROM base
        )
        SELECT pillar, z, momentum_3m, diffusion
        FROM s WHERE rn = 1
        ORDER BY pillar
        """
    ).fetchall()
    pillars = [
        {"pillar": r[0], "z": float(r[1]), "momentum_3m": float(r[2]), "diffusion": float(r[3]), "components": []}
        for r in rows
    ]
    return {"pillars": pillars, "meta": meta_with_warnings(warnings=warnings)}

@app.get("/overview/movers")
def movers(window_days: int = 7, top_k: int = 10):
    warnings: List[str] = []
    if not (os.path.exists("data/indicators") and len(os.listdir("data/indicators")) > 0):
        warnings.append("Indicators parquet not found under data/indicators; run build_pipeline.")
        return {"gainers": [], "losers": [], "meta": meta_with_warnings(warnings=warnings)}
    con = connect()
    mapping = load_sources_mapping()
    res = con.sql(
        """
        WITH z AS (
            SELECT series_id, date, z,
                   LAG(z, 1) OVER (PARTITION BY series_id ORDER BY date) AS z_prev
            FROM read_parquet('data/indicators/*.parquet', union_by_name=true)
            WHERE date IS NOT NULL AND z IS NOT NULL
        ), latest AS (
            SELECT series_id,
                   ANY_VALUE(z) FILTER (WHERE rn=1) AS z_after,
                   ANY_VALUE(z_prev) FILTER (WHERE rn=1) AS z_before
            FROM (
                SELECT *, ROW_NUMBER() OVER (PARTITION BY series_id ORDER BY date DESC) AS rn
                FROM z
            ) t
            WHERE rn <= 1
            GROUP BY series_id
        )
        SELECT series_id, COALESCE(z_after, 0.0) AS z_after,
               COALESCE(z_after - z_before, 0.0) AS delta
        FROM latest
        ORDER BY delta DESC
        """
    ).fetchall()
    rows = [{"id": r[0], "z_after": float(r[1]), "delta": float(r[2])} for r in res]
    for row in rows:
        info = mapping.get(row["id"], {})
        row["label"] = info.get("name", row["id"])
        row["pillar"] = info.get("pillar")
        row["source"] = "FRED"
    gainers = rows[:top_k]
    losers = sorted(rows, key=lambda x: x["delta"])[:top_k]
    return {"gainers": gainers, "losers": losers, "meta": meta_with_warnings(warnings=warnings)}

@app.get("/market/regimes")
def market_regimes(tickers: str = "SPX,UST2Y,UST10Y,USD,GOLD,BTC"):
    warnings: List[str] = []
    con = connect()
    assets = []
    for t in tickers.split(","):
        alias = t.strip()
        path = f"data/raw/market/{alias}.parquet"
        if not os.path.exists(path):
            warnings.append(f"Missing market parquet for {alias} at {path}")
            continue
        res = con.sql(f"""
            WITH s AS (
                SELECT date, value
                FROM read_parquet('{path}')
                ORDER BY date
            ), f AS (
                SELECT *,
                    AVG(value) OVER (ROWS BETWEEN 49 PRECEDING AND CURRENT ROW) AS ma50,
                    value / NULLIF(LAG(value) OVER (ORDER BY date), 0) - 1.0 AS ret
                FROM s
            ), g AS (
                SELECT *,
                    STDDEV_SAMP(ret) OVER (ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS vol20
                FROM f
            )
            SELECT * FROM g ORDER BY date DESC LIMIT 60
        """).fetchdf()
        if res.empty:
            warnings.append(f"No rows for {alias}; check data source")
            continue
        last = res.iloc[0]
        val, ma50, vol20 = float(last["value"]), float(last["ma50"]), float(last.get("vol20") or 0.0)
        state = "trend_up" if val > ma50 else ("trend_down" if val < ma50 else "range")
        p_up = 0.7 if state == "trend_up" else 0.2
        p_down = 0.7 if state == "trend_down" else 0.2
        p_range = 1.0 - max(p_up, p_down)
        spark = list(reversed(res["value"].head(7).astype(float).tolist()))
        assets.append({
            "ticker": alias,
            "label": f"{'Trend-Up' if state=='trend_up' else ('Trend-Down' if state=='trend_down' else 'Range')} / Vol {vol20:.2f}",
            "state": state,
            "probas": [
                {"state": "trend_up", "p": round(p_up, 2)},
                {"state": "range", "p": round(p_range, 2)},
                {"state": "trend_down", "p": round(p_down, 2)},
            ],
            "sparkline": spark,
            "last_updated": datetime.utcnow().isoformat(),
        })
    return {"assets": assets, "meta": meta_with_warnings(warnings=warnings)}

# Drivers/Data Browser endpoints
DATA_DIR = "data"

@app.get("/drivers/pillars")
def drivers_pillars():
    warnings: List[str] = []
    fp = os.path.join(DATA_DIR, "pillars", "pillars.parquet")
    if not os.path.exists(fp):
        warnings.append("Missing pillars parquet; run build_pillars.")
        return {"pillars": [], "meta": meta_with_warnings(warnings=warnings)}
    df = pl.read_parquet(fp)
    latest = df.group_by("pillar").agg(pl.col("date").max().alias("date")).join(df, on=["pillar", "date"], how="left")
    pillars = []
    for r in latest.to_dicts():
        pillars.append(
            {
                "pillar": r["pillar"],
                "z": float(r["z"]) if r.get("z") is not None else None,
                "momentum_3m": None,
                "diffusion": float(r["diffusion"]) if r.get("diffusion") is not None else None,
                "components": [],
            }
        )
    return {"pillars": pillars, "meta": meta_with_warnings(warnings=warnings)}


@app.get("/drivers/indicator-heatmap")
def indicator_heatmap(pillar: str = Query(..., description="pillar id, e.g. growth")):
    base = os.path.join(DATA_DIR, "indicators")
    if not os.path.isdir(base):
        return {"dates": [], "series": [], "matrix": [], "meta": meta_with_warnings(warnings=["No indicators found"]) }
    mats = []
    for fn in os.listdir(base):
        if fn.endswith(".parquet") and not fn.startswith("_"):
            df = pl.read_parquet(os.path.join(base, fn))
            if df.height == 0:
                continue
            # Filter pillar
            if pillar not in df["pillar"].unique().to_list():
                continue
            mats.append(df.select(["date", "z", "series_id"]).drop_nulls())
    if not mats:
        return {"dates": [], "series": [], "matrix": [], "meta": meta_with_warnings(warnings=[f"No indicators for pillar {pillar}"]) }
    allz = pl.concat(mats).sort(["series_id", "date"])
    wide = allz.pivot(values="z", index="date", columns="series_id").sort("date")
    dates = [str(d) for d in wide["date"].to_list()]
    series_cols = [c for c in wide.columns if c != "date"]
    matrix = [ [ row.get(c) for c in series_cols ] for row in wide.iter_rows(named=True) ]
    return {"dates": dates, "series": series_cols, "matrix": matrix, "meta": meta_with_warnings()}


@app.get("/drivers/contributions")
def contributions():
    fp = os.path.join(DATA_DIR, "composite", "contributions.parquet")
    if not os.path.exists(fp):
        return {"items": [], "meta": meta_with_warnings(warnings=["Missing contributions parquet; run build_composite."]) }
    df = pl.read_parquet(fp).sort(pl.col("contribution").abs(), descending=True)
    return {"items": df.to_dicts(), "meta": meta_with_warnings()}

@app.get("/meta/changelog")
def changelog():
    return {"entries":[{"ts":datetime.utcnow().isoformat(),"kind":"source","version":"v0","message":"initial demo"}]}
