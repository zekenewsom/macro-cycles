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

# Explainability + Monthly Note endpoints

ART_DIR = "data/artifacts"


@app.get("/explain/indicator-snapshot")
def explain_indicator_snapshot(top_k: int = 12):
    fp = os.path.join(ART_DIR, "explain_indicator_snapshot.parquet")
    if not os.path.exists(fp):
        return {"items": []}
    df = pl.read_parquet(fp).sort(pl.col("composite_contrib_est").abs(), descending=True).head(top_k)
    return {"items": df.to_dicts()}


@app.get("/explain/pillar-contrib-timeseries")
def explain_pillar_contrib_timeseries():
    fp = os.path.join(ART_DIR, "explain_pillar_contrib_timeseries.parquet")
    if not os.path.exists(fp):
        return {"series": []}
    df = pl.read_parquet(fp).sort(["pillar", "date"]) if os.path.getsize(fp) > 0 else pl.DataFrame()
    if df.is_empty():
        return {"series": []}
    # ensure numeric and null-safe values
    if "contribution" in df.columns:
        df = df.with_columns(pl.col("contribution").cast(pl.Float64).fill_null(0.0))
    # reshape: one series per pillar
    series = []
    for p in df.select("pillar").unique()["pillar"].to_list():
        sub = df.filter(pl.col("pillar") == p).select(["date", "contribution"]).to_dicts()
        points = []
        for r in sub:
            val = r.get("contribution")
            points.append({"date": str(r.get("date")), "value": float(val) if val is not None else 0.0})
        series.append({"pillar": p, "points": points})
    return {"series": series}


@app.get("/explain/probit-effects")
def explain_probit_effects():
    fp = os.path.join(ART_DIR, "explain_probit_effects.parquet")
    if not os.path.exists(fp):
        return {"items": []}
    df = pl.read_parquet(fp)
    return {"items": df.to_dicts()}


@app.get("/note/monthly")
def note_monthly():
    fp = os.path.join(ART_DIR, "monthly_note.md")
    if not os.path.exists(fp):
        return {"markdown": "# Monthly Macro Note\n\n(Generate the note first.)"}
    with open(fp, "r") as f:
        md = f.read()
    return {"markdown": md, "generated_at": os.path.getmtime(fp)}


# Turning Points: track labels for regime bands
@app.get("/turning-points/track")
def turning_points_track(name: str = "business"):
    base = os.path.join("data", "regimes")
    fp = os.path.join(base, f"{name}.parquet")
    if not os.path.exists(fp):
        return {"labels": []}
    try:
        df = pl.read_parquet(fp)
        if df.height == 0:
            return {"labels": []}
        rows = df.select(["date", "label"]).drop_nulls().to_dicts()
        return {"labels": [{"date": str(r["date"]), "label": r["label"]} for r in rows]}
    except Exception:
        return {"labels": []}

@app.get("/meta/dq")
def meta_dq():
    dq_dir = os.path.join(DATA_DIR, "_dq")
    items = []
    warnings: List[str] = []
    if not os.path.isdir(dq_dir):
        warnings.append("DQ directory not found; run build_indicators for diagnostics")
        return {"items": items, "meta": meta_with_warnings(warnings=warnings)}
    for fn in os.listdir(dq_dir):
        if fn.endswith(".json"):
            try:
                with open(os.path.join(dq_dir, fn), "r") as f:
                    items.append(json.load(f))
            except Exception:
                continue
    # surface warnings for stale (>45 days) or low rows
    for it in items:
        if (it.get("staleness_days") or 0) > 45:
            warnings.append(f"{it['series_id']} stale {it['staleness_days']}d")
        if (it.get("rows") or 0) < 10:
            warnings.append(f"{it['series_id']} few rows: {it['rows']}")
    return {"items": items, "meta": meta_with_warnings(warnings=list(set(warnings)))}

# Data Browser endpoints
@app.get("/data/catalog")
def data_catalog():
    base = os.path.join(DATA_DIR, "indicators")
    cfg_fp = os.path.join("config", "indicators.yaml")
    if not os.path.isdir(base):
        return {"items": []}
    # load config mapping for freq, label, pillar
    try:
        with open(cfg_fp, "r") as f:
            cfg = yaml.safe_load(f) or {}
        ind = cfg.get("indicators", []) or []
    except Exception:
        ind = []
    meta = {i.get("series_id"): i for i in ind}
    items = []
    for fn in os.listdir(base):
        if fn.endswith(".parquet") and not fn.startswith("_"):
            fp = os.path.join(base, fn)
            try:
                df = pl.read_parquet(fp, n_rows=5)
                sid = df.get_column("series_id").head(1).to_list()[0] if "series_id" in df.columns else fn.replace(".parquet", "")
                label = df.get_column("label").head(1).to_list()[0] if "label" in df.columns else sid
                pillar = df.get_column("pillar").head(1).to_list()[0] if "pillar" in df.columns else (meta.get(sid, {}).get("pillar"))
                freq = meta.get(sid, {}).get("freq")
                # min/max date via lazy scan to avoid loading full file
                df2 = pl.read_parquet(fp, columns=["date"], use_pyarrow=True)
                if df2.height:
                    dmin = str(df2.get_column("date").min())
                    dmax = str(df2.get_column("date").max())
                    count = int(df2.height)
                else:
                    dmin = dmax = None
                    count = 0
                items.append({
                    "series_id": sid,
                    "label": label,
                    "pillar": pillar,
                    "freq": freq,
                    "min_date": dmin,
                    "max_date": dmax,
                    "count": count,
                    "path": fp,
                })
            except Exception:
                continue
    return {"items": items, "meta": meta_with_warnings()}


@app.get("/data/series")
def data_series(series_id: str, include_z: bool = True, vintage: str | None = None):
    base = os.path.join(DATA_DIR, "indicators")
    path = os.path.join(base, f"{series_id}.parquet")
    if not os.path.exists(path):
        return {"series": [], "meta": meta_with_warnings(warnings=[f"No indicators parquet for {series_id}"])}
    df = pl.read_parquet(path)
    cols = ["date", "value"] + (["z"] if include_z and "z" in df.columns and not vintage else [])
    df = df.select([c for c in cols if c in df.columns]).sort("date")
    # Optional vintage-as-of selection (using ALFRED vintages if available)
    if vintage:
        try:
            vdt = pl.datetime.strptime(vintage, "%Y-%m-%d")
        except Exception:
            vdt = None
        vint_path = os.path.join(DATA_DIR, "vintage", "fred", f"{series_id}.parquet")
        if vdt and os.path.exists(vint_path):
            vint = pl.read_parquet(vint_path).select(["date", "value", "vintage"]).filter(pl.col("vintage") <= vdt).sort(["date", "vintage"])  # type: ignore
            vint_asof = vint.group_by("date").agg(pl.col("value").last()).rename({"value": "value_vint"})
            df = df.join(vint_asof, on="date", how="left").with_columns(pl.coalesce([pl.col("value_vint"), pl.col("value")]).alias("value")).drop("value_vint")
            # If vintage-as-of is used, z from latest is not appropriate; omit z
            if "z" in df.columns:
                df = df.drop("z")
    out = [{"date": str(r[0]), "value": (float(r[1]) if r[1] is not None else None), **({"z": (float(r[2]) if len(r) > 2 and r[2] is not None else None)} if len(cols) > 2 else {})} for r in df.iter_rows()]
    # attach basic metadata
    meta_row = pl.read_parquet(path, n_rows=1)
    label = meta_row.get_column("label").to_list()[0] if "label" in meta_row.columns and meta_row.height else series_id
    pillar = meta_row.get_column("pillar").to_list()[0] if "pillar" in meta_row.columns and meta_row.height else None
    return {"series": out, "meta": meta_with_warnings(extra={"series_id": series_id, "label": label, "pillar": pillar, "vintage": vintage})}


# Markets summary / assets endpoints
@app.get("/market/summary")
def market_summary(ids: str = "SPX,UST2Y,UST10Y,TWEXB,GOLD,BTC"):
    out = []
    for t in ids.split(","):
        sid = t.strip()
        path = f"data/raw/market/{sid}.parquet"
        if not os.path.exists(path):
            out.append({"id": sid, "warning": "missing"})
            continue
        df = pl.read_parquet(path).select(["date", "value"]).drop_nulls().sort("date")
        if df.height == 0:
            out.append({"id": sid, "warning": "empty"})
            continue
        last = float(df.tail(1)["value"][0])
        # simple sparkline last 30 values
        spark = [float(v) for v in df.tail(30)["value"].to_list()]
        # 50-day moving average and 20-day vol proxy if daily
        ma = None
        try:
            ma = float(df.tail(50)["value"].mean()) if df.height >= 50 else None
        except Exception:
            ma = None
        out.append({"id": sid, "last": last, "ma50": ma, "spark": spark})
    return {"items": out, "meta": meta_with_warnings()}


@app.get("/market/assets")
def market_assets(ids: str = "SPX,UST2Y,UST10Y,TWEXB,GOLD,BTC"):
    series = []
    for t in ids.split(","):
        sid = t.strip()
        path = f"data/raw/market/{sid}.parquet"
        if not os.path.exists(path):
            series.append({"id": sid, "points": []})
            continue
        df = pl.read_parquet(path).select(["date", "value"]).drop_nulls().sort("date")
        pts = [{"date": str(d), "value": float(v)} for d, v in zip(df["date"].to_list(), df["value"].to_list())]
        series.append({"id": sid, "points": pts})
    return {"series": series, "meta": meta_with_warnings()}
