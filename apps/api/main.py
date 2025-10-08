from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime, timedelta
import duckdb, os, json
import yaml
from typing import List
import polars as pl
from math import isfinite
from apps.api.models import (
    MarketSummaryResponse,
    RegimeHistoryResponse,
    HMMResponse,
    TransitionMatrixResponse,
)
from libs.py.vintage import asof_snapshot

app = FastAPI(title="Macro Cycles Local API", version="0.1.0")

# CORS for local Next.js dev server
_origins = os.getenv("CORS_ORIGINS", "http://localhost:3000,http://127.0.0.1:3000").split(",")
origins = [o.strip() for o in _origins if o.strip()]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins or ["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
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

COMP_LIVE = os.path.join("data", "composite", "composite.parquet")
COMP_ASOF_DIR = os.path.join("data", "composite_asof")

@app.get("/overview/composite")
def composite(method: str = "weighted", asof: str | None = None, max_points: int = 2000):
    warnings: List[str] = []
    # Backward-compatible: if no asof, return the richer live series shape (score, regime, confidence)
    if not asof:
        fp = COMP_LIVE
        if not os.path.exists(fp):
            warnings.append("Composite parquet not found; run build_composite.")
            return {"series": [], "meta": meta_with_warnings(warnings=warnings)}
        df = pl.read_parquet(fp).select(["date", "score", "z", "z_median", "z_trimmed_mean", "diffusion_composite", "regime", "confidence"]).drop_nulls(subset=["date"]).sort("date")
        col = "z"
        m = (method or "weighted").lower()
        if m == "median" and "z_median" in df.columns:
            col = "z_median"
        elif m in ("trimmed", "trimmed_mean") and "z_trimmed_mean" in df.columns:
            col = "z_trimmed_mean"
        elif m == "diffusion" and "diffusion_composite" in df.columns:
            col = "diffusion_composite"
        series = []
        for r in df.iter_rows(named=True):
            series.append({
                "date": str(r["date"]),
                "score": float(r.get("score") or 0.0),
                "z": float(r.get(col) or 0.0),
                "regime": r.get("regime"),
                "confidence": float(r.get("confidence") or 0.0),
            })
        return {"series": series, "meta": meta_with_warnings(extra={"method": method, "asof": "live"})}
    # As-of path: minimal z-only series from composite_asof
    fp = os.path.join(COMP_ASOF_DIR, f"composite_{method}.parquet")
    if not os.path.exists(fp):
        warnings.append("As-of composite parquet not found; run build_composite_asof.")
        return {"series": [], "meta": meta_with_warnings(warnings=warnings)}
    df = pl.read_parquet(fp).select(["date", "z"]).drop_nulls(subset=["date"]).sort("date")
    df = df.filter(pl.col("date") <= pl.lit(asof))
    if df.height > max_points:
        stride = max(1, df.height // max_points)
        df = df.with_row_count().filter(pl.col("row_nr") % stride == 0).drop("row_nr")
    series = [{"date": str(d), "z": float(z)} for d, z in zip(df["date"], df["z"]) ]
    return {"series": series, "meta": meta_with_warnings(extra={"method": method, "asof": asof})}

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
def movers(window_days: int = 7, top_k: int = 10, pillar: str | None = None):
    warnings: List[str] = []
    if not (os.path.exists("data/indicators") and len(os.listdir("data/indicators")) > 0):
        warnings.append("Indicators parquet not found under data/indicators; run build_pipeline.")
        return {"gainers": [], "losers": [], "meta": meta_with_warnings(warnings=warnings)}
    con = connect()
    mapping = load_sources_mapping()
    res = con.sql(
        """
        WITH base AS (
            SELECT series_id,
                   date,
                   z,
                   COALESCE(pillar, NULL) AS pillar,
                   COALESCE(label, series_id) AS label
            FROM read_parquet('data/indicators/*.parquet', union_by_name=true)
            WHERE date IS NOT NULL AND z IS NOT NULL AND series_id IS NOT NULL
        ),
        s AS (
            SELECT *,
                   LAG(z, 1) OVER (PARTITION BY series_id ORDER BY date) AS z_prev,
                   ROW_NUMBER() OVER (PARTITION BY series_id ORDER BY date DESC) AS rn
            FROM base
        ),
        last AS (
            SELECT series_id,
                   ANY_VALUE(pillar) FILTER (WHERE rn=1) AS pillar,
                   ANY_VALUE(label)  FILTER (WHERE rn=1) AS label,
                   ANY_VALUE(z)      FILTER (WHERE rn=1) AS z_after,
                   ANY_VALUE(z_prev) FILTER (WHERE rn=1) AS z_before
            FROM s
            WHERE rn <= 1
            GROUP BY series_id
        )
        SELECT series_id, pillar, label,
               COALESCE(z_after, 0.0) AS z_after,
               COALESCE(z_after - z_before, 0.0) AS delta
        FROM last
        ORDER BY delta DESC
        """
    ).fetchall()
    rows = [{"id": r[0], "pillar": r[1], "label": r[2], "z_after": float(r[3]), "delta": float(r[4])} for r in res]
    # backfill labels/pillars from config mapping if missing
    for row in rows:
        info = mapping.get(row["id"], {})
        row["label"] = row.get("label") or info.get("name", row["id"])  # keep series_id fallback
        row["pillar"] = row.get("pillar") or info.get("pillar")
        row["source"] = row.get("source") or "FRED"
    # optional filter by pillar
    if pillar:
        rows = [r for r in rows if (r.get("pillar") or "").lower() == pillar.lower()]
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

# --- Market Regimes: history ribbons + transition matrices ---
REG_DIR = "data/regimes"
RAW_MKT_DIR = "data/raw/market"

def _labels_from_ma_vol(df: pl.DataFrame) -> list[dict]:
    # expects date,value; build ma50/vol20 then label per row
    if df.is_empty():
        return []
    d = df.sort("date").with_columns([
        pl.col("value").rolling_mean(window_size=50).alias("ma50"),
        pl.col("value").pct_change().rolling_std(window_size=20).alias("vol20"),
    ])
    d = d.with_columns(
        pl.when(pl.col("value") > pl.col("ma50")).then(pl.lit("Trend-Up")).otherwise(pl.lit("Trend-Down")).alias("trend")
    )
    # compute scalar quantiles on non-null vol20
    dv = d.filter(pl.col("vol20").is_not_null())
    q1 = q2 = None
    if not dv.is_empty():
        try:
            q1 = dv.select(pl.col("vol20").quantile(0.33, interpolation="nearest")).item()
            q2 = dv.select(pl.col("vol20").quantile(0.66, interpolation="nearest")).item()
        except Exception:
            q1 = q2 = None
    if q1 is not None and q2 is not None:
        d = d.with_columns(
            pl.when(pl.col("vol20").is_not_null() & (pl.col("vol20") <= pl.lit(q1)))
            .then(pl.lit("Low-Vol"))
            .when(pl.col("vol20").is_not_null() & (pl.col("vol20") <= pl.lit(q2)))
            .then(pl.lit("Mid-Vol"))
            .otherwise(pl.lit("High-Vol"))
            .alias("vol_bucket")
        )
    else:
        d = d.with_columns(pl.lit("Mid-Vol").alias("vol_bucket"))
    d = d.select(["date", "trend", "vol_bucket"]).drop_nulls(subset=["date", "trend"]).sort("date")
    return [{"date": str(r["date"]), "label": f"{r['trend']} / {r['vol_bucket']}"} for r in d.to_dicts()]


def _transition_matrix(labels: list[str]):
    states: list[str] = []
    for s in labels:
        if s not in states:
            states.append(s)
    idx = {s: i for i, s in enumerate(states)}
    n = len(states)
    if n == 0:
        return states, []
    counts = [[0] * n for _ in range(n)]
    for a, b in zip(labels[:-1], labels[1:]):
        counts[idx[a]][idx[b]] += 1
    probs = []
    for i in range(n):
        s = sum(counts[i]) or 1
        probs.append([counts[i][j] / s for j in range(n)])
    return states, probs


def _hmm_path(ticker: str) -> str:
    return os.path.join(REG_DIR, f"market_{ticker}_hmm.parquet")

def _stationary_from_matrix(mat: list[list[float]], iters: int = 500) -> list[float]:
    try:
        import numpy as np  # type: ignore
        if not mat:
            return []
        P = np.array(mat, dtype=float)
        n = P.shape[0]
        pi = np.ones(n, dtype=float) / n
        for _ in range(iters):
            pi = pi @ P
            s = float(pi.sum()) or 1.0
            pi = pi / s
        return [float(round(x, 4)) for x in pi.tolist()]
    except Exception:
        n = len(mat)
        if n == 0:
            return []
        pi = [1.0 / n] * n
        for _ in range(iters):
            nxt = [0.0] * n
            for i in range(n):
                row = mat[i] if i < len(mat) else []
                for j in range(n):
                    nxt[j] += pi[i] * (row[j] if j < len(row) else 0.0)
            s = sum(nxt) or 1.0
            pi = [x / s for x in nxt]
        return [round(x, 4) for x in pi]
    
# Backwards-compatible alias using eigenvector method if available
def _stationary_distribution(P: list[list[float]]) -> list[float]:
    try:
        import numpy as np  # type: ignore
        M = np.array(P, dtype=float)
        if M.ndim != 2 or M.shape[0] != M.shape[1] or M.size == 0:
            return []
        w, v = np.linalg.eig(M.T)
        j = int(np.argmin(np.abs(w - 1.0)))
        vec = np.real(v[:, j])
        vec = np.maximum(vec, 0)
        s = float(vec.sum())
        if s <= 0:
            return []
        pi = (vec / s).tolist()
        return [float(x) for x in pi]
    except Exception:
        return _stationary_from_matrix(P)


@app.get("/market/regimes/hmm", response_model=HMMResponse)
def market_regimes_hmm(tickers: str = "SPX,UST2Y,UST10Y,TWEXB,GOLD,BTC", days: int = 252 * 5):
    items = []
    for t in tickers.split(","):
        sid = t.strip()
        fp = _hmm_path(sid)
        if not os.path.exists(fp):
            items.append({"ticker": sid, "labels": [], "probs": []})
            continue
        df = pl.read_parquet(fp).sort("date")
        if days > 0 and df.height > days:
            df = df.tail(days)
        labels = [{"date": str(d), "label": lab} for d, lab in zip(df["date"], df["label"])]
        prob_cols = sorted([c for c in df.columns if c.startswith("p")])
        probs = []
        for c in prob_cols:
            probs.append({
                "state": c,
                "points": [{"date": str(d), "p": (float(v) if v is not None else None)} for d, v in zip(df["date"], df[c])],
            })
        # derive human labels per state index (mode of label by state)
        state_labels: list[str] = []
        try:
            for i in range(len(prob_cols)):
                sub = df.filter(pl.col("state") == i)
                if sub.height:
                    top = sub.group_by("label").count().sort("count", descending=True).head(1)["label"].to_list()[0]
                    state_labels.append(str(top))
                else:
                    state_labels.append(f"p{i}")
        except Exception:
            state_labels = [f"p{i}" for i in range(len(prob_cols))]
        items.append({"ticker": sid, "labels": labels, "probs": probs, "latest": labels[-1] if labels else None, "state_labels": state_labels})
    return {"items": items, "meta": meta_with_warnings()}


@app.get("/market/regimes/hmm/transition-matrix", response_model=TransitionMatrixResponse)
def market_regimes_hmm_transition_matrix(tickers: str = "SPX,UST2Y,UST10Y,TWEXB,GOLD,BTC", days: int = 252 * 2):
    items = []
    for t in tickers.split(","):
        sid = t.strip()
        fp = _hmm_path(sid)
        if not os.path.exists(fp):
            items.append({"ticker": sid, "states": [], "matrix": []})
            continue
        df = pl.read_parquet(fp).select(["date", "state"]).sort("date")
        if days > 0 and df.height > days:
            df = df.tail(days)
        states = df["state"].to_list()
        n = (max(states) + 1) if states else 0
        counts = [[0] * n for _ in range(n)]
        for a, b in zip(states[:-1], states[1:]):
            counts[a][b] += 1
        mat = []
        for i in range(n):
            s = sum(counts[i]) or 1
            mat.append([counts[i][j] / s for j in range(n)])
        # derive labels per state
        labels_map: list[str] = []
        try:
            for i in range(n):
                sub = pl.read_parquet(fp).filter(pl.col("state") == i)
                if sub.height:
                    top = sub.group_by("label").count().sort("count", descending=True).head(1)["label"].to_list()[0]
                    labels_map.append(str(top))
                else:
                    labels_map.append(f"p{i}")
        except Exception:
            labels_map = [f"p{i}" for i in range(n)]
        items.append({"ticker": sid, "states": [f"p{idx}" for idx in range(n)], "matrix": mat, "labels": labels_map, "stationary": _stationary_from_matrix(mat)})
    return {"items": items, "meta": meta_with_warnings()}


@app.get("/market/regimes/history", response_model=RegimeHistoryResponse)
def market_regime_history(tickers: str = "SPX,UST2Y,UST10Y,TWEXB,GOLD,BTC", days: int = 252 * 5, mode: str = "auto"):
    # mode: auto -> prefer hmm if exists; "hmm" -> force hmm; "heur" -> MA/vol heuristic
    assets = []
    for t in tickers.split(","):
        sid = t.strip()
        hmm_fp = _hmm_path(sid)
        if mode in ("auto", "hmm") and os.path.exists(hmm_fp):
            df = pl.read_parquet(hmm_fp).sort("date")
            if days > 0 and df.height > days:
                df = df.tail(days)
            assets.append({"ticker": sid, "points": [{"date": str(d), "label": lab} for d, lab in zip(df["date"], df["label"]) ]})
            continue
        # fallback heuristic
        path = os.path.join(RAW_MKT_DIR, f"{sid}.parquet")
        if not os.path.exists(path):
            assets.append({"ticker": sid, "points": []})
            continue
        df = pl.read_parquet(path).select(["date", "value"]).drop_nulls().sort("date")
        if days > 0 and df.height > days:
            df = df.tail(days)
        labs = _labels_from_ma_vol(df)
        assets.append({"ticker": sid, "points": labs})
    return {"assets": assets, "meta": meta_with_warnings()}


@app.get("/market/regimes/transition-matrix")
def market_regime_transition_matrix(tickers: str = "SPX,UST2Y,UST10Y,TWEXB,GOLD,BTC", days: int = 252 * 2, mode: str = "auto"):
    items = []
    for t in tickers.split(","):
        sid = t.strip()
        hmm_fp = _hmm_path(sid)
        if mode in ("auto", "hmm") and os.path.exists(hmm_fp):
            df = pl.read_parquet(hmm_fp).select(["date", "state"]).sort("date")
            if days > 0 and df.height > days:
                df = df.tail(days)
            sarr = df["state"].to_list()
            n = (max(sarr) + 1) if sarr else 0
            C = [[0] * n for _ in range(n)]
            for a, b in zip(sarr[:-1], sarr[1:]):
                C[a][b] += 1
            P = [[(C[i][j] / (sum(C[i]) or 1)) for j in range(n)] for i in range(n)]
            items.append({"ticker": sid, "states": [f"p{idx}" for idx in range(n)], "matrix": P, "stationary": _stationary_from_matrix(P)})
            continue
        # fallback heuristic label transitions
        path = os.path.join(RAW_MKT_DIR, f"{sid}.parquet")
        if not os.path.exists(path):
            items.append({"ticker": sid, "states": [], "matrix": []})
            continue
        df = pl.read_parquet(path).select(["date", "value"]).drop_nulls().sort("date")
        if days > 0 and df.height > days:
            df = df.tail(days)
        d = df.sort("date").with_columns(pl.col("value").rolling_mean(window_size=50).alias("ma50"))
        d = d.with_columns(pl.when(pl.col("value") > pl.col("ma50")).then(pl.lit("U")).otherwise(pl.lit("D")).alias("s"))
        labs = d["s"].to_list()
        states = list(dict.fromkeys(labs))
        idx = {s: i for i, s in enumerate(states)}
        n = len(states)
        C = [[0] * n for _ in range(n)]
        for a, b in zip(labs[:-1], labs[1:]):
            C[idx[a]][idx[b]] += 1
        P = [[(C[i][j] / (sum(C[i]) or 1)) for j in range(n)] for i in range(n)]
        items.append({"ticker": sid, "states": states, "matrix": P, "stationary": _stationary_from_matrix(P)})
    return {"items": items, "meta": meta_with_warnings()}

# Drivers/Data Browser endpoints
DATA_DIR = "data"
IND_DIR = os.path.join(DATA_DIR, "indicators")
VINT_DIR = os.path.join(DATA_DIR, "vintages", "fred")

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


@app.get("/drivers/pillar-series")
def drivers_pillar_series(pillar: str, years: int | None = 20):
    """Return time series of a pillar's z-score for deep dives.
    If years is provided, tail-limit to approximately that many years (monthly data assumed).
    """
    warnings: List[str] = []
    fp = os.path.join(DATA_DIR, "pillars", "pillars.parquet")
    if not os.path.exists(fp):
        warnings.append("Missing pillars parquet; run build_pillars.")
        return {"pillar": pillar, "points": [], "meta": meta_with_warnings(warnings=warnings)}
    try:
        df = pl.read_parquet(fp).select(["date", "pillar", "z"]).drop_nulls().sort("date")
        df = df.filter(pl.col("pillar") == pillar)
        if df.height == 0:
            return {"pillar": pillar, "points": [], "meta": meta_with_warnings(warnings=[f"No rows for pillar {pillar}"])}
        if years and years > 0:
            # approximate by last N years worth of monthly observations
            n = df.height
            months = years * 12
            if n > months:
                df = df.tail(months)
        pts = [{"date": str(d), "z": (float(v) if v is not None else None)} for d, v in zip(df["date"], df["z"]) ]
        return {"pillar": pillar, "points": pts, "meta": meta_with_warnings()}
    except Exception as e:
        warnings.append(f"pillar-series error: {type(e).__name__}")
        return {"pillar": pillar, "points": [], "meta": meta_with_warnings(warnings=warnings)}


@app.get("/drivers/indicator-heatmap")
def indicator_heatmap(
    pillar: str = Query(..., description="pillar id, e.g. growth"),
    last_months: int = 120,
):
    base = os.path.join(DATA_DIR, "indicators")
    if not os.path.isdir(base):
        return {"dates": [], "series": [], "matrix": [], "meta": meta_with_warnings(warnings=["No indicators found"]) }
    mats = []
    labels_map: dict[str, str] = {}
    for fn in os.listdir(base):
        if fn.endswith(".parquet") and not fn.startswith("_"):
            df = pl.read_parquet(os.path.join(base, fn))
            if df.height == 0:
                continue
            # Filter pillar
            if pillar not in df["pillar"].unique().to_list():
                continue
            # Downsample/match to monthly grid so mixed frequencies align in the heatmap
            dsel = df.select(["date", "z", "series_id"]).drop_nulls()
            dmon = (
                dsel.group_by_dynamic("date", every="1mo", period="1mo", closed="left", label="right")
                .agg([
                    pl.col("z").last().alias("z"),
                    pl.col("series_id").first().alias("series_id"),
                ])
                .drop_nulls(subset=["date"])  # ensure monthly date present
            )
            mats.append(dmon)
            try:
                sid = df.get_column("series_id").head(1).to_list()[0]
                lab = df.get_column("label").head(1).to_list()[0] if "label" in df.columns else sid
                labels_map[str(sid)] = str(lab)
            except Exception:
                pass
    if not mats:
        return {"dates": [], "series": [], "matrix": [], "meta": meta_with_warnings(warnings=[f"No indicators for pillar {pillar}"]) }
    allz = pl.concat(mats).sort(["series_id", "date"])  # long: date, z, series_id
    wide = allz.pivot(values="z", index="date", columns="series_id").sort("date")
    if last_months and wide.height > last_months:
        wide = wide.tail(last_months)
    dates = [str(d) for d in wide["date"].to_list()]
    series_cols = [c for c in wide.columns if c != "date"]
    # Plotly heatmap expects z rows to align with y (series), columns with x (dates)
    matrix = []
    for sid in series_cols:
        col_vals = wide[sid].to_list()
        matrix.append(col_vals)
    series_labels = [labels_map.get(s, s) for s in series_cols]
    return {
        "dates": dates,
        "series": series_cols,
        "series_labels": series_labels,
        "matrix": matrix,
        "meta": meta_with_warnings(),
    }


@app.get("/drivers/contributions")
def contributions(horizon_months: int = 1, method: str = "weighted"):
    """Compute contributions as weight × Δpillar over the last N months.
    Falls back to equal weights if composite config is missing.
    """
    warnings: List[str] = []
    # Load weights map
    weights: dict[str, float] = {}
    try:
        with open(os.path.join("config", "composite.yaml"), "r") as f:
            cfg = yaml.safe_load(f) or {}
        weights = (cfg.get("pillar_weights") or {})
        # normalize keys to str
        weights = {str(k): float(v) for k, v in weights.items()}
    except Exception:
        weights = {}
    # Load pillars parquet
    pfp = os.path.join(DATA_DIR, "pillars", "pillars.parquet")
    if not os.path.exists(pfp):
        warnings.append("Missing pillars parquet; run build_pillars.")
        return {"items": [], "meta": meta_with_warnings(warnings=warnings)}
    df = pl.read_parquet(pfp).select(["date", "pillar", "z"]).drop_nulls().sort(["pillar", "date"])  # monthly
    if df.is_empty():
        return {"items": [], "meta": meta_with_warnings(warnings=["No pillar rows found"]) }
    k = max(1, int(horizon_months))
    # Row-based lag approximates monthly horizon
    df = df.with_columns(
        pl.col("z").shift(k).over("pillar").alias("z_prev")
    ).with_columns(
        (pl.col("z") - pl.col("z_prev")).alias("delta")
    )
    # pick latest per pillar
    latest = (
        df.group_by("pillar").agg(pl.col("date").max().alias("date"))
        .join(df, on=["pillar", "date"], how="left")
        .drop_nulls(subset=["z"])  # ensure current z exists
    )
    # Equal weight fallback
    if not weights:
        uniq = latest.select("pillar").unique()["pillar"].to_list()
        if uniq:
            w = 1.0 / len(uniq)
            weights = {str(p): w for p in uniq}
    # Build output
    items: list[dict] = []
    for r in latest.to_dicts():
        p = str(r["pillar"]) if r.get("pillar") is not None else None
        if not p:
            continue
        w = float(weights.get(p, 0.0))
        d = float(r.get("delta") or 0.0)
        items.append({
            "pillar": p,
            "weight": w,
            "delta": d,
            "contribution": w * d,
        })
    items = sorted(items, key=lambda x: abs(x.get("contribution") or 0.0), reverse=True)
    return {"items": items, "meta": meta_with_warnings(extra={"horizon_months": k, "method": method})}

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


RISK_DIR = os.path.join("data", "risk")

@app.get("/risk/recession")
def risk_recession(asof: str | None = None, horizon_m: int = 12):
    """Return recession probability using probit on 10y-3m spread (vintage-clean as-of)."""
    coef_fp = os.path.join(RISK_DIR, f"probit_{horizon_m}m_coef.npy")
    if not os.path.exists(coef_fp):
        return {"asof": asof, "p": None, "meta": meta_with_warnings(warnings=["model not fit"]) }
    try:
        from orchestration.models.recession_probit import predict_asof
        # if no asof provided, attempt using today's date (best-effort)
        if not asof:
            asof = datetime.utcnow().date().isoformat()
        res = predict_asof.fn(asof, horizon_m)
        return {**res, "meta": meta_with_warnings()}
    except Exception as e:
        return {"asof": asof, "p": None, "meta": meta_with_warnings(warnings=[f"risk error: {type(e).__name__}"]) }


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
def turning_points_track(name: str = "business", method: str = "weighted"):
    base = os.path.join("data", "regimes")
    fp = os.path.join(base, f"{name}_{method}_hmm.parquet") if name == "business" else os.path.join(base, f"{name}.parquet")
    if not os.path.exists(fp):
        # fallback to legacy if exists
        alt = os.path.join(base, f"{name}.parquet")
        if not os.path.exists(alt):
            return {"labels": []}
        fp = alt
    try:
        df = pl.read_parquet(fp)
        if df.height == 0:
            return {"labels": []}
        rows = df.select(["date", "label"]).drop_nulls().sort("date").to_dicts()
        return {"labels": [{"date": str(r["date"]), "label": r["label"]} for r in rows], "meta": meta_with_warnings(extra={"name": name, "method": method})}
    except Exception:
        return {"labels": []}

@app.get("/turning-points/spans")
def turning_points_spans(name: str = "business"):
    base = os.path.join("data", "regimes")
    fp = os.path.join(base, f"{name}.parquet")
    if not os.path.exists(fp):
        return {"spans": []}
    try:
        df = pl.read_parquet(fp).select(["date", "label"]).drop_nulls().sort("date")
        rows = df.to_dicts()
        if not rows:
            return {"spans": []}
        spans = []
        start = rows[0]["date"]
        curr = rows[0]["label"]
        for i in range(1, len(rows) + 1):
            l = rows[i]["label"] if i < len(rows) else None
            if l != curr:
                end = rows[i - 1]["date"]
                try:
                    length = int((end - start).days)  # type: ignore
                except Exception:
                    length = None
                spans.append({"label": curr, "start": str(start), "end": str(end), "length_days": length})
                if i < len(rows):
                    start = rows[i]["date"]
                    curr = rows[i]["label"]
        return {"spans": spans, "meta": meta_with_warnings()}
    except Exception:
        return {"spans": []}

@app.get("/meta/dq")
def meta_dq():
    dq_dir = os.path.join(DATA_DIR, "_dq")
    raw_docs: list[dict] = []
    warnings: List[str] = []
    if not os.path.isdir(dq_dir):
        warnings.append("DQ directory not found; run build_indicators for diagnostics")
        return {"items": [], "meta": meta_with_warnings(warnings=warnings)}
    for fn in os.listdir(dq_dir):
        if fn.endswith(".json"):
            try:
                with open(os.path.join(dq_dir, fn), "r") as f:
                    raw_docs.append(json.load(f))
            except Exception:
                continue
    # flatten any {items:[...]} docs into a single flat list
    flat_items: list[dict] = []
    for doc in raw_docs:
        if isinstance(doc, dict) and isinstance(doc.get("items"), list):
            for it in doc["items"]:
                if isinstance(it, dict):
                    flat_items.append(it)
        elif isinstance(doc, dict):
            flat_items.append(doc)
    # surface warnings for stale (>45 days) or very low rows; be robust to missing values
    for it in flat_items:
        ident = it.get("series_id") or os.path.basename(it.get("path", "?"))
        # stale
        try:
            stale = int(it.get("staleness_days")) if it.get("staleness_days") is not None else None
        except Exception:
            stale = None
        if stale is not None and stale > 45:
            warnings.append(f"{ident} stale {stale}d")
        # few rows
        rows_val = it.get("rows")
        try:
            rows_n = int(rows_val) if rows_val is not None else None
        except Exception:
            rows_n = None
        if rows_n is not None and rows_n < 10:
            warnings.append(f"{ident} few rows: {rows_n}")
    return {"items": flat_items, "meta": meta_with_warnings(warnings=list(set(warnings)))}

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
                source = df.get_column("source").head(1).to_list()[0] if "source" in df.columns else None
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
                    "source": source,
                    "min_date": dmin,
                    "max_date": dmax,
                    "count": count,
                    "path": fp,
                })
            except Exception:
                continue
    return {"items": items, "meta": meta_with_warnings()}


@app.get("/data/series")
def data_series(
    series_id: str,
    field: str = "z",
    start: str | None = None,
    end: str | None = None,
    downsample: int = 1500,
    asof: str | None = None,
    vintage: str | None = None,  # backward-compat alias for asof
    include_z: bool = True,      # backward-compat control
):
    """Return a series either from indicators (field=z/value) or as-of vintages.
    If 'asof' is provided and vintages exist under data/vintages/fred/{series_id}.parquet,
    serve the as-published values (value only). Otherwise serve indicator field.
    """
    # Vintage path
    if (vintage and not asof):
        asof = vintage
    vint_fp = os.path.join(VINT_DIR, f"{series_id}.parquet")
    if asof and os.path.exists(vint_fp):
        df = pl.read_parquet(vint_fp)
        df = asof_snapshot(df, asof)
        if start:
            df = df.filter(pl.col("date") >= pl.lit(start))
        if end:
            df = df.filter(pl.col("date") <= pl.lit(end))
        n = df.height
        if n > downsample and downsample > 0:
            stride = max(1, n // downsample)
            df = df.with_row_count().filter(pl.col("row_nr") % stride == 0).drop("row_nr")
        return {
            "series": [{"date": str(d), "value": float(v)} for d, v in zip(df["date"], df["value"])],
            "meta": meta_with_warnings(extra={"series_id": series_id, "mode": "vintage", "asof": asof, "points": df.height}),
        }
    # Indicator path
    fp = os.path.join(IND_DIR, f"{series_id}.parquet")
    if not os.path.exists(fp):
        return {"series": [], "meta": meta_with_warnings(warnings=[f"No indicators parquet for {series_id}"])}
    base = pl.read_parquet(fp)
    # honor previous include_z flag if field isn't explicitly set
    fallback_field = "z" if include_z and ("z" in base.columns) else ("value" if "value" in base.columns else "z")
    use_field = field if field in base.columns else fallback_field
    df = base.select(["date", use_field]).rename({use_field: "value"}).drop_nulls().sort("date")
    if start:
        df = df.filter(pl.col("date") >= pl.lit(start))
    if end:
        df = df.filter(pl.col("date") <= pl.lit(end))
    n = df.height
    if n > downsample and downsample > 0:
        stride = max(1, n // downsample)
        df = df.with_row_count().filter(pl.col("row_nr") % stride == 0).drop("row_nr")
    # attach basic metadata
    label = base.get_column("label").to_list()[0] if "label" in base.columns and base.height else series_id
    pillar = base.get_column("pillar").to_list()[0] if "pillar" in base.columns and base.height else None
    return {
        "series": [{"date": str(d), "value": float(v)} for d, v in zip(df["date"], df["value"])],
        "meta": meta_with_warnings(extra={"series_id": series_id, "label": label, "pillar": pillar, "field": use_field, "points": df.height}),
    }


@app.get("/drivers/pillar-indicator-contrib")
def pillar_indicator_contrib(pillar: str, window: int = 1, top_k: int = 20):
    """Per-indicator Δz for a given pillar over the past N months.
    Scans `data/indicators/*.parquet`, filters to pillar, computes latest Δz = z - z.shift(window).
    Returns top_k by absolute Δz.
    """
    base = os.path.join(DATA_DIR, "indicators")
    if not os.path.isdir(base):
        return {"items": [], "meta": meta_with_warnings(warnings=["No indicators directory"]) }
    k = max(1, int(window))
    rows: list[dict] = []
    for fn in os.listdir(base):
        if not fn.endswith(".parquet") or fn.startswith("_"):
            continue
        fp = os.path.join(base, fn)
        try:
            df = pl.read_parquet(fp)
            if df.is_empty() or "pillar" not in df.columns:
                continue
            if pillar not in (df.get_column("pillar").unique().to_list()):
                continue
            cols = [c for c in ["date", "series_id", "label", "pillar", "z"] if c in df.columns]
            d = df.select(cols).drop_nulls(subset=["date"]).sort("date")
            if "z" not in d.columns:
                continue
            d = d.with_columns(pl.col("z").shift(k).alias("z_prev")).with_columns((pl.col("z") - pl.col("z_prev")).alias("delta"))
            # latest row only
            last = d.tail(1).to_dicts()[0] if d.height else None
            if last:
                rows.append({
                    "series_id": last.get("series_id") or fn.replace(".parquet", ""),
                    "label": last.get("label") or last.get("series_id"),
                    "pillar": pillar,
                    "delta": float(last.get("delta") or 0.0),
                    "z_after": float(last.get("z") or 0.0),
                })
        except Exception:
            continue
    rows = sorted(rows, key=lambda x: abs(x.get("delta") or 0.0), reverse=True)
    if top_k and len(rows) > top_k:
        rows = rows[:top_k]
    return {"items": rows, "meta": meta_with_warnings(extra={"pillar": pillar, "window": k})}


# Markets summary / assets endpoints
@app.get("/market/summary", response_model=MarketSummaryResponse)
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


@app.get("/market/price-series")
def market_price_series(ticker: str, start: str | None = None, end: str | None = None, downsample: int = 1500):
    sid = ticker.strip()
    path = f"data/raw/market/{sid}.parquet"
    if not os.path.exists(path):
        return {"series": [], "meta": {"ticker": sid}}
    df = pl.read_parquet(path).select(["date", "value"]).drop_nulls().sort("date")
    if start:
        df = df.filter(pl.col("date") >= pl.lit(start))
    if end:
        df = df.filter(pl.col("date") <= pl.lit(end))
    n = df.height
    if n > downsample and downsample > 0:
        stride = max(1, n // downsample)
        df = df.with_row_count().filter(pl.col("row_nr") % stride == 0).drop("row_nr")
    series = [{"date": str(d), "value": float(v)} for d, v in zip(df["date"].to_list(), df["value"].to_list())]
    return {"series": series, "meta": {"ticker": sid, "points": df.height}}


@app.get("/backtest/spx_v0")
def backtest_spx_v0():
    fp = os.path.join("data", "backtests", "spx_signal_v0.parquet")
    mp = os.path.join("data", "backtests", "spx_signal_v0_metrics.parquet")
    if not os.path.exists(fp):
        return {"series": [], "metrics": {}, "meta": meta_with_warnings(warnings=["No backtest artifact found"]) }
    df = pl.read_parquet(fp).sort("date")
    ser = [
      {"date": str(d), "ret": (float(r) if r is not None else 0.0), "ret_sig": (float(s) if s is not None else 0.0)}
      for d, r, s in zip(df["date"], df.get_column("ret"), df.get_column("ret_sig"))
    ]
    metrics = pl.read_parquet(mp).to_dicts()[0] if os.path.exists(mp) else {}
    return {"series": ser, "metrics": metrics, "meta": meta_with_warnings()}


@app.get("/backtest/composite-threshold")
def backtest_composite_threshold(ticker: str = "SPX", method: str = "weighted", cost_bps: float = 10.0):
    mkt_fp = os.path.join("data", "raw", "market", f"{ticker}.parquet")
    if not os.path.exists(mkt_fp):
        return {"series": [], "metrics": {}, "meta": meta_with_warnings(warnings=["no market parquet"]) }
    px = pl.read_parquet(mkt_fp).select(["date", "value"]).drop_nulls().sort("date")
    # Monthly end-of-month sampling without duplicating the 'date' column
    mpx = (
        px.group_by_dynamic("date", every="1mo", period="1mo", closed="left", label="right")
          .agg(pl.col("value").last().alias("close"))
          .drop_nulls(subset=["date"])  # ensure date present
          .sort("date")
    )
    mpx = mpx.with_columns([
        pl.col("date").cast(pl.Date),  # align join key dtype to composite_asof (Date)
        pl.col("close").pct_change().alias("ret"),
    ])
    warnings: list[str] = []
    comp_fp = os.path.join("data", "composite_asof", f"composite_{method}.parquet")
    comp: pl.DataFrame | None = None
    if os.path.exists(comp_fp):
        try:
            comp = pl.read_parquet(comp_fp).drop_nulls(subset=["date"]).with_columns(pl.col("date").cast(pl.Date)).sort("date")
        except Exception:
            comp = None
    if comp is None or comp.is_empty() or ("z" in comp.columns and comp["z"].drop_nulls().is_empty()):
        # Fallback to live composite if as-of artifact is missing/empty
        live_fp = os.path.join("data", "composite", "composite.parquet")
        if os.path.exists(live_fp):
            df = pl.read_parquet(live_fp).drop_nulls(subset=["date"]).with_columns(pl.col("date").cast(pl.Date)).sort("date")
            col = "z"
            m = (method or "weighted").lower()
            if m == "median" and "z_median" in df.columns:
                col = "z_median"
            elif m in ("trimmed", "trimmed_mean") and "z_trimmed_mean" in df.columns:
                col = "z_trimmed_mean"
            elif m == "diffusion" and "diffusion_composite" in df.columns:
                col = "diffusion_composite"
            comp = df.select(["date", col]).rename({col: "z"}).drop_nulls(subset=["z"]).sort("date")
            warnings.append("fallback to live composite; as-of artifact missing or empty")
        else:
            return {"series": [], "metrics": {}, "meta": meta_with_warnings(warnings=["no composite artifacts found"]) }
    # Debug meta for overlap diagnostics
    price_min = str(mpx["date"].min()) if mpx.height else None
    price_max = str(mpx["date"].max()) if mpx.height else None
    comp_min = str(comp["date"].min()) if comp.height else None
    comp_max = str(comp["date"].max()) if comp.height else None
    df = mpx.join(comp, on="date", how="inner").sort("date")
    df = df.with_columns([(pl.col("z") > 0).cast(pl.Int8).alias("sig")])
    df = df.with_columns(pl.col("sig").shift(1).fill_null(0).alias("sig_lag"))
    df = df.with_columns((pl.col("sig_lag") != pl.col("sig_lag").shift(1)).cast(pl.Int8).fill_null(0).alias("_chg"))
    tc = cost_bps / 10000.0
    df = df.with_columns((pl.col("ret").fill_nan(0) * pl.col("sig_lag") - pl.col("_chg") * tc).alias("ret_sig"))
    import numpy as np, math
    retn = df["ret"].fill_nan(0).fill_null(0).to_numpy()
    rets = df["ret_sig"].fill_nan(0).fill_null(0).to_numpy()
    eq_bh = np.cumprod(1 + retn) if retn.size else np.array([1.0])
    eq_sig = np.cumprod(1 + rets) if rets.size else np.array([1.0])
    def sharpe(x):
        x = np.array(x)
        if x.size == 0:
            return 0.0
        m = float(np.mean(x))
        s = float(np.std(x))
        return float(m / (s + 1e-12) * np.sqrt(12))
    def maxdd(x):
        x = np.array(x)
        peak = np.maximum.accumulate(x)
        dd = (x - peak) / peak
        return float(np.min(dd))
    n_months = int(df.height)
    # turnover: fraction of months with a signal change
    try:
        chg_count = int(df["_chg"].fill_null(0).sum()) if n_months > 0 else 0
    except Exception:
        chg_count = 0
    turnover_m = (chg_count / n_months) if n_months > 0 else 0.0
    met = {
        "n_months": n_months,
        "bh_cagr": float(eq_bh[-1] ** (12 / n_months) - 1) if n_months > 0 else 0.0,
        "strat_cagr": float(eq_sig[-1] ** (12 / n_months) - 1) if n_months > 0 else 0.0,
        "bh_sharpe": sharpe(df["ret"].fill_null(0)),
        "strat_sharpe": sharpe(df["ret_sig"].fill_null(0)),
        "strat_maxdd": maxdd(eq_sig) if n_months > 0 else 0.0,
        "turnover_m": float(turnover_m),
    }
    def sfloat(v: float | None) -> float:
        try:
            if v is None:
                return 0.0
            fv = float(v)
            if math.isnan(fv) or math.isinf(fv):
                return 0.0
            return fv
        except Exception:
            return 0.0
    ser = [
        {"date": str(d), "ret": sfloat(r), "ret_sig": sfloat(s)}
        for d, r, s in zip(df["date"], df["ret"], df["ret_sig"])
    ]
    return {
        "series": ser,
        "metrics": met,
        "meta": meta_with_warnings(
            extra={
                "ticker": ticker,
                "method": method,
                "cost_bps": cost_bps,
                "n_price": int(mpx.height),
                "n_comp": int(comp.height),
                "n_join": int(df.height),
                "price_min": price_min,
                "price_max": price_max,
                "comp_min": comp_min,
                "comp_max": comp_max,
            },
            warnings=warnings,
        ),
    }
