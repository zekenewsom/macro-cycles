from fastapi import FastAPI, Query
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
def composite(window_years: int = 40, method: str = "weighted"):
    warnings: List[str] = []
    fp = os.path.join("data", "composite", "composite.parquet")
    if not os.path.exists(fp):
        warnings.append("Composite parquet not found under data/composite; run build_pipeline.")
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
def movers(window_days: int = 7, top_k: int = 10, pillar: str | None = None):
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
            WHERE date IS NOT NULL AND z IS NOT NULL AND series_id IS NOT NULL
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
    # surface warnings for stale (>45 days) or low rows
    for it in flat_items:
        ident = it.get("series_id") or os.path.basename(it.get("path", "?"))
        if (it.get("staleness_days") or 0) > 45:
            warnings.append(f"{ident} stale {it.get('staleness_days')}d")
        if (it.get("rows") or 0) < 10:
            warnings.append(f"{ident} few rows: {it.get('rows')}")
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
