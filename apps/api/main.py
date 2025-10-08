from fastapi import FastAPI
from datetime import datetime, timedelta
import duckdb, os, json

app = FastAPI(title="Macro Cycles Local API", version="0.1.0")
DB_PATH = os.getenv("DUCKDB_PATH", "db/catalog.duckdb")

def connect():
    os.makedirs("db", exist_ok=True)
    return duckdb.connect(DB_PATH)

@app.get("/health")
def health():
    return {"status": "ok", "ts": datetime.utcnow().isoformat()}

@app.get("/overview/composite")
def composite(window_years: int = 40):
    con = connect()
    con.execute("""
        CREATE TABLE IF NOT EXISTS composite (
            date DATE,
            score DOUBLE,
            z DOUBLE,
            regime VARCHAR,
            confidence DOUBLE
        );
    """)
    # Fallback: populate a tiny demo series if empty
    cnt = con.sql("SELECT count(*) FROM composite").fetchone()[0]
    if cnt == 0:
        base = datetime(1985,1,1)
        rows = []
        for i in range(0, 365*40, 7):
            d = base + timedelta(days=i)
            z = 2.0 * __import__("math").sin(i/180.0)
            rows.append((d.date(), z, z, "Expansion" if z>0 else "Contraction", abs(z)/2))
        con.execute("INSERT INTO composite VALUES (?, ?, ?, ?, ?)", rows)
    res = con.sql("""
        SELECT * FROM composite
        WHERE date >= DATE '1970-01-01'
        ORDER BY date
    """).fetchall()
    series = [{"date": str(r[0]), "score": float(r[1]), "z": float(r[2]),
               "regime": r[3], "confidence": float(r[4])} for r in res]
    meta = {"data_vintage": datetime.utcnow().isoformat(), "config_version": "v0", "git_hash": "local"}
    return {"series": series, "meta": meta}

@app.get("/overview/pillars")
def pillars():
    meta = {"data_vintage": datetime.utcnow().isoformat(), "config_version": "v0", "git_hash": "local"}
    # demo snapshot of 3 pillars; Prefect will replace
    pillars = [
        {"pillar":"growth","z":0.8,"momentum_3m":0.2,"diffusion":0.62,"components":[]},
        {"pillar":"inflation","z":-0.4,"momentum_3m":-0.1,"diffusion":0.45,"components":[]},
        {"pillar":"liquidity","z":1.1,"momentum_3m":0.3,"diffusion":0.70,"components":[]},
    ]
    return {"pillars": pillars, "meta": meta}

@app.get("/overview/movers")
def movers(window_days: int = 7, top_k: int = 10):
    meta = {"data_vintage": datetime.utcnow().isoformat(), "config_version": "v0", "git_hash": "local"}
    gainers = [{"id":"ISMNO","label":"ISM New Orders","pillar":"growth","delta":0.35,"z_after":0.8,"source":"FRED"}]
    losers  = [{"id":"CPILFESL","label":"Core CPI 3m ann","pillar":"inflation","delta":-0.22,"z_after":-0.4,"source":"FRED"}]
    return {"gainers": gainers, "losers": losers, "meta": meta}

@app.get("/market/regimes")
def market_regimes(tickers: str = "SPX,UST2Y,UST10Y,TWEXB,GOLD,BTC"):
    meta = {"data_vintage": datetime.utcnow().isoformat(), "config_version": "v0", "git_hash": "local"}
    assets = []
    for t in tickers.split(","):
        assets.append({
            "ticker": t,
            "label": "Trend-Up / Low-Vol / Easy-Liq",
            "state": "trend_up",
            "probas": [{"state":"trend_up","p":0.7},{"state":"range","p":0.2},{"state":"trend_down","p":0.1}],
            "sparkline": [100,101,102,101,103,104,105],
            "last_updated": datetime.utcnow().isoformat()
        })
    return {"assets": assets, "meta": meta}

@app.get("/meta/changelog")
def changelog():
    return {"entries":[{"ts":datetime.utcnow().isoformat(),"kind":"source","version":"v0","message":"initial demo"}]}
