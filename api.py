"""FastAPI Data Service exposing candle endpoints and health check (fixed timestamp conversion)."""
from fastapi import FastAPI, Request, HTTPException
from datetime import datetime, timezone
import sqlite3, os, logging
from typing import List, Dict

DB_PATH = os.getenv("DATA_DB_PATH", "/root/analytics-tool-v2/market_data.db")
API_KEY = os.getenv("DATA_API_KEY", "")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("data-service")

app = FastAPI(title="Data Service", version="0.1.2")

# latest observed candle timestamp (UTC)
latest_ts = datetime.now(tz=timezone.utc)

@app.middleware("http")
async def _auth(request: Request, call_next):
    if API_KEY and API_KEY != "" and request.headers.get("x-api-key") != API_KEY:
        raise HTTPException(status_code=401, detail="unauthorized")
    return await call_next(request)

def _query_db(sql: str, params: tuple = ()) -> List[tuple]:
    conn = sqlite3.connect(DB_PATH, timeout=30, check_same_thread=False)
    cur = conn.execute(sql, params)
    rows = cur.fetchall()
    conn.close()
    return rows


def _to_dt(ts_int: int) -> datetime:
    """Convert seconds or milliseconds int timestamp to datetime (UTC)."""
    try:
        if ts_int > 1e12:  # assume ms
            ts_int = ts_int / 1000
        return datetime.fromtimestamp(ts_int, tz=timezone.utc)
    except Exception:
        return datetime.now(tz=timezone.utc)


@app.get("/candles/{symbol}/{tf}")
def get_candles(symbol: str, tf: str = "15m", limit: int = 200) -> Dict:
    symbol = symbol.upper()
    rows = _query_db(
        "SELECT timestamp, open, high, low, close, volume FROM candles WHERE symbol=? ORDER BY timestamp DESC LIMIT ?",
        (symbol, limit),
    )
    candles = [dict(zip(["ts", "open", "high", "low", "close", "vol"], r)) for r in rows]

    if candles:
        global latest_ts
        latest_ts = _to_dt(candles[0]["ts"])

    return {
        "symbol": symbol,
        "tf": tf,
        "count": len(candles),
        "candles": list(reversed(candles)),
        "latest_time": candles[0]["ts"] if candles else None,
    }


@app.get("/health")
def health():
    age = (datetime.now(tz=timezone.utc) - latest_ts).total_seconds()
    return {"ok": age < 120, "age_sec": age}
