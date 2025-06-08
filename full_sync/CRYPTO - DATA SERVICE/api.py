"""FastAPI Data Service exposing candle endpoints and health check (fixed timestamp conversion)."""
from fastapi import FastAPI, Request, HTTPException
from datetime import datetime, timezone
import sqlite3, os, logging
from typing import List, Dict

DB_PATH = os.getenv("DATA_DB_PATH", "/root/analytics-tool-v2/market_data.db")
ORDERBOOK_DB = os.getenv("ORDERBOOK_DB_PATH", "/root/data-service/orderbook.db")
FUTURES_DB = os.getenv("FUTURES_DB_PATH", "/root/data-service/futures_metrics.db")
MACRO_DB = os.getenv("MACRO_DB_PATH", "/root/data-service/macro.db")
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


def _query_external(db_path: str, sql: str, params: tuple = ()) -> List[tuple]:
    conn = sqlite3.connect(db_path, timeout=30, check_same_thread=False)
    cur = conn.execute(sql, params)
    rows = cur.fetchall()
    conn.close()
    return rows


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


@app.get("/orderbook/{symbol}")
def orderbook(symbol: str, limit: int = 1):
    symbol = symbol.upper()
    rows = _query_external(
        ORDERBOOK_DB,
        "SELECT ts, bidVol, askVol FROM ob_imbalance WHERE symbol=? ORDER BY ts DESC LIMIT ?",
        (symbol, limit),
    )
    data = [dict(zip(["ts", "bid", "ask"], r)) for r in rows]
    return {"symbol": symbol, "count": len(data), "orderbook": list(reversed(data))}


@app.get("/oi/{symbol}/5m")
def open_interest(symbol: str, limit: int = 10):
    symbol = symbol.upper()
    rows = _query_external(
        FUTURES_DB,
        "SELECT ts, oi FROM open_interest WHERE symbol=? ORDER BY ts DESC LIMIT ?",
        (symbol, limit),
    )
    data = [dict(zip(["ts", "oi"], r)) for r in rows]
    return {"symbol": symbol, "tf": "5m", "count": len(data), "oi": list(reversed(data))}


@app.get("/funding/{symbol}")
def funding_rate(symbol: str, limit: int = 20):
    symbol = symbol.upper()
    rows = _query_external(
        FUTURES_DB,
        "SELECT ts, rate FROM funding_rate WHERE symbol=? ORDER BY ts DESC LIMIT ?",
        (symbol, limit),
    )
    data = [dict(zip(["ts", "rate"], r)) for r in rows]
    return {"symbol": symbol, "count": len(data), "funding": list(reversed(data))}


@app.get("/macro/latest")
def macro_latest():
    rows = _query_external(
        MACRO_DB,
        "SELECT symbol, ts, value FROM daily_indices ORDER BY ts DESC",
    )
    latest: Dict[str, Dict] = {}
    for sym, ts, val in rows:
        if sym not in latest:  # first row per symbol is latest due to DESC order
            latest[sym] = {"ts": ts, "value": val}
    return latest
