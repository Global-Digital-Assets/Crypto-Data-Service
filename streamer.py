#!/usr/bin/env python3
"""Dynamic-token Binance streamer (see previous description)."""
import asyncio, csv, logging, os, sqlite3
from typing import Dict, List, Optional
from binance import AsyncClient

DB_PATH = os.getenv("DATA_DB_PATH", "/root/analytics-tool-v2/market_data.db")
BUCKET_CSV = os.getenv("BUCKET_MAP", "/root/analytics-tool-v2/bucket_mapping.csv")
AGG_INT = 900
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("streamer")

try:
    with open(BUCKET_CSV, newline="") as fh:
        TOKENS: List[str] = sorted({r["symbol"].upper() for r in csv.DictReader(fh)})
except Exception as exc:
    log.error("bucket load failed: %s", exc)
    TOKENS = ["BTCUSDT", "ETHUSDT"]

class Streamer:
    def __init__(self):
        self.db = DB_PATH
        self.invalid = set()
        self._init_db()
        self.client = None

    def _init_db(self):
        conn = sqlite3.connect(self.db)
        cur = conn.cursor()
        cur.execute("CREATE TABLE IF NOT EXISTS candles (symbol TEXT,timestamp INTEGER,open REAL,high REAL,low REAL,close REAL,volume REAL,PRIMARY KEY(symbol,timestamp))")
        cur.execute(f"CREATE TABLE IF NOT EXISTS candles_{AGG_INT}s (symbol TEXT,timestamp INTEGER,open REAL,high REAL,low REAL,close REAL,volume REAL,PRIMARY KEY(symbol,timestamp))")
        conn.commit(); conn.close()

    async def _fetch(self, sym: str) -> Optional[Dict]:
        try:
            k = (await self.client.get_klines(symbol=sym, interval='1m', limit=1))[0]
            return {"timestamp": int(k[0]), "open": float(k[1]), "high": float(k[2]), "low": float(k[3]), "close": float(k[4]), "volume": float(k[5])}
        except Exception as e:
            # Fallback: try underlying spot symbol if prefixed with 1000
            if "Invalid symbol" in str(e) and sym.startswith("1000"):
                base_sym = sym[4:]
                try:
                    k = (await self.client.get_klines(symbol=base_sym, interval='1m', limit=1))[0]
                    log.warning("%s missing on spot; using %s candles", sym, base_sym)
                    return {"timestamp": int(k[0]), "open": float(k[1]), "high": float(k[2]), "low": float(k[3]), "close": float(k[4]), "volume": float(k[5])}
                except Exception:
                    pass
            log.error("%s error %s", sym, e)
            self.invalid.add(sym)
            return None

    def _upsert(self, table: str, sym: str, c: Dict):
        conn = sqlite3.connect(self.db)
        conn.execute(f"INSERT OR REPLACE INTO {table} (symbol,timestamp,open,high,low,close,volume) VALUES (?,?,?,?,?,?,?)", (sym,c['timestamp'],c['open'],c['high'],c['low'],c['close'],c['volume']))
        conn.commit(); conn.close()

    async def run(self):
        self.client = await AsyncClient.create()
        try:
            while True:
                for s in list(TOKENS):
                    if s in self.invalid:
                        continue
                    c = await self._fetch(s)
                    if c:
                        self._upsert('candles',s,c)
                        bucket = c.copy(); bucket['timestamp']=(c['timestamp']//AGG_INT)*AGG_INT
                        self._upsert(f'candles_{AGG_INT}s',s,bucket)
                log.info("stored %d symbols", len(TOKENS))
                await asyncio.sleep(60)
        finally:
            if self.client:
                await self.client.close_connection()

if __name__=='__main__':
    log.info("Streamer for %d symbols", len(TOKENS))
    asyncio.run(Streamer().run())
