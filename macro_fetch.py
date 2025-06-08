#!/usr/bin/env python3
"""Fetch daily macro indices (DXY, VIX) and store in macro.db."""
import datetime as dt
import logging
import os
import time
from typing import Dict

import aiosqlite
import pandas as pd

DB_PATH = os.getenv("MACRO_DB", "/root/data-service/macro.db")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("macro-fetch")

URLS: Dict[str, str] = {
    "DXY": "https://stooq.com/q/d/l/?s=%5EDXY&i=d",
    "VIX": "https://stooq.com/q/d/l/?s=%5EVIX&i=d",
}


async def ensure_schema():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "CREATE TABLE IF NOT EXISTS daily_indices (symbol TEXT, ts INTEGER, value REAL, PRIMARY KEY(symbol, ts));"
        )
        await db.commit()


async def upsert(symbol: str, ts: int, value: float):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT OR REPLACE INTO daily_indices (symbol, ts, value) VALUES (?,?,?)",
            (symbol, ts, value),
        )
        await db.commit()


async def fetch_symbol(symbol: str, url: str):
    try:
        df = pd.read_csv(url)
        today_row = df.iloc[-1]
        date_str = today_row["Date"]
        price = float(today_row["Close"])
        ts = int(time.mktime(dt.datetime.strptime(date_str, "%Y-%m-%d").timetuple()) * 1000)
        await upsert(symbol, ts, price)
        log.info("%s %s %.2f", symbol, date_str, price)
    except Exception as e:
        log.error("macro fetch %s failed: %s", symbol, e)


async def main():
    await ensure_schema()
    for sym, url in URLS.items():
        await fetch_symbol(sym, url)


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
