#!/usr/bin/env python3
"""Fetch daily macro indices (DXY, VIX) and store in macro.db."""
import datetime as dt
import logging
import os
import time
from typing import Dict

import aiosqlite
import pandas as pd
import yfinance as yf

DB_PATH = os.getenv("MACRO_DB", "/root/data-service/macro.db")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("macro-fetch")

URLS: Dict[str, str] = {
    "DXY": "https://query1.finance.yahoo.com/v7/finance/download/DXY?period1=1104537600&period2=4102444800&interval=1d&events=history&includeAdjustedClose=true",
    "VIX": "https://query1.finance.yahoo.com/v7/finance/download/%5EVIX?period1=1104537600&period2=4102444800&interval=1d&events=history&includeAdjustedClose=true",
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
        if df.empty:
            raise ValueError("empty csv")
        today_row = df.iloc[-1]
        date_str = str(today_row["Date"])
        price = float(today_row["Close"])
    except Exception as csv_err:
        log.warning("CSV fetch failed for %s: %s. Falling back to yfinance.", symbol, csv_err)
        # Map to Yahoo ticker (prefix ^ for index)
        yf_sym = {"DXY": "DX-Y.NYB", "VIX": "^VIX"}.get(symbol, symbol)
        try:
            hist = yf.Ticker(yf_sym).history(period="1d")
            today_row = hist.iloc[-1]
            date_str = today_row.name.strftime("%Y-%m-%d")
            price = float(today_row["Close"])
        except Exception as yf_err:
            log.error("yfinance fetch failed for %s: %s", symbol, yf_err)
            return
    ts = int(time.mktime(dt.datetime.strptime(date_str, "%Y-%m-%d").timetuple()) * 1000)
    await upsert(symbol, ts, price)
    log.info("%s %s %.2f", symbol, date_str, price)


async def main():
    await ensure_schema()
    for sym, url in URLS.items():
        await fetch_symbol(sym, url)


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
