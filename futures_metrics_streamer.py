#!/usr/bin/env python3
"""Futures open-interest & funding-rate streamer.
Polls Binance endpoints every 5 min and writes to futures_metrics.db.
Gracefully skips symbols without futures markets.
"""
import asyncio
import csv
import logging
import os
import time
from typing import List, Optional

import aiosqlite
from binance import AsyncClient
from binance.exceptions import BinanceAPIException

DB_PATH = os.getenv("FUTURES_DB", "/root/data-service/futures_metrics.db")
BUCKET_CSV = os.getenv("BUCKET_MAP", "/root/analytics-tool-v2/bucket_mapping.csv")
INTERVAL_SECONDS = int(os.getenv("FUT_METRICS_SECS", "300"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("futures-metrics")
FUTURES_MAP = {}  # cache spot->futures symbol (e.g., PEPEUSDT -> 1000PEPEUSDT)


def load_symbols() -> List[str]:
    try:
        with open(BUCKET_CSV, newline="") as fh:
            return sorted({r["symbol"].upper() for r in csv.DictReader(fh) if r["bucket"] in {"high", "ultra"}})
    except Exception as exc:
        log.error("bucket file read error: %s", exc)
        return []


async def ensure_schema():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "CREATE TABLE IF NOT EXISTS open_interest (symbol TEXT, ts INTEGER, oi REAL, PRIMARY KEY(symbol, ts));"
        )
        await db.execute(
            "CREATE TABLE IF NOT EXISTS funding_rate (symbol TEXT, ts INTEGER, rate REAL, PRIMARY KEY(symbol, ts));"
        )
        await db.commit()


async def upsert(table: str, symbol: str, ts: int, value: float):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(f"INSERT OR REPLACE INTO {table} (symbol, ts, { 'oi' if table=='open_interest' else 'rate' }) VALUES (?,?,?)", (symbol, ts, value))
        await db.commit()


async def fetch_for_symbol(client: AsyncClient, symbol: str):
    fut_sym = FUTURES_MAP.get(symbol, symbol)

    async def _try(symbol_to_query: str):
        oi_val = None
        rate_val = None
        ts_ms = int(time.time() * 1000)
        try:
            oi_data = await client.futures_open_interest(symbol=symbol_to_query)
            oi_val = float(oi_data["openInterest"])
            await upsert("open_interest", symbol, ts_ms, oi_val)
        except BinanceAPIException as be:
            if be.code == -1121:  # invalid symbol
                raise
            log.debug("OI error %s: %s", symbol_to_query, be)
        except Exception as e:
            log.debug("OI error %s: %s", symbol_to_query, e)

        try:
            fr = await client.futures_premium_index(symbol=symbol_to_query)
            ts_ms = int(fr["time"])
            rate_val = float(fr["lastFundingRate"])
            await upsert("funding_rate", symbol, ts_ms, rate_val)
        except BinanceAPIException as be:
            if be.code == -1121:
                raise
            log.debug("Funding error %s: %s", symbol_to_query, be)
        except Exception as e:
            log.debug("Funding error %s: %s", symbol_to_query, e)

        return (oi_val is not None) or (rate_val is not None)

    success = True
    try:
        success = await _try(fut_sym)
    except BinanceAPIException:
        success = False

    if not success:
        alt = f"1000{symbol}"
        try:
            if await _try(alt):
                FUTURES_MAP[symbol] = alt
                log.info("Mapped %s -> %s futures symbol", symbol, alt)
        except BinanceAPIException:
            log.debug("No futures contract for %s (tried %s)", symbol, alt)


async def main():
    symbols = load_symbols()
    if not symbols:
        log.error("No symbols loaded; exit.")
        return
    await ensure_schema()

    client = await AsyncClient.create()
    try:
        while True:
            tasks = [fetch_for_symbol(client, s) for s in symbols]
            await asyncio.gather(*tasks)
            await asyncio.sleep(INTERVAL_SECONDS)
    finally:
        await client.close_connection()


if __name__ == "__main__":
    asyncio.run(main())
