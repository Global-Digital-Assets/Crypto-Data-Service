#!/usr/bin/env python3
"""Order-book imbalance streamer for high/ultra tokens.
Writes depth-20 bid / ask volume snapshot every 30 s to orderbook.db (table ob_imbalance).
This is a lightweight approximation (REST snapshots) – sufficient for ML feature.
"""
import asyncio
import csv
import logging
import os
import time
from typing import List

from binance import AsyncClient

import aiosqlite

DB_PATH = os.getenv("ORDERBOOK_DB", "/root/data-service/orderbook.db")
BUCKET_CSV = os.getenv("BUCKET_MAP", "/root/analytics-tool-v2/bucket_mapping.csv")
INTERVAL_SECONDS = int(os.getenv("OB_POLL_SECS", "30"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("ob-streamer")


def load_symbols() -> List[str]:
    try:
        with open(BUCKET_CSV, newline="") as fh:
            return sorted({r["symbol"].upper() for r in csv.DictReader(fh) if r["bucket"] in {"high", "ultra"}})
    except Exception as exc:
        log.error("bucket file error: %s", exc)
        return []


async def ensure_schema():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """CREATE TABLE IF NOT EXISTS ob_imbalance (
                symbol TEXT,
                ts     INTEGER,
                bidVol REAL,
                askVol REAL,
                PRIMARY KEY(symbol, ts)
            );"""
        )
        await db.commit()


async def store(symbol: str, ts: int, bid_vol: float, ask_vol: float):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT OR REPLACE INTO ob_imbalance (symbol, ts, bidVol, askVol) VALUES (?,?,?,?)",
            (symbol, ts, bid_vol, ask_vol),
        )
        await db.commit()


async def process_symbol(client: AsyncClient, symbol: str):
    try:
        depth = await client.get_order_book(symbol=symbol, limit=20)
        bid_vol = sum(float(b[1]) for b in depth["bids"])
        ask_vol = sum(float(a[1]) for a in depth["asks"])
        await store(symbol, int(time.time() * 1000), bid_vol, ask_vol)
        log.info("%s ob snapshot bid=%.2f ask=%.2f", symbol, bid_vol, ask_vol)
    except Exception as e:
        log.warning("%s depth fetch error: %s", symbol, e)


async def main():
    symbols = load_symbols()
    if not symbols:
        log.error("No symbols; exiting")
        return
    await ensure_schema()

    client = await AsyncClient.create()
    try:
        while True:
            tasks = [process_symbol(client, s) for s in symbols]
            await asyncio.gather(*tasks)
            await asyncio.sleep(INTERVAL_SECONDS)
    finally:
        await client.close_connection()


if __name__ == "__main__":
    asyncio.run(main())
