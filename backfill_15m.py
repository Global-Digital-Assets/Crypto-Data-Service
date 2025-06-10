#!/usr/bin/env python3
"""Aggregate 1-minute candles into 15-minute and upsert into candles_900s table."""
import sqlite3, os, logging, time
DB = os.getenv('DATA_DB_PATH', '/root/analytics-tool-v2/market_data.db')
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger('backfill15m')

def main():
    t0 = time.time()
    conn = sqlite3.connect(DB)
    cur = conn.cursor()
    cur.executescript("""
    CREATE TABLE IF NOT EXISTS candles_900s (
        symbol TEXT,
        timestamp INTEGER,
        open REAL,
        high REAL,
        low REAL,
        close REAL,
        volume REAL,
        PRIMARY KEY(symbol, timestamp)
    );
    INSERT OR REPLACE INTO candles_900s (symbol,timestamp,open,high,low,close,volume)
    SELECT symbol,
           ts_bucket,
           MIN(open) KEEP (FIRST ORDER BY ts_in) AS open,
           MAX(high) AS high,
           MIN(low) AS low,
           MIN(close) KEEP (LAST ORDER BY ts_in) AS close,
           SUM(volume) AS volume
    FROM (
        SELECT c.symbol,
               c.timestamp AS ts_in,
               (c.timestamp/900000)*900000 AS ts_bucket,
               c.open, c.high, c.low, c.close, c.volume
        FROM candles c
    )
    GROUP BY symbol, ts_bucket;
    """)
    conn.commit()
    conn.close()
    log.info('Backfill done in %.1fs', time.time()-t0)

if __name__ == '__main__':
    main()
