"""SQLite persistence.

Writes are queued and flushed on a background task so a slow disk can
never stall the collector. History is what makes the live numbers
worth anything later — you cannot measure whether book imbalance
predicted the next move without having kept it.
"""

from __future__ import annotations

import asyncio
import logging
import sqlite3
import time

log = logging.getLogger("msl.store")

SCHEMA = """
CREATE TABLE IF NOT EXISTS book_samples (
    ts REAL, symbol TEXT, mid REAL,
    imb_5 REAL, imb_10 REAL, imb_25 REAL,
    spread_bps REAL, cvd REAL, oi REAL, funding REAL
);
CREATE INDEX IF NOT EXISTS ix_book_ts ON book_samples(symbol, ts);

CREATE TABLE IF NOT EXISTS liquidations (
    ts REAL, symbol TEXT, price REAL, qty REAL, usd REAL, side TEXT
);
CREATE INDEX IF NOT EXISTS ix_liq_ts ON liquidations(symbol, ts);

CREATE TABLE IF NOT EXISTS large_trades (
    ts REAL, symbol TEXT, price REAL, usd REAL, side TEXT
);
CREATE INDEX IF NOT EXISTS ix_lt_ts ON large_trades(symbol, ts);
"""


class Store:
    def __init__(self, path: str, enabled: bool = True) -> None:
        self.enabled = enabled
        self.queue: asyncio.Queue = asyncio.Queue(maxsize=10_000)
        self.conn: sqlite3.Connection | None = None
        if enabled:
            self.conn = sqlite3.connect(path, check_same_thread=False)
            self.conn.execute("PRAGMA journal_mode=WAL")
            self.conn.executescript(SCHEMA)
            self.conn.commit()

    def put(self, table: str, row: tuple) -> None:
        if not self.enabled:
            return
        try:
            self.queue.put_nowait((table, row))
        except asyncio.QueueFull:
            log.warning("store queue full — dropping a row")

    async def run(self) -> None:
        if not self.enabled:
            return
        while True:
            await asyncio.sleep(2.0)
            batch: dict[str, list[tuple]] = {}
            while not self.queue.empty():
                table, row = self.queue.get_nowait()
                batch.setdefault(table, []).append(row)
            if not batch:
                continue
            try:
                for table, rows in batch.items():
                    placeholders = ",".join("?" * len(rows[0]))
                    self.conn.executemany(
                        f"INSERT INTO {table} VALUES ({placeholders})", rows)
                self.conn.commit()
            except Exception as exc:
                log.error("store flush failed: %s", exc)

    def sample_state(self, st) -> None:
        if not self.enabled or not st.book.ready:
            return
        imb = st.imbalances()
        keys = list(imb.keys())
        self.put("book_samples", (
            time.time(), st.symbol, st.book.mid,
            imb.get(keys[0]) if len(keys) > 0 else None,
            imb.get(keys[1]) if len(keys) > 1 else None,
            imb.get(keys[2]) if len(keys) > 2 else None,
            None, st.cvd, st.open_interest, st.funding,
        ))
