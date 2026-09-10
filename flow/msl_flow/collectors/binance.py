"""Binance USD-M futures collector.

Four public streams per symbol, no API key required:
    @depth@100ms   sequenced order book diffs
    @aggTrade      aggressor-tagged prints, the source of CVD
    @forceOrder    liquidations
    @markPrice@1s  mark price and the live funding rate

Open interest is not streamed, so it is polled on a slow timer.

The collector's contract with the rest of the service: a book is either
provably in sync or explicitly not ready. It never serves a book it
cannot vouch for.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time

import aiohttp
import websockets

from ..book import DesyncError
from ..metrics import Liquidation, Trade

log = logging.getLogger("msl.binance")

WS_BASE = "wss://fstream.binance.com/stream?streams="
REST_BASE = "https://fapi.binance.com"


class BinanceCollector:
    def __init__(self, states: dict, cfg: dict) -> None:
        self.states = states
        self.cfg = cfg
        self.symbols = [s.upper() for s in cfg["symbols"]]
        self.pending: dict[str, list[dict]] = {s: [] for s in self.symbols}
        self.syncing: dict[str, bool] = {s: False for s in self.symbols}
        self.connected = False
        self.last_msg_ts = 0.0
        self._session: aiohttp.ClientSession | None = None

    # ── lifecycle ───────────────────────────────────────────────────

    async def run(self) -> None:
        self._session = aiohttp.ClientSession()
        try:
            await asyncio.gather(self._ws_loop(), self._oi_loop(), self._sample_loop())
        finally:
            await self._session.close()

    def _stream_url(self) -> str:
        parts = []
        for s in self.symbols:
            low = s.lower()
            parts += [f"{low}@depth@100ms", f"{low}@aggTrade",
                      f"{low}@forceOrder", f"{low}@markPrice@1s"]
        return WS_BASE + "/".join(parts)

    async def _ws_loop(self) -> None:
        backoff = 1.0
        while True:
            try:
                async with websockets.connect(
                    self._stream_url(), ping_interval=20, ping_timeout=20, close_timeout=5,
                    max_queue=2048,
                ) as ws:
                    self.connected = True
                    backoff = 1.0
                    log.info("connected: %s", ", ".join(self.symbols))
                    # A reconnect invalidates every local book.
                    for sym in self.symbols:
                        self._force_resync(sym)
                    async for raw in ws:
                        self.last_msg_ts = time.time()
                        await self._dispatch(json.loads(raw))
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.connected = False
                log.warning("websocket dropped (%s) — reconnecting in %.0fs", exc, backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60.0)

    # ── dispatch ────────────────────────────────────────────────────

    async def _dispatch(self, msg: dict) -> None:
        data = msg.get("data")
        if not data:
            return
        etype = data.get("e")
        sym = data.get("s") or (data.get("o") or {}).get("s")
        if sym not in self.states:
            return
        if etype == "depthUpdate":
            await self._on_depth(sym, data)
        elif etype == "aggTrade":
            self._on_trade(sym, data)
        elif etype == "forceOrder":
            self._on_liquidation(sym, data)
        elif etype == "markPriceUpdate":
            self._on_mark(sym, data)

    async def _on_depth(self, sym: str, ev: dict) -> None:
        state = self.states[sym]
        book = state.book

        if not book.ready:
            # Buffer while the snapshot is in flight, then replay.
            self.pending[sym].append(ev)
            if not self.syncing[sym]:
                asyncio.create_task(self._resync(sym))
            return

        try:
            book.apply_diff(ev)
        except DesyncError as exc:
            log.warning("%s desync: %s — resyncing", sym, exc)
            self._force_resync(sym)
            self.pending[sym].append(ev)
            asyncio.create_task(self._resync(sym))
            return

        # A crossed replica means we have drifted, whatever the ids say.
        if book.crossed():
            log.warning("%s crossed book — resyncing", sym)
            self._force_resync(sym)
            asyncio.create_task(self._resync(sym))

    def _force_resync(self, sym: str) -> None:
        self.states[sym].book.mark_desynced()
        self.pending[sym].clear()

    async def _resync(self, sym: str) -> None:
        if self.syncing[sym]:
            return
        self.syncing[sym] = True
        try:
            await asyncio.sleep(0.5)          # let a few diffs accumulate
            snap = await self._fetch_snapshot(sym)
            if snap is None:
                return
            book = self.states[sym].book
            book.apply_snapshot(snap)
            buffered, self.pending[sym] = self.pending[sym], []
            applied = 0
            for ev in buffered:
                try:
                    book.apply_diff(ev)
                    applied += 1
                except DesyncError:
                    continue          # pre-snapshot events, safe to skip
            log.info("%s resynced at %s (%d buffered events replayed)",
                     sym, book.last_update_id, applied)
        except Exception as exc:
            log.error("%s resync failed: %s", sym, exc)
            self.states[sym].book.mark_desynced()
        finally:
            self.syncing[sym] = False

    async def _fetch_snapshot(self, sym: str) -> dict | None:
        url = f"{REST_BASE}/fapi/v1/depth"
        params = {"symbol": sym, "limit": self.cfg["book"]["depth_limit"]}
        try:
            async with self._session.get(url, params=params, timeout=10) as r:
                if r.status != 200:
                    log.error("snapshot %s returned HTTP %s", sym, r.status)
                    return None
                return await r.json()
        except Exception as exc:
            log.error("snapshot %s failed: %s", sym, exc)
            return None

    def _on_trade(self, sym: str, d: dict) -> None:
        # "m" is true when the BUYER was the maker, which means the
        # aggressor — the side that actually initiated — was the seller.
        self.states[sym].on_trade(Trade(
            ts=d["T"] / 1000.0,
            price=float(d["p"]),
            qty=float(d["q"]),
            is_buy=not d["m"],
        ))

    def _on_liquidation(self, sym: str, d: dict) -> None:
        o = d["o"]
        # A forced SELL closes a long. A forced BUY closes a short.
        self.states[sym].on_liquidation(Liquidation(
            ts=o["T"] / 1000.0,
            price=float(o.get("ap") or o["p"]),
            qty=float(o["q"]),
            side="long" if o["S"] == "SELL" else "short",
        ))

    def _on_mark(self, sym: str, d: dict) -> None:
        st = self.states[sym]
        st.mark_price = float(d["p"])
        if d.get("r") not in (None, ""):
            st.funding = float(d["r"])

    # ── slow pollers ────────────────────────────────────────────────

    async def _oi_loop(self) -> None:
        while True:
            for sym in self.symbols:
                try:
                    url = f"{REST_BASE}/fapi/v1/openInterest"
                    async with self._session.get(url, params={"symbol": sym}, timeout=10) as r:
                        if r.status == 200:
                            oi = float((await r.json())["openInterest"])
                            st = self.states[sym]
                            st.oi_prev = st.open_interest
                            st.open_interest = oi
                except Exception as exc:
                    log.debug("open interest poll failed for %s: %s", sym, exc)
                await asyncio.sleep(1.0)
            await asyncio.sleep(15.0)

    async def _sample_loop(self) -> None:
        while True:
            now = time.time()
            for st in self.states.values():
                if st.book.ready:
                    st.sample(now)
            await asyncio.sleep(1.0)

    def health(self) -> dict:
        return {
            "connected": self.connected,
            "seconds_since_message": (time.time() - self.last_msg_ts) if self.last_msg_ts else None,
            "books": {s: {"ready": self.states[s].book.ready,
                          "resyncs": self.states[s].book.resyncs} for s in self.symbols},
        }
