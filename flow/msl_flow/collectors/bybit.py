"""Bybit v5 linear-perpetual collector.

Four public topics per symbol, no API key required:
    orderbook.{depth}.{sym}   snapshot + sequenced deltas
    publicTrade.{sym}         taker-tagged prints, the source of CVD
    allLiquidation.{sym}      liquidations
    tickers.{sym}             mark price, funding rate, open interest

Bybit pushes its own book snapshot on subscribe and again whenever its
matching engine restarts, so unlike Binance there is no REST snapshot
to race against — a resync is just a resubscribe.

One convention worth stating loudly, because getting it backwards
inverts the signal: in allLiquidation, S is the side of the POSITION
that was liquidated, not the side of the order that closed it.
S == "Buy" therefore means a LONG was liquidated. This is the opposite
of Binance's forceOrder feed. Confirmed against Bybit's own API
announcement channel and the tardis-node reference mapper.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time

import websockets

from ..metrics import Liquidation, Trade

log = logging.getLogger("msl.bybit")

WS_URL = "wss://stream.bybit.com/v5/public/linear"
VALID_DEPTHS = (1, 50, 200, 500)


class BybitCollector:
    name = "bybit"

    def __init__(self, states: dict, cfg: dict) -> None:
        self.states = states
        self.cfg = cfg
        self.symbols = [s.upper() for s in cfg["symbols"]]
        self.depth = self._pick_depth(cfg["book"].get("depth_limit", 500))
        self.expected_u: dict[str, int | None] = {s: None for s in self.symbols}
        self.ticker_cache: dict[str, dict] = {s: {} for s in self.symbols}
        self.connected = False
        self.last_msg_ts = 0.0
        self._resync_flag = False

    @staticmethod
    def _pick_depth(requested: int) -> int:
        """Bybit only serves fixed depths for linear contracts."""
        for d in VALID_DEPTHS:
            if requested <= d:
                return d
        return VALID_DEPTHS[-1]

    # ── lifecycle ───────────────────────────────────────────────────

    async def run(self) -> None:
        await asyncio.gather(self._ws_loop(), self._sample_loop())

    def _topics(self) -> list[str]:
        out = []
        for s in self.symbols:
            out += [f"orderbook.{self.depth}.{s}", f"publicTrade.{s}",
                    f"allLiquidation.{s}", f"tickers.{s}"]
        return out

    async def _ws_loop(self) -> None:
        backoff = 1.0
        while True:
            try:
                async with websockets.connect(
                    WS_URL, ping_interval=None, close_timeout=5, max_queue=2048
                ) as ws:
                    await ws.send(json.dumps({"op": "subscribe", "args": self._topics()}))
                    self.connected = True
                    backoff = 1.0
                    log.info("connected: %s (book depth %d)", ", ".join(self.symbols), self.depth)
                    for sym in self.symbols:
                        self.states[sym].book.mark_desynced()
                        self.expected_u[sym] = None

                    keepalive = asyncio.create_task(self._keepalive(ws))
                    try:
                        async for raw in ws:
                            self.last_msg_ts = time.time()
                            self._dispatch(json.loads(raw))
                            if self._resync_flag:
                                self._resync_flag = False
                                raise ConnectionError("book desync — forcing resubscribe")
                    finally:
                        keepalive.cancel()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.connected = False
                log.warning("websocket dropped (%s) — reconnecting in %.0fs", exc, backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60.0)

    async def _keepalive(self, ws) -> None:
        """Bybit closes idle connections; it wants a JSON ping, not a
        protocol-level one."""
        while True:
            await asyncio.sleep(20)
            try:
                await ws.send(json.dumps({"op": "ping"}))
            except Exception:
                return

    # ── dispatch ────────────────────────────────────────────────────

    def _dispatch(self, msg: dict) -> None:
        topic = msg.get("topic")
        if not topic:
            return                     # subscribe ack / pong
        head = topic.split(".")[0]
        if head == "orderbook":
            self._on_book(msg)
        elif head == "publicTrade":
            self._on_trades(msg)
        elif head == "allLiquidation":
            self._on_liquidations(msg)
        elif head == "tickers":
            self._on_ticker(msg)

    def _on_book(self, msg: dict) -> None:
        d = msg.get("data") or {}
        sym = d.get("s")
        if sym not in self.states:
            return
        book = self.states[sym].book
        u = int(d["u"])

        if msg.get("type") == "snapshot" or u == 1:
            # u == 1 signals Bybit restarted the sequence for this topic.
            book.load_levels(d.get("b", []), d.get("a", []), u)
            self.expected_u[sym] = u + 1
            return

        if not book.ready:
            return                     # waiting for the opening snapshot

        expected = self.expected_u[sym]
        if expected is not None and u != expected:
            log.warning("%s sequence gap: expected u=%s got u=%s — resubscribing",
                        sym, expected, u)
            book.mark_desynced()
            self.expected_u[sym] = None
            self._resync_flag = True
            return

        book.apply_levels(d.get("b", []), d.get("a", []), u)
        self.expected_u[sym] = u + 1

        if book.crossed():
            log.warning("%s crossed book — resubscribing", sym)
            book.mark_desynced()
            self.expected_u[sym] = None
            self._resync_flag = True

    def _on_trades(self, msg: dict) -> None:
        for t in msg.get("data", []):
            sym = t.get("s")
            if sym not in self.states:
                continue
            # For publicTrade, S IS the taker's direction.
            self.states[sym].on_trade(Trade(
                ts=int(t["T"]) / 1000.0,
                price=float(t["p"]),
                qty=float(t["v"]),
                is_buy=t["S"] == "Buy",
            ))

    def _on_liquidations(self, msg: dict) -> None:
        for l in msg.get("data", []):
            sym = l.get("s")
            if sym not in self.states:
                continue
            # S is the POSITION side. Buy == a long was liquidated.
            self.states[sym].on_liquidation(Liquidation(
                ts=int(l["T"]) / 1000.0,
                price=float(l["p"]),
                qty=float(l["v"]),
                side="long" if l["S"] == "Buy" else "short",
            ))

    def _on_ticker(self, msg: dict) -> None:
        d = msg.get("data") or {}
        sym = d.get("symbol")
        if sym not in self.states:
            return
        # tickers arrives as deltas carrying only changed fields, so the
        # cache is merged rather than replaced.
        cache = self.ticker_cache[sym]
        cache.update(d)
        st = self.states[sym]
        if cache.get("markPrice"):
            st.mark_price = float(cache["markPrice"])
        if cache.get("fundingRate") not in (None, ""):
            st.funding = float(cache["fundingRate"])
        if cache.get("openInterest") not in (None, ""):
            oi = float(cache["openInterest"])
            if st.open_interest is None or oi != st.open_interest:
                st.oi_prev = st.open_interest
                st.open_interest = oi

    async def _sample_loop(self) -> None:
        while True:
            now = time.time()
            for st in self.states.values():
                if st.book.ready:
                    st.sample(now)
            await asyncio.sleep(1.0)

    def health(self) -> dict:
        return {
            "exchange": "bybit",
            "connected": self.connected,
            "seconds_since_message": (time.time() - self.last_msg_ts) if self.last_msg_ts else None,
            "books": {s: {"ready": self.states[s].book.ready,
                          "resyncs": self.states[s].book.resyncs} for s in self.symbols},
        }
