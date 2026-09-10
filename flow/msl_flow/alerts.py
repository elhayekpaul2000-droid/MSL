"""Threshold alerts pushed to Telegram.

Deliberately quiet. An alerting service you learn to ignore is worse
than none, so every rule carries a cooldown and only fires on a state
CHANGE, never on a level being held.
"""

from __future__ import annotations

import logging
import time

import aiohttp

log = logging.getLogger("msl.alerts")


class Alerter:
    def __init__(self, cfg: dict) -> None:
        acfg = cfg.get("alerts", {})
        self.enabled = bool(acfg.get("enabled"))
        self.token = acfg.get("telegram_token") or ""
        self.chat_id = acfg.get("telegram_chat_id") or ""
        self.cooldown = float(acfg.get("cooldown_s", 120))
        self.rules = acfg.get("rules", {})
        self.cfg = cfg
        self._last: dict[str, float] = {}
        self._prev_state: dict[str, str] = {}
        self.log_only = not (self.token and self.chat_id)
        if self.enabled and self.log_only:
            log.warning("alerts on but no Telegram credentials — logging to console instead")

    async def send(self, session: aiohttp.ClientSession, key: str, text: str) -> None:
        if not self.enabled:
            return
        now = time.time()
        if now - self._last.get(key, 0.0) < self.cooldown:
            return
        self._last[key] = now
        if self.log_only:
            log.info("ALERT %s | %s", key, text)
            return
        try:
            url = f"https://api.telegram.org/bot{self.token}/sendMessage"
            async with session.post(url, json={"chat_id": self.chat_id, "text": text},
                                    timeout=10) as r:
                if r.status != 200:
                    log.error("telegram HTTP %s: %s", r.status, await r.text())
        except Exception as exc:
            log.error("telegram send failed: %s", exc)

    async def evaluate(self, session: aiohttp.ClientSession, st) -> None:
        if not self.enabled or not st.book.ready:
            return
        sym = st.symbol

        # Book imbalance flipping across the threshold — a change of
        # state, not a level being sustained.
        thresh = float(self.rules.get("book_flip", 0))
        if thresh > 0:
            bands = self.cfg["book"]["bands_bps"]
            imb = st.book.imbalance(bands[-1])
            if imb is not None:
                now_state = "bid" if imb >= thresh else "ask" if imb <= -thresh else "flat"
                prev = self._prev_state.get(f"{sym}:book")
                self._prev_state[f"{sym}:book"] = now_state
                if prev and prev != now_state and now_state != "flat":
                    side = "BID heavy" if now_state == "bid" else "ASK heavy"
                    await self.send(session, f"{sym}:book:{now_state}",
                                    f"{sym} book flipped {side} "
                                    f"({bands[-1]}bps imbalance {imb:+.2f}) @ {st.book.mid:.2f}")

        if self.rules.get("liq_burst"):
            burst = st.liquidation_burst()
            limit = float(self.cfg["liquidations"]["burst_usd"])
            if burst["total_usd"] >= limit:
                dom = "longs" if burst["long_usd"] >= burst["short_usd"] else "shorts"
                await self.send(session, f"{sym}:liqburst",
                                f"{sym} liquidation burst — ${burst['total_usd']:,.0f} "
                                f"in 10s, mostly {dom} @ {st.book.mid:.2f}")

        if self.rules.get("cvd_divergence"):
            div = st.cvd_divergence()
            prev = self._prev_state.get(f"{sym}:div")
            self._prev_state[f"{sym}:div"] = div or ""
            if div and div != prev:
                word = "new high on unconfirming flow" if div == "bearish" \
                       else "new low on unconfirming flow"
                await self.send(session, f"{sym}:div:{div}",
                                f"{sym} {word} — price/CVD divergence ({div}) @ {st.book.mid:.2f}")
