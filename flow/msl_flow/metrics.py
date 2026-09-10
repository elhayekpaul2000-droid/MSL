"""Rolling flow state and the metrics computed from it.

Everything here is derived from data the exchange actually published.
Where a figure is a model rather than an observation it says so in the
field name and in the docstring — `liquidation clusters` below are
observed liquidations that have already happened, not a projection of
where future ones sit.
"""

from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass, field

from .book import OrderBook


@dataclass
class Trade:
    ts: float
    price: float
    qty: float
    is_buy: bool          # aggressor side

    @property
    def usd(self) -> float:
        return self.price * self.qty


@dataclass
class Liquidation:
    ts: float
    price: float
    qty: float
    side: str             # "long" or "short" — the side that was closed out

    @property
    def usd(self) -> float:
        return self.price * self.qty


class SymbolState:
    def __init__(self, symbol: str, cfg: dict) -> None:
        self.symbol = symbol
        self.cfg = cfg
        self.book = OrderBook(symbol)

        self.trades: deque[Trade] = deque()
        self.liquidations: deque[Liquidation] = deque()
        self.large_trades: deque[Trade] = deque(maxlen=50)
        self.heatmap: deque[dict] = deque(maxlen=cfg["book"]["heatmap_history"])

        self.cvd = 0.0                # cumulative, session-scoped
        self.cvd_series: deque[tuple[float, float]] = deque(maxlen=1800)
        self.price_series: deque[tuple[float, float]] = deque(maxlen=1800)

        self.funding: float | None = None
        self.open_interest: float | None = None
        self.oi_prev: float | None = None
        self.mark_price: float | None = None

        self.last_heatmap_ts = 0.0

    # ── ingestion ───────────────────────────────────────────────────

    def on_trade(self, t: Trade) -> None:
        self.trades.append(t)
        self.cvd += t.usd if t.is_buy else -t.usd
        if t.usd >= self.cfg["flow"]["large_trade_usd"]:
            self.large_trades.append(t)
        self._expire(self.trades, self.cfg["flow"]["cvd_window_s"])

    def on_liquidation(self, liq: Liquidation) -> None:
        self.liquidations.append(liq)
        self._expire(self.liquidations, self.cfg["liquidations"]["cluster_window_s"])

    def sample(self, now: float | None = None) -> None:
        """Take a once-per-second sample of the book and the series."""
        now = now or time.time()
        if now - self.last_heatmap_ts < 1.0:
            return
        self.last_heatmap_ts = now
        row = self.book.heatmap_row(
            self.cfg["book"]["heatmap_bucket_bps"],
            self.cfg["book"]["heatmap_range_bps"],
        )
        row["ts"] = now
        self.heatmap.append(row)
        mid = self.book.mid
        if mid is not None:
            self.price_series.append((now, mid))
            self.cvd_series.append((now, self.cvd))

    @staticmethod
    def _expire(dq: deque, window_s: float, now: float | None = None) -> None:
        now = now or time.time()
        while dq and now - dq[0].ts > window_s:
            dq.popleft()

    # ── metrics ─────────────────────────────────────────────────────

    def imbalances(self) -> dict[str, float | None]:
        return {f"{b}bps": self.book.imbalance(b) for b in self.cfg["book"]["bands_bps"]}

    def cvd_window(self) -> float:
        """Signed USD flow inside the rolling window."""
        return sum(t.usd if t.is_buy else -t.usd for t in self.trades)

    def trade_intensity(self) -> dict:
        buy = sum(t.usd for t in self.trades if t.is_buy)
        sell = sum(t.usd for t in self.trades if not t.is_buy)
        total = buy + sell
        return {
            "buy_usd": buy,
            "sell_usd": sell,
            "ratio": (buy - sell) / total if total > 0 else None,
            "count": len(self.trades),
        }

    def liquidation_clusters(self) -> list[dict]:
        """Liquidations that HAVE happened, grouped by price bucket.

        This is observed data. It is not a projection of where future
        liquidations sit — that requires modelling leverage cohorts and
        is not something the exchange publishes.
        """
        mid = self.book.mid
        if mid is None or not self.liquidations:
            return []
        bucket = mid * self.cfg["liquidations"]["cluster_bucket_bps"] / 10_000.0
        agg: dict[float, dict] = {}
        for liq in self.liquidations:
            key = round(round(liq.price / bucket) * bucket, 8)
            slot = agg.setdefault(key, {"price": key, "long_usd": 0.0, "short_usd": 0.0})
            slot["long_usd" if liq.side == "long" else "short_usd"] += liq.usd
        out = sorted(agg.values(), key=lambda d: d["price"])
        for slot in out:
            slot["total_usd"] = slot["long_usd"] + slot["short_usd"]
        return out

    def liquidation_burst(self) -> dict:
        now = time.time()
        recent = [l for l in self.liquidations if now - l.ts <= 10.0]
        longs = sum(l.usd for l in recent if l.side == "long")
        shorts = sum(l.usd for l in recent if l.side == "short")
        return {"long_usd": longs, "short_usd": shorts, "total_usd": longs + shorts}

    def cvd_divergence(self) -> str | None:
        """Price makes a window extreme, cumulative delta does not.

        Returns "bearish" when price prints a new high on flow that is
        not confirming, "bullish" for the mirror case, None otherwise.
        """
        if len(self.price_series) < 60 or len(self.cvd_series) < 60:
            return None
        prices = [p for _, p in self.price_series]
        cvds = [c for _, c in self.cvd_series]
        last_p, last_c = prices[-1], cvds[-1]
        if last_p >= max(prices) and last_c < max(cvds):
            return "bearish"
        if last_p <= min(prices) and last_c > min(cvds):
            return "bullish"
        return None

    def oi_delta(self) -> float | None:
        if self.open_interest is None or self.oi_prev is None or self.oi_prev == 0:
            return None
        return self.open_interest / self.oi_prev - 1.0

    # ── serialisation for the dashboard ─────────────────────────────

    def snapshot(self) -> dict:
        mid = self.book.mid
        return {
            "symbol": self.symbol,
            "ready": self.book.ready,
            "resyncs": self.book.resyncs,
            "mid": mid,
            "best_bid": self.book.best_bid,
            "best_ask": self.book.best_ask,
            "spread_bps": ((self.book.best_ask - self.book.best_bid) / mid * 10_000.0)
                          if mid and self.book.best_bid and self.book.best_ask else None,
            "imbalance": self.imbalances(),
            "cvd_session": self.cvd,
            "cvd_window": self.cvd_window(),
            "intensity": self.trade_intensity(),
            "liq_clusters": self.liquidation_clusters(),
            "liq_burst": self.liquidation_burst(),
            "cvd_divergence": self.cvd_divergence(),
            "funding": self.funding,
            "open_interest": self.open_interest,
            "oi_delta": self.oi_delta(),
            "large_trades": [
                {"ts": t.ts, "price": t.price, "usd": t.usd, "side": "buy" if t.is_buy else "sell"}
                for t in list(self.large_trades)[-12:]
            ],
            "heatmap": list(self.heatmap),
            "price_series": list(self.price_series)[-300:],
            "cvd_series": list(self.cvd_series)[-300:],
        }
