"""Local replica of an exchange order book, maintained from a diff stream.

The heatmap and every imbalance figure downstream are only as honest as
this file. Binance publishes a sequenced diff stream and a REST
snapshot; if a single update is dropped the book silently drifts and
every number computed from it becomes fiction. So the sequencing rules
are enforced strictly and any violation forces a full resynchronisation
rather than a best-effort patch.
"""

from __future__ import annotations

import bisect
from dataclasses import dataclass, field


@dataclass
class BookLevel:
    price: float
    qty: float


class DesyncError(Exception):
    """The diff stream no longer lines up with the local replica."""


class OrderBook:
    def __init__(self, symbol: str) -> None:
        self.symbol = symbol
        self.bids: dict[float, float] = {}
        self.asks: dict[float, float] = {}
        self.last_update_id: int | None = None
        self.ready = False
        self.resyncs = 0
        self._first_applied = False

    # ── construction ────────────────────────────────────────────────

    def apply_snapshot(self, snapshot: dict) -> None:
        self.bids = {float(p): float(q) for p, q in snapshot["bids"] if float(q) > 0}
        self.asks = {float(p): float(q) for p, q in snapshot["asks"] if float(q) > 0}
        self.last_update_id = int(snapshot["lastUpdateId"])
        self.ready = True
        self._first_applied = False

    def load_levels(self, bids, asks, seq: int) -> None:
        """Replace the book wholesale from an exchange-pushed snapshot.

        Bybit streams its own snapshot on subscribe and again whenever
        its matching engine restarts, so no REST call is needed.
        """
        self.bids = {float(p): float(q) for p, q in bids if float(q) > 0}
        self.asks = {float(p): float(q) for p, q in asks if float(q) > 0}
        self.last_update_id = int(seq)
        self.ready = True
        self._first_applied = True

    def apply_levels(self, bids, asks, seq: int) -> None:
        """Apply a delta. Sequencing is the caller's responsibility —
        it differs per exchange and this class stays neutral about it."""
        for price, qty in bids:
            self._set(self.bids, float(price), float(qty))
        for price, qty in asks:
            self._set(self.asks, float(price), float(qty))
        self.last_update_id = int(seq)

    def apply_diff(self, ev: dict) -> None:
        """Apply one depthUpdate event.

        Raises DesyncError when the event cannot be applied safely. The
        caller is expected to re-snapshot rather than continue.
        """
        if not self.ready or self.last_update_id is None:
            raise DesyncError("book has no snapshot")

        u_first = int(ev["U"])
        u_last = int(ev["u"])
        u_prev = ev.get("pu")

        # Wholly historical relative to the snapshot: safe to ignore.
        if u_last < self.last_update_id:
            return

        if not self._first_applied:
            # Binance futures sends `pu` on every event including the
            # first one after a snapshot, so the first event cannot be
            # recognised by its absence — it is recognised by state.
            # It must straddle the snapshot id.
            if not (u_first <= self.last_update_id <= u_last):
                raise DesyncError(
                    f"first event {u_first}-{u_last} does not straddle "
                    f"snapshot {self.last_update_id}"
                )
        elif u_prev is not None and int(u_prev) != self.last_update_id:
            # Every later event must chain onto the previous one exactly.
            raise DesyncError(
                f"sequence break: pu={u_prev} but local={self.last_update_id}"
            )

        for price, qty in ev.get("b", []):
            self._set(self.bids, float(price), float(qty))
        for price, qty in ev.get("a", []):
            self._set(self.asks, float(price), float(qty))

        self.last_update_id = u_last
        self._first_applied = True

    @staticmethod
    def _set(side: dict[float, float], price: float, qty: float) -> None:
        if qty <= 0:
            side.pop(price, None)
        else:
            side[price] = qty

    def mark_desynced(self) -> None:
        self.ready = False
        self._first_applied = False
        self.resyncs += 1

    # ── reads ───────────────────────────────────────────────────────

    @property
    def best_bid(self) -> float | None:
        return max(self.bids) if self.bids else None

    @property
    def best_ask(self) -> float | None:
        return min(self.asks) if self.asks else None

    @property
    def mid(self) -> float | None:
        b, a = self.best_bid, self.best_ask
        if b is None or a is None or a <= b:
            return None
        return (a + b) / 2.0

    def crossed(self) -> bool:
        """A crossed book means the replica is corrupt, not that the
        market is. Treated as a desync signal."""
        b, a = self.best_bid, self.best_ask
        return b is not None and a is not None and b >= a

    def notional_within(self, bps: float) -> tuple[float, float]:
        """Resting notional inside `bps` of mid, as (bid_usd, ask_usd)."""
        mid = self.mid
        if mid is None:
            return 0.0, 0.0
        span = mid * bps / 10_000.0
        lo, hi = mid - span, mid + span
        bid_usd = sum(p * q for p, q in self.bids.items() if p >= lo)
        ask_usd = sum(p * q for p, q in self.asks.items() if p <= hi)
        return bid_usd, ask_usd

    def imbalance(self, bps: float) -> float | None:
        """(bid - ask) / (bid + ask) inside a band. -1 to +1."""
        bid_usd, ask_usd = self.notional_within(bps)
        total = bid_usd + ask_usd
        if total <= 0:
            return None
        return (bid_usd - ask_usd) / total

    def heatmap_row(self, bucket_bps: float, range_bps: float) -> dict:
        """One column of the depth heatmap: resting notional per price
        bucket, signed positive for bids and negative for asks."""
        mid = self.mid
        if mid is None:
            return {"mid": None, "buckets": {}}
        bucket = mid * bucket_bps / 10_000.0
        span = mid * range_bps / 10_000.0
        buckets: dict[int, float] = {}
        for p, q in self.bids.items():
            if mid - span <= p <= mid:
                buckets[int((p - mid) // bucket)] = buckets.get(int((p - mid) // bucket), 0.0) + p * q
        for p, q in self.asks.items():
            if mid <= p <= mid + span:
                k = int((p - mid) // bucket)
                buckets[k] = buckets.get(k, 0.0) - p * q
        return {"mid": mid, "bucket_size": bucket, "buckets": buckets}

    def depth_profile(self, bucket_bps: float, range_bps: float, side: str) -> list[tuple[float, float]]:
        """Resting notional by absolute price, one side, for the ladder."""
        mid = self.mid
        if mid is None:
            return []
        book = self.bids if side == "bid" else self.asks
        bucket = mid * bucket_bps / 10_000.0
        span = mid * range_bps / 10_000.0
        agg: dict[float, float] = {}
        for p, q in book.items():
            if abs(p - mid) <= span:
                key = round(round(p / bucket) * bucket, 8)
                agg[key] = agg.get(key, 0.0) + p * q
        return sorted(agg.items())
