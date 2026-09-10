"""End-to-end test with synthetic Bybit messages. No network."""
import asyncio, json, time
from msl_flow import config
from msl_flow.metrics import SymbolState
from msl_flow.collectors.bybit import BybitCollector

cfg = config.load()
cfg["symbols"] = ["BTCUSDT"]
states = {"BTCUSDT": SymbolState("BTCUSDT", cfg)}
c = BybitCollector(states, cfg)
st = states["BTCUSDT"]
ok = 0
def check(n, cond):
    global ok
    assert cond, "FAILED: " + n
    ok += 1; print("  pass:", n)

now = int(time.time() * 1000)

# depth selection must snap to a value Bybit actually serves
check("depth snaps to a served tier", c.depth in (1, 50, 200, 500))

# 1 — opening snapshot
c._dispatch({"topic": "orderbook.500.BTCUSDT", "type": "snapshot", "ts": now,
             "data": {"s": "BTCUSDT", "u": 100, "seq": 1,
                      "b": [["100.0", "5"], ["99.5", "10"]],
                      "a": [["100.5", "4"], ["101.0", "8"]]}})
check("snapshot builds the book", st.book.ready and st.book.mid == 100.25)

# 2 — in-sequence delta
c._dispatch({"topic": "orderbook.500.BTCUSDT", "type": "delta", "ts": now,
             "data": {"s": "BTCUSDT", "u": 101, "seq": 2,
                      "b": [["99.5", "0"]], "a": [["100.5", "6"]]}})
check("delta applies and deletes", 99.5 not in st.book.bids and st.book.asks[100.5] == 6.0)

# 3 — a sequence gap must desync rather than silently drift
c._dispatch({"topic": "orderbook.500.BTCUSDT", "type": "delta", "ts": now,
             "data": {"s": "BTCUSDT", "u": 999, "seq": 3, "b": [], "a": []}})
check("sequence gap marks the book unusable", not st.book.ready)
check("gap raises the resubscribe flag", c._resync_flag)
c._resync_flag = False

# 4 — recovery via a fresh snapshot
c._dispatch({"topic": "orderbook.500.BTCUSDT", "type": "snapshot", "ts": now,
             "data": {"s": "BTCUSDT", "u": 1, "seq": 4,
                      "b": [["100.0", "5"]], "a": [["100.5", "5"]]}})
check("resubscribe snapshot recovers", st.book.ready)

# 5 — trades: S is the TAKER side
c._dispatch({"topic": "publicTrade.BTCUSDT", "type": "snapshot", "ts": now,
             "data": [{"T": now, "s": "BTCUSDT", "S": "Buy", "v": "2", "p": "100"},
                      {"T": now, "s": "BTCUSDT", "S": "Sell", "v": "1", "p": "100"}]})
check("CVD signs taker direction", abs(st.cvd - 100.0) < 1e-9)   # +200 -100

# 6 — the convention that matters: Bybit S is the POSITION side
c._dispatch({"topic": "allLiquidation.BTCUSDT", "type": "snapshot", "ts": now,
             "data": [{"T": now, "s": "BTCUSDT", "S": "Buy", "v": "3", "p": "100"}]})
check("S=Buy is recorded as a LONG liquidation", st.liquidations[-1].side == "long")
c._dispatch({"topic": "allLiquidation.BTCUSDT", "type": "snapshot", "ts": now,
             "data": [{"T": now, "s": "BTCUSDT", "S": "Sell", "v": "1", "p": "100"}]})
check("S=Sell is recorded as a SHORT liquidation", st.liquidations[-1].side == "short")

# 7 — tickers arrive as partial deltas and must merge, not replace
c._dispatch({"topic": "tickers.BTCUSDT", "type": "snapshot", "ts": now,
             "data": {"symbol": "BTCUSDT", "markPrice": "100.2",
                      "fundingRate": "0.0001", "openInterest": "5000"}})
c._dispatch({"topic": "tickers.BTCUSDT", "type": "delta", "ts": now,
             "data": {"symbol": "BTCUSDT", "openInterest": "5100"}})
check("funding survives a partial ticker delta", st.funding == 0.0001)
check("open interest updates", st.open_interest == 5100.0)
check("previous OI retained for the delta", st.oi_prev == 5000.0)
check("oi_delta computed", abs(st.oi_delta() - 0.02) < 1e-9)

# 8 — unknown symbols and non-data frames are ignored, not fatal
c._dispatch({"success": True, "op": "subscribe"})
c._dispatch({"topic": "publicTrade.ETHUSDT", "data": [{"T": now, "s": "ETHUSDT",
             "S": "Buy", "v": "1", "p": "1"}]})
check("subscribe ack and foreign symbol ignored", True)

# 9 — snapshot serialises for the dashboard
st.sample(time.time())
snap = st.snapshot()
check("snapshot is JSON-serialisable", isinstance(json.dumps(snap, default=float), str))
check("snapshot carries the live fields",
      snap["mid"] == 100.25 and snap["liq_burst"]["total_usd"] > 0
      and snap["imbalance"] and snap["heatmap"])

# 10 — liquidation clusters group by price and split by side
cl = st.liquidation_clusters()
check("clusters split long and short notional",
      len(cl) == 1 and cl[0]["long_usd"] == 300.0 and cl[0]["short_usd"] == 100.0)

# 11 — the health payload the dashboard reads
h = c.health()
check("health reports book state", h["exchange"] == "bybit" and "BTCUSDT" in h["books"])

print(f"\n{ok} pipeline checks passed")
