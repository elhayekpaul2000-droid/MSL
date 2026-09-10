"""Sequencing tests. These run without a network connection."""
from msl_flow.book import OrderBook, DesyncError

def snap(uid, bids, asks):
    return {"lastUpdateId": uid, "bids": bids, "asks": asks}

def ev(U, u, pu=None, b=None, a=None):
    e = {"U": U, "u": u, "b": b or [], "a": a or []}
    if pu is not None: e["pu"] = pu
    return e

ok = 0
def check(name, cond):
    global ok
    assert cond, "FAILED: " + name
    ok += 1
    print("  pass:", name)

# 1 — snapshot loads
ob = OrderBook("BTCUSDT")
ob.apply_snapshot(snap(100, [["100.0","2"],["99.0","3"]], [["101.0","1"],["102.0","4"]]))
check("snapshot sets best bid/ask", ob.best_bid == 100.0 and ob.best_ask == 101.0)
check("mid is correct", ob.mid == 100.5)

# 2 — stale events are ignored, not fatal
ob.apply_diff(ev(50, 90, pu=49))
check("stale event ignored", ob.last_update_id == 100)

# 3 — first event must straddle the snapshot id (pu present, as Binance sends)
ob2 = OrderBook("X"); ob2.apply_snapshot(snap(100, [["100","1"]], [["101","1"]]))
try:
    ob2.apply_diff(ev(200, 300, pu=199)); bad = True
except DesyncError:
    bad = False
check("non-straddling first event rejected", not bad)

# 4 — valid first event applies despite pu not matching
ob3 = OrderBook("X"); ob3.apply_snapshot(snap(100, [["100","1"]], [["101","1"]]))
ob3.apply_diff(ev(98, 105, pu=97, b=[["100","5"]]))
check("straddling first event applies", ob3.bids[100.0] == 5.0 and ob3.last_update_id == 105)

# 5 — chained events
ob3.apply_diff(ev(106, 110, pu=105, a=[["101","9"]]))
check("chained event applies", ob3.asks[101.0] == 9.0)

# 6 — a gap forces desync
try:
    ob3.apply_diff(ev(120, 130, pu=119)); bad = True
except DesyncError:
    bad = False
check("sequence gap raises DesyncError", not bad)

# 7 — zero qty deletes
ob4 = OrderBook("X"); ob4.apply_snapshot(snap(1, [["100","1"],["99","1"]], [["101","1"]]))
ob4.apply_diff(ev(1, 2, pu=0, b=[["99","0"]]))
check("zero qty removes the level", 99.0 not in ob4.bids)

# 8 — imbalance maths
ob5 = OrderBook("X")
ob5.apply_snapshot(snap(1, [["100","10"]], [["100.5","5"]]))   # mid 100.25
imb = ob5.imbalance(100)
expected = (100*10 - 100.5*5) / (100*10 + 100.5*5)
check("imbalance matches hand calculation", abs(imb - expected) < 1e-12)
check("imbalance bounded", -1.0 <= imb <= 1.0)

# 9 — empty band returns None rather than a fake zero
ob6 = OrderBook("X"); ob6.apply_snapshot(snap(1, [["50","1"]], [["150","1"]]))
check("no depth in band -> None, not 0", ob6.imbalance(1) is None)

# 10 — crossed book detected
ob7 = OrderBook("X"); ob7.apply_snapshot(snap(1, [["101","1"]], [["100","1"]]))
check("crossed book detected", ob7.crossed() and ob7.mid is None)

# 11 — resync clears the first-event flag
ob7.mark_desynced()
check("desync clears ready state", not ob7.ready and ob7.resyncs == 1)

# 12 — heatmap is signed: bids positive, asks negative
ob8 = OrderBook("X"); ob8.apply_snapshot(snap(1, [["99.9","10"]], [["100.1","10"]]))
row = ob8.heatmap_row(2.5, 150)
check("heatmap has both sides", any(v > 0 for v in row["buckets"].values())
                                and any(v < 0 for v in row["buckets"].values()))

print(f"\n{ok}/12 book tests passed")
