# MSL Flow

Live order-flow service. Reads the parts of the market that Pine cannot:
resting book depth, true taker-side volume delta, liquidations, funding
and open interest. Runs beside TradingView, not inside it — Pine has no
way to ingest an external feed, so this is a second screen.

Currently collects **Bybit linear perpetuals** over public WebSockets.
No API key, no account, no data cost.

---

## Running it

You need Python 3.11 or newer. Three commands:

```bash
cd flow
pip install -r requirements.txt
python run.py
```

Then open **http://127.0.0.1:8787**.

To stop it, press Ctrl+C in the terminal.

The first few seconds show an empty dashboard while the book snapshot
arrives and the one-second samples start stacking up. The heatmap needs
roughly a minute before it has enough columns to read.

---

## Changing what it watches

Everything lives in `config.yaml`. Edit it, save, restart.

```yaml
symbols:
  - BTCUSDT
  - ETHUSDT
```

Use Bybit's linear perpetual symbol names. Anything Bybit lists works.

---

## Phone alerts

Off by default in the sense that with no credentials it just logs to the
terminal. To get them on your phone:

1. Open Telegram, message **@BotFather**, send `/newbot`, follow the
   prompts. It gives you a token.
2. Message **@userinfobot**. It replies with your numeric chat id.
3. Put both in `config.yaml` under `alerts:` and restart.

Three rules ship enabled. Each fires on a change of state, never on a
level being held, and each has a cooldown — an alerting service you
learn to ignore is worse than none.

| Rule | Fires when |
|---|---|
| `book_flip` | 25bps book imbalance crosses ±0.35 |
| `liq_burst` | more than $500k liquidated inside 10 seconds |
| `cvd_divergence` | price makes a window extreme and cumulative delta does not |

---

## What the dashboard shows

**Depth heatmap** — resting notional by price over time. Blue is bid,
red is ask, and intensity is size. The dashed line is mid. Walls that
persist are real; walls that vanish as price approaches were never
there to trade against.

**Mid price** and **CVD** are deliberately two separate panels rather
than one chart with two y-axes. A dual-axis chart lets you slide the
scales until any two series look correlated, which is how people talk
themselves into divergences that are not there.

**Liquidation clusters** — liquidations that have *already printed*,
grouped by price. Long liquidations render red because they produce
forced selling; short liquidations render blue because they produce
forced buying. The colour follows the direction of resulting flow, the
same convention as the heatmap.

This is **not** a liquidation-level projection. Sites that sell you a
"liquidation heatmap" of where future liquidations sit are modelling
leverage cohorts, not reading data — no exchange publishes it.

**Large prints** — single fills over the configured threshold.

---

## Storage

Every second, a sample of mid, imbalance, CVD, OI and funding is written
to `msl_flow.db` (SQLite). This is the point of running it early: you
cannot measure whether book imbalance actually predicted the next move
on your instrument until you have months of it. Set
`storage.persist: false` to turn it off.

---

## Correctness notes

The heatmap and every imbalance figure are only as good as the local
book replica. Bybit streams sequenced deltas; a single dropped update
makes the replica drift and every number computed from it becomes
fiction. So the collector enforces the sequence strictly: any gap, or
any crossed book, marks the book *not ready* and forces a resubscribe
rather than patching over it. The dashboard's book indicator goes red
while that happens. A book that is not provably in sync is never served.

One convention worth knowing, because getting it backwards inverts the
signal: in Bybit's `allLiquidation` feed, `S` is the side of the
**position** that was liquidated, so `S="Buy"` means a *long* was
liquidated. This is the opposite of Binance's `forceOrder` feed. Both
collectors handle their own convention.

---

## Testing

```bash
python test_book.py       # order book sequencing
python test_pipeline.py   # collector -> state -> dashboard payload
python test_server.py     # HTTP and JSON surface
```

These run offline against synthetic messages built to Bybit's documented
v5 schema. **They do not prove the live feed works** — that is verified
the first time you run `python run.py` against the real exchange.

---

## Not built yet

- **Forex.** FXCM is an execution venue; its APIs give you price and
  order management, not market microstructure, and it publishes no
  order book. Real FX order flow means CME FX futures depth (paid).
  Retail positioning and stop clusters mean OANDA's free order book and
  position book API, which needs only a free account — you do not have
  to trade there to read it.
- **Options gamma.** Needs a paid chain feed. Relevant to index CFDs.
- **Binance collector** is written and included; switch with
  `exchange: binance` in the config. Worth having even while executing
  on Bybit, since Binance carries the deeper book.
