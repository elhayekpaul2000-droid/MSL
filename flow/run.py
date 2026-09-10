#!/usr/bin/env python3
"""MSL Flow — entry point.

    python run.py                 # uses ./config.yaml
    python run.py my-config.yaml

Everything runs in one asyncio loop: the exchange collector, the SQLite
writer, the alert evaluator and the dashboard server.
"""

from __future__ import annotations

import asyncio
import logging
import sys

import aiohttp
import uvicorn

from msl_flow import config
from msl_flow.alerts import Alerter
from msl_flow.metrics import SymbolState
from msl_flow.server import build_app
from msl_flow.store import Store

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s %(name)-12s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("msl")


def make_collector(name: str, states: dict, cfg: dict):
    if name == "bybit":
        from msl_flow.collectors.bybit import BybitCollector
        return BybitCollector(states, cfg)
    if name == "binance":
        from msl_flow.collectors.binance import BinanceCollector
        return BinanceCollector(states, cfg)
    raise SystemExit(f"unknown exchange: {name!r} (expected bybit or binance)")


async def housekeeping(states: dict, store: Store, alerter: Alerter) -> None:
    """Persist a sample and evaluate the alert rules once per second."""
    async with aiohttp.ClientSession() as session:
        while True:
            await asyncio.sleep(1.0)
            for st in states.values():
                try:
                    store.sample_state(st)
                    await alerter.evaluate(session, st)
                except Exception as exc:
                    log.error("housekeeping failed for %s: %s", st.symbol, exc)


async def main() -> None:
    cfg = config.load(sys.argv[1] if len(sys.argv) > 1 else None)
    states = {s: SymbolState(s, cfg) for s in cfg["symbols"]}
    collector = make_collector(cfg.get("exchange", "bybit"), states, cfg)
    store = Store(cfg["storage"]["path"], cfg["storage"].get("persist", True))
    alerter = Alerter(cfg)

    app = build_app(states, collector, cfg)
    server = uvicorn.Server(uvicorn.Config(
        app, host=cfg["server"]["host"], port=cfg["server"]["port"],
        log_level="warning", access_log=False,
    ))

    log.info("dashboard → http://%s:%s", cfg["server"]["host"], cfg["server"]["port"])
    await asyncio.gather(
        collector.run(),
        store.run(),
        housekeeping(states, store, alerter),
        server.serve(),
    )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nstopped")
