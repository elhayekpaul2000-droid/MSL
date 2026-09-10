"""Local dashboard and JSON API.

Binds to 127.0.0.1 by default. Nothing here authenticates anything, so
do not move it to 0.0.0.0 on a machine other people can reach.
"""

from __future__ import annotations

import asyncio
import json
import logging
import pathlib

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse

log = logging.getLogger("msl.server")
STATIC = pathlib.Path(__file__).resolve().parent / "static"


def build_app(states: dict, collector, cfg: dict) -> FastAPI:
    app = FastAPI(title="MSL Flow", docs_url=None, redoc_url=None)

    def payload() -> dict:
        return {
            "health": collector.health(),
            "symbols": {sym: st.snapshot() for sym, st in states.items()},
        }

    @app.get("/")
    async def index():
        return FileResponse(STATIC / "index.html")

    @app.get("/health")
    async def health():
        return JSONResponse(collector.health())

    @app.get("/api/snapshot")
    async def snapshot():
        return JSONResponse(payload())

    @app.websocket("/ws")
    async def feed(ws: WebSocket):
        await ws.accept()
        try:
            while True:
                await ws.send_text(json.dumps(payload(), default=float))
                await asyncio.sleep(1.0)
        except (WebSocketDisconnect, ConnectionError):
            pass
        except Exception as exc:
            log.debug("websocket client dropped: %s", exc)

    return app
