import asyncio, json, threading, time, urllib.request
from msl_flow import config
from msl_flow.metrics import SymbolState
from msl_flow.collectors.bybit import BybitCollector
from msl_flow.server import build_app
import uvicorn

cfg = config.load(); cfg["symbols"] = ["BTCUSDT"]
states = {"BTCUSDT": SymbolState("BTCUSDT", cfg)}
c = BybitCollector(states, cfg)
c._dispatch({"topic":"orderbook.500.BTCUSDT","type":"snapshot","ts":0,
  "data":{"s":"BTCUSDT","u":1,"seq":1,"b":[["100","5"]],"a":[["101","5"]]}})
states["BTCUSDT"].sample(time.time())

app = build_app(states, c, cfg)
srv = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=8799, log_level="error"))
threading.Thread(target=srv.run, daemon=True).start()
time.sleep(2.5)

for path in ["/health", "/api/snapshot", "/"]:
    with urllib.request.urlopen(f"http://127.0.0.1:8799{path}", timeout=5) as r:
        body = r.read()
        print(f"  {path:<15} HTTP {r.status}  {len(body):>6} bytes")
        assert r.status == 200
        if path == "/api/snapshot":
            d = json.loads(body)
            assert d["symbols"]["BTCUSDT"]["mid"] == 100.5
            assert "health" in d
            print(f"                   mid={d['symbols']['BTCUSDT']['mid']} "
                  f"imbalance={d['symbols']['BTCUSDT']['imbalance']}")
        if path == "/":
            assert b"MSL FLOW" in body and b"Depth heatmap" in body
srv.should_exit = True
print("\nserver surface OK")
