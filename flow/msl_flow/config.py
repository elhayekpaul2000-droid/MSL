from __future__ import annotations

import pathlib
import yaml

DEFAULT = pathlib.Path(__file__).resolve().parent.parent / "config.yaml"


def load(path: str | pathlib.Path | None = None) -> dict:
    p = pathlib.Path(path) if path else DEFAULT
    if not p.exists():
        raise SystemExit(f"config not found: {p}")
    with p.open() as fh:
        cfg = yaml.safe_load(fh)
    cfg.setdefault("symbols", ["BTCUSDT"])
    cfg["symbols"] = [s.upper() for s in cfg["symbols"]]
    return cfg
