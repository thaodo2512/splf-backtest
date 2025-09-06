#!/usr/bin/env python
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import pandas as pd

# Make project root importable when running from scripts/
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from splf.data_handler.minute_builder import build_minute_frame, save_minute_parquet
from splf.utils.io import load_yaml


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    args = ap.parse_args()

    cfg = load_yaml(args.config)
    paths = cfg["paths"]
    universe = cfg["universe"]
    symbols = universe.get("symbols") or (universe.get("tier_a", []) + universe.get("tier_b", []) + universe.get("tier_c", []))
    period = cfg["period"]
    include_spot = cfg.get("datasets", {}).get("spot_aggTrades", False)

    for sym in symbols:
        print(f"Building 1m for {sym}…")
        df = build_minute_frame(paths["raw_dir"], sym, period["start"], period["end"], include_spot=include_spot and sym in cfg.get("features", {}).get("spot_for", []))
        if df.empty:
            print(f"No data for {sym}")
            continue
        out = save_minute_parquet(df, paths["processed_dir"], sym)
        print(f"Saved {out}")


if __name__ == "__main__":
    main()
