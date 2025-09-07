#!/usr/bin/env python
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import pandas as pd
from concurrent.futures import ProcessPoolExecutor, as_completed

# Make project root importable when running from scripts/
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from splf.data_handler.minute_builder import build_minute_frame, save_minute_parquet
from splf.utils.io import load_yaml


def _build_one(sym: str, paths: dict, period: dict, include_spot: bool, spot_for: set):
    try:
        df = build_minute_frame(paths["raw_dir"], sym, period["start"], period["end"], include_spot=include_spot and (sym in spot_for))
        if df.empty:
            return sym, "", "no_data"
        out = save_minute_parquet(df, paths["processed_dir"], sym)
        return sym, str(out), "ok"
    except Exception as e:
        return sym, str(e), "error"


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

    workers = int(cfg.get("runtime", {}).get("workers", 1))
    spot_for = set(cfg.get("features", {}).get("spot_for", []))
    print(f"Building 1m for {len(symbols)} symbols with {workers} workers…")
    if workers > 1 and len(symbols) > 1:
        with ProcessPoolExecutor(max_workers=workers) as ex:
            futs = [ex.submit(_build_one, sym, paths, period, include_spot, spot_for) for sym in symbols]
            for fut in as_completed(futs):
                sym, msg, status = fut.result()
                if status == "ok":
                    print(f"[{sym}] Saved {msg}")
                elif status == "no_data":
                    print(f"[{sym}] No data")
                else:
                    print(f"[{sym}] Error: {msg}")
    else:
        for sym in symbols:
            print(f"Building 1m for {sym}…")
            sym, msg, status = _build_one(sym, paths, period, include_spot, spot_for)
            if status == "ok":
                print(f"[{sym}] Saved {msg}")
            elif status == "no_data":
                print(f"[{sym}] No data")
            else:
                print(f"[{sym}] Error: {msg}")


if __name__ == "__main__":
    main()
