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

from splf.feature_engine.features import compute_features_1m, resample_to_5m, save_features_parquet
from splf.utils.io import load_yaml


def _compute_one(sym: str, paths: dict, cfg: dict):
    try:
        p = Path(paths["processed_dir"]) / sym / "minute.parquet"
        if not p.exists():
            return sym, f"Missing {p}", "no_data"
        df_1m = pd.read_parquet(p)
        df_feat_1m = compute_features_1m(df_1m, sym, cfg)
        df_feat_5m = resample_to_5m(df_feat_1m)
        out = save_features_parquet(df_feat_5m, paths["features_dir"], sym)
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

    workers = int(cfg.get("runtime", {}).get("workers", 1))
    print(f"Computing features for {len(symbols)} symbols with {workers} workers…")
    if workers > 1 and len(symbols) > 1:
        with ProcessPoolExecutor(max_workers=workers) as ex:
            futs = [ex.submit(_compute_one, sym, paths, cfg) for sym in symbols]
            for fut in as_completed(futs):
                sym, msg, status = fut.result()
                if status == "ok":
                    print(f"[{sym}] Saved {msg}")
                elif status == "no_data":
                    print(f"[{sym}] Skip: {msg}")
                else:
                    print(f"[{sym}] Error: {msg}")
    else:
        for sym in symbols:
            sym, msg, status = _compute_one(sym, paths, cfg)
            if status == "ok":
                print(f"[{sym}] Saved {msg}")
            elif status == "no_data":
                print(f"[{sym}] Skip: {msg}")
            else:
                print(f"[{sym}] Error: {msg}")


if __name__ == "__main__":
    main()
