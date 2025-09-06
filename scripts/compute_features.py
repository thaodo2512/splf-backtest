#!/usr/bin/env python
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import pandas as pd

# Make project root importable when running from scripts/
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from splf.feature_engine.features import compute_features_1m, resample_to_5m, save_features_parquet
from splf.utils.io import load_yaml


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    args = ap.parse_args()

    cfg = load_yaml(args.config)
    paths = cfg["paths"]
    universe = cfg["universe"]
    symbols = universe.get("symbols") or (universe.get("tier_a", []) + universe.get("tier_b", []) + universe.get("tier_c", []))

    for sym in symbols:
        p = Path(paths["processed_dir"]) / sym / "minute.parquet"
        if not p.exists():
            print(f"Missing minute parquet for {sym}: {p}")
            continue
        print(f"Computing features for {sym}…")
        df_1m = pd.read_parquet(p)
        df_feat_1m = compute_features_1m(df_1m, sym, cfg)
        df_feat_5m = resample_to_5m(df_feat_1m)
        out = save_features_parquet(df_feat_5m, paths["features_dir"], sym)
        print(f"Saved {out}")


if __name__ == "__main__":
    main()
