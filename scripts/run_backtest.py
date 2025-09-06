#!/usr/bin/env python
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import pandas as pd

# Make project root importable when running from scripts/
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from splf.backtesting.runner import BacktestConfig, run_walk_forward
from splf.utils.io import ensure_dir, load_yaml


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    args = ap.parse_args()

    cfg = load_yaml(args.config)
    paths = cfg["paths"]
    universe = cfg["universe"]
    symbols = universe.get("symbols") or (universe.get("tier_a", []) + universe.get("tier_b", []) + universe.get("tier_c", []))

    bt_cfg = BacktestConfig(
        train_window_days=cfg.get("backtest", {}).get("train_window_days", 30),
        retrain_every_hours=cfg.get("backtest", {}).get("retrain_every_hours", 8),
        score_qtile=float(cfg.get("backtest", {}).get("score_qtile", 0.98)),
        prealert_consecutive_mins=int(cfg.get("backtest", {}).get("prealert_consecutive_mins", 2)),
        confirm_bars_5m=int(cfg.get("backtest", {}).get("confirm_bars_5m", 1)),
        mask_funding_minutes=int(cfg.get("backtest", {}).get("mask_funding_minutes", 10)),
    )

    for sym in symbols:
        p_min = Path(paths["processed_dir"]) / sym / "minute.parquet"
        p_feat = Path(paths["features_dir"]) / sym / "features_5m.parquet"
        if not p_min.exists() or not p_feat.exists():
            print(f"Missing inputs for {sym}")
            continue
        print(f"Backtesting {sym}…")
        df_1m = pd.read_parquet(p_min)
        df_5m = pd.read_parquet(p_feat)
        alerts = run_walk_forward(df_1m, df_5m, sym, bt_cfg)
        out_dir = ensure_dir(Path(paths["artifacts_dir"]) / "alerts")
        out = out_dir / f"{sym}.csv"
        alerts.to_csv(out, index=False)
        print(f"Saved {out}")


if __name__ == "__main__":
    main()
