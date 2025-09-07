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

from splf.backtesting.runner import BacktestConfig, run_walk_forward
from splf.utils.io import ensure_dir, load_yaml


def _run_one(sym: str, paths: dict, bt_cfg: BacktestConfig):
    try:
        p_min = Path(paths["processed_dir"]) / sym / "minute.parquet"
        p_feat = Path(paths["features_dir"]) / sym / "features_5m.parquet"
        if not p_min.exists() or not p_feat.exists():
            return sym, f"Missing inputs: {p_min} or {p_feat}", "no_data"
        df_1m = pd.read_parquet(p_min)
        df_5m = pd.read_parquet(p_feat)
        alerts = run_walk_forward(df_1m, df_5m, sym, bt_cfg)
        out_dir = Path(paths["artifacts_dir"]) / "alerts"
        out_dir.mkdir(parents=True, exist_ok=True)
        out = out_dir / f"{sym}.csv"
        alerts.to_csv(out, index=False)
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

    bt_cfg = BacktestConfig(
        train_window_days=cfg.get("backtest", {}).get("train_window_days", 30),
        retrain_every_hours=cfg.get("backtest", {}).get("retrain_every_hours", 8),
        score_qtile=float(cfg.get("backtest", {}).get("score_qtile", 0.98)),
        prealert_consecutive_mins=int(cfg.get("backtest", {}).get("prealert_consecutive_mins", 2)),
        confirm_bars_5m=int(cfg.get("backtest", {}).get("confirm_bars_5m", 1)),
        mask_funding_minutes=int(cfg.get("backtest", {}).get("mask_funding_minutes", 10)),
        model_backend=cfg.get("model", {}).get("backend", "auto"),
    )

    # Auto-detect reasonable workers if not specified or set to 0
    cfg_workers = cfg.get("runtime", {}).get("workers")
    if cfg_workers in (None, 0, "auto"):
        workers = max(1, min(len(symbols), (os.cpu_count() or 1)))
    else:
        workers = int(cfg_workers)
    print(f"Backtesting {len(symbols)} symbols with {workers} workers (backend={bt_cfg.model_backend})…")
    if workers > 1 and len(symbols) > 1:
        with ProcessPoolExecutor(max_workers=workers) as ex:
            futs = [ex.submit(_run_one, sym, paths, bt_cfg) for sym in symbols]
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
            sym, msg, status = _run_one(sym, paths, bt_cfg)
            if status == "ok":
                print(f"[{sym}] Saved {msg}")
            elif status == "no_data":
                print(f"[{sym}] Skip: {msg}")
            else:
                print(f"[{sym}] Error: {msg}")


if __name__ == "__main__":
    main()
