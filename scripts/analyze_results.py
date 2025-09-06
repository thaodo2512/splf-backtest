#!/usr/bin/env python
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import pandas as pd

# Make project root importable when running from scripts/
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from splf.backtesting.labeling import compute_explosion_labels
from splf.backtesting.metrics import compute_metrics
from splf.utils.io import ensure_dir, load_yaml, save_json


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    args = ap.parse_args()

    cfg = load_yaml(args.config)
    paths = cfg["paths"]
    universe = cfg["universe"]
    symbols = universe.get("symbols") or (universe.get("tier_a", []) + universe.get("tier_b", []) + universe.get("tier_c", []))
    horizons = cfg.get("backtest", {}).get("horizons_min", [30, 60, 90, 120])

    all_alerts = []
    all_outcomes = []
    for sym in symbols:
        p_alerts = Path(paths["artifacts_dir"]) / "alerts" / f"{sym}.csv"
        p_min = Path(paths["processed_dir"]) / sym / "minute.parquet"
        if not p_alerts.exists() or not p_min.exists():
            print(f"Skip {sym}: missing alerts or minute data")
            continue
        alerts = pd.read_csv(p_alerts, parse_dates=["ts"])
        price_1m = pd.read_parquet(p_min)["perp_mark"].fillna(method="ffill").fillna(method="bfill")
        outcomes = compute_explosion_labels(price_1m, alerts, horizons)
        all_alerts.append(alerts)
        all_outcomes.append(outcomes)

    if not all_alerts:
        print("No alerts found")
        return

    alerts_df = pd.concat(all_alerts)
    outcomes_df = pd.concat(all_outcomes)
    metrics = compute_metrics(alerts_df, outcomes_df, horizons)
    out_dir = ensure_dir(Path(paths["artifacts_dir"]) / "metrics")
    save_json(out_dir / "metrics.json", metrics)
    print(f"Saved metrics to {out_dir / 'metrics.json'}")


if __name__ == "__main__":
    main()
