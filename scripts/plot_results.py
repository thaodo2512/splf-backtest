#!/usr/bin/env python
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import Optional

import json
import pandas as pd
import matplotlib.pyplot as plt

# Make project root importable when running from scripts/
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from splf.utils.io import load_yaml


def plot_metrics_bars(metrics_path: Path, out_dir: Path) -> Optional[Path]:
    if not metrics_path.exists():
        print(f"No metrics.json at {metrics_path}")
        return None
    with open(metrics_path, "r") as f:
        metrics = json.load(f)
    if not metrics:
        print("Empty metrics.json")
        return None
    # Build a small DataFrame for plotting
    rows = []
    for k, v in metrics.items():
        rows.append({"horizon": k, **v})
    df = pd.DataFrame(rows)
    if df.empty:
        return None
    fig, ax = plt.subplots(1, 1, figsize=(8, 4))
    x = range(len(df))
    width = 0.25
    ax.bar([i - width for i in x], df["precision"], width=width, label="precision")
    ax.bar(x, df["recall"], width=width, label="recall")
    ax.bar([i + width for i in x], df["f1"], width=width, label="f1")
    ax.set_xticks(list(x), df["horizon"].tolist())
    ax.set_ylim(0, 1)
    ax.set_title("Metrics by horizon")
    ax.legend()
    ax.grid(True, alpha=0.3)
    out = out_dir / "metrics_bars.png"
    fig.tight_layout()
    fig.savefig(out, dpi=150)
    plt.close(fig)
    print(f"Saved {out}")
    return out


def plot_symbol_overview(cfg: dict, symbol: str) -> Optional[Path]:
    paths = cfg["paths"]
    p_min = Path(paths["processed_dir"]) / symbol / "minute.parquet"
    p_alerts = Path(paths["artifacts_dir"]) / "alerts" / f"{symbol}.csv"
    if not p_min.exists():
        print(f"Missing {p_min}")
        return None
    df = pd.read_parquet(p_min)
    alerts = pd.read_csv(p_alerts, parse_dates=["ts"]) if p_alerts.exists() else pd.DataFrame()

    # Select a few available columns
    cols = {
        "price": [c for c in ["perp_mark", "index_px"] if c in df.columns],
        "perp_impulse": [c for c in ["perp_impulse"] if c in df.columns],
        "funding": [c for c in ["funding_now", "funding_slope_30m"] if c in df.columns],
        "oi": [c for c in ["oi", "doi_1h", "doi_4h"] if c in df.columns],
        "liq": [c for c in ["liq_long", "liq_short", "liq_count"] if c in df.columns],
    }
    # Build subplots depending on what's available
    groups = [k for k, v in cols.items() if v]
    n = max(1, len(groups))
    fig, axes = plt.subplots(n, 1, figsize=(12, 2.6 * n), sharex=True)
    if n == 1:
        axes = [axes]
    i = 0
    if cols["price"]:
        ax = axes[i]; i += 1
        for c in cols["price"]:
            ax.plot(df.index, df[c], label=c)
        ax.set_ylabel("Price"); ax.legend(loc="upper left"); ax.grid(True, alpha=0.3)
        if not alerts.empty and "perp_mark" in df.columns:
            ax.scatter(alerts["ts"], df.reindex(alerts["ts"])["perp_mark"], s=12, color="red", label="alerts")
            ax.legend(loc="upper left")
    if cols["perp_impulse"]:
        ax = axes[i]; i += 1
        for c in cols["perp_impulse"]:
            ax.plot(df.index, df[c], label=c)
        ax.set_ylabel("perp_impulse"); ax.legend(loc="upper left"); ax.grid(True, alpha=0.3)
    if cols["funding"]:
        ax = axes[i]; i += 1
        for c in cols["funding"]:
            ax.plot(df.index, df[c], label=c)
        ax.set_ylabel("funding"); ax.legend(loc="upper left"); ax.grid(True, alpha=0.3)
    if cols["oi"]:
        ax = axes[i]; i += 1
        for c in cols["oi"]:
            ax.plot(df.index, df[c], label=c)
        ax.set_ylabel("OI"); ax.legend(loc="upper left"); ax.grid(True, alpha=0.3)
    if cols["liq"]:
        ax = axes[i]; i += 1
        for c in cols["liq"]:
            ax.plot(df.index, df[c], label=c)
        ax.set_ylabel("liq"); ax.legend(loc="upper left"); ax.grid(True, alpha=0.3)

    fig.suptitle(f"{symbol} — overview")
    fig.autofmt_xdate(); fig.tight_layout(rect=[0,0,1,0.97])
    out = Path(cfg["paths"]["artifacts_dir"]) / "plots" / f"{symbol}_overview.png"
    out.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out, dpi=150)
    plt.close(fig)
    print(f"Saved {out}")
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description="Plot SPLF backtest results")
    ap.add_argument("--config", required=True)
    ap.add_argument("--symbol", help="Symbol for time-series overview (default: first in config)")
    args = ap.parse_args()

    cfg = load_yaml(args.config)
    paths = cfg["paths"]
    uni = cfg.get("universe", {})
    symbols = args.symbol and [args.symbol] or (uni.get("symbols") or (uni.get("tier_a", []) + uni.get("tier_b", []) + uni.get("tier_c", [])))
    if not symbols:
        print("No symbols configured")
        return

    # Plot metrics bars
    out_dir = Path(paths["artifacts_dir"]) / "plots"
    out_dir.mkdir(parents=True, exist_ok=True)
    plot_metrics_bars(Path(paths["artifacts_dir"]) / "metrics" / "metrics.json", out_dir)
    # Plot symbol overview
    plot_symbol_overview(cfg, symbols[0])


if __name__ == "__main__":
    main()

