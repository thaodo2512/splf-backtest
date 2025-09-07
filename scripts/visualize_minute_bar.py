#!/usr/bin/env python
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import Optional, Tuple

import pandas as pd

# Make project root importable when running from scripts/
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from splf.utils.io import load_yaml


def _false_ranges(mask: pd.Series) -> list[Tuple[pd.Timestamp, pd.Timestamp]]:
    """Return contiguous ranges where mask is False as (start, end)."""
    if mask.empty:
        return []
    s = (~mask).astype(int)
    if s.sum() == 0:
        return []
    diff = s.diff().fillna(s.iloc[0])
    starts = s.index[(diff == 1)]
    ends = s.index[(diff == -1)]
    if len(ends) < len(starts):
        ends = ends.append(pd.Index([s.index[-1]]))
    return list(zip(starts, ends))


def main() -> None:
    ap = argparse.ArgumentParser(description="Visualize 1-minute bars for a symbol")
    ap.add_argument("--config", required=True)
    ap.add_argument("--symbol", required=False, help="Symbol to visualize (defaults to first in config)")
    ap.add_argument("--start", required=False, help="Optional start timestamp (YYYY-MM-DD or ISO)")
    ap.add_argument("--end", required=False, help="Optional end timestamp (YYYY-MM-DD or ISO)")
    ap.add_argument("--out", required=False, help="Output PNG path (defaults to artifacts/plots/{symbol}_minute.png)")
    ap.add_argument("--show", action="store_true", help="Show interactively")
    args = ap.parse_args()

    cfg = load_yaml(args.config)
    paths = cfg["paths"]
    uni = cfg.get("universe", {})
    symbols = args.symbol and [args.symbol] or (uni.get("symbols") or (uni.get("tier_a", []) + uni.get("tier_b", []) + uni.get("tier_c", [])))
    if not symbols:
        print("No symbols configured")
        return
    sym = symbols[0]

    p_min = Path(paths["processed_dir"]) / sym / "minute.parquet"
    if not p_min.exists():
        print(f"Missing {p_min}. Run scripts/build_minute_bars.py first.")
        return

    df = pd.read_parquet(p_min)
    if args.start or args.end:
        s = pd.to_datetime(args.start) if args.start else df.index.min()
        e = pd.to_datetime(args.end) if args.end else df.index.max()
        df = df.loc[(df.index >= s) & (df.index <= e)]

    # Compute helper series
    price_cols = [c for c in ("perp_mark", "index_px") if c in df.columns]
    basis_bps = None
    if {"perp_mark", "index_px"}.issubset(df.columns):
        with pd.option_context("mode.use_inf_as_na", True):
            basis_bps = (df["perp_mark"] - df["index_px"]) / df["index_px"] * 10000.0

    spread = df.get("spread_bps")
    flow_imb = None
    if {"taker_buy_qty", "taker_sell_qty"}.issubset(df.columns):
        flow_imb = (df["taker_buy_qty"] - df["taker_sell_qty"]).rolling("15T", min_periods=1).sum()

    # Plot
    import matplotlib.pyplot as plt

    nrows = 1 + (basis_bps is not None) + (spread is not None) + (flow_imb is not None)
    fig, axes = plt.subplots(nrows=int(nrows), ncols=1, figsize=(12, 2.5 * nrows), sharex=True)
    if nrows == 1:
        axes = [axes]
    ax_idx = 0

    # Prices
    ax = axes[ax_idx]
    for c in price_cols:
        ax.plot(df.index, df[c], label=c)
    ax.set_ylabel("Price")
    ax.legend(loc="upper left")
    ax.grid(True, alpha=0.3)
    ax_idx += 1

    # Basis
    if basis_bps is not None:
        ax = axes[ax_idx]
        ax.plot(df.index, basis_bps, color="tab:purple", label="basis_bps")
        ax.axhline(0, color="#666", linewidth=0.8)
        ax.set_ylabel("Basis (bps)")
        ax.legend(loc="upper left")
        ax.grid(True, alpha=0.3)
        ax_idx += 1

    # Spread
    if spread is not None:
        ax = axes[ax_idx]
        ax.plot(df.index, spread, color="tab:orange", label="spread_bps")
        ax.set_ylabel("Spread (bps)")
        ax.legend(loc="upper left")
        ax.grid(True, alpha=0.3)
        ax_idx += 1

    # Flow imbalance
    if flow_imb is not None:
        ax = axes[ax_idx]
        ax.plot(df.index, flow_imb, color="tab:green", label="15m CVD (perp)")
        ax.set_ylabel("Flow (qty)")
        ax.legend(loc="upper left")
        ax.grid(True, alpha=0.3)
        ax_idx += 1

    # Shade data_ok=False regions
    if "data_ok" in df.columns:
        mask = df["data_ok"].fillna(False).astype(bool)
        for ax in axes:
            for s, e in _false_ranges(mask):
                ax.axvspan(s, e, color="red", alpha=0.1)

    fig.suptitle(f"{sym} minute bars")
    fig.autofmt_xdate()
    fig.tight_layout(rect=[0, 0, 1, 0.98])

    out = Path(args.out) if args.out else Path(paths["artifacts_dir"]) / "plots" / f"{sym}_minute.png"
    out.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out, dpi=150)
    print(f"Saved plot to {out}")
    if args.show:
        plt.show()


if __name__ == "__main__":
    main()

