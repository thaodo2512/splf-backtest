from __future__ import annotations

import math
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import numpy as np
import pandas as pd


def _rolling_sum(s: pd.Series, window_min: int) -> pd.Series:
    return s.rolling(f"{window_min}T", min_periods=1).sum()


def _rolling_std(s: pd.Series, window_min: int) -> pd.Series:
    return s.rolling(f"{window_min}T", min_periods=max(2, window_min)).std()


def compute_features_1m(df_minute: pd.DataFrame, symbol: str, config: Dict) -> pd.DataFrame:
    """
    Compute SPLF features on 1-minute grid; features reflect 5-minute logic using rolling windows.
    Returns a DataFrame indexed by minute with engineered columns.
    """
    df = df_minute.copy()

    # Basis related
    if {"perp_mark", "index_px"}.issubset(df.columns):
        df["basis_now"] = (df["perp_mark"] - df["index_px"]) / df["index_px"]
    else:
        df["basis_now"] = np.nan

    for w in config.get("features", {}).get("basis_twap_minutes", [60, 120]):
        df[f"basis_TWAP_{w}m"] = df["basis_now"].rolling(f"{w}T", min_periods=max(2, w)).mean()

    # Premium as funding proxy
    if "premium" in df.columns:
        for w in [60, 120, 480]:
            df[f"premium_TWAP_{w}m"] = df["premium"].rolling(f"{w}T", min_periods=max(2, w)).mean()
        df["basis_minus_fundTWAP"] = df["basis_now"] - df["premium_TWAP_480m"].fillna(df["premium_TWAP_120m"])
    else:
        df["basis_minus_fundTWAP"] = np.nan

    # Flow & share
    for w in config.get("features", {}).get("cvd_windows_min", [5, 15]):
        if "taker_buy_qty" in df.columns and "taker_sell_qty" in df.columns:
            df[f"cvd_perp_{w}m"] = _rolling_sum(df["taker_buy_qty"] - df["taker_sell_qty"], w)
        else:
            df[f"cvd_perp_{w}m"] = np.nan

        if "taker_buy_qty_spot" in df.columns and "taker_sell_qty_spot" in df.columns:
            df[f"cvd_spot_{w}m"] = _rolling_sum(df["taker_buy_qty_spot"] - df["taker_sell_qty_spot"], w)
        else:
            df[f"cvd_spot_{w}m"] = np.nan

    if {"vol_perp", "vol_spot"}.issubset(df.columns):
        volp = _rolling_sum(df["vol_perp"], 60)
        vols = _rolling_sum(df["vol_spot"], 60)
        df["perp_share_60m"] = volp / (volp + vols).replace(0, np.nan)
        df["dperp_share_60m"] = df["perp_share_60m"].diff()
    else:
        df["perp_share_60m"] = np.nan
        df["dperp_share_60m"] = np.nan

    # Liquidity
    if "spread_bps" in df.columns:
        df["spread_bps"] = df["spread_bps"].clip(upper=df["spread_bps"].quantile(0.99))
    else:
        df["spread_bps"] = np.nan

    # Volatility
    if "perp_mark" in df.columns:
        ret1m = np.log(df["perp_mark"]).diff()
    elif "index_px" in df.columns:
        ret1m = np.log(df["index_px"]).diff()
    else:
        ret1m = pd.Series(index=df.index, dtype=float)

    df["rv_15m"] = ret1m.rolling("15T", min_periods=5).std()

    # Meta flags
    if {"perp_mark", "index_px"}.issubset(df.columns):
        df["index_deviation_flag"] = ((df["perp_mark"] - df["index_px"]).abs() / df["index_px"]).fillna(0) > 0.005
    else:
        df["index_deviation_flag"] = False

    return df


def resample_to_5m(df_1m: pd.DataFrame) -> pd.DataFrame:
    # Downsample to 5-minute bars, keeping last observation for non-aggregated features
    agg: Dict[str, str] = {}
    for c in df_1m.columns:
        if c.startswith("cvd_") or c.endswith("_TWAP_"):
            agg[c] = "last"
        elif c in {"taker_buy_qty", "taker_sell_qty", "vol_perp", "vol_spot"}:
            agg[c] = "sum"
        else:
            agg[c] = "last"
    df_5m = df_1m.resample("5T").agg(agg)
    return df_5m


def save_features_parquet(df: pd.DataFrame, features_dir: os.PathLike | str, symbol: str) -> Path:
    out = Path(features_dir) / symbol / "features_5m.parquet"
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out)
    return out

