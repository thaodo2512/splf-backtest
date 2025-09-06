from __future__ import annotations

from typing import Dict, List

import numpy as np
import pandas as pd


def compute_explosion_labels(price_1m: pd.Series, alerts: pd.DataFrame, horizons_min: List[int]) -> pd.DataFrame:
    """
    For each alert, compute whether price move magnitude or RV crosses percentile p80/p90 within T minutes.
    Uses absolute return over horizon as proxy for explosion; percentile estimated from rolling 30d history.
    """
    price = price_1m.ffill().dropna()
    ret1m = np.log(price).diff()

    rows = []
    for _, a in alerts.iterrows():
        t0 = a["ts"]
        # compute percentiles using a 30d window up to t0
        hist = ret1m.loc[:t0].iloc[-(30 * 24 * 60) :]
        base = hist.abs().rolling(60).sum()  # 60m abs move proxy
        p80 = base.quantile(0.8)
        p90 = base.quantile(0.9)

        row = {"ts": t0, "symbol": a["symbol"], "leader_state": a.get("leader_state", "")}
        for T in horizons_min:
            seg = ret1m.loc[t0 : t0 + pd.Timedelta(minutes=T)]
            move = seg.abs().sum()
            row[f"explosion_{T}m_p80"] = bool(move >= p80) if not np.isnan(p80) else False
            row[f"explosion_{T}m_p90"] = bool(move >= p90) if not np.isnan(p90) else False
        rows.append(row)
    return pd.DataFrame(rows)

