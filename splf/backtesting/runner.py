from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

from ..modeling.isolation_forest import IFModel


@dataclass
class BacktestConfig:
    train_window_days: int = 30
    retrain_every_hours: int = 8
    score_qtile: float = 0.98
    prealert_consecutive_mins: int = 2
    confirm_bars_5m: int = 1
    mask_funding_minutes: int = 10
    model_backend: str = "auto"  # 'auto' | 'sklearn' | 'cuml'


def rolling_windows(df_5m: pd.DataFrame, cfg: BacktestConfig):
    step = f"{cfg.retrain_every_hours}H"
    idx = df_5m.index
    if len(idx) == 0:
        return
    start = idx[0]
    end = idx[-1]
    cur = start
    one_day = pd.Timedelta(days=cfg.train_window_days)
    while cur <= end:
        train_end = cur
        train_start = train_end - one_day
        score_end = cur + pd.Timedelta(hours=cfg.retrain_every_hours)
        train_slice = df_5m[(df_5m.index > train_start) & (df_5m.index <= train_end)]
        score_slice = df_5m[(df_5m.index > train_end) & (df_5m.index <= score_end)]
        if not train_slice.empty and not score_slice.empty:
            yield train_slice, score_slice
        cur = score_end


def threshold_from_scores(scores: pd.Series, days: int = 14, q: float = 0.98) -> float:
    if scores.empty:
        return float("nan")
    recent = scores.iloc[-(days * 24 * 12) :]  # 14 days at 5m bars
    return float(np.quantile(recent.values, q))


def derive_leader_state(row: pd.Series) -> str:
    votes = 0
    # basis sign vote: positive basis → perp-led
    if not np.isnan(row.get("basis_now", np.nan)):
        votes += 1 if row["basis_now"] > 0 else -1
    # funding slope proxy: use premium TWAP slope if available
    prem = row.get("premium_TWAP_120m", np.nan)
    prem60 = row.get("premium_TWAP_60m", np.nan)
    if not np.isnan(prem) and not np.isnan(prem60):
        votes += 1 if (prem - prem60) > 0 else -1
    # cvd diff
    cvd_spot = row.get("cvd_spot_15m", np.nan)
    cvd_perp = row.get("cvd_perp_15m", np.nan)
    if not np.isnan(cvd_spot) and not np.isnan(cvd_perp):
        diff = cvd_spot - cvd_perp
        votes += -1 if diff > 0 else 1
    # delta perp share
    dshare = row.get("dperp_share_60m", np.nan)
    if not np.isnan(dshare):
        votes += 1 if dshare > 0 else -1

    if votes >= 2:
        return "perp-led"
    elif votes <= -2:
        return "spot-led"
    elif abs(votes) == 1:
        return "divergence"
    else:
        return "confluence"


def run_walk_forward(df_1m: pd.DataFrame, df_5m: pd.DataFrame, symbol: str, cfg: BacktestConfig) -> pd.DataFrame:
    # Fit on 5m bars, score 5m bars, then propagate to 1m grid
    model = IFModel(backend=cfg.model_backend)
    scores_5m = []
    train_hist_scores = []
    for train, score in rolling_windows(df_5m, cfg):
        model.fit(train)
        s_train = model.score(train)
        train_hist_scores.append(s_train)
        s = model.score(score)
        s.name = "if_score"
        s_df = s.to_frame()
        # leader/state at 5m resolution
        ls = score.apply(derive_leader_state, axis=1)
        s_df["leader_state"] = ls.values
        scores_5m.append(s_df)

    if not scores_5m:
        return pd.DataFrame()

    scores_5m = pd.concat(scores_5m).sort_index()
    # Threshold from recent scores
    hist = pd.concat(train_hist_scores) if train_hist_scores else pd.Series(dtype=float)
    thr = threshold_from_scores(hist, days=14, q=cfg.score_qtile)
    scores_5m["threshold"] = thr
    scores_5m["storm_raw"] = scores_5m["if_score"] >= thr

    # Map 5m scores to 1m by forward-fill
    aligned = df_1m.join(scores_5m[["if_score", "threshold", "storm_raw", "leader_state"]].resample("1T").ffill(), how="left")
    aligned["storm_raw"] = aligned["storm_raw"].fillna(False)

    # Persistence logic: pre-alert requires consecutive 1m >= thr
    above = aligned["if_score"] >= aligned["threshold"]
    pre = above.rolling(cfg.prealert_consecutive_mins, min_periods=cfg.prealert_consecutive_mins).apply(lambda x: 1.0 if x.all() else 0.0)
    aligned["pre_alert"] = pre == 1.0

    # Confirm after N closed 5m bars still above
    # Create 5m bar closes
    closes_5m = aligned.index.floor("5T").to_series().shift() != aligned.index.floor("5T").to_series()
    bar_close_idx = aligned.index[closes_5m.fillna(False)]

    confirm = pd.Series(False, index=aligned.index)
    pre_idx = aligned.index[aligned["pre_alert"].fillna(False)]
    for t0 in pre_idx:
        t_confirm = t0 + pd.Timedelta(minutes=5 * cfg.confirm_bars_5m)
        if t_confirm in aligned.index:
            confirm.loc[t_confirm] = above.loc[t_confirm]
    aligned["storm"] = confirm

    # Build alerts
    alerts = aligned.loc[aligned["storm"].fillna(False), ["if_score", "threshold", "leader_state"]].copy()
    alerts["symbol"] = symbol
    alerts["ts"] = alerts.index
    alerts = alerts.reset_index(drop=True)
    return alerts
