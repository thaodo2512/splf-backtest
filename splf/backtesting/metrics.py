from __future__ import annotations

from typing import Dict, List

import numpy as np
import pandas as pd


def precision_recall(labels: pd.Series, preds: pd.Series) -> Dict[str, float]:
    tp = int(((preds == 1) & (labels == 1)).sum())
    fp = int(((preds == 1) & (labels == 0)).sum())
    fn = int(((preds == 0) & (labels == 1)).sum())
    precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
    recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0.0
    return {"precision": precision, "recall": recall, "f1": f1, "tp": tp, "fp": fp, "fn": fn}


def compute_metrics(alerts: pd.DataFrame, outcomes: pd.DataFrame, horizons_min: List[int]) -> Dict[str, Dict[str, float]]:
    # Merge on ts & symbol
    df = alerts.merge(outcomes, on=["ts", "symbol"], how="left")
    metrics: Dict[str, Dict[str, float]] = {}
    df["alert"] = 1
    for T in horizons_min:
        for p in (80, 90):
            col = f"explosion_{T}m_p{p}"
            if col not in df.columns:
                continue
            y_true = df[col].fillna(False).astype(int)
            y_pred = df["alert"].astype(int)
            metrics[f"T{T}_p{p}"] = precision_recall(y_true, y_pred)
    return metrics

