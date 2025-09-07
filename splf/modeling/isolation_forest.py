from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest as SkIsolationForest
from sklearn.preprocessing import RobustScaler

try:
    import cupy as cp  # type: ignore
    from cuml.ensemble import IsolationForest as CuIsolationForest  # type: ignore
    _HAS_CUML = True
except Exception:  # pragma: no cover
    _HAS_CUML = False


FEATURE_COLUMNS_DEFAULT = [
    # price/basis
    "basis_now",
    "basis_TWAP_60m",
    "basis_TWAP_120m",
    "basis_minus_fundTWAP",
    # flow
    "cvd_perp_5m",
    "cvd_perp_15m",
    "cvd_spot_5m",
    "cvd_spot_15m",
    "perp_share_60m",
    "dperp_share_60m",
    # liquidity & vol
    "spread_bps",
    "rv_15m",
]


@dataclass
class IFConfig:
    contamination: float = 0.04
    random_state: int = 42
    n_estimators: int = 200
    max_samples: str | int = "auto"


class IFModel:
    def __init__(self, features: Optional[List[str]] = None, config: Optional[IFConfig] = None, backend: str = "auto"):
        self.features = features or FEATURE_COLUMNS_DEFAULT
        self.config = config or IFConfig()
        self.scaler = RobustScaler()
        self.backend = backend  # 'auto' | 'sklearn' | 'cuml'
        self._use_cuml = (_HAS_CUML and backend in ("auto", "cuml"))
        if self._use_cuml:
            # cuML model will be created lazily to avoid GPU init in CPU paths
            self.model = None
        else:
            self.model = SkIsolationForest(
                n_estimators=self.config.n_estimators,
                contamination=self.config.contamination,
                max_samples=self.config.max_samples,
                random_state=self.config.random_state,
                n_jobs=-1,
            )

    def _select(self, df: pd.DataFrame) -> pd.DataFrame:
        cols = [c for c in self.features if c in df.columns]
        X = df[cols].replace([np.inf, -np.inf], np.nan).fillna(0.0)
        return X

    def fit(self, df: pd.DataFrame) -> "IFModel":
        X = self._select(df)
        Xs = self.scaler.fit_transform(X)
        if self._use_cuml:
            # Create cuML model on demand
            self.model = CuIsolationForest(
                n_estimators=self.config.n_estimators,
                contamination=self.config.contamination,
                random_state=self.config.random_state,
            )
            X_gpu = cp.asarray(Xs)
            self.model.fit(X_gpu)
        else:
            self.model.fit(Xs)
        return self

    def score(self, df: pd.DataFrame) -> pd.Series:
        X = self._select(df)
        Xs = self.scaler.transform(X)
        if self._use_cuml:
            X_gpu = cp.asarray(Xs)
            raw = None
            # Try decision_function → higher means more normal in sklearn; we invert later
            try:
                dfun = self.model.decision_function(X_gpu)  # type: ignore[attr-defined]
                raw = -cp.asnumpy(dfun)
            except Exception:
                try:
                    scores = self.model.score_samples(X_gpu)  # type: ignore[attr-defined]
                    raw = -cp.asnumpy(scores)
                except Exception:
                    preds = self.model.predict(X_gpu)
                    # cuML returns 1 for inliers, -1 for outliers; map to {0,1}
                    raw = (cp.asnumpy(preds) == -1).astype(float)
        else:
            # Higher is more anomalous → invert decision function
            raw = -self.model.decision_function(Xs)
        return pd.Series(raw, index=df.index, name="if_score")
