from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple, Union

import pandas as pd
from tqdm.auto import tqdm

from .utils.io import ensure_dir, load_yaml, save_json
from .data_handler.downloader import BinanceDownloader
from .data_handler.minute_builder import build_minute_frame, save_minute_parquet
from .feature_engine.features import compute_features_1m, resample_to_5m, save_features_parquet
from .backtesting.runner import BacktestConfig, run_walk_forward
from .backtesting.labeling import compute_explosion_labels
from .backtesting.metrics import compute_metrics


def _symbols_from_cfg(cfg: dict) -> List[str]:
    uni = cfg.get("universe", {})
    return uni.get("symbols") or (uni.get("tier_a", []) + uni.get("tier_b", []) + uni.get("tier_c", []))


@dataclass
class SPLFNotebook:
    """Convenience API for running the SPLF backtest pipeline from Jupyter notebooks.

    Example
    -------
    nb = SPLFNotebook("config/config.yaml")
    nb.download()  # optional, large
    df1m = nb.build_minute(["BTCUSDT"], return_df=True)["BTCUSDT"]
    feats = nb.features(["BTCUSDT"], return_df=True)["BTCUSDT"]
    alerts = nb.backtest(["BTCUSDT"]) ["BTCUSDT"]
    metrics, outcomes = nb.analyze(["BTCUSDT"]) 
    """

    config: Union[str, dict]

    def __post_init__(self):
        self.cfg = load_yaml(self.config) if isinstance(self.config, (str, Path)) else dict(self.config)
        self.paths = self.cfg["paths"]
        self.symbols = _symbols_from_cfg(self.cfg)
        self.horizons = self.cfg.get("backtest", {}).get("horizons_min", [30, 60, 90, 120])

    # ------------------------
    # Data download
    # ------------------------
    def download(self, symbols: Optional[List[str]] = None, datasets: Optional[List[str]] = None, force: Optional[bool] = None, workers: Optional[int] = None) -> pd.DataFrame:
        sym_list = symbols or self.symbols
        ds_list = datasets or [k for k, v in self.cfg.get("datasets", {}).items() if v]
        period = self.cfg["period"]
        dl = BinanceDownloader(self.paths["raw_dir"], workers=workers or self.cfg.get("runtime", {}).get("workers", 4))
        tasks = dl.plan(sym_list, period["start"], period["end"], ds_list)
        results = dl.download(tasks, force=(force if force is not None else self.cfg.get("runtime", {}).get("force", False)))
        df = pd.DataFrame({"dest": [str(p) for p, _ in results], "ok": [ok for _, ok in results]})
        return df

    # ------------------------
    # Minute bars
    # ------------------------
    def build_minute(self, symbols: Optional[List[str]] = None, return_df: bool = False) -> Dict[str, Union[Path, pd.DataFrame]]:
        sym_list = symbols or self.symbols
        period = self.cfg["period"]
        include_spot = self.cfg.get("datasets", {}).get("spot_aggTrades", False)
        spot_for = set(self.cfg.get("features", {}).get("spot_for", []))
        outputs: Dict[str, Union[Path, pd.DataFrame]] = {}
        for sym in tqdm(sym_list, desc="Minute bars"):
            df = build_minute_frame(self.paths["raw_dir"], sym, period["start"], period["end"], include_spot=include_spot and (sym in spot_for))
            if df.empty:
                outputs[sym] = pd.DataFrame()
                continue
            out = save_minute_parquet(df, self.paths["processed_dir"], sym)
            outputs[sym] = df if return_df else out
        return outputs

    # ------------------------
    # Features
    # ------------------------
    def features(self, symbols: Optional[List[str]] = None, return_df: bool = False) -> Dict[str, Union[Path, pd.DataFrame]]:
        sym_list = symbols or self.symbols
        outputs: Dict[str, Union[Path, pd.DataFrame]] = {}
        for sym in tqdm(sym_list, desc="Features"):
            p = Path(self.paths["processed_dir"]) / sym / "minute.parquet"
            if not p.exists():
                outputs[sym] = pd.DataFrame()
                continue
            df_1m = pd.read_parquet(p)
            df_feat_1m = compute_features_1m(df_1m, sym, self.cfg)
            df_feat_5m = resample_to_5m(df_feat_1m)
            out = save_features_parquet(df_feat_5m, self.paths["features_dir"], sym)
            outputs[sym] = df_feat_5m if return_df else out
        return outputs

    # ------------------------
    # Backtest
    # ------------------------
    def backtest(self, symbols: Optional[List[str]] = None) -> Dict[str, pd.DataFrame]:
        sym_list = symbols or self.symbols
        cfgbt = self.cfg.get("backtest", {})
        bt_cfg = BacktestConfig(
            train_window_days=int(cfgbt.get("train_window_days", 30)),
            retrain_every_hours=int(cfgbt.get("retrain_every_hours", 8)),
            score_qtile=float(cfgbt.get("score_qtile", 0.98)),
            prealert_consecutive_mins=int(cfgbt.get("prealert_consecutive_mins", 2)),
            confirm_bars_5m=int(cfgbt.get("confirm_bars_5m", 1)),
            mask_funding_minutes=int(cfgbt.get("mask_funding_minutes", 10)),
        )
        outputs: Dict[str, pd.DataFrame] = {}
        out_dir = ensure_dir(Path(self.paths["artifacts_dir"]) / "alerts")
        for sym in tqdm(sym_list, desc="Backtest"):
            p_min = Path(self.paths["processed_dir"]) / sym / "minute.parquet"
            p_feat = Path(self.paths["features_dir"]) / sym / "features_5m.parquet"
            if not p_min.exists() or not p_feat.exists():
                outputs[sym] = pd.DataFrame()
                continue
            df_1m = pd.read_parquet(p_min)
            df_5m = pd.read_parquet(p_feat)
            alerts = run_walk_forward(df_1m, df_5m, sym, bt_cfg)
            alerts.to_csv(out_dir / f"{sym}.csv", index=False)
            outputs[sym] = alerts
        return outputs

    # ------------------------
    # Analysis
    # ------------------------
    def analyze(self, symbols: Optional[List[str]] = None) -> Tuple[Dict[str, Dict[str, float]], pd.DataFrame]:
        sym_list = symbols or self.symbols
        horizons = self.horizons
        all_alerts = []
        all_outcomes = []
        for sym in tqdm(sym_list, desc="Analyze"):
            p_alerts = Path(self.paths["artifacts_dir"]) / "alerts" / f"{sym}.csv"
            p_min = Path(self.paths["processed_dir"]) / sym / "minute.parquet"
            if not p_alerts.exists() or not p_min.exists():
                continue
            alerts = pd.read_csv(p_alerts, parse_dates=["ts"])
            price_1m = pd.read_parquet(p_min)["perp_mark"].fillna(method="ffill").fillna(method="bfill")
            outcomes = compute_explosion_labels(price_1m, alerts, horizons)
            all_alerts.append(alerts)
            all_outcomes.append(outcomes)

        if not all_alerts:
            return {}, pd.DataFrame()
        alerts_df = pd.concat(all_alerts)
        outcomes_df = pd.concat(all_outcomes)
        metrics = compute_metrics(alerts_df, outcomes_df, horizons)
        out_dir = ensure_dir(Path(self.paths["artifacts_dir"]) / "metrics")
        save_json(out_dir / "metrics.json", metrics)
        return metrics, outcomes_df

