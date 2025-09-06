SPLF Offline Backtest (Binance UM)
=================================

This repository implements an offline backtesting pipeline for the SPLF (StormComing + Leader/State) model using Binance public daily dumps (USDⓈ-M futures), based on the two specifications in this repo.

Pipeline
--------
- Data download (Binance Vision daily dumps)
- 1-minute resampling & alignment
- Feature engineering (5m features refreshed every 1m)
- Walk-forward Isolation Forest backtest with persistence
- Outcome labeling and metrics

Quick Start
-----------
1) Install deps

    python -m venv .venv && source .venv/bin/activate
    pip install -r requirements.txt

2) Configure

    cp config/example.config.yaml config/config.yaml
    # Edit symbols, date range, and paths as needed

3) Download raw data (optional large)

    python scripts/download_data.py --config config/config.yaml

4) Build minute bars

    python scripts/build_minute_bars.py --config config/config.yaml

5) Compute features

    python scripts/compute_features.py --config config/config.yaml

6) Run backtest

    python scripts/run_backtest.py --config config/config.yaml

7) Analyze results

    python scripts/analyze_results.py --config config/config.yaml

Artifacts
---------
- data/raw/{SYMBOL}/...             Raw zip files
- data/processed/{SYMBOL}/minute.parquet
- data/features/{SYMBOL}/features_5m.parquet
- artifacts/alerts/{SYMBOL}.csv
- artifacts/metrics/metrics.json

Notes
-----
- Open Interest and Liquidations are optional and not required for the core backtest.
- This pipeline is optimized for research and can be driven from scripts or notebooks.

Notebook Usage
--------------
Use the notebook-friendly helper to run steps interactively and get DataFrames back:

    from splf.notebook import SPLFNotebook
    nb = SPLFNotebook("config/config.yaml")
    # Optional: download data (large)
    # nb.download()
    
    # Build 1m bars and compute features
    df1m = nb.build_minute(["BTCUSDT"], return_df=True)["BTCUSDT"]
    feats5m = nb.features(["BTCUSDT"], return_df=True)["BTCUSDT"]
    
    # Run backtest and analyze
    alerts = nb.backtest(["BTCUSDT"]) ["BTCUSDT"]
    metrics, outcomes = nb.analyze(["BTCUSDT"]) 
    metrics
