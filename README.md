SPLF Offline Backtest (Binance UM)
=================================

This repository implements an offline backtesting pipeline for the SPLF (StormComing + Leader/State) model using Binance public daily dumps (USDⓈ-M futures), based on the two specifications in this repo.

Pipeline
--------
- Data download (Binance Vision daily dumps)
- (Optional) REST ingestion (Binance or Coinalyze) for funding/OI/liquidations
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

4) (Optional) Enrich with Binance REST (funding, OI, liquidations)

    python scripts/ingest_binance.py --config config/config.yaml

5) (Optional) Enrich with Coinalyze API (OI, funding, liquidations)

    # Put your key into .env: COINALYZE_API_KEY=xxxx
    python scripts/ingest_coinalyze.py --config config/config.yaml

6) Build minute bars

    python scripts/build_minute_bars.py --config config/config.yaml

7) Compute features

    python scripts/compute_features.py --config config/config.yaml

8) Run backtest

    python scripts/run_backtest.py --config config/config.yaml

9) Analyze results

One-Command E2E
---------------
The E2E runner loads `.env` (if present) and executes all stages with logging to `debug.log`.

    # .env should include COINALYZE_API_KEY if you use Coinalyze
    bash scripts/run_e2e.sh config/config.yaml

    python scripts/analyze_results.py --config config/config.yaml

Artifacts
---------
- data/raw/{SYMBOL}/...             Raw zip files
- data/processed/{SYMBOL}/minute.parquet
- data/features/{SYMBOL}/features_5m.parquet
 - data/ingest-binance/{SYMBOL}/oi*.parquet | funding*.parquet | *liquidation*.parquet
- artifacts/alerts/{SYMBOL}.csv
- artifacts/metrics/metrics.json

Notes
-----
- Open Interest and Liquidations are optional; if present (from Binance or Coinalyze) the pipeline auto‑derives related features (doi_*, liq_*_15m).
- Coinalyze API: 40 calls/min; honor Retry‑After on 429. 1m intraday retains ~1500–2000 points (~1–1.4 days). Use 1d interval for long backtests.
- The E2E runner loads `.env` (secrets) and prints detailed debug to `debug.log`.
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

Jupyter Setup
-------------
Run notebooks with your conda env and a registered kernel.

1) Activate env and install deps

    pyenv local miniforge3-24.11.3-2
    conda create -n splf -y  # once
    conda activate splf
    pip install -r requirements.txt
    conda install -n splf -c conda-forge ipykernel -y

2) Register the kernel

    python -m ipykernel install --user --name splf --display-name "Python (splf)"

3) Start Jupyter from the repo root and select the "Python (splf)" kernel

    jupyter lab --no-browser --port 8888

4) If you see `ModuleNotFoundError: splf`, add this as the first cell so notebooks can import the repo modules:

    from pathlib import Path
    import sys
    root = Path.cwd()
    if not (root / "splf").exists():
        root = root.parent
    sys.path.insert(0, str(root))
    print("Repo root:", root)

5) If you see a Parquet engine error in pandas, install one in the active kernel:

    %pip install pyarrow fastparquet

Container Usage
---------------
Build a portable multi-arch image (amd64/arm64) and run either the CLI or Jupyter.

1) Build

    docker build -t splf-backtest:latest .

2) Run CLI (mount repo to persist outputs)

    docker run --rm -it \
      -v "$(pwd)":/app \
      -w /app \
      splf-backtest:latest \
      bash scripts/run_e2e.sh config/config.yaml

3) Run Jupyter Lab

    docker-compose up

  Then open: http://localhost:8888 (tokenless)

Notes
- CPU-only by default; cuML on Jetson/ARM is out-of-scope for this image. If you need GPU cuML, prefer a host install with RAPIDS or build a custom image.
- Mounts ensure `data/` and `artifacts/` persist on the host.
