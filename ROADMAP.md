# Next Phase: Real System Development Plan

## Objectives
- Operate the SPLF backtest as a reliable, repeatable system (batch first, streaming later).
- Separate creation (ETL + features + modeling) from visualization and analysis.
- Support Jetson Orin (edge) and a cloud/server profile (GPU/CPU) with the same code.

## Architecture (Target)
- Ingestion/ETL (batch):
  - Downloader → Minute Builder → Feature Engine → Backtest → Metrics
  - Persist artifacts to durable storage (see Data & Storage).
- Control plane: simple scheduler now (cron or systemd timers), move to Airflow/Prefect later.
- Visualization: notebooks + lightweight CLI plots; optional dashboard (Grafana) in later phase.

## Data & Storage
- Working files (Parquet/CSV):
  - `data/processed/{SYMBOL}/minute.parquet`
  - `data/features/{SYMBOL}/features_5m.parquet`
  - `artifacts/alerts/{SYMBOL}.csv`, `artifacts/metrics/metrics.json`
- Retention: keep raw zips; prune intermediate Parquet beyond N days (configurable).
- Optional remotes (later):
  - Object store (S3/MinIO) for Parquet/CSV; Postgres/Timescale for alerts/metrics.

### Align With Product Spec v1 (Coinglass Standard)
- Migrate data source to Coinglass Standard (1‑minute cadence) with the following minimal ingest tables per symbol:
  - `price_1m(ts, index_px, perp_mark|perp_mid)`
  - `funding_1m(ts, funding_now, funding_pred_next)`
  - `oi_1m(ts, oi)`
  - `taker_perp_1m(ts, taker_buy, taker_sell, vol_perp)`
  - `taker_spot_1m(ts, taker_buy, taker_sell, vol_spot)`
  - `orderbook_1m(ts, best_bid, best_ask, depth_p05, depth_p1)`
  - `liq_5m(ts, liq_long, liq_short, liq_count)`
- Persist as compact Parquet partitioned by symbol/day; derive `data_ok`, staleness, and `index_deviation_flag` client‑side.

## Orchestration & Deploy
- Short term (edge-friendly): cron or systemd timers calling the existing scripts in order, wrapped by `scripts/run_e2e.sh`.
- Medium term: containerize with Docker/Compose and add a simple scheduler service.
- Long term: Airflow/Prefect with task-level retries and backfills.

## Performance & Scaling
- Parallelism: keep per-symbol parallelism; add per-day streaming to cap memory.
- GPU: keep GPU for modeling via cuML; do not GPU-accelerate minute build (I/O bound).
- Jetson tips: limit threads (`OMP_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1`), prefer 1–2 workers.
- Config knobs: `runtime.workers: 0|auto`, narrow `period`, disable optional datasets.

### Feature Cadence & Windows (per spec)
- Compute features on 5‑minute windows but refresh signals every 1 minute (no need to wait for bar close).
- Add/extend features: `dbasis_5m/15m`, `funding_now/pred`, `funding_slope_30/60/90m`, `funding_pctile_30d`, `doi_1h/4h`, `cvd_*_5m/15m` (spot & perp), `perp_share_60m` and delta, `spread_bps`, `depth_ratio` (±0.5%/±1%), `rv_15m`, `liq_long/short_15m`.
- Masks: funding settlement windows ±10m; macro prints (configurable); drop/flag when `data_ok=false`.

## Observability & Ops
- Logging: continue writing to `debug.log` (already wired); include per-step banners in `run_e2e.sh`.
- Health checks: add `scripts/check_env.py` to cron pre-flight; alert on failures via exit codes.
- Metrics (later): export counts/durations to a text file or Prometheus endpoint.

### Model Ops & Retraining (per spec)
- Walk‑forward Isolation Forest (RobustScaler + IF) on 5‑minute bars.
- Retrain every 6–12h on a rolling 14–45d window; after training, rescore the last 7–14d to refresh q‑tiles and sanity‑check alert rates, then atomically swap model (`model_id`, `trained_at`, `window_start/end`, `contamination`, `feature_set_hash`).
- Thresholding: compute dynamic score quantile (e.g., q97–q99) from last 7–14d of scores; asset‑tier overrides (BTC/ETH vs alts).
- Drift‑triggered retrain: triggers on score distribution drift (KS/PSI), alert‑rate spikes, effectiveness drop, or regime switches (funding crowding, structural OI change).

## Reliability & QA
- Determinism: freeze dependencies, pin config, and record seeds.
- Tests: add pytest suites for
  - Data readers (schema drift, bad rows),
  - Feature windows (shape/NaN policies),
  - Backtest windows/thresholding (small synthetic fixtures).
- Backfill: support partial reruns by day/symbol; avoid reprocessing when outputs exist unless `force`.

### Labels, Alerts, KPIs (per spec)
- Leader/State labeling (single pass, vote‑based): perp vs spot votes over `basis_now`, `funding_slope`, CVD diff, `dperp_share_60m`; label perp‑led / spot‑led / confluence / divergence; record context (`perp_impulse`, `funding_pctile_30d`, `doi_4h`).
- Storm detection modes: Hybrid (recommended) uses 1‑minute pre‑alert persistence 2–3, confirm after 1–2 closed 5‑minute bars; conservative (bar‑closed) and aggressive modes as config.
- KPIs: Precision/Recall/F1, AUC‑PR, IR (if traded), lead‑time distribution, hit‑rate by state, explosion labels (|ΔPx| or RV ≥ p80–p90 within T=30–120m).

## Security & Config
- Keep secrets out of repo; none required for Binance Vision.
- External endpoints (later): store creds in env/`.env` and read via `os.environ`.
- Document conda+pyenv setup (README) and kernel registration.

## Deliverables (Milestones)
1) Hardening (1–2 weeks)
- Per‑day streaming minute builder (constant‑memory); progress bars (done); finalize masks and `data_ok` rules.
- Add Coinglass ingest prototype with minimal tables; unify schemas with current offline path.
- Extend feature set per spec; add unit tests for windows/NaNs; CI: `pytest -q`.

2) Packaging & Orchestration (1 week)
- Package module with console scripts: `splf-ingest`, `splf-build`, `splf-features`, `splf-backtest`, `splf-analyze`.
- Dockerfile + docker-compose.yml; cron/systemd sample unit or Compose scheduled job.
- Add config profiles: Edge (Jetson) vs Server (GPU/CPU) with sensible defaults.

3) Persistence & Dashboard (1–2 weeks, optional)
- Push alerts/metrics to Postgres; simple Grafana dashboard showing storm badges, leader/state, KPIs by horizon.
- S3/MinIO sync; retention jobs for Parquet; publish model/version metadata for audit.

4) Model Ops (ongoing)
- Implement scheduled + drift‑triggered retraining; warm‑up rescoring 7–14d; safe model swap.
- Asset‑tier thresholds (BTC/ETH vs alts) via config; maintain q‑tile buffers.

## How To Run (Current)
- Full pipeline: `bash scripts/run_e2e.sh config/config.yaml`
- Environment check: `python scripts/check_env.py`
- Visualize: notebooks `01_End_to_End.ipynb` (create+visualize) and `02_Visualize_Results.ipynb` (visualize-only).

## Open Questions
- Target RPO/RTO and retention policies?
- Cloud profile: preferred storage (S3 vs. DB) and cadence (hourly/daily)?
- Alert delivery (email/Slack/Webhook) for “storm” events?
