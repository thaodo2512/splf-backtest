# Project TODO

Instructions
- Keep this file up to date after each change. Be concise and include copy‑pastable commands. Large outputs/logs go to `debug.log` (git‑ignored).
- Align work with specs: `SPLF_Offline_Backtest_Spec_Binance.md`, `backtest_spec.md`.

## Current Goal
- What outcome are we targeting right now?

## Context / Decisions
- Key assumptions, scope decisions, and config choices (e.g., symbols, period, datasets).

## Next Steps
- [ ] Step 1
- [ ] Step 2
- [ ] Step 3

## Commands To Run
Copy/paste in order as needed (adjust config path):

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# Data → Minute → Features → Backtest → Metrics
python scripts/download_data.py --config config/config.yaml
python scripts/build_minute_bars.py --config config/config.yaml
python scripts/compute_features.py --config config/config.yaml
python scripts/run_backtest.py --config config/config.yaml
python scripts/analyze_results.py --config config/config.yaml

# (Optional) Tests
pytest -q
```

## Results & Artifacts
- Alerts: `artifacts/alerts/{SYMBOL}.csv`
- Metrics: `artifacts/metrics/metrics.json`
- Data: `data/processed/{SYMBOL}/minute.parquet`, `data/features/{SYMBOL}/features_5m.parquet`

## Open Questions / Risks
- Unknowns, blockers, performance concerns, or data quality issues.

---

## Bug‑Fix Template
Use this section when tracking a bug.

- Summary: What’s broken and where it appears.
- Repro Steps (commands + config):
  - `...`
- Evidence: Paste minimal excerpts from `debug.log` (full log stays in file).
- Suspected Module(s): `splf/...`
- Fix Plan: Minimal, targeted change + add/adjust a test.
- Verification: Command(s) used to validate the fix.

