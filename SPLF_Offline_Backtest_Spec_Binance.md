# SPLF Offline Backtest — Binance Public Data (UM Perps)
**Version:** 1.0  
**Owner:** You  
**Goal:** Reproduce & evaluate SPLF (*StormComing + Leader/State*) using Binance public daily dumps (USDⓈ-M futures) — fully offline.  
**Foundation:** Mirrors the live SPLF spec (features, timeframes, gating, metrics).

---

## 0) Objectives
1. **Detectability:** How well does SPLF’s anomaly engine (Isolation Forest) anticipate explosive moves within **T = 30–120 min**?  
2. **Attribution:** Accuracy of **leader/state** labels (*spot-led / perp-led / confluence / divergence*).  
3. **Robustness:** Sensitivity to symbol tiers, regimes (vol/liquidity), and data gaps.  
4. **Cost/Scale:** Practicality of a historical run (CPU/RAM, disk), reproducible artifacts.

---

## 1) Universe & Horizon
- **Markets:** Binance **USDⓈ-M** perpetuals (UM).  
- **Symbols (tiers):**  
  - **Tier-A:** BTCUSDT, ETHUSDT (always).  
  - **Tier-B:** Top-20 by turnover (rolling).  
  - **Tier-C (optional):** Next-30 alts (liquidity-screened).  
- **Period:** Rolling **12–24 months** (bounded by storage).  
- **Session TZ:** UTC (convert to Asia/Ho_Chi_Minh for reports if desired).

---

## 2) Data Sources (Binance public dumps; offline)
> Base prefix: `https://data.binance.vision/data/futures/um/daily/…`

| SPLF need | Dataset (per symbol) | Use |
|---|---|---|
| Index price (1m) | `indexPriceKlines/1m/` | `index_px` for basis & RV |
| Mark price (1m) | `markPriceKlines/1m/` | `perp_mark` for basis |
| Premium index (1m) | `premiumIndexKlines/1m/` | `funding_TWAP_proxy`, premium dynamics |
| Perp OHLCV (1m) | `klines/1m/` | sanity, volumes |
| AggTrades (tick) | `aggTrades/` | **CVD_perp** (buy vs sell via `isBuyerMaker`) |
| bookTicker (tick) | `bookTicker/` | **spread_bps** (best bid/ask) |
| Depth snapshots | `bookDepth/` (levelled snapshots) | **depth_±bands** → **depth_ratio** |
| Funding (8h hist) | `monthly/fundingRate/` | `funding_now` (hist), slope, percentiles |
| Spot side (optional) | `/data/spot/daily/aggTrades/` | **CVD_spot**, spot volume for **perp_share** |

**Known gaps:**  
- **Open Interest (OI):** Not fully available as daily CSV in the archive. For a purely offline backtest, **omit OI features** or restrict OI usage to periods where you supplement via REST (short backrange).  
- **Liquidations:** Archive coverage is inconsistent; treat as **optional**. Backtest remains valid without it.

### 2a) Optional REST Enrichment (Binance-only backtests)
To approximate live features while staying Binance‑only, the project supports an optional REST ingestion step for historically backfillable series:
- **Funding (8h points → 1m ffill):** `funding_now`, `funding_slope_30/60/90m`.
- **Open Interest (5m series → 1m ffill):** `oi`, `doi_1h/4h`.
- **Liquidations (events → 1m/15m aggregates):** `liq_long_15m`, `liq_short_15m`, `liq_count_15m`.
Depth bands (±0.5%/±1%) are live‑oriented and excluded from offline backtests.

---

## 3) Pre-Processing & Normalization
1. **Download & decompress** per symbol & date; verify checksums.  
2. **Timestamp normalization:** Convert all to **UTC ms**; some **spot** files use **µs** — down-cast carefully.  
3. **De-dup & sort** each file; assert monotone timestamps.  
4. **Resample to 1-minute grid:**
   - **Prices:** last known value per minute (index/mark).  
   - **AggTrades → taker buys/sells/vol:** bucket by minute (sum qty & quoteQty; side via `isBuyerMaker`).  
   - **bookTicker:** keep last tick in minute → **best_bid/ask**; compute **spread_bps**; optional minute-avg.  
   - **Depth:** if available, compute notional within **±0.5%** and **±1%** of mid (per minute snapshot).  
   - **Funding:** map each event to its timestamp; forward-fill minute series for convenience.  
5. **Quality checks:** Missing-bar audit; drop days with >1% minute holes for a symbol; mark `data_ok=false` when staleness > 2 minutes.

---

## 4) Feature Engineering (offline, 5-minute bars; refreshed per 1-minute)
Mirrors live SPLF; **omit OI/Liquidations** if unavailable.

### 4.1 Price / Index / Basis
- `basis_now = (mark − index)/index`  
- `dbasis_5m` (Δ vs prior 5m), `dbasis_15m` (Δ vs prior 15m)  
- `basis_TWAP_60m`, `basis_TWAP_120m`  
- `basis_minus_fundTWAP = basis_now − funding_TWAP_proxy` (using `premiumIndexKlines` 1m → per-8h equivalent)

### 4.2 Funding (monthly fundingRate + premiumIndex)
- `funding_now` (hist), `funding_slope_30/60/90m` (interpolate between events or use predicted proxy from premium)  
- `funding_pctile_30d` (symbol-wise)

### 4.3 Flow & Share
- **Perp:** `cvd_perp_5m/15m` from **aggTrades** minute buckets.  
- **Spot (recommended for BTC/ETH):** `cvd_spot_5m/15m` from spot aggTrades.  
- `cvd_diff_15m = cvd_spot_15m − cvd_perp_15m`  
- `perp_share_60m = vol_perp_60m / (vol_perp_60m + vol_spot_60m)`; `dperp_share_60m`

### 4.4 Liquidity
- `spread_bps = 10,000 × (ask − bid)/mid` (clip outliers p99)  
- `depth_ratio = depth_±0.5% / max(depth_±1%, ε)` (if depth snapshots exist)

### 4.5 Volatility
- `rv_15m = std(log return 1m over 15m)`

### 4.6 Meta
- `data_ok`  
- `index_deviation_flag = |mark − index|/index > 0.5%`

> **Scaling:** RobustScaler (median/MAD) per symbol; winsorize to p1–p99.

---

## 5) Storm Detection & Labels (backtest)
- **Model:** Isolation Forest (IF), per symbol (Tier-A) or per tier (Tier-B/C).  
- **Train window:** rolling **30 days** (alt: 14–45d), **5-minute bars**.  
- **Retrain cadence (offline walk-forward):** every **8 hours** of simulated time.  
- **Thresholding:** compute **q-tile (q97–q98)** from last **7–14 days** of *scores* (no refit).  
- **Persistence:**  
  - **Pre-Alert:** score ≥ q-tile for **≥2 consecutive 1-minute** updates.  
  - **StormComing:** still ≥ q-tile after **1 (BTC/ETH) to 2 (alts) closed 5-minute bars**.

**Leader/State labeling:**  
- **Votes:** basis sign, funding slope, `cvd_diff` sign, Δ(perp_share).  
- **Labels:** **perp-led**, **spot-led**, **confluence**, **divergence**.

**Masks:** ±10 min around funding settlements; ±5–10 min around scheduled macro (optional calendar).

---

## 6) Outcome Labels & Metrics
### 6.1 Outcomes (per alert)
- **Explosion:** |ΔPrice| or **RV** crosses **p80/p90** within **T ∈ {30, 60, 90, 120} min** post-alert.  
- **Directionality (optional):** sign(ΔPrice) agrees with **leader**.

### 6.2 Metrics
- **Classification:** Precision/Recall/F1; AUC-PR by T horizon.  
- **Quality:** Lead-time distribution; hit-rate by **state** (perp-led/spot-led/…); false-alert density by tier/regime.  
- **Trading proxy (optional):** Information Ratio using a simple rule (enter on confirm; exit on leader flip/basis→0; fixed stop).

---

## 7) Experiments (grid)
- **Contamination:** {0.03, 0.04, 0.05} (alts higher).  
- **q-tile:** {0.97, 0.98, 0.99}.  
- **Persistence:** Pre-Alert {2,3,4}×1m; Confirm {1,2}×5m.  
- **Feature sets:** {Full}, {No OI / No Liq}, {No spot CVD}.  
- **Depth usage:** {spread only}, {spread + depth_ratio}.  
- **Universes:** Tier-A only vs A+B vs A+B+C.

---

## 8) Implementation Plan
1. **Downloader** — symbol/date planner; parallel downloads; checksum verify; retry/backoff.  
2. **Parsers** — CSV→Parquet; column typing; NaN-safe arithmetic; microsecond handling (spot).  
3. **(Optional) REST ingestion** — fundingRate, openInterestHist, allForceOrders → Parquet per symbol; join as 1m series (ffill).  
4. **Minute builder** — align mark/index/premium/aggTrades/bookTicker (+ join funding/OI/liqs); 1m buckets.  
5. **Feature engine** — build 5-minute rolling features refreshed per 1-minute step (adds funding slopes, ΔOI, liq_15m if present).  
6. **Model runner (walk-forward)** — rolling fit → score → thresholding (Pre-Alert/Storm); persistence & masks.  
7. **Labeler & scorer** — outcome labels; metrics; per-tier aggregation; confidence intervals (bootstrap).  
8. **Artifacts** — Parquet features; alert logs; metrics JSON; plots (PR curves, lead-time).

---

## 9) Assumptions & Limitations
- **OI & Liquidations:** Not guaranteed in the archive → excluded in **Minimal** feature set; results still representative (basis/funding/CVD/liquidity carry most signal).  
- **Depth:** Snapshots are sparse (not tick) → treat **depth_ratio** as a proxy; don’t over-interpret microsecond microstructure.  
- **Survivorship:** Use symbol lists per day (handle delistings/listings in planner).  
- **Clock skew:** Timestamps normalized to UTC; bar boundaries aligned to exchange minute.

---

## 10) Deliverables
- **`features_5m.parquet`** (per symbol, compressed).  
- **`alerts.csv`** (ts, symbol, storm, leader/state, scores, context).  
- **`metrics.json`** (by T horizon, by tier, by regime).  
- **Tech note**: data gaps report; parameter grid and best settings.

---

## 11) Minimal vs Enhanced Setups
- **Minimal (portable):** Basis/Funding (incl. premium TWAP), Perp CVD, Spot CVD for majors, Spread, RV.  
- **Enhanced:** Add Depth bands; attempt Liquidations if available; supplement OI via REST for a restricted backrange to sanity-check ΔOI heuristics.

---

## 12) Resource Estimate (12 mo, 20 symbols)
- **Disk:** ~60–120 GB (aggTrades + bookTicker dominate).  
- **RAM active:** ~2–4 GB (feature frames + IF state) if processed in symbol batches.  
- **Runtime:** Parallel I/O and parquet caching recommended; IF fit is light vs I/O.

---

### TL;DR
You can backtest SPLF **fully offline** on Binance UM dumps with high fidelity for the **core** signals (basis/funding/premium, CVD, spread, RV). Exclude OI/Liquidations or treat them as optional. Use **walk-forward Isolation Forest** with **q-tile thresholds & persistence**, mirror the live timeframes, and score alerts against **30–120m** explosion labels. Results port cleanly to production.
