# SPLF — Spot‑Perp Leader & Forecast (v1.2)
**Last updated:** 2025‑09‑06 (Asia/Ho_Chi_Minh)  
**Scope:** Coinglass **Standard** as the **sole** data provider. Raw cadence **1‑minute**; SPLF features computed on **5‑minute** bars.  
**Mission:** A microstructure radar that detects **storm‑coming** regimes and labels **who leads** (spot‑led, perp‑led) including **confluence** and **divergence**, to augment execution, risk, and PDI gating.

---

## 0) Executive Summary — Go/No‑Go
**Decision: GO.** Coinglass (Standard) covers all **P0** signals (index/spot, perp, funding now/next, OI, taker buy/sell for CVD, volume, liquidations, orderbook snapshots). 1‑minute inputs → robust 5‑minute features. Orderbook depth is snapshot‑based (~60–90s), sufficient as a liquidity proxy. Data‑quality flags must be implemented client‑side.

---

## 1) Objectives
1. **Leader/Follower:** Real‑time labeling of **spot‑led**, **perp‑led**, **confluence**, **divergence**.  
2. **Early Warning:** Raise **StormComing** when multiple micro signals align (basis momentum, funding slope, ΔOI, CVD differential, volume‑share flip, liquidity thinning).  
3. **Confirmation & Risk:** Quantify **crowding** (funding percentile), **squeeze risk** (OI + funding + liq), and **trend health**.  
4. **Ops:** 24/7, low memory/CPU; clean schemas; guardrails; repeatable backtests.

---

## 2) Definitions
- **Index (index_px):** spot basket reference (Coinglass spot/index).  
- **Perp mark/mid (perp_mark/perp_mid):** fair price; mid ≈ (bid+ask)/2 (fallback: mark/last).  
- **Basis:** `(perp − index) / index`.  
- **Funding:** long↔short transfer (now/next; 8h/4h/2h cycles).  
- **CVD:** cumulative (taker buy − taker sell), per market (spot, perp).  
- **Perp share:** futures volume share in (perp+spot).  
- **OI:** open interest.  
- **RV:** realized vol (std of 1‑minute returns over 15 minutes).  
- **Depth bands:** aggregated orderbook quantity within ±0.5% / ±1% of mid.

---

## 3) Data Inputs (Coinglass — Standard)
**Ingestion cadence:**  
- Prices (spot/index & perp): **60s** polling.  
- Funding (now & next) + history: ~**60s** updates.  
- OI: **60s**.  
- Taker buy/sell (spot & perp): **60s** (aggregated).  
- Orderbook snapshots: best bid/ask, depth ±bands: **60–90s**.  
- Liquidations: 5m/15m/1h series + near real‑time events.  
- Meta/health: client‑derived (`data_ok`, staleness, index deviation).

**Minimal ingest tables (per symbol):**
- `price_1m(ts, index_px, perp_mark|perp_mid)`  
- `funding_1m(ts, funding_now, funding_pred_next)`  
- `oi_1m(ts, oi)`  
- `taker_perp_1m(ts, taker_buy, taker_sell, vol_perp)`  
- `taker_spot_1m(ts, taker_buy, taker_sell, vol_spot)`  
- `orderbook_1m(ts, best_bid, best_ask, depth_p05, depth_p1)`  
- `liq_5m(ts, liq_long, liq_short, liq_count)`

---

## 3a) Timeframes — Ingestion, Features & Alerts
**Operating cadence**  
- **Ingest:** 1‑minute series for prices/index, funding (now/next), OI, taker buy/sell (spot & perp). Orderbook snapshots (best bid/ask, depth ±bands) every **60–90s**.  
- **Features:** computed on **5‑minute rolling bars** but **refreshed every 1 minute** (no need to wait for a 5‑minute close to update signals).  
- **Confirm windows:** slower metrics (funding slope 30–90m, perp_share 60m, ΔOI 1–4h) are used to confirm and size, not to trigger.

**Feature timeframes (summary)**
| Feature | Raw cadence | Window | Update | Role |
|---|---|---:|---|---|
| `basis_now` | 1m prices | instant (5m smoothing) | 1m | Lead (perp deviation)
| `dbasis_5m`, `dbasis_15m` | 1m | 5m / 15m | 1m | Lead (momentum)
| `basis_TWAP_60/120m` | 1m | 60m / 120m | 1m | Regime baseline
| `funding_now / pred` | ~1m | instant | 1m | Confirm / crowding
| `funding_slope_30/60/90m` | ~1m | 30–90m | 1m | Confirm direction
| `funding_pctile_30d` | ~1m | 30d | 1m | Risk (overheat)
| `doi_1h`, `doi_4h` | 1m OI | 1h / 4h | 1m | Confirm leverage build
| `cvd_*_5m` / `cvd_*_15m` | 1m taker | 5m / 15m | 1m | Lead/Confirm (flow)
| `perp_share_60m`, `dperp_share_60m` | 1m vol | 60m | 1m | Confirm regime flip
| `spread_bps` | 60–90s L2 | 5m (avg/last) | 1m | Liquidity risk
| `depth_ratio` (±0.5%/±1%) | 60–90s L2 | 5m (avg/last) | 1m | Liquidity/cascade
| `rv_15m` | 1m price | 15m | 1m | Vol regime
| `liq_long/short_15m` | 5m/events | 15m | 1m | Squeeze dynamics

**Storm detection modes**  
- **Hybrid (recommended):** Pre‑Alert when IF score ≥ **q‑tile** (e.g., q97 BTC/ETH; q98 alts) for **2–3 consecutive 1‑minute updates**. **Confirm** `StormComing=true` if score remains ≥ q‑tile after **1–2 closed 5‑minute bars** (5–10 minutes).  
- **Bar‑closed (conservative):** trigger only on **closed 5‑minute** bars; persistence ≥ **2 bars**.  
- **Aggressive (fastest):** compute 1‑minute features; persistence **3–4×1m**; must be gated by funding/ΔOI/liquidity to avoid noise.

**Masks & special times**  
- **Funding settlements:** mask **±10 minutes**; do not count persistence inside this window.  
- **Major macro prints/listings:** mask **±5–10 minutes**.  
- **Data health:** if staleness > **120s** or `index_deviation_flag=true`, suppress alerts.

**Tiered defaults**  
- **BTC/ETH:** Pre‑Alert **2–3×1m**; Confirm **1×5m** (or 2×5m if you prefer).  
- **Top‑alts:** Pre‑Alert **3–4×1m**; Confirm **2×5m**; require stronger ΔOI/perp‑share.

## 4) Feature Engineering (5‑minute bars)
**Normalization:** RobustScaler (median/MAD); winsorize outliers; ffill ≤2 minutes; beyond ⇒ `data_ok=false`.

### 4.1 Price / Index / Basis
- `basis_now = (perp − index) / index`  
- `dbasis_5m = basis_now − basis_now[t−1]`  
- `dbasis_15m = basis_now − basis_now[t−3]`  
- `basis_TWAP_60m` (mean of last 60m); `basis_TWAP_120m`  
- `basis_minus_fundTWAP = basis_now − funding_TWAP_proxy`

### 4.2 Funding
- `funding_slope_30m/60m/90m = funding_now(or pred) − funding_[30/60/90]m_ago`  
- `funding_pctile_30d` (percentile vs 30‑day history)  
- `funding_TWAP_proxy` — premium‑based per‑8h equivalent (fallback only)

### 4.3 Positioning (OI)
- `doi_1h = oi_now/oi_1h_ago − 1`  
- `doi_4h = oi_now/oi_4h_ago − 1`  
- `oi_pct_30d` (percentile vs 30‑day history)

### 4.4 Flow & Volume Share
- `cvd_perp_5m = Σ (taker_buy − taker_sell) perp over 5m`; `cvd_perp_15m = rolling(3×5m)`  
- `cvd_spot_5m`, `cvd_spot_15m` analogously  
- `cvd_diff_15m = cvd_spot_15m − cvd_perp_15m`  
- `perp_share_60m = vol_perp_60m / (vol_perp_60m + vol_spot_60m)`  
- `dperp_share_60m = perp_share_now − perp_share_60m_ago`

### 4.5 Liquidity
- `spread_bps = 10,000 × (ask − bid) / mid` (clip outliers)  
- `depth_ratio = depth_±0.5% / max(depth_±1%, ε)` (clip 0..5)

### 4.6 Volatility & Liquidations
- `rv_15m = std(log returns 1m over 15m)`  
- `liq_long_15m`, `liq_short_15m`; `liq_ratio = liq_long_15m / max(liq_short_15m, ε)`

### 4.7 Meta
- `data_ok` (bool): `now − last_update ≤ 120s` and mandatory fields present  
- `index_deviation_flag` (bool): `|perp − index|/index > θ` (θ default 0.5%)

---

## 5) Anomaly Engine — “StormComing”
- **Model:** Isolation Forest (IF).  
- **Features:** 12–14 core variables spanning basis/funding/ΔOI/CVD/share/liquidity/RV.  
- **Defaults:**  
  - BTC/ETH: `contamination=0.03`; Alts: `0.05`  
  - `n_estimators=300`, `max_samples='auto'`, `random_state=42`  
  - Train rolling **14–45 days** (5m bars), **retrain every 6–12h** (walk‑forward)  
  - Threshold: **q97–q98** on `−score_samples` over the last **7–14 days**  
  - **Persistence:** ≥2 bars (BTC/ETH), ≥3 (alts)

**Outputs:** `if_score`, `if_qtile`, `storm: boolean`.

---

## 5a) Training Cadence — No Real‑Time Training
**Inference (continuous):** every **1 minute** run features → **IF score** with the current model → update rolling **7–14‑day** score buffer → evaluate Pre‑Alert/Storm as per timeframes above. Recompute the **q‑tile threshold** (e.g., q97) dynamically from the last **7–14 days** of scores (out‑of‑sample to model fit).

**Scheduled retraining (not real‑time):**  
- **Every 6–12 hours** (default **8h**), fit **RobustScaler + IsolationForest** on a **rolling 14–45‑day** window (5‑minute bars).  
- After training, **rescore** the last **7–14 days** with the new model to warm up fresh q‑tiles, validate alert‑rate sanity, then atomically **swap** the model.

**Drift‑triggered retrain (early):** retrain ahead of schedule if any trigger fires:  
- **Score distribution drift:** KS/PSI vs prior week above threshold; median/IQR shift > **30%**.  
- **Alert‑rate spike:** Storm frequency **>2×** baseline for **>4h** (excluding masked windows).  
- **Effectiveness drop:** 7‑day hit‑rate (e.g., |ΔPx| or RV > p90 within 30–120m) down **>40%** vs 30‑day baseline.  
- **Regime switch:** persistent funding crowding (pctile ≥90%) or structural OI change.

**Versioning & ops:** persist `{model_id, trained_at, window_start, window_end, contamination, feature_set_hash}`. Stagger retrains **+10 minutes after funding settlements**.

## 6) Leader & State Labeling (one‑pass)
**Votes by sign:**  
- `perp_vote = 1[basis_now>0] + 1[funding_slope>0] + 1[doi_1h>0] + 1[cvd_perp_15m>cvd_spot_15m] + 1[dperp_share_60m>0]`  
- `spot_vote = 1[basis_now<0] + 1[funding_slope≤0] + 1[cvd_spot_15m>cvd_perp_15m] + 1[dperp_share_60m<0]`

**Labels:**  
- **Perp‑led** if `perp_vote − spot_vote ≥ 2`  
- **Spot‑led** if `spot_vote − perp_vote ≥ 2`  
- **Confluence** if `sign(CVD_spot)==sign(CVD_perp)` and `|basis_now|<5–10bps`  
- **Divergence** if `sign(CVD_spot)≠sign(CVD_perp)` or basis widens while opposite CVD does not confirm

**Context metrics:**  
- `perp_impulse = basis_now − funding_TWAP_proxy` (≫0: perp driving)  
- `funding_pctile_30d` (crowding)  
- `doi_4h` (leverage build)

---

## 7) Signal Flow & Playbook
**Proactive sequence (recommended):**  
1) **RADAR (SPLF):** `storm=true` (IF q≥97 with persistence). Record `leader`, `state`, `perp_impulse`, `funding_pctile`, `doi_1h/4h`, `spread_bps`, `depth_ratio`.  
2) **FOCUS (0–5m):** place on priority watchlist; drop if `data_ok=false` or `index_deviation_flag=true`.  
3) **CONFIRM (30–90m):** with PDI (or internal confirms):  
   - **Long:** spot‑led **or** perp‑led with `funding_pctile<80%`, `ΔOI_1h≥+3%`, basis maintained ≥2–3×5m.  
   - **Short:** perp‑led dump with funding slope ↓, basis<0, `ΔOI_1h≥+3%`.  
4) **EXECUTE:** size by context (crowding & liquidity).  
5) **MANAGE:** add if OI↑ & funding moderate; reduce if `funding_pctile≥90%` & `perp_impulse≤0`.  
6) **EXIT:** basis snaps to 0, leader flips, OI contracts as price stalls, or PDI loses threshold.

(Alternative **Reactive** mode: PDI first → SPLF gates/scale.)

---

## 8) Default Thresholds (starting points)
- **Storm:** IF q≥0.97 (BTC/ETH), 0.98 (alts); persistence 2/3 bars.  
- **Perp‑led (up):** `basis_now>0` & `funding_slope>0` & `ΔOI_1h>+3%`.  
- **Spot‑led (up):** price +1%/30–60m while funding ~0/neg; `CVD_spot ≫ CVD_perp`.  
- **Overheat:** `funding_pctile≥90%` **and** `perp_impulse≤0` ⇒ trim/avoid chase.  
- **Thin liquidity:** unusual `spread_bps` or high `depth_ratio` ⇒ caution.

---

## 9) Alerting & UI
- **Badges:** `storm`, `leader`, `state`, `crowded (funding_pctile)`, `squeeze risk (doi, liq)`, `liquidity thin (spread/depth)`.  
- **Priority watchlist:** sort by `if_qtile`, then `|perp_impulse|`.  
- **Debounce:** suppress alerts if `data_ok=false` or within masked windows (±10m around funding settlement or major macro prints).

---

## 10) Backtesting & KPIs
- **Explosion label:** |ΔPx| or RV above **p80–p90** within **T=30–120m** post‑alert.  
- **Metrics:** Precision/Recall/F1, AUC‑PR, IR (if traded), lead‑time distribution, hit‑rate by state (perp‑led/spot‑led/confluence/divergence).  
- **Tuning:** contamination (0.02–0.06), threshold q (0.97–0.99), persistence (2–3), vote gap (2–3) by asset tier.

---

## 11) Architecture & Operations
- **Polling (per symbol):**  
  Prices/Funding/OI/Taker/Liq: **60s**; Orderbook bands & best bid/ask: **60–90s**.  
- **Feature engine:** build 5m bars; robust scaling; winsorize; ring buffers.  
- **IF trainer:** retrain every **6–12h** on **14–45d** window; walk‑forward only.  
- **Buffers:** ffill ≤2 points; beyond ⇒ `data_ok=false`.  
- **Guardrails:** watchdog staleness; index deviation rule; settlement/news masks; API retry/backoff.

**Memory guide (active RAM):**  
`End‑to‑end ≈ N_symbols × Days × 288 × F_features × 4 bytes × ~4×`  
Example: **300 symbols × 30 days × 14 features** → ~**0.54 GB**.

---

## 12) I/O Schema
**Per 5m per symbol:**
```json
{
  "ts": "2025-09-06T10:25:00Z",
  "storm": true,
  "if_score": 3.12,
  "if_qtile": 0.98,
  "leader": "perp-led",
  "state": "divergence",
  "basis_now": 0.0018,
  "dbasis_5m": 0.0007,
  "funding_pctile": 0.83,
  "funding_slope_60m": 0.0002,
  "doi_1h": 0.036,
  "doi_4h": 0.085,
  "cvd_diff_15m": -125.4,
  "perp_share": 0.66,
  "dperp_share_60m": 0.11,
  "spread_bps": 4.2,
  "depth_ratio": 2.4,
  "rv_15m": 0.012,
  "liq_long_15m": 3100000,
  "liq_short_15m": 900000,
  "data_ok": true,
  "index_dev": false
}
```

---

## 13) PDI Integration (optional)
- **Proactive:** SPLF filters focus set → PDI scores & confirms.  
- **Reactive:** PDI triggers → SPLF gates/scales by leader, crowding, liquidity.

---

## 14) Roadmap (Phase‑2)
- Add **exchange WebSockets** for tick‑level CVD and full L2 (higher reactivity).  
- Per‑exchange decomposition (basis/funding/OI/CVD) to read venue leadership.  
- Macro/ETF flow context (BTC/ETH) if required.  
- Adaptive thresholds by regime (vol/liq) with online calibration.

---

## 15) Risks & Mitigations
- **API rate limits:** batch, cache, backoff, staggered schedules.  
- **Data gaps:** short ffill; set `data_ok=false` and suppress alerts beyond tolerance.  
- **Concept drift:** scheduled retrains; monitor IF score distribution; early retrain on drift.  
- **Illiquid alts:** raise q‑tile & persistence; maintain exclude/low‑priority lists.

---

## 16) Final Call
With Coinglass Standard, SPLF P0 is **operational** at 1m → 5m cadence.
It delivers **early warnings** and **leader/state** context with lightweight compute and clear guardrails. Upgrade to Phase‑2 (exchange WS) only if you later require sub‑minute microstructure fidelity.

