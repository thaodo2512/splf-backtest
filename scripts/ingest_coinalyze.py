#!/usr/bin/env python
from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
import requests

# Make project root importable when running from scripts/
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from splf.utils.io import ensure_dir, load_yaml


def _to_ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def _daterange(start: str, end: str) -> tuple[datetime, datetime]:
    s = datetime.fromisoformat(start).replace(tzinfo=timezone.utc)
    e = datetime.fromisoformat(end).replace(tzinfo=timezone.utc)
    return s, e


def _detect_keys(sample: dict, ts_cands: List[str], val_cands: List[str]) -> tuple[Optional[str], Optional[str]]:
    ts_key = next((k for k in ts_cands if k in sample), None)
    v_key = next((k for k in val_cands if k in sample), None)
    return ts_key, v_key


def _get_json(sess: requests.Session, url: str, params: dict, headers: dict, min_sleep: float = 1.5) -> Optional[List[dict]]:
    r = sess.get(url, params=params, headers=headers, timeout=30)
    if r.status_code == 429:
        ra = r.headers.get("Retry-After")
        try:
            wait_s = max(min_sleep, float(ra)) if ra is not None else 2.0
        except Exception:
            wait_s = 2.0
        print(f"[COINALYZE] 429 rate limit; sleeping {wait_s}s … params={params}")
        time.sleep(wait_s)
        return _get_json(sess, url, params, headers, min_sleep)
    if r.status_code != 200:
        print(f"[COINALYZE] HTTP {r.status_code} {url} params={params} body={(r.text or '')[:200]}…")
        return None
    try:
        return r.json()
    except Exception:
        print(f"[COINALYZE] Non-JSON response ({len(r.text)} bytes): {(r.text or '')[:200]}…")
        return None


def fetch_oi(api_key: str, market: str, start: datetime, endpoint: str, interval: str) -> pd.DataFrame:
    sess = requests.Session()
    headers = {"X-API-KEY": api_key, "User-Agent": "splf-backtest/1.0"}
    params = {"market": market, "interval": interval, "startTime": _to_ms(start)}
    data = _get_json(sess, endpoint, params, headers)
    if not data:
        return pd.DataFrame(columns=["ts", "oi"]).set_index(pd.DatetimeIndex([], tz="UTC"))
    ts_key, oi_key = _detect_keys(data[0], ["timestamp", "time", "t", "ts"], ["open_interest", "oi", "openInterest", "value"])
    if not ts_key or not oi_key:
        print(f"[COINALYZE] OI: cannot detect keys in sample {list(data[0].keys())}")
        return pd.DataFrame(columns=["ts", "oi"]).set_index(pd.DatetimeIndex([], tz="UTC"))
    df = pd.DataFrame(data)[[ts_key, oi_key]].rename(columns={ts_key: "ts", oi_key: "oi"})
    df["ts"] = pd.to_datetime(df["ts"], unit="ms", utc=True)
    df["oi"] = pd.to_numeric(df["oi"], errors="coerce")
    return df.dropna(subset=["ts"]).set_index("ts").sort_index()


def fetch_funding(api_key: str, market: str, start: datetime, endpoint: str) -> pd.DataFrame:
    sess = requests.Session()
    headers = {"X-API-KEY": api_key, "User-Agent": "splf-backtest/1.0"}
    params = {"market": market, "startTime": _to_ms(start)}
    data = _get_json(sess, endpoint, params, headers)
    if not data:
        return pd.DataFrame(columns=["ts", "funding_now"]).set_index(pd.DatetimeIndex([], tz="UTC"))
    ts_key, fr_key = _detect_keys(data[0], ["timestamp", "time", "t", "ts"], ["funding_rate", "fundingRate", "value", "rate"])
    if not ts_key or not fr_key:
        print(f"[COINALYZE] Funding: cannot detect keys in sample {list(data[0].keys())}")
        return pd.DataFrame(columns=["ts", "funding_now"]).set_index(pd.DatetimeIndex([], tz="UTC"))
    df = pd.DataFrame(data)[[ts_key, fr_key]].rename(columns={ts_key: "ts", fr_key: "funding_now"})
    df["ts"] = pd.to_datetime(df["ts"], unit="ms", utc=True)
    df["funding_now"] = pd.to_numeric(df["funding_now"], errors="coerce")
    return df.dropna(subset=["ts"]).set_index("ts").sort_index()


def fetch_liqs(api_key: str, market: str, start: datetime, endpoint: str, interval: str = "1m") -> pd.DataFrame:
    sess = requests.Session()
    headers = {"X-API-KEY": api_key, "User-Agent": "splf-backtest/1.0"}
    params = {"market": market, "interval": interval, "startTime": _to_ms(start)}
    data = _get_json(sess, endpoint, params, headers)
    if not data:
        return pd.DataFrame(columns=["ts", "side", "price", "qty"]).set_index(pd.DatetimeIndex([], tz="UTC"))
    sample = data[0]
    ts_key, _ = _detect_keys(sample, ["timestamp", "time", "t", "ts"], ["_ignore"])  # second not used
    side_key = "side" if "side" in sample else None
    price_key = "price" if "price" in sample else None
    qty_key = next((k for k in ["qty", "quantity", "amount", "size", "value"] if k in sample), None)
    if not ts_key or not qty_key:
        print(f"[COINALYZE] Liqs: cannot detect keys in sample {list(sample.keys())}")
        return pd.DataFrame(columns=["ts", "side", "price", "qty"]).set_index(pd.DatetimeIndex([], tz="UTC"))
    cols = {ts_key: "ts"}
    if side_key:
        cols[side_key] = "side"
    if price_key:
        cols[price_key] = "price"
    cols[qty_key] = "qty"
    df = pd.DataFrame(data)[list(cols.keys())].rename(columns=cols)
    df["ts"] = pd.to_datetime(df["ts"], unit="ms", utc=True)
    for c in ["qty", "price"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df.dropna(subset=["ts"]).set_index("ts").sort_index()


def main() -> None:
    ap = argparse.ArgumentParser(description="Ingest Coinalyze OI/Funding/Liquidations for backtesting")
    ap.add_argument("--config", required=True)
    ap.add_argument("--symbols", nargs="*", help="Override symbols (default from config)")
    ap.add_argument("--start", help="Override period.start (YYYY-MM-DD)")
    ap.add_argument("--end", help="Override period.end (YYYY-MM-DD)")
    args = ap.parse_args()

    cfg = load_yaml(args.config)
    paths = cfg["paths"]
    ingest_dir = Path(paths.get("ingest_dir", "data/ingest-binance"))
    uni = cfg.get("universe", {})
    symbols = args.symbols or (uni.get("symbols") or (uni.get("tier_a", []) + uni.get("tier_b", []) + uni.get("tier_c", [])))
    per = cfg.get("period", {})
    s0 = args.start or per.get("start")
    e0 = args.end or per.get("end")
    if not (s0 and e0):
        raise SystemExit("Missing start/end period")
    s, e = _daterange(s0, e0)

    ing = cfg.get("ingest", {}).get("coinalyze", {})
    if not ing or not ing.get("enabled", False):
        print("Coinalyze ingest disabled (config.ingest.coinalyze.enabled=false)")
        return
    api_key_env = ing.get("api_key_env", "COINALYZE_API_KEY")
    api_key = os.environ.get(api_key_env)
    if not api_key:
        raise SystemExit(f"Missing API key env var: {api_key_env}")

    market_prefix = ing.get("market_prefix", "binance:")
    interval = ing.get("interval", "1d")
    ep_oi = ing.get("endpoint_oi", ing.get("endpoint", "https://api.coinalyze.net/v1/open-interest-history"))
    ep_fr = ing.get("endpoint_funding", "https://api.coinalyze.net/v1/funding-rate-history")
    ep_lq = ing.get("endpoint_liq", "https://api.coinalyze.net/v1/liquidation-history")

    for sym in symbols:
        market = f"{market_prefix}{sym}"
        out_dir = ensure_dir(ingest_dir / sym)
        print(f"Coinalyze ingest {sym} ({market}): {s0} → {e0}")
        # OI
        df_oi = fetch_oi(api_key, market, s, ep_oi, interval)
        if not df_oi.empty:
            df_oi.to_parquet(out_dir / "oi_coinalyze.parquet")
            print(f"  oi: {len(df_oi)} rows → {out_dir / 'oi_coinalyze.parquet'}")
        else:
            print("  oi: no data (Coinalyze)")
        # Funding
        df_fr = fetch_funding(api_key, market, s, ep_fr)
        if not df_fr.empty:
            df_fr.to_parquet(out_dir / "funding_coinalyze.parquet")
            print(f"  funding: {len(df_fr)} rows → {out_dir / 'funding_coinalyze.parquet'}")
        else:
            print("  funding: no data (Coinalyze)")
        # Liquidations
        df_lq = fetch_liqs(api_key, market, s, ep_lq, interval=("1m" if interval == "1m" else "5m"))
        if not df_lq.empty:
            df_lq.to_parquet(out_dir / "liq_coinalyze.parquet")
            print(f"  liq: {len(df_lq)} rows → {out_dir / 'liq_coinalyze.parquet'}")
        else:
            print("  liq: no data (Coinalyze)")


if __name__ == "__main__":
    main()

