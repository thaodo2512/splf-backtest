#!/usr/bin/env python
from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests

# Make project root importable when running from scripts/
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from splf.utils.io import ensure_dir, load_yaml


BINANCE_FAPI = "https://fapi.binance.com"
BINANCE_DATA = "https://fapi.binance.com/futures/data"


def _to_ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def _daterange(start: str, end: str) -> Tuple[datetime, datetime]:
    s = datetime.fromisoformat(start).replace(tzinfo=timezone.utc)
    e = datetime.fromisoformat(end).replace(tzinfo=timezone.utc)
    return s, e


def fetch_funding(symbol: str, s: datetime, e: datetime, session: Optional[requests.Session] = None) -> pd.DataFrame:
    """Fetch funding rate history (8h points) in [s, e] inclusive."""
    sess = session or requests.Session()
    rows: List[Dict] = []
    start = s
    while start <= e:
        params = {
            "symbol": symbol,
            "startTime": _to_ms(start),
            "endTime": _to_ms(min(e, start + timedelta(days=14))),
            "limit": 1000,
        }
        r = sess.get(f"{BINANCE_FAPI}/fapi/v1/fundingRate", params=params, timeout=20)
        r.raise_for_status()
        data = r.json()
        if not data:
            break
        rows.extend(data)
        last_ms = int(data[-1]["fundingTime"])
        start = datetime.fromtimestamp(last_ms / 1000.0, tz=timezone.utc) + timedelta(milliseconds=1)
        time.sleep(0.2)
    if not rows:
        return pd.DataFrame(columns=["ts", "funding_now"]).set_index(pd.DatetimeIndex([], tz="UTC"))
    df = pd.DataFrame(rows)
    df = df[["fundingTime", "fundingRate"]].rename(columns={"fundingTime": "ts", "fundingRate": "funding_now"})
    df["ts"] = pd.to_datetime(df["ts"], unit="ms", utc=True)
    df["funding_now"] = pd.to_numeric(df["funding_now"], errors="coerce")
    df = df.dropna(subset=["ts"]).set_index("ts").sort_index()
    return df


def fetch_oi(symbol: str, s: datetime, e: datetime, session: Optional[requests.Session] = None) -> pd.DataFrame:
    """Fetch open interest history (5m points) in [s, e]. Shrinks window on HTTP/parse errors."""
    sess = session or requests.Session()
    rows: List[Dict] = []
    start = s
    window_days = 7
    while start <= e:
        endw = min(e, start + timedelta(days=window_days))
        params = {
            "symbol": symbol,
            "period": "5m",
            "startTime": _to_ms(start),
            "endTime": _to_ms(endw),
        }
        url = f"{BINANCE_DATA}/openInterestHist"
        r = sess.get(url, params=params, timeout=20, headers={"User-Agent": "splf-backtest/1.0"})
        if r.status_code != 200:
            print(f"[OI] HTTP {r.status_code} {url} params={params}")
            if window_days > 1:
                window_days = max(1, window_days // 2)
                continue
            else:
                break
        try:
            data = r.json()
        except Exception:
            txt = (r.text or "")[:200]
            print(f"[OI] Non-JSON response ({len(r.text)} bytes): {txt}…")
            if window_days > 1:
                window_days = max(1, window_days // 2)
                continue
            else:
                break
        if not data:
            start = endw + timedelta(milliseconds=1)
            continue
        rows.extend(data)
        last_ms = int(data[-1].get("timestamp") or data[-1].get("time") or _to_ms(start))
        start = datetime.fromtimestamp(last_ms / 1000.0, tz=timezone.utc) + timedelta(milliseconds=1)
        time.sleep(0.2)
    if not rows:
        return pd.DataFrame(columns=["ts", "oi"]).set_index(pd.DatetimeIndex([], tz="UTC"))
    df = pd.DataFrame(rows)
    # Normalize possible field names
    if "sumOpenInterest" in df.columns:
        oi_col = "sumOpenInterest"
    elif "openInterest" in df.columns:
        oi_col = "openInterest"
    else:
        oi_col = None
    if oi_col is None:
        return pd.DataFrame(columns=["ts", "oi"]).set_index(pd.DatetimeIndex([], tz="UTC"))
    ts_col = "timestamp" if "timestamp" in df.columns else "time"
    df = df[[ts_col, oi_col]].rename(columns={ts_col: "ts", oi_col: "oi"})
    df["ts"] = pd.to_datetime(df["ts"], unit="ms", utc=True)
    df["oi"] = pd.to_numeric(df["oi"], errors="coerce")
    df = df.dropna(subset=["ts"]).set_index("ts").sort_index()
    return df


def fetch_liquidations(symbol: str, s: datetime, e: datetime, session: Optional[requests.Session] = None) -> pd.DataFrame:
    """Fetch liquidation orders in [s, e] and return events with ts, side, price, qty. Shrinks window on HTTP/parse errors."""
    sess = session or requests.Session()
    rows: List[Dict] = []
    start = s
    window_hours = 24
    while start <= e:
        endw = min(e, start + timedelta(hours=window_hours))
        params = {
            "symbol": symbol,
            "startTime": _to_ms(start),
            "endTime": _to_ms(endw),
            "limit": 1000,
        }
        url = f"{BINANCE_FAPI}/fapi/v1/allForceOrders"
        r = sess.get(url, params=params, timeout=20, headers={"User-Agent": "splf-backtest/1.0"})
        if r.status_code != 200:
            print(f"[LIQ] HTTP {r.status_code} {url} params={params}")
            if window_hours > 1:
                window_hours = max(1, window_hours // 2)
                continue
            else:
                break
        try:
            data = r.json()
        except Exception:
            txt = (r.text or "")[:200]
            print(f"[LIQ] Non-JSON response ({len(r.text)} bytes): {txt}…")
            if window_hours > 1:
                window_hours = max(1, window_hours // 2)
                continue
            else:
                break
        if not data:
            start = endw + timedelta(milliseconds=1)
            continue
        rows.extend(data)
        last_ms = int(data[-1].get("time") or data[-1].get("updateTime") or _to_ms(start))
        start = datetime.fromtimestamp(last_ms / 1000.0, tz=timezone.utc) + timedelta(milliseconds=1)
        time.sleep(0.3)
    if not rows:
        return pd.DataFrame(columns=["ts", "side", "price", "qty"]).set_index(pd.DatetimeIndex([], tz="UTC"))
    df = pd.DataFrame(rows)
    ts_col = "time" if "time" in df.columns else "updateTime"
    df = df[[ts_col, "side", "price", "origQty"]].rename(columns={ts_col: "ts", "origQty": "qty"})
    df["ts"] = pd.to_datetime(df["ts"], unit="ms", utc=True)
    df["qty"] = pd.to_numeric(df["qty"], errors="coerce")
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df = df.dropna(subset=["ts", "qty"]).set_index("ts").sort_index()
    return df


def main() -> None:
    ap = argparse.ArgumentParser(description="Ingest Binance REST data for backtesting (funding, OI, liquidations)")
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

    ingest_cfg = cfg.get("ingest", {})
    do_funding = ingest_cfg.get("funding", True)
    do_oi = ingest_cfg.get("open_interest", True)
    do_liq = ingest_cfg.get("liquidations", True)

    for sym in symbols:
        out_dir = ensure_dir(ingest_dir / sym)
        print(f"Ingest {sym}: {s0} → {e0}")
        sess = requests.Session()
        if do_funding:
            df = fetch_funding(sym, s, e, sess)
            if not df.empty:
                df.to_parquet(out_dir / "funding.parquet")
                print(f"  funding: {len(df)} rows → {out_dir / 'funding.parquet'}")
            else:
                print("  funding: no data")
        if do_oi:
            df = fetch_oi(sym, s, e, sess)
            if not df.empty:
                df.to_parquet(out_dir / "oi.parquet")
                print(f"  oi: {len(df)} rows → {out_dir / 'oi.parquet'}")
            else:
                print("  oi: no data")
        if do_liq:
            df = fetch_liquidations(sym, s, e, sess)
            if not df.empty:
                df.to_parquet(out_dir / "liquidations.parquet")
                print(f"  liquidations: {len(df)} events → {out_dir / 'liquidations.parquet'}")
            else:
                print("  liquidations: no data")


if __name__ == "__main__":
    main()
