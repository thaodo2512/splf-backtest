from __future__ import annotations

import io
import os
import zipfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import logging
import numpy as np
import pandas as pd
from dateutil import tz


def _read_zip_csv(path: os.PathLike | str, names: Optional[List[str]] = None, dtype=None, usecols=None) -> pd.DataFrame:
    p = Path(path)
    if not p.exists():
        return pd.DataFrame()
    with zipfile.ZipFile(p, "r") as zf:
        # choose first file
        fname = zf.namelist()[0]
        with zf.open(fname) as f:
            df = pd.read_csv(
                f,
                header=None,
                names=names,
                dtype=dtype,
                usecols=usecols,
                low_memory=False,
            )
    return df


def _to_minute_index(ts_ms: pd.Series) -> pd.DatetimeIndex:
    return pd.to_datetime(ts_ms, unit="ms", utc=True).dt.floor("min")


def _safe_float(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")


def _normalize_ts_ms(ts: pd.Series, context: str = "") -> pd.Series:
    """Heuristically normalize timestamps to milliseconds.
    Handles seconds, microseconds, or nanoseconds by inspecting magnitude.
    """
    s = pd.to_numeric(ts, errors="coerce")
    sv = s.dropna()
    if sv.empty:
        return s
    med = float(sv.median())
    # typical ms since epoch ~ 1.6e12 in 2020s
    # seconds ~ 1.6e9; microseconds ~ 1.6e15; nanoseconds ~ 1.6e18
    factor = 1.0
    if med > 1e17:
        factor = 1e6  # ns → ms
    elif med > 1e14:
        factor = 1e3  # us → ms
    elif med < 1e11:
        factor = 1e-3  # s → ms
    if factor != 1.0:
        s = s / factor
    return s


def _to_int01_isbuyer(s: pd.Series) -> pd.Series:
    """Robustly convert isBuyerMaker to {0,1} from mixed types (bool/int/str)."""
    out = pd.to_numeric(s, errors="coerce")
    mask = out.isna()
    if mask.any():
        st = s.astype(str).str.strip().str.lower()
        mapd = st.map({"true": 1, "false": 0, "1": 1, "0": 0})
        out = out.where(~mask, mapd)
    return pd.to_numeric(out, errors="coerce")


def build_minute_frame(
    raw_dir: os.PathLike | str,
    symbol: str,
    start: str,
    end: str,
    include_spot: bool = False,
    progress: bool = False,
    ingest_dir: Optional[str] = None,
) -> pd.DataFrame:
    """
    Build a unified 1-minute DataFrame from daily zip files between start and end.
    Columns: index_px, perp_mark, premium, bid, ask, spread_bps, taker_buy_qty, taker_sell_qty, vol_perp, vol_spot (opt)
    """
    logger = logging.getLogger(__name__)
    raw = Path(raw_dir) / symbol
    logger.debug(f"minute_builder.start symbol={symbol} raw_dir={raw} start={start} end={end} include_spot={include_spot}")
    ingest_base = Path(ingest_dir) if ingest_dir else Path("data/ingest-binance")

    # aggregate per day then concat
    frames: List[pd.DataFrame] = []
    date_range = pd.date_range(start=start, end=end, freq="D", tz="UTC")
    iterator = date_range
    if progress:
        try:
            from tqdm.auto import tqdm  # type: ignore
            iterator = tqdm(date_range, desc=f"{symbol} days", leave=True)
        except Exception:
            iterator = date_range
    for dt in iterator:
        date_str = dt.strftime("%Y-%m-%d")

        def p(ds: str) -> Path:
            # Build expected file path
            dir_map = {
                "klines_1m": f"klines_1m/{symbol}-1m-{date_str}.zip",
                "indexPriceKlines_1m": f"indexPriceKlines_1m/{symbol}-1m-{date_str}.zip",
                "markPriceKlines_1m": f"markPriceKlines_1m/{symbol}-1m-{date_str}.zip",
                "premiumIndexKlines_1m": f"premiumIndexKlines_1m/{symbol}-1m-{date_str}.zip",
                "aggTrades": f"aggTrades/{symbol}-aggTrades-{date_str}.zip",
                "bookTicker": f"bookTicker/{symbol}-bookTicker-{date_str}.zip",
                "spot_aggTrades": f"spot_aggTrades/{symbol}-aggTrades-{date_str}.zip",
            }
            return raw / dir_map[ds]

        # index price klines (1m): [open time, open, high, low, close, close time, ...]
        logger.debug(
            f"paths date={date_str} index={p('indexPriceKlines_1m').exists()} mark={p('markPriceKlines_1m').exists()} prem={p('premiumIndexKlines_1m').exists()} agg={p('aggTrades').exists()} bt={p('bookTicker').exists()} spot={p('spot_aggTrades').exists()}"
        )
        idx_cols = [
            "open_time",
            "open",
            "high",
            "low",
            "close",
            "close_time",
            "v1",
            "v2",
            "v3",
            "v4",
            "v5",
            "v6",
        ]
        df_index = _read_zip_csv(p("indexPriceKlines_1m"), names=idx_cols, usecols=[0, 4])
        if not df_index.empty:
            df_index.rename(columns={"open_time": "ts", "close": "index_px"}, inplace=True)
            df_index["ts"] = pd.to_numeric(df_index["ts"], errors="coerce")
            df_index["index_px"] = pd.to_numeric(df_index["index_px"], errors="coerce")
            df_index = df_index.dropna(subset=["ts"])  # drop header if present
        logger.debug(f"read index df rows={len(df_index)} date={date_str}")

        # mark price klines (1m)
        df_mark = _read_zip_csv(p("markPriceKlines_1m"), names=idx_cols, usecols=[0, 4])
        if not df_mark.empty:
            df_mark.rename(columns={"open_time": "ts", "close": "perp_mark"}, inplace=True)
            df_mark["ts"] = pd.to_numeric(df_mark["ts"], errors="coerce")
            df_mark["perp_mark"] = pd.to_numeric(df_mark["perp_mark"], errors="coerce")
            df_mark = df_mark.dropna(subset=["ts"])  # drop header if present
        logger.debug(f"read mark df rows={len(df_mark)} date={date_str}")

        # premiumIndexKlines (1m): use close as premium proxy
        df_prem = _read_zip_csv(p("premiumIndexKlines_1m"), names=idx_cols, usecols=[0, 4])
        if not df_prem.empty:
            df_prem.rename(columns={"open_time": "ts", "close": "premium"}, inplace=True)
            df_prem["ts"] = pd.to_numeric(df_prem["ts"], errors="coerce")
            df_prem["premium"] = pd.to_numeric(df_prem["premium"], errors="coerce")
            df_prem = df_prem.dropna(subset=["ts"])  # drop header if present
        logger.debug(f"read premium df rows={len(df_prem)} date={date_str}")

        # aggTrades: [id, price, qty, firstId, lastId, timestamp, isBuyerMaker]
        at_cols = [
            "aggId",
            "price",
            "qty",
            "firstId",
            "lastId",
            "ts",
            "isBuyerMaker",
        ]
        # Only read the needed columns to reduce memory footprint
        df_at = _read_zip_csv(p("aggTrades"), names=["qty", "ts", "isBuyerMaker"], usecols=[2, 5, 6])
        if not df_at.empty:
            # Coerce types and drop header rows accidentally read as data
            df_at["qty"] = pd.to_numeric(df_at["qty"], errors="coerce")  # base qty
            df_at["ts"] = _normalize_ts_ms(df_at["ts"], context="perp_aggTrades")
            df_at["isBuyerMaker"] = _to_int01_isbuyer(df_at["isBuyerMaker"])  # 1=sell (buyer is maker)
            df_at = df_at.dropna(subset=["qty", "ts", "isBuyerMaker"])  # drop header line if present
            df_at["isBuyerMaker"] = df_at["isBuyerMaker"].astype(int)
            df_at["minute"] = _to_minute_index(df_at["ts"]).astype("datetime64[ns, UTC]")
            # Aggregate without groupby.apply to avoid shape issues
            sell = df_at.loc[df_at["isBuyerMaker"] == 1].groupby("minute")["qty"].sum()
            buy = df_at.loc[df_at["isBuyerMaker"] == 0].groupby("minute")["qty"].sum()
            vol = df_at.groupby("minute")["qty"].sum()
            df_cvd = pd.concat(
                [
                    buy.rename("taker_buy_qty"),
                    sell.rename("taker_sell_qty"),
                    vol.rename("vol_perp"),
                ],
                axis=1,
            )
        else:
            df_cvd = pd.DataFrame()
        logger.debug(f"read aggTrades rows={len(df_at)} cvd_index_len={len(df_cvd.index) if not df_cvd.empty else 0} date={date_str}")

        # bookTicker: [ts, symbol, bidPrice, bidQty, askPrice, askQty] (schema may vary)
        bt_cols_variants = [
            ["ts", "symbol", "bidPrice", "bidQty", "askPrice", "askQty"],
            ["symbol", "bidPrice", "bidQty", "askPrice", "askQty", "ts"],
        ]
        df_bt = pd.DataFrame()
        bt_zip = p("bookTicker")
        if bt_zip.exists():
            with zipfile.ZipFile(bt_zip, "r") as zf:
                fname = zf.namelist()[0]
                with zf.open(fname) as f:
                    raw_bytes = f.read()
                for cols in bt_cols_variants:
                    try:
                        tmp = pd.read_csv(io.BytesIO(raw_bytes), header=None, names=cols, low_memory=False)
                        df_bt = tmp
                        logger.debug(f"read bookTicker variant={cols} rows={len(df_bt)} date={date_str}")
                        break
                    except Exception:
                        continue
        if not df_bt.empty:
            for c in ["bidPrice", "askPrice"]:
                df_bt[c] = _safe_float(df_bt[c])
            if "ts" in df_bt.columns:
                df_bt["ts"] = _normalize_ts_ms(df_bt["ts"], context="bookTicker")
                df_bt = df_bt.dropna(subset=["ts"])  # drop header if present
                df_bt.loc[:, "minute"] = _to_minute_index(df_bt["ts"]).astype("datetime64[ns, UTC]")
                df_bt = df_bt.sort_values("minute").groupby("minute").last()
            mid = (df_bt["bidPrice"] + df_bt["askPrice"]) / 2.0
            spread_bps = (df_bt["askPrice"] - df_bt["bidPrice"]) / mid * 10000.0
            df_liq = pd.DataFrame({
                "spread_bps": spread_bps
            })
        else:
            df_liq = pd.DataFrame()
        logger.debug(f"bookTicker df rows={len(df_bt)} liq_rows={len(df_liq)} date={date_str}")

        # spot aggTrades (optional)
        df_spot = pd.DataFrame()
        if include_spot and p("spot_aggTrades").exists():
            # Minimize columns for spot aggTrades as well
            df_s = _read_zip_csv(p("spot_aggTrades"), names=["qty", "ts", "isBuyerMaker"], usecols=[2, 5, 6])
            if not df_s.empty:
                df_s["qty"] = pd.to_numeric(df_s["qty"], errors="coerce")
                df_s["ts"] = _normalize_ts_ms(df_s["ts"], context="spot_aggTrades")
                df_s["isBuyerMaker"] = _to_int01_isbuyer(df_s["isBuyerMaker"])  # 1=sell (buyer is maker)
                df_s = df_s.dropna(subset=["qty", "ts", "isBuyerMaker"])  # drop header rows
                df_s["isBuyerMaker"] = df_s["isBuyerMaker"].astype(int)
                df_s.loc[:, "minute"] = _to_minute_index(df_s["ts"]).astype("datetime64[ns, UTC]")
                # Vectorized groupby aggregation (faster than groupby.apply)
                buy_s = df_s.loc[df_s["isBuyerMaker"] == 0].groupby("minute")["qty"].sum()
                sell_s = df_s.loc[df_s["isBuyerMaker"] == 1].groupby("minute")["qty"].sum()
                vol_s = df_s.groupby("minute")["qty"].sum()
                df_spot = pd.concat(
                    [
                        buy_s.rename("taker_buy_qty_spot"),
                        sell_s.rename("taker_sell_qty_spot"),
                        vol_s.rename("vol_spot"),
                    ],
                    axis=1,
                )

        # Merge all by minute index
        if not df_index.empty:
            df_index = df_index.copy()
            df_index["minute"] = _to_minute_index(df_index["ts"]).astype("datetime64[ns, UTC]")
            df_index.drop(columns=["ts"], inplace=True)
            df_index.set_index("minute", inplace=True)
        if not df_mark.empty:
            df_mark = df_mark.copy()
            df_mark["minute"] = _to_minute_index(df_mark["ts"]).astype("datetime64[ns, UTC]")
            df_mark.drop(columns=["ts"], inplace=True)
            df_mark.set_index("minute", inplace=True)
        if not df_prem.empty:
            df_prem = df_prem.copy()
            df_prem["minute"] = _to_minute_index(df_prem["ts"]).astype("datetime64[ns, UTC]")
            df_prem.drop(columns=["ts"], inplace=True)
            df_prem.set_index("minute", inplace=True)

        df_day = pd.DataFrame(index=pd.date_range(dt, dt + pd.Timedelta(days=1) - pd.Timedelta(minutes=1), freq="min", tz="UTC"))
        if not df_index.empty:
            df_day = df_day.join(df_index[["index_px"]], how="left")
        if not df_mark.empty:
            df_day = df_day.join(df_mark[["perp_mark"]], how="left")
        if not df_prem.empty:
            df_day = df_day.join(df_prem[["premium"]], how="left")
        if not df_cvd.empty:
            df_day = df_day.join(df_cvd, how="left")
        if not df_liq.empty:
            df_day = df_day.join(df_liq, how="left")
        if not df_spot.empty:
            df_day = df_day.join(df_spot, how="left")
        logger.debug(f"df_day date={date_str} shape={df_day.shape} cols={list(df_day.columns)}")
        frames.append(df_day)

    if not frames:
        return pd.DataFrame()

    df = pd.concat(frames).sort_index()
    logger.debug(f"concat frames total_shape={df.shape}")
    # forward fill prices for continuous series
    for c in ["index_px", "perp_mark", "premium"]:
        if c in df.columns:
            df[c] = df[c].ffill()

    df["data_ok"] = True
    # mark data_ok false if last price stale > 2 minutes
    valid_price = df[[c for c in ["index_px", "perp_mark"] if c in df.columns]].notna().any(axis=1)
    last_ts = df.index.to_series().where(valid_price).ffill()
    staleness = (df.index.to_series() - last_ts).dt.total_seconds() / 60.0
    stale_cnt = int((staleness > 2).sum())
    df.loc[staleness > 2, "data_ok"] = False
    logger.debug(f"final symbol={symbol} shape={df.shape} stale_minutes={stale_cnt}")

    # Enrich from REST ingestion if available
    try:
        ing_sym_dir = ingest_base / symbol
        if (ing_sym_dir / "funding.parquet").exists():
            f = pd.read_parquet(ing_sym_dir / "funding.parquet").sort_index()
            f1m = f.reindex(df.index, method="ffill").rename(columns={"funding_now": "funding_now"})
            df = df.join(f1m[["funding_now"]], how="left")
        elif (ing_sym_dir / "funding_coinalyze.parquet").exists():
            f = pd.read_parquet(ing_sym_dir / "funding_coinalyze.parquet").sort_index()
            f1m = f.reindex(df.index, method="ffill").rename(columns={"funding_now": "funding_now"})
            df = df.join(f1m[["funding_now"]], how="left")
        oi_path = None
        if (ing_sym_dir / "oi.parquet").exists():
            oi_path = ing_sym_dir / "oi.parquet"
        elif (ing_sym_dir / "oi_coinalyze.parquet").exists():
            oi_path = ing_sym_dir / "oi_coinalyze.parquet"
        if oi_path is not None:
            oi = pd.read_parquet(oi_path).sort_index()
            oi1m = oi.reindex(df.index, method="ffill").rename(columns={"oi": "oi"})
            df = df.join(oi1m[["oi"]], how="left")
        if (ing_sym_dir / "liquidations.parquet").exists():
            liq = pd.read_parquet(ing_sym_dir / "liquidations.parquet").sort_index()
            grp = liq.groupby(pd.Grouper(freq="1T"))
            liq_min = pd.DataFrame({
                "liq_long": grp.apply(lambda g: g.loc[g.get("side", "BUY").str.upper() == "BUY", "qty"].sum()),
                "liq_short": grp.apply(lambda g: g.loc[g.get("side", "SELL").str.upper() == "SELL", "qty"].sum()),
                "liq_count": grp.size(),
            })
            df = df.join(liq_min, how="left")
        elif (ing_sym_dir / "liq_coinalyze.parquet").exists():
            liq = pd.read_parquet(ing_sym_dir / "liq_coinalyze.parquet").sort_index()
            grp = liq.groupby(pd.Grouper(freq="1T"))
            liq_min = pd.DataFrame({
                "liq_long": grp.apply(lambda g: g.loc[g.get("side", "BUY").str.upper() == "BUY", "qty"].sum()),
                "liq_short": grp.apply(lambda g: g.loc[g.get("side", "SELL").str.upper() == "SELL", "qty"].sum()),
                "liq_count": grp.size(),
            })
            df = df.join(liq_min, how="left")
    except Exception as e:
        logger.exception(f"ingest_enrich_failed symbol={symbol} error={e}")

    return df


def save_minute_parquet(df: pd.DataFrame, processed_dir: os.PathLike | str, symbol: str) -> Path:
    out = Path(processed_dir) / symbol / "minute.parquet"
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out)
    return out
