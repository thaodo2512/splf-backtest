from __future__ import annotations

import io
import os
import zipfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional

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
    return pd.to_datetime(ts_ms, unit="ms", utc=True).dt.floor("T")


def _safe_float(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")


def build_minute_frame(raw_dir: os.PathLike | str, symbol: str, start: str, end: str, include_spot: bool = False) -> pd.DataFrame:
    """
    Build a unified 1-minute DataFrame from daily zip files between start and end.
    Columns: index_px, perp_mark, premium, bid, ask, spread_bps, taker_buy_qty, taker_sell_qty, vol_perp, vol_spot (opt)
    """
    raw = Path(raw_dir) / symbol

    # aggregate per day then concat
    frames: List[pd.DataFrame] = []
    date_range = pd.date_range(start=start, end=end, freq="D", tz="UTC")
    for dt in date_range:
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
        df_index.rename(columns={"open_time": "ts", "close": "index_px"}, inplace=True)

        # mark price klines (1m)
        df_mark = _read_zip_csv(p("markPriceKlines_1m"), names=idx_cols, usecols=[0, 4])
        df_mark.rename(columns={"open_time": "ts", "close": "perp_mark"}, inplace=True)

        # premiumIndexKlines (1m): use close as premium proxy
        df_prem = _read_zip_csv(p("premiumIndexKlines_1m"), names=idx_cols, usecols=[0, 4])
        df_prem.rename(columns={"open_time": "ts", "close": "premium"}, inplace=True)

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
        df_at = _read_zip_csv(p("aggTrades"), names=at_cols)
        if not df_at.empty:
            # Coerce types and drop header rows accidentally read as data
            df_at["qty"] = pd.to_numeric(df_at["qty"], errors="coerce")  # base qty
            df_at["ts"] = pd.to_numeric(df_at["ts"], errors="coerce")
            df_at["isBuyerMaker"] = pd.to_numeric(df_at["isBuyerMaker"], errors="coerce")
            df_at = df_at.dropna(subset=["qty", "ts", "isBuyerMaker"])  # drop header line if present
            df_at["isBuyerMaker"] = df_at["isBuyerMaker"].astype(int)
            df_at["minute"] = _to_minute_index(df_at["ts"])
            grp = df_at.groupby("minute")
            taker_sell_qty = grp.apply(lambda g: g.loc[g["isBuyerMaker"] == 1, "qty"].sum())
            taker_buy_qty = grp.apply(lambda g: g.loc[g["isBuyerMaker"] == 0, "qty"].sum())
            df_cvd = pd.DataFrame({
                "taker_buy_qty": taker_buy_qty,
                "taker_sell_qty": taker_sell_qty,
                "vol_perp": grp["qty"].sum(),
            })
        else:
            df_cvd = pd.DataFrame()

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
                    raw = f.read()
                for cols in bt_cols_variants:
                    try:
                        tmp = pd.read_csv(io.BytesIO(raw), header=None, names=cols, low_memory=False)
                        df_bt = tmp
                        break
                    except Exception:
                        continue
        if not df_bt.empty:
            for c in ["bidPrice", "askPrice"]:
                df_bt[c] = _safe_float(df_bt[c])
            if "ts" in df_bt.columns:
                df_bt["ts"] = pd.to_numeric(df_bt["ts"], errors="coerce")
                df_bt = df_bt.dropna(subset=["ts"])  # drop header if present
                df_bt["minute"] = _to_minute_index(df_bt["ts"])
                df_bt = df_bt.sort_values("minute").groupby("minute").last()
            mid = (df_bt["bidPrice"] + df_bt["askPrice"]) / 2.0
            spread_bps = (df_bt["askPrice"] - df_bt["bidPrice"]) / mid * 10000.0
            df_liq = pd.DataFrame({
                "spread_bps": spread_bps
            })
        else:
            df_liq = pd.DataFrame()

        # spot aggTrades (optional)
        df_spot = pd.DataFrame()
        if include_spot and p("spot_aggTrades").exists():
            df_s = _read_zip_csv(p("spot_aggTrades"), names=at_cols)
            if not df_s.empty:
                df_s["qty"] = pd.to_numeric(df_s["qty"], errors="coerce")
                df_s["ts"] = pd.to_numeric(df_s["ts"], errors="coerce")
                df_s["isBuyerMaker"] = pd.to_numeric(df_s["isBuyerMaker"], errors="coerce")
                df_s = df_s.dropna(subset=["qty", "ts", "isBuyerMaker"])  # drop header rows
                df_s["isBuyerMaker"] = df_s["isBuyerMaker"].astype(int)
                df_s["minute"] = _to_minute_index(df_s["ts"]).astype("datetime64[ns, UTC]")
                grp = df_s.groupby("minute")
                df_spot = pd.DataFrame({
                    "taker_buy_qty_spot": grp.apply(lambda g: g.loc[g["isBuyerMaker"] == 0, "qty"].sum()),
                    "taker_sell_qty_spot": grp.apply(lambda g: g.loc[g["isBuyerMaker"] == 1, "qty"].sum()),
                    "vol_spot": grp["qty"].sum(),
                })

        # Merge all by minute index
        for d in (df_index, df_mark, df_prem):
            if not d.empty:
                d["minute"] = _to_minute_index(d["ts"]).astype("datetime64[ns, UTC]")
                d.drop(columns=["ts"], inplace=True)
                d.set_index("minute", inplace=True)

        df_day = pd.DataFrame(index=pd.date_range(dt, dt + pd.Timedelta(days=1) - pd.Timedelta(minutes=1), freq="T", tz="UTC"))
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

        frames.append(df_day)

    if not frames:
        return pd.DataFrame()

    df = pd.concat(frames).sort_index()
    # forward fill prices for continuous series
    for c in ["index_px", "perp_mark", "premium"]:
        if c in df.columns:
            df[c] = df[c].ffill()

    df["data_ok"] = True
    # mark data_ok false if last price stale > 2 minutes
    staleness = (df.index.to_series() - df[["index_px", "perp_mark"]].notna().apply(lambda s: df.index.to_series().where(s).ffill())).dt.total_seconds() / 60.0
    if not staleness.empty:
        df.loc[staleness > 2, "data_ok"] = False

    return df


def save_minute_parquet(df: pd.DataFrame, processed_dir: os.PathLike | str, symbol: str) -> Path:
    out = Path(processed_dir) / symbol / "minute.parquet"
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out)
    return out
