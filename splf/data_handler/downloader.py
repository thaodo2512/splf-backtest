from __future__ import annotations

import concurrent.futures as futures
import hashlib
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable, List, Optional, Tuple

import requests
from dateutil import rrule
from tqdm import tqdm

BINANCE_VISION = "https://data.binance.vision/data"


@dataclass
class DownloadTask:
    url: str
    dest: Path
    checksum_url: Optional[str] = None


def daterange(start: str, end: str) -> List[datetime]:
    s = datetime.fromisoformat(start).date()
    e = datetime.fromisoformat(end).date()
    return [dt for dt in rrule.rrule(rrule.DAILY, dtstart=datetime(s.year, s.month, s.day), until=datetime(e.year, e.month, e.day))]


def checksum_ok(path: Path, checksum: str) -> bool:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest() == checksum


def parse_checksum(text: str, filename: str) -> Optional[str]:
    for line in text.splitlines():
        if filename in line:
            parts = line.strip().split()
            if len(parts) >= 2:
                return parts[0]
    return None


class BinanceDownloader:
    """
    Planner and downloader for Binance Vision daily dumps for UM futures and spot.
    """

    def __init__(self, raw_dir: os.PathLike | str, workers: int = 4, timeout: int = 60):
        self.raw_dir = Path(raw_dir)
        self.workers = workers
        self.timeout = timeout

    def _build_url(self, dataset: str, symbol: str, dt: datetime, is_spot: bool = False) -> Tuple[str, str]:
        base = BINANCE_VISION
        market = "spot" if is_spot else "futures/um"
        if dataset == "fundingRate_monthly":
            # monthly by year-month
            ym = dt.strftime("%Y-%m")
            path = f"{base}/{market}/monthly/fundingRate/{symbol}/{symbol}-fundingRate-{ym}.zip"
            path_chk = path + ".CHECKSUM"
            return path, path_chk

        date_str = dt.strftime("%Y-%m-%d")
        if dataset == "aggTrades":
            path = f"{base}/{market}/daily/aggTrades/{symbol}/{symbol}-aggTrades-{date_str}.zip"
        elif dataset == "bookTicker":
            path = f"{base}/{market}/daily/bookTicker/{symbol}/{symbol}-bookTicker-{date_str}.zip"
        elif dataset == "klines_1m":
            path = f"{base}/{market}/daily/klines/{symbol}/1m/{symbol}-1m-{date_str}.zip"
        elif dataset == "indexPriceKlines_1m":
            path = f"{base}/{market}/daily/indexPriceKlines/{symbol}/1m/{symbol}-1m-{date_str}.zip"
        elif dataset == "markPriceKlines_1m":
            path = f"{base}/{market}/daily/markPriceKlines/{symbol}/1m/{symbol}-1m-{date_str}.zip"
        elif dataset == "premiumIndexKlines_1m":
            path = f"{base}/{market}/daily/premiumIndexKlines/{symbol}/1m/{symbol}-1m-{date_str}.zip"
        elif dataset == "spot_aggTrades":
            path = f"{base}/spot/daily/aggTrades/{symbol}/{symbol}-aggTrades-{date_str}.zip"
        else:
            raise ValueError(f"Unknown dataset: {dataset}")

        return path, path + ".CHECKSUM"

    def plan(self, symbols: Iterable[str], start: str, end: str, datasets: Iterable[str]) -> List[DownloadTask]:
        tasks: List[DownloadTask] = []
        for sym in symbols:
            for dt in daterange(start, end):
                for ds in datasets:
                    is_spot = ds.startswith("spot_")
                    url, chk = self._build_url(ds, sym, dt, is_spot=is_spot)
                    out_dir = self.raw_dir / sym / ds
                    out_name = Path(url).name
                    dest = out_dir / out_name
                    tasks.append(DownloadTask(url=url, dest=dest, checksum_url=chk))
        return tasks

    def _download_one(self, task: DownloadTask, force: bool = False) -> Tuple[Path, bool]:
        dest = task.dest
        dest.parent.mkdir(parents=True, exist_ok=True)
        if dest.exists() and not force:
            return dest, True

        # download file
        r = requests.get(task.url, stream=True, timeout=self.timeout)
        if r.status_code != 200:
            raise RuntimeError(f"HTTP {r.status_code} for {task.url}")
        total = int(r.headers.get("Content-Length", 0))
        with open(dest, "wb") as f, tqdm(total=total, unit="B", unit_scale=True, desc=dest.name, leave=False) as pbar:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)
                    pbar.update(len(chunk))

        # checksum verify if available
        ok = True
        if task.checksum_url:
            rc = requests.get(task.checksum_url, timeout=self.timeout)
            if rc.status_code == 200:
                checksum = parse_checksum(rc.text, dest.name)
                if checksum:
                    ok = checksum_ok(dest, checksum)
        return dest, ok

    def download(self, tasks: List[DownloadTask], force: bool = False, max_workers: Optional[int] = None) -> List[Tuple[Path, bool]]:
        results: List[Tuple[Path, bool]] = []
        max_workers = max_workers or self.workers
        with futures.ThreadPoolExecutor(max_workers=max_workers) as ex:
            futs = [ex.submit(self._download_one, t, force) for t in tasks]
            for fut in tqdm(futs, desc="Downloading", leave=False):
                try:
                    results.append(fut.result())
                except Exception as e:
                    results.append((Path(""), False))
        return results

