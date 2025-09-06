#!/usr/bin/env python
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

# Make project root importable when running from scripts/
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from splf.data_handler.downloader import BinanceDownloader
from splf.utils.io import load_yaml


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    ap.add_argument("--force", action="store_true")
    args = ap.parse_args()

    cfg = load_yaml(args.config)
    paths = cfg["paths"]
    universe = cfg["universe"]
    symbols = universe.get("symbols") or (universe.get("tier_a", []) + universe.get("tier_b", []) + universe.get("tier_c", []))
    period = cfg["period"]
    datasets = [k for k, v in cfg.get("datasets", {}).items() if v]

    dl = BinanceDownloader(paths["raw_dir"], workers=cfg.get("runtime", {}).get("workers", 4))
    tasks = dl.plan(symbols, period["start"], period["end"], datasets)
    results = dl.download(tasks, force=args.force or cfg.get("runtime", {}).get("force", False))
    ok = sum(1 for _, b in results if b)
    print(f"Downloaded {ok}/{len(results)} files")


if __name__ == "__main__":
    main()
