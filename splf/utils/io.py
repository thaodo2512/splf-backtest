from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict

import yaml


def ensure_dir(path: os.PathLike | str) -> Path:
    p = Path(path)
    p.mkdir(parents=True, exist_ok=True)
    return p


def load_yaml(path: os.PathLike | str) -> Dict[str, Any]:
    with open(path, "r") as f:
        return yaml.safe_load(f)


def save_json(path: os.PathLike | str, obj: Any) -> None:
    import json

    p = Path(path)
    ensure_dir(p.parent)
    with open(p, "w") as f:
        json.dump(obj, f, indent=2, sort_keys=True)


def save_parquet(df, path: os.PathLike | str) -> None:
    from pathlib import Path

    p = Path(path)
    ensure_dir(p.parent)
    df.to_parquet(p)


def read_parquet(path: os.PathLike | str):
    from pathlib import Path

    p = Path(path)
    return None if not p.exists() else __import__("pandas").read_parquet(p)


def dt_floor_minute(ts):
    import pandas as pd

    return pd.to_datetime(ts, unit="ms", utc=True).floor("T")

