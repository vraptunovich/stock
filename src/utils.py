#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from datetime import datetime, timezone
from pathlib import Path
from typing import Union, Optional
from zoneinfo import ZoneInfo

import pandas as pd
import yaml

# Repository root, assuming this file is under src/
BASE_DIR = Path(__file__).resolve().parent.parent


def load_config(path: Optional[Union[str, Path]] = None) -> dict:
    """
    Load YAML configuration from the given path.

    If path is not provided, config/config.yaml under the repository root is used.
    """
    if path is None:
        cfg_path = BASE_DIR / "config" / "config.yaml"
    else:
        cfg_path = Path(path)

    if not cfg_path.exists():
        raise FileNotFoundError(f"Config file not found: {cfg_path}")

    with cfg_path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def parse_date(s: str) -> datetime:
    """
    Parse a date string in either 'YYYY-MM-DD' or 'DD-MM-YYYY' format.
    Returns datetime in UTC timezone.
    """
    s = s.strip()
    for fmt in ("%Y-%m-%d", "%d-%m-%Y"):
        try:
            dt = datetime.strptime(s, fmt)
            return dt.replace(tzinfo=timezone.utc)
        except ValueError:
            pass
    raise ValueError(f"Cannot parse date: {s}. Use YYYY-MM-DD or DD-MM-YYYY.")


def ensure_dir(path: Path):
    """Create directory if it does not exist."""
    path.mkdir(parents=True, exist_ok=True)


def to_edt(dt: pd.Timestamp | None) -> str | None:
    """
    Convert a pandas timestamp to America/New_York timezone (EST/EDT)
    and return it as a formatted string.
    """
    if pd.isna(dt):
        return None
    try:
        ts = pd.to_datetime(dt, utc=True)
    except Exception:
        return None
    if ts.tz is None:
        ts = ts.tz_localize("UTC")
    edt = ts.tz_convert(ZoneInfo("America/New_York"))
    return edt.strftime("%Y-%m-%d %H:%M:%S %Z")
