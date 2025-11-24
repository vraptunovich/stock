#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Union, Optional
from zoneinfo import ZoneInfo

import pandas as pd
import yaml
import yfinance as yf

# Repository root, assuming this file is under src/
BASE_DIR = Path(__file__).resolve().parent.parent

logger = logging.getLogger(__name__)


def load_config(path: Optional[Union[str, Path]] = None) -> dict:
    """
    Load YAML configuration from the given path.

    If path is not provided, config/parameters.yaml under the repository root is used.
    """
    if path is None:
        cfg_path = BASE_DIR / "config" / "parameters.yaml"
        logger.info("Loading configuration from default path: %s", cfg_path)
    else:
        cfg_path = Path(path)
        logger.info("Loading configuration from explicit path: %s", cfg_path)

    if not cfg_path.exists():
        logger.error("Config file not found: %s", cfg_path)
        raise FileNotFoundError(f"Config file not found: {cfg_path}")

    try:
        with cfg_path.open("r", encoding="utf-8") as f:
            config = yaml.safe_load(f) or {}
    except Exception as exc:
        logger.exception("Failed to read or parse config file %s: %s", cfg_path, exc)
        raise

    logger.debug("Configuration loaded successfully with keys: %s", list(config.keys()))
    return config


def parse_date(s: str) -> datetime:
    """
    Parse a date string in either 'YYYY-MM-DD' or 'DD-MM-YYYY' format.
    Returns datetime in UTC timezone.
    """
    original = s
    s = s.strip()
    logger.debug("Parsing date string: %r", original)

    for fmt in ("%Y-%m-%d", "%d-%m-%Y"):
        try:
            dt = datetime.strptime(s, fmt)
            dt_utc = dt.replace(tzinfo=timezone.utc)
            logger.debug("Parsed date %r with format %s -> %s", s, fmt, dt_utc.isoformat())
            return dt_utc
        except ValueError:
            continue

    logger.error("Failed to parse date string: %r", original)
    raise ValueError(f"Cannot parse date: {s}. Use YYYY-MM-DD or DD-MM-YYYY.")


def ensure_dir(path: Path):
    """Create directory if it does not exist."""
    if path.exists():
        if path.is_dir():
            logger.debug("Directory already exists: %s", path)
        else:
            logger.warning("Path exists but is not a directory: %s", path)
    else:
        logger.info("Creating directory: %s", path)
        path.mkdir(parents=True, exist_ok=True)


def to_edt(dt: pd.Timestamp | None) -> str | None:
    """
    Convert a pandas timestamp to America/New_York timezone (EST/EDT)
    and return it as a formatted string.
    """
    if pd.isna(dt):
        logger.debug("to_edt: received NaN/None timestamp")
        return None

    try:
        ts = pd.to_datetime(dt, utc=True)
    except Exception as exc:
        logger.warning("to_edt: failed to convert %r to datetime: %s", dt, exc)
        return None

    if ts.tz is None:
        ts = ts.tz_localize("UTC")

    try:
        edt = ts.tz_convert(ZoneInfo("America/New_York"))
    except Exception as exc:
        logger.warning("to_edt: failed to convert %s to America/New_York: %s", ts, exc)
        return None

    formatted = edt.strftime("%Y-%m-%d %H:%M:%S %Z")
    logger.debug("to_edt: %s -> %s", ts, formatted)
    return formatted


def get_spot_price(ticker: str, snap_dt: datetime | None = None) -> float:
    """
    Returns the spot (underlying) price for a ticker.

    - If snap_dt is None:
        returns the current/last available price.
    - If snap_dt is provided:
        returns the close price for that specific calendar date.
    """

    t = yf.Ticker(ticker)

    # Case 1: No historical date provided → use latest price
    if snap_dt is None:
        info = getattr(t, "fast_info", None)
        price = None

        if info is not None:
            for key in ("lastPrice", "last_price", "last", "regularMarketPrice"):
                if key in info and info[key] is not None:
                    price = info[key]
                    break

        # Fallback if fast_info is not available
        if price is None:
            hist = t.history(period="1d")
            if hist.empty:
                raise ValueError(f"No current price data available for {ticker}.")
            price = hist["Close"].iloc[-1]

        return float(price)

    # Case 2: Historical price for snapshot date
    start = snap_dt.strftime("%Y-%m-%d")
    end = (snap_dt + timedelta(days=1)).strftime("%Y-%m-%d")

    hist = t.history(start=start, end=end)
    if hist.empty:
        raise ValueError(f"No historical price found for {ticker} on {snap_dt.date()}.")

    return float(hist["Close"].iloc[0])
