#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
This script:
  - loads configuration from config/parameters.yaml
  - reads snap_date (or uses current UTC date if missing)
  - optionally reads spot_price_overrides per ticker from config
  - for each ticker:
        * uses overridden spot_price if provided
        * otherwise fetches spot_price from yfinance
  - loads all CSV files under outdir/<ticker>/
  - computes:
        spot_price = underlying price
        relative_strike = ABS(strike / spot_price) * 100
  - overwrites the CSV files
"""

from datetime import datetime
from pathlib import Path
import logging

import pandas as pd

from helper import load_config, parse_date, get_spot_price

# Resolve repository root assuming this file is under src/
BASE_DIR = Path(__file__).resolve().parent.parent

# ---------------------------------------------------------
# Logging setup
# ---------------------------------------------------------

logger = logging.getLogger(__name__)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)


# ---------------------------------------------------------
# Core logic
# ---------------------------------------------------------

def add_relative_strike_to_file(csv_path: Path, spot_price: float) -> None:
    """
    Loads a CSV file and computes:

        - spot_price (constant per file)
        - relative_strike = ABS(strike / spot_price) * 100

    The updated file overwrites the original CSV.
    """
    logger.info("Processing file: %s", csv_path)

    df = pd.read_csv(csv_path)

    if "strike" not in df.columns:
        logger.warning("Skipped file %s: 'strike' column not found", csv_path.name)
        return

    df["spot_price"] = float(spot_price)
    df["relative_strike"] = (df["strike"] / df["spot_price"]).abs() * 100.0

    df.to_csv(csv_path, index=False)
    logger.info("Updated file: %s", csv_path)


def resolve_spot_price_for_ticker(
        ticker: str,
        snap_dt: datetime,
        overrides: dict,
) -> float:
    """
    Resolves spot_price for a given ticker:

        - If the ticker is present in overrides, use that value.
        - Otherwise, call get_spot_price(...) (yfinance).
    """
    if overrides and ticker in overrides:
        spot_price = float(overrides[ticker])
        logger.info(
            "Ticker %s: using overridden spot_price from config = %.4f",
            ticker,
            spot_price,
        )
        return spot_price

    spot_price = get_spot_price(ticker, snap_dt)
    logger.info(
        "Ticker %s: spot_price from yfinance for %s = %.4f",
        ticker,
        snap_dt.date().isoformat(),
        spot_price,
    )
    return spot_price


def run(config: dict) -> None:
    """
    Main execution:

        - resolve tickers
        - determine snap_date (config or current UTC date)
        - read spot_price_overrides (if present)
        - for each ticker:
              * resolve spot_price (override or yfinance)
              * enrich each CSV with spot_price and relative_strike
    """
    tickers = config.get("tickers", [])
    outdir_name = config.get("outdir", "csv_out")
    snap_date_str = config.get("snap_date")
    spot_price_overrides = config.get("spot_price_overrides", {}) or {}

    if not tickers:
        logger.warning("No tickers configured. Nothing to process.")
        return

    # Resolve snapshot date
    if snap_date_str:
        snap_dt = parse_date(snap_date_str)
        logger.info("Using snap_date from config: %s", snap_dt.date().isoformat())
    else:
        snap_dt = datetime.utcnow()
        logger.info(
            "snap_date not provided → using current UTC date: %s",
            snap_dt.date().isoformat(),
        )

    # Output directory
    outdir = BASE_DIR / outdir_name

    if not outdir.exists():
        logger.warning("Output directory does not exist: %s", outdir)
        return

    logger.info("Starting relative_strike enrichment")
    logger.info("Output directory: %s", outdir)
    logger.info("Tickers: %s", ", ".join(tickers))

    if spot_price_overrides:
        logger.info(
            "spot_price_overrides found in config for tickers: %s",
            ", ".join(spot_price_overrides.keys()),
        )

    # Per-ticker processing
    for ticker in tickers:
        ticker_dir = outdir / ticker

        if not ticker_dir.exists():
            logger.warning("Directory for ticker %s not found: %s", ticker, ticker_dir)
            continue

        csv_files = sorted(ticker_dir.glob("*.csv"))
        if not csv_files:
            logger.warning("No CSV files for ticker %s in %s", ticker, ticker_dir)
            continue

        # Resolve spot_price (override or yfinance)
        try:
            spot_price = resolve_spot_price_for_ticker(
                ticker=ticker,
                snap_dt=snap_dt,
                overrides=spot_price_overrides,
            )
        except Exception as exc:
            logger.error(
                "Failed to resolve spot_price for ticker %s: %s (Skipping ticker)",
                ticker,
                exc,
            )
            continue

        logger.info("Ticker %s: %d CSV files found", ticker, len(csv_files))

        for csv_path in csv_files:
            add_relative_strike_to_file(csv_path, spot_price)

    logger.info("relative_strike enrichment completed.")


if __name__ == "__main__":
    cfg = load_config()
    run(cfg)
