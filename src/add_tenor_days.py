#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
This script:
  - loads configuration from config/parameters.yaml
  - reads snap_date from config (or uses current date if not set)
  - iterates over all CSV files under outdir/<ticker>/
  - computes:
      * tenor_days = (expiration - snap_date) in days
      * snap_date column with the snapshot date used
  - overwrites the CSV files
"""

from datetime import datetime
from pathlib import Path
import logging

import pandas as pd

from helper import load_config
from helper import parse_date

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

def add_tenor_to_file(csv_path: Path, snap_dt: datetime) -> None:
    """
    Load a CSV file, compute:
      - tenor_days = (expiration - snap_date) in days
      - snap_date column with the snapshot date used
    Add both as columns and overwrite the original file.
    """
    logger.info("Processing file: %s", csv_path)

    df = pd.read_csv(csv_path)

    if "expiration" not in df.columns:
        logger.warning("Skipped file %s: no 'expiration' column", csv_path.name)
        return

    # Convert expiration column to timezone-aware UTC timestamps
    exp_ts = pd.to_datetime(df["expiration"], errors="coerce", utc=True)

    # Convert snap_dt to timezone-aware UTC timestamp
    if snap_dt.tzinfo is None:
        snap_ts = pd.to_datetime(snap_dt).tz_localize("UTC")
    else:
        snap_ts = pd.to_datetime(snap_dt).tz_convert("UTC")

    # Calculate tenor in days; rows with invalid expiration will get NaN
    df["tenor_days"] = (exp_ts - snap_ts).dt.days

    # Add snap_date as a separate column (ISO date string, same for all rows)
    df["snap_date"] = snap_dt.date().isoformat()

    df.to_csv(csv_path, index=False)
    logger.info("✅ Updated file: %s", csv_path)


def run(config: dict) -> None:
    """
    Main execution function:
      - loads configuration
      - locates all CSV files under outdir/<ticker> directories
      - adds tenor_days and snap_date columns based on snap_date
        (if snap_date is not set, uses current date)
    """
    tickers = config.get("tickers", [])
    outdir_name = config.get("outdir", "csv_out")
    snap_date_str = config.get("snap_date")

    if not tickers:
        logger.warning("No tickers configured. Nothing to do.")
        return

    # Resolve snapshot date
    if not snap_date_str:
        # Use current UTC time if snap_date is not provided
        snap_dt = datetime.utcnow()
        logger.info(
            "snap_date is not set in config. Using current date as snap_date: %s",
            snap_dt.date().isoformat(),
        )
    else:
        snap_dt = parse_date(snap_date_str)
        logger.info("Using snap_date from config: %s", snap_dt.date().isoformat())

    # Base output directory (root/outdir)
    outdir = BASE_DIR / outdir_name

    if not outdir.exists():
        logger.warning("Output directory does not exist: %s", outdir)
        return

    logger.info("Starting tenor_days enrichment")
    logger.info("Output directory: %s", outdir)
    logger.info("Tickers: %s", ", ".join(tickers))

    for ticker in tickers:
        ticker_dir = outdir / ticker
        if not ticker_dir.exists():
            logger.warning(
                "Directory for ticker %s does not exist: %s", ticker, ticker_dir
            )
            continue

        csv_files = sorted(ticker_dir.glob("*.csv"))
        if not csv_files:
            logger.warning("No CSV files found for ticker %s in %s", ticker, ticker_dir)
            continue

        logger.info("Ticker %s: found %d CSV files", ticker, len(csv_files))
        for csv_path in csv_files:
            add_tenor_to_file(csv_path, snap_dt)

    logger.info("tenor_days enrichment completed")


if __name__ == "__main__":
    cfg = load_config()
    run(cfg)
