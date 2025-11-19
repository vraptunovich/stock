#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
This script:
  - loads configuration from config/parameters.yaml
  - reads spot_price from config
  - iterates over all CSV files under outdir/<ticker>/
  - reads strike column
  - computes:
      * relative_strike = ABS(strike / spot_price) * 100
      * spot_price column with the spot price used
  - overwrites the CSV files
"""

from pathlib import Path
import logging

import pandas as pd

from helper import load_config

# Repository root assuming this file is under src/
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
    Load a CSV file, compute:
      - relative_strike = ABS(strike / spot_price) * 100
      - spot_price column with the spot price used
    Add both as columns and overwrite the original file.
    """
    logger.info("Processing file: %s", csv_path)

    df = pd.read_csv(csv_path)

    if "strike" not in df.columns:
        logger.warning("Skipped file %s: no 'strike' column", csv_path.name)
        return

    if spot_price == 0:
        logger.error("Skipped file %s: spot_price is 0 (division by zero)", csv_path.name)
        return

    # Ensure strike is numeric
    df["strike"] = pd.to_numeric(df["strike"], errors="coerce")

    # Compute relative_strike in percent
    df["relative_strike"] = (df["strike"] / spot_price).abs() * 100.0

    # Add spot_price as a separate column (same value for all rows)
    df["spot_price"] = spot_price

    df.to_csv(csv_path, index=False)
    logger.info("✅ Updated file: %s", csv_path)


def run(config: dict) -> None:
    """
    Main execution function:
      - reads tickers, outdir, spot_price from config
      - iterates over all CSV files under outdir/<ticker>/
      - calls add_relative_strike_to_file for each file
    """
    tickers = config.get("tickers", [])
    outdir_name = config.get("outdir", "csv_out")
    spot_price = config.get("spot_price", None)

    if not tickers:
        logger.warning("No tickers configured. Nothing to do.")
        return

    if spot_price is None:
        logger.error("spot_price is not set in config. Please provide spot_price.")
        raise ValueError("spot_price is not set in config. Please provide spot_price.")

    try:
        spot_price = float(spot_price)
    except (TypeError, ValueError):
        logger.error("Invalid spot_price in config: %r", spot_price)
        raise ValueError(f"Invalid spot_price in config: {spot_price!r}")

    outdir = BASE_DIR / outdir_name

    if not outdir.exists():
        logger.warning("Output directory does not exist: %s", outdir)
        return

    logger.info("Starting relative_strike enrichment")
    logger.info("Output directory: %s", outdir)
    logger.info("Tickers: %s", ", ".join(tickers))
    logger.info("spot_price: %s", spot_price)

    for ticker in tickers:
        ticker_dir = outdir / ticker
        if not ticker_dir.exists():
            logger.warning("No directory for ticker %s: %s", ticker, ticker_dir)
            continue

        csv_files = sorted(ticker_dir.glob("*.csv"))
        if not csv_files:
            logger.warning("No CSV files for ticker %s in %s", ticker, ticker_dir)
            continue

        logger.info("Ticker %s: found %d CSV files", ticker, len(csv_files))
        for csv_path in csv_files:
            add_relative_strike_to_file(csv_path, spot_price)

    logger.info("relative_strike enrichment completed")


if __name__ == "__main__":
    cfg = load_config()
    run(cfg)
