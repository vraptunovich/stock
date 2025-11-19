#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
This script:
  - loads configuration from config/parameters.yaml
  - iterates over all CSV files under outdir/<ticker>/
  - reads tenor_days and strike
  - computes max_tenor_for_strike = MAXIFS(tenor_days, strike == current_strike)
  - overwrites the CSV files
"""

from pathlib import Path
import logging

import pandas as pd

from helper import load_config  # shared config loader

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

def calculate_max_tenor(csv_path: Path) -> None:
    """
    Add column max_tenor_for_strike to the CSV:
    max_tenor_for_strike = max tenor_days for each strike.
    Equivalent to Excel: =MAXIFS(S:S, D:D, D2)
    """
    logger.info("Processing file: %s", csv_path)

    df = pd.read_csv(csv_path)

    if "tenor_days" not in df.columns:
        logger.warning("Skipped file %s: no 'tenor_days' column", csv_path.name)
        return

    if "strike" not in df.columns:
        logger.warning("Skipped file %s: no 'strike' column", csv_path.name)
        return

    # Group by strike and find max tenor_days
    df["max_tenor_for_strike"] = df.groupby("strike")["tenor_days"].transform("max")

    df.to_csv(csv_path, index=False)
    logger.info("✅ Updated file: %s", csv_path)


def run(config: dict):
    """
    Iterate over all tickers and CSV files and update them.
    """
    tickers = config.get("tickers", [])
    outdir_name = config.get("outdir", "csv_out")

    outdir = BASE_DIR / outdir_name

    if not outdir.exists():
        logger.warning("Output directory does not exist: %s", outdir)
        return

    if not tickers:
        logger.warning("No tickers configured. Nothing to do.")
        return

    logger.info("Starting max_tenor_for_strike calculation")
    logger.info("Output directory: %s", outdir)
    logger.info("Tickers: %s", ", ".join(tickers))

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
            calculate_max_tenor(csv_path)

    logger.info("max_tenor_for_strike calculation completed")


if __name__ == "__main__":
    cfg = load_config()  # config/parameters.yaml
    run(cfg)
