#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
This script:
  - loads main configuration from config/config.yaml
  - loads strike bucket configuration from config/strike_buckets.yaml
  - for each ticker:
      - reads a single input file: outdir/<ticker>/<ticker>_options_all_expirations_filtered.csv
      - uses 'relative_strike' and 'max_tenor_for_strike' columns
      - for each strike bucket [lower, upper) (over relative_strike) computes
        max(max_tenor_for_strike) among rows that fall into the bucket
      - writes one summary CSV per ticker:
        outdir/<ticker>/<ticker>_strike_buckets_summary.csv

Input file per ticker:
  outdir/<ticker>/<ticker>_options_all_expirations_filtered.csv

Required columns in input file:
  - relative_strike
  - max_tenor_for_strike

Output columns (per bucket):
  - ticker
  - lower
  - upper
  - max_tenor_for_strike
"""

from pathlib import Path
from typing import List, Dict, Any
import logging

import pandas as pd
import yaml

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


def load_strike_buckets(path: Path | None = None) -> List[Dict[str, Any]]:
    """
    Load strike bucket configuration from YAML.

    Expected format in config/strike_buckets.yaml:

    strike_buckets:
      - lower: 0.0
        upper: 25.0
      - lower: 25.0
        upper: 35.0
      ...

    Buckets are interpreted as ranges over *relative_strike*.

    :param path: optional path to strike_buckets.yaml
    :return: list of bucket dicts with 'lower' and 'upper'
    """
    cfg_path = path or (BASE_DIR / "config" / "strike_buckets.yaml")
    logger.info("Loading strike buckets configuration from: %s", cfg_path)

    if not cfg_path.exists():
        logger.error("Strike buckets config not found: %s", cfg_path)
        raise FileNotFoundError(f"Strike buckets config not found: {cfg_path}")

    with cfg_path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}

    buckets = data.get("strike_buckets", [])
    if not isinstance(buckets, list) or not buckets:
        logger.error("strike_buckets is missing or empty in %s", cfg_path)
        raise ValueError(f"strike_buckets is missing or empty in {cfg_path}")

    logger.info("Loaded %d strike buckets", len(buckets))
    logger.debug("Strike buckets: %s", buckets)
    return buckets


def aggregate_for_ticker(
        ticker: str,
        outdir: Path,
        buckets: List[Dict[str, Any]],
) -> None:
    """
    For a given ticker:
      - read single CSV file under outdir/<ticker>/:
            <ticker>_options_all_expirations_filtered.csv
      - compute max(max_tenor_for_strike) per *relative_strike* bucket
      - save summary to:
            outdir/<ticker>/<ticker>_strike_buckets_summary.csv

    Expected columns in input CSV:
      - relative_strike
      - max_tenor_for_strike
    """
    ticker_dir = outdir / ticker
    if not ticker_dir.exists():
        logger.warning("No directory for ticker %s: %s", ticker, ticker_dir)
        return

    input_file = ticker_dir / f"{ticker}_options_all_expirations_filtered.csv"
    if not input_file.exists():
        logger.warning(
            "Input file for ticker %s not found: %s",
            ticker,
            input_file,
        )
        return

    logger.info(
        "Aggregating max_tenor_for_strike per relative_strike bucket for ticker: %s",
        ticker,
    )
    logger.info("Input file: %s", input_file)

    df = pd.read_csv(input_file)

    # We need relative_strike and max_tenor_for_strike columns
    if "relative_strike" not in df.columns or "max_tenor_for_strike" not in df.columns:
        logger.warning(
            "Input file %s is missing 'relative_strike' or 'max_tenor_for_strike' columns. Skipping.",
            input_file.name,
        )
        return

    # Ensure numeric
    df["relative_strike"] = pd.to_numeric(df["relative_strike"], errors="coerce")
    df["max_tenor_for_strike"] = pd.to_numeric(
        df["max_tenor_for_strike"], errors="coerce"
    )

    df = df.dropna(subset=["relative_strike", "max_tenor_for_strike"])
    if df.empty:
        logger.warning(
            "All rows are NaN for (relative_strike, max_tenor_for_strike) in %s. Nothing to aggregate for ticker %s.",
            input_file.name,
            ticker,
        )
        return

    logger.info(
        "Total rows to aggregate for ticker %s after cleaning: %d",
        ticker,
        len(df),
    )

    # Prepare result table: one row per bucket
    rows = []
    for b in buckets:
        lower = float(b["lower"])
        upper = float(b["upper"])

        # Filter by relative_strike bucket: [lower, upper)
        mask = (df["relative_strike"] >= lower) & (df["relative_strike"] < upper)
        bucket_df = df[mask]

        if bucket_df.empty:
            max_tenor = None
            logger.debug(
                "Bucket [%.4f, %.4f): no data for ticker %s (relative_strike)",
                lower,
                upper,
                ticker,
            )
        else:
            # key aggregation: maximum of max_tenor_for_strike within this bucket
            max_tenor = bucket_df["max_tenor_for_strike"].max()
            logger.debug(
                "Bucket [%.4f, %.4f): max_tenor_for_strike=%.4f for ticker %s",
                lower,
                upper,
                max_tenor,
                ticker,
            )

        rows.append(
            {
                "ticker": ticker,
                "lower": lower,
                "upper": upper,
                "max_tenor_for_strike": max_tenor,
            }
        )

    result_df = pd.DataFrame(rows)

    # Summary file: outdir/<ticker>/<ticker>_strike_buckets_summary.csv
    out_path = ticker_dir / f"{ticker}_strike_buckets_summary.csv"
    result_df.to_csv(out_path, index=False)
    logger.info("✅ Saved summary for %s: %s", ticker, out_path)


def run(config: dict) -> None:
    """
    Main entry point:
      - load tickers and outdir from main config
      - load strike buckets from dedicated YAML
      - aggregate for each ticker
    """
    tickers = config.get("tickers", [])
    outdir_name = config.get("outdir", "csv_out")

    if not tickers:
        logger.warning("No tickers configured. Nothing to do.")
        return

    outdir = BASE_DIR / outdir_name
    if not outdir.exists():
        logger.warning("Output directory does not exist: %s", outdir)
        return

    logger.info("Starting strike bucket aggregation")
    logger.info("Tickers: %s", ", ".join(tickers))
    logger.info("Output directory: %s", outdir)

    buckets = load_strike_buckets()

    for ticker in tickers:
        aggregate_for_ticker(ticker, outdir, buckets)

    logger.info("Strike bucket aggregation completed")


if __name__ == "__main__":
    cfg = load_config()
    run(cfg)
