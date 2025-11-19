#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
This script:
  - loads main configuration from config/config.yaml
  - loads strike bucket configuration from config/strike_buckets.yaml
  - iterates over all CSV files under outdir/<ticker>/
  - reads 'strike' and 'relative_strike' columns
  - aggregates max(relative_strike) per strike bucket
  - writes one summary CSV per ticker: <outdir>/<ticker>/<ticker>_strike_buckets_summary.csv
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

    Expected format:

    strike_buckets:
      - lower: 0.0
        upper: 25.0
      - lower: 25.0
        upper: 35.0
      ...

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
      - read all CSV files under outdir/<ticker>/
      - concatenate them
      - compute max(relative_strike) per strike bucket
      - save summary to outdir/<ticker>/<ticker>_strike_buckets_summary.csv
    """
    ticker_dir = outdir / ticker
    if not ticker_dir.exists():
        logger.warning("No directory for ticker %s: %s", ticker, ticker_dir)
        return

    csv_files = sorted(ticker_dir.glob("*.csv"))
    if not csv_files:
        logger.warning("No CSV files for ticker %s in %s", ticker, ticker_dir)
        return

    logger.info("Aggregating strike buckets for ticker: %s", ticker)
    logger.info("Found %d CSV files in %s", len(csv_files), ticker_dir)

    frames = []
    for csv_path in csv_files:
        logger.info("  • Loading %s", csv_path.name)
        df = pd.read_csv(csv_path)

        # We need strike and relative_strike columns
        if "strike" not in df.columns or "relative_strike" not in df.columns:
            logger.warning(
                "    Skipped (missing 'strike' or 'relative_strike'): %s",
                csv_path.name,
            )
            continue

        # Ensure strike and relative_strike are numeric
        df["strike"] = pd.to_numeric(df["strike"], errors="coerce")
        df["relative_strike"] = pd.to_numeric(df["relative_strike"], errors="coerce")

        frames.append(df[["strike", "relative_strike"]])

    if not frames:
        logger.warning(
            "No suitable data (strike + relative_strike) for ticker %s", ticker
        )
        return

    all_df = pd.concat(frames, ignore_index=True).dropna(
        subset=["strike", "relative_strike"]
    )
    if all_df.empty:
        logger.warning("All rows are NaN after cleaning for ticker %s", ticker)
        return

    logger.info(
        "Total rows to aggregate for ticker %s after cleaning: %d",
        ticker,
        len(all_df),
    )

    # Prepare result table
    rows = []
    for b in buckets:
        lower = float(b["lower"])
        upper = float(b["upper"])

        # Filter strikes in [lower, upper)
        mask = (all_df["strike"] >= lower) & (all_df["strike"] < upper)
        bucket_df = all_df[mask]

        if bucket_df.empty:
            max_rel = None
            logger.debug(
                "Bucket [%.2f, %.2f): no data for ticker %s",
                lower,
                upper,
                ticker,
            )
        else:
            max_rel = bucket_df["relative_strike"].max()
            logger.debug(
                "Bucket [%.2f, %.2f): max_relative_strike=%.4f for ticker %s",
                lower,
                upper,
                max_rel,
                ticker,
            )

        rows.append(
            {
                "ticker": ticker,
                "lower_strike": lower,
                "upper_strike": upper,
                "max_relative_strike": max_rel,
            }
        )

    result_df = pd.DataFrame(rows)

    # File name now includes the ticker
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
