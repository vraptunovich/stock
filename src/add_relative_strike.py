#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
This script:
  - loads configuration from config/parameters.yaml
  - reads spot_price from config
  - iterates over all CSV files under outdir/<ticker>/
  - reads strike column
  - computes relative_strike = ABS(strike / spot_price) * 100
  - overwrites the CSV files
"""

from pathlib import Path
import pandas as pd

from helper import load_config

# Repository root assuming this file is under src/
BASE_DIR = Path(__file__).resolve().parent.parent


def add_relative_strike_to_file(csv_path: Path, spot_price: float) -> None:
    """
    Load a CSV file, compute relative_strike = ABS(strike / spot_price) * 100,
    add it as a new column, and overwrite the original file.
    """
    print(f"Processing: {csv_path}")

    df = pd.read_csv(csv_path)

    if "strike" not in df.columns:
        print(f"  ⚠️ Skipped: no 'strike' column.")
        return

    if spot_price == 0:
        print("  ⚠️ Skipped: spot_price is 0, division by zero.")
        return

    # Ensure strike is numeric
    df["strike"] = pd.to_numeric(df["strike"], errors="coerce")

    # Compute relative_strike in percent
    df["relative_strike"] = (df["strike"] / spot_price).abs() * 100.0

    df.to_csv(csv_path, index=False)
    print(f"  ✅ Updated: {csv_path}")


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
        print("⚠️ No tickers configured. Nothing to do.")
        return

    if spot_price is None:
        raise ValueError("spot_price is not set in config. Please provide spot_price.")

    try:
        spot_price = float(spot_price)
    except (TypeError, ValueError):
        raise ValueError(f"Invalid spot_price in config: {spot_price!r}")

    outdir = BASE_DIR / outdir_name

    if not outdir.exists():
        print(f"⚠️ Output directory does not exist: {outdir}")
        return

    for ticker in tickers:
        ticker_dir = outdir / ticker
        if not ticker_dir.exists():
            print(f"⚠️ No directory for ticker {ticker}: {ticker_dir}")
            continue

        csv_files = sorted(ticker_dir.glob("*.csv"))
        if not csv_files:
            print(f"⚠️ No CSV files for ticker {ticker}")
            continue

        print(f"\n=== Ticker: {ticker} | CSV files: {len(csv_files)} ===")
        for csv_path in csv_files:
            add_relative_strike_to_file(csv_path, spot_price)


if __name__ == "__main__":
    cfg = load_config()
    run(cfg)
