#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
This script:
  - loads configuration from config/config.yaml
  - iterates over all CSV files under outdir/<ticker>/
  - reads tenor_days and strike
  - computes max_tenor_for_strike = MAXIFS(tenor_days, strike == current_strike)
  - overwrites the CSV files
"""

from pathlib import Path

import pandas as pd

from utils import load_config  # shared config loader

# Repository root assuming this file is under src/
BASE_DIR = Path(__file__).resolve().parent.parent


def calculate_max_tenor(csv_path: Path) -> None:
    """
    Add column max_tenor_for_strike to the CSV:
    max_tenor_for_strike = max tenor_days for each strike.
    Equivalent to Excel: =MAXIFS(S:S, D:D, D2)
    """

    print(f"Processing: {csv_path}")

    df = pd.read_csv(csv_path)

    if "tenor_days" not in df.columns:
        print(f"  ⚠️ Skipped: no tenor_days column.")
        return

    if "strike" not in df.columns:
        print(f"  ⚠️ Skipped: no strike column.")
        return

    # Group by strike and find max tenor_days
    df["max_tenor_for_strike"] = df.groupby("strike")["tenor_days"].transform("max")

    df.to_csv(csv_path, index=False)
    print(f"  ✅ Updated: {csv_path}")


def run(config: dict):
    """
    Iterate over all tickers and CSV files and update them.
    """

    tickers = config.get("tickers", [])
    outdir_name = config.get("outdir", "csv_out")

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
            calculate_max_tenor(csv_path)


if __name__ == "__main__":
    cfg = load_config()  # config/config.yaml
    run(cfg)
