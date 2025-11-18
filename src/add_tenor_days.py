#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from datetime import datetime
from pathlib import Path

import pandas as pd

from utils import load_config
from utils import parse_date

# Resolve repository root assuming this file is under src/
BASE_DIR = Path(__file__).resolve().parent.parent


# ---------------------------------------------------------
# Core logic
# ---------------------------------------------------------

def add_tenor_to_file(csv_path: Path, snap_dt: datetime) -> None:
    """
    Load a CSV file, compute tenor_days = (expiration - snap_date) in days,
    add it as a new column, and overwrite the original file.
    """
    print(f"Processing file: {csv_path}")

    df = pd.read_csv(csv_path)

    if "expiration" not in df.columns:
        print(f"  ⚠️ Skipped: no 'expiration' column in {csv_path.name}")
        return

    # Convert expiration column to datetime
    exp_ts = pd.to_datetime(df["expiration"], errors="coerce")

    # Convert snap_dt to pandas Timestamp
    snap_ts = pd.to_datetime(snap_dt)

    # Calculate tenor in days; rows with invalid expiration will get NaN
    df["tenor_days"] = (exp_ts - snap_ts).dt.days

    df.to_csv(csv_path, index=False)
    print(f"  ✅ Updated: {csv_path}")


def run(config: dict) -> None:
    """
    Main execution function:
      - loads configuration
      - locates all CSV files under outdir/<ticker> directories
      - adds tenor_days column based on snap_date
    """
    tickers = config.get("tickers", [])
    outdir_name = config.get("outdir", "csv_out")
    snap_date_str = config.get("snap_date")

    if not tickers:
        print("⚠️ No tickers configured. Nothing to do.")
        return

    # Resolve snapshot date:
    if not snap_date_str:
        raise ValueError("snap_date is not set in config. Please provide snap_date.")

    snap_dt = parse_date(snap_date_str)
    print(f"Using snap_date: {snap_dt.date().isoformat()}")

    # Base output directory (root/outdir)
    outdir = BASE_DIR / outdir_name

    if not outdir.exists():
        print(f"⚠️ Output directory does not exist: {outdir}")
        return

    for ticker in tickers:
        ticker_dir = outdir / ticker
        if not ticker_dir.exists():
            print(f"⚠️ Directory for ticker {ticker} does not exist: {ticker_dir}")
            continue

        csv_files = sorted(ticker_dir.glob("*.csv"))
        if not csv_files:
            print(f"⚠️ No CSV files found for ticker {ticker} in {ticker_dir}")
            continue

        print(f"\n=== Ticker: {ticker} | files: {len(csv_files)} ===")
        for csv_path in csv_files:
            add_tenor_to_file(csv_path, snap_dt)


if __name__ == "__main__":
    cfg = load_config()
    run(cfg)
