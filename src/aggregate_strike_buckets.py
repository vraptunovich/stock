#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
This script:
  - loads main configuration from config/config.yaml
  - loads strike bucket configuration from config/strike_buckets.yaml
  - iterates over all CSV files under outdir/<ticker>/
  - reads 'strike' and 'relative_strike' columns
  - aggregates max(relative_strike) per strike bucket
  - writes one summary CSV per ticker: <outdir>/<ticker>/strike_buckets_summary.csv
"""

from pathlib import Path
from typing import List, Dict, Any

import pandas as pd
import yaml

from helper import load_config

# Repository root assuming this file is under src/
BASE_DIR = Path(__file__).resolve().parent.parent


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
    if not cfg_path.exists():
        raise FileNotFoundError(f"Strike buckets config not found: {cfg_path}")

    with cfg_path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}

    buckets = data.get("strike_buckets", [])
    if not isinstance(buckets, list) or not buckets:
        raise ValueError(f"strike_buckets is missing or empty in {cfg_path}")

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
      - save summary to outdir/<ticker>/strike_buckets_summary.csv
    """
    ticker_dir = outdir / ticker
    if not ticker_dir.exists():
        print(f"⚠️ No directory for ticker {ticker}: {ticker_dir}")
        return

    csv_files = sorted(ticker_dir.glob("*.csv"))
    if not csv_files:
        print(f"⚠️ No CSV files for ticker {ticker}")
        return

    print(f"\n=== Aggregating strike buckets for ticker: {ticker} ===")
    print(f"Found {len(csv_files)} CSV files in {ticker_dir}")

    frames = []
    for csv_path in csv_files:
        print(f"  • Loading {csv_path.name}")
        df = pd.read_csv(csv_path)

        # We need strike and relative_strike columns
        if "strike" not in df.columns or "relative_strike" not in df.columns:
            print(f"    ⚠️ Skipped (missing 'strike' or 'relative_strike'): {csv_path.name}")
            continue

        # Ensure strike and relative_strike are numeric
        df["strike"] = pd.to_numeric(df["strike"], errors="coerce")
        df["relative_strike"] = pd.to_numeric(df["relative_strike"], errors="coerce")

        frames.append(df[["strike", "relative_strike"]])

    if not frames:
        print(f"⚠️ No suitable data (strike + relative_strike) for ticker {ticker}")
        return

    all_df = pd.concat(frames, ignore_index=True).dropna(subset=["strike", "relative_strike"])
    if all_df.empty:
        print(f"⚠️ All rows are NaN after cleaning for ticker {ticker}")
        return

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
        else:
            max_rel = bucket_df["relative_strike"].max()

        rows.append(
            {
                "lower_strike": lower,
                "upper_strike": upper,
                "max_relative_strike": max_rel,
            }
        )

    result_df = pd.DataFrame(rows)

    out_path = ticker_dir / "strike_buckets_summary.csv"
    result_df.to_csv(out_path, index=False)
    print(f"✅ Saved summary for {ticker}: {out_path}")


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
        print("⚠️ No tickers configured. Nothing to do.")
        return

    outdir = BASE_DIR / outdir_name
    if not outdir.exists():
        print(f"⚠️ Output directory does not exist: {outdir}")
        return

    buckets = load_strike_buckets()

    for ticker in tickers:
        aggregate_for_ticker(ticker, outdir, buckets)


if __name__ == "__main__":
    cfg = load_config()
    run(cfg)
