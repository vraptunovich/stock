#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

import pandas as pd
import yfinance as yf

from utils import ensure_dir
from utils import load_config
from utils import parse_date
from utils import to_edt

# Resolve repository root assuming this file is under src/
BASE_DIR = Path(__file__).resolve().parent.parent

# Columns to preserve and order in the final CSV
YAHOO_COL_ORDER = [
    "contractSymbol", "type", "expiration", "strike",
    "lastTradeDate", "lastTradeDateEDT",
    "lastPrice", "bid", "ask",
    "change", "percentChange", "volume", "openInterest",
    "impliedVolatility", "inTheMoney", "contractSize", "currency"
]


# ---------------------------------------------------------
# Core logic
# ---------------------------------------------------------

def normalize_chain(df: pd.DataFrame, opt_type: str, expiration: str) -> pd.DataFrame:
    """
    Normalize a calls/puts chain into a unified column set.
    """
    if df is None or df.empty:
        return pd.DataFrame(columns=YAHOO_COL_ORDER)

    out = df.copy()
    out["type"] = opt_type
    out["expiration"] = expiration

    # Ensure missing columns exist
    for col in YAHOO_COL_ORDER:
        if col not in out.columns and col != "lastTradeDateEDT":
            out[col] = pd.NA

    out["lastTradeDateEDT"] = out["lastTradeDate"].apply(to_edt)

    # Reorder columns
    out = out[[c for c in YAHOO_COL_ORDER if c in out.columns]]
    return out


def load_for_expiration(ticker: str, expiration: str) -> pd.DataFrame:
    """
    Load option chains for a single expiration date (calls + puts).
    """
    t = yf.Ticker(ticker)
    chain = t.option_chain(expiration)

    frames = [
        normalize_chain(chain.calls, "call", expiration),
        normalize_chain(chain.puts, "put", expiration),
    ]

    df = pd.concat([f for f in frames if not f.empty], ignore_index=True)
    if not df.empty:
        df.sort_values(by=["expiration", "type", "strike"],
                       inplace=True, ignore_index=True)
    return df


def apply_filters(df: pd.DataFrame) -> pd.DataFrame:
    """
    Filter rows:
      - openInterest > 0
      - non-empty lastTradeDateEDT
    """
    if df.empty:
        return df

    oi_ok = df["openInterest"].fillna(0) > 0
    ltd_ok = df["lastTradeDateEDT"].notna() & (
            df["lastTradeDateEDT"].astype(str).str.len() > 0
    )

    return df[oi_ok & ltd_ok].reset_index(drop=True)


def pick_expirations(all_exps: list[str],
                     exp_start: str | None,
                     exp_end: str | None,
                     exp_dates: Iterable[str] | None) -> list[str]:
    """
    Select which expiration dates to load.

    Priority:
        1. exp_dates (exact list provided by user)
        2. exp_start/exp_end range
        3. All available expirations
    """
    if not all_exps:
        return []

    # Exact dates provided
    if exp_dates:
        target = set(parse_date(d).strftime("%Y-%m-%d") for d in exp_dates)
        return [d for d in all_exps if d in target]

    # Range selection
    start_dt = parse_date(exp_start) if exp_start else None
    end_dt = parse_date(exp_end) if exp_end else None

    if start_dt or end_dt:
        picked = []
        for exp in all_exps:
            dt = datetime.strptime(exp, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            if start_dt and dt < start_dt:
                continue
            if end_dt and dt > end_dt:
                continue
            picked.append(exp)
        return picked

    # No filters → return all
    return list(all_exps)


# ---------------------------------------------------------
# Main work
# ---------------------------------------------------------

def run(config: dict):
    """
    Main execution function:
      - loads configuration
      - fetches option chains
      - applies filters
      - saves results into CSV files inside the configured output directory.
      - each ticker has its own subdirectory under the output directory.
    """

    tickers = config.get("tickers", ["AAPL"])
    exp_start = config.get("exp_start") or ""
    exp_end = config.get("exp_end") or ""
    exp_dates = config.get("exp_dates") or []
    outdir_name = config.get("outdir", "csv_out")

    # Base output directory is resolved relative to the repository root
    outdir = BASE_DIR / outdir_name
    ensure_dir(outdir)

    for ticker in tickers:
        print(f"\n================ {ticker} :: options ================")

        # Create per-ticker subdirectory, e.g. csv_out/AAPL, csv_out/MSFT
        ticker_dir = outdir / ticker
        ensure_dir(ticker_dir)

        t = yf.Ticker(ticker)
        all_exps = t.options or []

        exps = pick_expirations(all_exps, exp_start, exp_end, exp_dates)

        # If nothing specified — use all expirations
        if not exps and not (exp_start or exp_end or exp_dates):
            exps = all_exps

        if not exps:
            print(f"⚠️ No matching expirations for {ticker}")
            continue

        print(f"Expirations to process: {len(exps)}")

        frames = []
        for exp in exps:
            print(f"  • Loading {exp} ...")
            df = load_for_expiration(ticker, exp)
            if df.empty:
                print("    – no data")
                continue
            frames.append(df)

        if not frames:
            print("⚠️ No data loaded for any expiration.")
            continue

        df_all = pd.concat(frames, ignore_index=True)
        df_filtered = apply_filters(df_all)

        # Build output file name
        if exp_dates:
            tag = "_".join(sorted([parse_date(d).strftime("%Y-%m-%d") for d in exp_dates]))
            out_name = f"{ticker}_options_exact_{tag}_filtered.csv"
        elif exp_start or exp_end:
            s = parse_date(exp_start).strftime("%Y-%m-%d") if exp_start else "MIN"
            e = parse_date(exp_end).strftime("%Y-%m-%d") if exp_end else "MAX"
            out_name = f"{ticker}_options_range_{s}_to_{e}_filtered.csv"
        else:
            out_name = f"{ticker}_options_all_expirations_filtered.csv"

        # Save file into per-ticker directory
        out_path = ticker_dir / out_name
        df_filtered.to_csv(out_path, index=False)

        print(f"Total rows (raw): {len(df_all)}, after filters: {len(df_filtered)}")
        print(f"✅ Saved: {out_path}")


if __name__ == "__main__":
    cfg = load_config()
    run(cfg)
