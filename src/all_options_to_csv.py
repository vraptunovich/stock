#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

import logging
import pandas as pd
import yfinance as yf

from helper import ensure_dir
from helper import load_config
from helper import parse_date
from helper import to_edt

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
# Logging setup
# ---------------------------------------------------------

logger = logging.getLogger(__name__)

# Basic console logging; GitHub Actions will capture stdout/stderr
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)


# ---------------------------------------------------------
# Core logic
# ---------------------------------------------------------

def normalize_chain(df: pd.DataFrame, opt_type: str, expiration: str) -> pd.DataFrame:
    """
    Normalize a calls/puts chain into a unified column set.
    """
    if df is None or df.empty:
        logger.debug("normalize_chain: empty dataframe for %s %s", opt_type, expiration)
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
    logger.debug("Loading option chain for %s @ %s", ticker, expiration)
    t = yf.Ticker(ticker)

    try:
        chain = t.option_chain(expiration)
    except Exception as exc:
        logger.warning("Failed to load option_chain for %s @ %s: %s", ticker, expiration, exc)
        return pd.DataFrame(columns=YAHOO_COL_ORDER)

    frames = [
        normalize_chain(chain.calls, "call", expiration),
        normalize_chain(chain.puts, "put", expiration),
    ]

    df = pd.concat([f for f in frames if not f.empty], ignore_index=True)
    if not df.empty:
        df.sort_values(
            by=["expiration", "type", "strike"],
            inplace=True,
            ignore_index=True,
        )
        logger.debug(
            "Loaded %d rows for %s @ %s",
            len(df),
            ticker,
            expiration,
        )
    else:
        logger.debug("No data for %s @ %s", ticker, expiration)

    return df


def apply_filters(df: pd.DataFrame) -> pd.DataFrame:
    """
    Filter rows:
      - openInterest > 0
      - non-empty lastTradeDateEDT
    """
    if df.empty:
        logger.debug("apply_filters: empty dataframe, nothing to filter")
        return df

    before = len(df)

    oi_ok = df["openInterest"].fillna(0) > 0
    ltd_ok = df["lastTradeDateEDT"].notna() & (
            df["lastTradeDateEDT"].astype(str).str.len() > 0
    )

    filtered = df[oi_ok & ltd_ok].reset_index(drop=True)
    after = len(filtered)

    logger.info("Filtering rows: %d -> %d after OI/lastTradeDate filters", before, after)
    return filtered


def pick_expirations(
        all_exps: list[str],
        exp_start: str | None,
        exp_end: str | None,
        exp_dates: Iterable[str] | None,
) -> list[str]:
    """
    Select which expiration dates to load.

    Priority:
        1. exp_dates (exact list provided by user)
        2. exp_start/exp_end range
        3. All available expirations
    """
    if not all_exps:
        logger.warning("No expirations available from Yahoo")
        return []

    # Exact dates provided
    if exp_dates:
        target = set(parse_date(d).strftime("%Y-%m-%d") for d in exp_dates)
        picked = [d for d in all_exps if d in target]
        logger.info(
            "Picked expirations by explicit dates (%d/%d): %s",
            len(picked),
            len(all_exps),
            ", ".join(picked),
        )
        return picked

    # Range selection
    start_dt = parse_date(exp_start) if exp_start else None
    end_dt = parse_date(exp_end) if exp_end else None

    if start_dt or end_dt:
        picked: list[str] = []
        for exp in all_exps:
            dt = datetime.strptime(exp, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            if start_dt and dt < start_dt:
                continue
            if end_dt and dt > end_dt:
                continue
            picked.append(exp)
        logger.info(
            "Picked expirations by range (%d/%d), start=%s, end=%s",
            len(picked),
            len(all_exps),
            start_dt.isoformat() if start_dt else "None",
            end_dt.isoformat() if end_dt else "None",
        )
        return picked

    # No filters → return all
    logger.info("No expiration filters provided, using all %d expirations", len(all_exps))
    return list(all_exps)


def enrich_with_tenor_and_snap_date(df: pd.DataFrame, snap_dt: datetime) -> pd.DataFrame:
    """
    Add tenor_days and snap_date columns based on snap_dt.
    tenor_days = (expiration - snap_date) in days.
    """
    if df.empty:
        logger.warning("enrich_with_tenor_and_snap_date: empty dataframe, nothing to enrich")
        return df

    # Convert expiration column to timezone-aware UTC timestamps
    exp_ts = pd.to_datetime(df["expiration"], errors="coerce", utc=True)

    # Convert snap_dt to timezone-aware UTC timestamp
    if snap_dt.tzinfo is None:
        snap_ts = pd.to_datetime(snap_dt).tz_localize("UTC")
    else:
        snap_ts = pd.to_datetime(snap_dt).tz_convert("UTC")

    df["tenor_days"] = (exp_ts - snap_ts).dt.days
    df["snap_date"] = snap_dt.date().isoformat()

    return df


# ---------------------------------------------------------
# Main work
# ---------------------------------------------------------

def run(config: dict):
    """
    Main execution function:
      - loads configuration
      - resolves snap_date (from config or current UTC)
      - fetches option chains for all tickers
      - applies filters
      - enriches with tenor_days and snap_date
      - saves results into a single CSV file inside the configured output directory.
        All tickers are combined into one file; a 'ticker' column is added.
    """

    tickers = config.get("tickers", ["AAPL"])
    exp_start = config.get("exp_start") or ""
    exp_end = config.get("exp_end") or ""
    exp_dates = config.get("exp_dates") or []
    outdir_name = config.get("outdir", "csv_out")
    snap_date_str = config.get("snap_date")

    logger.info("Starting options download job")
    logger.info("Configured tickers: %s", ", ".join(tickers))
    logger.info("Expiration filters: start=%r, end=%r, dates=%r", exp_start, exp_end, exp_dates)
    logger.info("Output directory name: %s", outdir_name)

    # Resolve snapshot date
    if not snap_date_str:
        snap_dt = datetime.utcnow()
        logger.info(
            "snap_date is not set in config. Using current date as snap_date: %s",
            snap_dt.date().isoformat(),
        )
    else:
        snap_dt = parse_date(snap_date_str)
        logger.info("Using snap_date from config: %s", snap_dt.date().isoformat())

    # Base output directory is resolved relative to the repository root
    outdir = BASE_DIR / outdir_name
    ensure_dir(outdir)
    logger.debug("Ensured base output directory exists: %s", outdir)

    # Determine common output file name (same for all tickers)
    if exp_dates:
        tag = "_".join(sorted([parse_date(d).strftime("%Y-%m-%d") for d in exp_dates]))
        out_name = f"options_exact_{tag}_filtered.csv"
    elif exp_start or exp_end:
        s = parse_date(exp_start).strftime("%Y-%m-%d") if exp_start else "MIN"
        e = parse_date(exp_end).strftime("%Y-%m-%d") if exp_end else "MAX"
        out_name = f"options_range_{s}_to_{e}_filtered.csv"
    else:
        out_name = "options_all_expirations_filtered.csv"

    all_results: list[pd.DataFrame] = []

    for ticker in tickers:
        logger.info("Processing ticker: %s", ticker)

        t = yf.Ticker(ticker)
        all_exps = t.options or []
        logger.info("Available expirations for %s: %d", ticker, len(all_exps))

        exps = pick_expirations(all_exps, exp_start, exp_end, exp_dates)

        # If nothing specified — use all expirations
        if not exps and not (exp_start or exp_end or exp_dates):
            exps = all_exps
            logger.info(
                "No filters and pick_expirations returned empty, falling back to all expirations (%d)",
                len(exps),
            )

        if not exps:
            logger.warning("No matching expirations for ticker %s, skipping", ticker)
            continue

        logger.info("Expirations to process for %s: %d", ticker, len(exps))

        frames: list[pd.DataFrame] = []
        for exp in exps:
            logger.info("Loading expiration %s for %s", exp, ticker)
            df = load_for_expiration(ticker, exp)
            if df.empty:
                logger.warning("No data for %s @ %s", ticker, exp)
                continue
            frames.append(df)

        if not frames:
            logger.warning("No data loaded for any expiration for ticker %s", ticker)
            continue

        df_all = pd.concat(frames, ignore_index=True)
        logger.info("Total rows before filtering for %s: %d", ticker, len(df_all))

        df_filtered = apply_filters(df_all)
        df_filtered["ticker"] = ticker

        logger.info(
            "Finished ticker %s: rows before=%d, after=%d",
            ticker,
            len(df_all),
            len(df_filtered),
        )

        all_results.append(df_filtered)

    if not all_results:
        logger.warning("No data collected for any ticker, nothing to save")
        return

    df_final = pd.concat(all_results, ignore_index=True)

    # Enrich with tenor_days and snap_date using snap_dt
    df_final = enrich_with_tenor_and_snap_date(df_final, snap_dt)

    out_path = outdir / out_name
    df_final.to_csv(out_path, index=False)

    logger.info(
        "Options download job completed: total rows=%d, saved to %s",
        len(df_final),
        out_path,
    )


if __name__ == "__main__":
    cfg = load_config()
    run(cfg)
