# file: add_fair_value_from_buckets.py

import sys
import logging
from pathlib import Path
from datetime import datetime, date

import pandas as pd
from helper import load_config  # shared config loader


# Configure basic logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


def parse_relative_strike(value):
    """
    Convert relative strike from strings like '69.44%' or '69,44%' to float.
    If it's already numeric, just cast to float.
    Returns None if value is NaN.
    """
    if pd.isna(value):
        return None

    if isinstance(value, str):
        v = value.strip().replace('%', '').replace(' ', '')
        v = v.replace(',', '.')  # comma -> dot
        return float(v)

    return float(value)


def load_summary_for(key, base_stock_dir: Path, cache: dict):
    """
    Load and cache bucket summary DataFrame for given (run_date, ticker).
    This avoids reading the same CSV multiple times.

    key: (run_date: date, ticker: str)

    Expected file path:
      <base_stock_dir>/csv_out/YYYY/MM/DD/enriched/<TICKER>/<TICKER>_strike_buckets_summary.csv

    Expected columns in summary CSV:
      - lower
      - upper
      - max_tenor_for_strike
    """
    if key in cache:
        return cache[key]

    run_date, ticker = key
    year = run_date.year
    month = run_date.month
    day = run_date.day

    summary_path = (
            base_stock_dir
            / "csv_out"
            / f"{year:04d}"
            / f"{month:02d}"
            / f"{day:02d}"
            / "enriched"
            / ticker
            / f"{ticker}_strike_buckets_summary.csv"
    )

    if not summary_path.exists():
        logging.warning(
            "Summary file not found for ticker=%s date=%s at path=%s",
            ticker,
            run_date,
            summary_path,
        )
        cache[key] = None
        return None

    logging.info("Loading summary file for ticker=%s from %s", ticker, summary_path)
    df_summary = pd.read_csv(summary_path)
    logging.info(
        "Loaded %d rows from summary file for ticker=%s",
        len(df_summary),
        ticker,
    )

    # Basic sanity check for expected columns
    required_cols = {"lower", "upper", "max_tenor_for_strike"}
    missing = required_cols.difference(df_summary.columns)
    if missing:
        logging.error(
            "Summary file %s for ticker=%s is missing columns: %s",
            summary_path,
            ticker,
            ", ".join(sorted(missing)),
        )
        cache[key] = None
        return None

    # Ensure numeric types
    df_summary["lower"] = pd.to_numeric(df_summary["lower"], errors="coerce")
    df_summary["upper"] = pd.to_numeric(df_summary["upper"], errors="coerce")
    df_summary["max_tenor_for_strike"] = pd.to_numeric(
        df_summary["max_tenor_for_strike"], errors="coerce"
    )

    cache[key] = df_summary
    return cache[key]


def parse_config_date(value) -> date:
    """
    Normalize valuation_date from config to a date object.

    Accepts:
      - datetime.date (returned e.g. by yaml.safe_load)
      - str in 'YYYY-MM-DD' format
    """
    if isinstance(value, date):
        return value

    if isinstance(value, str):
        try:
            dt = datetime.strptime(value, "%Y-%m-%d")
            return dt.date()
        except ValueError as e:
            logging.error(
                "Invalid date format in config for valuation_date='%s': %s",
                value,
                e,
            )
            sys.exit(1)

    logging.error(
        "Unsupported type for valuation_date in config: %r (type=%s)",
        value,
        type(value),
    )
    sys.exit(1)


def resolve_run_date(cfg) -> date:
    """
    Resolve valuation_date from config.
    If missing or empty, use today's date.
    """
    raw = cfg.get("valuation_date")

    if raw is None:
        today = date.today()
        logging.info(
            "valuation_date is not set in config, using today's date: %s", today
        )
        return today

    if isinstance(raw, str) and raw.strip() == "":
        today = date.today()
        logging.info(
            "valuation_date is empty string in config, using today's date: %s",
            today,
        )
        return today

    # Otherwise, parse with standard logic
    return parse_config_date(raw)


def main():
    # __file__ is in: <project_root>/src/add_fair_value_from_buckets.py
    src_dir = Path(__file__).resolve().parent
    project_root = src_dir.parent

    # Config file path (shared parameters.yaml in project_root/config)
    config_path = project_root / "config" / "parameters.yaml"

    logging.info("Loading configuration from explicit path: %s", config_path)
    cfg = load_config(config_path)

    # Expect portfolio_file_name in parameters.yaml
    if "portfolio_file_name" not in cfg:
        logging.error("Missing 'portfolio_file_name' in config")
        sys.exit(1)

    portfolio_file_name = cfg["portfolio_file_name"]
    run_date = resolve_run_date(cfg)

    # --- Resolve input dir (where portfolio CSV lives) ---
    candidate_input_dirs = [
        src_dir / "input",       # <project_root>/src/input
        project_root / "input",  # <project_root>/input
    ]

    portfolio_file = None
    input_dir = None

    for cand_dir in candidate_input_dirs:
        cand_file = cand_dir / portfolio_file_name
        if cand_file.exists():
            input_dir = cand_dir
            portfolio_file = cand_file
            break

    if portfolio_file is None:
        logging.error(
            "Portfolio file '%s' not found in any of these locations: %s",
            portfolio_file_name,
            ", ".join(str(d / portfolio_file_name) for d in candidate_input_dirs),
        )
        sys.exit(1)

    # --- Resolve stock/csv_out root (where csv_out lives) ---
    candidate_stock_dirs = [
        src_dir / "stock",       # <project_root>/src/stock
        project_root,            # <project_root> (e.g. project_root/csv_out/...)
        project_root / "stock",  # <project_root>/stock
    ]

    stock_dir = None
    for cand_dir in candidate_stock_dirs:
        if (cand_dir / "csv_out").exists():
            stock_dir = cand_dir
            break

    if stock_dir is None:
        logging.error(
            "Could not find 'csv_out' directory under any of these roots: %s",
            ", ".join(str(d) for d in candidate_stock_dirs),
        )
        sys.exit(1)

    logging.info("Starting fair value calculation script")
    logging.info("Project root: %s", project_root)
    logging.info("Src dir: %s", src_dir)
    logging.info("Config file path: %s", config_path)
    logging.info("Resolved input dir: %s", input_dir)
    logging.info("Portfolio file path: %s", portfolio_file)
    logging.info("Resolved stock root (csv_out parent): %s", stock_dir)
    logging.info("Using valuation_date for lookup: %s", run_date)

    # Load portfolio
    df = pd.read_csv(portfolio_file)
    logging.info("Loaded portfolio with %d rows", len(df))

    # Log tickers found in portfolio
    unique_tickers = sorted(df["ticker"].astype(str).unique())
    logging.info("Tickers in portfolio: %s", ", ".join(unique_tickers))

    # Cache for already loaded bucket summaries (key: (run_date, ticker))
    summary_cache: dict = {}

    def compute_fair_value(row):
        """
        Compute fair_value for a single portfolio row using
        strike bucket summary for (run_date, ticker).

        fair_value is taken from 'max_tenor_for_strike' in the bucket where:
          lower <= relative_strike < upper
        """
        ticker = str(row["ticker"])
        key = (run_date, ticker)

        summary_df = load_summary_for(key, stock_dir, summary_cache)
        if summary_df is None:
            logging.warning(
                "No summary data available for ticker=%s, setting fair_value=NaN",
                ticker,
            )
            return float("nan")

        rel = parse_relative_strike(row["relative_strike"])
        if rel is None:
            logging.warning(
                "relative_strike is NaN for trade_id=%s, ticker=%s, setting fair_value=NaN",
                row.get("trade_id", "N/A"),
                ticker,
            )
            return float("nan")

        # Bucket condition over relative_strike: lower <= rel < upper
        mask = (summary_df["lower"] <= rel) & (summary_df["upper"] > rel)
        match = summary_df[mask]

        if match.empty:
            logging.warning(
                "No matching bucket for ticker=%s date=%s rel=%s (trade_id=%s)",
                ticker,
                run_date,
                rel,
                row.get("trade_id", "N/A"),
            )
            return float("nan")

        if len(match) > 1:
            logging.info(
                "Multiple matching buckets found for ticker=%s rel=%s, using first row",
                ticker,
                rel,
            )

        fair_value = match["max_tenor_for_strike"].iloc[0]
        logging.debug(
            "Matched bucket for ticker=%s rel=%s -> fair_value=%s",
            ticker,
            rel,
            fair_value,
        )
        return fair_value

    # Compute fair_value for each row in the portfolio
    logging.info("Calculating fair_value for each portfolio row...")
    df["fair_value"] = df.apply(compute_fair_value, axis=1)

    # Build output file name based on input file name
    output_file = input_dir / f"{Path(portfolio_file_name).stem}_with_fair_value.csv"
    df.to_csv(output_file, index=False)

    logging.info("Saved portfolio with fair_value to: %s", output_file)
    logging.info("Script finished successfully")


if __name__ == "__main__":
    main()
