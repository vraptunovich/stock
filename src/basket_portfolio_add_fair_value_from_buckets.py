#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import logging
from pathlib import Path
from datetime import date

import pandas as pd

from helper import load_config, ensure_dir, BASE_DIR

logger = logging.getLogger(__name__)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)


def compute_tenor_days(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        if df is None:
            return pd.DataFrame()
        if "tenor_days" not in df.columns:
            df["tenor_days"] = pd.Series(dtype="float64")
        return df

    df["trade_date"] = pd.to_datetime(df.get("trade_date"), errors="coerce")
    df["expiry"] = pd.to_datetime(df.get("expiry"), errors="coerce")
    df["tenor_days"] = (df["expiry"] - df["trade_date"]).dt.days
    return df


def parse_list_field(value) -> list[str]:
    if pd.isna(value):
        return []
    return [p.strip() for p in str(value).split(",") if p.strip()]


def resolve_run_date(cfg: dict) -> date:
    raw = cfg.get("valuation_date")
    if raw is None or (isinstance(raw, str) and raw.strip() == ""):
        return date.today()
    return pd.to_datetime(raw).date()


def find_stock_dir() -> Path:
    for d in (BASE_DIR, BASE_DIR / "stock"):
        if (d / "csv_out").exists():
            return d
    raise FileNotFoundError("csv_out directory not found")


def load_summary(run_date: date, ticker: str, stock_dir: Path, cache: dict) -> pd.DataFrame | None:
    key = (run_date, ticker)
    if key in cache:
        return cache[key]

    path = (
            stock_dir
            / "csv_out"
            / f"{run_date.year:04d}"
            / f"{run_date.month:02d}"
            / f"{run_date.day:02d}"
            / "enriched"
            / ticker
            / f"{ticker}_strike_buckets_summary.csv"
    )

    if not path.exists():
        cache[key] = None
        return None

    df = pd.read_csv(path)

    required = {"lower", "upper", "max_tenor_for_strike"}
    if not required.issubset(df.columns):
        cache[key] = None
        return None

    df["lower"] = pd.to_numeric(df["lower"], errors="coerce")
    df["upper"] = pd.to_numeric(df["upper"], errors="coerce")
    df["max_tenor_for_strike"] = pd.to_numeric(df["max_tenor_for_strike"], errors="coerce")

    cache[key] = df
    return df


def parse_bucket_midpoint(bucket: str) -> float | None:
    if pd.isna(bucket):
        return None
    b = str(bucket).strip()
    if "-" not in b:
        return None
    lo, hi = b.split("-", 1)
    try:
        return (float(lo) + float(hi)) / 2.0
    except Exception:
        return None


def add_max_tenor_and_fair_value(
        df: pd.DataFrame,
        run_date: date,
        stock_dir: Path,
) -> pd.DataFrame:
    if df is None or df.empty:
        df["max_tenor_for_strike"] = ""
        df["fair_value"] = ""
        return df

    cache: dict = {}

    def per_row(row):
        tickers = parse_list_field(row.get("baskettickers"))
        if not tickers:
            return "", ""

        rel = parse_bucket_midpoint(row.get("strike_bucket"))
        tenor_days = row.get("tenor_days")

        max_tenors: list[str] = []
        fair_values: list[str] = []

        for t in tickers:
            if rel is None or pd.isna(tenor_days):
                max_tenors.append("")
                fair_values.append("")
                continue

            summary = load_summary(run_date, t, stock_dir, cache)
            if summary is None:
                max_tenors.append("")
                fair_values.append("")
                continue

            match = summary[(summary["lower"] <= rel) & (summary["upper"] > rel)]
            if match.empty:
                max_tenors.append("")
                fair_values.append("")
                continue

            max_tenor = match["max_tenor_for_strike"].iloc[0]
            if pd.isna(max_tenor):
                max_tenors.append("")
                fair_values.append("")
                continue

            max_tenors.append(str(int(max_tenor)) if float(max_tenor).is_integer() else str(max_tenor))
            fair_values.append("2" if tenor_days <= max_tenor else "3")

        return ",".join(max_tenors), ",".join(fair_values)

    result = df.apply(per_row, axis=1, result_type="expand")
    df["max_tenor_for_strike"] = result[0]
    df["fair_value"] = result[1]

    return df


def process_all_csv(
        input_dir: Path,
        output_dir: Path,
        stock_dir: Path,
        run_date: date,
) -> None:
    ensure_dir(output_dir)

    for csv_path in sorted(input_dir.glob("*.csv")):
        try:
            logger.info("Processing %s", csv_path.name)
            df = pd.read_csv(csv_path)

            df = compute_tenor_days(df)
            df = add_max_tenor_and_fair_value(df, run_date, stock_dir)

            out_path = output_dir / csv_path.name
            df.to_csv(out_path, index=False)
            logger.info("Saved: %s", out_path)

        except Exception as exc:
            logger.exception("Failed processing %s: %s", csv_path, exc)


def main():
    cfg = load_config()

    input_dir = Path(cfg["basket_portfolio_input"])
    output_dir = Path(cfg["basket_portfolio_output"])
    run_date = resolve_run_date(cfg)
    stock_dir = find_stock_dir()

    if not input_dir.is_absolute():
        input_dir = BASE_DIR / input_dir
    if not output_dir.is_absolute():
        output_dir = BASE_DIR / output_dir

    process_all_csv(input_dir, output_dir, stock_dir, run_date)


if __name__ == "__main__":
    main()
