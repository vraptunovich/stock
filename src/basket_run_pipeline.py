#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import subprocess
import sys
from pathlib import Path
from datetime import datetime, date
import shutil

from helper import load_config  # to read parameters.yaml

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)


def run_cmd(cmd, cwd: Path | None = None):
    logging.info("Running: %s", " ".join(cmd))
    result = subprocess.run(cmd, cwd=str(cwd) if cwd else None)
    if result.returncode != 0:
        logging.error("Command failed with exit code %s", result.returncode)
        sys.exit(result.returncode)


def resolve_run_date(cfg: dict) -> date:
    """
    Resolve valuation_date from config.
    If missing or empty, use today's date.
    """
    raw = cfg.get("valuation_date")

    if raw is None:
        today = date.today()
        logging.info(
            "valuation_date is not set in config, using today's date: %s",
            today,
        )
        return today

    if isinstance(raw, str) and raw.strip() == "":
        today = date.today()
        logging.info(
            "valuation_date is empty in config, using today's date: %s",
            today,
        )
        return today

    if isinstance(raw, date):
        return raw

    if isinstance(raw, str):
        try:
            dt = datetime.strptime(raw, "%Y-%m-%d")
            return dt.date()
        except ValueError as e:
            logging.warning(
                "Invalid valuation_date format '%s' in config (%s), using today's date",
                raw,
                e,
            )
            return date.today()

    logging.warning(
        "Unsupported valuation_date type in config: %r (type=%s), using today's date",
        raw,
        type(raw),
    )
    return date.today()


def move_csv_out_to_enriched(repo_root: Path, run_date: date):
    """
    Move generated CSV data into dated enriched directory:

      csv_out/<ticker>/...  ->  csv_out/YYYY/MM/DD/enriched/<ticker>/...

    Only moves top-level children of csv_out that are not:
      - year-like directories (e.g. '2025')
      - 'enriched'
    """
    csv_root = repo_root / "csv_out"
    if not csv_root.exists():
        logging.warning("csv_out directory does not exist: %s", csv_root)
        return

    year = run_date.year
    month = run_date.month
    day = run_date.day

    dest_root = (
            csv_root
            / f"{year:04d}"
            / f"{month:02d}"
            / f"{day:02d}"
            / "enriched"
    )
    logging.info("Moving CSV data into: %s", dest_root)
    dest_root.mkdir(parents=True, exist_ok=True)

    for child in csv_root.iterdir():
        # Skip already structured year folders and 'enriched'
        if child.name == "enriched":
            continue
        if child.is_dir() and child.name.isdigit() and len(child.name) == 4:
            # looks like '2025' etc.
            continue

        target = dest_root / child.name
        logging.info("  • Moving %s -> %s", child, target)
        # shutil.move работает и с файлами, и с директориями
        shutil.move(str(child), str(target))

    logging.info("CSV data successfully moved to dated enriched directory")


def main():
    # This script is in the same folder as options_to_csv.py, add_tenor_days.py, etc.
    project_root = Path(__file__).resolve().parent  # e.g. .../stock/src
    src_dir = project_root
    repo_root = project_root.parent                 # e.g. .../stock

    logging.info("Project root (src dir): %s", project_root)
    logging.info("Repository root: %s", repo_root)

    # Load config to get valuation_date
    config_path = repo_root / "config" / "parameters.yaml"
    logging.info("Loading configuration from: %s", config_path)
    cfg = load_config(config_path)
    run_date = resolve_run_date(cfg)
    logging.info("Using valuation_date for folder structure: %s", run_date)

    # 1) Run options script
    run_cmd([sys.executable, str(src_dir / "options_to_csv.py")], cwd=repo_root)

    # 2) Add tenor_days column
    run_cmd([sys.executable, str(src_dir / "add_tenor_days.py")], cwd=repo_root)

    # 3) Add max_tenor_for_strike
    run_cmd([sys.executable, str(src_dir / "add_max_tenor.py")], cwd=repo_root)

    # 4) Add relative_strike
    run_cmd([sys.executable, str(src_dir / "add_relative_strike.py")], cwd=repo_root)

    # 5) Aggregate strike buckets
    run_cmd([sys.executable, str(src_dir / "aggregate_strike_buckets.py")], cwd=repo_root)

    # 6) Move all generated data into csv_out/YYYY/MM/DD/enriched/...
    move_csv_out_to_enriched(repo_root, run_date)

    # 7) Add fair value
    run_cmd([sys.executable, str(src_dir / "basket_portfolio_add_fair_value_from_buckets.py")], cwd=repo_root)

    logging.info("✅ Pipeline finished successfully")


if __name__ == "__main__":
    main()
