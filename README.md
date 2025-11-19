📈 Options Data Processing Pipeline

This repository contains an automated end-to-end pipeline for downloading, processing, enriching, and aggregating options chain data from Yahoo Finance.
The pipeline runs both locally and via GitHub Actions, producing ready-to-use CSV datasets and ZIP artifacts.

🚀 Features
1. Download Options Chains

options_to_csv.py:

Fetches option chains from Yahoo Finance

Applies expiration filters:

exact dates (exp_dates)

date range (exp_start → exp_end)

all expirations if filters are not provided

Normalizes columns into a consistent format

Saves data into per-ticker directories:

csv_out/<TICKER>/<file>.csv

2. Add Tenor (Days to Expiration)

add_tenor_days.py:

Computes

tenor_days = expiration_date − snap_date


Adds a snap_date column to every row

Overwrites the original CSV files

3. Calculate Relative Strike

add_relative_strike.py:

Computes

relative_strike = ABS(strike / spot_price) * 100


Adds spot_price column

Overwrites each CSV file

4. Add MAXIFS-style Aggregation

add_max_tenor_for_strike.py:

Computes

max_tenor_for_strike = MAX(tenor_days WHERE strike == current_strike)


Equivalent to Excel:
=MAXIFS(S:S, D:D, D2)

5. Strike Bucket Aggregation

strike_buckets_summary.py:

Reads bucket definitions from config/strike_buckets.yaml

Aggregates max(relative_strike) per bucket

Produces summary file:

csv_out/<TICKER>/<TICKER>_strike_buckets_summary.csv

📂 Repository Structure
.
├── src/
│   ├── options_to_csv.py
│   ├── add_tenor_days.py
│   ├── add_relative_strike.py
│   ├── add_max_tenor_for_strike.py
│   ├── strike_buckets_summary.py
│   ├── helper.py
│
├── config/
│   ├── parameters.yaml
│   ├── strike_buckets.yaml
│
├── csv_out/                  # auto-generated output
│   └── <TICKER>/
│       ├── *.csv
│       └── <TICKER>_strike_buckets_summary.csv
│
├── .github/
│   └── workflows/
│       └── pipeline.yml
│
└── README.md

⚙️ Configuration
parameters.yaml
tickers:
- AAPL
- MSFT

outdir: csv_out

snap_date: "2025-01-01"
spot_price: 247.77

exp_start: "2025-08-01"
exp_end: "2028-10-10"
exp_dates: []

strike_buckets.yaml
strike_buckets:
- lower: 0.0
  upper: 25.0
- lower: 25.0
  upper: 35.0
  ...
- lower: 175.0
  upper: 9999.0

▶️ Running the Pipeline Locally

Run all steps:

python src/options_to_csv.py
python src/add_tenor_days.py
python src/add_relative_strike.py
python src/add_max_tenor_for_strike.py
python src/strike_buckets_summary.py


Run a single step:

python src/add_relative_strike.py

🤖 GitHub Actions Automation

The pipeline can run automatically:

on a schedule (cron)

on push to main

with artifact export

Example workflow (.github/workflows/pipeline.yml):

on:
schedule:
- cron: "0 4 * * *"
push:
branches: [ "main" ]

jobs:
run-pipeline:
runs-on: ubuntu-latest

    steps:
      - uses: actions/checkout@v3

      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: "3.11"

      - name: Install dependencies
        run: pip install -r requirements.txt

      - name: Run pipeline
        run: |
          python src/options_to_csv.py
          python src/add_tenor_days.py
          python src/add_relative_strike.py
          python src/add_max_tenor_for_strike.py
          python src/strike_buckets_summary.py

      - name: Upload results
        uses: actions/upload-artifact@v3
        with:
          name: options-data
          path: csv_out/

🧪 Dependencies

Install using:

pip install -r requirements.txt


Required packages:

pandas

yfinance

pyyaml

zoneinfo (Python 3.9+)

Python 3.11 recommended

📜 Logging

All scripts use a shared logging format:

%(asctime)s [%(levelname)s] %(name)s - %(message)s


Logs appear in GitHub Actions in real time.

📦 Artifacts

GitHub Actions exports:

options-data.zip
└── csv_out/
└── <TICKER>/
├── *_filtered.csv
├── *_tenor.csv
├── *_relative.csv
├── *_max_tenor_for_strike.csv
└── <TICKER>_strike_buckets_summary.csv
