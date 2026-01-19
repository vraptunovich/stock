# 📈 Options Data Processing Pipeline

This repository contains an automated end-to-end pipeline for downloading, processing, enriching, and aggregating options chain data from Yahoo Finance.  
The pipeline runs both locally and via GitHub Actions, producing ready-to-use CSV datasets and artifacts.

---

## 🚀 Features

### 1. **Download Options Chains**

**`options_to_csv.py`**:

- Fetches option chains from Yahoo Finance
- Applies expiration filters:
    - exact dates (`exp_dates`)
    - date range (`exp_start → exp_end`)
    - all expirations for extracted date if filters are not provided
- Normalizes columns into a consistent format
- Saves data into per-ticker directories:

  ```text
  csv_out/<TICKER>/<file>.csv
    ```
  ## 2. Add Tenor (Days to Expiration)

**`add_tenor_days.py`**

Computes:

```text
tenor_days = expiration_date − snap_date
```
Adds:
- tenor_days
- snap_date (taken from parameters.yaml, or current UTC date if not set)
- Overwrites the original CSV files.

## 3. Add max_tenor

**`add_max_tenor.py`** 

Computes:

```text
max_tenor_for_strike = MAX(tenor_days WHERE strike == current_strike)
```
Equivalent to Excel:
```text
=MAXIFS(S:S, D:D, D2)
```
Adds max_tenor_for_strike per row and overwrites the CSV files.

## 4. Add relative strike

Computes:

```text
relative_strike = (current_strike/spot_price) * 100
```
Equivalent to Excel:
```text
=ABS(D2/spot_price) *100
```
Adds relative_strike per row and overwrites the CSV files.

## 5. Strike Bucket Aggregation

**`aggregate_strike_buckets.py`** 

- Reads bucket definitions from `config/strike_buckets.yaml`
- Aggregates (e.g. `max(relative_strike)`) per bucket

Produces per-ticker summary CSV:

```text
csv_out/<TICKER>/<TICKER>_strike_buckets_summary.csv
```

## 6. Repository Structure
```text
.
├── src/
│   ├── options_to_csv.py
│   ├── add_tenor_days.py
│   ├── add_relative_strike.py
│   ├── add_max_tenor_for_strike.py      # or add_max_tenor.py
│   ├── strike_buckets_summary.py        # or aggregate_strike_buckets.py
│   └── helper.py
│
├── config/
│   ├── parameters.yaml
│   └── strike_buckets.yaml
│
├── csv_out/                             # runtime output + dated snapshots
│   ├── <TICKER>/...                     # local runs / working area
│   └── YYYY/
│       └── MM/
│           └── DD/
│               └── enriched/
│                   ├── AAPL/...
│                   ├── MSFT/...
│                   └── ...
│
├── _strike_buckets_summary.csv          # optional global summary
│
├── .github/
│   └── workflows/
│       ├── options_pre_close.yml        # pre-close snapshot
│       └── options_post_close.yml       # post-close enrichment
│
└── README.md
```
## ⚙️ 7. Configuration
parameters.yaml

Example:
```yaml
tickers:
  - AAPL
  - MSFT

outdir: csv_out

# Optional: if not set, current UTC date is used
snap_date: "2025-01-01"

# Optional per-ticker overrides for spot price.
# If a ticker is not present here, spot_price is taken from yfinance.
spot_price_overrides:
  AAPL: 247.77
  MSFT: 410.25

# Expiration filters
exp_start: "2025-08-01"
exp_end: "2028-10-10"
exp_dates: []      # or a list of explicit expiration dates
```
Strike buckets:
```yaml
strike_buckets:
  - lower: 0.0
    upper: 25.0
  - lower: 25.0
    upper: 35.0
  # ...
  - lower: 175.0
    upper: 9999.0
```

## 🖥8. Running the Pipeline Locally
Run all steps
```bash 
python src/options_to_csv.py
python src/add_tenor_days.py
python src/add_relative_strike.py
python src/add_max_tenor_for_strike.py   # or add_max_tenor.py
python src/strike_buckets_summary.py     # or aggregate_strike_buckets.py
```
You can also run any individual step independently.

## ⚙️ 9. GitHub Actions Automation

The pipeline is split into two workflows:

### 1. `options_pre_close.yml` – pre-close snapshot

Runs ~30 minutes before US market close (via `cron`) or manually via `workflow_dispatch`.

Steps:

- Cleans today’s `csv_out/YYYY/MM/DD/enriched` if it exists
- Runs `options_to_csv.py`
- Copies current `csv_out/<TICKER>/...` into a dated snapshot:

  ```text
  csv_out/YYYY/MM/DD/<TICKER>/...
    ```
  Commits the snapshot via GITHUB_TOKEN
### 2. `options_post_close.yml` – post-close enrichment

Runs after US market close (via `cron`) or manually.

If there is **no snapshot for today** (`csv_out/YYYY/MM/DD`), the workflow stops with a clear message:
Please run the 'Options snapshot before US close' workflow first.

Otherwise it performs:

#### Workflow Steps

- Restores today’s snapshot into the working `csv_out/` directory
- Runs the enrichment scripts in order:

   ```text
   add_tenor_days.py
   add_max_tenor_for_strike.py   (or add_max_tenor.py)
   add_relative_strike.py
   strike_buckets_summary.py     (or aggregate_strike_buckets.py)
   ``` 
   Saves enriched data into:
    - csv_out/YYYY/MM/DD/enriched/<TICKER>/...
    - Removes raw data under csv_out/YYYY/MM/DD/* except enriched/
    - Commits updated csv_out/YYYY/MM/DD back to the repository
    - Uploads an artifact containing only today's enriched data: csv_out/YYYY/MM/DD/enriched/

## 🧪10. Dependencies

Install all required packages:

```bash
pip install -r requirements.txt
```
Required packages include (non-exhaustive):
- pandas
- yfinance
- pyyaml
- python-dateutil
- tzdata / zoneinfo (for time zones, depending on Python version)

Python 3.11 recommended

## 📦11. Artifacts

The post-close workflow uploads an artifact for the current run:

```text
options-csv-<run_number>.zip
└── csv_out/
    └── YYYY/
        └── MM/
            └── DD/
                └── enriched/
                    ├── AAPL/
                    │   ├── AAPL_options_*.csv
                    │   └── ...
                    ├── MSFT/
                    │   ├── MSFT_options_*.csv
                    │   └── ...
                    └── ...
```
This artifact contains only enriched data for the date on which the workflow was executed.
