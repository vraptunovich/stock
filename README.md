📈 Options Data Processing Pipeline
<p align="center"> <img src="https://img.shields.io/badge/Python-3.11-blue?logo=python" /> <img src="https://img.shields.io/badge/Status-Automated-green?logo=githubactions" /> <img src="https://img.shields.io/badge/Data-Yahoo%20Finance-yellow?logo=yahoo" /> <img src="https://img.shields.io/badge/CI-GitHub%20Actions-black?logo=github" /> </p>

This repository contains a fully automated data pipeline for downloading, processing, enriching, and aggregating options chain data from Yahoo Finance.
The pipeline runs locally or via GitHub Actions, producing ready-to-use CSV datasets and downloadable artifacts.

🏗 Pipeline Architecture (Flowchart)
flowchart TD

A[Load parameters.yaml] --> B[Download option chains\n(options_to_csv.py)]
B --> C[Add tenor_days\n(add_tenor_days.py)]
C --> D[Add relative_strike\n(add_relative_strike.py)]
D --> E[Add max_tenor_for_strike\n(add_max_tenor_for_strike.py)]
E --> F[Aggregate strike buckets\n(strike_buckets_summary.py)]
F --> G[Export ZIP artifact\nGitHub Actions]

✨ Features
✔ Download clean option chains
✔ Compute tenor (days to expiration)
✔ Compute relative strike
✔ MAXIFS-style aggregation per strike
✔ Bucket-based aggregation
✔ Fully automated CI/CD with artifacts
✔ Modular scripts, logging, configs
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
├── docs/
│   ├── pipeline.md
│   ├── configuration.md
│   ├── architecture.md
│   └── usage_examples.md
│
├── csv_out/
│   └── <TICKER>/
│       ├── *.csv
│       └── <TICKER>_strike_buckets_summary.csv
│
├── .github/workflows/pipeline.yml
├── README.md
└── requirements.txt

⚙️ Configuration
config/parameters.yaml
tickers:
- AAPL
- MSFT

outdir: csv_out

snap_date: "2025-01-01"
spot_price: 247.77

exp_start: "2025-08-01"
exp_end: "2028-10-10"
exp_dates: []

config/strike_buckets.yaml
strike_buckets:
- lower: 0.0
  upper: 25.0
- lower: 25.0
  upper: 35.0
  ...
- lower: 175.0
  upper: 9999.0

🚀 Installation
1. Clone the repo
   git clone <repo-url>
   cd <repo-folder>

2. Create virtual environment
   python3 -m venv .venv
   source .venv/bin/activate   # Linux/Mac
   .venv\Scripts\activate      # Windows

3. Install dependencies
   pip install -r requirements.txt

4. Verify installation
   python --version
   pip list

▶️ Running the Pipeline Locally

Run full pipeline in order:

python src/options_to_csv.py
python src/add_tenor_days.py
python src/add_relative_strike.py
python src/add_max_tenor_for_strike.py
python src/strike_buckets_summary.py


Run a single stage:

python src/add_relative_strike.py

🤖 GitHub Actions Automation

The repository includes a workflow:

.github/workflows/pipeline.yml
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


Artifacts appear under GitHub → Actions → Run → Artifacts.

🧪 Dependencies
pandas
yfinance
pyyaml


Recommended: Python 3.11

🔍 Logging

All scripts use a unified logging setup:

%(asctime)s [%(levelname)s] %(name)s - %(message)s


Full logs visible in GitHub Actions.

📦 Artifacts Structure

Downloadable ZIP looks like:

options-data.zip
└── csv_out/
└── AAPL/
├── aapl_options_filtered.csv
├── aapl_options_tenor.csv
├── aapl_options_relative_strike.csv
├── aapl_max_tenor_for_strike.csv
└── AAPL_strike_buckets_summary.csv

📚 Documentation (docs/ folder)
docs/pipeline.md

Detailed description of each pipeline stage.

docs/configuration.md

Full explanation of YAML config fields.

docs/architecture.md

System architecture, diagrams, data flows.

docs/usage_examples.md

Real examples of CSV transformations.

🧬 Full Pipeline Diagram (Detailed)
graph LR

A[Start] --> B[Load configs<br>parameters.yaml<br>strike_buckets.yaml]
B --> C[Fetch options chain<br>(yfinance)]
C --> D[Normalize & filter<br>CSV export per ticker]
D --> E[Compute tenor_days<br>snap_date applied]
E --> F[Compute relative_strike<br>spot_price applied]
F --> G[Compute max_tenor_for_strike<br>(MAXIFS)]
G --> H[Aggregate strike buckets<br>summary CSV]
H --> I[Upload artifact ZIP]
I --> J[Finish]

📄 License

MIT (or specify another if desired).