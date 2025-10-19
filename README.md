# OCC New Listings Fetcher

`occ_new_listings.py` is a small command line helper that finds the latest OCC “New Listings” options report, downloads the current month’s CSV, and prints the tickers that begin trading today, any future day, or within the prior two calendar days (all in Eastern Time).

## How It Works

1. Loads the public New Listings page and discovers the backing configuration endpoint dynamically.
2. Uses the configuration to:
   - Fetch the list of available report years.
   - Request the JSON feed containing CSV download links for the current year.
3. Chooses the CSV whose filename matches the current month (e.g., `october.csv`).
4. Downloads the CSV, parses it, filters rows by the activation window, and deduplicates tickers across exchanges (keeping the earliest activation date).
5. Prints the ticker symbol alongside its activation date, “new/existing” flag, company name, and exchange.

Cloudflare protects the OCC site, so the script relies on [`cloudscraper`](https://pypi.org/project/cloudscraper/) to retrieve all resources.

## Prerequisites

- Python 3.9+ (needs the standard library `zoneinfo` module).
- `pip install cloudscraper`

If you are re-running in a new environment:

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install cloudscraper
```

## Usage

```bash
python occ_new_listings.py
```

The script prints the source CSV URL, the evaluated activation window, and then each qualifying ticker. Duplicate tickers (multiple exchanges) are collapsed to a single entry, retaining the earliest qualifying activation date.

If today’s EST month has not been published yet, the script automatically falls back to the most recent year provided by OCC. If the CSV timestamp year differs from the selected year, a warning is emitted so you can double-check the data.
