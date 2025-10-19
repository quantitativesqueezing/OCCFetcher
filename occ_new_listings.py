#!/usr/bin/env python3
"""
Fetch OCC "New Listings" options report for the current month and list
unique tickers that activate today, in the future, or within the prior two days.

The script starts from the public page, discovers the monthly CSV link for
the current month (EST), downloads the data, filters it, and prints the
resulting tickers.
"""

from __future__ import annotations

import csv
import sys
import re
from collections import OrderedDict
from dataclasses import dataclass
from datetime import datetime, timedelta
from io import StringIO
from typing import Dict, Iterable, Optional
from urllib.parse import urljoin, urlparse, parse_qs

try:
    from zoneinfo import ZoneInfo
except Exception as exc:  # pragma: no cover - Python < 3.9 or misconfigured
    raise SystemExit("This script requires Python 3.9+ with zoneinfo support.") from exc

try:
    import cloudscraper
except ImportError:
    cloudscraper = None  # type: ignore[assignment]

import requests

BASE_URL = "https://www.theocc.com"
ENTRY_PAGE = (
    "https://www.theocc.com/market-data/market-data-reports/"
    "series-and-trading-data/new-listings"
)
EST = ZoneInfo("America/New_York")


@dataclass
class Listing:
    ticker: str
    date: datetime.date
    company: str
    exchange: str
    flag: str


def create_http_client() -> requests.Session:
    """
    Build an HTTP client that can pass OCC's Cloudflare challenge.
    """
    if cloudscraper is None:
        print(
            "The 'cloudscraper' package is required to reach the OCC site reliably.\n"
            "Install it with: pip install cloudscraper",
            file=sys.stderr,
        )
        sys.exit(1)

    session = cloudscraper.create_scraper()
    # Keep output deterministic and ensure we always accept CSV/JSON.
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0 Safari/537.36"
            ),
            "Accept": (
                "text/html,application/json,application/xml,"
                "text/csv,text/plain;q=0.9,*/*;q=0.8"
            ),
        }
    )
    return session


def discover_config_endpoint(html: str) -> str:
    """
    Locate the data configuration endpoint from the rendered page HTML.
    """
    match = re.search(
        r'id="market-data"[^>]*data-api="(?P<endpoint>[^"]+)"',
        html,
        re.IGNORECASE,
    )
    if not match:
        raise RuntimeError(
            "Unable to find the market-data configuration endpoint in the OCC page."
        )
    config_path = match.group("endpoint")
    return urljoin(BASE_URL, config_path)


def load_config(session: requests.Session) -> Dict:
    """
    Fetch the market data configuration JSON referenced by the page.
    """
    try:
        page_resp = session.get(ENTRY_PAGE, timeout=30)
        page_resp.raise_for_status()
    except requests.RequestException as exc:
        raise RuntimeError(f"Failed to load OCC entry page: {exc}") from exc

    config_url = discover_config_endpoint(page_resp.text)

    try:
        config_resp = session.get(config_url, timeout=30)
        config_resp.raise_for_status()
    except requests.RequestException as exc:
        raise RuntimeError(f"Failed to load OCC configuration JSON: {exc}") from exc

    try:
        return config_resp.json()
    except ValueError as exc:
        raise RuntimeError("Configuration endpoint did not return valid JSON.") from exc


def build_endpoint_url(base_path: str) -> str:
    """
    Construct a fully qualified URL for an endpoint defined in the configuration.
    """
    return urljoin(BASE_URL, base_path)


def locate_control(config: Dict, control_name: str) -> Dict:
    """
    Find a specific control definition by its name in the configuration.
    """
    for group in config.get("input", {}).get("groups", []):
        for control in group.get("controls", []):
            if control.get("name") == control_name:
                return control
    raise RuntimeError(f"Unable to find control definition for '{control_name}'.")


def determine_target_year(session: requests.Session, years_url: str) -> int:
    """
    Choose the appropriate report year, preferring the current EST year.
    """
    try:
        years_resp = session.get(years_url, timeout=30)
        years_resp.raise_for_status()
    except requests.RequestException as exc:
        raise RuntimeError(f"Unable to load available years: {exc}") from exc

    try:
        year_strings: Iterable[str] = years_resp.json()
    except ValueError as exc:
        raise RuntimeError("Years endpoint did not return valid JSON.") from exc

    years = sorted({int(value) for value in year_strings}, reverse=True)
    if not years:
        raise RuntimeError("No available years returned by OCC.")

    current_year = datetime.now(EST).year
    for year in years:
        if year == current_year:
            return year

    # If the current year does not exist yet, pick the latest year below it.
    for year in years:
        if year < current_year:
            return year

    # Fallback to the most recent year if everything else fails.
    return years[0]


def fetch_month_link(
    session: requests.Session,
    reports_url: str,
    query_param_map: Dict[str, str],
    month_slug: str,
) -> str:
    """
    Retrieve the CSV hyperlink for the requested month.
    """
    try:
        reports_resp = session.get(reports_url, params=query_param_map, timeout=30)
        reports_resp.raise_for_status()
    except requests.RequestException as exc:
        raise RuntimeError(f"Failed to fetch monthly report list: {exc}") from exc

    try:
        entries: Iterable[Dict[str, str]] = reports_resp.json()
    except ValueError as exc:
        raise RuntimeError("Report list is not valid JSON.") from exc

    month_key = f"{month_slug}.csv"
    for entry in entries:
        perm_url = entry.get("permamentUrl") or ""
        if month_key in perm_url.lower():
            return urljoin(BASE_URL, perm_url)

    raise RuntimeError(
        f"Could not find a CSV link containing '{month_key}' in this year's reports."
    )


def parse_ts_year(download_url: str) -> Optional[int]:
    """
    Extract the four-digit year from the ts=YYYYMMDDhhmm query parameter, if present.
    """
    parsed = urlparse(download_url)
    query = parse_qs(parsed.query)
    ts_values = query.get("ts")
    if not ts_values:
        return None
    ts = ts_values[0]
    if len(ts) >= 4 and ts[:4].isdigit():
        return int(ts[:4])
    return None


def fetch_csv(session: requests.Session, csv_url: str) -> str:
    """
    Download the CSV text for the located monthly report.
    """
    try:
        resp = session.get(csv_url, timeout=60)
        resp.raise_for_status()
    except requests.RequestException as exc:
        raise RuntimeError(f"Failed to download CSV data: {exc}") from exc

    # OCC sometimes serves CSV as text/plain; use text to handle BOM automatically.
    return resp.text


def within_window(row_date: datetime.date, today: datetime.date) -> bool:
    """
    Determine whether the row's date is within the allowed window.
    """
    window_start = today - timedelta(days=2)
    return row_date >= window_start


def parse_csv(csv_text: str, today: datetime.date) -> Dict[str, Listing]:
    """
    Parse the CSV and deduplicate tickers by earliest qualifying date.
    """
    dedup: Dict[str, Listing] = OrderedDict()

    reader = csv.DictReader(StringIO(csv_text))
    for row in reader:
        ticker = (row.get("Stock Symbol") or "").strip().upper()
        if not ticker:
            continue

        raw_date = (row.get("Date") or "").strip()
        try:
            row_date = datetime.strptime(raw_date, "%m/%d/%Y").date()
        except ValueError:
            # Ignore rows without a valid activation date.
            continue

        if not within_window(row_date, today):
            continue

        company = (row.get("Company") or "").strip()
        exchange = (row.get("Exchange") or "").strip()
        flag = (row.get("N/E") or "").strip().upper()

        listing = Listing(
            ticker=ticker,
            date=row_date,
            company=company,
            exchange=exchange,
            flag=flag,
        )

        existing = dedup.get(ticker)
        if existing is None or row_date < existing.date:
            dedup[ticker] = listing

    return dedup


def print_results(listings: Iterable[Listing], csv_url: str, today: datetime.date) -> None:
    """
    Output the deduplicated listings in a readable format.
    """
    window_start = today - timedelta(days=2)
    window_desc = f"{window_start.isoformat()} through future dates (EST)"

    print(f"OCC new listings sourced from: {csv_url}")
    print(f"Activation window: {window_desc}")
    print()

    sorted_listings = sorted(listings, key=lambda item: (item.date, item.ticker))
    if not sorted_listings:
        print("No qualifying tickers in the current window.")
        return

    for listing in sorted_listings:
        flag = f"{listing.flag}-listing" if listing.flag else "listing"
        print(
            f"{listing.ticker:<6} {listing.date.isoformat()}  "
            f"[{flag}]  {listing.company} (Exchange: {listing.exchange})"
        )


def main() -> int:
    session = create_http_client()
    config = load_config(session)

    try:
        reports_endpoint = config["submit"]["endpoints"][0]["endpoint"]["prod"]
    except (KeyError, IndexError, TypeError) as exc:
        raise RuntimeError("Unexpected OCC configuration structure.") from exc

    report_year_control = locate_control(config, "report_year")
    try:
        years_endpoint = report_year_control["data"]["endpoint"]["prod"]
    except (KeyError, TypeError) as exc:
        raise RuntimeError("Year control does not expose an endpoint URL.") from exc

    years_url = build_endpoint_url(years_endpoint)
    reports_url = build_endpoint_url(reports_endpoint)

    target_year = determine_target_year(session, years_url)

    # Build query parameters based on config mapping.
    query_values = {"report_type": "options", "report_year": str(target_year)}
    try:
        query_items = config["submit"]["endpoints"][0]["query"]
    except (KeyError, IndexError, TypeError) as exc:
        raise RuntimeError("Missing query definition in OCC configuration.") from exc

    query_params: Dict[str, str] = {}
    for key, value_spec in query_items:
        if isinstance(value_spec, dict) and value_spec.get("dynamic"):
            source_key = value_spec.get("value")
            if source_key not in query_values:
                raise RuntimeError(f"No value defined for dynamic field '{source_key}'.")
            query_params[key] = query_values[source_key]
        else:
            query_params[key] = str(value_spec)

    today_est = datetime.now(EST).date()
    month_slug = today_est.strftime("%B").lower()
    csv_url = fetch_month_link(session, reports_url, query_params, month_slug)

    ts_year = parse_ts_year(csv_url)
    if ts_year and ts_year != target_year:
        print(
            f"Warning: CSV timestamp year ({ts_year}) differs from selected year ({target_year}).",
            file=sys.stderr,
        )

    csv_text = fetch_csv(session, csv_url)
    listings_map = parse_csv(csv_text, today_est)
    print_results(listings_map.values(), csv_url, today_est)

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except RuntimeError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        raise SystemExit(1)
