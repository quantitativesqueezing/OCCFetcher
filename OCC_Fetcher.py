#!/usr/bin/env python3
"""
Fetch OCC "New Listings" options report for the current month and list
unique tickers that activate today (EST), with an option to limit results
to the primary exchanges.

The script starts from the public page, discovers the monthly CSV link for
the current month (EST), downloads the data, filters it, and prints the
resulting tickers.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
from collections import OrderedDict
from dataclasses import dataclass
from datetime import datetime
from io import StringIO
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence
from urllib.parse import parse_qs, urljoin, urlparse

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
    "https://www.theocc.com/market-data/market-data-reports/series-and-trading-data/new-listings"
)
EST = ZoneInfo("America/New_York")
PRIMARY_EXCHANGES = {"CBOE", "AMEX", "ARCA"}


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
    Determine whether the row's date matches the target day.
    """
    return row_date == today


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
        exchange_upper = exchange.upper()
        flag = (row.get("N/E") or "").strip().upper()

        listing = Listing(
            ticker=ticker,
            date=row_date,
            company=company,
            exchange=exchange,
            flag=flag,
        )

        existing = dedup.get(ticker)
        if existing is None:
            dedup[ticker] = listing
            continue

        if row_date < existing.date:
            dedup[ticker] = listing
            continue

        if row_date == existing.date:
            existing_primary = existing.exchange.upper() in PRIMARY_EXCHANGES
            current_primary = exchange_upper in PRIMARY_EXCHANGES
            if current_primary and not existing_primary:
                dedup[ticker] = listing

    return dedup


def filter_primary_exchanges(listings: Iterable[Listing]) -> Iterable[Listing]:
    """
    Retain only listings originating from the primary exchanges.
    """
    return [
        listing
        for listing in listings
        if listing.exchange.upper() in PRIMARY_EXCHANGES
    ]


def resolve_env_var(name: str) -> Optional[str]:
    """
    Try to find an environment variable, falling back to values defined in .env.
    """
    value = os.environ.get(name)
    if value:
        return value.strip()

    env_path = Path(__file__).resolve().with_name(".env")
    if not env_path.exists():
        return None

    try:
        with env_path.open("r", encoding="utf-8") as handle:
            collecting = False
            buffer: List[str] = []
            key_prefix = f"{name}="

            for raw_line in handle:
                if collecting:
                    stripped = raw_line.strip()
                    if not stripped:
                        break
                    if re.match(r"^[A-Za-z0-9_]+\s*=", raw_line):
                        break
                    buffer.append(raw_line)
                elif raw_line.startswith(key_prefix):
                    collecting = True
                    buffer.append(raw_line[len(key_prefix) :])

            if not buffer:
                return None

            block = "".join(buffer)
            url_match = re.search(r"https?://[^\s\"']+", block)
            if url_match:
                return url_match.group(0)

            stripped_block = block.strip()
            if not stripped_block:
                return None
            if stripped_block.startswith('"') and stripped_block.endswith('"'):
                return stripped_block.strip('"')
            return stripped_block.split()[0]
    except OSError:
        return None


def parse_arguments(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    """
    Parse CLI arguments controlling output format and optional Discord posting.
    """
    parser = argparse.ArgumentParser(
        description="Fetch OCC 'New Listings' report and emit structured JSON."
    )
    parser.add_argument(
        "--include-all",
        dest="primary_only",
        action="store_false",
        help="Include all exchanges instead of limiting to primary exchanges.",
    )
    parser.set_defaults(primary_only=True)
    parser.add_argument(
        "--discord",
        dest="post_to_discord",
        action="store_true",
        help="Post the JSON report to a Discord webhook after generation.",
    )
    parser.add_argument(
        "--test",
        dest="use_test_webhook",
        action="store_true",
        help=(
            "When posting to Discord, prefer the DISCORD_WEBHOOK_TEST_URL "
            "environment variable."
        ),
    )
    parser.add_argument(
        "--discord-webhook",
        dest="discord_webhook",
        help=(
            "Discord webhook URL to use when --discord is supplied. "
            "Defaults to the DISCORD_WEBHOOK_URL environment variable."
        ),
    )
    parser.add_argument(
        "--discord-template",
        dest="discord_template",
        default="discord_webhook_object.json",
        help=(
            "Path to the Discord webhook JSON template. "
            "Defaults to discord_webhook_object.json."
        ),
    )
    return parser.parse_args(argv)


def sort_listings(listings: Iterable[Listing]) -> List[Listing]:
    """
    Sort listings consistently by activation date and ticker symbol.
    """
    return sorted(listings, key=lambda item: (item.date, item.ticker))


def build_report(
    csv_url: str,
    today: datetime.date,
    listings: Iterable[Listing],
    primary_only: bool,
) -> Dict[str, Any]:
    """
    Assemble a JSON-serializable report structure from the collected listings.
    """
    sorted_listings = sort_listings(listings)
    tickers = [listing.ticker for listing in sorted_listings]
    latest_date = (
        sorted_listings[-1].date.isoformat() if sorted_listings else None
    )

    listings_payload = [
        {
            "ticker": listing.ticker,
            "activation_date": listing.date.isoformat(),
            "company": listing.company,
            "exchange": listing.exchange,
            "flag": listing.flag,
        }
        for listing in sorted_listings
    ]

    return {
        "metadata": {
            "generated_at": datetime.now(EST).isoformat(),
            "activation_date": today.isoformat(),
            "activation_window": f"{today.isoformat()} (EST)",
            "primary_exchanges_only": primary_only,
            "source_csv": csv_url,
        },
        "listings": listings_payload,
        "summary": {
            "total": len(listings_payload),
            "tickers": tickers,
            "latest_activation_date": latest_date,
        },
    }


def load_discord_template(path: str) -> Any:
    """
    Read and parse the Discord webhook payload template from disk.
    """
    try:
        with open(path, "r", encoding="utf-8") as handle:
            return json.load(handle)
    except OSError as exc:
        raise RuntimeError(f"Unable to read Discord template at '{path}': {exc}") from exc
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            f"Discord template '{path}' does not contain valid JSON: {exc}"
        ) from exc


def _format_template(value: Any, context: Dict[str, str]) -> Any:
    if isinstance(value, str):
        try:
            return value.format(**context)
        except KeyError as exc:
            missing = exc.args[0]
            raise RuntimeError(
                f"Discord template references missing placeholder '{{{missing}}}'."
            ) from exc
    if isinstance(value, list):
        return [_format_template(item, context) for item in value]
    if isinstance(value, dict):
        return {key: _format_template(inner, context) for key, inner in value.items()}
    return value


def prepare_discord_payload(report: Dict[str, Any], template_path: str) -> Dict[str, Any]:
    """
    Replace placeholders in the Discord template using the generated report.
    """
    template = load_discord_template(template_path)
    summary = report.get("summary", {})
    metadata = report.get("metadata", {})

    tickers: List[str] = summary.get("tickers", []) or []
    if tickers:
        added_value = ", ".join(tickers)
        latest_activation = summary.get("latest_activation_date") or metadata.get(
            "activation_date"
        )
    else:
        added_value = "No qualifying tickers in the current window."
        latest_activation = metadata.get("activation_date")

    context = {
        "latest_date": latest_activation or "",
        "added": added_value,
    }
    return _format_template(template, context)


def post_to_discord(webhook_url: str, payload: Dict[str, Any]) -> None:
    """
    Send the prepared payload to the Discord webhook URL.
    """
    try:
        response = requests.post(webhook_url, json=payload, timeout=30)
        response.raise_for_status()
    except requests.RequestException as exc:
        raise RuntimeError(f"Failed to post Discord message: {exc}") from exc


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_arguments(argv)
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

    listings: List[Listing] = list(listings_map.values())
    if args.primary_only:
        listings = list(filter_primary_exchanges(listings))

    report = build_report(csv_url, today_est, listings, args.primary_only)
    print(json.dumps(report, indent=2))

    if args.post_to_discord:
        webhook_url: str
        if args.discord_webhook:
            webhook_url = args.discord_webhook
        else:
            env_var = (
                "DISCORD_WEBHOOK_TEST_URL"
                if args.use_test_webhook
                else "DISCORD_WEBHOOK_URL"
            )
            webhook_url = resolve_env_var(env_var) or ""
        if not webhook_url:
            if args.use_test_webhook and not args.discord_webhook:
                raise RuntimeError(
                    "--discord requested but no webhook URL provided. "
                    "Pass --discord-webhook, set DISCORD_WEBHOOK_TEST_URL, "
                    "or define it in .env."
                )
            raise RuntimeError(
                "--discord requested but no webhook URL provided. "
                "Pass --discord-webhook, set DISCORD_WEBHOOK_URL, "
                "or define it in .env."
            )
        payload = prepare_discord_payload(report, args.discord_template)
        post_to_discord(webhook_url, payload)

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except RuntimeError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        raise SystemExit(1)
