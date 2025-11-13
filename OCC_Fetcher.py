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
import calendar
import csv
import json
import os
import re
import sys
from collections import OrderedDict
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from functools import lru_cache
from io import StringIO
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple
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
LOG_APP_ID = "OCC_Fetcher"
LOG_FILE_PATH = (
    Path.home()
    / "Library"
    / "Mobile Documents"
    / "com~apple~CloudDocs"
    / "Development"
    / "Projects"
    / "OCC_Fetcher"
    / "Logs"
    / "webhook_post_log.json"
)


@dataclass
class Listing:
    ticker: str
    date: date
    company: str
    exchange: str
    flag: str


def _observed_date(original: date) -> date:
    """
    Shift fixed-date holidays that fall on weekends to their observed weekday.
    """
    if original.weekday() == 5:  # Saturday
        return original - timedelta(days=1)
    if original.weekday() == 6:  # Sunday
        return original + timedelta(days=1)
    return original


def _nth_weekday(year: int, month: int, weekday: int, occurrence: int) -> date:
    """
    Locate the nth weekday (1-indexed) in a given month.
    """
    if occurrence <= 0:
        raise ValueError("occurrence must be >= 1")
    count = 0
    for week in calendar.monthcalendar(year, month):
        day = week[weekday]
        if day == 0:
            continue
        count += 1
        if count == occurrence:
            return datetime(year, month, day).date()
    raise ValueError("Unable to locate requested weekday.")


def _last_weekday(year: int, month: int, weekday: int) -> date:
    """
    Locate the last occurrence of a weekday in a month.
    """
    for week in reversed(calendar.monthcalendar(year, month)):
        day = week[weekday]
        if day != 0:
            return datetime(year, month, day).date()
    raise ValueError("Unable to locate requested weekday.")


def _calculate_easter_sunday(year: int) -> date:
    """
    Anonymous Gregorian algorithm for Easter Sunday.
    """
    a = year % 19
    b = year // 100
    c = year % 100
    d = b // 4
    e = b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i = c // 4
    k = c % 4
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    month = (h + l - 7 * m + 114) // 31
    day = ((h + l - 7 * m + 114) % 31) + 1
    return datetime(year, month, day).date()


@lru_cache(maxsize=None)
def us_market_holidays(year: int) -> Set[date]:
    """
    Return the set of full-day US equity market holidays for the provided year.
    """
    holidays: Set[date] = set()

    def add(date_obj: date) -> None:
        holidays.add(date_obj)

    # Fixed-date holidays (with weekend observation).
    new_years_day = datetime(year, 1, 1).date()
    add(_observed_date(new_years_day))

    next_new_year = datetime(year + 1, 1, 1).date()
    observed_next_new_year = _observed_date(next_new_year)
    if observed_next_new_year.year == year:
        add(observed_next_new_year)

    mlk_day = _nth_weekday(year, 1, calendar.MONDAY, 3)
    add(mlk_day)

    presidents_day = _nth_weekday(year, 2, calendar.MONDAY, 3)
    add(presidents_day)

    good_friday = _calculate_easter_sunday(year) - timedelta(days=2)
    add(good_friday)

    memorial_day = _last_weekday(year, 5, calendar.MONDAY)
    add(memorial_day)

    juneteenth = datetime(year, 6, 19).date()
    add(_observed_date(juneteenth))

    independence_day = datetime(year, 7, 4).date()
    add(_observed_date(independence_day))

    labor_day = _nth_weekday(year, 9, calendar.MONDAY, 1)
    add(labor_day)

    thanksgiving = _nth_weekday(year, 11, calendar.THURSDAY, 4)
    add(thanksgiving)

    christmas = datetime(year, 12, 25).date()
    add(_observed_date(christmas))

    return holidays


def is_market_holiday(date_obj: date) -> bool:
    """
    Determine whether the provided date is a full US market holiday.
    """
    return date_obj in us_market_holidays(date_obj.year)


def is_trading_day(date_obj: date) -> bool:
    """
    Return True when the date represents a standard US trading session.
    """
    if date_obj.weekday() >= 5:  # Saturday/Sunday
        return False
    if is_market_holiday(date_obj):
        return False
    return True


def next_trading_day(date_obj: date) -> date:
    """
    Compute the next trading day strictly after the provided date.
    """
    candidate = date_obj + timedelta(days=1)
    while not is_trading_day(candidate):
        candidate += timedelta(days=1)
    return candidate


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


def determine_target_year(
    session: requests.Session,
    years_url: str,
    preferred_year: Optional[int] = None,
) -> int:
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

    if preferred_year is not None:
        for year in years:
            if year == preferred_year:
                return year

    current_year = preferred_year or datetime.now(EST).year
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


def parse_csv(csv_text: str, start_date: date) -> Dict[date, Dict[str, Listing]]:
    """
    Parse the CSV and deduplicate tickers for the start date and any later dates.
    """
    dedup: Dict[date, Dict[str, Listing]] = {}

    # OCC occasionally prepends a UTF-8 BOM, which causes DictReader to expose
    # header names like "\ufeffStock Symbol" and breaks downstream lookups.
    csv_text = csv_text.lstrip("\ufeff")

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

        if row_date < start_date:
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

        bucket = dedup.setdefault(row_date, OrderedDict())
        existing = bucket.get(ticker)
        if existing is None:
            bucket[ticker] = listing
            continue

        if row_date < existing.date:
            bucket[ticker] = listing
            continue

        if row_date == existing.date:
            existing_primary = existing.exchange.upper() in PRIMARY_EXCHANGES
            current_primary = exchange_upper in PRIMARY_EXCHANGES
            if current_primary and not existing_primary:
                bucket[ticker] = listing

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


def _read_env_block(name: str) -> Optional[str]:
    """
    Read the full value block for the specified variable from .env, preserving
    comments and indentation.
    """
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
                    if re.match(r"^[A-Za-z0-9_]+\s*=", raw_line):
                        break
                    buffer.append(raw_line)
                elif raw_line.startswith(key_prefix):
                    collecting = True
                    buffer.append(raw_line[len(key_prefix) :])

            if not buffer:
                return None
            return "".join(buffer).strip()
    except OSError:
        return None


def _parse_webhook_entries(value: str) -> List[Tuple[str, Optional[str]]]:
    """
    Parse a webhook configuration block of the form:

        ["https://..." => "thread_id", ...]

    Returning a list of (url, optional_thread_id) tuples.
    """
    entries: List[Tuple[str, Optional[str]]] = []
    cleaned_lines: List[str] = []

    for line in value.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        cleaned_lines.append(stripped)

    cleaned = "\n".join(cleaned_lines)
    for match in re.finditer(r'"([^"]+)"\s*=>\s*"([^"]*)"', cleaned):
        url = match.group(1).strip()
        thread = match.group(2).strip()
        if url:
            entries.append((url, thread or None))
    if entries:
        return entries

    # Fallback: attempt to locate URLs even if no explicit mapping syntax.
    url_match = re.search(r"https?://[^\s\",]+", cleaned)
    if url_match:
        url = url_match.group(0).strip()
        if url:
            entries.append((url, None))

    stripped = cleaned.strip()
    if not entries and stripped:
        token = stripped.strip('"').split()[0]
        if token:
            entries.append((token, None))

    return entries


def resolve_webhook_entry(name: str) -> Optional[Tuple[str, Optional[str]]]:
    """
    Deprecated single-entry resolver retained for backward compatibility.
    """
    entries = resolve_webhook_entries(name)
    return entries[0] if entries else None


def resolve_webhook_entries(name: str) -> List[Tuple[str, Optional[str]]]:
    """
    Try to resolve the webhook URL and optional thread ID from the environment or .env.
    """
    candidates: List[Tuple[str, Optional[str]]] = []

    raw_env = os.environ.get(name)
    if raw_env:
        entries = _parse_webhook_entries(raw_env)
        if entries:
            candidates.extend(entries)
        else:
            stripped = raw_env.strip()
            if stripped:
                candidates.append((stripped, None))

    if candidates:
        return candidates

    block = _read_env_block(name)
    if not block:
        return []

    entries = _parse_webhook_entries(block)
    if entries:
        return entries
    stripped_block = block.strip()
    if stripped_block:
        return [(stripped_block, None)]
    return []


def load_post_log(path: Path) -> Dict[str, str]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    if not isinstance(data, dict):
        return {}
    return {str(key): str(value) for key, value in data.items()}


def save_post_log(path: Path, data: Dict[str, str]) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(data, indent=2), encoding="utf-8")
    except OSError as exc:
        raise RuntimeError(f"Failed to write webhook post log {path}: {exc}") from exc


def parse_log_timestamp(raw: str) -> Optional[datetime]:
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw)
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=EST)
        return parsed
    except ValueError:
        pass
    try:
        parsed_date = datetime.strptime(raw, "%Y-%m-%d").date()
        return datetime.combine(parsed_date, datetime.min.time(), EST)
    except ValueError:
        return None


def _coerce_cli_webhook(value: str) -> List[Tuple[str, Optional[str]]]:
    """Interpret a CLI-provided webhook reference into (url, thread_id) tuples."""
    if not value:
        return []

    trimmed = value.strip()
    if not trimmed:
        return []

    # Support the associative mapping syntax used in .env.
    if "=>" in trimmed or trimmed.startswith("["):
        entries = _parse_webhook_entries(trimmed)
        return entries if entries else []

    # Allow simple "url|thread" CLI format.
    if "|" in trimmed:
        url_part, thread_part = trimmed.split("|", 1)
        url_part = url_part.strip()
        thread_part = thread_part.strip()
        if url_part:
            return [(url_part, thread_part or None)]
        return []

    return [(trimmed, None)]


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
        "--force",
        dest="force_post",
        action="store_true",
        help="Bypass the 24-hour Discord throttle and post immediately.",
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
    parser.add_argument(
        "--activation-date",
        dest="activation_date",
        help="Override the activation window date (YY-mm-dd).",
    )
    return parser.parse_args(argv)


def sort_listings(listings: Iterable[Listing]) -> List[Listing]:
    """
    Sort listings consistently by activation date and ticker symbol.
    """
    return sorted(listings, key=lambda item: (item.date, item.ticker))


def build_report(
    csv_url: str,
    activation_date: date,
    next_trading_date: date,
    listings_by_date: Dict[date, Iterable[Listing]],
    primary_only: bool,
) -> Dict[str, Any]:
    """
    Assemble a JSON-serializable report structure from the collected listings.
    """
    sorted_dates = sorted(listings_by_date.keys())
    payloads_by_date: Dict[str, List[Dict[str, Any]]] = {}
    dates_summary: List[Dict[str, Any]] = []
    total_count = 0
    combined_tickers: List[str] = []
    next_activation_with_data: Optional[str] = None

    latest_iso = activation_date.isoformat()

    for current_date in sorted_dates:
        sorted_listings = sort_listings(listings_by_date.get(current_date, []))
        payload = [
            {
                "ticker": listing.ticker,
                "activation_date": listing.date.isoformat(),
                "company": listing.company,
                "exchange": listing.exchange,
                "flag": listing.flag,
            }
            for listing in sorted_listings
        ]
        iso = current_date.isoformat()
        payloads_by_date[iso] = payload

        tickers = [listing.ticker for listing in sorted_listings]
        combined_tickers.extend(tickers)

        total_count += len(payload)
        dates_summary.append(
            {
                "date": iso,
                "weekday": current_date.strftime("%A"),
                "total": len(payload),
                "tickers": tickers,
            }
        )

        if current_date > activation_date and payload and next_activation_with_data is None:
            next_activation_with_data = iso

    metadata: Dict[str, Any] = {
        "generated_at": datetime.now(EST).isoformat(),
        "activation_date": activation_date.isoformat(),
        "activation_window": f"{activation_date.isoformat()} (EST)",
        "primary_exchanges_only": primary_only,
        "source_csv": csv_url,
    }
    metadata["next_trading_date"] = next_trading_date.isoformat()
    if next_activation_with_data:
        metadata["next_listings_activation_date"] = next_activation_with_data

    week_start = activation_date - timedelta(days=activation_date.weekday())
    week_end = week_start + timedelta(days=4)
    metadata["week_start"] = week_start.isoformat()
    metadata["week_end"] = week_end.isoformat()

    latest_total = len(payloads_by_date.get(latest_iso, []))
    next_total = sum(
        len(payload)
        for iso_value, payload in payloads_by_date.items()
        if iso_value > latest_iso
    )

    summary: Dict[str, Any] = {
        "total": total_count,
        "latest_total": latest_total,
        "next_total": next_total,
        "tickers": combined_tickers,
        "latest_activation_date": activation_date.isoformat(),
        "next_activation_date": next_activation_with_data,
        "next_trading_date": next_trading_date.isoformat(),
        "dates": dates_summary,
    }

    return {
        "metadata": metadata,
        "listings": {
            "by_date": payloads_by_date,
        },
        "summary": summary,
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
    listings_section = report.get("listings", {})

    MAX_FIELD_LENGTH = 1024

    def format_ticker_values(tickers: Sequence[str], historical: bool = False) -> str:
        if not tickers:
            return "No new listings added." if historical else "No new listings scheduled."

        result_parts: List[str] = []
        current_length = 0
        total = len(tickers)
        shown = 0

        for ticker in tickers:
            token = (", " if result_parts else "") + ticker
            token_len = len(token)
            if current_length + token_len <= MAX_FIELD_LENGTH:
                result_parts.append(token if result_parts else ticker)
                current_length += token_len
                shown += 1
                continue
            break

        hidden = total - shown
        result = "".join(result_parts)
        if hidden <= 0:
            return result

        suffix = f"... (+{hidden} more tickers)"
        if current_length + 1 + len(suffix) <= MAX_FIELD_LENGTH:
            return f"{result}\n{suffix}"

        available = MAX_FIELD_LENGTH - len(suffix) - 1
        if available <= 0:
            return suffix[:MAX_FIELD_LENGTH]

        truncated = result[:available].rstrip(", ")
        if not truncated:
            return suffix[:MAX_FIELD_LENGTH]
        return f"{truncated}…\n{suffix}"

    def tickers_for_date(iso_date: str) -> List[str]:
        payload = listings_section.get("by_date", {}).get(iso_date, []) or []
        extracted: List[str] = []
        for entry in payload:
            ticker = entry.get("ticker")
            if ticker:
                extracted.append(ticker)
        return extracted

    context = {
        "latest_date": summary.get("latest_activation_date")
        or metadata.get("activation_date")
        or "",
        "next_date": metadata.get("next_trading_date") or "",
        "next_activation_date": summary.get("next_activation_date") or "",
        "latest_total": str(summary.get("latest_total", 0)),
        "next_total": str(summary.get("next_total", 0)),
        "total": str(summary.get("total", 0)),
    }
    week_start_iso = metadata.get("week_start")
    week_end_iso = metadata.get("week_end")

    def format_title_date(value: Optional[str]) -> Optional[str]:
        if not value:
            return None
        try:
            return datetime.fromisoformat(value).strftime("%a %m/%d/%Y")
        except ValueError:
            return None

    week_start_display = format_title_date(week_start_iso)
    week_end_display = format_title_date(week_end_iso)
    activation_display = format_title_date(context["latest_date"])
    if week_start_display and week_end_display:
        context["week_range"] = f"{week_start_display} - {week_end_display}"
    else:
        context["week_range"] = activation_display or context["latest_date"]

    payload = _format_template(template, context)

    week_fields: List[Dict[str, Any]] = []

    def parse_iso_date(value: Optional[str], default: date) -> date:
        if not value:
            return default
        try:
            return datetime.fromisoformat(value).date()
        except ValueError:
            return default

    default_today = datetime.now(EST).date()
    week_start_date = parse_iso_date(week_start_iso, default_today)
    week_end_date = parse_iso_date(week_end_iso, week_start_date + timedelta(days=4))

    current_day = week_start_date
    while current_day <= week_end_date:
        if is_trading_day(current_day):
            iso = current_day.isoformat()
            tickers = tickers_for_date(iso)
            field_value = format_ticker_values(
                tickers,
                historical=current_day < datetime.now(EST).date(),
            )
            display_name = current_day.strftime("%A")
            week_fields.append(
                {
                    "name": display_name,
                    "value": field_value,
                    "inline": True,
                }
            )
        current_day += timedelta(days=1)

    if not week_fields:
        week_fields.append(
            {
                "name": "No Upcoming Trading Days",
                "value": "No qualifying tickers in the current trading week.",
                "inline": False,
            }
        )

    embeds = payload.get("embeds")
    if isinstance(embeds, list) and embeds:
        embeds[0]["fields"] = week_fields
    else:
        payload["embeds"] = [{"fields": week_fields}]

    return payload


def post_to_discord(
    webhook_url: str, payload: Dict[str, Any], thread_id: Optional[str] = None
) -> None:
    """
    Send the prepared payload to the Discord webhook URL.
    """
    try:
        params = {"thread_id": thread_id} if thread_id else None
        response = requests.post(webhook_url, json=payload, params=params, timeout=30)
        response.raise_for_status()
    except requests.RequestException as exc:
        detail = ""
        if hasattr(exc, "response") and exc.response is not None:
            try:
                detail_json = exc.response.json()
                detail = f" Response: {json.dumps(detail_json, indent=2)}"
            except ValueError:
                detail = f" Response: {exc.response.text.strip()}"
        raise RuntimeError(f"Failed to post Discord message: {exc}.{detail}") from exc


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_arguments(argv)
    override_date: Optional[date] = None
    if args.activation_date:
        for pattern in ("%y-%m-%d", "%Y-%m-%d"):
            try:
                override_date = datetime.strptime(args.activation_date, pattern).date()
                break
            except ValueError:
                continue
        if override_date is None:
            raise SystemExit(
                "Invalid --activation-date value. Expected YY-mm-dd (or YYYY-mm-dd)."
            )

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

    target_year = determine_target_year(
        session,
        years_url,
        preferred_year=override_date.year if override_date else None,
    )

    if override_date and target_year != override_date.year:
        print(
            f"Warning: Requested activation year {override_date.year} not available; "
            f"using {target_year} instead.",
            file=sys.stderr,
        )

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

    today_est = override_date or datetime.now(EST).date()
    next_date = next_trading_day(today_est)
    month_slug = today_est.strftime("%B").lower()
    csv_url = fetch_month_link(session, reports_url, query_params, month_slug)

    ts_year = parse_ts_year(csv_url)
    if ts_year and ts_year != target_year:
        print(
            f"Warning: CSV timestamp year ({ts_year}) differs from selected year ({target_year}).",
            file=sys.stderr,
        )

    csv_text = fetch_csv(session, csv_url)
    week_start = today_est - timedelta(days=today_est.weekday())
    listings_map = parse_csv(csv_text, week_start)

    listings_by_date: Dict[date, List[Listing]] = {}
    for activation in sorted(listings_map.keys()):
        day_listings = list(listings_map[activation].values())
        if args.primary_only:
            day_listings = list(filter_primary_exchanges(day_listings))
        listings_by_date[activation] = day_listings

    # Ensure keys for today/next date exist even if no qualifying rows are returned.
    listings_by_date.setdefault(today_est, [])
    listings_by_date.setdefault(next_date, [])

    report = build_report(
        csv_url,
        today_est,
        next_date,
        listings_by_date,
        args.primary_only,
    )
    print(json.dumps(report, indent=2))

    should_post = args.post_to_discord or args.use_test_webhook
    if should_post:
        entries: List[Tuple[str, Optional[str]]] = []
        if args.use_test_webhook:
            if args.discord_webhook:
                entries.extend(_coerce_cli_webhook(args.discord_webhook))
            else:
                entries.extend(resolve_webhook_entries("DISCORD_WEBHOOK_TEST_URL"))
        elif args.discord_webhook:
            entries.extend(_coerce_cli_webhook(args.discord_webhook))
        else:
            entries.extend(resolve_webhook_entries("DISCORD_WEBHOOK_URL"))

        if not entries:
            if args.use_test_webhook:
                raise RuntimeError(
                    "--test requested but no webhook URL provided. "
                    "Pass --discord-webhook, set DISCORD_WEBHOOK_TEST_URL, "
                    "or define it in .env."
                )
            raise RuntimeError(
                "--discord requested but no webhook URL provided. "
                "Pass --discord-webhook, set DISCORD_WEBHOOK_URL, "
                "or define it in .env."
            )

        now_eastern_dt = datetime.now(EST)
        post_log_path = LOG_FILE_PATH
        post_log = load_post_log(post_log_path)
        log_updated = False

        normalized_entries: List[Tuple[str, Optional[str]]] = []
        seen_keys = set()
        for url, thread in entries:
            if not url:
                continue
            key = (url, thread or "")
            if key in seen_keys:
                continue
            seen_keys.add(key)
            normalized_entries.append((url, thread))

        for webhook_url, thread_id in normalized_entries:
            log_key = f"{LOG_APP_ID}|{webhook_url}|{thread_id or ''}"

        for webhook_url, thread_id in entries:
            log_key = f"{LOG_APP_ID}|{webhook_url}|{thread_id or ''}"
            legacy_key = f"{webhook_url}|{thread_id or ''}"

            last_token = post_log.get(log_key)
            legacy_used = False
            if last_token is None and legacy_key in post_log:
                last_token = post_log[legacy_key]
                legacy_used = True

            last_timestamp = parse_log_timestamp(last_token or "")
            skip_post = False
            if not args.force_post and last_timestamp:
                if last_timestamp.tzinfo is None:
                    last_timestamp = last_timestamp.replace(tzinfo=EST)
                delta = now_eastern_dt - last_timestamp
                delta_seconds = abs(delta.total_seconds())
                if delta_seconds < 24 * 3600:
                    if legacy_used:
                        post_log[log_key] = last_token  # migrate legacy key
                        del post_log[legacy_key]
                        log_updated = True
                    skip_post = True

            if skip_post:
                print(
                    f"Skipping Discord webhook {webhook_url} (already posted within the last 24 hours).",
                    file=sys.stderr,
                )
                continue

            if not args.post_to_discord:
                print(
                    "Info: --test implicitly enables Discord posting using the test webhook.",
                    file=sys.stderr,
                )

            payload = prepare_discord_payload(report, args.discord_template)
            post_to_discord(webhook_url, payload, thread_id)
            post_log[log_key] = now_eastern_dt.isoformat()
            if legacy_key in post_log:
                del post_log[legacy_key]
            log_updated = True

        if log_updated:
            save_post_log(post_log_path, post_log)

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except RuntimeError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        raise SystemExit(1)
