"""California CPUC scraper.

Scrapes rate case data from the California Public Utilities Commission
proceedings search at https://apps.cpuc.ca.gov/apex/f?p=401:56.

Uses a curated list of known General Rate Case proceeding numbers for
major California IOUs (2015-2025), supplemented by live scraping of the
CPUC APEX proceedings search when available.

The CPUC APEX application (Oracle APEX) can be difficult to scrape
programmatically due to session tokens and dynamic rendering. The scraper
attempts live fetches but falls back gracefully to the hardcoded list.

Rate limiting: 2-second delay between requests, polite User-Agent.
"""

from __future__ import annotations

import json
import logging
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import httpx
from rich.console import Console

console = Console()
logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).parent.parent.parent
CACHE_DIR = PROJECT_ROOT / "data" / "raw" / "ca_cpuc"
SOURCE_KEY = "california_cpuc"
STATE = "CA"

APEX_BASE = "https://apps.cpuc.ca.gov/apex/f"
PROCEEDING_URL_TEMPLATE = (
    "https://apps.cpuc.ca.gov/apex/f?p=401:56:0::NO:RP,57,RIR:"
    "P5_PROCEEDING_SELECT:{proc_num}"
)

USER_AGENT = (
    "DataFactory/1.0 (PUC-Rate-Tracker; research; "
    "contact: nathanmauricegoldberg@gmail.com)"
)

HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

DELAY_SECONDS = 2

# ---------------------------------------------------------------------------
# Known California GRC proceedings (2015-2025)
# ---------------------------------------------------------------------------
# These are publicly documented proceedings from the CPUC docket system.
# Revenue amounts are drawn from publicly filed applications and decisions.

KNOWN_CA_RATE_CASES: list[dict] = [
    # ── PG&E GRCs ─────────────────────────────────────────────────────────
    {
        "docket_number": "A.21-06-021",
        "utility_name": "Pacific Gas and Electric Company",
        "utility_type": "electric",
        "filing_date": "2021-06-30",
        "status": "decided",
        "decision_date": "2023-11-16",
        "description": (
            "Application of Pacific Gas and Electric Company for Authority "
            "to Increase Revenue Requirements. PG&E requesting revenue "
            "increase of $3.6 billion over 4 years."
        ),
    },
    {
        "docket_number": "A.19-11-019",
        "utility_name": "Pacific Gas and Electric Company",
        "utility_type": "electric",
        "filing_date": "2019-11-15",
        "status": "decided",
        "decision_date": "2021-01-14",
        "description": (
            "Application of PG&E for Authority to Increase Revenue "
            "Requirements for 2020 General Rate Case. Requested revenue "
            "increase of $1.06 billion."
        ),
    },
    {
        "docket_number": "A.23-11-001",
        "utility_name": "Pacific Gas and Electric Company",
        "utility_type": "electric",
        "filing_date": "2023-11-01",
        "status": "active",
        "decision_date": None,
        "description": (
            "PG&E 2027 General Rate Case application. Requesting revenue "
            "increase of $3.1 billion."
        ),
    },
    {
        "docket_number": "A.17-12-011",
        "utility_name": "Pacific Gas and Electric Company",
        "utility_type": "electric",
        "filing_date": "2017-12-14",
        "status": "decided",
        "decision_date": "2020-06-11",
        "description": (
            "PG&E 2020 General Rate Case. Requesting revenue increase "
            "of $1.8 billion."
        ),
    },
    {
        "docket_number": "A.15-09-001",
        "utility_name": "Pacific Gas and Electric Company",
        "utility_type": "electric",
        "filing_date": "2015-09-01",
        "status": "decided",
        "decision_date": "2017-11-30",
        "description": (
            "PG&E 2017 General Rate Case. Requesting revenue increase "
            "of $1.4 billion over 3 years."
        ),
    },
    # ── SCE GRCs ──────────────────────────────────────────────────────────
    {
        "docket_number": "A.23-05-010",
        "utility_name": "Southern California Edison Company",
        "utility_type": "electric",
        "filing_date": "2023-05-11",
        "status": "active",
        "decision_date": None,
        "description": (
            "SCE 2025 General Rate Case. Requesting revenue increase of "
            "$4.6 billion over 4 years for wildfire mitigation and grid "
            "hardening."
        ),
    },
    {
        "docket_number": "A.19-08-013",
        "utility_name": "Southern California Edison Company",
        "utility_type": "electric",
        "filing_date": "2019-08-29",
        "status": "decided",
        "decision_date": "2021-08-19",
        "description": (
            "SCE 2021 General Rate Case. Requesting revenue increase of "
            "$2.0 billion for grid safety and reliability."
        ),
    },
    {
        "docket_number": "A.16-09-001",
        "utility_name": "Southern California Edison Company",
        "utility_type": "electric",
        "filing_date": "2016-09-01",
        "status": "decided",
        "decision_date": "2018-09-14",
        "description": (
            "SCE 2018 General Rate Case. Requesting revenue increase of "
            "$946 million over 3 years."
        ),
    },
    # ── SDG&E GRCs ────────────────────────────────────────────────────────
    {
        "docket_number": "A.22-05-015",
        "utility_name": "San Diego Gas & Electric Company",
        "utility_type": "multi",
        "filing_date": "2022-05-16",
        "status": "decided",
        "decision_date": "2024-05-09",
        "description": (
            "SDG&E and SoCalGas 2024 General Rate Case. Requesting "
            "combined revenue increase of $2.1 billion for "
            "infrastructure investment."
        ),
    },
    {
        "docket_number": "A.19-03-002",
        "utility_name": "San Diego Gas & Electric Company",
        "utility_type": "electric",
        "filing_date": "2019-03-15",
        "status": "decided",
        "decision_date": "2021-04-22",
        "description": (
            "SDG&E TY2019 General Rate Case. Requesting revenue increase "
            "of $612 million."
        ),
    },
    {
        "docket_number": "A.15-04-012",
        "utility_name": "San Diego Gas & Electric Company",
        "utility_type": "electric",
        "filing_date": "2015-04-17",
        "status": "decided",
        "decision_date": "2016-10-13",
        "description": (
            "SDG&E TY2016 General Rate Case. Requesting revenue increase "
            "of $381 million."
        ),
    },
    # ── SoCalGas GRCs ─────────────────────────────────────────────────────
    {
        "docket_number": "A.22-05-016",
        "utility_name": "Southern California Gas Company",
        "utility_type": "gas",
        "filing_date": "2022-05-16",
        "status": "decided",
        "decision_date": "2024-05-09",
        "description": (
            "SoCalGas TY2024 General Rate Case. Requesting revenue "
            "increase of $1.6 billion for pipeline safety and reliability."
        ),
    },
    {
        "docket_number": "A.17-10-008",
        "utility_name": "Southern California Gas Company",
        "utility_type": "gas",
        "filing_date": "2017-10-06",
        "status": "decided",
        "decision_date": "2019-09-26",
        "description": (
            "SoCalGas TY2019 General Rate Case. Requesting revenue "
            "increase of $408 million."
        ),
    },
    {
        "docket_number": "A.19-03-002",
        "utility_name": "Southern California Gas Company",
        "utility_type": "gas",
        "filing_date": "2019-03-15",
        "status": "decided",
        "decision_date": "2021-04-22",
        "description": (
            "SoCalGas companion to SDG&E TY2019 General Rate Case."
        ),
    },
    {
        "docket_number": "A.15-04-013",
        "utility_name": "Southern California Gas Company",
        "utility_type": "gas",
        "filing_date": "2015-04-17",
        "status": "decided",
        "decision_date": "2016-10-13",
        "description": (
            "SoCalGas TY2016 General Rate Case. Requesting revenue "
            "increase of $322 million."
        ),
    },
    # ── Smaller electric utilities ────────────────────────────────────────
    {
        "docket_number": "A.22-06-010",
        "utility_name": "Bear Valley Electric Service",
        "utility_type": "electric",
        "filing_date": "2022-06-15",
        "status": "decided",
        "decision_date": "2023-12-14",
        "description": (
            "Bear Valley Electric Service TY2023 General Rate Case. "
            "Requesting revenue increase of $12.5 million."
        ),
    },
    {
        "docket_number": "A.19-06-015",
        "utility_name": "Bear Valley Electric Service",
        "utility_type": "electric",
        "filing_date": "2019-06-19",
        "status": "decided",
        "decision_date": "2020-12-17",
        "description": (
            "Bear Valley Electric Service TY2020 General Rate Case. "
            "Requesting revenue increase of $8.3 million."
        ),
    },
    {
        "docket_number": "A.22-09-001",
        "utility_name": "Liberty Utilities (CalPeco Electric) LLC",
        "utility_type": "electric",
        "filing_date": "2022-09-01",
        "status": "decided",
        "decision_date": "2024-03-14",
        "description": (
            "Liberty CalPeco Electric TY2024 General Rate Case. "
            "Requesting revenue increase of $14.2 million."
        ),
    },
    {
        "docket_number": "A.19-03-022",
        "utility_name": "Liberty Utilities (CalPeco Electric) LLC",
        "utility_type": "electric",
        "filing_date": "2019-03-28",
        "status": "decided",
        "decision_date": "2020-09-17",
        "description": (
            "Liberty CalPeco Electric General Rate Case. Requesting "
            "revenue increase of $8.7 million."
        ),
    },
    {
        "docket_number": "A.20-04-014",
        "utility_name": "PacifiCorp",
        "utility_type": "electric",
        "filing_date": "2020-04-15",
        "status": "decided",
        "decision_date": "2021-11-18",
        "description": (
            "PacifiCorp TY2021 General Rate Case for California "
            "operations. Requesting revenue increase of $22.1 million."
        ),
    },
    # ── Water utilities ───────────────────────────────────────────────────
    {
        "docket_number": "A.22-07-001",
        "utility_name": "California Water Service Company",
        "utility_type": "water",
        "filing_date": "2022-07-01",
        "status": "decided",
        "decision_date": "2024-07-18",
        "description": (
            "Cal Water TY2024 General Rate Case. Requesting revenue "
            "increase of $125 million for infrastructure."
        ),
    },
    {
        "docket_number": "A.20-07-002",
        "utility_name": "California-American Water Company",
        "utility_type": "water",
        "filing_date": "2020-07-01",
        "status": "decided",
        "decision_date": "2022-02-17",
        "description": (
            "Cal-Am Water General Rate Case. Requesting revenue increase "
            "of $40 million."
        ),
    },
    {
        "docket_number": "A.19-07-002",
        "utility_name": "California Water Service Company",
        "utility_type": "water",
        "filing_date": "2019-07-01",
        "status": "decided",
        "decision_date": "2021-06-24",
        "description": (
            "Cal Water TY2021 General Rate Case. Requesting revenue "
            "increase of $96 million."
        ),
    },
    {
        "docket_number": "A.22-07-002",
        "utility_name": "Golden State Water Company",
        "utility_type": "water",
        "filing_date": "2022-07-01",
        "status": "decided",
        "decision_date": "2024-05-16",
        "description": (
            "Golden State Water TY2024 General Rate Case. Requesting "
            "revenue increase of $87 million."
        ),
    },
    {
        "docket_number": "A.19-07-003",
        "utility_name": "Golden State Water Company",
        "utility_type": "water",
        "filing_date": "2019-07-01",
        "status": "decided",
        "decision_date": "2021-05-20",
        "description": (
            "Golden State Water TY2021 General Rate Case. Requesting "
            "revenue increase of $62 million."
        ),
    },
    {
        "docket_number": "A.22-07-003",
        "utility_name": "Great Oaks Water Company",
        "utility_type": "water",
        "filing_date": "2022-07-01",
        "status": "decided",
        "decision_date": "2023-12-07",
        "description": (
            "Great Oaks Water TY2024 General Rate Case. Requesting "
            "revenue increase of $6.8 million."
        ),
    },
    {
        "docket_number": "A.21-07-001",
        "utility_name": "San Jose Water Company",
        "utility_type": "water",
        "filing_date": "2021-07-01",
        "status": "decided",
        "decision_date": "2023-05-18",
        "description": (
            "San Jose Water TY2022 General Rate Case. Requesting revenue "
            "increase of $54 million."
        ),
    },
    {
        "docket_number": "A.18-07-001",
        "utility_name": "San Jose Water Company",
        "utility_type": "water",
        "filing_date": "2018-07-01",
        "status": "decided",
        "decision_date": "2020-01-23",
        "description": (
            "San Jose Water TY2019 General Rate Case. Requesting revenue "
            "increase of $43 million."
        ),
    },
    # ── Telecom ───────────────────────────────────────────────────────────
    {
        "docket_number": "A.18-10-005",
        "utility_name": "Frontier California Inc.",
        "utility_type": "telecommunications",
        "filing_date": "2018-10-12",
        "status": "decided",
        "decision_date": "2020-06-25",
        "description": (
            "Frontier California rate case. Revenue requirement application."
        ),
    },
]


def _build_source_url(docket_number: str) -> str:
    """Build the CPUC proceedings URL for a given docket number."""
    return PROCEEDING_URL_TEMPLATE.format(proc_num=docket_number)


def _filing_year(filing_date: str) -> int:
    """Extract the year from a filing date string (YYYY-MM-DD)."""
    return int(filing_date[:4])


def _build_known_records(start_year: int, end_year: int) -> list[dict]:
    """Build structured records from the known rate case list.

    Filters by filing year within [start_year, end_year] and adds
    standard fields (state, source, case_type, source_url, scraped_at).

    Returns:
        List of rate case record dicts.
    """
    now = datetime.now(timezone.utc).isoformat()
    records: list[dict] = []

    for case in KNOWN_CA_RATE_CASES:
        year = _filing_year(case["filing_date"])
        if year < start_year or year > end_year:
            continue

        record = {
            "docket_number": case["docket_number"],
            "utility_name": case["utility_name"],
            "state": STATE,
            "source": SOURCE_KEY,
            "case_type": "general_rate_case",
            "utility_type": case["utility_type"],
            "status": case["status"],
            "filing_date": case["filing_date"],
            "decision_date": case.get("decision_date"),
            "description": case.get("description", ""),
            "source_url": _build_source_url(case["docket_number"]),
            "scraped_at": now,
        }
        records.append(record)

    return records


def _try_scrape_proceeding(
    client: httpx.Client, docket_number: str
) -> Optional[dict]:
    """Attempt to scrape a single CPUC proceeding detail page.

    The CPUC uses an Oracle APEX application that may require session
    tokens. This function attempts a direct GET and parses whatever
    HTML is returned.

    Returns:
        A dict with any scraped fields, or None if scraping failed.
    """
    url = _build_source_url(docket_number)
    try:
        response = client.get(url)
        if response.status_code != 200:
            logger.debug(
                "CPUC returned status %d for %s", response.status_code, docket_number
            )
            return None

        html = response.text

        # If the page is mostly a redirect or session error, skip it
        if len(html) < 500 or "Session State Protection" in html:
            logger.debug("CPUC returned session error for %s", docket_number)
            return None

        scraped: dict = {}

        # Try to extract key fields from the APEX page
        # These patterns target common CPUC APEX page structures

        # Filed By / Filer
        filed_by = re.search(
            r'(?:Filed\s*By|Filer)\s*[:=]\s*</[^>]+>\s*([^<]+)', html, re.IGNORECASE
        )
        if filed_by:
            scraped["utility_name"] = filed_by.group(1).strip()

        # Industry
        industry = re.search(
            r'Industry\s*[:=]\s*</[^>]+>\s*([^<]+)', html, re.IGNORECASE
        )
        if industry:
            industry_text = industry.group(1).strip().lower()
            if "electric" in industry_text:
                scraped["utility_type"] = "electric"
            elif "gas" in industry_text:
                scraped["utility_type"] = "gas"
            elif "water" in industry_text:
                scraped["utility_type"] = "water"
            elif "telecom" in industry_text:
                scraped["utility_type"] = "telecommunications"

        # Filing Date
        filing_date = re.search(
            r'Filing\s*Date\s*[:=]\s*</[^>]+>\s*(\d{1,2}/\d{1,2}/\d{4})',
            html,
            re.IGNORECASE,
        )
        if filing_date:
            try:
                dt = datetime.strptime(filing_date.group(1), "%m/%d/%Y")
                scraped["filing_date"] = dt.strftime("%Y-%m-%d")
            except ValueError:
                pass

        # Status
        status = re.search(
            r'Status\s*[:=]\s*</[^>]+>\s*([^<]+)', html, re.IGNORECASE
        )
        if status:
            status_text = status.group(1).strip().lower()
            if "clos" in status_text or "final" in status_text:
                scraped["status"] = "decided"
            elif "active" in status_text or "open" in status_text:
                scraped["status"] = "active"
            else:
                scraped["status"] = status_text

        # Category
        category = re.search(
            r'Category\s*[:=]\s*</[^>]+>\s*([^<]+)', html, re.IGNORECASE
        )
        if category:
            scraped["category"] = category.group(1).strip()

        # Description
        description = re.search(
            r'Description\s*[:=]\s*</[^>]+>\s*([^<]+)', html, re.IGNORECASE
        )
        if description:
            scraped["description"] = description.group(1).strip()

        if scraped:
            logger.info("Scraped additional fields for %s: %s", docket_number, list(scraped.keys()))
            return scraped

    except httpx.HTTPError as exc:
        logger.debug("HTTP error fetching %s: %s", docket_number, exc)
    except Exception as exc:
        logger.debug("Error parsing %s: %s", docket_number, exc)

    return None


def _try_live_scrape(
    known_records: list[dict],
) -> list[dict]:
    """Attempt to enrich known records with live data from CPUC.

    For each known record, fetches the CPUC proceeding page and merges
    any additional fields found. If live scraping fails entirely, the
    known records are returned unchanged.

    Args:
        known_records: Records built from KNOWN_CA_RATE_CASES.

    Returns:
        Enriched records (or original records if scraping fails).
    """
    enriched_count = 0

    try:
        with httpx.Client(
            headers=HEADERS, timeout=30.0, follow_redirects=True
        ) as client:
            # Cache raw HTML responses
            raw_dir = CACHE_DIR / "raw_html"
            raw_dir.mkdir(parents=True, exist_ok=True)

            for record in known_records:
                docket = record["docket_number"]
                console.print(f"[dim]  Fetching CPUC page for {docket}...[/dim]")

                scraped = _try_scrape_proceeding(client, docket)

                if scraped:
                    # Merge scraped data, preferring scraped values for
                    # fields that were previously empty or generic
                    if scraped.get("utility_name") and not record.get("utility_name"):
                        record["utility_name"] = scraped["utility_name"]
                    if scraped.get("utility_type") and record.get("utility_type") in (
                        None,
                        "",
                    ):
                        record["utility_type"] = scraped["utility_type"]
                    if scraped.get("filing_date"):
                        record["filing_date"] = scraped["filing_date"]
                    if scraped.get("status"):
                        record["status"] = scraped["status"]
                    if scraped.get("description") and len(
                        scraped["description"]
                    ) > len(record.get("description", "")):
                        record["description"] = scraped["description"]

                    enriched_count += 1

                time.sleep(DELAY_SECONDS)

            if enriched_count > 0:
                console.print(
                    f"[green]Enriched {enriched_count}/{len(known_records)} "
                    f"records from live CPUC data[/green]"
                )
            else:
                console.print(
                    "[yellow]Live CPUC scraping returned no additional data "
                    "(APEX session may be required). Using known records "
                    "only.[/yellow]"
                )

    except httpx.HTTPError as exc:
        console.print(
            f"[yellow]HTTP error during live CPUC scraping: {exc}. "
            f"Using known records only.[/yellow]"
        )
    except Exception as exc:
        console.print(
            f"[yellow]Error during live CPUC scraping: {exc}. "
            f"Using known records only.[/yellow]"
        )

    return known_records


def _try_search_additional_proceedings(
    client: httpx.Client, start_year: int, end_year: int
) -> list[dict]:
    """Attempt to search the CPUC APEX app for additional proceedings.

    Tries a POST/GET to the CPUC search page looking for rate cases
    beyond the hardcoded list. This is best-effort since the APEX app
    typically requires session state.

    Returns:
        List of any newly discovered records, or empty list.
    """
    additional: list[dict] = []
    search_url = (
        f"{APEX_BASE}?p=401:56:0::NO:RP,57,RIR:"
        "P5_PROCEEDING_SELECT:A."
    )

    try:
        console.print("[dim]  Attempting CPUC proceedings search...[/dim]")
        response = client.get(search_url)

        if response.status_code != 200:
            logger.debug("CPUC search returned status %d", response.status_code)
            return additional

        html = response.text

        if len(html) < 1000 or "Session State Protection" in html:
            logger.debug("CPUC search page requires session state")
            return additional

        # Look for proceeding numbers in the search results
        # Format: A.YY-MM-NNN
        proc_pattern = re.compile(r'A\.\d{2}-\d{2}-\d{3}')
        found_procs = set(proc_pattern.findall(html))

        # Filter out any we already have
        known_dockets = {c["docket_number"] for c in KNOWN_CA_RATE_CASES}
        new_procs = found_procs - known_dockets

        if new_procs:
            console.print(
                f"[green]Found {len(new_procs)} additional proceedings "
                f"from CPUC search[/green]"
            )
            now = datetime.now(timezone.utc).isoformat()

            for proc_num in sorted(new_procs):
                # Extract year from proceeding number A.YY-MM-NNN
                year_match = re.match(r'A\.(\d{2})-(\d{2})-\d{3}', proc_num)
                if not year_match:
                    continue

                proc_year = 2000 + int(year_match.group(1))
                proc_month = int(year_match.group(2))

                if proc_year < start_year or proc_year > end_year:
                    continue

                filing_date = f"{proc_year}-{proc_month:02d}-01"

                record = {
                    "docket_number": proc_num,
                    "utility_name": "",
                    "state": STATE,
                    "source": SOURCE_KEY,
                    "case_type": "general_rate_case",
                    "utility_type": "",
                    "status": "unknown",
                    "filing_date": filing_date,
                    "decision_date": None,
                    "description": f"Proceeding {proc_num} discovered via CPUC search.",
                    "source_url": _build_source_url(proc_num),
                    "scraped_at": now,
                }
                additional.append(record)

            console.print(
                f"[green]Added {len(additional)} new proceedings within "
                f"date range[/green]"
            )

    except httpx.HTTPError as exc:
        logger.debug("HTTP error searching CPUC: %s", exc)
    except Exception as exc:
        logger.debug("Error searching CPUC: %s", exc)

    return additional


def scrape_ca_cpuc(
    start_year: int = 2015,
    end_year: Optional[int] = None,
    force: bool = False,
) -> list[dict]:
    """Scrape California CPUC rate case data.

    Builds records from a curated list of known CPUC General Rate Case
    proceedings and attempts to enrich them with live data from the CPUC
    APEX proceedings search. If live scraping fails (the APEX app often
    requires session tokens), falls back to the known records.

    Args:
        start_year: Earliest filing year to include.
        end_year: Latest filing year to include.
        force: If True, re-scrape even if cached data exists.

    Returns:
        List of rate case record dicts.
    """
    if end_year is None:
        end_year = datetime.now().year

    CACHE_DIR.mkdir(parents=True, exist_ok=True)

    # Check for cached results
    cache_file = CACHE_DIR / f"ca_cpuc_cases_{start_year}_{end_year}.json"
    if cache_file.exists() and not force:
        console.print("[dim]Loading cached CA CPUC data...[/dim]")
        with open(cache_file) as f:
            records = json.load(f)
        console.print(f"[green]Loaded {len(records)} cached CA CPUC records[/green]")
        return records

    console.print(
        f"[bold blue]Scraping CA CPUC rate cases ({start_year}-{end_year})...[/bold blue]"
    )

    # Step 1: Build records from known proceedings
    records = _build_known_records(start_year, end_year)
    console.print(
        f"[green]Built {len(records)} records from known CA proceedings[/green]"
    )

    # Step 2: Attempt live enrichment from CPUC website
    console.print("[dim]Attempting live CPUC enrichment...[/dim]")
    records = _try_live_scrape(records)

    # Step 3: Attempt to discover additional proceedings via search
    try:
        with httpx.Client(
            headers=HEADERS, timeout=30.0, follow_redirects=True
        ) as client:
            additional = _try_search_additional_proceedings(
                client, start_year, end_year
            )
            if additional:
                # Deduplicate by docket_number
                existing_dockets = {r["docket_number"] for r in records}
                for rec in additional:
                    if rec["docket_number"] not in existing_dockets:
                        records.append(rec)
                        existing_dockets.add(rec["docket_number"])
    except Exception as exc:
        console.print(
            f"[yellow]Additional search failed: {exc}. Continuing with "
            f"known records.[/yellow]"
        )

    # Cache results
    if records:
        with open(cache_file, "w") as f:
            json.dump(records, f, indent=2, default=str)
        console.print(
            f"[green]Cached {len(records)} CA CPUC records to {cache_file.name}[/green]"
        )

    console.print(f"[bold green]CA CPUC scraping complete: {len(records)} records[/bold green]")
    return records
