"""Georgia PSC (Public Service Commission) scraper.

Scrapes rate case data from the Georgia PSC website at psc.ga.gov.
Uses the major cases listing page to discover rate case docket IDs,
then fetches individual docket pages for titles and metadata.

The document filing table requires JavaScript, so only the docket title
and year are extracted from the server-rendered HTML.

Rate limiting: 2-second delay between requests, polite User-Agent.
"""

from __future__ import annotations

import json
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import httpx
from rich.console import Console

console = Console()

PROJECT_ROOT = Path(__file__).parent.parent.parent
CACHE_DIR = PROJECT_ROOT / "data" / "raw" / "ga_psc"
SOURCE_KEY = "georgia_psc"
STATE = "GA"

BASE_URL = "https://psc.ga.gov"

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

# Known GA utilities for entity extraction
GA_UTILITIES = {
    "georgia power": "Georgia Power Company",
    "gpc ": "Georgia Power Company",
    "gpc'": "Georgia Power Company",
    "atlanta gas light": "Atlanta Gas Light Company",
    "southern company gas": "Atlanta Gas Light Company",
    "liberty": "Liberty Utilities",
    "citizens telephone": "Citizens Telephone Company",
    "city of dalton": "City of Dalton Utilities",
    "marietta board": "Marietta Board of Lights and Water",
    "axpo": "Axpo U.S. LLC",
}


def _extract_utility_name(text: str) -> str:
    """Extract utility name from title text."""
    t = text.lower()
    for pattern, canonical in GA_UTILITIES.items():
        if pattern in t:
            return canonical
    return ""


def _classify_utility_type(text: str) -> str:
    """Classify utility type from title text."""
    t = text.lower()
    if any(kw in t for kw in ["gas light", "gas company", "natural gas", " gas "]):
        return "gas"
    if any(kw in t for kw in ["electric", "power"]):
        return "electric"
    if any(kw in t for kw in ["water"]):
        return "water"
    if any(kw in t for kw in ["telephone", "telecom"]):
        return "telecommunications"
    return "unknown"


def _extract_year_from_title(title: str) -> Optional[int]:
    """Extract year from a docket title like 'Georgia Power 2022 Rate Case'."""
    match = re.search(r'\b(19\d{2}|20\d{2})\b', title)
    if match:
        return int(match.group(1))
    return None


def scrape_ga_psc(
    start_year: int = 2000,
    end_year: Optional[int] = None,
    force: bool = False,
) -> list[dict]:
    """Scrape Georgia PSC rate case data.

    Discovers rate case docket IDs from the major cases listing page,
    then fetches individual docket pages for titles and metadata.

    Args:
        start_year: Earliest year to include.
        end_year: Latest year to include.
        force: If True, re-scrape even if cached data exists.

    Returns:
        List of raw rate case records (dicts).
    """
    if end_year is None:
        end_year = datetime.now().year

    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache_file = CACHE_DIR / f"ga_psc_cases_{start_year}_{end_year}.json"

    if cache_file.exists() and not force:
        console.print("[dim]Loading cached GA PSC data...[/dim]")
        with open(cache_file) as f:
            records = json.load(f)
        console.print(f"[green]Loaded {len(records)} cached GA PSC records[/green]")
        return records

    console.print(f"[cyan]Scraping GA PSC dockets ({start_year}-{end_year})...[/cyan]")

    try:
        records = _fetch_rate_cases(start_year, end_year)
    except Exception as e:
        console.print(f"[red]GA PSC scrape failed: {e}[/red]")
        records = []

    if not records:
        console.print("[yellow]No records found from GA PSC.[/yellow]")
        return []

    with open(cache_file, "w") as f:
        json.dump(records, f, indent=2, default=str)

    console.print(f"[green]Scraped {len(records)} GA PSC rate cases[/green]")
    return records


def _fetch_rate_cases(start_year: int, end_year: int) -> list[dict]:
    """Fetch rate case data from GA PSC."""
    records = []
    seen_dockets = set()

    with httpx.Client(timeout=30, follow_redirects=True, headers=HEADERS) as client:
        # Step 1: Discover docket IDs from major cases page
        console.print("[dim]  Fetching major cases listing...[/dim]")
        docket_ids = _get_major_case_docket_ids(client)
        console.print(f"[dim]  Found {len(docket_ids)} docket IDs[/dim]")

        # Step 2: Also scan nearby IDs around known dockets to find more
        expanded_ids = set(docket_ids)
        for did in docket_ids:
            # Check +/- 5 IDs around each known docket
            for offset in range(-5, 6):
                expanded_ids.add(did + offset)
        expanded_ids = sorted(expanded_ids)
        console.print(f"[dim]  Expanded to {len(expanded_ids)} candidate IDs[/dim]")

        # Step 3: Fetch each docket page
        rate_case_count = 0
        for did in expanded_ids:
            record = _fetch_docket_page(client, did)
            if record:
                docket_key = str(did)
                if docket_key not in seen_dockets:
                    year = _extract_year_from_title(record.get("description", ""))
                    if year and start_year <= year <= end_year:
                        records.append(record)
                        seen_dockets.add(docket_key)
                        rate_case_count += 1
            time.sleep(DELAY_SECONDS)

        console.print(f"[dim]  Found {rate_case_count} rate cases in range[/dim]")

    return records


def _get_major_case_docket_ids(client: httpx.Client) -> list[int]:
    """Get docket IDs from the GA PSC major cases page."""
    try:
        response = client.get(f"{BASE_URL}/major-cases-heard-by-the-commission/")
        if response.status_code != 200:
            return []

        # Extract docket IDs from links
        docket_ids = re.findall(
            r'/search/facts-docket/\?docketId=(\d+)',
            response.text
        )
        # Deduplicate and convert to int
        return sorted(set(int(d) for d in docket_ids))
    except Exception as e:
        console.print(f"[yellow]  Error fetching major cases: {e}[/yellow]")
        return []


def _fetch_docket_page(client: httpx.Client, docket_id: int) -> Optional[dict]:
    """Fetch and parse a GA PSC docket page.

    Returns a record dict if the page contains a rate case, None otherwise.
    """
    try:
        url = f"{BASE_URL}/search/facts-docket/?docketId={docket_id}"
        response = client.get(url)

        if response.status_code != 200:
            return None

        text = response.text

        # Extract title
        title_match = re.search(
            r'<div\s+id="dockTitle">\s*(.*?)\s*</div>',
            text, re.DOTALL
        )
        if not title_match:
            return None

        title = title_match.group(1).strip()
        title = re.sub(r'<[^>]+>', '', title).strip()

        if not title:
            return None

        # Only keep rate cases
        title_lower = title.lower()
        rate_keywords = [
            "rate case", "rate increase", "base rate", "rate adjustment",
            "general rate", "rate filing", "rate proceeding",
            "revenue requirement", "rate plan",
        ]
        if not any(kw in title_lower for kw in rate_keywords):
            return None

        # Extract utility name
        utility_name = _extract_utility_name(title)
        utility_type = _classify_utility_type(title)

        # Extract year from title for filing date estimate
        year = _extract_year_from_title(title)
        filing_date = f"{year}-01-01" if year else None

        return {
            "docket_number": str(docket_id),
            "utility_name": utility_name,
            "state": STATE,
            "source": SOURCE_KEY,
            "case_type": "general_rate_case",
            "utility_type": utility_type,
            "status": "decided",
            "filing_date": filing_date,
            "description": title[:500],
            "source_url": f"{BASE_URL}/search/facts-docket/?docketId={docket_id}",
            "scraped_at": datetime.now(timezone.utc).isoformat(),
        }

    except httpx.HTTPError:
        return None
    except Exception:
        return None
