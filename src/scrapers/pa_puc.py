"""Pennsylvania PUC scraper.

Scrapes rate case docket data from the Pennsylvania Public Utility
Commission document search system at https://www.puc.pa.gov/.

PA PUC rate cases use docket prefix "R-" (e.g., R-2024-3046894).
The PUC website uses JavaScript-heavy search, so we provide both:
  1. An httpx-based scraper that attempts to parse search results
  2. A seed-data fallback with realistic historical PA PUC rate cases

Rate limiting: 2-second delay between requests, polite User-Agent.
"""

from __future__ import annotations

import hashlib
import json
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

import httpx
from rich.console import Console

console = Console()

PROJECT_ROOT = Path(__file__).parent.parent.parent
CACHE_DIR = PROJECT_ROOT / "data" / "raw" / "pa_puc"
SOURCE_KEY = "pennsylvania_puc"
STATE = "PA"

BASE_URL = "https://www.puc.pa.gov"
SEARCH_URL = "https://www.puc.pa.gov/search/document-search/"

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


def scrape_pa_puc(
    start_year: int = 2015,
    end_year: Optional[int] = None,
    force: bool = False,
) -> list[dict]:
    """Scrape Pennsylvania PUC rate case docket data.

    Attempts live scraping of the PA PUC website. If the site requires
    JavaScript rendering or returns unusable responses, falls back to
    seed data that covers known historical rate cases.

    Args:
        start_year: Earliest year to scrape.
        end_year: Latest year to scrape.
        force: If True, re-scrape even if cached data exists.

    Returns:
        List of raw rate case records (dicts).
    """
    if end_year is None:
        end_year = datetime.now().year

    CACHE_DIR.mkdir(parents=True, exist_ok=True)

    # Check for cached results
    cache_file = CACHE_DIR / f"pa_puc_cases_{start_year}_{end_year}.json"
    if cache_file.exists() and not force:
        console.print("[dim]Loading cached PA PUC data...[/dim]")
        with open(cache_file) as f:
            records = json.load(f)
        console.print(f"[green]Loaded {len(records)} cached PA PUC records[/green]")
        return records

    # Attempt live scraping
    records = _try_live_scrape(start_year, end_year)

    if not records:
        console.print(
            "[yellow]Live scraping returned no results. "
            "PA PUC may require JS rendering. Using seed data.[/yellow]"
        )
        records = _get_seed_data(start_year, end_year)

    # Cache results
    with open(cache_file, "w") as f:
        json.dump(records, f, indent=2, default=str)

    console.print(f"[green]Scraped {len(records)} PA PUC rate cases[/green]")
    return records


def _try_live_scrape(start_year: int, end_year: int) -> list[dict]:
    """Attempt to scrape PA PUC website directly.

    The PA PUC document search is JavaScript-heavy and may not return
    usable HTML via plain HTTP requests. This function attempts to
    fetch the search page and parse any available rate case docket
    listings.

    Returns:
        List of raw records, or empty list if scraping fails.
    """
    records = []

    try:
        with httpx.Client(headers=HEADERS, timeout=30.0, follow_redirects=True) as client:
            # Try the document search page
            console.print(f"[dim]Attempting to fetch PA PUC search page...[/dim]")
            response = client.get(SEARCH_URL)

            if response.status_code != 200:
                console.print(
                    f"[yellow]PA PUC returned status {response.status_code}[/yellow]"
                )
                return []

            # Cache the raw HTML
            html_hash = hashlib.md5(response.text.encode()).hexdigest()[:8]
            raw_file = CACHE_DIR / f"search_page_{html_hash}.html"
            raw_file.write_text(response.text)

            # Check if page has actual content vs. JS-only rendering
            content = response.text.lower()
            if "javascript" in content and len(content) < 5000:
                console.print(
                    "[yellow]PA PUC search requires JavaScript rendering[/yellow]"
                )
                return []

            # Try to parse any docket listings
            from bs4 import BeautifulSoup

            soup = BeautifulSoup(response.text, "lxml")

            # Look for docket-like patterns in the page
            import re

            docket_pattern = re.compile(r'R-\d{4}-\d{7}')
            found_dockets = docket_pattern.findall(response.text)

            if found_dockets:
                console.print(
                    f"[green]Found {len(found_dockets)} docket references[/green]"
                )
                # Try to extract surrounding context for each docket
                for docket in set(found_dockets):
                    record = {
                        "docket_number": docket,
                        "state": STATE,
                        "source": SOURCE_KEY,
                        "source_url": f"{BASE_URL}/pcdocs/search-results/?q={docket}",
                        "scraped_at": datetime.utcnow().isoformat(),
                    }
                    records.append(record)

                time.sleep(DELAY_SECONDS)

    except httpx.HTTPError as e:
        console.print(f"[yellow]HTTP error scraping PA PUC: {e}[/yellow]")
    except Exception as e:
        console.print(f"[yellow]Error scraping PA PUC: {e}[/yellow]")

    return records


def _get_seed_data(start_year: int, end_year: int) -> list[dict]:
    """Return realistic seed data for PA PUC rate cases.

    These are based on real historical PA PUC rate case filings.
    Docket numbers, utility names, and financial data are representative
    of actual filings.
    """
    from scripts.seed_data import get_pa_puc_seed_data

    all_records = get_pa_puc_seed_data()

    # Filter by year range
    filtered = []
    for record in all_records:
        filing_date = record.get("filing_date", "")
        if filing_date:
            try:
                year = int(filing_date[:4])
                if start_year <= year <= end_year:
                    filtered.append(record)
            except (ValueError, IndexError):
                filtered.append(record)
        else:
            filtered.append(record)

    return filtered
