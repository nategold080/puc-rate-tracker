"""Connecticut PURA (Public Utilities Regulatory Authority) scraper.

Scrapes rate case docket data from the CT PURA Lotus Notes/Domino system.
The PURA website uses server-rendered HTML accessible via httpx.

Strategy:
  1. Browse the docket view to collect all docket numbers
  2. Search for rate-related keywords to identify rate case dockets
  3. For each rate case, extract title/utility from expanded view or search context

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
CACHE_DIR = PROJECT_ROOT / "data" / "raw" / "ct_pura"
SOURCE_KEY = "connecticut_pura"
STATE = "CT"

BASE_URL = "https://www.dpuc.state.ct.us"
DOCKET_DB = f"{BASE_URL}/dockcurr.nsf"
VIEW_ID = "8e6fc37a54110e3e852576190052b64d"
SEARCH_URL = f"{DOCKET_DB}/{VIEW_ID}"

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

# CT utility name patterns for entity extraction
CT_UTILITIES = {
    "eversource": "Eversource Energy",
    "cl&p": "Connecticut Light and Power Company",
    "connecticut light": "Connecticut Light and Power Company",
    "united illuminating": "United Illuminating Company",
    "avangrid": "United Illuminating Company",
    "connecticut natural gas": "Connecticut Natural Gas Corporation",
    " cng ": "Connecticut Natural Gas Corporation",
    "(cng)": "Connecticut Natural Gas Corporation",
    "southern connecticut gas": "Southern Connecticut Gas Company",
    " scg ": "Southern Connecticut Gas Company",
    "(scg)": "Southern Connecticut Gas Company",
    "yankee gas": "Yankee Gas Services Company",
    " ygs ": "Yankee Gas Services Company",
    "(ygs)": "Yankee Gas Services Company",
    "aquarion": "Aquarion Water Company",
    "connecticut water": "Connecticut Water Company",
    "pseg": "PSEG New Haven LLC",
    "genconn": "GenConn Energy LLC",
    "hazardville": "Hazardville Water Company",
    "jewett city": "Jewett City Water Company",
    "valley water": "Valley Water Systems Inc.",
    "gb ii": "GB II New Haven LLC",
    "gbii": "GB II New Haven LLC",
}


def _classify_utility_type(text: str) -> str:
    """Classify utility type from docket title text."""
    t = text.lower()
    if any(kw in t for kw in ["electric", "light and power", "illuminat", "cl&p", "genconn", "gb ii", "gbii"]):
        return "electric"
    if any(kw in t for kw in ["gas", "cng", "scg", "yankee", "ygs"]):
        return "gas"
    if any(kw in t for kw in ["water", "aquarion"]):
        return "water"
    if any(kw in t for kw in ["sewer", "wastewater"]):
        return "wastewater"
    if any(kw in t for kw in ["telecom", "at&t", "frontier"]):
        return "telecommunications"
    return "unknown"


def _extract_utility_name(text: str) -> str:
    """Extract and normalize utility name from docket title."""
    t = text.lower()
    for pattern, canonical in CT_UTILITIES.items():
        if pattern in t:
            return canonical
    return ""


def _docket_to_year(docket_number: str) -> Optional[int]:
    """Extract filing year from CT docket number (YY-MM-NN format)."""
    match = re.match(r'(\d{2})-', docket_number)
    if match:
        yy = int(match.group(1))
        return 2000 + yy if yy < 50 else 1900 + yy
    return None


def _docket_to_filing_date(docket_number: str) -> Optional[str]:
    """Estimate filing date from CT docket number (YY-MM-NN)."""
    match = re.match(r'(\d{2})-(\d{2})-(\d{2})', docket_number)
    if match:
        yy = int(match.group(1))
        mm = int(match.group(2))
        year = 2000 + yy if yy < 50 else 1900 + yy
        if 1 <= mm <= 12:
            return f"{year}-{mm:02d}-01"
    return None


def scrape_ct_pura(
    start_year: int = 2000,
    end_year: Optional[int] = None,
    force: bool = False,
) -> list[dict]:
    """Scrape Connecticut PURA rate case docket data.

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
    cache_file = CACHE_DIR / f"ct_pura_cases_{start_year}_{end_year}.json"

    if cache_file.exists() and not force:
        console.print("[dim]Loading cached CT PURA data...[/dim]")
        with open(cache_file) as f:
            records = json.load(f)
        console.print(f"[green]Loaded {len(records)} cached CT PURA records[/green]")
        return records

    console.print(f"[cyan]Scraping CT PURA dockets ({start_year}-{end_year})...[/cyan]")

    try:
        records = _fetch_rate_cases(start_year, end_year)
    except Exception as e:
        console.print(f"[red]CT PURA scrape failed: {e}[/red]")
        records = []

    if not records:
        console.print("[yellow]No records found from CT PURA.[/yellow]")
        return []

    with open(cache_file, "w") as f:
        json.dump(records, f, indent=2, default=str)

    console.print(f"[green]Scraped {len(records)} CT PURA rate cases[/green]")
    return records


def _fetch_rate_cases(start_year: int, end_year: int) -> list[dict]:
    """Fetch rate case data from CT PURA."""
    # Collect docket numbers and context from search results
    docket_context: dict[str, str] = {}  # docket_num -> best context text

    with httpx.Client(timeout=60, follow_redirects=True, headers=HEADERS) as client:
        # Step 1: Search for rate-related keywords
        search_queries = [
            "rate+case", "general+rate+case", "revenue+requirement",
            "revenue+requirements", "rate+increase", "base+rate",
            "rate+application", "rate+adjustment", "purchased+gas+adjustment",
            "rate+schedule+amendment",
        ]

        for query in search_queries:
            console.print(f"[dim]  Searching: {query}...[/dim]")
            try:
                url = f"{SEARCH_URL}?SearchView&Query={query}&SearchMax=500"
                response = client.get(url)
                if response.status_code == 200:
                    _extract_docket_refs(response.text, docket_context)
                time.sleep(DELAY_SECONDS)
            except Exception as e:
                console.print(f"[yellow]  Search error: {e}[/yellow]")

        console.print(f"[dim]  Found {len(docket_context)} unique docket refs from search[/dim]")

        # Step 2: Browse docket listing to get titles for known dockets
        console.print("[dim]  Browsing docket listing for titles...[/dim]")
        docket_titles = _browse_docket_titles(client)
        console.print(f"[dim]  Got titles for {len(docket_titles)} dockets[/dim]")

    # Step 3: Build records for actual rate case dockets (filter aggressively)
    records = []
    seen_base = set()

    # Rate-related keywords to validate a docket is a rate case
    rate_keywords = [
        "rate case", "rate increase", "rate decrease", "rate application",
        "revenue requirement", "base rate", "general rate", "rate design",
        "rate adjustment", "purchased gas adjustment", "rate of return",
        "cost of service", "rate schedule", "revenue decoupling",
        "annual review of rate", "application to amend rate",
        "application to establish", "request for rate",
    ]

    for docket_num, context in sorted(docket_context.items()):
        # Get base docket (strip RE/WI suffixes)
        base = re.sub(r'[A-Z]{2}\d{2}$', '', docket_num).strip()

        year = _docket_to_year(base)
        if year is None or year < start_year or year > end_year:
            continue

        if base in seen_base:
            continue

        # Get the best available title/context
        title = docket_titles.get(base, "") or docket_titles.get(docket_num, "")
        combined = f"{title} {context}".lower()

        # Filter: only keep dockets that are actual rate cases
        # Must have utility name AND rate-related context OR be in browse titles
        utility_name = _extract_utility_name(combined)
        is_rate_related = any(kw in combined for kw in rate_keywords)

        # Skip if no utility name and not clearly rate-related
        if not utility_name and not is_rate_related:
            continue

        # Skip correspondence and compliance-only references
        if not is_rate_related and not title:
            # Only have search context — check if context is just correspondence
            context_lower = context.lower()
            if any(skip in context_lower for skip in [
                "corres.", "correspondence", "motion to", "entry of appearance",
                "protective order", "scheduling conference",
            ]):
                continue

        seen_base.add(base)
        utility_type = _classify_utility_type(combined)

        records.append({
            "docket_number": base,
            "utility_name": utility_name,
            "state": STATE,
            "source": SOURCE_KEY,
            "case_type": "general_rate_case",
            "utility_type": utility_type,
            "status": "decided",
            "filing_date": _docket_to_filing_date(base),
            "description": (title or context)[:500] if (title or context) else None,
            "source_url": f"{BASE_URL}/dockcurr.nsf/{VIEW_ID}?SearchView&Query=%5B{base}%5D",
            "scraped_at": datetime.now(timezone.utc).isoformat(),
        })

    return records


def _extract_docket_refs(html: str, docket_context: dict[str, str]) -> None:
    """Extract docket number references from CT PURA search results.

    Search results reference dockets in bracket format [YY-MM-NN].
    Also extracts context text around each reference.
    """
    # Find bracket-format docket references with context
    bracket_re = re.compile(r'\[(\d{2}-\d{2}-\d{2}(?:[A-Z]{2}\d{2})?)\]')

    for match in bracket_re.finditer(html):
        docket_num = match.group(1)
        # Get surrounding context (200 chars each side)
        start = max(0, match.start() - 200)
        end = min(len(html), match.end() + 200)
        context = re.sub(r'<[^>]+>', ' ', html[start:end]).strip()
        context = re.sub(r'\s+', ' ', context)

        # Keep the longer context for each docket
        if docket_num not in docket_context or len(context) > len(docket_context[docket_num]):
            docket_context[docket_num] = context

    # Also look for docket numbers in link text
    link_pattern = re.compile(
        r'<a[^>]*>[^<]*?(\d{2}-\d{2}-\d{2}(?:[A-Z]{2}\d{2})?)[^<]*?</a>',
        re.DOTALL,
    )
    for match in link_pattern.finditer(html):
        docket_num = match.group(1)
        # Validate: month should be 01-12, YY should be reasonable
        parts = docket_num.split('-')
        if len(parts) >= 3:
            mm = int(parts[1])
            if mm < 1 or mm > 12:
                continue

        if docket_num not in docket_context:
            start = max(0, match.start() - 200)
            end = min(len(html), match.end() + 200)
            context = re.sub(r'<[^>]+>', ' ', html[start:end]).strip()
            docket_context[docket_num] = context


def _browse_docket_titles(client: httpx.Client) -> dict[str, str]:
    """Browse CT PURA docket listing and get titles via expanded views.

    Returns dict mapping docket_number -> title text.
    """
    titles: dict[str, str] = {}
    start: str | int = 1
    max_pages = 50

    for page in range(max_pages):
        try:
            # Use ExpandView to get docket entries with their first document title
            url = f"{SEARCH_URL}?OpenView&Count=300&Start={start}&ExpandView"
            response = client.get(url)
            if response.status_code != 200:
                break

            html = response.text

            # Extract docket numbers from green font tags
            docket_re = re.compile(
                r'<font[^>]*color="#008000"[^>]*>(\d{2}-\d{2}-\d{2}(?:[A-Z]{2}\d{2})?)</font>'
            )
            # Get titles from expanded entries (links after the docket number)
            # Pattern: docket number row, then title in next row's link
            all_dockets = docket_re.findall(html)

            if not all_dockets:
                break

            # For each docket, find the first linked title text after it
            for docket_num in all_dockets:
                idx = html.find(docket_num)
                if idx < 0:
                    continue
                # Look for the first document link after this docket number
                after = html[idx:idx + 1000]
                title_match = re.search(
                    r'<a[^>]*href="[^"]*\?OpenDocument[^"]*"[^>]*>([^<]+)</a>',
                    after,
                )
                if title_match:
                    title = title_match.group(1).strip()
                    if title and len(title) > 5:
                        titles[docket_num] = title

            console.print(f"[dim]    Page {page+1}: {len(all_dockets)} dockets[/dim]")

            # Find next page start
            next_starts = re.findall(r'Start=([\d.]+)', html)
            # Get the largest Start value (next page)
            current = str(start)
            next_start = None
            for s in set(next_starts):
                if s != current and s != '1':
                    try:
                        # Compare as tuples of floats
                        if next_start is None:
                            next_start = s
                        elif float(s.split('.')[0]) > float(next_start.split('.')[0]):
                            next_start = s
                    except (ValueError, IndexError):
                        continue

            if next_start and next_start != str(start):
                start = next_start
            else:
                break

            time.sleep(DELAY_SECONDS)
        except Exception as e:
            console.print(f"[yellow]    Browse page {page+1} error: {e}[/yellow]")
            break

    return titles
