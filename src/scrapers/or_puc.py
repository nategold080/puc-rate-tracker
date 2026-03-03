"""Oregon PUC scraper — extracts rate case data from EDOCKETS system.

The Oregon PUC EDOCKETS system at apps.puc.state.or.us/edockets/ has an
accessible search form that returns structured table results for rate cases.
Docket types: UE (electric), UG (gas), UM (multi-service), UC (telecom).
"""

from __future__ import annotations

import json
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

import httpx
from rich.console import Console

console = Console()
PROJECT_ROOT = Path(__file__).parent.parent.parent
CACHE_DIR = PROJECT_ROOT / "data" / "raw" / "or_puc"

USER_AGENT = "DataFactory/1.0 (research; contact: nathanmauricegoldberg@gmail.com)"
DELAY_SECONDS = 2

# Map OR PUC docket prefixes to utility types
DOCKET_TYPE_MAP = {
    "UE": "electric",
    "UG": "gas",
    "UM": "multi",
    "UC": "telecom",
    "UW": "water",
}


def _parse_date(date_str: str) -> Optional[str]:
    """Parse OR PUC date formats to ISO format."""
    if not date_str or not date_str.strip():
        return None
    date_str = date_str.strip()
    for fmt in ("%m/%d/%Y", "%m/%d/%y", "%Y-%m-%d"):
        try:
            return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def _classify_utility_type(docket_number: str) -> str:
    """Classify utility type from OR PUC docket prefix."""
    prefix = re.match(r"(U[EGMCW])", docket_number)
    if prefix:
        return DOCKET_TYPE_MAP.get(prefix.group(1), "unknown")
    return "unknown"


def scrape_or_puc(
    start_year: int = 1990,
    end_year: Optional[int] = None,
    force: bool = False,
) -> list[dict]:
    """Scrape Oregon PUC rate case data from EDOCKETS.

    Searches the EDOCKETS system for all rate cases and parses the
    result table into structured records.
    """
    if end_year is None:
        end_year = datetime.now().year

    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache_file = CACHE_DIR / f"or_puc_cases_{start_year}_{end_year}.json"

    if cache_file.exists() and not force:
        console.print("[dim]Loading cached OR PUC data...[/dim]")
        with open(cache_file) as f:
            records = json.load(f)
        console.print(f"[green]Loaded {len(records)} cached OR PUC records[/green]")
        return records

    console.print(f"[cyan]Scraping OR PUC EDOCKETS (rate cases, {start_year}-{end_year})...[/cyan]")

    try:
        records = _fetch_rate_cases()
    except Exception as e:
        console.print(f"[red]OR PUC scrape failed: {e}[/red]")
        console.print("[yellow]Falling back to seed data...[/yellow]")
        from scripts.seed_data import get_or_puc_seed_data
        records = get_or_puc_seed_data()
        with open(cache_file, "w") as f:
            json.dump(records, f, indent=2, default=str)
        return records

    # Filter by year range
    filtered = []
    for r in records:
        fd = r.get("filing_date", "")
        if fd:
            try:
                y = int(fd[:4])
                if start_year <= y <= end_year:
                    filtered.append(r)
            except (ValueError, IndexError):
                filtered.append(r)
        else:
            filtered.append(r)

    # Cache results
    with open(cache_file, "w") as f:
        json.dump(filtered, f, indent=2, default=str)

    console.print(f"[green]Scraped {len(filtered)} OR PUC rate cases[/green]")
    return filtered


def _fetch_rate_cases() -> list[dict]:
    """Fetch rate case data from OR PUC EDOCKETS search."""
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "text/html",
    }

    with httpx.Client(timeout=120, follow_redirects=True, headers=headers) as client:
        url = "https://apps.puc.state.or.us/edockets/srchlist.asp"
        form_data = {
            "dession": "",
            "dession_all": "on",
            "case_type": "rate",
            "party": "",
            "keyword": "",
            "submit": "Search",
        }

        response = client.post(url, data=form_data)
        response.raise_for_status()

        return _parse_search_results(response.text)


def _parse_search_results(html: str) -> list[dict]:
    """Parse the OR PUC EDOCKETS search result HTML table."""
    rows = re.findall(r"<tr[^>]*>(.*?)</tr>", html, re.DOTALL)
    cases = []
    seen_dockets = set()

    for row in rows:
        cells = re.findall(r"<td[^>]*>(.*?)</td>", row, re.DOTALL)
        if len(cells) < 4:
            continue

        # Clean HTML tags from cells
        clean = [
            re.sub(r"<[^>]+>", "", c).strip().replace("&nbsp;", "").strip()
            for c in cells
        ]

        # First cell should be a docket number (UE/UG/UM/UC followed by digits)
        docket = clean[0].strip()
        if not re.match(r"U[EGMCW]\s*\d+", docket):
            continue

        # Normalize docket number (ensure space between prefix and number)
        docket = re.sub(r"(U[EGMCW])\s*(\d+)", r"\1 \2", docket)

        # Skip duplicates
        if docket in seen_dockets:
            continue
        seen_dockets.add(docket)

        utility_name = clean[1].strip() if len(clean) > 1 else ""
        filing_date = _parse_date(clean[2]) if len(clean) > 2 else None
        # clean[3] is typically the order number
        decision_date = _parse_date(clean[4]) if len(clean) > 4 else None

        # Strip " - Met Retention" suffix (docket retention metadata, not part of name)
        utility_name = re.sub(r"\s*-\s*Met Retention\s*$", "", utility_name).strip()

        # Strip "[PDF]" artifacts from docket numbers
        docket = re.sub(r"\[PDF\]", "", docket).strip()

        utility_type = _classify_utility_type(docket)

        record = {
            "docket_number": docket,
            "utility_name": utility_name,
            "state": "OR",
            "source": "or_puc",
            "filing_date": filing_date,
            "decision_date": decision_date,
            "case_type": "general_rate_case",
            "utility_type": utility_type,
            "status": "decided" if decision_date else "active",
        }

        cases.append(record)

    return cases
