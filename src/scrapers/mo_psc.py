"""Missouri PSC (Public Service Commission) scraper.

Scrapes rate case data from the Missouri PSC EFIS (Electronic Filing
Information System) at efis.psc.mo.gov. Case detail pages are
server-rendered HTML accessible via httpx.

Case number prefixes:
  ER- = Electric Rate case
  GR- = Gas Rate case
  WR- = Water Rate case
  SR- = Sewer Rate case

Rate limiting: 2-second delay between requests, polite User-Agent.
"""

from __future__ import annotations

import html as html_mod
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
CACHE_DIR = PROJECT_ROOT / "data" / "raw" / "mo_psc"
SOURCE_KEY = "missouri_psc"
STATE = "MO"

EFIS_BASE = "https://efis.psc.mo.gov"
CASE_URL_TEMPLATE = f"{EFIS_BASE}/Case/Display/{{case_id}}"

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

# Rate case prefixes
RATE_CASE_PREFIXES = ("ER-", "GR-", "WR-", "SR-")

# Case number pattern
CASE_NUM_RE = re.compile(r'([A-Z]{2}-\d{4}-\d{3,5})')

# Known case ID ranges by type (discovered via web search)
# These ranges contain rate cases for each utility type
SCAN_RANGES = [
    # ER (Electric Rate) cases - older system IDs
    (11100, 11450, "ER cases (2006-2023)"),
    # GR (Gas Rate) cases - older system IDs
    (12200, 12420, "GR cases (2004-2023)"),
    # WR (Water Rate) cases - older system IDs
    (17100, 17320, "WR cases (2005-2023)"),
]

# Known case IDs from web search (2024+ era with new ID ranges)
KNOWN_CASE_IDS = [
    # Electric rate cases (2024+)
    83704,   # ER-2024-0189 (Evergy MO West)
    84473,   # ER-2024-0221
    85589,   # ER-2024-0261 (Empire District/Liberty)
    87208,   # ER-2024-0319 (Ameren MO)
    # Water rate cases (2024+)
    82647,   # WR-2024-0104 (Liberty Utilities Water)
    # Gas rate cases (2025+)
    # Will discover via range scanning if needed
]

# Utility type map from case prefix
PREFIX_UTILITY_TYPE = {
    "ER": "electric",
    "GR": "gas",
    "WR": "water",
    "SR": "wastewater",
}


def scrape_mo_psc(
    start_year: int = 2000,
    end_year: Optional[int] = None,
    force: bool = False,
) -> list[dict]:
    """Scrape Missouri PSC rate case data from EFIS.

    Uses a combination of known case IDs and range scanning to discover
    rate cases.

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
    cache_file = CACHE_DIR / f"mo_psc_cases_{start_year}_{end_year}.json"

    if cache_file.exists() and not force:
        console.print("[dim]Loading cached MO PSC data...[/dim]")
        with open(cache_file) as f:
            records = json.load(f)
        console.print(f"[green]Loaded {len(records)} cached MO PSC records[/green]")
        return records

    console.print(f"[cyan]Scraping MO PSC EFIS ({start_year}-{end_year})...[/cyan]")

    try:
        records = _fetch_rate_cases(start_year, end_year)
    except Exception as e:
        console.print(f"[red]MO PSC scrape failed: {e}[/red]")
        records = []

    if not records:
        console.print("[yellow]No records found from MO PSC.[/yellow]")
        return []

    # Cache results
    with open(cache_file, "w") as f:
        json.dump(records, f, indent=2, default=str)

    console.print(f"[green]Scraped {len(records)} MO PSC rate cases[/green]")
    return records


def _fetch_rate_cases(start_year: int, end_year: int) -> list[dict]:
    """Fetch rate case data from MO PSC EFIS via ID enumeration."""
    all_records: dict[str, dict] = {}  # keyed by case_number

    with httpx.Client(timeout=30, follow_redirects=True, headers=HEADERS) as client:
        # Strategy 1: Scan known ID ranges
        for range_start, range_end, label in SCAN_RANGES:
            console.print(f"[dim]  Scanning {label} (IDs {range_start}-{range_end})...[/dim]")
            found = 0
            for case_id in range(range_start, range_end + 1):
                record = _fetch_case_page(client, case_id)
                if record:
                    case_num = record["docket_number"]
                    if case_num not in all_records:
                        all_records[case_num] = record
                        found += 1
                time.sleep(DELAY_SECONDS)
            console.print(f"[dim]    Found {found} rate cases[/dim]")

        # Strategy 2: Fetch known 2024+ case IDs
        console.print(f"[dim]  Fetching {len(KNOWN_CASE_IDS)} known 2024+ case IDs...[/dim]")
        for case_id in KNOWN_CASE_IDS:
            record = _fetch_case_page(client, case_id)
            if record:
                case_num = record["docket_number"]
                if case_num not in all_records:
                    all_records[case_num] = record
            time.sleep(DELAY_SECONDS)

    # Filter by year range
    records = []
    for case_num, record in sorted(all_records.items()):
        year = _case_year(case_num)
        if year and start_year <= year <= end_year:
            records.append(record)

    console.print(f"[dim]  Total: {len(records)} rate cases in {start_year}-{end_year}[/dim]")
    return records


def _fetch_case_page(client: httpx.Client, case_id: int) -> Optional[dict]:
    """Fetch and parse a single MO PSC EFIS case page.

    Returns a record dict if the case is a rate case, None otherwise.
    """
    try:
        url = CASE_URL_TEMPLATE.format(case_id=case_id)
        response = client.get(url)

        if response.status_code != 200:
            return None

        text = response.text

        # Quick check: skip "Submission not found" pages
        if "Submission not found" in text[:1000]:
            return None

        # Extract case number from title
        title_match = re.search(r'Docket Sheet - ([A-Z]{2}-\d{4}-\d{3,5})', text)
        if not title_match:
            return None

        case_number = title_match.group(1)
        prefix = case_number[:2]

        # Only process rate cases
        if not case_number.startswith(RATE_CASE_PREFIXES):
            return None

        # Extract fields from the HTML
        record = _parse_case_html(text, case_number, case_id)
        return record

    except httpx.HTTPError:
        return None
    except Exception:
        return None


def _parse_case_html(html: str, case_number: str, case_id: int) -> dict:
    """Parse structured data from a MO PSC EFIS case page."""

    # Extract status
    status_match = re.search(
        r'Status\s*</div>\s*<div[^>]*>\s*(.*?)\s*</div>',
        html, re.DOTALL
    )
    status_text = ""
    if status_match:
        status_text = re.sub(r'<[^>]+>', '', status_match.group(1)).strip()

    # Parse status
    if "closed" in status_text.lower():
        status = "decided"
    elif "pending" in status_text.lower() or "open" in status_text.lower():
        status = "active"
    else:
        status = "decided" if status_text else "unknown"

    # Extract status date if present (e.g., "Closed (9/9/2025)")
    status_date_match = re.search(r'\((\d{1,2}/\d{1,2}/\d{4})\)', status_text)
    decision_date = None
    if status_date_match:
        decision_date = _parse_mo_date(status_date_match.group(1))

    # Extract utility type from page
    utype_match = re.search(
        r'Utility Type\s*</div>\s*<div[^>]*>\s*(.*?)\s*</div>',
        html, re.DOTALL
    )
    utility_type_text = ""
    if utype_match:
        utility_type_text = re.sub(r'<[^>]+>', '', utype_match.group(1)).strip().lower()

    # Map to standard utility type
    prefix = case_number[:2]
    utility_type = PREFIX_UTILITY_TYPE.get(prefix, "unknown")
    if utility_type == "unknown" and utility_type_text:
        if "electric" in utility_type_text:
            utility_type = "electric"
        elif "gas" in utility_type_text:
            utility_type = "gas"
        elif "water" in utility_type_text:
            utility_type = "water"
        elif "sewer" in utility_type_text:
            utility_type = "wastewater"

    # Extract case type
    case_type_match = re.search(
        r'Type of Case\s*</div>\s*<div[^>]*>\s*(.*?)\s*</div>',
        html, re.DOTALL
    )
    case_type_text = ""
    if case_type_match:
        case_type_text = re.sub(r'<[^>]+>', '', case_type_match.group(1)).strip()

    # Classify case type
    case_type = _classify_mo_case_type(case_type_text, case_number)

    # Extract company names from Subject Companies section
    company_names = _extract_companies(html)
    utility_name = company_names[0] if company_names else ""

    # Extract Style of Case (full title)
    style_match = re.search(
        r'Style of Case\s*</div>\s*<div[^>]*>(.*?)</div>',
        html, re.DOTALL
    )
    description = ""
    if style_match:
        description = html_mod.unescape(re.sub(r'<[^>]+>', '', style_match.group(1)).strip())

    # Enrich description with financial data from filing titles
    if description and "$" not in description:
        filing_titles = _extract_filing_titles(html)
        financial_title = _best_financial_title(filing_titles)
        if financial_title:
            description = f"{description} | {financial_title}"

    # Try to extract filing date from first filing in the docket
    filing_date = _extract_filing_date(html)

    return {
        "docket_number": case_number,
        "utility_name": utility_name,
        "state": STATE,
        "source": SOURCE_KEY,
        "case_type": case_type,
        "utility_type": utility_type,
        "status": status,
        "filing_date": filing_date,
        "decision_date": decision_date,
        "description": description[:500] if description else None,
        "source_url": CASE_URL_TEMPLATE.format(case_id=case_id),
        "scraped_at": datetime.now(timezone.utc).isoformat(),
    }


def _extract_filing_titles(html: str) -> list[str]:
    """Extract all filing/document titles from the case page.

    Filing titles appear as link text in anchors pointing to
    /Case/FilingDisplay/{filing_id}.
    """
    pattern = re.compile(r'<a[^>]*href="/Case/FilingDisplay/\d+"[^>]*>([^<]+)</a>')
    titles = []
    for match in pattern.finditer(html):
        title = html_mod.unescape(match.group(1).strip())
        if title and len(title) > 5:
            titles.append(title)
    return titles


# Keywords that signal a filing title contains financial / revenue data
_FINANCIAL_KEYWORDS = re.compile(
    r'(?:revenue|rate\s+increase|rate\s+decrease|increase|decrease|'
    r'million|billion|\$)',
    re.IGNORECASE,
)


def _best_financial_title(titles: list[str]) -> Optional[str]:
    """Return the most relevant financial filing title, or None.

    Prefers titles that contain an explicit dollar amount ('$'), then
    falls back to titles mentioning revenue/increase/decrease language.
    """
    with_dollar: list[str] = []
    with_keyword: list[str] = []

    for title in titles:
        if "$" in title:
            with_dollar.append(title)
        elif _FINANCIAL_KEYWORDS.search(title):
            with_keyword.append(title)

    # Prefer the first title with a dollar sign; fall back to keyword match
    if with_dollar:
        return with_dollar[0]
    if with_keyword:
        return with_keyword[0]
    return None


def _extract_companies(html: str) -> list[str]:
    """Extract company names from the Subject Companies section."""
    companies = []

    # Look for company links in the Subject Companies section
    section_match = re.search(
        r'Subject Companies(.*?)(?:Style of Case|Assigned Judge)',
        html, re.DOTALL
    )
    if section_match:
        section = section_match.group(1)
        # Company names appear in links or as plain text with (Type) (Ownership)
        company_pattern = re.compile(
            r'(?:title="View"[^>]*>|>)\s*([A-Z][^<(]{3,60}?)\s*(?:<|\()',
        )
        for match in company_pattern.finditer(section):
            name = html_mod.unescape(match.group(1).strip())
            if name and len(name) > 3:
                # Clean up "d/b/a" format
                dba_match = re.search(r'd/b/a\s+(.+)', name)
                if dba_match:
                    companies.append(dba_match.group(1).strip())
                else:
                    companies.append(name)

    # Fallback: extract from Style of Case
    if not companies:
        style_match = re.search(
            r'(?:Application|Request|Tariff[s]?)[^<]*?of\s+([A-Z][^<,]{5,60}?)(?:,|\s+for|\s+d/b/a)',
            html
        )
        if style_match:
            companies.append(style_match.group(1).strip())

    # Also check aria-labels for company names
    aria_matches = re.findall(
        r'aria-label="[^"]*relationships for ([^"]+)"',
        html
    )
    for name in aria_matches:
        name = html_mod.unescape(name)
        if name not in companies:
            companies.append(name)

    return companies


def _extract_filing_date(html: str) -> Optional[str]:
    """Extract the earliest filing date from the docket filing list."""
    # Look for dates in the filing table
    # The table has rows with dates in MM/DD/YYYY format
    dates = re.findall(r'(\d{1,2}/\d{1,2}/\d{4})', html)

    if not dates:
        return None

    # Parse all dates and find the earliest
    parsed = []
    for d in dates:
        parsed_date = _parse_mo_date(d)
        if parsed_date:
            parsed.append(parsed_date)

    if parsed:
        # Return the earliest date (likely the initial filing)
        return min(parsed)

    return None


def _parse_mo_date(date_str: str) -> Optional[str]:
    """Parse MO PSC date to ISO format."""
    if not date_str:
        return None
    for fmt in ("%m/%d/%Y", "%m/%d/%y", "%Y-%m-%d"):
        try:
            return datetime.strptime(date_str.strip(), fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def _classify_mo_case_type(case_type_text: str, case_number: str) -> str:
    """Classify MO PSC case type into standard categories."""
    text_lower = (case_type_text or "").lower()

    if "general rate" in text_lower or "rate increase" in text_lower:
        return "general_rate_case"
    if "fuel" in text_lower or "fac" in text_lower:
        return "fuel_cost_adjustment"
    if "infrastructure" in text_lower or "isrs" in text_lower:
        return "infrastructure_rider"
    if "rate design" in text_lower:
        return "rate_design"
    if "tariff" in text_lower:
        return "general_rate_case"
    if "application" in text_lower:
        return "general_rate_case"

    # Default based on case prefix
    prefix = case_number[:2] if case_number else ""
    if prefix in ("ER", "GR", "WR", "SR"):
        return "general_rate_case"

    return "unknown"


def _case_year(case_number: str) -> Optional[int]:
    """Extract year from MO PSC case number (XX-YYYY-NNNN)."""
    match = re.search(r'-(\d{4})-', case_number)
    if match:
        return int(match.group(1))
    return None
