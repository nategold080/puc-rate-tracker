"""Indiana IURC (Indiana Utility Regulatory Commission) scraper.

Scrapes rate case data from the Indiana IURC portal via its REST API.
The portal at iurc.portal.in.gov uses an Azure companion app backend
that returns structured JSON data without authentication.

API base: https://zus1iurcprodd365companionappmaster-appservice.azurewebsites.net
Search endpoint: POST /api/search/advanced
Detail endpoints: POST /api/list/parties, /api/document/orders, etc.

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
CACHE_DIR = PROJECT_ROOT / "data" / "raw" / "in_iurc"
SOURCE_KEY = "indiana_iurc"
STATE = "IN"

API_BASE = "https://zus1iurcprodd365companionappmaster-appservice.azurewebsites.net"
SEARCH_URL = f"{API_BASE}/api/search/advanced"
PARTIES_URL = f"{API_BASE}/api/list/parties"
ORDERS_URL = f"{API_BASE}/api/document/orders"
CASE_DETAIL_URL = "https://iurc.portal.in.gov/docketed-case-details/?id={case_id}"

USER_AGENT = (
    "DataFactory/1.0 (PUC-Rate-Tracker; research; "
    "contact: nathanmauricegoldberg@gmail.com)"
)

HEADERS = {
    "User-Agent": USER_AGENT,
    "Content-Type": "application/json",
    "Accept": "application/json",
    "Origin": "https://iurc.portal.in.gov",
}

DELAY_SECONDS = 2

# Petition type GUIDs for rate-related cases
PETITION_TYPES = {
    "Rates": "bfc8e1c3-d881-e611-8107-1458d04eabe0",
    "Rates & Financing": "cbc8e1c3-d881-e611-8107-1458d04eabe0",
}

# Industry type mapping
INDUSTRY_MAP = {
    "electric": "electric",
    "gas": "gas",
    "water": "water",
    "sewer": "wastewater",
    "water-sewer": "water",
    "telecommunications": "telecommunications",
    "electric-gas": "multi_service",
    "electric-gas-water-sewer": "multi_service",
    "gas-water-sewer": "multi_service",
    "pipeline safety": "gas",
    "video": "telecommunications",
}


def scrape_in_iurc(
    start_year: int = 2000,
    end_year: Optional[int] = None,
    force: bool = False,
) -> list[dict]:
    """Scrape Indiana IURC rate case data via REST API.

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
    cache_file = CACHE_DIR / f"in_iurc_cases_{start_year}_{end_year}.json"

    if cache_file.exists() and not force:
        console.print("[dim]Loading cached IN IURC data...[/dim]")
        with open(cache_file) as f:
            records = json.load(f)
        console.print(f"[green]Loaded {len(records)} cached IN IURC records[/green]")
        return records

    console.print(f"[cyan]Scraping IN IURC rate cases ({start_year}-{end_year})...[/cyan]")

    try:
        records = _fetch_rate_cases(start_year, end_year)
    except Exception as e:
        console.print(f"[red]IN IURC scrape failed: {e}[/red]")
        records = []

    if not records:
        console.print("[yellow]No records found from IN IURC.[/yellow]")
        return []

    # Cache results
    with open(cache_file, "w") as f:
        json.dump(records, f, indent=2, default=str)

    console.print(f"[green]Scraped {len(records)} IN IURC rate cases[/green]")
    return records


def _fetch_rate_cases(start_year: int, end_year: int) -> list[dict]:
    """Fetch rate case data from IN IURC API."""
    all_records: dict[str, dict] = {}  # keyed by cause number

    with httpx.Client(timeout=30, headers=HEADERS) as client:
        for petition_name, petition_guid in PETITION_TYPES.items():
            console.print(f"[dim]  Searching: {petition_name}...[/dim]")
            cases = _search_cases(client, petition_guid)
            console.print(f"[dim]    Found {len(cases)} cases[/dim]")

            for case in cases:
                cause_num = case.get("iurc_docketnumber", "")
                if not cause_num:
                    continue

                # Filter by year
                petition_date = case.get("iurc_petitiondate", "")
                year = _extract_year(petition_date)
                if year and (year < start_year or year > end_year):
                    continue

                if cause_num in all_records:
                    continue

                # Get the petitioner (utility) name
                case_id = case.get("iurc_legalcaseid", "")
                utility_name = _extract_petitioner(case)

                # Get orders for decision date
                decision_date = None
                if case_id:
                    orders = _fetch_orders(client, case_id)
                    decision_date = _extract_decision_date(orders)
                    time.sleep(DELAY_SECONDS)

                # Map industry to utility type
                industry = (case.get("iurc_industry") or "").lower()
                utility_type = INDUSTRY_MAP.get(industry, "unknown")

                # Classify case type
                case_type = _classify_case_type(petition_name, case)

                # Map status
                status_raw = case.get("iurc_casestatustype", "")
                status = _map_status(status_raw)

                record = {
                    "docket_number": f"IURC-{cause_num}",
                    "utility_name": utility_name,
                    "state": STATE,
                    "source": SOURCE_KEY,
                    "case_type": case_type,
                    "utility_type": utility_type,
                    "status": status,
                    "filing_date": _parse_date(petition_date),
                    "decision_date": decision_date,
                    "description": _build_description(case),
                    "source_url": CASE_DETAIL_URL.format(case_id=case_id) if case_id else None,
                    "scraped_at": datetime.now(timezone.utc).isoformat(),
                }

                all_records[cause_num] = record

    records = list(all_records.values())
    console.print(f"[dim]  Total: {len(records)} rate cases in {start_year}-{end_year}[/dim]")
    return records


def _search_cases(
    client: httpx.Client,
    petition_type_guid: str,
) -> list[dict]:
    """Search for cases by petition type, paginating through all results."""
    all_cases = []
    page = 1

    while True:
        payload = {
            "txtCause": "",
            "txtSubDocket": "",
            "ddlPetitionType": petition_type_guid,
            "ddlCaseStatus": "-1",
            "ddlIndustry": "-1",
            "txtParties": "",
            "ddlUtilities": "-1",
            "txtDateBegin": "",
            "txtDateEnd": "",
            "txtFilingDateBegin": "",
            "txtFilingDateEnd": "",
            "txtOrderDateBegin": "",
            "txtOrderDateEnd": "",
            "txtPageNumber": str(page),
        }

        try:
            response = client.post(SEARCH_URL, json=payload)
            if response.status_code != 200:
                console.print(f"[yellow]    Page {page}: HTTP {response.status_code}[/yellow]")
                break

            data = response.json()
            cases = data.get("data", [])
            if not cases:
                break

            all_cases.extend(cases)

            total_pages = data.get("PagerDetails", {}).get("TotalPages", 1)
            if page >= total_pages:
                break

            page += 1
            time.sleep(DELAY_SECONDS)

        except Exception as e:
            console.print(f"[yellow]    Page {page} error: {e}[/yellow]")
            break

    return all_cases


def _fetch_orders(client: httpx.Client, case_id: str) -> list[dict]:
    """Fetch orders for a specific case."""
    try:
        payload = {"txtPageNumber": "1", "Id": f" {case_id} "}
        response = client.post(ORDERS_URL, json=payload)
        if response.status_code == 200:
            data = response.json()
            if isinstance(data, list):
                return data
    except Exception:
        pass
    return []


def _extract_petitioner(case: dict) -> str:
    """Extract the petitioner (utility) name from search results."""
    parties_str = case.get("iurc_forpetionersearch", "")
    if not parties_str:
        return ""

    # The first party in the comma-separated list is typically the petitioner
    # Format: "Utility Name - Industry, Party2, Party3..."
    parts = parties_str.split(",")
    if parts:
        first_party = parts[0].strip()
        # Remove zero-width spaces
        first_party = first_party.replace("\u200b", "").strip()
        # Remove industry suffix (e.g., " - Electric")
        dash_idx = first_party.rfind(" - ")
        if dash_idx > 0:
            first_party = first_party[:dash_idx].strip()
        return first_party

    return ""


def _extract_decision_date(orders: list[dict]) -> Optional[str]:
    """Extract the final order date from orders list."""
    for order in orders:
        order_type = (order.get("iurc_ordertype") or "").lower()
        if "final order" in order_type:
            return _parse_date(order.get("iurc_orderdate"))

    # Fallback: use the latest order date
    dates = []
    for order in orders:
        d = _parse_date(order.get("iurc_orderdate"))
        if d:
            dates.append(d)
    return max(dates) if dates else None


def _extract_year(date_str: str) -> Optional[int]:
    """Extract year from a date string."""
    if not date_str:
        return None
    for fmt in ("%m/%d/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(date_str.strip(), fmt).year
        except ValueError:
            continue
    # Try to find a 4-digit year
    match = re.search(r'(\d{4})', date_str)
    if match:
        return int(match.group(1))
    return None


def _parse_date(date_str: Optional[str]) -> Optional[str]:
    """Parse date string to ISO format."""
    if not date_str:
        return None
    for fmt in ("%m/%d/%Y", "%m/%d/%y", "%Y-%m-%d"):
        try:
            return datetime.strptime(date_str.strip(), fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def _classify_case_type(petition_name: str, case: dict) -> str:
    """Classify the case type."""
    name_lower = petition_name.lower()
    if "rates & financing" in name_lower:
        return "general_rate_case"
    if "rates" in name_lower:
        return "general_rate_case"
    return "general_rate_case"


def _map_status(status_raw: str) -> str:
    """Map IURC status to standard status."""
    s = status_raw.lower()
    if s == "decided":
        return "decided"
    if s in ("pending", "new"):
        return "active"
    if s == "appealed":
        return "decided"
    if s in ("archived", "void", "consolidated"):
        return "decided"
    return "unknown"


def _build_description(case: dict) -> Optional[str]:
    """Build a description from case data."""
    parts = []
    petition_type = case.get("iurc_petitiontypeid", "")
    industry = case.get("iurc_industry", "")
    if petition_type:
        parts.append(petition_type)
    if industry:
        parts.append(f"({industry})")

    parties = case.get("iurc_forpetionersearch", "")
    if parties:
        # Clean up zero-width spaces
        parties = parties.replace("\u200b", "").strip()
        if parties:
            parts.append(f"- {parties[:200]}")

    return " ".join(parts)[:500] if parts else None
