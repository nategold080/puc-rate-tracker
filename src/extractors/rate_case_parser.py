"""Rate case parser and extractor.

Parses raw scraped data (HTML or structured dicts) into validated
RateCase records. Uses rule-based regex patterns to extract:
  - Dollar amounts (revenue requests, approvals, rate base)
  - Dates in multiple formats
  - Case type classification
  - Utility type classification
  - Status normalization

Zero LLM dependency: all extraction is deterministic.
"""

from __future__ import annotations

import re
from datetime import date, datetime
from pathlib import Path
from typing import Any, Optional

import yaml
from rich.console import Console

from src.validation.schemas import (
    CaseStatus,
    CaseType,
    RateCase,
    UtilityType,
    parse_date_flexible,
    parse_dollar_amount,
    parse_percentage,
)

console = Console()

PROJECT_ROOT = Path(__file__).parent.parent.parent
CONFIG_DIR = PROJECT_ROOT / "config"


# --- Load classification rules from config ---


def _load_config(filename: str) -> dict:
    """Load a YAML config file."""
    path = CONFIG_DIR / filename
    if path.exists():
        with open(path) as f:
            return yaml.safe_load(f)
    return {}


_rate_case_types_config = None
_sources_config = None


def _get_rate_case_types() -> dict:
    global _rate_case_types_config
    if _rate_case_types_config is None:
        _rate_case_types_config = _load_config("rate_case_types.yaml")
    return _rate_case_types_config


def _get_sources() -> dict:
    global _sources_config
    if _sources_config is None:
        _sources_config = _load_config("sources.yaml")
    return _sources_config


# --- Dollar Amount Extraction ---


# Pattern: $X.X million/billion
_DOLLAR_UNIT_RE = re.compile(
    r'\$\s*([\d,]+(?:\.\d+)?)\s*(million|billion|M|B)',
    re.IGNORECASE
)

# Pattern: $X,XXX,XXX.XX (plain)
_DOLLAR_PLAIN_RE = re.compile(
    r'\$\s*([\d,]+(?:\.\d{1,2})?)'
)

# Negative/decrease indicators
_DECREASE_RE = re.compile(
    r'(?:decrease|reduction|lower|cut)',
    re.IGNORECASE
)


def extract_dollar_amount(text: str) -> Optional[float]:
    """Extract a dollar amount from text, returning value in millions.

    Handles:
      - "$245.6 million" -> 245.6
      - "$2.8 billion" -> 2800.0
      - "$25,300,000" -> 25.3
      - "$25.3M" -> 25.3
      - "decrease of $10 million" -> -10.0 (negative)

    Returns:
        Float in millions, or None if no amount found.
    """
    if not text:
        return None

    is_decrease = bool(_DECREASE_RE.search(text))

    # Try unit-based pattern first
    match = _DOLLAR_UNIT_RE.search(text)
    if match:
        amount = float(match.group(1).replace(",", ""))
        unit = match.group(2).lower()
        if unit in ("billion", "b"):
            amount *= 1000.0
        # million/M: amount is already in millions
        result = round(amount, 3)
        return -result if is_decrease else result

    # Try plain dollar amount
    match = _DOLLAR_PLAIN_RE.search(text)
    if match:
        amount = float(match.group(1).replace(",", ""))
        result = round(amount / 1_000_000, 3)
        if result < 0.001:
            return None  # Too small to be meaningful
        return -result if is_decrease else result

    return None


def extract_all_dollar_amounts(text: str) -> list[float]:
    """Extract all dollar amounts from text, returned in millions."""
    if not text:
        return []

    amounts = []
    is_decrease = bool(_DECREASE_RE.search(text))

    for match in _DOLLAR_UNIT_RE.finditer(text):
        amount = float(match.group(1).replace(",", ""))
        unit = match.group(2).lower()
        if unit in ("billion", "b"):
            amount *= 1000.0
        result = round(amount, 3)
        amounts.append(-result if is_decrease else result)

    if not amounts:
        for match in _DOLLAR_PLAIN_RE.finditer(text):
            amount = float(match.group(1).replace(",", ""))
            result = round(amount / 1_000_000, 3)
            if result >= 0.001:
                amounts.append(-result if is_decrease else result)

    return amounts


# --- Date Extraction ---


_DATE_PATTERNS = [
    # ISO: 2024-03-28
    re.compile(r'(\d{4}-\d{2}-\d{2})'),
    # US: 03/28/2024 or 3/28/2024
    re.compile(r'(\d{1,2}/\d{1,2}/\d{4})'),
    # Long: March 28, 2024
    re.compile(
        r'((?:January|February|March|April|May|June|July|August|September|'
        r'October|November|December)\s+\d{1,2},?\s+\d{4})',
        re.IGNORECASE
    ),
    # Short: Mar 28, 2024
    re.compile(
        r'((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\.?\s+\d{1,2},?\s+\d{4})',
        re.IGNORECASE
    ),
]


def extract_date(text: str) -> Optional[date]:
    """Extract the first date from text.

    Supports: YYYY-MM-DD, MM/DD/YYYY, Month DD YYYY, Mon DD YYYY.
    """
    if not text:
        return None

    for pattern in _DATE_PATTERNS:
        match = pattern.search(text)
        if match:
            result = parse_date_flexible(match.group(1))
            if result:
                return result

    return None


def extract_all_dates(text: str) -> list[date]:
    """Extract all dates from text."""
    if not text:
        return []

    dates = []
    for pattern in _DATE_PATTERNS:
        for match in pattern.finditer(text):
            d = parse_date_flexible(match.group(1))
            if d and d not in dates:
                dates.append(d)

    return sorted(dates)


# --- Case Type Classification ---


def classify_case_type(text: str, docket_number: str = "") -> CaseType:
    """Classify the type of rate case from description text and docket number.

    Uses pattern matching from rate_case_types.yaml config.
    """
    if not text and not docket_number:
        return CaseType.UNKNOWN

    combined = f"{docket_number} {text}".lower()
    config = _get_rate_case_types()
    case_types = config.get("case_types", {})

    # Check each case type's patterns
    # Priority: more specific types first, then general
    priority_order = [
        "fuel_cost_adjustment",
        "infrastructure_rider",
        "decoupling_mechanism",
        "rate_design",
        "distribution_rate_case",
        "transmission_rate_case",
        "general_rate_case",
    ]

    for type_key in priority_order:
        type_config = case_types.get(type_key, {})
        patterns = type_config.get("docket_patterns", [])
        keywords = type_config.get("keywords", [])

        # Check regex patterns
        for pattern in patterns:
            try:
                if re.search(pattern, combined, re.IGNORECASE):
                    return CaseType(type_key)
            except re.error:
                continue

        # Check keywords
        for keyword in keywords:
            if keyword.lower() in combined:
                return CaseType(type_key)

    return CaseType.UNKNOWN


# --- Utility Type Classification ---


def classify_utility_type(
    text: str, docket_number: str = "", state: str = ""
) -> UtilityType:
    """Classify utility type from description text and docket number.

    Uses service_types config and docket prefix patterns.
    """
    if not text and not docket_number:
        return UtilityType.UNKNOWN

    combined = f"{docket_number} {text}".lower()
    config = _get_rate_case_types()
    service_types = config.get("service_types", {})

    # Check docket prefix patterns (state-specific)
    if docket_number:
        docket_upper = docket_number.upper()
        # Oregon / Washington prefixes
        if docket_upper.startswith("UE-") or docket_upper.startswith("UE "):
            return UtilityType.ELECTRIC
        if docket_upper.startswith("UG-") or docket_upper.startswith("UG "):
            return UtilityType.GAS
        if docket_upper.startswith("UW-") or docket_upper.startswith("UW "):
            return UtilityType.WATER

    # Map of config keys to UtilityType
    # Order matters: check more specific types before broader ones
    # (natural_gas before electric to avoid "distribution" keyword clash)
    type_mapping = [
        ("natural_gas", UtilityType.GAS),
        ("water", UtilityType.WATER),
        ("wastewater", UtilityType.WASTEWATER),
        ("telecommunications", UtilityType.TELECOMMUNICATIONS),
        ("multi_service", UtilityType.MULTI_SERVICE),
        ("electric", UtilityType.ELECTRIC),
    ]

    for type_key, utility_type in type_mapping:
        type_config = service_types.get(type_key, {})
        patterns = type_config.get("docket_patterns", [])
        keywords = type_config.get("keywords", [])

        for pattern in patterns:
            try:
                if re.search(pattern, combined, re.IGNORECASE):
                    return utility_type
            except re.error:
                continue

        for keyword in keywords:
            if keyword.lower() in combined:
                return utility_type

    return UtilityType.UNKNOWN


# --- Status Normalization ---


def normalize_status(raw_status: str) -> CaseStatus:
    """Normalize raw status text into a standard CaseStatus enum value.

    Uses case_statuses config patterns.
    """
    if not raw_status:
        return CaseStatus.UNKNOWN

    raw_status = raw_status.strip()
    config = _get_rate_case_types()
    statuses = config.get("case_statuses", {})

    for status_key, status_config in statuses.items():
        patterns = status_config.get("source_patterns", [])
        for pattern in patterns:
            try:
                if re.search(pattern, raw_status, re.IGNORECASE):
                    return CaseStatus(status_key)
            except (re.error, ValueError):
                continue

    return CaseStatus.UNKNOWN


# --- Revenue Context Extraction ---


# Multiple patterns tried in order for revenue requests
_REVENUE_REQUEST_PATTERNS = [
    # "requesting revenue increase of $X million"
    re.compile(
        r'(?:request(?:ed|ing)?|seek(?:s|ing)?|propos(?:ed|ing)?)\s+'
        r'(?:a\s+)?(?:revenue\s+)?(?:increase|decrease|change)\s+'
        r'(?:of\s+)?\$\s*([\d,]+(?:\.\d+)?)\s*(million|billion|M|B)?',
        re.IGNORECASE
    ),
    # "increase revenue requirements by $X billion"
    re.compile(
        r'(?:increase|decrease|change)\s+(?:its?\s+)?'
        r'(?:authorized\s+)?(?:revenue\s+)?(?:requirements?|revenues?)\s+'
        r'(?:by\s+)?\$\s*([\d,]+(?:\.\d+)?)\s*(million|billion|M|B)?',
        re.IGNORECASE
    ),
    # "revenue increase of $X million"
    re.compile(
        r'revenue\s+(?:increase|decrease|change)\s+'
        r'(?:of\s+)?\$\s*([\d,]+(?:\.\d+)?)\s*(million|billion|M|B)?',
        re.IGNORECASE
    ),
    # "rate increase of $X million" / "rate increase requesting $X million"
    re.compile(
        r'rate\s+increase\s+(?:of\s+|requesting\s+)?\$\s*([\d,]+(?:\.\d+)?)\s*(million|billion|M|B)?',
        re.IGNORECASE
    ),
    # "$X million revenue increase" / "$X billion over N years"
    re.compile(
        r'\$\s*([\d,]+(?:\.\d+)?)\s*(million|billion|M|B)\s+'
        r'(?:revenue\s+)?(?:increase|decrease|change|over)',
        re.IGNORECASE
    ),
    # "requesting $X million" (simple)
    re.compile(
        r'(?:request(?:ed|ing)?|seek(?:s|ing)?)\s+'
        r'(?:a\s+(?:total\s+)?(?:revenue\s+)?(?:increase|decrease)\s+of\s+)?'
        r'\$\s*([\d,]+(?:\.\d+)?)\s*(million|billion|M|B)',
        re.IGNORECASE
    ),
]

_REVENUE_APPROVED_PATTERNS = [
    # "approved revenue increase of $X million"
    re.compile(
        r'(?:approv(?:ed|ing)|authoriz(?:ed|ing)|grant(?:ed|ing))\s+'
        r'(?:a\s+)?(?:revenue\s+)?(?:increase|decrease|change)\s+'
        r'(?:of\s+)?\$\s*([\d,]+(?:\.\d+)?)\s*(million|billion|M|B)?',
        re.IGNORECASE
    ),
    # "approved $X million"
    re.compile(
        r'(?:approv(?:ed|ing)|authoriz(?:ed|ing)|grant(?:ed|ing))\s+'
        r'\$\s*([\d,]+(?:\.\d+)?)\s*(million|billion|M|B)',
        re.IGNORECASE
    ),
]

_ROE_RE = re.compile(
    r'(?:return\s+on\s+equity|ROE)\s+(?:of\s+)?'
    r'([\d]+(?:\.\d+)?)\s*%',
    re.IGNORECASE
)

_RATE_BASE_RE = re.compile(
    r'rate\s+base\s+(?:of\s+)?\$\s*([\d,]+(?:\.\d+)?)\s*(million|billion|M|B)?',
    re.IGNORECASE
)


def extract_revenue_request(text: str) -> Optional[float]:
    """Extract the requested revenue change amount from text (in $M)."""
    if not text:
        return None
    for pattern in _REVENUE_REQUEST_PATTERNS:
        match = pattern.search(text)
        if match:
            return _parse_matched_amount(match)
    return None


def extract_revenue_approved(text: str) -> Optional[float]:
    """Extract the approved revenue change amount from text (in $M)."""
    if not text:
        return None
    for pattern in _REVENUE_APPROVED_PATTERNS:
        match = pattern.search(text)
        if match:
            return _parse_matched_amount(match)
    return None


def extract_roe(text: str) -> Optional[float]:
    """Extract return on equity percentage from text."""
    if not text:
        return None
    match = _ROE_RE.search(text)
    if match:
        return round(float(match.group(1)), 2)
    return None


def extract_rate_base(text: str) -> Optional[float]:
    """Extract rate base amount from text (in $M)."""
    if not text:
        return None
    match = _RATE_BASE_RE.search(text)
    if match:
        return _parse_matched_amount(match)
    return None


def _parse_matched_amount(match: re.Match) -> float:
    """Parse a dollar amount from a regex match with optional unit group."""
    amount = float(match.group(1).replace(",", ""))
    unit = match.group(2).lower() if match.group(2) else None

    if unit in ("billion", "b"):
        return round(amount * 1000.0, 3)
    elif unit in ("million", "m"):
        return round(amount, 3)
    else:
        # Plain number; assume dollars, convert to millions
        return round(amount / 1_000_000, 3)


# --- Full Record Parsing ---


def parse_raw_record(raw: dict[str, Any]) -> Optional[dict]:
    """Parse a raw scraped record into a normalized dict suitable for RateCase.

    This is the main extraction function that takes a raw dict from a scraper
    and applies all extraction rules to produce a clean record.

    Args:
        raw: Raw data dict from a scraper.

    Returns:
        Normalized record dict, or None if critical fields are missing.
    """
    docket_number = raw.get("docket_number", "").strip()
    if not docket_number:
        return None

    utility_name = raw.get("utility_name", "").strip()
    if not utility_name:
        return None

    state = raw.get("state", "").strip().upper()
    source = raw.get("source", "").strip()

    if not state or not source:
        return None

    # Description text for classification
    description = raw.get("description", "") or ""

    # Classify case type
    case_type = raw.get("case_type")
    if case_type and case_type != "unknown":
        try:
            case_type = CaseType(case_type)
        except ValueError:
            case_type = classify_case_type(description, docket_number)
    else:
        case_type = classify_case_type(description, docket_number)

    # Classify utility type
    utility_type = raw.get("utility_type")
    if utility_type and utility_type != "unknown":
        try:
            utility_type = UtilityType(utility_type)
        except ValueError:
            utility_type = classify_utility_type(description, docket_number, state)
    else:
        utility_type = classify_utility_type(description, docket_number, state)

    # Normalize status
    raw_status = raw.get("status", "")
    if raw_status and raw_status != "unknown":
        status = normalize_status(raw_status)
    else:
        status = CaseStatus.UNKNOWN

    # Parse dates
    filing_date = _parse_field_date(raw.get("filing_date"))
    decision_date = _parse_field_date(raw.get("decision_date"))
    effective_date = _parse_field_date(raw.get("effective_date"))

    # Parse financial data
    requested_rev = _parse_field_float(raw.get("requested_revenue_change"))
    approved_rev = _parse_field_float(raw.get("approved_revenue_change"))
    rate_base = _parse_field_float(raw.get("rate_base"))
    roe = _parse_field_float(raw.get("return_on_equity"))
    req_pct = _parse_field_float(raw.get("requested_rate_change_pct"))
    app_pct = _parse_field_float(raw.get("approved_rate_change_pct"))

    # If financial data is missing, try to extract from description
    if description:
        if requested_rev is None:
            requested_rev = extract_revenue_request(description)
        if approved_rev is None:
            approved_rev = extract_revenue_approved(description)
        if roe is None:
            roe = extract_roe(description)
        if rate_base is None:
            rate_base = extract_rate_base(description)
        # Fallback: if still no requested revenue, try generic dollar extraction
        # (for descriptions that just mention "$X million" without specific verbs)
        if requested_rev is None:
            requested_rev = extract_dollar_amount(description)

    return {
        "docket_number": docket_number,
        "utility_name": utility_name,
        "state": state,
        "source": source,
        "case_type": case_type.value if hasattr(case_type, 'value') else case_type,
        "utility_type": utility_type.value if hasattr(utility_type, 'value') else utility_type,
        "status": status.value if hasattr(status, 'value') else status,
        "filing_date": filing_date,
        "decision_date": decision_date,
        "effective_date": effective_date,
        "requested_revenue_change": requested_rev,
        "approved_revenue_change": approved_rev,
        "rate_base": rate_base,
        "return_on_equity": roe,
        "requested_rate_change_pct": req_pct,
        "approved_rate_change_pct": app_pct,
        "source_url": raw.get("source_url"),
        "description": description or None,
        "scraped_at": raw.get("scraped_at"),
    }


def _parse_field_date(value: Any) -> Optional[date]:
    """Parse a date field that might be a string, date, or None."""
    if value is None:
        return None
    if isinstance(value, date):
        return value
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, str):
        return parse_date_flexible(value)
    return None


def _parse_field_float(value: Any) -> Optional[float]:
    """Parse a float field that might be a string, number, or None."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return parse_dollar_amount(value)
    return None


# --- Batch Extraction ---


def extract_source(source_key: str) -> list[dict]:
    """Extract records from a specific source's cached raw data.

    Args:
        source_key: The source identifier (e.g., 'pennsylvania_puc').

    Returns:
        List of parsed record dicts.
    """
    import json

    raw_dir = PROJECT_ROOT / "data" / "raw"
    source_dirs = {
        "pennsylvania_puc": raw_dir / "pa_puc",
        "oregon_puc": raw_dir / "or_puc",
        "california_cpuc": raw_dir / "ca_cpuc",
        "indiana_iurc": raw_dir / "in_iurc",
        "washington_utc": raw_dir / "wa_utc",
        "connecticut_pura": raw_dir / "ct_pura",
        "missouri_psc": raw_dir / "mo_psc",
        "georgia_psc": raw_dir / "ga_psc",
    }

    source_dir = source_dirs.get(source_key)
    if not source_dir or not source_dir.exists():
        console.print(f"[yellow]No raw data directory for {source_key}[/yellow]")
        return []

    records = []
    json_files = list(source_dir.glob("*.json"))

    for json_file in json_files:
        try:
            with open(json_file) as f:
                raw_records = json.load(f)

            if isinstance(raw_records, list):
                for raw in raw_records:
                    parsed = parse_raw_record(raw)
                    if parsed:
                        records.append(parsed)
            elif isinstance(raw_records, dict):
                parsed = parse_raw_record(raw_records)
                if parsed:
                    records.append(parsed)
        except (json.JSONDecodeError, KeyError) as e:
            console.print(f"[yellow]Error parsing {json_file}: {e}[/yellow]")

    console.print(f"[green]Extracted {len(records)} records from {source_key}[/green]")
    return records


def extract_all() -> dict[str, list[dict]]:
    """Extract records from all sources.

    Returns:
        Dict mapping source_key to list of parsed record dicts.
    """
    sources = [
        "pennsylvania_puc",
        "oregon_puc",
        "california_cpuc",
        "indiana_iurc",
        "washington_utc",
        "connecticut_pura",
        "missouri_psc",
        "georgia_psc",
    ]

    all_records = {}
    total = 0

    for source_key in sources:
        records = extract_source(source_key)
        all_records[source_key] = records
        total += len(records)

    console.print(f"\n[bold green]Total extracted: {total} records from {len(sources)} sources[/bold green]")
    return all_records
