"""Utility entity resolution and normalization.

Resolves utility names across different PUC systems to canonical forms.
Uses a three-stage approach:
  1. Exact alias lookup from utility_aliases.yaml
  2. Cleaned name matching (strip suffixes, expand abbreviations)
  3. Fuzzy matching via thefuzz (token_sort_ratio >= 85)

Also classifies utility types and links to parent companies.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Optional

import yaml
from rich.console import Console
from thefuzz import fuzz

from src.validation.schemas import OwnershipType, UtilityType

console = Console()

PROJECT_ROOT = Path(__file__).parent.parent.parent
CONFIG_DIR = PROJECT_ROOT / "config"

# Cache for loaded config
_aliases_config: Optional[dict] = None


def _load_aliases() -> dict:
    """Load utility aliases configuration."""
    global _aliases_config
    if _aliases_config is not None:
        return _aliases_config

    path = CONFIG_DIR / "utility_aliases.yaml"
    if path.exists():
        with open(path) as f:
            _aliases_config = yaml.safe_load(f) or {}
    else:
        _aliases_config = {}

    return _aliases_config


def _build_alias_lookup() -> dict[str, dict]:
    """Build a flat lookup from every alias to its canonical info.

    Returns:
        Dict mapping lowercased alias -> {canonical, parent, utility_type, state}.
    """
    config = _load_aliases()
    canonical_names = config.get("canonical_names", {})
    lookup = {}

    for _key, entry in canonical_names.items():
        canonical = entry.get("canonical", "")
        parent = entry.get("parent")
        utype = entry.get("utility_type", "unknown")
        state = entry.get("state")

        info = {
            "canonical": canonical,
            "parent": parent,
            "utility_type": utype,
            "state": state,
        }

        # Add the canonical name itself
        lookup[canonical.lower()] = info

        # Add all aliases
        for alias in entry.get("aliases", []):
            lookup[alias.lower()] = info

    return lookup


# Cached lookup table
_alias_lookup: Optional[dict[str, dict]] = None


def _get_alias_lookup() -> dict[str, dict]:
    global _alias_lookup
    if _alias_lookup is None:
        _alias_lookup = _build_alias_lookup()
    return _alias_lookup


# --- Name Cleaning ---


def clean_utility_name(name: str) -> str:
    """Clean a utility name by stripping suffixes and normalizing whitespace.

    Args:
        name: Raw utility name.

    Returns:
        Cleaned name with suffixes removed and whitespace normalized.
    """
    config = _load_aliases()
    suffixes = config.get("strip_suffixes", [])

    cleaned = name.strip()

    # Remove common suffixes
    for suffix in sorted(suffixes, key=len, reverse=True):
        # Match suffix at end of string, optionally preceded by comma
        pattern = r',?\s*' + re.escape(suffix) + r'\s*$'
        cleaned = re.sub(pattern, '', cleaned, flags=re.IGNORECASE)

    # Expand abbreviations
    expansions = config.get("abbreviation_expansions", {})
    for abbr, expanded in expansions.items():
        # Only expand if the abbreviation is a word boundary
        if abbr == "&":
            cleaned = cleaned.replace(" & ", f" {expanded} ")
        else:
            pattern = r'\b' + re.escape(abbr) + r'\b'
            cleaned = re.sub(pattern, expanded, cleaned, flags=re.IGNORECASE)

    # Normalize whitespace
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()

    # Remove trailing punctuation
    cleaned = cleaned.rstrip(".,;:")

    return cleaned


def resolve_utility_name(
    raw_name: str, state: Optional[str] = None
) -> dict[str, Optional[str]]:
    """Resolve a raw utility name to its canonical form.

    Three-stage resolution:
    1. Exact alias lookup
    2. Cleaned name exact lookup
    3. Fuzzy matching (token_sort_ratio >= threshold)

    Args:
        raw_name: The raw utility name from scraping.
        state: Optional state code to improve matching.

    Returns:
        Dict with keys: canonical_name, parent_company, utility_type, match_method.
    """
    if not raw_name or not raw_name.strip():
        return {
            "canonical_name": None,
            "parent_company": None,
            "utility_type": "unknown",
            "match_method": None,
        }

    lookup = _get_alias_lookup()
    raw_lower = raw_name.strip().lower()

    # Stage 1: Exact alias lookup
    if raw_lower in lookup:
        info = lookup[raw_lower]
        return {
            "canonical_name": info["canonical"],
            "parent_company": info["parent"],
            "utility_type": info["utility_type"],
            "match_method": "exact_alias",
        }

    # Stage 2: Cleaned name lookup
    cleaned = clean_utility_name(raw_name)
    cleaned_lower = cleaned.lower()

    if cleaned_lower in lookup:
        info = lookup[cleaned_lower]
        return {
            "canonical_name": info["canonical"],
            "parent_company": info["parent"],
            "utility_type": info["utility_type"],
            "match_method": "cleaned_exact",
        }

    # Stage 3: Fuzzy matching
    config = _load_aliases()
    fuzzy_config = config.get("fuzzy_match", {})
    threshold = fuzzy_config.get("threshold", 85)

    best_score = 0
    best_info = None

    for alias_key, info in lookup.items():
        # If state is known, prefer same-state matches
        if state and info.get("state") and info["state"] != state.upper():
            continue

        score = fuzz.token_sort_ratio(cleaned_lower, alias_key)
        if score > best_score and score >= threshold:
            best_score = score
            best_info = info

    # If no state-filtered match, try without state filter
    if best_info is None and state:
        for alias_key, info in lookup.items():
            score = fuzz.token_sort_ratio(cleaned_lower, alias_key)
            if score > best_score and score >= threshold:
                best_score = score
                best_info = info

    if best_info:
        return {
            "canonical_name": best_info["canonical"],
            "parent_company": best_info["parent"],
            "utility_type": best_info["utility_type"],
            "match_method": f"fuzzy_{best_score}",
        }

    # No match found — use cleaned name as canonical
    return {
        "canonical_name": cleaned,
        "parent_company": None,
        "utility_type": "unknown",
        "match_method": "unresolved",
    }


def classify_ownership_type(name: str) -> OwnershipType:
    """Classify the ownership type of a utility from its name.

    Args:
        name: Utility name.

    Returns:
        OwnershipType enum value.
    """
    name_lower = name.lower()

    # Check for cooperative patterns
    if any(kw in name_lower for kw in ["cooperative", "co-op", "coop", "rural electric"]):
        return OwnershipType.COOPERATIVE

    # Check for municipal patterns
    if any(kw in name_lower for kw in [
        "city of", "town of", "village of", "borough of",
        "municipal electric", "municipal gas", "municipal water",
        "municipal utility", "municipal light"
    ]):
        return OwnershipType.MUNICIPAL

    # Check for IOU patterns (most regulated utilities)
    if any(kw in name_lower for kw in [
        "inc.", "incorporated", "corp.", "corporation", "company",
        "co.", "llc", "lp", "l.p."
    ]):
        return OwnershipType.INVESTOR_OWNED

    # Default: if it's in a PUC docket system, likely IOU
    return OwnershipType.INVESTOR_OWNED


def get_parent_company(utility_name: str) -> Optional[str]:
    """Look up the parent company for a utility.

    Args:
        utility_name: Utility name (raw or canonical).

    Returns:
        Parent company name, or None if not found.
    """
    result = resolve_utility_name(utility_name)
    return result["parent_company"]


def normalize_all_utilities(
    records: list[dict],
) -> tuple[list[dict], dict[str, str]]:
    """Normalize utility names across all records.

    Args:
        records: List of rate case record dicts.

    Returns:
        Tuple of (updated records, name_mapping dict).
    """
    name_mapping = {}  # raw_name -> canonical_name
    updated = []

    for record in records:
        raw_name = record.get("utility_name", "")
        state = record.get("state")

        if raw_name not in name_mapping:
            result = resolve_utility_name(raw_name, state)
            name_mapping[raw_name] = result["canonical_name"]

        record = dict(record)
        record["canonical_utility_name"] = name_mapping[raw_name]
        updated.append(record)

    return updated, name_mapping


def normalize_utilities() -> None:
    """Run normalization on all rate cases in the database.

    Updates canonical_utility_name on all rate case records and
    creates/updates utility entries.
    """
    from src.storage.database import (
        get_all_rate_cases,
        get_connection,
        update_canonical_names,
        upsert_utility,
    )

    conn = get_connection()

    # Get all rate cases
    cases = get_all_rate_cases(limit=10000, conn=conn)
    console.print(f"[dim]Normalizing utilities for {len(cases)} rate cases...[/dim]")

    name_mapping = {}
    utility_data = {}  # canonical -> utility info

    for case in cases:
        raw_name = case.get("utility_name", "")
        state = case.get("state")

        if raw_name not in name_mapping:
            result = resolve_utility_name(raw_name, state)
            canonical = result["canonical_name"] or raw_name
            name_mapping[raw_name] = canonical

            if canonical not in utility_data:
                ownership = classify_ownership_type(canonical)
                utility_data[canonical] = {
                    "name": canonical,
                    "canonical_name": canonical,
                    "state": state,
                    "utility_type": result["utility_type"],
                    "ownership_type": ownership.value,
                    "parent_company": result["parent_company"],
                }

    # Update canonical names on rate cases
    count = update_canonical_names(name_mapping, conn=conn)
    console.print(f"[green]Updated canonical names on {count} rate case records[/green]")

    # Upsert utility records
    util_count = 0
    for canonical, data in utility_data.items():
        upsert_utility(data, conn=conn)
        util_count += 1

    console.print(f"[green]Created/updated {util_count} utility records[/green]")

    resolved = sum(1 for v in name_mapping.values() if v != "unresolved")
    console.print(
        f"[dim]Resolution: {resolved}/{len(name_mapping)} utility names resolved[/dim]"
    )

    conn.close()
