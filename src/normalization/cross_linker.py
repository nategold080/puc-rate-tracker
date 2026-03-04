"""Cross-link PUC utilities to EIA utility IDs.

Uses a three-stage matching process:
1. Exact normalized name + state match
2. Fuzzy name matching with state constraint
3. Fuzzy name matching without state (for multi-state utilities)

Also links to eGRID emissions data by matching utility names.
"""

from __future__ import annotations

from typing import Any, Optional

from rich.console import Console
from thefuzz import fuzz

from src.normalization.utilities import clean_utility_name

console = Console()

# Common words to strip for better matching
_NOISE_WORDS = {
    "company", "corporation", "corp", "inc", "incorporated",
    "llc", "lp", "co", "the", "of", "and", "utilities",
    "utility", "electric", "gas", "power", "energy",
}


def _normalize_for_match(name: str) -> str:
    """Normalize a utility name for matching purposes."""
    cleaned = clean_utility_name(name).lower().strip()
    # Remove common noise but keep distinctive parts
    tokens = cleaned.split()
    # Keep at least 2 tokens even if they're "noise"
    filtered = [t for t in tokens if t not in _NOISE_WORDS]
    if len(filtered) < 2:
        filtered = tokens
    return " ".join(filtered)


def cross_link_utilities(
    puc_utilities: list[dict[str, Any]],
    eia_records: list[dict[str, Any]],
    fuzzy_threshold: int = 82,
) -> list[dict[str, Any]]:
    """Link PUC utilities to EIA utility IDs.

    Args:
        puc_utilities: List of PUC utility dicts (name, state, canonical_name).
        eia_records: List of EIA operations records (eia_utility_id, utility_name, state).
        fuzzy_threshold: Minimum fuzzy match score.

    Returns:
        List of link dicts: {utility_name, state, eia_utility_id, match_confidence, match_method}.
    """
    # Build EIA lookup: (normalized_name, state) -> eia_utility_id
    eia_by_name_state: dict[tuple[str, str], dict] = {}
    eia_by_name: dict[str, list[dict]] = {}

    seen_eia = set()
    for rec in eia_records:
        eia_id = rec.get("eia_utility_id")
        name = rec.get("utility_name", "")
        state = (rec.get("state") or "").upper().strip()

        if not eia_id or not name:
            continue

        key = (eia_id, state)
        if key in seen_eia:
            continue
        seen_eia.add(key)

        norm = _normalize_for_match(name)
        info = {"eia_utility_id": eia_id, "utility_name": name, "state": state, "norm": norm}

        eia_by_name_state[(norm, state)] = info
        eia_by_name.setdefault(norm, []).append(info)

    links = []
    matched = 0
    unmatched = 0

    for util in puc_utilities:
        puc_name = util.get("canonical_name") or util.get("name", "")
        puc_state = (util.get("state") or "").upper().strip()

        if not puc_name:
            continue

        puc_norm = _normalize_for_match(puc_name)

        # Stage 1: Exact name + state match
        if (puc_norm, puc_state) in eia_by_name_state:
            info = eia_by_name_state[(puc_norm, puc_state)]
            links.append({
                "utility_name": puc_name,
                "state": puc_state,
                "eia_utility_id": info["eia_utility_id"],
                "match_confidence": 1.0,
                "match_method": "exact",
            })
            matched += 1
            continue

        # Stage 2: Fuzzy match with state constraint
        best_score = 0
        best_info = None

        for (norm, st), info in eia_by_name_state.items():
            if st != puc_state:
                continue
            score = fuzz.token_sort_ratio(puc_norm, norm)
            if score > best_score:
                best_score = score
                best_info = info

        if best_info and best_score >= fuzzy_threshold:
            links.append({
                "utility_name": puc_name,
                "state": puc_state,
                "eia_utility_id": best_info["eia_utility_id"],
                "match_confidence": round(best_score / 100, 2),
                "match_method": f"fuzzy_{best_score}",
            })
            matched += 1
            continue

        # Stage 3: Fuzzy match without state (multi-state utilities)
        best_score = 0
        best_info = None

        for norm, infos in eia_by_name.items():
            score = fuzz.token_sort_ratio(puc_norm, norm)
            if score > best_score:
                best_score = score
                best_info = infos[0]

        if best_info and best_score >= fuzzy_threshold + 5:  # Higher threshold for cross-state
            links.append({
                "utility_name": puc_name,
                "state": puc_state,
                "eia_utility_id": best_info["eia_utility_id"],
                "match_confidence": round(best_score / 100, 2),
                "match_method": f"fuzzy_cross_state_{best_score}",
            })
            matched += 1
            continue

        unmatched += 1

    console.print(
        f"[green]Cross-linked {matched} utilities to EIA IDs "
        f"({unmatched} unmatched)[/green]"
    )

    return links


def cross_link_emissions(
    eia_links: list[dict[str, Any]],
    emissions: list[dict[str, Any]],
) -> int:
    """Link emissions records to EIA utility IDs using existing links.

    Updates emissions records in-place with eia_utility_id where a
    matching link exists.

    Args:
        eia_links: List of utility-EIA link dicts.
        emissions: List of emissions record dicts.

    Returns:
        Number of emissions records linked.
    """
    # Build name+state -> eia_id lookup from existing links
    link_lookup: dict[tuple[str, str], int] = {}
    for link in eia_links:
        name = _normalize_for_match(link["utility_name"])
        state = link.get("state", "").upper()
        link_lookup[(name, state)] = link["eia_utility_id"]

    linked = 0
    for rec in emissions:
        egrid_name = rec.get("utility_name_egrid", "")
        state = (rec.get("state") or "").upper()
        norm = _normalize_for_match(egrid_name)

        if (norm, state) in link_lookup:
            rec["eia_utility_id"] = link_lookup[(norm, state)]
            linked += 1
            continue

        # Fuzzy match against linked utilities
        best_score = 0
        best_eia_id = None
        for (ln, ls), eia_id in link_lookup.items():
            if ls != state:
                continue
            score = fuzz.token_sort_ratio(norm, ln)
            if score > best_score and score >= 82:
                best_score = score
                best_eia_id = eia_id

        if best_eia_id:
            rec["eia_utility_id"] = best_eia_id
            linked += 1

    console.print(f"[green]Linked {linked}/{len(emissions)} emissions records to EIA IDs[/green]")
    return linked


def compute_rate_case_impacts(
    rate_cases: list[dict[str, Any]],
    eia_links: list[dict[str, Any]],
    operations: list[dict[str, Any]],
    avg_monthly_kwh: float = 886.0,
) -> list[dict[str, Any]]:
    """Compute consumer bill impact for rate cases with revenue data.

    Args:
        rate_cases: List of rate case dicts.
        eia_links: List of utility-EIA link dicts.
        operations: List of utility operations dicts.
        avg_monthly_kwh: Average residential monthly consumption (default: US avg ~886 kWh).

    Returns:
        List of impact dicts.
    """
    # Build utility name -> eia_id lookup
    name_to_eia: dict[str, int] = {}
    for link in eia_links:
        name_to_eia[link["utility_name"].lower()] = link["eia_utility_id"]

    # Build eia_id -> latest operations
    eia_ops: dict[int, dict] = {}
    for op in sorted(operations, key=lambda x: x.get("year", 0)):
        eid = op.get("eia_utility_id")
        if eid:
            eia_ops[eid] = op

    impacts = []
    for case in rate_cases:
        approved = case.get("approved_revenue_change")
        if not approved:
            continue

        # Find EIA ID for this utility
        canonical = (case.get("canonical_utility_name") or case.get("utility_name", "")).lower()
        eia_id = name_to_eia.get(canonical)

        if not eia_id:
            continue

        ops = eia_ops.get(eia_id)
        if not ops:
            continue

        total_customers = ops.get("total_customers")
        if not total_customers or total_customers <= 0:
            continue

        # Revenue change is in $M, convert to dollars
        annual_impact = (approved * 1_000_000) / total_customers
        monthly_impact = annual_impact / 12

        # Percentage of average bill
        res_price = ops.get("residential_avg_price")  # cents/kWh
        pct_of_avg = None
        if res_price and res_price > 0:
            avg_monthly_bill = (res_price / 100) * avg_monthly_kwh
            if avg_monthly_bill > 0:
                pct_of_avg = round((monthly_impact / avg_monthly_bill) * 100, 2)

        impacts.append({
            "docket_number": case["docket_number"],
            "source": case["source"],
            "eia_utility_id": eia_id,
            "total_customers": total_customers,
            "monthly_bill_impact": round(monthly_impact, 2),
            "annual_bill_impact": round(annual_impact, 2),
            "pct_of_avg_bill": pct_of_avg,
            "residential_price_before": res_price,
            "residential_price_after": None,
        })

    console.print(f"[green]Computed impacts for {len(impacts)} rate cases[/green]")
    return impacts
