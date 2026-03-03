"""Quality scoring for PUC Rate Case records.

Each rate case record is scored 0.0 to 1.0 based on data completeness
and validity. Components and weights per CLAUDE.md specification.

Quality Score Weights:
  - has_docket_number: 0.15
  - has_utility_name_resolved: 0.15
  - has_case_type_classified: 0.10
  - has_filing_date: 0.10
  - has_decision_date: 0.10
  - has_revenue_request_amount: 0.15
  - has_revenue_approved_amount: 0.15
  - has_case_status: 0.05
  - has_source_url: 0.05

Also performs referential integrity checks and data range validation.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any, Optional

from rich.console import Console

console = Console()


# --- Quality Score Weights ---

WEIGHTS = {
    "has_docket_number": 0.15,
    "has_utility_name_resolved": 0.15,
    "has_case_type_classified": 0.10,
    "has_filing_date": 0.10,
    "has_decision_date": 0.10,
    "has_revenue_request_amount": 0.15,
    "has_revenue_approved_amount": 0.15,
    "has_case_status": 0.05,
    "has_source_url": 0.05,
}


def score_rate_case(record: dict[str, Any], document_count: int = 0) -> dict:
    """Compute a quality score for a single rate case record.

    Args:
        record: Rate case dict with standard fields.
        document_count: Number of linked documents for this docket.

    Returns:
        Dict with:
          - quality_score: float 0.0-1.0
          - component_scores: dict of each component's contribution
          - issues: list of quality issue descriptions
    """
    components = {}
    issues = []

    # 1. Docket number (0.15)
    if record.get("docket_number"):
        components["has_docket_number"] = 1.0
    else:
        components["has_docket_number"] = 0.0
        issues.append("Missing docket number")

    # 2. Utility name resolved (0.15)
    if record.get("canonical_utility_name"):
        components["has_utility_name_resolved"] = 1.0
    elif record.get("utility_name"):
        components["has_utility_name_resolved"] = 0.7
    else:
        components["has_utility_name_resolved"] = 0.0
        issues.append("Missing utility name")

    # 3. Case type classified (0.10)
    case_type = record.get("case_type", "unknown")
    if case_type and case_type != "unknown":
        components["has_case_type_classified"] = 1.0
    else:
        components["has_case_type_classified"] = 0.0
        issues.append("Case type not classified")

    # 4. Filing date (0.10)
    if record.get("filing_date"):
        components["has_filing_date"] = 1.0
    else:
        components["has_filing_date"] = 0.0
        issues.append("Missing filing date")

    # 5. Decision date (0.10)
    if record.get("decision_date"):
        components["has_decision_date"] = 1.0
    else:
        status = record.get("status", "")
        if status in ("active", "filed"):
            components["has_decision_date"] = 0.5
        else:
            components["has_decision_date"] = 0.0
            issues.append("Missing decision date")

    # 6. Revenue request amount (0.15)
    if record.get("requested_revenue_change") is not None:
        components["has_revenue_request_amount"] = 1.0
    else:
        components["has_revenue_request_amount"] = 0.0
        issues.append("No revenue request amount")

    # 7. Revenue approved amount (0.15)
    if record.get("approved_revenue_change") is not None:
        components["has_revenue_approved_amount"] = 1.0
    else:
        components["has_revenue_approved_amount"] = 0.0
        issues.append("No revenue approved amount")

    # 8. Case status (0.05)
    if record.get("status") and record["status"] != "unknown":
        components["has_case_status"] = 1.0
    else:
        components["has_case_status"] = 0.0
        issues.append("Missing or unknown case status")

    # 9. Source URL (0.05)
    if record.get("source_url"):
        components["has_source_url"] = 1.0
    else:
        components["has_source_url"] = 0.0

    # Compute weighted total
    quality_score = sum(
        components[key] * WEIGHTS[key] for key in WEIGHTS
    )
    quality_score = round(min(max(quality_score, 0.0), 1.0), 3)

    return {
        "quality_score": quality_score,
        "component_scores": components,
        "issues": issues,
    }


def validate_record(record: dict[str, Any]) -> list[str]:
    """Run validation checks on a rate case record.

    Returns a list of validation error descriptions.
    """
    errors = []

    # Required fields
    if not record.get("docket_number"):
        errors.append("Missing docket_number")
    if not record.get("utility_name"):
        errors.append("Missing utility_name")
    if not record.get("state"):
        errors.append("Missing state")
    if not record.get("source"):
        errors.append("Missing source")

    # Date range validation
    filing_date = _parse_date(record.get("filing_date"))
    decision_date = _parse_date(record.get("decision_date"))

    if filing_date:
        if filing_date.year < 1990 or filing_date.year > 2030:
            errors.append(f"Filing date {filing_date} outside valid range (1990-2030)")
    if decision_date:
        if decision_date.year < 1990 or decision_date.year > 2030:
            errors.append(f"Decision date {decision_date} outside valid range (1990-2030)")

    # Date ordering
    if filing_date and decision_date and decision_date < filing_date:
        errors.append(
            f"Decision date ({decision_date}) before filing date ({filing_date})"
        )

    # Financial sanity checks
    requested = record.get("requested_revenue_change")
    approved = record.get("approved_revenue_change")

    if requested is not None and abs(requested) > 50000:
        errors.append(f"Requested revenue ${requested}M seems unreasonably large")

    if approved is not None and abs(approved) > 50000:
        errors.append(f"Approved revenue ${approved}M seems unreasonably large")

    if requested is not None and approved is not None:
        # Approved should generally not exceed requested (in absolute terms)
        if abs(approved) > abs(requested) * 1.5:
            errors.append(
                f"Approved ${approved}M significantly exceeds requested ${requested}M"
            )

    # ROE sanity
    roe = record.get("return_on_equity")
    if roe is not None:
        if roe < 0 or roe > 25:
            errors.append(f"ROE {roe}% outside reasonable range (0-25%)")

    # Rate base sanity
    rate_base = record.get("rate_base")
    if rate_base is not None:
        if rate_base < 0:
            errors.append(f"Rate base ${rate_base}M is negative")
        if rate_base > 100000:
            errors.append(f"Rate base ${rate_base}M seems unreasonably large")

    # State code validation
    state = record.get("state", "")
    if state and len(state) != 2:
        errors.append(f"State code '{state}' should be 2 characters")

    return errors


def check_referential_integrity(
    records: list[dict[str, Any]],
) -> list[str]:
    """Check referential integrity across a set of records.

    Args:
        records: List of rate case dicts.

    Returns:
        List of integrity issue descriptions.
    """
    issues = []
    docket_numbers = set()
    duplicates = set()

    for record in records:
        docket = record.get("docket_number", "")
        source = record.get("source", "")
        key = (docket, source)

        if key in docket_numbers:
            duplicates.add(key)
        docket_numbers.add(key)

    if duplicates:
        for docket, source in duplicates:
            issues.append(f"Duplicate docket: {docket} from {source}")

    # Check for utilities with multiple states (cross-state entity)
    utility_states: dict[str, set[str]] = {}
    for record in records:
        name = record.get("canonical_utility_name") or record.get("utility_name", "")
        state = record.get("state", "")
        if name and state:
            utility_states.setdefault(name, set()).add(state)

    for name, states in utility_states.items():
        if len(states) > 1:
            issues.append(
                f"Utility '{name}' appears in multiple states: {', '.join(sorted(states))}"
            )

    return issues


def _parse_date(value: Any) -> Optional[date]:
    """Parse a date from various formats."""
    if value is None:
        return None
    if isinstance(value, date):
        return value
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, str):
        try:
            return date.fromisoformat(value[:10])
        except (ValueError, IndexError):
            return None
    return None


# --- Batch Quality Scoring ---


def score_all_records(
    records: list[dict[str, Any]],
    document_counts: Optional[dict[str, int]] = None,
) -> list[dict]:
    """Score quality for a batch of records.

    Args:
        records: List of rate case dicts.
        document_counts: Optional dict mapping docket_number to document count.

    Returns:
        List of records with quality_score added.
    """
    if document_counts is None:
        document_counts = {}

    scored = []
    for record in records:
        doc_count = document_counts.get(record.get("docket_number", ""), 0)
        result = score_rate_case(record, document_count=doc_count)
        record = dict(record)
        record["quality_score"] = result["quality_score"]
        scored.append(record)

    return scored


def validate_all() -> None:
    """Run quality validation on all records in the database.

    Updates quality scores and logs issues.
    """
    from src.storage.database import (
        get_all_rate_cases,
        get_connection,
        update_quality_scores,
    )

    conn = get_connection()
    cases = get_all_rate_cases(limit=10000, conn=conn)
    console.print(f"[dim]Validating {len(cases)} rate cases...[/dim]")

    scores = {}
    total_issues = 0
    error_count = 0

    for case in cases:
        docket = case.get("docket_number", "")
        source = case.get("source", "")

        # Validate
        errors = validate_record(case)
        if errors:
            error_count += 1

        # Score
        result = score_rate_case(case)
        scores[(docket, source)] = result["quality_score"]
        total_issues += len(result["issues"])

    # Update scores in database
    updated = update_quality_scores(scores, conn=conn)

    # Summary statistics
    all_scores = list(scores.values())
    if all_scores:
        avg_score = sum(all_scores) / len(all_scores)
        above_threshold = sum(1 for s in all_scores if s >= 0.6)
        min_score = min(all_scores)
        max_score = max(all_scores)
    else:
        avg_score = 0.0
        above_threshold = 0
        min_score = 0.0
        max_score = 0.0

    console.print(f"\n[bold]Quality Validation Results:[/bold]")
    console.print(f"  Records scored: {len(all_scores)}")
    console.print(f"  Average score: {avg_score:.3f}")
    console.print(f"  Min score: {min_score:.3f}")
    console.print(f"  Max score: {max_score:.3f}")
    console.print(f"  Above threshold (>=0.6): {above_threshold}/{len(all_scores)}")
    console.print(f"  Records with validation errors: {error_count}")
    console.print(f"  Total quality issues: {total_issues}")

    # Check referential integrity
    integrity_issues = check_referential_integrity(cases)
    if integrity_issues:
        console.print(f"\n[yellow]Referential integrity issues ({len(integrity_issues)}):[/yellow]")
        for issue in integrity_issues[:10]:
            console.print(f"  [yellow]- {issue}[/yellow]")

    conn.close()
