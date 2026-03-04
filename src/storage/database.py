"""SQLite storage layer for PUC Rate Case Tracker.

Uses WAL mode for concurrent reads. All database functions accept an
optional `conn` parameter for testability and transaction control.
"""

from __future__ import annotations

import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from rich.console import Console
from rich.table import Table

PROJECT_ROOT = Path(__file__).parent.parent.parent
DB_PATH = PROJECT_ROOT / "data" / "puc_rate_cases.db"

console = Console()


# --- Schema DDL ---

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS utilities (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    canonical_name TEXT,
    state TEXT,
    utility_type TEXT DEFAULT 'unknown',
    ownership_type TEXT DEFAULT 'unknown',
    parent_company TEXT,
    customer_count INTEGER,
    eia_utility_id INTEGER,
    ferc_respondent_id INTEGER,
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now')),
    UNIQUE(name, state)
);

CREATE TABLE IF NOT EXISTS rate_cases (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    docket_number TEXT NOT NULL,
    utility_name TEXT NOT NULL,
    canonical_utility_name TEXT,
    state TEXT NOT NULL,
    source TEXT NOT NULL,
    case_type TEXT DEFAULT 'unknown',
    utility_type TEXT DEFAULT 'unknown',
    status TEXT DEFAULT 'unknown',
    filing_date TEXT,
    decision_date TEXT,
    effective_date TEXT,
    requested_revenue_change REAL,
    approved_revenue_change REAL,
    rate_base REAL,
    return_on_equity REAL,
    requested_rate_change_pct REAL,
    approved_rate_change_pct REAL,
    source_url TEXT,
    description TEXT,
    quality_score REAL,
    scraped_at TEXT,
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now')),
    UNIQUE(docket_number, source)
);

CREATE TABLE IF NOT EXISTS case_documents (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    docket_number TEXT NOT NULL,
    document_type TEXT DEFAULT 'other',
    title TEXT,
    filed_by TEXT,
    filing_date TEXT,
    url TEXT,
    source TEXT NOT NULL,
    state TEXT NOT NULL,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS pipeline_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL UNIQUE,
    source TEXT NOT NULL,
    stage TEXT NOT NULL,
    status TEXT NOT NULL,
    records_processed INTEGER DEFAULT 0,
    records_created INTEGER DEFAULT 0,
    records_updated INTEGER DEFAULT 0,
    errors INTEGER DEFAULT 0,
    started_at TEXT DEFAULT (datetime('now')),
    completed_at TEXT,
    notes TEXT
);

-- EIA Form 861 utility operational data
CREATE TABLE IF NOT EXISTS utility_operations (
    eia_utility_id INTEGER NOT NULL,
    year INTEGER NOT NULL,
    utility_name TEXT,
    state TEXT,
    ownership_type TEXT,
    residential_customers INTEGER,
    commercial_customers INTEGER,
    industrial_customers INTEGER,
    total_customers INTEGER,
    residential_revenue REAL,
    commercial_revenue REAL,
    industrial_revenue REAL,
    total_revenue REAL,
    residential_sales_mwh REAL,
    commercial_sales_mwh REAL,
    industrial_sales_mwh REAL,
    total_sales_mwh REAL,
    residential_avg_price REAL,
    commercial_avg_price REAL,
    industrial_avg_price REAL,
    avg_price REAL,
    revenue_per_customer REAL,
    quality_score REAL,
    created_at TEXT DEFAULT (datetime('now')),
    PRIMARY KEY (eia_utility_id, year)
);

-- Link PUC utilities to EIA utility IDs
CREATE TABLE IF NOT EXISTS utility_eia_links (
    utility_name TEXT NOT NULL,
    state TEXT NOT NULL,
    eia_utility_id INTEGER NOT NULL,
    match_confidence REAL,
    match_method TEXT,
    PRIMARY KEY (utility_name, state, eia_utility_id)
);

-- EPA eGRID utility emissions data
CREATE TABLE IF NOT EXISTS utility_emissions (
    utility_name_egrid TEXT NOT NULL,
    state TEXT NOT NULL,
    year INTEGER NOT NULL,
    eia_utility_id INTEGER,
    net_generation_mwh REAL,
    co2_tons REAL,
    nox_tons REAL,
    so2_tons REAL,
    co2_rate_lbs_mwh REAL,
    nox_rate_lbs_mwh REAL,
    so2_rate_lbs_mwh REAL,
    coal_pct REAL,
    gas_pct REAL,
    nuclear_pct REAL,
    hydro_pct REAL,
    wind_pct REAL,
    solar_pct REAL,
    other_renewable_pct REAL,
    quality_score REAL,
    created_at TEXT DEFAULT (datetime('now')),
    PRIMARY KEY (utility_name_egrid, state, year)
);

-- EIA Form 860 utility generation capacity
CREATE TABLE IF NOT EXISTS utility_capacity (
    eia_utility_id INTEGER NOT NULL,
    year INTEGER NOT NULL,
    coal_capacity_mw REAL,
    gas_capacity_mw REAL,
    nuclear_capacity_mw REAL,
    hydro_capacity_mw REAL,
    wind_capacity_mw REAL,
    solar_capacity_mw REAL,
    other_capacity_mw REAL,
    total_capacity_mw REAL,
    num_plants INTEGER,
    num_generators INTEGER,
    avg_generator_age REAL,
    planned_additions_mw REAL,
    planned_retirements_mw REAL,
    quality_score REAL,
    created_at TEXT DEFAULT (datetime('now')),
    PRIMARY KEY (eia_utility_id, year)
);

-- Rate case consumer impact estimates
CREATE TABLE IF NOT EXISTS rate_case_impacts (
    docket_number TEXT NOT NULL,
    source TEXT NOT NULL,
    eia_utility_id INTEGER,
    total_customers INTEGER,
    monthly_bill_impact REAL,
    annual_bill_impact REAL,
    pct_of_avg_bill REAL,
    residential_price_before REAL,
    residential_price_after REAL,
    created_at TEXT DEFAULT (datetime('now')),
    PRIMARY KEY (docket_number, source)
);

-- Indexes for enrichment tables
CREATE INDEX IF NOT EXISTS idx_operations_utility ON utility_operations(eia_utility_id);
CREATE INDEX IF NOT EXISTS idx_operations_year ON utility_operations(year);
CREATE INDEX IF NOT EXISTS idx_operations_state ON utility_operations(state);
CREATE INDEX IF NOT EXISTS idx_operations_ownership ON utility_operations(ownership_type);
CREATE INDEX IF NOT EXISTS idx_eia_links_utility ON utility_eia_links(utility_name, state);
CREATE INDEX IF NOT EXISTS idx_eia_links_eia ON utility_eia_links(eia_utility_id);
CREATE INDEX IF NOT EXISTS idx_emissions_state ON utility_emissions(state);
CREATE INDEX IF NOT EXISTS idx_emissions_co2 ON utility_emissions(co2_rate_lbs_mwh);
CREATE INDEX IF NOT EXISTS idx_emissions_eia ON utility_emissions(eia_utility_id);
CREATE INDEX IF NOT EXISTS idx_capacity_utility ON utility_capacity(eia_utility_id);
CREATE INDEX IF NOT EXISTS idx_capacity_year ON utility_capacity(year);
CREATE INDEX IF NOT EXISTS idx_impacts_docket ON rate_case_impacts(docket_number);

-- Indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_rate_cases_docket ON rate_cases(docket_number);
CREATE INDEX IF NOT EXISTS idx_rate_cases_state ON rate_cases(state);
CREATE INDEX IF NOT EXISTS idx_rate_cases_utility ON rate_cases(utility_name);
CREATE INDEX IF NOT EXISTS idx_rate_cases_canonical_utility ON rate_cases(canonical_utility_name);
CREATE INDEX IF NOT EXISTS idx_rate_cases_filing_date ON rate_cases(filing_date);
CREATE INDEX IF NOT EXISTS idx_rate_cases_decision_date ON rate_cases(decision_date);
CREATE INDEX IF NOT EXISTS idx_rate_cases_case_type ON rate_cases(case_type);
CREATE INDEX IF NOT EXISTS idx_rate_cases_status ON rate_cases(status);
CREATE INDEX IF NOT EXISTS idx_rate_cases_source ON rate_cases(source);
CREATE INDEX IF NOT EXISTS idx_rate_cases_quality ON rate_cases(quality_score);
CREATE INDEX IF NOT EXISTS idx_case_documents_docket ON case_documents(docket_number);
CREATE INDEX IF NOT EXISTS idx_case_documents_source ON case_documents(source);
CREATE INDEX IF NOT EXISTS idx_utilities_name ON utilities(name);
CREATE INDEX IF NOT EXISTS idx_utilities_canonical ON utilities(canonical_name);
CREATE INDEX IF NOT EXISTS idx_utilities_state ON utilities(state);
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_run_id ON pipeline_runs(run_id);
"""


# --- Connection Management ---


def get_connection(db_path: Optional[Path] = None) -> sqlite3.Connection:
    """Get a SQLite connection with WAL mode and row factory enabled.

    Args:
        db_path: Optional path to database file. Defaults to standard location.

    Returns:
        sqlite3.Connection configured with WAL mode and Row factory.
    """
    if db_path is None:
        db_path = DB_PATH

    # Ensure data directory exists
    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=5000")
    return conn


def init_db(conn: Optional[sqlite3.Connection] = None) -> sqlite3.Connection:
    """Initialize the database schema.

    Creates all tables and indexes if they don't exist.

    Args:
        conn: Optional existing connection. Creates one if not provided.

    Returns:
        The connection (either provided or newly created).
    """
    should_close = conn is None
    if conn is None:
        conn = get_connection()

    conn.executescript(SCHEMA_SQL)
    conn.commit()

    if not should_close:
        return conn

    return conn


# --- Rate Case CRUD ---


def upsert_rate_case(case_data: dict[str, Any], conn: Optional[sqlite3.Connection] = None) -> int:
    """Insert or update a rate case record.

    Uses docket_number + source as the unique key for upsert.

    Args:
        case_data: Dictionary of rate case fields.
        conn: Optional database connection.

    Returns:
        Row ID of the inserted/updated record.
    """
    should_close = conn is None
    if conn is None:
        conn = get_connection()

    now = datetime.now(timezone.utc).isoformat()

    # Fields that map to columns
    columns = [
        "docket_number", "utility_name", "canonical_utility_name", "state",
        "source", "case_type", "utility_type", "status",
        "filing_date", "decision_date", "effective_date",
        "requested_revenue_change", "approved_revenue_change",
        "rate_base", "return_on_equity",
        "requested_rate_change_pct", "approved_rate_change_pct",
        "source_url", "description", "quality_score", "scraped_at",
    ]

    # Build the data dict with only present keys
    data = {}
    for col in columns:
        if col in case_data and case_data[col] is not None:
            val = case_data[col]
            # Convert date objects to strings
            if hasattr(val, "isoformat"):
                val = val.isoformat()
            # Convert enums to their value
            if hasattr(val, "value"):
                val = val.value
            data[col] = val

    data["updated_at"] = now

    # Build upsert SQL
    cols = list(data.keys())
    placeholders = ["?" for _ in cols]
    update_clause = ", ".join(
        f"{c} = excluded.{c}" for c in cols if c not in ("docket_number", "source")
    )

    sql = f"""
        INSERT INTO rate_cases ({', '.join(cols)}, created_at)
        VALUES ({', '.join(placeholders)}, ?)
        ON CONFLICT(docket_number, source) DO UPDATE SET
        {update_clause}
    """

    values = [data[c] for c in cols] + [now]

    cursor = conn.execute(sql, values)
    conn.commit()

    row_id = cursor.lastrowid

    if should_close:
        conn.close()

    return row_id


def upsert_rate_cases_batch(
    cases: list[dict[str, Any]], conn: Optional[sqlite3.Connection] = None
) -> tuple[int, int]:
    """Batch upsert rate case records.

    Args:
        cases: List of rate case data dicts.
        conn: Optional database connection.

    Returns:
        Tuple of (created_count, updated_count).
    """
    should_close = conn is None
    if conn is None:
        conn = get_connection()

    created = 0
    updated = 0

    for case_data in cases:
        # Check if exists
        existing = conn.execute(
            "SELECT id FROM rate_cases WHERE docket_number = ? AND source = ?",
            (case_data.get("docket_number"), case_data.get("source")),
        ).fetchone()

        upsert_rate_case(case_data, conn=conn)

        if existing:
            updated += 1
        else:
            created += 1

    if should_close:
        conn.close()

    return created, updated


def insert_documents(
    documents: list[dict[str, Any]], conn: Optional[sqlite3.Connection] = None
) -> int:
    """Insert case documents.

    Args:
        documents: List of document data dicts.
        conn: Optional database connection.

    Returns:
        Number of documents inserted.
    """
    should_close = conn is None
    if conn is None:
        conn = get_connection()

    now = datetime.now(timezone.utc).isoformat()
    count = 0

    for doc in documents:
        # Convert date objects
        filing_date = doc.get("filing_date")
        if filing_date and hasattr(filing_date, "isoformat"):
            filing_date = filing_date.isoformat()

        # Convert enum values
        doc_type = doc.get("document_type", "other")
        if hasattr(doc_type, "value"):
            doc_type = doc_type.value

        try:
            conn.execute(
                """INSERT INTO case_documents
                   (docket_number, document_type, title, filed_by, filing_date, url, source, state, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    doc["docket_number"],
                    doc_type,
                    doc.get("title"),
                    doc.get("filed_by"),
                    filing_date,
                    doc.get("url"),
                    doc["source"],
                    doc["state"],
                    now,
                ),
            )
            count += 1
        except sqlite3.IntegrityError:
            pass  # Skip duplicates

    conn.commit()

    if should_close:
        conn.close()

    return count


def upsert_utility(
    utility_data: dict[str, Any], conn: Optional[sqlite3.Connection] = None
) -> int:
    """Insert or update a utility record.

    Args:
        utility_data: Dictionary of utility fields.
        conn: Optional database connection.

    Returns:
        Row ID of the inserted/updated record.
    """
    should_close = conn is None
    if conn is None:
        conn = get_connection()

    now = datetime.now(timezone.utc).isoformat()

    name = utility_data.get("name", "").strip()
    state = utility_data.get("state")

    # Convert enum values
    utility_type = utility_data.get("utility_type", "unknown")
    if hasattr(utility_type, "value"):
        utility_type = utility_type.value

    ownership_type = utility_data.get("ownership_type", "unknown")
    if hasattr(ownership_type, "value"):
        ownership_type = ownership_type.value

    sql = """
        INSERT INTO utilities (name, canonical_name, state, utility_type, ownership_type,
                               parent_company, customer_count, eia_utility_id, ferc_respondent_id,
                               created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(name, state) DO UPDATE SET
            canonical_name = COALESCE(excluded.canonical_name, utilities.canonical_name),
            utility_type = CASE WHEN excluded.utility_type != 'unknown' THEN excluded.utility_type ELSE utilities.utility_type END,
            ownership_type = CASE WHEN excluded.ownership_type != 'unknown' THEN excluded.ownership_type ELSE utilities.ownership_type END,
            parent_company = COALESCE(excluded.parent_company, utilities.parent_company),
            customer_count = COALESCE(excluded.customer_count, utilities.customer_count),
            eia_utility_id = COALESCE(excluded.eia_utility_id, utilities.eia_utility_id),
            ferc_respondent_id = COALESCE(excluded.ferc_respondent_id, utilities.ferc_respondent_id),
            updated_at = excluded.updated_at
    """

    cursor = conn.execute(
        sql,
        (
            name,
            utility_data.get("canonical_name"),
            state,
            utility_type,
            ownership_type,
            utility_data.get("parent_company"),
            utility_data.get("customer_count"),
            utility_data.get("eia_utility_id"),
            utility_data.get("ferc_respondent_id"),
            now,
            now,
        ),
    )
    conn.commit()

    row_id = cursor.lastrowid

    if should_close:
        conn.close()

    return row_id


# --- Pipeline Run Tracking ---


def start_pipeline_run(
    source: str, stage: str, conn: Optional[sqlite3.Connection] = None
) -> str:
    """Record the start of a pipeline run.

    Args:
        source: PUC source key.
        stage: Pipeline stage name.
        conn: Optional database connection.

    Returns:
        Unique run_id string.
    """
    should_close = conn is None
    if conn is None:
        conn = get_connection()

    run_id = str(uuid.uuid4())[:8]
    now = datetime.now(timezone.utc).isoformat()

    conn.execute(
        """INSERT INTO pipeline_runs (run_id, source, stage, status, started_at)
           VALUES (?, ?, ?, 'running', ?)""",
        (run_id, source, stage, now),
    )
    conn.commit()

    if should_close:
        conn.close()

    return run_id


def complete_pipeline_run(
    run_id: str,
    records_processed: int = 0,
    records_created: int = 0,
    records_updated: int = 0,
    errors: int = 0,
    status: str = "completed",
    notes: Optional[str] = None,
    conn: Optional[sqlite3.Connection] = None,
) -> None:
    """Record the completion of a pipeline run.

    Args:
        run_id: The run identifier from start_pipeline_run.
        records_processed: Total records processed.
        records_created: New records created.
        records_updated: Existing records updated.
        errors: Number of errors encountered.
        status: Final status (completed, failed).
        notes: Optional notes about the run.
        conn: Optional database connection.
    """
    should_close = conn is None
    if conn is None:
        conn = get_connection()

    now = datetime.now(timezone.utc).isoformat()

    conn.execute(
        """UPDATE pipeline_runs
           SET status = ?, records_processed = ?, records_created = ?,
               records_updated = ?, errors = ?, completed_at = ?, notes = ?
           WHERE run_id = ?""",
        (status, records_processed, records_created, records_updated, errors, now, notes, run_id),
    )
    conn.commit()

    if should_close:
        conn.close()


# --- Query Functions ---


def get_all_rate_cases(
    state: Optional[str] = None,
    utility_name: Optional[str] = None,
    case_type: Optional[str] = None,
    status: Optional[str] = None,
    min_quality: Optional[float] = None,
    limit: int = 1000,
    offset: int = 0,
    conn: Optional[sqlite3.Connection] = None,
) -> list[dict]:
    """Query rate cases with optional filters.

    Args:
        state: Filter by state code.
        utility_name: Filter by utility name (partial match).
        case_type: Filter by case type.
        status: Filter by case status.
        min_quality: Minimum quality score.
        limit: Maximum records to return.
        offset: Records to skip.
        conn: Optional database connection.

    Returns:
        List of rate case dictionaries.
    """
    should_close = conn is None
    if conn is None:
        conn = get_connection()

    conditions = []
    params = []

    if state:
        conditions.append("state = ?")
        params.append(state.upper())
    if utility_name:
        conditions.append("(utility_name LIKE ? OR canonical_utility_name LIKE ?)")
        params.extend([f"%{utility_name}%", f"%{utility_name}%"])
    if case_type:
        conditions.append("case_type = ?")
        params.append(case_type)
    if status:
        conditions.append("status = ?")
        params.append(status)
    if min_quality is not None:
        conditions.append("quality_score >= ?")
        params.append(min_quality)

    where = " WHERE " + " AND ".join(conditions) if conditions else ""

    sql = f"""
        SELECT * FROM rate_cases
        {where}
        ORDER BY filing_date DESC NULLS LAST
        LIMIT ? OFFSET ?
    """
    params.extend([limit, offset])

    rows = conn.execute(sql, params).fetchall()
    result = [dict(row) for row in rows]

    if should_close:
        conn.close()

    return result


def get_all_utilities(conn: Optional[sqlite3.Connection] = None) -> list[dict]:
    """Get all utilities."""
    should_close = conn is None
    if conn is None:
        conn = get_connection()

    rows = conn.execute("SELECT * FROM utilities ORDER BY name").fetchall()
    result = [dict(row) for row in rows]

    if should_close:
        conn.close()

    return result


def get_documents_for_docket(
    docket_number: str, conn: Optional[sqlite3.Connection] = None
) -> list[dict]:
    """Get all documents for a specific docket."""
    should_close = conn is None
    if conn is None:
        conn = get_connection()

    rows = conn.execute(
        "SELECT * FROM case_documents WHERE docket_number = ? ORDER BY filing_date",
        (docket_number,),
    ).fetchall()
    result = [dict(row) for row in rows]

    if should_close:
        conn.close()

    return result


def get_rate_case_by_docket(
    docket_number: str, source: Optional[str] = None, conn: Optional[sqlite3.Connection] = None
) -> Optional[dict]:
    """Get a single rate case by docket number."""
    should_close = conn is None
    if conn is None:
        conn = get_connection()

    if source:
        row = conn.execute(
            "SELECT * FROM rate_cases WHERE docket_number = ? AND source = ?",
            (docket_number, source),
        ).fetchone()
    else:
        row = conn.execute(
            "SELECT * FROM rate_cases WHERE docket_number = ?",
            (docket_number,),
        ).fetchone()

    result = dict(row) if row else None

    if should_close:
        conn.close()

    return result


def update_quality_scores(
    scores: dict[str, float], conn: Optional[sqlite3.Connection] = None
) -> int:
    """Batch update quality scores for rate cases.

    Args:
        scores: Dict mapping (docket_number, source) tuples or docket_number strings to scores.
        conn: Optional database connection.

    Returns:
        Number of records updated.
    """
    should_close = conn is None
    if conn is None:
        conn = get_connection()

    count = 0
    now = datetime.now(timezone.utc).isoformat()

    for key, score in scores.items():
        if isinstance(key, tuple):
            docket_number, source = key
            conn.execute(
                "UPDATE rate_cases SET quality_score = ?, updated_at = ? WHERE docket_number = ? AND source = ?",
                (score, now, docket_number, source),
            )
        else:
            conn.execute(
                "UPDATE rate_cases SET quality_score = ?, updated_at = ? WHERE docket_number = ?",
                (score, now, key),
            )
        count += 1

    conn.commit()

    if should_close:
        conn.close()

    return count


def update_canonical_names(
    mappings: dict[str, str], conn: Optional[sqlite3.Connection] = None
) -> int:
    """Update canonical utility names on rate cases.

    Args:
        mappings: Dict mapping utility_name to canonical_name.
        conn: Optional database connection.

    Returns:
        Number of records updated.
    """
    should_close = conn is None
    if conn is None:
        conn = get_connection()

    count = 0
    now = datetime.now(timezone.utc).isoformat()

    for raw_name, canonical in mappings.items():
        cursor = conn.execute(
            "UPDATE rate_cases SET canonical_utility_name = ?, updated_at = ? WHERE utility_name = ?",
            (canonical, now, raw_name),
        )
        count += cursor.rowcount

    conn.commit()

    if should_close:
        conn.close()

    return count


# --- Statistics ---


def get_stats(conn: Optional[sqlite3.Connection] = None, print_output: bool = True) -> dict:
    """Get summary statistics about the database.

    Args:
        conn: Optional database connection.
        print_output: Whether to print a formatted table.

    Returns:
        Dictionary of statistics.
    """
    should_close = conn is None
    if conn is None:
        conn = get_connection()

    stats = {}

    # Total rate cases
    row = conn.execute("SELECT COUNT(*) as cnt FROM rate_cases").fetchone()
    stats["total_rate_cases"] = row["cnt"]

    # Rate cases by state
    rows = conn.execute(
        "SELECT state, COUNT(*) as cnt FROM rate_cases GROUP BY state ORDER BY cnt DESC"
    ).fetchall()
    stats["by_state"] = {r["state"]: r["cnt"] for r in rows}

    # Rate cases by source
    rows = conn.execute(
        "SELECT source, COUNT(*) as cnt FROM rate_cases GROUP BY source ORDER BY cnt DESC"
    ).fetchall()
    stats["by_source"] = {r["source"]: r["cnt"] for r in rows}

    # Rate cases by case type
    rows = conn.execute(
        "SELECT case_type, COUNT(*) as cnt FROM rate_cases GROUP BY case_type ORDER BY cnt DESC"
    ).fetchall()
    stats["by_case_type"] = {r["case_type"]: r["cnt"] for r in rows}

    # Rate cases by status
    rows = conn.execute(
        "SELECT status, COUNT(*) as cnt FROM rate_cases GROUP BY status ORDER BY cnt DESC"
    ).fetchall()
    stats["by_status"] = {r["status"]: r["cnt"] for r in rows}

    # Financial summary
    row = conn.execute("""
        SELECT
            COUNT(*) as cases_with_request,
            SUM(requested_revenue_change) as total_requested,
            AVG(requested_revenue_change) as avg_requested,
            SUM(approved_revenue_change) as total_approved,
            AVG(approved_revenue_change) as avg_approved,
            AVG(return_on_equity) as avg_roe
        FROM rate_cases
        WHERE requested_revenue_change IS NOT NULL
    """).fetchone()
    stats["financial"] = {
        "cases_with_revenue_data": row["cases_with_request"],
        "total_requested_M": round(row["total_requested"] or 0, 1),
        "avg_requested_M": round(row["avg_requested"] or 0, 1),
        "total_approved_M": round(row["total_approved"] or 0, 1),
        "avg_approved_M": round(row["avg_approved"] or 0, 1),
        "avg_roe_pct": round(row["avg_roe"] or 0, 2),
    }

    # Quality stats
    row = conn.execute("""
        SELECT
            AVG(quality_score) as avg_quality,
            MIN(quality_score) as min_quality,
            MAX(quality_score) as max_quality,
            COUNT(CASE WHEN quality_score >= 0.6 THEN 1 END) as above_threshold
        FROM rate_cases
        WHERE quality_score IS NOT NULL
    """).fetchone()
    stats["quality"] = {
        "avg_score": round(row["avg_quality"] or 0, 3),
        "min_score": round(row["min_quality"] or 0, 3),
        "max_score": round(row["max_quality"] or 0, 3),
        "above_threshold": row["above_threshold"],
    }

    # Unique utilities
    row = conn.execute(
        "SELECT COUNT(DISTINCT utility_name) as cnt FROM rate_cases"
    ).fetchone()
    stats["unique_utilities"] = row["cnt"]

    # Total documents
    row = conn.execute("SELECT COUNT(*) as cnt FROM case_documents").fetchone()
    stats["total_documents"] = row["cnt"]

    # Date range
    row = conn.execute("""
        SELECT MIN(filing_date) as earliest, MAX(filing_date) as latest
        FROM rate_cases WHERE filing_date IS NOT NULL
    """).fetchone()
    stats["date_range"] = {
        "earliest_filing": row["earliest"],
        "latest_filing": row["latest"],
    }

    if print_output:
        _print_stats(stats)

    if should_close:
        conn.close()

    return stats


def _print_stats(stats: dict) -> None:
    """Print formatted statistics table."""
    table = Table(title="PUC Rate Case Tracker - Database Statistics")
    table.add_column("Metric", style="cyan")
    table.add_column("Value", style="green")

    table.add_row("Total Rate Cases", str(stats["total_rate_cases"]))
    table.add_row("Unique Utilities", str(stats["unique_utilities"]))
    table.add_row("Total Documents", str(stats["total_documents"]))

    if stats["date_range"]["earliest_filing"]:
        table.add_row(
            "Date Range",
            f"{stats['date_range']['earliest_filing']} to {stats['date_range']['latest_filing']}",
        )

    table.add_row("", "")
    table.add_row("[bold]By State[/bold]", "")
    for state, count in stats["by_state"].items():
        table.add_row(f"  {state}", str(count))

    table.add_row("", "")
    table.add_row("[bold]Financial Summary[/bold]", "")
    fin = stats["financial"]
    table.add_row("  Cases with Revenue Data", str(fin["cases_with_revenue_data"]))
    table.add_row("  Total Requested ($M)", f"${fin['total_requested_M']:,.1f}M")
    table.add_row("  Total Approved ($M)", f"${fin['total_approved_M']:,.1f}M")
    table.add_row("  Avg ROE", f"{fin['avg_roe_pct']:.2f}%")

    table.add_row("", "")
    table.add_row("[bold]Quality[/bold]", "")
    qual = stats["quality"]
    table.add_row("  Average Score", f"{qual['avg_score']:.3f}")
    table.add_row("  Above Threshold (>=0.6)", str(qual["above_threshold"]))

    console.print(table)


# --- Store Records (used by pipeline) ---


def store_records(
    source_key: str, records: list[dict], conn: Optional[sqlite3.Connection] = None
) -> tuple[int, int]:
    """Store extracted rate case records into the database.

    Args:
        source_key: PUC source identifier.
        records: List of extracted rate case dicts.
        conn: Optional database connection.

    Returns:
        Tuple of (created, updated) counts.
    """
    should_close = conn is None
    if conn is None:
        conn = get_connection()

    run_id = start_pipeline_run(source_key, "store", conn=conn)

    created, updated = 0, 0
    errors = 0

    for record in records:
        try:
            record["source"] = source_key
            existing = conn.execute(
                "SELECT id FROM rate_cases WHERE docket_number = ? AND source = ?",
                (record.get("docket_number"), source_key),
            ).fetchone()

            upsert_rate_case(record, conn=conn)

            if existing:
                updated += 1
            else:
                created += 1
        except Exception as e:
            errors += 1
            console.print(f"[red]Error storing record: {e}[/red]")

    complete_pipeline_run(
        run_id,
        records_processed=len(records),
        records_created=created,
        records_updated=updated,
        errors=errors,
        conn=conn,
    )

    console.print(
        f"[green]Stored {created} new, {updated} updated from {source_key} "
        f"({errors} errors)[/green]"
    )

    if should_close:
        conn.close()

    return created, updated


# --- EIA Operations CRUD ---


def upsert_utility_operations_batch(
    records: list[dict[str, Any]], conn: Optional[sqlite3.Connection] = None
) -> tuple[int, int]:
    """Batch upsert EIA utility operations records.

    Args:
        records: List of utility operations dicts.
        conn: Optional database connection.

    Returns:
        Tuple of (created, updated) counts.
    """
    should_close = conn is None
    if conn is None:
        conn = get_connection()

    created, updated = 0, 0

    for rec in records:
        existing = conn.execute(
            "SELECT 1 FROM utility_operations WHERE eia_utility_id = ? AND year = ?",
            (rec["eia_utility_id"], rec["year"]),
        ).fetchone()

        cols = [
            "eia_utility_id", "year", "utility_name", "state", "ownership_type",
            "residential_customers", "commercial_customers", "industrial_customers",
            "total_customers", "residential_revenue", "commercial_revenue",
            "industrial_revenue", "total_revenue", "residential_sales_mwh",
            "commercial_sales_mwh", "industrial_sales_mwh", "total_sales_mwh",
            "residential_avg_price", "commercial_avg_price", "industrial_avg_price",
            "avg_price", "revenue_per_customer", "quality_score",
        ]

        data = {c: rec.get(c) for c in cols if rec.get(c) is not None}
        col_names = list(data.keys())
        placeholders = ", ".join(["?"] * len(col_names))
        update_clause = ", ".join(
            f"{c} = excluded.{c}" for c in col_names
            if c not in ("eia_utility_id", "year")
        )

        sql = f"""
            INSERT INTO utility_operations ({', '.join(col_names)})
            VALUES ({placeholders})
            ON CONFLICT(eia_utility_id, year) DO UPDATE SET {update_clause}
        """
        conn.execute(sql, [data[c] for c in col_names])

        if existing:
            updated += 1
        else:
            created += 1

    conn.commit()
    if should_close:
        conn.close()

    return created, updated


def upsert_utility_eia_links_batch(
    links: list[dict[str, Any]], conn: Optional[sqlite3.Connection] = None
) -> int:
    """Batch upsert utility-EIA linkage records.

    Args:
        links: List of link dicts with utility_name, state, eia_utility_id.
        conn: Optional database connection.

    Returns:
        Number of links stored.
    """
    should_close = conn is None
    if conn is None:
        conn = get_connection()

    count = 0
    for link in links:
        conn.execute(
            """INSERT INTO utility_eia_links
               (utility_name, state, eia_utility_id, match_confidence, match_method)
               VALUES (?, ?, ?, ?, ?)
               ON CONFLICT(utility_name, state, eia_utility_id) DO UPDATE SET
               match_confidence = excluded.match_confidence,
               match_method = excluded.match_method""",
            (
                link["utility_name"],
                link["state"],
                link["eia_utility_id"],
                link.get("match_confidence"),
                link.get("match_method"),
            ),
        )
        count += 1

    conn.commit()
    if should_close:
        conn.close()

    return count


def upsert_utility_emissions_batch(
    records: list[dict[str, Any]], conn: Optional[sqlite3.Connection] = None
) -> tuple[int, int]:
    """Batch upsert eGRID emissions records.

    Args:
        records: List of emissions dicts.
        conn: Optional database connection.

    Returns:
        Tuple of (created, updated) counts.
    """
    should_close = conn is None
    if conn is None:
        conn = get_connection()

    created, updated = 0, 0

    for rec in records:
        existing = conn.execute(
            "SELECT 1 FROM utility_emissions WHERE utility_name_egrid = ? AND state = ? AND year = ?",
            (rec["utility_name_egrid"], rec["state"], rec["year"]),
        ).fetchone()

        cols = [
            "utility_name_egrid", "state", "year", "eia_utility_id",
            "net_generation_mwh", "co2_tons", "nox_tons", "so2_tons",
            "co2_rate_lbs_mwh", "nox_rate_lbs_mwh", "so2_rate_lbs_mwh",
            "coal_pct", "gas_pct", "nuclear_pct", "hydro_pct",
            "wind_pct", "solar_pct", "other_renewable_pct", "quality_score",
        ]

        data = {c: rec.get(c) for c in cols if rec.get(c) is not None}
        col_names = list(data.keys())
        placeholders = ", ".join(["?"] * len(col_names))
        update_clause = ", ".join(
            f"{c} = excluded.{c}" for c in col_names
            if c not in ("utility_name_egrid", "state", "year")
        )

        sql = f"""
            INSERT INTO utility_emissions ({', '.join(col_names)})
            VALUES ({placeholders})
            ON CONFLICT(utility_name_egrid, state, year) DO UPDATE SET {update_clause}
        """
        conn.execute(sql, [data[c] for c in col_names])

        if existing:
            updated += 1
        else:
            created += 1

    conn.commit()
    if should_close:
        conn.close()

    return created, updated


def upsert_utility_capacity_batch(
    records: list[dict[str, Any]], conn: Optional[sqlite3.Connection] = None
) -> tuple[int, int]:
    """Batch upsert EIA 860 capacity records.

    Args:
        records: List of capacity dicts.
        conn: Optional database connection.

    Returns:
        Tuple of (created, updated) counts.
    """
    should_close = conn is None
    if conn is None:
        conn = get_connection()

    created, updated = 0, 0

    for rec in records:
        existing = conn.execute(
            "SELECT 1 FROM utility_capacity WHERE eia_utility_id = ? AND year = ?",
            (rec["eia_utility_id"], rec["year"]),
        ).fetchone()

        cols = [
            "eia_utility_id", "year", "coal_capacity_mw", "gas_capacity_mw",
            "nuclear_capacity_mw", "hydro_capacity_mw", "wind_capacity_mw",
            "solar_capacity_mw", "other_capacity_mw", "total_capacity_mw",
            "num_plants", "num_generators", "avg_generator_age",
            "planned_additions_mw", "planned_retirements_mw", "quality_score",
        ]

        data = {c: rec.get(c) for c in cols if rec.get(c) is not None}
        col_names = list(data.keys())
        placeholders = ", ".join(["?"] * len(col_names))
        update_clause = ", ".join(
            f"{c} = excluded.{c}" for c in col_names
            if c not in ("eia_utility_id", "year")
        )

        sql = f"""
            INSERT INTO utility_capacity ({', '.join(col_names)})
            VALUES ({placeholders})
            ON CONFLICT(eia_utility_id, year) DO UPDATE SET {update_clause}
        """
        conn.execute(sql, [data[c] for c in col_names])

        if existing:
            updated += 1
        else:
            created += 1

    conn.commit()
    if should_close:
        conn.close()

    return created, updated


def upsert_rate_case_impacts_batch(
    impacts: list[dict[str, Any]], conn: Optional[sqlite3.Connection] = None
) -> int:
    """Batch upsert rate case consumer impact records.

    Args:
        impacts: List of impact dicts.
        conn: Optional database connection.

    Returns:
        Number of impacts stored.
    """
    should_close = conn is None
    if conn is None:
        conn = get_connection()

    count = 0
    for imp in impacts:
        conn.execute(
            """INSERT INTO rate_case_impacts
               (docket_number, source, eia_utility_id, total_customers,
                monthly_bill_impact, annual_bill_impact, pct_of_avg_bill,
                residential_price_before, residential_price_after)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(docket_number, source) DO UPDATE SET
               eia_utility_id = excluded.eia_utility_id,
               total_customers = excluded.total_customers,
               monthly_bill_impact = excluded.monthly_bill_impact,
               annual_bill_impact = excluded.annual_bill_impact,
               pct_of_avg_bill = excluded.pct_of_avg_bill,
               residential_price_before = excluded.residential_price_before,
               residential_price_after = excluded.residential_price_after""",
            (
                imp["docket_number"],
                imp["source"],
                imp.get("eia_utility_id"),
                imp.get("total_customers"),
                imp.get("monthly_bill_impact"),
                imp.get("annual_bill_impact"),
                imp.get("pct_of_avg_bill"),
                imp.get("residential_price_before"),
                imp.get("residential_price_after"),
            ),
        )
        count += 1

    conn.commit()
    if should_close:
        conn.close()

    return count


# --- Enrichment Query Functions ---


def get_utility_operations(
    eia_utility_id: Optional[int] = None,
    state: Optional[str] = None,
    year: Optional[int] = None,
    conn: Optional[sqlite3.Connection] = None,
) -> list[dict]:
    """Query utility operations data."""
    should_close = conn is None
    if conn is None:
        conn = get_connection()

    conditions, params = [], []
    if eia_utility_id:
        conditions.append("eia_utility_id = ?")
        params.append(eia_utility_id)
    if state:
        conditions.append("state = ?")
        params.append(state.upper())
    if year:
        conditions.append("year = ?")
        params.append(year)

    where = " WHERE " + " AND ".join(conditions) if conditions else ""
    rows = conn.execute(
        f"SELECT * FROM utility_operations{where} ORDER BY year DESC", params
    ).fetchall()
    result = [dict(r) for r in rows]

    if should_close:
        conn.close()
    return result


def get_utility_eia_links(conn: Optional[sqlite3.Connection] = None) -> list[dict]:
    """Get all utility-EIA linkage records."""
    should_close = conn is None
    if conn is None:
        conn = get_connection()

    rows = conn.execute("SELECT * FROM utility_eia_links").fetchall()
    result = [dict(r) for r in rows]

    if should_close:
        conn.close()
    return result


def get_utility_emissions(
    state: Optional[str] = None,
    year: Optional[int] = None,
    conn: Optional[sqlite3.Connection] = None,
) -> list[dict]:
    """Query utility emissions data."""
    should_close = conn is None
    if conn is None:
        conn = get_connection()

    conditions, params = [], []
    if state:
        conditions.append("state = ?")
        params.append(state.upper())
    if year:
        conditions.append("year = ?")
        params.append(year)

    where = " WHERE " + " AND ".join(conditions) if conditions else ""
    rows = conn.execute(
        f"SELECT * FROM utility_emissions{where} ORDER BY co2_tons DESC", params
    ).fetchall()
    result = [dict(r) for r in rows]

    if should_close:
        conn.close()
    return result


def get_utility_capacity(
    eia_utility_id: Optional[int] = None,
    year: Optional[int] = None,
    conn: Optional[sqlite3.Connection] = None,
) -> list[dict]:
    """Query utility capacity data."""
    should_close = conn is None
    if conn is None:
        conn = get_connection()

    conditions, params = [], []
    if eia_utility_id:
        conditions.append("eia_utility_id = ?")
        params.append(eia_utility_id)
    if year:
        conditions.append("year = ?")
        params.append(year)

    where = " WHERE " + " AND ".join(conditions) if conditions else ""
    rows = conn.execute(
        f"SELECT * FROM utility_capacity{where} ORDER BY total_capacity_mw DESC", params
    ).fetchall()
    result = [dict(r) for r in rows]

    if should_close:
        conn.close()
    return result


def get_rate_case_impacts(conn: Optional[sqlite3.Connection] = None) -> list[dict]:
    """Get all rate case consumer impact records."""
    should_close = conn is None
    if conn is None:
        conn = get_connection()

    rows = conn.execute(
        "SELECT * FROM rate_case_impacts ORDER BY abs(monthly_bill_impact) DESC"
    ).fetchall()
    result = [dict(r) for r in rows]

    if should_close:
        conn.close()
    return result


def get_enrichment_stats(conn: Optional[sqlite3.Connection] = None) -> dict:
    """Get statistics about enrichment data."""
    should_close = conn is None
    if conn is None:
        conn = get_connection()

    stats = {}

    row = conn.execute("SELECT COUNT(*) as cnt FROM utility_operations").fetchone()
    stats["utility_operations"] = row["cnt"]

    row = conn.execute("SELECT COUNT(*) as cnt FROM utility_eia_links").fetchone()
    stats["eia_links"] = row["cnt"]

    row = conn.execute("SELECT COUNT(*) as cnt FROM utility_emissions").fetchone()
    stats["emissions_records"] = row["cnt"]

    row = conn.execute("SELECT COUNT(*) as cnt FROM utility_capacity").fetchone()
    stats["capacity_records"] = row["cnt"]

    row = conn.execute("SELECT COUNT(*) as cnt FROM rate_case_impacts").fetchone()
    stats["impact_records"] = row["cnt"]

    row = conn.execute(
        "SELECT COUNT(DISTINCT eia_utility_id) as cnt FROM utility_operations"
    ).fetchone()
    stats["unique_eia_utilities"] = row["cnt"]

    row = conn.execute(
        "SELECT COUNT(DISTINCT eia_utility_id) as cnt FROM utility_eia_links"
    ).fetchone()
    stats["linked_utilities"] = row["cnt"]

    if should_close:
        conn.close()

    return stats
