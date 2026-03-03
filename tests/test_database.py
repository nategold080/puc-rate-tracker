"""Tests for SQLite database operations."""

import pytest
import sqlite3
import tempfile
from datetime import date, datetime
from pathlib import Path

from src.storage.database import (
    complete_pipeline_run,
    get_all_rate_cases,
    get_all_utilities,
    get_connection,
    get_documents_for_docket,
    get_rate_case_by_docket,
    get_stats,
    init_db,
    insert_documents,
    start_pipeline_run,
    store_records,
    update_canonical_names,
    update_quality_scores,
    upsert_rate_case,
    upsert_rate_cases_batch,
    upsert_utility,
)


@pytest.fixture
def db_conn(tmp_path):
    """Create a temporary database connection for testing."""
    db_path = tmp_path / "test.db"
    conn = get_connection(db_path)
    init_db(conn)
    yield conn
    conn.close()


@pytest.fixture
def sample_case():
    return {
        "docket_number": "R-2024-3046894",
        "utility_name": "PECO Energy Company",
        "state": "PA",
        "source": "pennsylvania_puc",
        "case_type": "general_rate_case",
        "utility_type": "electric",
        "status": "active",
        "filing_date": "2024-03-28",
        "requested_revenue_change": 245.6,
        "rate_base": 8750.0,
        "return_on_equity": 10.95,
    }


class TestInitDb:
    def test_creates_tables(self, db_conn):
        tables = db_conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        ).fetchall()
        table_names = [t["name"] for t in tables]
        assert "rate_cases" in table_names
        assert "utilities" in table_names
        assert "case_documents" in table_names
        assert "pipeline_runs" in table_names

    def test_wal_mode(self, db_conn):
        result = db_conn.execute("PRAGMA journal_mode").fetchone()
        assert result[0] == "wal"

    def test_creates_indexes(self, db_conn):
        indexes = db_conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index' AND name LIKE 'idx_%'"
        ).fetchall()
        index_names = [i["name"] for i in indexes]
        assert "idx_rate_cases_docket" in index_names
        assert "idx_rate_cases_state" in index_names
        assert "idx_rate_cases_utility" in index_names
        assert "idx_rate_cases_filing_date" in index_names

    def test_idempotent(self, db_conn):
        # Should not error on second call
        init_db(db_conn)


class TestUpsertRateCase:
    def test_insert_new(self, db_conn, sample_case):
        row_id = upsert_rate_case(sample_case, conn=db_conn)
        assert row_id > 0

    def test_retrieve_after_insert(self, db_conn, sample_case):
        upsert_rate_case(sample_case, conn=db_conn)
        result = get_rate_case_by_docket("R-2024-3046894", source="pennsylvania_puc", conn=db_conn)
        assert result is not None
        assert result["utility_name"] == "PECO Energy Company"
        assert result["state"] == "PA"
        assert result["requested_revenue_change"] == 245.6

    def test_upsert_updates_existing(self, db_conn, sample_case):
        upsert_rate_case(sample_case, conn=db_conn)

        # Update the case
        updated = dict(sample_case)
        updated["status"] = "decided"
        updated["approved_revenue_change"] = 175.0
        upsert_rate_case(updated, conn=db_conn)

        result = get_rate_case_by_docket("R-2024-3046894", source="pennsylvania_puc", conn=db_conn)
        assert result["status"] == "decided"
        assert result["approved_revenue_change"] == 175.0

    def test_different_sources_separate(self, db_conn, sample_case):
        upsert_rate_case(sample_case, conn=db_conn)

        other_source = dict(sample_case)
        other_source["source"] = "other_source"
        upsert_rate_case(other_source, conn=db_conn)

        cases = get_all_rate_cases(conn=db_conn)
        assert len(cases) == 2


class TestBatchUpsert:
    def test_batch_insert(self, db_conn):
        cases = [
            {"docket_number": f"R-2024-{i}", "utility_name": f"Utility {i}",
             "state": "PA", "source": "test"}
            for i in range(5)
        ]
        created, updated = upsert_rate_cases_batch(cases, conn=db_conn)
        assert created == 5
        assert updated == 0

    def test_batch_upsert_mixed(self, db_conn):
        cases = [
            {"docket_number": "R-2024-001", "utility_name": "Utility 1",
             "state": "PA", "source": "test"}
        ]
        upsert_rate_cases_batch(cases, conn=db_conn)

        cases2 = [
            {"docket_number": "R-2024-001", "utility_name": "Utility 1 Updated",
             "state": "PA", "source": "test"},
            {"docket_number": "R-2024-002", "utility_name": "Utility 2",
             "state": "PA", "source": "test"},
        ]
        created, updated = upsert_rate_cases_batch(cases2, conn=db_conn)
        assert created == 1
        assert updated == 1


class TestInsertDocuments:
    def test_insert_documents(self, db_conn, sample_case):
        upsert_rate_case(sample_case, conn=db_conn)
        docs = [
            {"docket_number": "R-2024-3046894", "document_type": "application",
             "title": "Initial Filing", "source": "pennsylvania_puc", "state": "PA"},
            {"docket_number": "R-2024-3046894", "document_type": "order",
             "title": "Final Order", "source": "pennsylvania_puc", "state": "PA"},
        ]
        count = insert_documents(docs, conn=db_conn)
        assert count == 2

    def test_retrieve_documents(self, db_conn, sample_case):
        upsert_rate_case(sample_case, conn=db_conn)
        docs = [
            {"docket_number": "R-2024-3046894", "document_type": "order",
             "title": "Final Order", "source": "pennsylvania_puc", "state": "PA"},
        ]
        insert_documents(docs, conn=db_conn)
        retrieved = get_documents_for_docket("R-2024-3046894", conn=db_conn)
        assert len(retrieved) == 1
        assert retrieved[0]["title"] == "Final Order"


class TestUpsertUtility:
    def test_insert_utility(self, db_conn):
        row_id = upsert_utility(
            {"name": "PECO Energy Company", "state": "PA", "utility_type": "electric"},
            conn=db_conn,
        )
        assert row_id > 0

    def test_retrieve_utilities(self, db_conn):
        upsert_utility(
            {"name": "PECO Energy Company", "state": "PA", "utility_type": "electric"},
            conn=db_conn,
        )
        utils = get_all_utilities(conn=db_conn)
        assert len(utils) == 1
        assert utils[0]["name"] == "PECO Energy Company"

    def test_upsert_utility_updates(self, db_conn):
        upsert_utility(
            {"name": "PECO", "state": "PA", "utility_type": "unknown"},
            conn=db_conn,
        )
        upsert_utility(
            {"name": "PECO", "state": "PA", "utility_type": "electric",
             "parent_company": "Exelon"},
            conn=db_conn,
        )
        utils = get_all_utilities(conn=db_conn)
        assert len(utils) == 1
        assert utils[0]["utility_type"] == "electric"
        assert utils[0]["parent_company"] == "Exelon"


class TestQueryFunctions:
    @pytest.fixture(autouse=True)
    def setup_data(self, db_conn):
        cases = [
            {"docket_number": "R-2024-001", "utility_name": "PECO",
             "state": "PA", "source": "pennsylvania_puc", "case_type": "general_rate_case",
             "status": "active", "filing_date": "2024-03-01",
             "quality_score": 0.8, "requested_revenue_change": 100.0},
            {"docket_number": "A.23-05-010", "utility_name": "PG&E",
             "state": "CA", "source": "california_cpuc", "case_type": "general_rate_case",
             "status": "decided", "filing_date": "2023-05-01",
             "quality_score": 0.9, "requested_revenue_change": 3586.0},
            {"docket_number": "UE-220066", "utility_name": "PSE",
             "state": "WA", "source": "washington_utc", "case_type": "general_rate_case",
             "status": "decided", "filing_date": "2022-01-01",
             "quality_score": 0.5},
        ]
        for case in cases:
            upsert_rate_case(case, conn=db_conn)
        self.conn = db_conn

    def test_filter_by_state(self):
        cases = get_all_rate_cases(state="PA", conn=self.conn)
        assert len(cases) == 1
        assert cases[0]["state"] == "PA"

    def test_filter_by_status(self):
        cases = get_all_rate_cases(status="decided", conn=self.conn)
        assert len(cases) == 2

    def test_filter_by_min_quality(self):
        cases = get_all_rate_cases(min_quality=0.7, conn=self.conn)
        assert len(cases) == 2

    def test_filter_by_utility_name(self):
        cases = get_all_rate_cases(utility_name="PECO", conn=self.conn)
        assert len(cases) == 1

    def test_get_all_no_filter(self):
        cases = get_all_rate_cases(conn=self.conn)
        assert len(cases) == 3


class TestUpdateScores:
    def test_update_quality_scores(self, db_conn, sample_case):
        upsert_rate_case(sample_case, conn=db_conn)
        scores = {("R-2024-3046894", "pennsylvania_puc"): 0.85}
        count = update_quality_scores(scores, conn=db_conn)
        assert count == 1

        result = get_rate_case_by_docket("R-2024-3046894", source="pennsylvania_puc", conn=db_conn)
        assert result["quality_score"] == 0.85

    def test_update_canonical_names(self, db_conn, sample_case):
        upsert_rate_case(sample_case, conn=db_conn)
        mappings = {"PECO Energy Company": "PECO Energy Co."}
        count = update_canonical_names(mappings, conn=db_conn)
        assert count == 1

        result = get_rate_case_by_docket("R-2024-3046894", source="pennsylvania_puc", conn=db_conn)
        assert result["canonical_utility_name"] == "PECO Energy Co."


class TestPipelineRuns:
    def test_start_and_complete(self, db_conn):
        run_id = start_pipeline_run("test", "scrape", conn=db_conn)
        assert run_id

        complete_pipeline_run(
            run_id, records_processed=10, records_created=8,
            records_updated=2, conn=db_conn,
        )

        row = db_conn.execute(
            "SELECT * FROM pipeline_runs WHERE run_id = ?", (run_id,)
        ).fetchone()
        assert row["status"] == "completed"
        assert row["records_processed"] == 10


class TestGetStats:
    def test_empty_database(self, db_conn):
        stats = get_stats(conn=db_conn, print_output=False)
        assert stats["total_rate_cases"] == 0

    def test_with_data(self, db_conn, sample_case):
        upsert_rate_case(sample_case, conn=db_conn)
        stats = get_stats(conn=db_conn, print_output=False)
        assert stats["total_rate_cases"] == 1
        assert "PA" in stats["by_state"]


class TestStoreRecords:
    def test_store_records(self, db_conn):
        records = [
            {"docket_number": "R-2024-001", "utility_name": "Test",
             "state": "PA", "source": "test"},
        ]
        created, updated = store_records("test", records, conn=db_conn)
        assert created == 1
        assert updated == 0
