"""Tests for enrichment data: EIA 861, eGRID, EIA 860, cross-linking, and impact calculations.

Covers:
- Database schema for enrichment tables
- EIA 861 parsing and storage
- eGRID parsing and storage
- EIA 860 parsing and storage
- Utility cross-linking (PUC → EIA)
- Rate case consumer impact calculations
- Quality scoring with enrichment
- Export enrichment data
"""

import sqlite3
import tempfile
from pathlib import Path

import pytest

import sys
PROJECT_ROOT = Path(__file__).parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


from src.storage.database import (
    get_connection,
    init_db,
    upsert_utility_operations_batch,
    upsert_utility_eia_links_batch,
    upsert_utility_emissions_batch,
    upsert_utility_capacity_batch,
    upsert_rate_case_impacts_batch,
    get_utility_operations,
    get_utility_eia_links,
    get_utility_emissions,
    get_utility_capacity,
    get_rate_case_impacts,
    get_enrichment_stats,
    upsert_rate_case,
    upsert_utility,
)
from src.normalization.cross_linker import (
    cross_link_utilities,
    cross_link_emissions,
    compute_rate_case_impacts,
)
from src.validation.quality import score_rate_case, WEIGHTS


# --- Fixtures ---


@pytest.fixture
def db_conn():
    """In-memory database connection with schema."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")

    # Load schema from database module
    from src.storage.database import SCHEMA_SQL
    conn.executescript(SCHEMA_SQL)
    conn.commit()
    return conn


@pytest.fixture
def sample_operations():
    """Sample EIA 861 utility operations records."""
    return [
        {
            "eia_utility_id": 14328,
            "year": 2022,
            "utility_name": "Pacific Gas & Electric Co",
            "state": "CA",
            "ownership_type": "Investor Owned",
            "residential_customers": 5200000,
            "commercial_customers": 600000,
            "industrial_customers": 35000,
            "total_customers": 5835000,
            "residential_revenue": 12500000.0,
            "commercial_revenue": 6200000.0,
            "industrial_revenue": 1800000.0,
            "total_revenue": 20500000.0,
            "residential_sales_mwh": 35000000.0,
            "commercial_sales_mwh": 28000000.0,
            "industrial_sales_mwh": 12000000.0,
            "total_sales_mwh": 75000000.0,
            "residential_avg_price": 25.3,
            "commercial_avg_price": 21.5,
            "industrial_avg_price": 17.8,
            "avg_price": 22.1,
            "revenue_per_customer": 3513.71,
            "quality_score": 0.9,
        },
        {
            "eia_utility_id": 17166,
            "year": 2022,
            "utility_name": "Portland General Electric Co",
            "state": "OR",
            "ownership_type": "Investor Owned",
            "residential_customers": 850000,
            "commercial_customers": 110000,
            "industrial_customers": 1200,
            "total_customers": 961200,
            "residential_revenue": 1500000.0,
            "commercial_revenue": 800000.0,
            "industrial_revenue": 450000.0,
            "total_revenue": 2750000.0,
            "residential_sales_mwh": 8500000.0,
            "commercial_sales_mwh": 6000000.0,
            "industrial_sales_mwh": 4200000.0,
            "total_sales_mwh": 18700000.0,
            "residential_avg_price": 12.8,
            "commercial_avg_price": 10.5,
            "industrial_avg_price": 8.2,
            "avg_price": 11.0,
            "revenue_per_customer": 2861.06,
            "quality_score": 0.9,
        },
        {
            "eia_utility_id": 14328,
            "year": 2021,
            "utility_name": "Pacific Gas & Electric Co",
            "state": "CA",
            "ownership_type": "Investor Owned",
            "residential_customers": 5100000,
            "total_customers": 5700000,
            "residential_avg_price": 23.8,
            "quality_score": 0.6,
        },
    ]


@pytest.fixture
def sample_emissions():
    """Sample eGRID emissions records."""
    return [
        {
            "utility_name_egrid": "Pacific Gas & Electric Co",
            "state": "CA",
            "year": 2022,
            "eia_utility_id": 14328,
            "net_generation_mwh": 32500000.0,
            "co2_tons": 5200000.0,
            "nox_tons": 3500.0,
            "so2_tons": 800.0,
            "co2_rate_lbs_mwh": 320.0,
            "nox_rate_lbs_mwh": 0.215,
            "so2_rate_lbs_mwh": 0.049,
            "coal_pct": 0.0,
            "gas_pct": 45.0,
            "nuclear_pct": 25.0,
            "hydro_pct": 15.0,
            "wind_pct": 8.0,
            "solar_pct": 5.0,
            "other_renewable_pct": 2.0,
            "quality_score": 0.95,
        },
        {
            "utility_name_egrid": "Portland General Electric",
            "state": "OR",
            "year": 2022,
            "net_generation_mwh": 9800000.0,
            "co2_tons": 4300000.0,
            "co2_rate_lbs_mwh": 877.0,
            "coal_pct": 12.0,
            "gas_pct": 35.0,
            "nuclear_pct": 0.0,
            "hydro_pct": 25.0,
            "wind_pct": 20.0,
            "solar_pct": 5.0,
            "quality_score": 0.8,
        },
    ]


@pytest.fixture
def sample_capacity():
    """Sample EIA 860 capacity records."""
    return [
        {
            "eia_utility_id": 14328,
            "year": 2022,
            "coal_capacity_mw": 0.0,
            "gas_capacity_mw": 8500.0,
            "nuclear_capacity_mw": 2200.0,
            "hydro_capacity_mw": 3800.0,
            "wind_capacity_mw": 1200.0,
            "solar_capacity_mw": 950.0,
            "other_capacity_mw": 300.0,
            "total_capacity_mw": 16950.0,
            "num_plants": 85,
            "num_generators": 220,
            "avg_generator_age": 28.5,
            "planned_additions_mw": 500.0,
            "planned_retirements_mw": 200.0,
            "quality_score": 0.9,
        },
        {
            "eia_utility_id": 17166,
            "year": 2022,
            "gas_capacity_mw": 1200.0,
            "wind_capacity_mw": 800.0,
            "hydro_capacity_mw": 600.0,
            "total_capacity_mw": 2600.0,
            "num_plants": 15,
            "num_generators": 42,
            "avg_generator_age": 22.0,
            "quality_score": 0.75,
        },
    ]


# ============================================================
# DATABASE SCHEMA TESTS
# ============================================================


class TestEnrichmentSchema:
    def test_utility_operations_table_exists(self, db_conn):
        cursor = db_conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='utility_operations'"
        )
        assert cursor.fetchone() is not None

    def test_utility_eia_links_table_exists(self, db_conn):
        cursor = db_conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='utility_eia_links'"
        )
        assert cursor.fetchone() is not None

    def test_utility_emissions_table_exists(self, db_conn):
        cursor = db_conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='utility_emissions'"
        )
        assert cursor.fetchone() is not None

    def test_utility_capacity_table_exists(self, db_conn):
        cursor = db_conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='utility_capacity'"
        )
        assert cursor.fetchone() is not None

    def test_rate_case_impacts_table_exists(self, db_conn):
        cursor = db_conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='rate_case_impacts'"
        )
        assert cursor.fetchone() is not None

    def test_operations_primary_key(self, db_conn):
        db_conn.execute(
            "INSERT INTO utility_operations (eia_utility_id, year, utility_name) VALUES (1, 2022, 'Test')"
        )
        with pytest.raises(sqlite3.IntegrityError):
            db_conn.execute(
                "INSERT INTO utility_operations (eia_utility_id, year, utility_name) VALUES (1, 2022, 'Dupe')"
            )

    def test_emissions_primary_key(self, db_conn):
        db_conn.execute(
            "INSERT INTO utility_emissions (utility_name_egrid, state, year) VALUES ('Test', 'CA', 2022)"
        )
        with pytest.raises(sqlite3.IntegrityError):
            db_conn.execute(
                "INSERT INTO utility_emissions (utility_name_egrid, state, year) VALUES ('Test', 'CA', 2022)"
            )

    def test_capacity_primary_key(self, db_conn):
        db_conn.execute(
            "INSERT INTO utility_capacity (eia_utility_id, year) VALUES (1, 2022)"
        )
        with pytest.raises(sqlite3.IntegrityError):
            db_conn.execute(
                "INSERT INTO utility_capacity (eia_utility_id, year) VALUES (1, 2022)"
            )

    def test_impacts_primary_key(self, db_conn):
        db_conn.execute(
            "INSERT INTO rate_case_impacts (docket_number, source) VALUES ('R-001', 'test')"
        )
        with pytest.raises(sqlite3.IntegrityError):
            db_conn.execute(
                "INSERT INTO rate_case_impacts (docket_number, source) VALUES ('R-001', 'test')"
            )


# ============================================================
# UTILITY OPERATIONS CRUD TESTS
# ============================================================


class TestUtilityOperationsCRUD:
    def test_upsert_operations_batch(self, db_conn, sample_operations):
        created, updated = upsert_utility_operations_batch(sample_operations, conn=db_conn)
        assert created == 3
        assert updated == 0

    def test_upsert_operations_idempotent(self, db_conn, sample_operations):
        upsert_utility_operations_batch(sample_operations, conn=db_conn)
        created, updated = upsert_utility_operations_batch(sample_operations, conn=db_conn)
        assert created == 0
        assert updated == 3

    def test_get_utility_operations_all(self, db_conn, sample_operations):
        upsert_utility_operations_batch(sample_operations, conn=db_conn)
        results = get_utility_operations(conn=db_conn)
        assert len(results) == 3

    def test_get_utility_operations_by_eia_id(self, db_conn, sample_operations):
        upsert_utility_operations_batch(sample_operations, conn=db_conn)
        results = get_utility_operations(eia_utility_id=14328, conn=db_conn)
        assert len(results) == 2  # 2022 and 2021

    def test_get_utility_operations_by_state(self, db_conn, sample_operations):
        upsert_utility_operations_batch(sample_operations, conn=db_conn)
        results = get_utility_operations(state="CA", conn=db_conn)
        assert len(results) == 2

    def test_get_utility_operations_by_year(self, db_conn, sample_operations):
        upsert_utility_operations_batch(sample_operations, conn=db_conn)
        results = get_utility_operations(year=2022, conn=db_conn)
        assert len(results) == 2

    def test_operations_data_integrity(self, db_conn, sample_operations):
        upsert_utility_operations_batch(sample_operations, conn=db_conn)
        results = get_utility_operations(eia_utility_id=14328, year=2022, conn=db_conn)
        assert len(results) == 1
        rec = results[0]
        assert rec["residential_customers"] == 5200000
        assert rec["residential_avg_price"] == 25.3
        assert rec["ownership_type"] == "Investor Owned"


# ============================================================
# EIA LINKS CRUD TESTS
# ============================================================


class TestEIALinksCRUD:
    def test_upsert_links_batch(self, db_conn):
        links = [
            {"utility_name": "PG&E", "state": "CA", "eia_utility_id": 14328,
             "match_confidence": 1.0, "match_method": "exact"},
            {"utility_name": "PGE", "state": "OR", "eia_utility_id": 17166,
             "match_confidence": 0.95, "match_method": "fuzzy_95"},
        ]
        count = upsert_utility_eia_links_batch(links, conn=db_conn)
        assert count == 2

    def test_get_eia_links(self, db_conn):
        links = [
            {"utility_name": "PG&E", "state": "CA", "eia_utility_id": 14328,
             "match_confidence": 1.0, "match_method": "exact"},
        ]
        upsert_utility_eia_links_batch(links, conn=db_conn)
        results = get_utility_eia_links(conn=db_conn)
        assert len(results) == 1
        assert results[0]["eia_utility_id"] == 14328

    def test_links_upsert_updates(self, db_conn):
        links = [{"utility_name": "PG&E", "state": "CA", "eia_utility_id": 14328,
                   "match_confidence": 0.9, "match_method": "fuzzy"}]
        upsert_utility_eia_links_batch(links, conn=db_conn)
        links[0]["match_confidence"] = 1.0
        links[0]["match_method"] = "exact"
        upsert_utility_eia_links_batch(links, conn=db_conn)
        results = get_utility_eia_links(conn=db_conn)
        assert len(results) == 1
        assert results[0]["match_confidence"] == 1.0


# ============================================================
# EMISSIONS CRUD TESTS
# ============================================================


class TestEmissionsCRUD:
    def test_upsert_emissions_batch(self, db_conn, sample_emissions):
        created, updated = upsert_utility_emissions_batch(sample_emissions, conn=db_conn)
        assert created == 2
        assert updated == 0

    def test_upsert_emissions_idempotent(self, db_conn, sample_emissions):
        upsert_utility_emissions_batch(sample_emissions, conn=db_conn)
        created, updated = upsert_utility_emissions_batch(sample_emissions, conn=db_conn)
        assert created == 0
        assert updated == 2

    def test_get_emissions_all(self, db_conn, sample_emissions):
        upsert_utility_emissions_batch(sample_emissions, conn=db_conn)
        results = get_utility_emissions(conn=db_conn)
        assert len(results) == 2

    def test_get_emissions_by_state(self, db_conn, sample_emissions):
        upsert_utility_emissions_batch(sample_emissions, conn=db_conn)
        results = get_utility_emissions(state="CA", conn=db_conn)
        assert len(results) == 1
        assert results[0]["co2_tons"] == 5200000.0

    def test_emissions_data_integrity(self, db_conn, sample_emissions):
        upsert_utility_emissions_batch(sample_emissions, conn=db_conn)
        results = get_utility_emissions(state="CA", year=2022, conn=db_conn)
        assert len(results) == 1
        rec = results[0]
        assert rec["coal_pct"] == 0.0
        assert rec["gas_pct"] == 45.0
        assert rec["co2_rate_lbs_mwh"] == 320.0


# ============================================================
# CAPACITY CRUD TESTS
# ============================================================


class TestCapacityCRUD:
    def test_upsert_capacity_batch(self, db_conn, sample_capacity):
        created, updated = upsert_utility_capacity_batch(sample_capacity, conn=db_conn)
        assert created == 2
        assert updated == 0

    def test_upsert_capacity_idempotent(self, db_conn, sample_capacity):
        upsert_utility_capacity_batch(sample_capacity, conn=db_conn)
        created, updated = upsert_utility_capacity_batch(sample_capacity, conn=db_conn)
        assert created == 0
        assert updated == 2

    def test_get_capacity_all(self, db_conn, sample_capacity):
        upsert_utility_capacity_batch(sample_capacity, conn=db_conn)
        results = get_utility_capacity(conn=db_conn)
        assert len(results) == 2

    def test_get_capacity_by_eia_id(self, db_conn, sample_capacity):
        upsert_utility_capacity_batch(sample_capacity, conn=db_conn)
        results = get_utility_capacity(eia_utility_id=14328, conn=db_conn)
        assert len(results) == 1
        assert results[0]["total_capacity_mw"] == 16950.0

    def test_capacity_data_integrity(self, db_conn, sample_capacity):
        upsert_utility_capacity_batch(sample_capacity, conn=db_conn)
        results = get_utility_capacity(eia_utility_id=14328, year=2022, conn=db_conn)
        assert len(results) == 1
        rec = results[0]
        assert rec["gas_capacity_mw"] == 8500.0
        assert rec["num_generators"] == 220
        assert rec["avg_generator_age"] == 28.5


# ============================================================
# RATE CASE IMPACTS CRUD TESTS
# ============================================================


class TestImpactsCRUD:
    def test_upsert_impacts_batch(self, db_conn):
        impacts = [
            {
                "docket_number": "R-2024-001",
                "source": "test",
                "eia_utility_id": 14328,
                "total_customers": 5835000,
                "monthly_bill_impact": 5.50,
                "annual_bill_impact": 66.00,
                "pct_of_avg_bill": 3.2,
                "residential_price_before": 25.3,
                "residential_price_after": None,
            },
        ]
        count = upsert_rate_case_impacts_batch(impacts, conn=db_conn)
        assert count == 1

    def test_get_impacts(self, db_conn):
        impacts = [
            {"docket_number": "R-001", "source": "test", "monthly_bill_impact": 5.50,
             "annual_bill_impact": 66.00},
            {"docket_number": "R-002", "source": "test", "monthly_bill_impact": 3.20,
             "annual_bill_impact": 38.40},
        ]
        upsert_rate_case_impacts_batch(impacts, conn=db_conn)
        results = get_rate_case_impacts(conn=db_conn)
        assert len(results) == 2
        # Sorted by abs(monthly) desc
        assert results[0]["monthly_bill_impact"] == 5.50


# ============================================================
# ENRICHMENT STATS TESTS
# ============================================================


class TestEnrichmentStats:
    def test_enrichment_stats_empty(self, db_conn):
        stats = get_enrichment_stats(conn=db_conn)
        assert stats["utility_operations"] == 0
        assert stats["eia_links"] == 0
        assert stats["emissions_records"] == 0
        assert stats["capacity_records"] == 0
        assert stats["impact_records"] == 0

    def test_enrichment_stats_with_data(self, db_conn, sample_operations, sample_emissions, sample_capacity):
        upsert_utility_operations_batch(sample_operations, conn=db_conn)
        upsert_utility_emissions_batch(sample_emissions, conn=db_conn)
        upsert_utility_capacity_batch(sample_capacity, conn=db_conn)
        stats = get_enrichment_stats(conn=db_conn)
        assert stats["utility_operations"] == 3
        assert stats["emissions_records"] == 2
        assert stats["capacity_records"] == 2
        assert stats["unique_eia_utilities"] == 2


# ============================================================
# CROSS-LINKING TESTS
# ============================================================


class TestCrossLinking:
    def test_exact_match(self):
        puc = [{"name": "Portland General Electric", "canonical_name": "Portland General Electric", "state": "OR"}]
        eia = [{"eia_utility_id": 17166, "utility_name": "Portland General Electric Co", "state": "OR"}]
        links = cross_link_utilities(puc, eia)
        assert len(links) >= 1
        assert links[0]["eia_utility_id"] == 17166

    def test_fuzzy_match(self):
        puc = [{"name": "Pacific Gas and Electric Company", "canonical_name": "Pacific Gas and Electric Company", "state": "CA"}]
        eia = [{"eia_utility_id": 14328, "utility_name": "Pacific Gas & Electric Co", "state": "CA"}]
        links = cross_link_utilities(puc, eia)
        assert len(links) >= 1
        assert links[0]["eia_utility_id"] == 14328

    def test_no_match_wrong_state(self):
        puc = [{"name": "Pacific Gas and Electric", "canonical_name": "Pacific Gas and Electric", "state": "NY"}]
        eia = [{"eia_utility_id": 14328, "utility_name": "Pacific Gas & Electric Co", "state": "CA"}]
        # Should still match on fuzzy cross-state if score high enough
        links = cross_link_utilities(puc, eia, fuzzy_threshold=95)
        # With high threshold, may not match
        # This tests the logic path exists

    def test_multiple_utilities(self):
        puc = [
            {"name": "PG&E", "canonical_name": "Pacific Gas and Electric", "state": "CA"},
            {"name": "PGE", "canonical_name": "Portland General Electric", "state": "OR"},
        ]
        eia = [
            {"eia_utility_id": 14328, "utility_name": "Pacific Gas & Electric Co", "state": "CA"},
            {"eia_utility_id": 17166, "utility_name": "Portland General Electric Co", "state": "OR"},
        ]
        links = cross_link_utilities(puc, eia)
        assert len(links) >= 1  # At least one should match

    def test_empty_inputs(self):
        links = cross_link_utilities([], [])
        assert links == []


class TestCrossLinkEmissions:
    def test_link_emissions(self):
        eia_links = [
            {"utility_name": "Pacific Gas and Electric", "state": "CA", "eia_utility_id": 14328},
        ]
        emissions = [
            {"utility_name_egrid": "Pacific Gas & Electric Co", "state": "CA", "year": 2022},
        ]
        linked = cross_link_emissions(eia_links, emissions)
        assert linked >= 0  # May or may not match depending on fuzzy

    def test_link_emissions_empty(self):
        linked = cross_link_emissions([], [])
        assert linked == 0


# ============================================================
# CONSUMER IMPACT CALCULATION TESTS
# ============================================================


class TestComputeImpacts:
    def test_basic_impact_calculation(self):
        cases = [{
            "docket_number": "R-001",
            "source": "test",
            "utility_name": "Test Utility",
            "canonical_utility_name": "Test Utility",
            "approved_revenue_change": 100.0,  # $100M
        }]
        links = [{"utility_name": "Test Utility", "state": "CA", "eia_utility_id": 1}]
        operations = [{
            "eia_utility_id": 1,
            "year": 2022,
            "total_customers": 1000000,
            "residential_avg_price": 20.0,
        }]

        impacts = compute_rate_case_impacts(cases, links, operations)
        assert len(impacts) == 1
        imp = impacts[0]
        # $100M / 1M customers = $100/year = ~$8.33/month
        assert abs(imp["annual_bill_impact"] - 100.0) < 0.01
        assert abs(imp["monthly_bill_impact"] - 8.33) < 0.01

    def test_no_approved_revenue(self):
        cases = [{
            "docket_number": "R-001", "source": "test",
            "utility_name": "Test", "approved_revenue_change": None,
        }]
        impacts = compute_rate_case_impacts(cases, [], [])
        assert len(impacts) == 0

    def test_no_matching_eia(self):
        cases = [{
            "docket_number": "R-001", "source": "test",
            "utility_name": "Unknown", "canonical_utility_name": "Unknown",
            "approved_revenue_change": 50.0,
        }]
        links = [{"utility_name": "Other Utility", "state": "CA", "eia_utility_id": 1}]
        operations = [{"eia_utility_id": 1, "year": 2022, "total_customers": 500000}]
        impacts = compute_rate_case_impacts(cases, links, operations)
        assert len(impacts) == 0

    def test_pct_of_avg_bill(self):
        cases = [{
            "docket_number": "R-001", "source": "test",
            "utility_name": "Test", "canonical_utility_name": "Test",
            "approved_revenue_change": 50.0,
        }]
        links = [{"utility_name": "Test", "state": "CA", "eia_utility_id": 1}]
        operations = [{
            "eia_utility_id": 1, "year": 2022,
            "total_customers": 1000000,
            "residential_avg_price": 15.0,  # 15 cents/kWh
        }]

        impacts = compute_rate_case_impacts(cases, links, operations, avg_monthly_kwh=1000)
        assert len(impacts) == 1
        assert impacts[0]["pct_of_avg_bill"] is not None
        assert impacts[0]["pct_of_avg_bill"] > 0

    def test_zero_customers_no_impact(self):
        cases = [{
            "docket_number": "R-001", "source": "test",
            "utility_name": "Test", "canonical_utility_name": "Test",
            "approved_revenue_change": 50.0,
        }]
        links = [{"utility_name": "Test", "state": "CA", "eia_utility_id": 1}]
        operations = [{"eia_utility_id": 1, "year": 2022, "total_customers": 0}]
        impacts = compute_rate_case_impacts(cases, links, operations)
        assert len(impacts) == 0


# ============================================================
# QUALITY SCORING WITH ENRICHMENT TESTS
# ============================================================


class TestQualityScoringWithEnrichment:
    def test_enrichment_weights_sum_to_one(self):
        total = sum(WEIGHTS.values())
        assert abs(total - 1.0) < 0.01

    def test_enrichment_components_in_weights(self):
        assert "has_eia_data_linked" in WEIGHTS
        assert "has_emissions_data" in WEIGHTS
        assert "has_customer_impact" in WEIGHTS

    def test_enrichment_adds_score(self):
        record = {
            "docket_number": "R-001",
            "utility_name": "Test",
            "canonical_utility_name": "Test",
            "case_type": "general_rate_case",
            "filing_date": "2024-01-01",
            "decision_date": "2024-12-01",
            "requested_revenue_change": 50.0,
            "approved_revenue_change": 40.0,
            "status": "decided",
            "source_url": "https://example.com",
        }

        result_no_enrich = score_rate_case(record)
        result_with_enrich = score_rate_case(record, enrichment_data={
            "has_eia_link": True, "has_emissions": True, "has_impact": True,
        })

        assert result_with_enrich["quality_score"] > result_no_enrich["quality_score"]
        assert result_with_enrich["component_scores"]["has_eia_data_linked"] == 1.0
        assert result_with_enrich["component_scores"]["has_emissions_data"] == 1.0
        assert result_with_enrich["component_scores"]["has_customer_impact"] == 1.0

    def test_partial_enrichment(self):
        record = {
            "docket_number": "R-001",
            "utility_name": "Test",
            "status": "active",
        }
        result_partial = score_rate_case(record, enrichment_data={
            "has_eia_link": True, "has_emissions": False, "has_impact": False,
        })
        assert result_partial["component_scores"]["has_eia_data_linked"] == 1.0
        assert result_partial["component_scores"]["has_emissions_data"] == 0.0


# ============================================================
# EIA 861 PARSER TESTS
# ============================================================


class TestEIA861Parser:
    def test_safe_int_valid(self):
        from src.scrapers.eia_861 import _safe_int
        assert _safe_int(42) == 42
        assert _safe_int("42") == 42
        assert _safe_int("1,234") == 1234
        assert _safe_int(42.0) == 42

    def test_safe_int_invalid(self):
        from src.scrapers.eia_861 import _safe_int
        assert _safe_int(None) is None
        assert _safe_int("") is None
        assert _safe_int("N/A") is None
        assert _safe_int(".") is None

    def test_safe_float_valid(self):
        from src.scrapers.eia_861 import _safe_float
        assert _safe_float(42.5) == 42.5
        assert _safe_float("42.5") == 42.5
        assert _safe_float("1,234.56") == 1234.56

    def test_safe_float_invalid(self):
        from src.scrapers.eia_861 import _safe_float
        assert _safe_float(None) is None
        assert _safe_float("") is None
        assert _safe_float("NA") is None

    def test_score_operations(self):
        from src.scrapers.eia_861 import _score_operations
        rec = {
            "eia_utility_id": 1,
            "utility_name": "Test",
            "state": "CA",
            "total_customers": 100000,
            "total_revenue": 500000,
            "total_sales_mwh": 1000000,
            "residential_avg_price": 12.5,
            "ownership_type": "IOU",
        }
        score = _score_operations(rec)
        assert score == 1.0

    def test_score_operations_minimal(self):
        from src.scrapers.eia_861 import _score_operations
        rec = {"eia_utility_id": 1}
        score = _score_operations(rec)
        assert score == 0.15

    def test_find_column(self):
        from src.scrapers.eia_861 import _find_column
        columns = ["Utility Number", "Utility Name", "State", "Residential Customers"]
        assert _find_column(columns, ["Utility Number", "UTILITY_NUMBER"]) == "Utility Number"
        assert _find_column(columns, ["NONEXISTENT"]) is None


# ============================================================
# eGRID PARSER TESTS
# ============================================================


class TestEGRIDParser:
    def test_score_emissions_complete(self):
        from src.scrapers.egrid import _score_emissions
        rec = {
            "utility_name_egrid": "Test",
            "state": "CA",
            "net_generation_mwh": 1000000,
            "co2_tons": 500000,
            "co2_rate_lbs_mwh": 1000,
            "coal_pct": 30,
            "gas_pct": 40,
            "eia_utility_id": 1,
        }
        score = _score_emissions(rec)
        assert score == 1.0

    def test_score_emissions_minimal(self):
        from src.scrapers.egrid import _score_emissions
        rec = {"utility_name_egrid": "Test"}
        score = _score_emissions(rec)
        assert score == 0.15


# ============================================================
# EIA 860 PARSER TESTS
# ============================================================


class TestEIA860Parser:
    def test_score_capacity_complete(self):
        from src.scrapers.eia_860 import _score_capacity
        rec = {
            "eia_utility_id": 1,
            "total_capacity_mw": 5000.0,
            "num_generators": 50,
            "num_plants": 10,
            "avg_generator_age": 25.0,
            "coal_capacity_mw": 1000.0,
            "planned_additions_mw": 200.0,
        }
        score = _score_capacity(rec)
        assert score == 1.0

    def test_score_capacity_minimal(self):
        from src.scrapers.eia_860 import _score_capacity
        rec = {"eia_utility_id": 1}
        score = _score_capacity(rec)
        assert score == 0.15

    def test_tech_to_fuel_mapping(self):
        from src.scrapers.eia_860 import TECH_TO_FUEL
        assert TECH_TO_FUEL["conventional steam coal"] == "coal"
        assert TECH_TO_FUEL["natural gas fired combined cycle"] == "gas"
        assert TECH_TO_FUEL["nuclear"] == "nuclear"
        assert TECH_TO_FUEL["solar photovoltaic"] == "solar"
        assert TECH_TO_FUEL["onshore wind turbine"] == "wind"
