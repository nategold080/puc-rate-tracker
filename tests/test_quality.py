"""Tests for quality scoring and validation."""

import pytest
from datetime import date

from src.validation.quality import (
    WEIGHTS,
    check_referential_integrity,
    score_rate_case,
    validate_record,
    score_all_records,
)


@pytest.fixture
def complete_case():
    """A complete, high-quality rate case record."""
    return {
        "docket_number": "R-2024-3046894",
        "utility_name": "PECO Energy Company",
        "canonical_utility_name": "PECO Energy Company",
        "state": "PA",
        "source": "pennsylvania_puc",
        "case_type": "general_rate_case",
        "utility_type": "electric",
        "status": "decided",
        "filing_date": "2024-03-28",
        "decision_date": "2024-12-15",
        "requested_revenue_change": 245.6,
        "approved_revenue_change": 175.0,
        "rate_base": 8750.0,
        "return_on_equity": 10.95,
    }


@pytest.fixture
def minimal_case():
    """A minimal rate case record with little data."""
    return {
        "docket_number": "R-2024-001",
        "utility_name": "Unknown Utility",
        "state": "PA",
        "source": "test",
    }


class TestScoreRateCase:
    def test_complete_case_high_score(self, complete_case):
        # Without enrichment data, max is ~0.80 (enrichment adds 0.20)
        result = score_rate_case(complete_case)
        assert result["quality_score"] >= 0.7
        assert len(result["issues"]) <= 1  # may lack source_url

    def test_complete_case_with_enrichment(self, complete_case):
        enrichment = {"has_eia_link": True, "has_emissions": True, "has_impact": True}
        result = score_rate_case(complete_case, enrichment_data=enrichment)
        assert result["quality_score"] >= 0.9

    def test_minimal_case_low_score(self, minimal_case):
        result = score_rate_case(minimal_case)
        assert result["quality_score"] < 0.5

    def test_score_range(self, complete_case):
        result = score_rate_case(complete_case)
        assert 0.0 <= result["quality_score"] <= 1.0

    def test_component_scores_present(self, complete_case):
        result = score_rate_case(complete_case)
        components = result["component_scores"]
        for key in WEIGHTS:
            assert key in components
            assert 0.0 <= components[key] <= 1.0

    def test_filing_date_contributes(self):
        case_with = {
            "docket_number": "TEST-001", "utility_name": "Test",
            "state": "PA", "source": "test",
            "filing_date": "2024-01-01",
        }
        case_without = {
            "docket_number": "TEST-001", "utility_name": "Test",
            "state": "PA", "source": "test",
        }
        score_with = score_rate_case(case_with)["quality_score"]
        score_without = score_rate_case(case_without)["quality_score"]
        assert score_with > score_without

    def test_financial_data_contributes(self):
        case_with = {
            "docket_number": "TEST-001", "utility_name": "Test",
            "state": "PA", "source": "test",
            "requested_revenue_change": 100.0,
            "approved_revenue_change": 75.0,
        }
        case_without = {
            "docket_number": "TEST-001", "utility_name": "Test",
            "state": "PA", "source": "test",
        }
        score_with = score_rate_case(case_with)["quality_score"]
        score_without = score_rate_case(case_without)["quality_score"]
        assert score_with > score_without

    def test_source_url_contributes(self, complete_case):
        case_with = dict(complete_case, source_url="https://example.com")
        case_without = dict(complete_case)
        score_with = score_rate_case(case_with)["quality_score"]
        score_without = score_rate_case(case_without)["quality_score"]
        assert score_with > score_without

    def test_active_case_partial_decision_credit(self):
        case = {
            "docket_number": "TEST-001", "utility_name": "Test",
            "state": "PA", "source": "test",
            "status": "active",
        }
        result = score_rate_case(case)
        assert result["component_scores"]["has_decision_date"] == 0.5

    def test_weights_sum_to_one(self):
        total = sum(WEIGHTS.values())
        assert abs(total - 1.0) < 0.001

    def test_issues_list_populated(self, minimal_case):
        result = score_rate_case(minimal_case)
        assert len(result["issues"]) > 0
        assert any("filing" in issue.lower() or "revenue" in issue.lower() for issue in result["issues"])


class TestValidateRecord:
    def test_valid_record_no_errors(self, complete_case):
        errors = validate_record(complete_case)
        assert len(errors) == 0

    def test_missing_required_fields(self):
        errors = validate_record({})
        assert len(errors) >= 3  # docket, utility, state, source

    def test_invalid_date_range(self):
        record = {
            "docket_number": "TEST-001", "utility_name": "Test",
            "state": "PA", "source": "test",
            "filing_date": "1800-01-01",
        }
        errors = validate_record(record)
        assert any("range" in e.lower() for e in errors)

    def test_decision_before_filing(self):
        record = {
            "docket_number": "TEST-001", "utility_name": "Test",
            "state": "PA", "source": "test",
            "filing_date": "2024-06-01",
            "decision_date": "2024-01-01",
        }
        errors = validate_record(record)
        assert any("before" in e.lower() for e in errors)

    def test_unreasonable_revenue(self):
        record = {
            "docket_number": "TEST-001", "utility_name": "Test",
            "state": "PA", "source": "test",
            "requested_revenue_change": 100000.0,
        }
        errors = validate_record(record)
        assert any("unreasonably large" in e.lower() for e in errors)

    def test_approved_exceeds_requested(self):
        record = {
            "docket_number": "TEST-001", "utility_name": "Test",
            "state": "PA", "source": "test",
            "requested_revenue_change": 100.0,
            "approved_revenue_change": 200.0,
        }
        errors = validate_record(record)
        assert any("exceeds" in e.lower() for e in errors)

    def test_roe_out_of_range(self):
        record = {
            "docket_number": "TEST-001", "utility_name": "Test",
            "state": "PA", "source": "test",
            "return_on_equity": 30.0,
        }
        errors = validate_record(record)
        assert any("roe" in e.lower() for e in errors)

    def test_negative_rate_base(self):
        record = {
            "docket_number": "TEST-001", "utility_name": "Test",
            "state": "PA", "source": "test",
            "rate_base": -100.0,
        }
        errors = validate_record(record)
        assert any("negative" in e.lower() for e in errors)


class TestReferentialIntegrity:
    def test_no_issues_clean_data(self):
        records = [
            {"docket_number": "R-001", "source": "test", "utility_name": "A", "state": "PA"},
            {"docket_number": "R-002", "source": "test", "utility_name": "B", "state": "CA"},
        ]
        issues = check_referential_integrity(records)
        assert len(issues) == 0

    def test_detects_duplicates(self):
        records = [
            {"docket_number": "R-001", "source": "test", "utility_name": "A", "state": "PA"},
            {"docket_number": "R-001", "source": "test", "utility_name": "A", "state": "PA"},
        ]
        issues = check_referential_integrity(records)
        assert any("duplicate" in i.lower() for i in issues)

    def test_cross_state_entity_warning(self):
        records = [
            {"docket_number": "R-001", "source": "test",
             "canonical_utility_name": "PacifiCorp", "state": "OR"},
            {"docket_number": "R-002", "source": "test",
             "canonical_utility_name": "PacifiCorp", "state": "WA"},
        ]
        issues = check_referential_integrity(records)
        assert any("multiple states" in i.lower() for i in issues)


class TestScoreAllRecords:
    def test_batch_scoring(self, complete_case, minimal_case):
        records = [complete_case, minimal_case]
        scored = score_all_records(records)
        assert len(scored) == 2
        assert scored[0]["quality_score"] > scored[1]["quality_score"]

    def test_with_document_counts(self, complete_case):
        records = [complete_case]
        counts = {"R-2024-3046894": 5}
        scored = score_all_records(records, document_counts=counts)
        assert scored[0]["quality_score"] > 0
