"""Tests for Pydantic v2 schemas — validation, parsing, edge cases."""

import pytest
from datetime import date, datetime
from pydantic import ValidationError

from src.validation.schemas import (
    CaseDocument,
    CaseStatus,
    CaseType,
    DocumentType,
    OwnershipType,
    PipelineRun,
    RateCase,
    Utility,
    UtilityType,
    parse_date_flexible,
    parse_dollar_amount,
    parse_percentage,
)


# --- RateCase Schema Tests ---


class TestRateCase:
    def test_minimal_valid_case(self):
        case = RateCase(
            docket_number="R-2024-3046894",
            utility_name="PECO Energy Company",
            state="PA",
            source="pennsylvania_puc",
        )
        assert case.docket_number == "R-2024-3046894"
        assert case.state == "PA"
        assert case.case_type == CaseType.UNKNOWN
        assert case.status == CaseStatus.UNKNOWN

    def test_full_valid_case(self):
        case = RateCase(
            docket_number="R-2024-3046894",
            utility_name="PECO Energy Company",
            state="PA",
            source="pennsylvania_puc",
            case_type=CaseType.GENERAL_RATE_CASE,
            utility_type=UtilityType.ELECTRIC,
            status=CaseStatus.ACTIVE,
            filing_date=date(2024, 3, 28),
            decision_date=None,
            requested_revenue_change=245.6,
            approved_revenue_change=None,
            rate_base=8750.0,
            return_on_equity=10.95,
        )
        assert case.requested_revenue_change == 245.6
        assert case.return_on_equity == 10.95

    def test_state_validation_lowercase(self):
        case = RateCase(
            docket_number="R-2024-001",
            utility_name="Test",
            state="pa",
            source="test",
        )
        assert case.state == "PA"

    def test_state_validation_invalid(self):
        with pytest.raises(ValidationError):
            RateCase(
                docket_number="R-2024-001",
                utility_name="Test",
                state="XX",
                source="test",
            )

    def test_date_range_validation(self):
        with pytest.raises(ValidationError):
            RateCase(
                docket_number="R-2024-001",
                utility_name="Test",
                state="PA",
                source="test",
                filing_date=date(1800, 1, 1),
            )

    def test_decision_before_filing_raises(self):
        with pytest.raises(ValidationError):
            RateCase(
                docket_number="R-2024-001",
                utility_name="Test",
                state="PA",
                source="test",
                filing_date=date(2024, 6, 1),
                decision_date=date(2024, 1, 1),
            )

    def test_dollar_amount_rounding(self):
        case = RateCase(
            docket_number="R-2024-001",
            utility_name="Test",
            state="PA",
            source="test",
            requested_revenue_change=245.6789,
        )
        assert case.requested_revenue_change == 245.679

    def test_unreasonable_dollar_amount(self):
        with pytest.raises(ValidationError):
            RateCase(
                docket_number="R-2024-001",
                utility_name="Test",
                state="PA",
                source="test",
                requested_revenue_change=100000.0,
            )

    def test_roe_out_of_range(self):
        with pytest.raises(ValidationError):
            RateCase(
                docket_number="R-2024-001",
                utility_name="Test",
                state="PA",
                source="test",
                return_on_equity=30.0,
            )

    def test_whitespace_stripping(self):
        case = RateCase(
            docket_number="  R-2024-001  ",
            utility_name="  PECO Energy  ",
            state="  PA  ",
            source="test",
        )
        assert case.docket_number == "R-2024-001"
        assert case.utility_name == "PECO Energy"
        assert case.state == "PA"

    def test_empty_docket_number_rejected(self):
        with pytest.raises(ValidationError):
            RateCase(
                docket_number="",
                utility_name="Test",
                state="PA",
                source="test",
            )


# --- Utility Schema Tests ---


class TestUtility:
    def test_minimal_utility(self):
        util = Utility(name="Test Utility")
        assert util.name == "Test Utility"
        assert util.utility_type == UtilityType.UNKNOWN

    def test_full_utility(self):
        util = Utility(
            name="PECO Energy Company",
            state="PA",
            utility_type=UtilityType.ELECTRIC,
            ownership_type=OwnershipType.INVESTOR_OWNED,
            parent_company="Exelon Corporation",
            customer_count=1600000,
        )
        assert util.parent_company == "Exelon Corporation"
        assert util.customer_count == 1600000

    def test_state_validation(self):
        util = Utility(name="Test", state="ca")
        assert util.state == "CA"

    def test_invalid_state(self):
        with pytest.raises(ValidationError):
            Utility(name="Test", state="ZZ")

    def test_negative_customer_count(self):
        with pytest.raises(ValidationError):
            Utility(name="Test", customer_count=-1)


# --- CaseDocument Schema Tests ---


class TestCaseDocument:
    def test_minimal_document(self):
        doc = CaseDocument(
            docket_number="R-2024-001",
            source="test",
            state="PA",
        )
        assert doc.document_type == DocumentType.OTHER

    def test_full_document(self):
        doc = CaseDocument(
            docket_number="R-2024-001",
            document_type=DocumentType.ORDER,
            title="Final Order",
            filed_by="PA PUC",
            filing_date=date(2024, 6, 1),
            url="https://example.com/order.pdf",
            source="pennsylvania_puc",
            state="PA",
        )
        assert doc.title == "Final Order"

    def test_invalid_url(self):
        with pytest.raises(ValidationError):
            CaseDocument(
                docket_number="R-2024-001",
                source="test",
                state="PA",
                url="not-a-url",
            )

    def test_valid_url(self):
        doc = CaseDocument(
            docket_number="R-2024-001",
            source="test",
            state="PA",
            url="https://www.puc.pa.gov/doc.pdf",
        )
        assert doc.url == "https://www.puc.pa.gov/doc.pdf"


# --- Date Parsing Tests ---


class TestParseDateFlexible:
    def test_iso_format(self):
        assert parse_date_flexible("2024-03-28") == date(2024, 3, 28)

    def test_us_format(self):
        assert parse_date_flexible("03/28/2024") == date(2024, 3, 28)

    def test_us_format_single_digits(self):
        assert parse_date_flexible("3/5/2024") == date(2024, 3, 5)

    def test_long_month(self):
        assert parse_date_flexible("March 28, 2024") == date(2024, 3, 28)

    def test_short_month(self):
        assert parse_date_flexible("Mar 28, 2024") == date(2024, 3, 28)

    def test_none_input(self):
        assert parse_date_flexible("") is None

    def test_invalid_string(self):
        assert parse_date_flexible("not-a-date") is None

    def test_whitespace(self):
        assert parse_date_flexible("  2024-03-28  ") == date(2024, 3, 28)


# --- Dollar Amount Parsing Tests ---


class TestParseDollarAmount:
    def test_millions(self):
        assert parse_dollar_amount("$245.6 million") == 245.6

    def test_billions(self):
        assert parse_dollar_amount("$2.8 billion") == 2800.0

    def test_m_suffix(self):
        assert parse_dollar_amount("$245.6M") == 245.6

    def test_b_suffix(self):
        assert parse_dollar_amount("$2.8B") == 2800.0

    def test_plain_amount(self):
        result = parse_dollar_amount("$25,300,000")
        assert result == 25.3

    def test_large_plain_amount(self):
        result = parse_dollar_amount("$1,000,000,000")
        assert result == 1000.0

    def test_none_input(self):
        assert parse_dollar_amount("") is None

    def test_no_dollar_sign(self):
        assert parse_dollar_amount("245 million") is None

    def test_very_small_amount(self):
        assert parse_dollar_amount("$50") is None  # Too small


# --- Percentage Parsing Tests ---


class TestParsePercentage:
    def test_with_percent_sign(self):
        assert parse_percentage("10.5%") == 10.5

    def test_with_percent_word(self):
        assert parse_percentage("10.5 percent") == 10.5

    def test_integer_percent(self):
        assert parse_percentage("8%") == 8.0

    def test_none_input(self):
        assert parse_percentage("") is None

    def test_no_percent(self):
        assert parse_percentage("just a number 10") is None


# --- Pipeline Run Schema Tests ---


class TestPipelineRun:
    def test_minimal_run(self):
        run = PipelineRun(source="test", stage="scrape", status="running")
        assert run.records_processed == 0
        assert run.errors == 0

    def test_completed_run(self):
        run = PipelineRun(
            source="pennsylvania_puc",
            stage="extract",
            status="completed",
            records_processed=100,
            records_created=90,
            records_updated=10,
        )
        assert run.records_processed == 100
