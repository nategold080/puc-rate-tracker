"""Tests for rate case parser and extractor."""

import pytest
from datetime import date

from src.extractors.rate_case_parser import (
    classify_case_type,
    classify_utility_type,
    extract_all_dates,
    extract_all_dollar_amounts,
    extract_date,
    extract_dollar_amount,
    extract_rate_base,
    extract_revenue_approved,
    extract_revenue_request,
    extract_roe,
    normalize_status,
    parse_raw_record,
)
from src.validation.schemas import CaseStatus, CaseType, UtilityType


class TestExtractDollarAmount:
    def test_millions_word(self):
        assert extract_dollar_amount("revenue increase of $245.6 million") == 245.6

    def test_billions_word(self):
        assert extract_dollar_amount("rate base of $2.8 billion") == 2800.0

    def test_m_suffix(self):
        assert extract_dollar_amount("$245.6M") == 245.6

    def test_b_suffix(self):
        assert extract_dollar_amount("$2.8B") == 2800.0

    def test_plain_dollars(self):
        result = extract_dollar_amount("$25,300,000")
        assert result == 25.3

    def test_plain_with_cents(self):
        result = extract_dollar_amount("$25,300,000.50")
        assert abs(result - 25.3) < 0.01

    def test_decrease_negative(self):
        result = extract_dollar_amount("decrease of $10 million")
        assert result == -10.0

    def test_none_on_empty(self):
        assert extract_dollar_amount("") is None
        assert extract_dollar_amount(None) is None

    def test_no_match(self):
        assert extract_dollar_amount("no dollar amounts here") is None

    def test_comma_separated(self):
        result = extract_dollar_amount("$1,234,567,890")
        assert abs(result - 1234.568) < 0.01

    def test_multiple_amounts_returns_first(self):
        result = extract_dollar_amount("requested $100 million, approved $75 million")
        assert result == 100.0


class TestExtractAllDollarAmounts:
    def test_multiple_amounts(self):
        amounts = extract_all_dollar_amounts("$100 million and $75 million")
        assert len(amounts) == 2
        assert 100.0 in amounts
        assert 75.0 in amounts

    def test_empty(self):
        assert extract_all_dollar_amounts("") == []


class TestExtractDate:
    def test_iso_date(self):
        assert extract_date("filed on 2024-03-28") == date(2024, 3, 28)

    def test_us_date(self):
        assert extract_date("filed on 03/28/2024") == date(2024, 3, 28)

    def test_long_month(self):
        assert extract_date("filed March 28, 2024") == date(2024, 3, 28)

    def test_short_month(self):
        assert extract_date("filed Mar 28, 2024") == date(2024, 3, 28)

    def test_no_date(self):
        assert extract_date("no date here") is None

    def test_empty(self):
        assert extract_date("") is None


class TestExtractAllDates:
    def test_multiple_dates(self):
        dates = extract_all_dates("filed 2024-03-28, decided 2024-12-15")
        assert len(dates) >= 2

    def test_empty(self):
        assert extract_all_dates("") == []


class TestClassifyCaseType:
    def test_general_rate_case(self):
        result = classify_case_type("General Rate Case - Electric Distribution")
        assert result == CaseType.GENERAL_RATE_CASE

    def test_base_rate(self):
        result = classify_case_type("Base Rate Filing for gas service")
        assert result == CaseType.GENERAL_RATE_CASE

    def test_fuel_cost(self):
        result = classify_case_type("Fuel Cost Adjustment Filing")
        assert result == CaseType.FUEL_COST_ADJUSTMENT

    def test_infrastructure_rider(self):
        result = classify_case_type("Distribution System Improvement Charge")
        assert result == CaseType.INFRASTRUCTURE_RIDER

    def test_rate_design(self):
        result = classify_case_type("Rate Design and Time-of-Use Proceeding")
        assert result == CaseType.RATE_DESIGN

    def test_unknown_text(self):
        result = classify_case_type("Something completely unrelated")
        assert result == CaseType.UNKNOWN

    def test_empty(self):
        result = classify_case_type("")
        assert result == CaseType.UNKNOWN

    def test_revenue_requirement_keyword(self):
        result = classify_case_type("Revenue Requirement Proceeding")
        assert result == CaseType.GENERAL_RATE_CASE

    def test_docket_number_helps(self):
        # GRC keyword in description
        result = classify_case_type("GRC Phase 2", "A.23-05-010")
        assert result == CaseType.GENERAL_RATE_CASE


class TestClassifyUtilityType:
    def test_electric_from_text(self):
        result = classify_utility_type("Electric Distribution Rate Case")
        assert result == UtilityType.ELECTRIC

    def test_gas_from_text(self):
        result = classify_utility_type("Natural Gas Distribution Rate Case")
        assert result == UtilityType.GAS

    def test_water_from_text(self):
        result = classify_utility_type("Water Service Company - Rate Case")
        assert result == UtilityType.WATER

    def test_telecom_from_text(self):
        result = classify_utility_type("Telecommunications Rate Filing")
        assert result == UtilityType.TELECOMMUNICATIONS

    def test_electric_from_docket_ue(self):
        result = classify_utility_type("", docket_number="UE-220066")
        assert result == UtilityType.ELECTRIC

    def test_gas_from_docket_ug(self):
        result = classify_utility_type("", docket_number="UG-220067")
        assert result == UtilityType.GAS

    def test_water_from_docket_uw(self):
        result = classify_utility_type("", docket_number="UW-123456")
        assert result == UtilityType.WATER

    def test_unknown(self):
        result = classify_utility_type("Something generic")
        assert result == UtilityType.UNKNOWN


class TestNormalizeStatus:
    def test_active(self):
        assert normalize_status("Active") == CaseStatus.ACTIVE

    def test_open(self):
        assert normalize_status("Open") == CaseStatus.ACTIVE

    def test_decided(self):
        assert normalize_status("Decided") == CaseStatus.DECIDED

    def test_closed(self):
        assert normalize_status("Closed") == CaseStatus.DECIDED

    def test_filed(self):
        assert normalize_status("Filed") == CaseStatus.FILED

    def test_withdrawn(self):
        assert normalize_status("Withdrawn") == CaseStatus.WITHDRAWN

    def test_suspended(self):
        assert normalize_status("Suspended") == CaseStatus.SUSPENDED

    def test_settled(self):
        assert normalize_status("Settlement reached") == CaseStatus.SETTLED

    def test_unknown(self):
        assert normalize_status("") == CaseStatus.UNKNOWN

    def test_random_text(self):
        assert normalize_status("xyzzy") == CaseStatus.UNKNOWN


class TestRevenueExtraction:
    def test_revenue_request(self):
        text = "The utility requested a revenue increase of $245.6 million"
        result = extract_revenue_request(text)
        assert result == 245.6

    def test_revenue_approved(self):
        text = "The commission approved a revenue increase of $175.0 million"
        result = extract_revenue_approved(text)
        assert result == 175.0

    def test_roe(self):
        text = "The authorized return on equity of 10.5%"
        result = extract_roe(text)
        assert result == 10.5

    def test_rate_base(self):
        text = "Based on a rate base of $8.75 billion"
        result = extract_rate_base(text)
        assert result == 8750.0

    def test_none_on_no_match(self):
        assert extract_revenue_request("no revenue info") is None
        assert extract_revenue_approved("no revenue info") is None
        assert extract_roe("no roe info") is None
        assert extract_rate_base("no rate base info") is None


class TestParseRawRecord:
    def test_basic_parsing(self):
        raw = {
            "docket_number": "R-2024-3046894",
            "utility_name": "PECO Energy Company",
            "state": "PA",
            "source": "pennsylvania_puc",
            "case_type": "general_rate_case",
            "utility_type": "electric",
            "status": "active",
            "filing_date": "2024-03-28",
            "requested_revenue_change": 245.6,
        }
        result = parse_raw_record(raw)
        assert result is not None
        assert result["docket_number"] == "R-2024-3046894"
        assert result["state"] == "PA"
        assert result["case_type"] == "general_rate_case"

    def test_missing_docket_returns_none(self):
        raw = {"utility_name": "Test", "state": "PA", "source": "test"}
        assert parse_raw_record(raw) is None

    def test_missing_utility_returns_none(self):
        raw = {"docket_number": "R-001", "state": "PA", "source": "test"}
        assert parse_raw_record(raw) is None

    def test_classification_from_description(self):
        raw = {
            "docket_number": "R-2024-001",
            "utility_name": "Test Electric Co",
            "state": "PA",
            "source": "test",
            "description": "General Rate Case - Electric Distribution Service",
        }
        result = parse_raw_record(raw)
        assert result is not None
        assert result["case_type"] == "general_rate_case"
        assert result["utility_type"] == "electric"

    def test_date_string_parsing(self):
        raw = {
            "docket_number": "R-2024-001",
            "utility_name": "Test",
            "state": "PA",
            "source": "test",
            "filing_date": "2024-03-28",
        }
        result = parse_raw_record(raw)
        assert result["filing_date"] == date(2024, 3, 28)

    def test_preserves_financial_data(self):
        raw = {
            "docket_number": "R-2024-001",
            "utility_name": "Test",
            "state": "PA",
            "source": "test",
            "requested_revenue_change": 100.5,
            "approved_revenue_change": 75.0,
            "rate_base": 5000.0,
            "return_on_equity": 10.5,
        }
        result = parse_raw_record(raw)
        assert result["requested_revenue_change"] == 100.5
        assert result["approved_revenue_change"] == 75.0
        assert result["rate_base"] == 5000.0
        assert result["return_on_equity"] == 10.5

    def test_state_uppercase(self):
        raw = {
            "docket_number": "R-001",
            "utility_name": "Test",
            "state": "pa",
            "source": "test",
        }
        result = parse_raw_record(raw)
        assert result["state"] == "PA"
