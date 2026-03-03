"""Tests for scraper helper functions (no network calls).

Tests parsing, classification, and extraction functions used by
the CT PURA, MO PSC, and GA PSC scrapers.
"""

import pytest


# ─── CT PURA Tests ──────────────────────────────────────────────────────


class TestCTPURAHelpers:
    """Test CT PURA scraper helper functions."""

    def test_classify_utility_type_electric(self):
        from src.scrapers.ct_pura import _classify_utility_type
        assert _classify_utility_type("Connecticut Light and Power Company") == "electric"
        assert _classify_utility_type("United Illuminating rate case") == "electric"
        assert _classify_utility_type("CL&P distribution rate") == "electric"
        assert _classify_utility_type("GenConn Energy LLC") == "electric"

    def test_classify_utility_type_gas(self):
        from src.scrapers.ct_pura import _classify_utility_type
        assert _classify_utility_type("Connecticut Natural Gas") == "gas"
        assert _classify_utility_type("Yankee Gas Services") == "gas"
        assert _classify_utility_type("Southern Connecticut Gas rate") == "gas"
        assert _classify_utility_type("Application for (SCG) rate") == "gas"

    def test_classify_utility_type_water(self):
        from src.scrapers.ct_pura import _classify_utility_type
        assert _classify_utility_type("Aquarion Water Company") == "water"
        assert _classify_utility_type("Connecticut Water Company") == "water"

    def test_classify_utility_type_unknown(self):
        from src.scrapers.ct_pura import _classify_utility_type
        assert _classify_utility_type("Eversource Energy") == "unknown"
        assert _classify_utility_type("some random text") == "unknown"

    def test_extract_utility_name(self):
        from src.scrapers.ct_pura import _extract_utility_name
        assert _extract_utility_name("Eversource Energy rate case") == "Eversource Energy"
        assert _extract_utility_name("Application by United Illuminating") == "United Illuminating Company"
        assert _extract_utility_name("Connecticut Light and Power") == "Connecticut Light and Power Company"
        assert _extract_utility_name("CL&P distribution") == "Connecticut Light and Power Company"
        assert _extract_utility_name("Aquarion Water rate") == "Aquarion Water Company"

    def test_extract_utility_name_gas(self):
        from src.scrapers.ct_pura import _extract_utility_name
        assert _extract_utility_name("Connecticut Natural Gas rate") == "Connecticut Natural Gas Corporation"
        assert _extract_utility_name("(CNG) application") == "Connecticut Natural Gas Corporation"
        assert _extract_utility_name("Southern Connecticut Gas") == "Southern Connecticut Gas Company"
        assert _extract_utility_name("(SCG) rate case") == "Southern Connecticut Gas Company"
        assert _extract_utility_name("Yankee Gas Services") == "Yankee Gas Services Company"

    def test_extract_utility_name_not_found(self):
        from src.scrapers.ct_pura import _extract_utility_name
        assert _extract_utility_name("unknown company rate case") == ""

    def test_docket_to_year(self):
        from src.scrapers.ct_pura import _docket_to_year
        assert _docket_to_year("24-10-04") == 2024
        assert _docket_to_year("00-01-01") == 2000
        assert _docket_to_year("99-12-01") == 1999
        assert _docket_to_year("18-03-28") == 2018
        assert _docket_to_year("invalid") is None

    def test_docket_to_filing_date(self):
        from src.scrapers.ct_pura import _docket_to_filing_date
        assert _docket_to_filing_date("24-10-04") == "2024-10-01"
        assert _docket_to_filing_date("18-03-28") == "2018-03-01"
        assert _docket_to_filing_date("00-01-01") == "2000-01-01"
        assert _docket_to_filing_date("24-13-01") is None  # invalid month
        assert _docket_to_filing_date("24-00-01") is None  # invalid month
        assert _docket_to_filing_date("invalid") is None

    def test_extract_docket_refs(self):
        from src.scrapers.ct_pura import _extract_docket_refs
        context = {}
        html = 'some text [24-10-04] more text about rate case and [18-03-28] filing'
        _extract_docket_refs(html, context)
        assert "24-10-04" in context
        assert "18-03-28" in context

    def test_extract_docket_refs_with_suffix(self):
        from src.scrapers.ct_pura import _extract_docket_refs
        context = {}
        html = 'text [24-10-04RE01] more text'
        _extract_docket_refs(html, context)
        assert "24-10-04RE01" in context

    def test_extract_docket_refs_keeps_longer_context(self):
        from src.scrapers.ct_pura import _extract_docket_refs
        context = {"24-10-04": "short"}
        html = 'a' * 200 + '[24-10-04]' + 'b' * 200
        _extract_docket_refs(html, context)
        assert len(context["24-10-04"]) > len("short")


# ─── MO PSC Tests ──────────────────────────────────────────────────────


class TestMOPSCHelpers:
    """Test MO PSC scraper helper functions."""

    def test_case_year(self):
        from src.scrapers.mo_psc import _case_year
        assert _case_year("ER-2024-0319") == 2024
        assert _case_year("GR-2021-0280") == 2021
        assert _case_year("WR-2007-0216") == 2007
        assert _case_year("invalid") is None

    def test_parse_mo_date(self):
        from src.scrapers.mo_psc import _parse_mo_date
        assert _parse_mo_date("9/9/2025") == "2025-09-09"
        assert _parse_mo_date("12/31/2024") == "2024-12-31"
        assert _parse_mo_date("1/5/2020") == "2020-01-05"
        assert _parse_mo_date("2024-03-15") == "2024-03-15"
        assert _parse_mo_date("") is None
        assert _parse_mo_date(None) is None

    def test_classify_mo_case_type(self):
        from src.scrapers.mo_psc import _classify_mo_case_type
        assert _classify_mo_case_type("General Rate Increase", "ER-2024-0319") == "general_rate_case"
        assert _classify_mo_case_type("Fuel Adjustment Clause", "ER-2024-0100") == "fuel_cost_adjustment"
        assert _classify_mo_case_type("Infrastructure Replacement Surcharge", "WR-2024-0050") == "infrastructure_rider"
        assert _classify_mo_case_type("Tariff Filing", "GR-2020-0100") == "general_rate_case"
        assert _classify_mo_case_type("", "ER-2024-0001") == "general_rate_case"
        assert _classify_mo_case_type("", "") == "unknown"

    def test_prefix_utility_type(self):
        from src.scrapers.mo_psc import PREFIX_UTILITY_TYPE
        assert PREFIX_UTILITY_TYPE["ER"] == "electric"
        assert PREFIX_UTILITY_TYPE["GR"] == "gas"
        assert PREFIX_UTILITY_TYPE["WR"] == "water"
        assert PREFIX_UTILITY_TYPE["SR"] == "wastewater"

    def test_rate_case_prefixes(self):
        from src.scrapers.mo_psc import RATE_CASE_PREFIXES
        assert "ER-" in RATE_CASE_PREFIXES
        assert "GR-" in RATE_CASE_PREFIXES
        assert "WR-" in RATE_CASE_PREFIXES
        assert "SR-" in RATE_CASE_PREFIXES

    def test_extract_companies_from_aria(self):
        from src.scrapers.mo_psc import _extract_companies
        html = '''
        <div>Subject Companies</div>
        <a aria-label="View relationships for Ameren Missouri" href="#">Ameren Missouri</a>
        <div>Style of Case</div>
        '''
        companies = _extract_companies(html)
        assert "Ameren Missouri" in companies

    def test_extract_filing_date(self):
        from src.scrapers.mo_psc import _extract_filing_date
        html = '<td>12/15/2024</td><td>1/2/2025</td><td>6/30/2024</td>'
        result = _extract_filing_date(html)
        assert result == "2024-06-30"  # Earliest date

    def test_extract_filing_date_no_dates(self):
        from src.scrapers.mo_psc import _extract_filing_date
        html = '<td>No dates here</td>'
        assert _extract_filing_date(html) is None


# ─── GA PSC Tests ──────────────────────────────────────────────────────


class TestGAPSCHelpers:
    """Test GA PSC scraper helper functions."""

    def test_extract_utility_name(self):
        from src.scrapers.ga_psc import _extract_utility_name
        assert _extract_utility_name("Georgia Power 2022 Rate Case") == "Georgia Power Company"
        assert _extract_utility_name("Atlanta Gas Light Rate Filing") == "Atlanta Gas Light Company"
        assert _extract_utility_name("Liberty Utilities rate adjustment") == "Liberty Utilities"

    def test_extract_utility_name_not_found(self):
        from src.scrapers.ga_psc import _extract_utility_name
        assert _extract_utility_name("Some unknown utility") == ""

    def test_classify_utility_type(self):
        from src.scrapers.ga_psc import _classify_utility_type
        assert _classify_utility_type("Georgia Power Rate Case") == "electric"
        assert _classify_utility_type("Atlanta Gas Light Rate") == "gas"
        assert _classify_utility_type("Natural gas rate case") == "gas"
        assert _classify_utility_type("Water rate case") == "water"
        assert _classify_utility_type("Telephone rate") == "telecommunications"
        assert _classify_utility_type("Unknown entity") == "unknown"

    def test_extract_year_from_title(self):
        from src.scrapers.ga_psc import _extract_year_from_title
        assert _extract_year_from_title("Georgia Power 2022 Rate Case") == 2022
        assert _extract_year_from_title("2019 Base Rate Case") == 2019
        assert _extract_year_from_title("Rate Case (2004)") == 2004
        assert _extract_year_from_title("No year here") is None


# ─── Integration Tests (Source Registration) ───────────────────────────


class TestSourceRegistration:
    """Verify all sources are registered in extractors and CLI."""

    def test_all_sources_in_extract_all(self):
        from src.extractors.rate_case_parser import extract_all
        import inspect
        source = inspect.getsource(extract_all)
        assert "connecticut_pura" in source
        assert "missouri_psc" in source
        assert "georgia_psc" in source

    def test_all_source_dirs_registered(self):
        from src.extractors.rate_case_parser import extract_source
        import inspect
        source = inspect.getsource(extract_source)
        assert "ct_pura" in source
        assert "mo_psc" in source
        assert "ga_psc" in source

    def test_source_keys_consistent(self):
        """Verify source keys match between scrapers and extractor."""
        from src.scrapers.ct_pura import SOURCE_KEY as ct_key
        from src.scrapers.mo_psc import SOURCE_KEY as mo_key
        from src.scrapers.ga_psc import SOURCE_KEY as ga_key
        assert ct_key == "connecticut_pura"
        assert mo_key == "missouri_psc"
        assert ga_key == "georgia_psc"

    def test_state_codes(self):
        from src.scrapers.ct_pura import STATE as ct_state
        from src.scrapers.mo_psc import STATE as mo_state
        from src.scrapers.ga_psc import STATE as ga_state
        assert ct_state == "CT"
        assert mo_state == "MO"
        assert ga_state == "GA"
