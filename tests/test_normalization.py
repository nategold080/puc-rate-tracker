"""Tests for utility name normalization and entity resolution."""

import pytest

from src.normalization.utilities import (
    classify_ownership_type,
    clean_utility_name,
    get_parent_company,
    normalize_all_utilities,
    resolve_utility_name,
)
from src.validation.schemas import OwnershipType


class TestCleanUtilityName:
    def test_strip_inc(self):
        result = clean_utility_name("PECO Energy Company Inc.")
        assert "Inc." not in result

    def test_strip_corporation(self):
        result = clean_utility_name("PPL Electric Utilities Corporation")
        assert "Corporation" not in result

    def test_strip_llc(self):
        result = clean_utility_name("Duke Energy Indiana LLC")
        assert "LLC" not in result

    def test_strip_co(self):
        result = clean_utility_name("Duquesne Light Co.")
        assert "Co." not in result

    def test_expand_ampersand(self):
        result = clean_utility_name("Pacific Gas & Electric")
        assert "and" in result

    def test_normalize_whitespace(self):
        result = clean_utility_name("  PECO   Energy   Company  ")
        assert "  " not in result
        assert result == result.strip()

    def test_empty_string(self):
        result = clean_utility_name("")
        assert result == ""

    def test_preserves_core_name(self):
        result = clean_utility_name("Portland General Electric Company")
        assert "Portland General Electric" in result

    def test_strip_trailing_punctuation(self):
        result = clean_utility_name("Test Utility,")
        assert not result.endswith(",")


class TestResolveUtilityName:
    def test_exact_alias_match(self):
        result = resolve_utility_name("PECO Energy Company")
        assert result["canonical_name"] == "PECO Energy Company"
        assert result["parent_company"] == "Exelon Corporation"
        assert result["match_method"] == "exact_alias"

    def test_short_alias_match(self):
        result = resolve_utility_name("PG&E")
        assert result["canonical_name"] == "Pacific Gas and Electric Company"
        assert result["match_method"] == "exact_alias"

    def test_ppl_electric(self):
        result = resolve_utility_name("PPL Electric Utilities Corporation")
        assert result["canonical_name"] == "PPL Electric Utilities Corporation"
        assert result["parent_company"] == "PPL Corporation"

    def test_duquesne_light(self):
        result = resolve_utility_name("Duquesne Light Company")
        assert result["canonical_name"] == "Duquesne Light Company"

    def test_fuzzy_match(self):
        # Slight variation should still match
        result = resolve_utility_name("PPL Electric Utilities Corp")
        assert result["canonical_name"] == "PPL Electric Utilities Corporation"
        assert "fuzzy" in result["match_method"] or result["match_method"] == "exact_alias"

    def test_unresolved_name(self):
        result = resolve_utility_name("Completely Unknown Utility XYZ 12345")
        assert result["match_method"] == "unresolved"
        assert result["canonical_name"] is not None  # Should still return cleaned name

    def test_empty_name(self):
        result = resolve_utility_name("")
        assert result["canonical_name"] is None
        assert result["match_method"] is None

    def test_state_filtering(self):
        # PacifiCorp exists in OR and WA — should prefer state match
        result = resolve_utility_name("PacifiCorp", state="OR")
        assert result["canonical_name"] is not None

    def test_columbia_gas(self):
        result = resolve_utility_name("Columbia Gas of Pennsylvania Inc.")
        assert "Columbia Gas" in result["canonical_name"]
        assert result["parent_company"] == "NiSource Inc."

    def test_california_water(self):
        result = resolve_utility_name("California Water Service Company")
        assert result["canonical_name"] == "California Water Service Company"

    def test_sce_abbreviation(self):
        result = resolve_utility_name("SCE")
        assert result["canonical_name"] == "Southern California Edison Company"

    def test_sdge(self):
        result = resolve_utility_name("SDG&E")
        assert result["canonical_name"] == "San Diego Gas & Electric Company"

    def test_philadelphia_gas_works(self):
        result = resolve_utility_name("PGW")
        assert result["canonical_name"] == "Philadelphia Gas Works"

    def test_avista(self):
        result = resolve_utility_name("Avista Corporation", state="WA")
        assert result["canonical_name"] is not None
        assert "Avista" in result["canonical_name"]


class TestClassifyOwnershipType:
    def test_investor_owned_inc(self):
        assert classify_ownership_type("PECO Energy Company Inc.") == OwnershipType.INVESTOR_OWNED

    def test_investor_owned_company(self):
        assert classify_ownership_type("Portland General Electric Company") == OwnershipType.INVESTOR_OWNED

    def test_cooperative(self):
        assert classify_ownership_type("Rural Electric Cooperative") == OwnershipType.COOPERATIVE

    def test_municipal(self):
        assert classify_ownership_type("City of Seattle Municipal Light") == OwnershipType.MUNICIPAL

    def test_default_iou(self):
        # Most PUC-regulated utilities are IOUs
        assert classify_ownership_type("Some Utility") == OwnershipType.INVESTOR_OWNED


class TestGetParentCompany:
    def test_known_parent(self):
        parent = get_parent_company("PECO Energy Company")
        assert parent == "Exelon Corporation"

    def test_unknown_parent(self):
        parent = get_parent_company("Totally Unknown Utility XYZZY")
        assert parent is None

    def test_pge_parent(self):
        parent = get_parent_company("Pacific Gas and Electric Company")
        assert parent == "PG&E Corporation"


class TestNormalizeAllUtilities:
    def test_batch_normalization(self):
        records = [
            {"utility_name": "PECO Energy Company", "state": "PA"},
            {"utility_name": "PG&E", "state": "CA"},
            {"utility_name": "Unknown Utility 123", "state": "PA"},
        ]
        updated, mapping = normalize_all_utilities(records)
        assert len(updated) == 3
        assert updated[0]["canonical_utility_name"] == "PECO Energy Company"
        assert updated[1]["canonical_utility_name"] == "Pacific Gas and Electric Company"
        # Unknown should still get a canonical name (the cleaned version)
        assert updated[2]["canonical_utility_name"] is not None

    def test_mapping_consistency(self):
        records = [
            {"utility_name": "PG&E", "state": "CA"},
            {"utility_name": "PG&E", "state": "CA"},
        ]
        updated, mapping = normalize_all_utilities(records)
        assert updated[0]["canonical_utility_name"] == updated[1]["canonical_utility_name"]
