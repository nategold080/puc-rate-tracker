# Problems Tracker — State PUC Rate Case Tracker

## P1: OR PUC "Met Retention" suffix filtering all records — DONE
**Problem:** The OR PUC scraper was filtering out all records containing "Met Retention" in the utility name, removing 777/1029 records. "Met Retention" is a docket retention metadata annotation, not a reason to exclude.
**Fix:** Changed from filtering records to stripping the " - Met Retention" suffix from utility names with `re.sub(r"\s*-\s*Met Retention\s*$", "", utility_name)`.

## P2: Quality scoring too penalizing for docket-level data — DONE
**Problem:** Original quality weights gave 25% to financial data and 10% to documents. Basic docket search results from PUC websites (which provide docket number, utility name, dates, status) could never exceed 0.55, below the 0.6 threshold. Only 39/621 records passed.
**Fix:** Updated quality scoring to match the CLAUDE.md specification with 9 granular components. Records with complete docket-level data now score 0.6+. All 621 records (100%) now above threshold.

## P3: PA/CA/IN/WA PUC sites require JavaScript rendering — DONE (workaround)
**Problem:** Pennsylvania, California, Indiana, and Washington PUC websites use JavaScript-heavy interfaces (Oracle APEX, Angular, React) that return empty or template HTML via plain HTTP requests.
**Workaround:** OR PUC EDOCKETS is accessible and provides 582 real records. Other states use structured reference data based on publicly filed cases. The architecture supports adding live scrapers as PUC interfaces evolve.

## P4: OR PUC [PDF] artifacts in docket numbers — DONE
**Problem:** Some OR PUC docket numbers contained "[PDF]" artifacts from HTML parsing of linked text.
**Fix:** Added `re.sub(r"\[PDF\]", "", docket).strip()` to the parser.

## P5: Cross-state utility identity (Cascade Natural Gas, PacifiCorp) — DONE
**Problem:** Referential integrity check flagged utilities appearing in multiple states as potential issues. However, Cascade Natural Gas (OR, WA) and PacifiCorp (OR, WA) are genuinely multi-state utilities.
**Resolution:** These are correct cross-state entities, not data errors. The entity resolution system properly links them. Documented as expected behavior.

## P6: WA UTC returns 403 Forbidden — DONE (documented)
**Problem:** Washington UTC website (utc.wa.gov) returns 403 Forbidden for all HTTP requests.
**Resolution:** WA UTC may require specific headers, cookies, or has bot protection. Documented as a limitation. Using structured reference data for WA.

## P8: Only 1 state (OR) with real scraped data — DONE
**Problem:** 582/621 records came from Oregon PUC EDOCKETS. Only 39 records were structured reference data from PA/CA/IN/WA (not live scraped). This makes the tracker essentially a single-state tool.
**Fix:** Built live scrapers for 3 additional accessible PUC systems:
- **CT PURA** (Lotus Notes/Domino) — SearchView + OpenView scraping, 115 rate cases
- **MO PSC** (EFIS) — Case ID enumeration across known ranges (273 ER + 192 GR + 120 WR + 5 known 2024+), 590 rate cases
- **GA PSC** (FACTS/Drupal) — Major cases listing + docket page scraping, 8 rate cases
Total: 1,334 records across 8 states (4 with real scraped data: OR, MO, CT, GA). 568 unique utilities. Average quality 0.677. 214 tests passing.

## P7: Test failure after quality scoring update — DONE
**Problem:** `test_documents_contribute` test expected documents to increase quality score, but the updated scoring formula replaced the documents component with source_url.
**Fix:** Updated test to `test_source_url_contributes` and fixed component name references in active case test (`has_decision_data` → `has_decision_date`).

## P9: Quality scoring weight redistribution for enrichment — DONE
**Problem:** Adding enrichment components (has_eia_data_linked: 0.10, has_emissions_data: 0.05, has_customer_impact: 0.05) required redistributing weights. Old 9-component weights summed to 1.0; new 12-component weights needed rebalancing.
**Fix:** Reduced original component weights proportionally (e.g., docket 0.15→0.12, utility name 0.15→0.10) so all 12 components sum to 1.0. Updated test_complete_case_high_score threshold from >=0.9 to >=0.7 (max without enrichment is ~0.80). Added test_complete_case_with_enrichment to verify full score with enrichment.

## P10: EIA 861 Excel column name variations across years — DONE
**Problem:** EIA Form 861 Excel files use different column naming conventions across years (e.g., "Residential Customers" vs "RESIDENTIAL.Customers" vs "Customers.Residential"). Some years use merged header rows.
**Fix:** Built SALES_COLUMN_MAPS with multiple aliases per field and a _find_column function that tries exact matches, then case-insensitive matches, then substring matching. Also handles merged header rows by detecting sub-header patterns.

## P11: EIA 860 column index 0 treated as falsy — DONE
**Problem:** `_find_col(["utility", "id"]) or _find_col(["utility", "number"])` used Python `or` to chain fallback column searches. If the first pattern matched column index 0, `0 or _find_col(...)` would incorrectly skip to the fallback because `0` is falsy in Python. This could silently use the wrong column for the utility ID.
**Fix:** Replaced `or` chaining with `_find_col_fallback()` that uses explicit `is not None` checks, correctly handling column index 0.

## P12: EIA 860 zero fuel capacity stored as NULL — DONE
**Problem:** In `parse_eia_860()`, fuel capacity values used `round(d["coal_mw"], 2) if d["coal_mw"] else None`, which converted `0.0` (legitimate zero coal capacity) to `None`. For journalistic accuracy, 0 MW (confirmed zero) is semantically different from NULL (unknown/missing). The `_score_capacity` function also used `any(rec.get(f"{f}_capacity_mw") for ...)` which treated `0.0` as falsy.
**Fix:** Removed falsy checks; all fuel capacities now always stored as their real value (including 0.0). Score check changed to `is not None`.

## P13: Emissions enrichment check was per-state, not per-utility — DONE
**Problem:** In `validate_all()`, the emissions enrichment flag checked `state in emission_states`, meaning ALL utilities in a state with ANY eGRID data would get `has_emissions = True` — even utilities without their own emissions data. This inflated quality scores.
**Fix:** Changed to per-utility check: looks up the utility's EIA ID via cross-links, then checks if that specific ID has emissions data in the eGRID table.

## P14: Quality scoring docstring listed stale weights — DONE
**Problem:** Module docstring in quality.py still listed the original 9-component weights (0.15, 0.15, etc.) after the redistribution to 12 components.
**Fix:** Updated docstring to match actual WEIGHTS dict (12 components summing to 1.0).

## P15: Markdown export missing blank line before enrichment heading — DONE
**Problem:** In `_export_markdown()`, the "## Enrichment Data" heading immediately followed the rate cases table with no blank line, breaking Markdown rendering (the heading would be interpreted as table content).
**Fix:** Added `lines.append("")` before the enrichment section heading.

## P16: EIA 861 _safe_int inconsistent string processing — DONE
**Problem:** `_safe_int()` stripped periods from the check string (`replace(".", "")`) but not from the conversion path, making the sentinel check for "." dead code (it was already stripped). Logic paths were inconsistent.
**Fix:** Unified to use a single cleaned string `s` (strips commas only) for both the sentinel check and the conversion.

## P17: HTML entities in MO PSC and CT PURA data — DONE
**Problem:** 97 rate_cases.utility_name values, 21 canonical names, and 486 descriptions contained unescaped HTML entities (`&amp;` instead of `&`, `&#x2019;` instead of `'`, etc.). Missouri PSC EFIS returns HTML-encoded text; CT PURA context extraction captured raw HTML fragments. These would display incorrectly in all exports and dashboards.
**Fix:** Added `html.unescape()` to MO PSC (`_parse_case_html`, `_extract_companies`) and CT PURA (context extraction, title extraction) scrapers. Cleaned all existing DB records. Added `_sanitize_record()` safety net in exporter.

## P18: Bogus CT PURA record 00-00-01 — DONE
**Problem:** Docket `00-00-01` from Connecticut PURA was a bad parse — placeholder docket number, NULL filing_date, HTML garbage in description.
**Fix:** Deleted from database. Added filter in CT PURA scraper to skip placeholder docket numbers (`00-00-01`, `00-00-00`).

## P19: OR PUC UM 228 decision date precedes filing date — DONE
**Problem:** Oregon docket `UM 228` had `decision_date=1990-09-27` but `filing_date=1993-08-01` — chronologically impossible. A related prior decision date was captured instead of the correct one.
**Fix:** Set decision_date to NULL for this record.

## P20: README.md stale after scraper expansion — DONE
**Problem:** README claimed 621 records, 5 states, 467 utilities, 186 tests — all wrong after adding CT, MO, GA scrapers and enrichment. Would immediately undermine credibility with any reviewer.
**Fix:** Rewrote README with accurate counts (1,333 records, 8 states, 568 utilities, 276 tests), added enrichment documentation, data units section.

## P21: Methodology.md listed old 9-component quality weights — DONE
**Problem:** Quality scoring section in methodology.md listed original 9 weights (0.15, 0.15, etc.) instead of the current 12-component system with enrichment weights.
**Fix:** Updated to show all 12 components with correct weights.

## P22: Media pitch referenced Carolinas (not in dataset) — DONE
**Problem:** Media email template suggested comparing Duke Energy filings "in Indiana and the Carolinas" — but NC/SC are not in the dataset. Would backfire if a journalist tried the comparison.
**Fix:** Changed to PacifiCorp Oregon vs. Washington comparison (both states are in the dataset).

## P23: Client email said "5 states" — DONE
**Problem:** Client pitch email subject line option referenced "5 states" instead of 8.
**Fix:** Updated to "8 states".

## P24: JSON export lacked unit metadata — DONE
**Problem:** Revenue values in JSON/CSV exports had no unit indication. A value of `3600.0` could be interpreted as dollars or millions. Dangerous ambiguity for journalistic use.
**Fix:** Added `data_units` dictionary to JSON metadata specifying units for all numeric fields.

## P25: Utility names truncated to 30 chars in markdown export — DONE
**Problem:** Largest rate cases table in markdown summary truncated utility names at 30 characters, cutting off names like "Pacific Gas and Electric Compa" (missing "ny").
**Fix:** Increased truncation limit to 45 characters.

## P26: PA PUC, CA CPUC, IN IURC, WA UTC scrapers need real implementation — OPEN
**Problem:** Four state scrapers exist as stubs but produce 0 real records. Previous seed/reference data for these states has been removed. The project currently has 1,294 real records from 4 states (MO, OR, CT, GA). Each stub has specific technical challenges:
- **PA PUC** (www.puc.pa.gov): Document search uses JavaScript rendering. Likely requires Selenium or Playwright for browser-based scraping.
- **CA CPUC** (apps.cpuc.ca.gov/apex/f?p=401): Built on Oracle APEX, a complex JavaScript application framework. May need Playwright or discovery of underlying API endpoints.
- **IN IURC** (iurc.portal.in.gov): JavaScript-heavy portal. Needs proper scraper development, potentially with browser automation.
- **WA UTC** (www.utc.wa.gov/casedocket): Returns 403 Forbidden for standard HTTP requests (see P6). May require specific headers, cookies, or a different access approach.
**Status:** Stubs remain in `src/scrapers/` for future development. Expanding to these states would significantly increase coverage.
