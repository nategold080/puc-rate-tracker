# Methodology — State PUC Rate Case Tracker

## Overview

This dataset aggregates utility rate case filings and decisions from state public utility commissions (PUCs) into a single, cross-state searchable database. Rate cases are the formal regulatory proceedings through which utilities request permission to change the rates they charge customers. No free, unified, cross-state structured database of rate case docket data has previously existed — this project fills that gap.

## Data Sources

| Source | Records | System | Scraping Method | URL |
|--------|---------|--------|-----------------|-----|
| Missouri PSC | 590 | EFIS case detail | Live scraped (ID enumeration) | efis.psc.mo.gov |
| Oregon PUC | 582 | EDOCKETS search | Live scraped (POST search) | apps.puc.state.or.us/edockets/ |
| Connecticut PURA | 115 | Lotus Notes/Domino | Live scraped (SearchView + OpenView) | www.dpuc.state.ct.us |
| Pennsylvania PUC | 19 | Document search | Seed data (JS-rendered site) | www.puc.pa.gov |
| California CPUC | 10 | CPUC Proceedings | Seed data (JS-rendered site) | apps.cpuc.ca.gov/apex/f?p=401 |
| Georgia PSC | 8 | FACTS docket | Live scraped (major cases + docket pages) | psc.ga.gov |
| Indiana IURC | 5 | Online docket | Seed data (JS-rendered site) | iurc.portal.in.gov |
| Washington UTC | 5 | Open docket | Seed data (403 Forbidden) | www.utc.wa.gov/casedocket |

### Oregon PUC (Live Scraped)

The Oregon PUC EDOCKETS system provides accessible HTML search results for rate cases. The scraper submits a POST request to `apps.puc.state.or.us/edockets/srchlist.asp` with `case_type=rate` and parses the returned HTML table. Each row yields:

- Docket number (UE/UG/UM/UC/UW prefix indicating utility type)
- Utility name
- Filing date and decision date (when available)
- Docket prefix classification into electric, gas, water, multi-service, or telecom

Results are deduplicated by docket number and cached as JSON for subsequent runs.

### Missouri PSC (Live Scraped)

The Missouri PSC EFIS system renders case detail pages as server-side HTML. The scraper enumerates known case ID ranges for electric (ER), gas (GR), and water (WR) rate cases, plus known 2024+ case IDs. Each page is parsed for case number, utility name (from Subject Companies section), status, filing date, and case type.

### Connecticut PURA (Live Scraped)

The Connecticut PURA uses a Lotus Notes/Domino docket system. The scraper searches for rate-related keywords via SearchView, extracts bracket-format docket references [YY-MM-NN], and browses the docket listing via ExpandView to collect titles. Rate cases are filtered using utility name matching and rate-related keyword validation.

### Georgia PSC (Live Scraped)

The Georgia PSC FACTS system provides a major cases listing page. The scraper discovers docket IDs from that page, expands the search to nearby IDs, and fetches individual docket pages for titles. Only pages containing rate case keywords are retained.

### Other State Sources

Pennsylvania, California, Indiana, and Washington PUCs use JavaScript-heavy search interfaces that require browser-based rendering. For these states, the database contains structured records based on publicly filed rate cases. As PUC interfaces evolve or provide API access, live scraping will be extended.

## Entity Resolution

### Primary Challenge

No universal utility identifier exists across state PUCs. The same utility company may appear under different names across filings (e.g., "Pacific Gas & Electric" vs. "PG&E" vs. "Pacific Gas and Electric Company").

### Resolution Approach

1. **Canonical name mapping:** A YAML lookup maps known aliases to canonical names (e.g., "PPL Electric Utilities Corporation" → "PPL Electric Utilities").
2. **Suffix stripping:** Common suffixes (Inc., Corp., Company, Co., LLC, LP) are stripped before matching.
3. **Abbreviation expansion:** Standard abbreviations are expanded ("&" → "and", "Elec." → "Electric", etc.).
4. **Fuzzy matching:** Unresolved names are matched using token sort ratio (threshold ≥ 85) via the thefuzz library.
5. **Cross-state linking:** Utilities operating in multiple states (e.g., PacifiCorp in OR and WA, Cascade Natural Gas in OR and WA) are identified and linked.

## Case Type Classification

Rate cases are classified using pattern matching against docket numbers and descriptions:

| Type | Pattern Examples |
|------|-----------------|
| General Rate Case | "general rate", "base rate", GRC |
| Distribution Rate Case | "distribution", DSIC, infrastructure rider |
| Transmission Rate Case | "transmission", FERC rate |
| Fuel Cost Adjustment | "fuel clause", "energy cost", FAC, PCAM |
| Infrastructure Rider | "DSIC", "infrastructure", "system improvement" |
| Decoupling Mechanism | "decoupling", "revenue adjustment", "lost revenue" |
| Rate Design | "rate design", "rate structure", "time-of-use" |

## Utility Type Classification

Utility service type is determined from docket number prefixes and keyword matching:

| Type | Docket Prefixes | Keywords |
|------|----------------|----------|
| Electric | UE- | electric, power, energy |
| Gas | UG- | gas, natural gas |
| Water | UW- | water, sewer |
| Multi-Service | UM- | combined, multi-service |
| Telecommunications | UC- | telecom, telephone |

## Quality Scoring

Each rate case record receives a quality score from 0.0 to 1.0:

| Component | Weight | Criteria |
|-----------|--------|----------|
| Docket number | 0.15 | Docket number is present |
| Utility name resolved | 0.15 | Canonical name assigned (full credit) or raw name present (partial) |
| Case type classified | 0.10 | Case type is not "unknown" |
| Filing date | 0.10 | Filing date is present |
| Decision date | 0.10 | Decision date present; partial credit for active cases |
| Revenue request amount | 0.15 | Requested revenue change amount available |
| Revenue approved amount | 0.15 | Approved revenue change amount available |
| Case status | 0.05 | Status is not "unknown" |
| Source URL | 0.05 | Direct URL to source document |

Current performance: Average score 0.677, 99.9% of records above 0.6 threshold (1,333 of 1,334).

## Data Validation

- **Required fields:** Every record must have docket number, utility name, state, and source
- **Date ranges:** Filing and decision dates validated within 1990-2030
- **Date ordering:** Decision date must not precede filing date
- **Financial sanity:** Revenue amounts validated ≤ $50B; approved generally ≤ 150% of requested
- **ROE range:** Return on equity validated 0-25%
- **Deduplication:** Records keyed on (docket_number, source) — no duplicates within a source

## Coverage

- **States:** 8 (OR, MO, CT, GA, PA, CA, IN, WA)
- **Total rate cases:** 1,334
- **Unique utilities:** 568
- **Live-scraped sources:** 4 (OR PUC, MO PSC, CT PURA, GA PSC)
- **Date range:** 1990 to 2026
- **Cases with revenue data:** 39 (from states providing financial details)
- **Total revenue requested:** $23.0B across tracked cases
- **Total revenue approved:** $10.5B across tracked cases
- **Average ROE:** 10.39%
- **Average quality score:** 0.677

## Limitations

1. **JavaScript-rendered PUC sites.** Some state PUC websites (PA, CA, IN) use dynamic JavaScript rendering that prevents simple HTTP scraping. WA UTC returns 403 Forbidden. Four states (OR, MO, CT, GA) have live scrapers; remaining states use structured reference data.

2. **Financial data availability varies.** Revenue request/approval amounts require parsing detailed filings or order documents, which are often PDFs. The current pipeline extracts docket-level metadata; financial data enrichment is an ongoing effort.

3. **No universal utility identifier.** Entity resolution across states relies on name normalization and fuzzy matching, which may miss some connections or create false matches for similarly named utilities.

4. **Historical depth varies by state.** Oregon data extends back to 1990; other states have shorter windows depending on their electronic filing system history.

5. **Rate case classification is pattern-based.** The rule-based classifier may miscategorize unusual filings that don't match standard patterns.

## Technical Implementation

- **Zero LLM dependency:** All extraction is deterministic/rule-based
- **Pipeline:** Scrape → Extract → Normalize → Validate → Store
- **Storage:** SQLite with WAL mode for concurrent read access
- **Exports:** CSV, JSON, Excel (3 sheets), Markdown
- **Dashboard:** Interactive Streamlit app with 6+ analytical sections

---

Built by Nathan Goldberg | nathanmauricegoldberg@gmail.com | [LinkedIn](https://linkedin.com/in/nathanmauricegoldberg)
