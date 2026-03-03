# State PUC Rate Case Tracker

## Project Overview
Cross-linked database of utility rate case filings and decisions across state public utility commissions. The first comprehensive free tool that aggregates rate case docket data — filings, decisions, requested vs. approved revenue changes, utility type, and case timelines — across multiple state PUCs into a single searchable, structured database.

## Core Value Proposition
- **S&P Global / Regulatory Research Associates (RRA)** tracks rate cases but charges six-figure enterprise subscriptions
- **EIA and FERC** publish aggregate statistics but not case-level structured data
- **Individual PUC docket systems** are searchable one-at-a-time with inconsistent interfaces and no cross-state normalization
- **Nobody** has built a free, unified, cross-state structured database linking utility identities across PUC systems with standardized case type classification and outcome tracking

## Data Sources

| Source | System | Key Fields | URL |
|--------|--------|------------|-----|
| Pennsylvania PUC | Public docket search | Docket number, utility name, filing date, case type, status, decision date | www.puc.pa.gov |
| Oregon PUC | EDOCKETS system | Docket ID, utility, filing type, dates, orders, revenue request | apps.puc.state.or.us/edockets |
| California CPUC | CPUC Docket/Proceedings | Proceeding number, utility, application type, filed/decision dates, revenue impact | apps.cpuc.ca.gov/apex/f?p=401 |
| Indiana IURC | Online docket search | Cause number, utility name, case type, filing/order dates | iurc.portal.in.gov |
| Washington UTC | Open docket system | Docket number, company, service type, filing date, order date, status | www.utc.wa.gov/casedocket |
| Connecticut PURA | Lotus Notes/Domino docket system | Docket number, utility, filing date, utility type, case type | www.dpuc.state.ct.us |
| Missouri PSC | EFIS case detail system | Case number, utility, filing date, decision date, utility type, status | efis.psc.mo.gov |
| Georgia PSC | FACTS docket system | Docket ID, utility, filing year, case type, utility type | psc.ga.gov |

### Source-Specific Details

**Pennsylvania PUC:**
- Base URL: https://www.puc.pa.gov/search/document-search/
- Rate cases use docket prefix "R-" (e.g., R-2024-3046894)
- Also covers water, gas, electric, telecommunications, and wastewater
- Decision documents (opinions and orders) are PDFs linked from docket pages
- Historical data availability: 2010+

**Oregon PUC:**
- EDOCKETS URL: https://apps.puc.state.or.us/edockets/
- Rate cases filed as "UE" (electric), "UG" (gas), "UW" (water) dockets
- Structured docket listing with filing dates, descriptions, and linked documents
- Staff reports and commission orders available as PDFs
- Historical data availability: 2005+

**California CPUC:**
- Proceedings search: https://apps.cpuc.ca.gov/apex/f?p=401:56
- Application numbers formatted as A.YY-MM-NNN (e.g., A.23-05-010)
- General Rate Cases (GRC) filed on 3-year cycles for large IOUs
- Detailed proceeding records with all filed documents, rulings, and decisions
- Revenue requirement data in testimony documents
- Historical data availability: 2000+

**Indiana IURC:**
- Portal: https://iurc.portal.in.gov/legal-case-search/
- Cause numbers formatted as IURC Cause No. NNNNN (e.g., 45990)
- Covers electric, gas, water, sewer, and telecommunications
- Order documents are PDFs
- Historical data availability: 2012+

**Washington UTC:**
- Docket search: https://www.utc.wa.gov/casedocket/
- Docket format: UE-NNNNNN (electric), UG-NNNNNN (gas), UW-NNNNNN (water)
- Rate case filings include initial filing, staff response, settlement, and final order
- Historical data availability: 2008+

## Technical Standards
- Python 3.12+, SQLite WAL mode, Click CLI, Streamlit + Plotly
- Zero LLM dependency for core pipeline
- Quality scoring on every record (weighted 0.0-1.0)
- All data from public PUC sources
- Dark theme dashboard: primaryColor="#0984E3", backgroundColor="#0E1117"
- Footer: "Built by Nathan Goldberg" + nathanmauricegoldberg@gmail.com + LinkedIn
- Contact in User-Agent headers: nathanmauricegoldberg@gmail.com

## Entity Resolution Strategy

### Primary Join Key
- No universal utility identifier exists across states. Entity resolution is the core technical challenge.

### Utility Company Name Normalization
1. **Canonical name mapping:** Maintain a YAML lookup of known utility aliases mapping to canonical names.
   - "PPL Electric Utilities" / "PPL Electric Utilities Corporation" / "PPL EU" -> "PPL Electric Utilities"
   - "Pacific Gas & Electric" / "Pacific Gas and Electric Company" / "PG&E" -> "Pacific Gas and Electric Company"
   - "Southern California Edison" / "SCE" / "So. Cal. Edison" -> "Southern California Edison Company"
2. **Suffix stripping:** Remove common suffixes ("Inc.", "Corp.", "Corporation", "Company", "Co.", "LLC", "LP", "L.P.") before fuzzy matching.
3. **Abbreviation expansion:** Expand "&" to "and", "Elec." to "Electric", "Utils." to "Utilities", "So." to "Southern", "No." to "Northern".
4. **Fuzzy matching threshold:** Use thefuzz with token_sort_ratio >= 85 for unresolved names after canonical lookup and normalization.
5. **Parent company linkage:** Map subsidiaries to parent holding companies (e.g., PPL Electric -> PPL Corporation, SCE -> Edison International).

### Cross-State Linking
- EIA Utility ID (from EIA-861) used as a secondary identifier when available
- FERC respondent ID for utilities that file at FERC
- Holding company grouping enables portfolio-level analysis

## Quality Scoring Formula
Each rate case record scored 0.0-1.0:
- has_docket_number: 0.15
- has_utility_name_resolved: 0.15
- has_case_type_classified: 0.10
- has_filing_date: 0.10
- has_decision_date: 0.10
- has_revenue_request_amount: 0.15
- has_revenue_approved_amount: 0.15
- has_case_status: 0.05
- has_source_url: 0.05

## Build Order
1. Config files (sources.yaml, rate_case_types.yaml, utility_aliases.yaml)
2. Scrapers — One per state PUC. HTML parsing with httpx + lxml/BeautifulSoup. Rate limited.
3. Extractors — Rule-based regex extraction of docket numbers, dates, dollar amounts, case types from docket pages
4. Normalization — Utility entity resolution, case type classification, dollar amount standardization
5. Validation — Pydantic schemas, quality scoring, deduplication
6. Storage — SQLite schema with tables: rate_cases, utilities, decisions, revenue_impacts
7. Pipeline — Wire scrape -> extract -> normalize -> validate -> store end-to-end
8. Run pipeline against PA, OR, CA (minimum), then expand to IN, WA
9. Exports — CSV, JSON, Excel, Markdown stats
10. Dashboard — 6+ interactive sections
11. Methodology doc — 1-page PDF
12. Tests — 50+ covering all stages

## Dashboard Sections (planned)
1. **National Overview** — KPI cards: total rate cases tracked, total states, total revenue requested, avg approval rate, active cases count
2. **Rate Case Explorer** — Search/filter by state, utility, case type, date range, status, revenue range
3. **Utility Profile** — Individual utility deep-dive: all filings, approval rates, average case duration, revenue trend
4. **Rate Case Outcomes** — Requested vs. approved revenue analysis, approval percentage distribution, over-time trends
5. **State Comparison** — Cross-state metrics: avg case duration, approval rates, cases per year, regulatory stringency index
6. **Timeline Analysis** — Case duration analysis, filing-to-decision timelines, seasonal filing patterns
7. **Revenue Impact** — Dollar-value analysis: total revenue changes by state, by utility type, by year

## Target Audiences
- **Energy consulting firms** (Brattle Group, Concentric Energy Advisors, ScottMadsen) — need rate case benchmarking data
- **Utility companies** — track competitor filings and regulatory trends
- **Consumer advocates / ratepayer groups** (AARP, state consumer advocates) — monitor rate increases
- **Investment analysts** covering regulated utilities (Morningstar, S&P Global Market Intelligence)
- **Academic researchers** studying utility regulation (MIT CEEPR, Resources for the Future, NBER)
- **Law firms** practicing utility/energy regulatory law (Van Ness Feldman, Steptoe, Troutman Pepper)
- **State legislators and staff** on energy/utility committees
- **Journalists** covering energy/utility regulation (Utility Dive, E&E News, S&P Global Platts)
