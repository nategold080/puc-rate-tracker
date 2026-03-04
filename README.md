# State PUC Rate Case Tracker

Cross-linked database of utility rate case filings and decisions across state public utility commissions. The first comprehensive free tool that aggregates rate case docket data — filings, decisions, requested vs. approved revenue changes, utility type, and case timelines — across multiple state PUCs into a single searchable, structured database.

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Run full pipeline (scrapes OR, MO, CT, GA live)
python -m src.cli pipeline

# Run enrichment (EIA 861, EIA 860, EPA eGRID, consumer impact estimates)
python -m src.cli enrich

# Export data
python -m src.cli export

# Launch dashboard
python -m src.cli dashboard
```

## Coverage

### Live Data (4 states)

| Source | Records | Type |
|--------|---------|------|
| Missouri PSC | 590 | Live scraped from EFIS |
| Oregon PUC | 582 | Live scraped from EDOCKETS |
| Connecticut PURA | 114 | Live scraped from Lotus Notes/Domino |
| Georgia PSC | 8 | Live scraped from FACTS |
| **Total** | **1,294** | **4 states with real data** |

### In Development (4 additional states)

Scraper stubs exist for Pennsylvania PUC, California CPUC, Indiana IURC, and Washington UTC but do not yet produce real data. These PUC websites use JavaScript-heavy interfaces or return 403 errors, requiring browser-based rendering (Selenium/Playwright) or alternative approaches. See PROBLEMS.md P26 for details.

## Key Features

- **Cross-state entity resolution:** Links utility identities across PUC systems using canonical name mapping, suffix stripping, abbreviation expansion, and fuzzy matching
- **Case type classification:** Rule-based classification into general rate cases, distribution/transmission cases, fuel cost adjustments, infrastructure riders, decoupling mechanisms, and rate design proceedings
- **Quality scoring:** Every record scored 0.0-1.0 on 12 weighted components (avg: 0.677, 100% above threshold)
- **EIA/EPA enrichment:** Cross-links utilities to EIA Form 861 (customers, revenue, sales), EIA Form 860 (generation capacity by fuel type), and EPA eGRID (emissions data)
- **Consumer impact estimates:** Calculates estimated monthly bill impact per customer for cases with financial and customer data
- **Zero LLM dependency:** All extraction is deterministic/rule-based
- **Interactive dashboard:** Streamlit app with 11 analytical sections including enrichment views

## Project Structure

```
├── src/
│   ├── cli.py                 # Click CLI
│   ├── scrapers/              # One per state PUC + EIA/EPA enrichment
│   ├── extractors/            # Rule-based extraction
│   ├── normalization/         # Entity resolution + cross-linking
│   ├── validation/            # Schemas + quality scoring
│   ├── storage/               # SQLite database
│   ├── export/                # CSV/JSON/Excel/Markdown
│   └── dashboard/             # Streamlit app
├── config/                    # YAML configs
├── tests/                     # 276 tests
├── data/                      # Database + exports
├── docs/                      # Methodology
└── outreach/                  # Target lists + pitch materials
```

## Exports

- **CSV** — Full dataset, filterable in any spreadsheet
- **JSON** — Machine-readable with metadata and unit documentation
- **Excel** — 3-sheet workbook (all cases, utilities, financial summary)
- **Markdown** — Summary statistics report

## Dashboard

11-section interactive dashboard:
1. National Overview — KPI cards and summary metrics
2. Rate Case Explorer — Search/filter by state, utility, case type, date range
3. Utility Analysis — Cross-utility comparison charts
4. Rate Change Tracker — Requested vs. approved revenue analysis
5. Geographic Map — Choropleth map of cases by state
6. Timeline View — Filing trends and case duration analysis
7. Case Deep Dive — Individual case detail view
8. Utility Operations — EIA 861 customer and revenue data
9. Emissions Profile — EPA eGRID emissions data by utility
10. Generation Mix — EIA 860 capacity data by fuel type
11. Consumer Impact — Estimated monthly bill impacts

## Tests

```bash
python -m pytest tests/ -v
# 276 tests covering scrapers, extractors, normalization,
# validation, storage, export, enrichment, and pipeline integration
```

## Data Units

- Revenue values (`requested_revenue_change`, `approved_revenue_change`) are in **millions of USD**
- Rate base values are in **millions of USD**
- Return on equity is a **percentage** (e.g., 10.39 = 10.39%)
- EIA 861 revenue is in **thousands of USD** (per EIA reporting convention)
- Capacity values are in **megawatts (MW)**
- Emissions are in **tons** (CO2, NOx, SO2)

---

Built by Nathan Goldberg | nathanmauricegoldberg@gmail.com | [LinkedIn](https://www.linkedin.com/in/nathan-goldberg-62a44522a/)
