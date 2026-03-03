# State PUC Rate Case Tracker

Cross-linked database of utility rate case filings and decisions across state public utility commissions. The first comprehensive free tool that aggregates rate case docket data — filings, decisions, requested vs. approved revenue changes, utility type, and case timelines — across multiple state PUCs into a single searchable, structured database.

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Load seed data + real scraped data
python -m src.cli seed

# Or run full pipeline (scrapes OR PUC live, uses seed data for other states)
python -m src.cli pipeline

# Export data
python -m src.cli export

# Launch dashboard
python -m src.cli dashboard
```

## Coverage

| Source | Records | Type |
|--------|---------|------|
| Oregon PUC | 582 | Live scraped from EDOCKETS |
| Pennsylvania PUC | 19 | Structured reference data |
| California CPUC | 10 | Structured reference data |
| Indiana IURC | 5 | Structured reference data |
| Washington UTC | 5 | Structured reference data |
| **Total** | **621** | **5 states, 467 utilities** |

## Key Features

- **Cross-state entity resolution:** Links utility identities across PUC systems using canonical name mapping, suffix stripping, abbreviation expansion, and fuzzy matching
- **Case type classification:** Rule-based classification into general rate cases, distribution/transmission cases, fuel cost adjustments, infrastructure riders, decoupling mechanisms, and rate design proceedings
- **Quality scoring:** Every record scored 0.0-1.0 on 9 weighted components (avg: 0.670, 100% above threshold)
- **Zero LLM dependency:** All extraction is deterministic/rule-based
- **Interactive dashboard:** Streamlit app with 7 analytical sections

## Project Structure

```
├── src/
│   ├── cli.py                 # Click CLI
│   ├── scrapers/              # One per state PUC
│   ├── extractors/            # Rule-based extraction
│   ├── normalization/         # Entity resolution
│   ├── validation/            # Schemas + quality scoring
│   ├── storage/               # SQLite database
│   ├── export/                # CSV/JSON/Excel/Markdown
│   └── dashboard/             # Streamlit app
├── config/                    # YAML configs
├── tests/                     # 186 tests
├── data/                      # Database + exports
├── docs/                      # Methodology
└── outreach/                  # Target lists + pitch materials
```

## Exports

- **CSV** — Full dataset, filterable in any spreadsheet
- **JSON** — Machine-readable for API integration
- **Excel** — 3-sheet workbook (all cases, by state, financial summary)
- **Markdown** — Summary statistics report

## Dashboard

7-section interactive dashboard:
1. National Overview — KPI cards and summary metrics
2. Rate Case Explorer — Search/filter by state, utility, case type, date range
3. Utility Analysis — Cross-utility comparison charts
4. Rate Change Tracker — Requested vs. approved revenue analysis
5. Geographic Map — Choropleth map of cases by state
6. Timeline View — Filing trends and case duration analysis
7. Case Deep Dive — Individual case detail view

## Tests

```bash
python -m pytest tests/ -v
# 186 tests covering scrapers, extractors, normalization,
# validation, storage, export, and pipeline integration
```

---

Built by Nathan Goldberg | nathanmauricegoldberg@gmail.com | [LinkedIn](https://linkedin.com/in/nathanmauricegoldberg)
