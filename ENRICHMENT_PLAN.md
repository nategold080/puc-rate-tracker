# PUC Rate Tracker — Data Enrichment Plan

## Overview
This enrichment transforms the PUC Rate Tracker from a "docket database" into a **utility performance and consumer impact platform** by adding EIA operational data, emissions profiles, and financial performance metrics. Currently only 39/1,334 records (2.9%) have financial data — this enrichment fills that critical gap.

**Current state:** 1,334 rate cases, 568 utilities, 8 states — but almost no financial data, no customer counts, no emissions data, and no utility operational metrics.

**Target state:** Utilities enriched with customer counts, electricity prices, generation mix, emissions profiles, and financial performance from EIA and eGRID data.

---

## Enrichment 1: EIA Form 861 — Utility Operational Data (HIGHEST PRIORITY)

### What It Adds
- Customer counts by utility (residential, commercial, industrial)
- Revenue by customer class
- Average electricity prices (cents/kWh) by utility
- Energy sales (MWh) by customer class
- Utility service territory information
- Ownership type (investor-owned, municipal, cooperative)

### Data Source
- **EIA Form 861 Annual Data** (FREE, no authentication for downloads)
- Download: `https://www.eia.gov/electricity/data/eia861/zip/f861{year}.zip`
- Format: ZIP containing multiple Excel/CSV files
- Key file: `Sales_Ult_Cust_{year}.xlsx` (sales to ultimate customers)
- Additional: `Operational_Data_{year}.xlsx`, `Utility_Data_{year}.xlsx`
- Coverage: All 3,300+ electric utilities in the US, annual
- Join key: Utility name + state (map to PUC utility_name/canonical_utility_name)
- EIA Utility ID: Can be stored in the existing `eia_utility_id` field in the utilities table (currently empty)
- Years available: 1990-present (annual)

### Key Fields to Extract
```python
EIA_861_FIELDS = {
    'utility_number': 'eia_utility_id',      # EIA's unique identifier
    'utility_name': 'name',
    'state': 'state',
    'ownership': 'ownership_type',            # IOU, Municipal, Coop, etc.
    'residential_customers': 'res_customers',
    'commercial_customers': 'com_customers',
    'industrial_customers': 'ind_customers',
    'total_customers': 'total_customers',
    'residential_revenue': 'res_revenue_thousands',
    'commercial_revenue': 'com_revenue_thousands',
    'industrial_revenue': 'ind_revenue_thousands',
    'residential_sales_mwh': 'res_sales_mwh',
    'commercial_sales_mwh': 'com_sales_mwh',
    'industrial_sales_mwh': 'ind_sales_mwh',
    'residential_avg_price': 'res_avg_price_cents_kwh',
    'commercial_avg_price': 'com_avg_price_cents_kwh',
    'industrial_avg_price': 'ind_avg_price_cents_kwh',
}
```

### Database Schema Additions

```sql
-- EIA utility operational data
CREATE TABLE utility_operations (
    eia_utility_id INTEGER NOT NULL,
    year INTEGER NOT NULL,
    utility_name TEXT,
    state TEXT,
    ownership_type TEXT,           -- 'Investor Owned', 'Municipal', 'Cooperative', 'State', 'Federal', 'Political Subdivision'
    -- Customer counts
    residential_customers INTEGER,
    commercial_customers INTEGER,
    industrial_customers INTEGER,
    total_customers INTEGER,
    -- Revenue (thousands of dollars)
    residential_revenue REAL,
    commercial_revenue REAL,
    industrial_revenue REAL,
    total_revenue REAL,
    -- Sales (MWh)
    residential_sales_mwh REAL,
    commercial_sales_mwh REAL,
    industrial_sales_mwh REAL,
    total_sales_mwh REAL,
    -- Average prices (cents/kWh)
    residential_avg_price REAL,
    commercial_avg_price REAL,
    industrial_avg_price REAL,
    avg_price REAL,
    -- Computed
    revenue_per_customer REAL,
    quality_score REAL,
    created_at TEXT DEFAULT (datetime('now')),
    PRIMARY KEY (eia_utility_id, year)
);

-- Link table connecting PUC utilities to EIA utility IDs
CREATE TABLE utility_eia_links (
    utility_name TEXT NOT NULL,        -- from our utilities table
    state TEXT NOT NULL,
    eia_utility_id INTEGER NOT NULL,
    match_confidence REAL,
    match_method TEXT,                 -- 'exact', 'normalized', 'fuzzy'
    PRIMARY KEY (utility_name, state, eia_utility_id)
);

CREATE INDEX idx_operations_utility ON utility_operations(eia_utility_id);
CREATE INDEX idx_operations_year ON utility_operations(year);
CREATE INDEX idx_operations_state ON utility_operations(state);
CREATE INDEX idx_operations_ownership ON utility_operations(ownership_type);
CREATE INDEX idx_eia_links_utility ON utility_eia_links(utility_name, state);
CREATE INDEX idx_eia_links_eia ON utility_eia_links(eia_utility_id);
```

### Cross-Linking Strategy
1. Normalize both PUC utility names and EIA utility names using existing normalization pipeline
2. Match by normalized name + state (exact first, then fuzzy)
3. Store EIA utility ID in the utilities table's existing `eia_utility_id` field
4. For multi-state utilities (e.g., PacifiCorp), aggregate across states

---

## Enrichment 2: EPA eGRID — Utility Emissions Data

### What It Adds
- CO2, NOx, SO2 emissions per utility
- Generation mix (coal, gas, nuclear, renewable percentages)
- Emission rates (lbs CO2/MWh)
- Enables: "rate case impact on emissions" analysis
- Enables: "clean vs. dirty utility" classification

### Data Source
- **EPA eGRID** (FREE direct download)
- Download: `https://www.epa.gov/egrid/download-data` → annual Excel workbooks
- Direct URL pattern: `https://www.epa.gov/system/files/documents/2024-01/egrid2022_data.xlsx`
- Format: Excel with multiple sheets (Plant, Generator, State, Utility, etc.)
- Key sheet: **UTNL** (Utility-level aggregation)
- Key fields: UTLSRVNM (utility name), UTLSRVST (state), UTNGENAN (annual net generation MWh), UTLCO2AN (annual CO2 tons), UTLNOXAN (NOx), UTLSO2AN (SO2), UTLCO2RA (CO2 emission rate lbs/MWh)
- Join key: Utility name + state → match to PUC utilities
- Coverage: All utilities with generation in the US

### Database Schema Addition

```sql
CREATE TABLE utility_emissions (
    utility_name_egrid TEXT NOT NULL,
    state TEXT NOT NULL,
    year INTEGER NOT NULL,
    eia_utility_id INTEGER,
    -- Generation
    net_generation_mwh REAL,
    -- Emissions (annual tons)
    co2_tons REAL,
    nox_tons REAL,
    so2_tons REAL,
    -- Emission rates (lbs/MWh)
    co2_rate_lbs_mwh REAL,
    nox_rate_lbs_mwh REAL,
    so2_rate_lbs_mwh REAL,
    -- Generation mix percentages
    coal_pct REAL,
    gas_pct REAL,
    nuclear_pct REAL,
    hydro_pct REAL,
    wind_pct REAL,
    solar_pct REAL,
    other_renewable_pct REAL,
    quality_score REAL,
    created_at TEXT DEFAULT (datetime('now')),
    PRIMARY KEY (utility_name_egrid, state, year)
);

CREATE INDEX idx_emissions_state ON utility_emissions(state);
CREATE INDEX idx_emissions_co2 ON utility_emissions(co2_rate_lbs_mwh);
CREATE INDEX idx_emissions_eia ON utility_emissions(eia_utility_id);
```

---

## Enrichment 3: EIA Form 860 — Utility Generation Capacity

### What It Adds
- Power plant capacity by utility (MW)
- Plant types (coal, gas, nuclear, wind, solar)
- Plant ages and retirement dates
- Planned capacity additions
- Enables: "rate case to fund new generation" analysis

### Data Source
- **EIA Form 860** (FREE)
- Download: `https://www.eia.gov/electricity/data/eia860/zip/eia860{year}.zip`
- Format: ZIP containing Excel files
- Key files: `3_1_Generator_Y{year}.xlsx` (generator-level data)
- Key fields: Utility ID, Plant Code, Generator ID, Technology, Nameplate Capacity MW, Operating Year, Planned Retirement Year, Status
- Join key: EIA Utility ID (from Enrichment 1 linkage)

### Database Schema Addition

```sql
CREATE TABLE utility_capacity (
    eia_utility_id INTEGER NOT NULL,
    year INTEGER NOT NULL,
    -- Capacity by fuel type (MW)
    coal_capacity_mw REAL,
    gas_capacity_mw REAL,
    nuclear_capacity_mw REAL,
    hydro_capacity_mw REAL,
    wind_capacity_mw REAL,
    solar_capacity_mw REAL,
    other_capacity_mw REAL,
    total_capacity_mw REAL,
    -- Fleet metrics
    num_plants INTEGER,
    num_generators INTEGER,
    avg_generator_age REAL,
    planned_additions_mw REAL,
    planned_retirements_mw REAL,
    quality_score REAL,
    created_at TEXT DEFAULT (datetime('now')),
    PRIMARY KEY (eia_utility_id, year)
);

CREATE INDEX idx_capacity_utility ON utility_capacity(eia_utility_id);
CREATE INDEX idx_capacity_year ON utility_capacity(year);
```

---

## Enrichment 4: Rate Case Financial Context

### What It Adds (builds on Enrichments 1-3)
- Pre/post rate case price changes for affected utilities
- Consumer bill impact estimates (revenue change / customers)
- Rate case decision context: what was the utility's financial position when filing?
- ROE comparison: requested vs. approved vs. industry average

### Computed Metrics
```python
def rate_case_impact(case, utility_ops):
    """Calculate consumer impact of a rate case decision."""
    if case.approved_revenue_change and utility_ops:
        total_customers = utility_ops.total_customers
        if total_customers and total_customers > 0:
            monthly_impact = (case.approved_revenue_change * 1_000_000) / total_customers / 12
            return {
                'monthly_bill_impact': monthly_impact,
                'annual_bill_impact': monthly_impact * 12,
                'pct_of_avg_bill': monthly_impact / (utility_ops.residential_avg_price * avg_monthly_kwh) * 100,
            }
    return None
```

---

## Dashboard Enhancements

### New Tabs
1. **Utility Profiles** — Operational metrics for each utility
   - Customer counts, revenue, average prices
   - Ownership type breakdown (IOU vs. muni vs. coop)
   - Multi-year trend charts

2. **Consumer Impact** — Rate case effects on ratepayers
   - Estimated monthly bill impact per rate case
   - Rate comparison across utilities and states
   - Residential vs. commercial vs. industrial pricing

3. **Environmental Impact** — Emissions and generation mix
   - CO2 emission rates by utility
   - Generation mix pie charts
   - Rate case funding for clean energy vs. fossil
   - Emissions trend over time

4. **Capacity & Infrastructure** — Generation fleet analysis
   - Capacity mix by fuel type
   - Fleet age analysis
   - Planned additions/retirements

### Enhanced Existing Tabs
- **Rate Case Explorer** — Add utility financial context (revenue, customers, prices)
- **Utility Analysis** — Add EIA operational data alongside rate case history
- **National Overview** — Add total customers served, total revenue, avg price across tracked utilities
- **Geographic Map** — Color by average electricity price, emission rate, or rate case activity

---

## Updated Quality Scoring

```python
QUALITY_WEIGHTS = {
    'has_docket_number': 0.12,
    'has_utility_name_resolved': 0.10,
    'has_case_type_classified': 0.08,
    'has_filing_date': 0.08,
    'has_decision_date': 0.08,
    'has_revenue_request': 0.12,
    'has_revenue_approved': 0.12,
    'has_case_status': 0.05,
    'has_source_url': 0.05,
    'has_eia_data_linked': 0.10,        # NEW
    'has_emissions_data': 0.05,          # NEW
    'has_customer_impact': 0.05,         # NEW
}
```

---

## Export Updates
- utility_operations.csv — EIA Form 861 data
- utility_emissions.csv — eGRID emissions data
- utility_capacity.csv — EIA Form 860 capacity data
- rate_case_impacts.csv — Consumer bill impact estimates
- Updated summary.md with utility operational statistics

---

## Test Requirements (Target: 40+ new tests)
- EIA Form 861 Excel parsing (column name variations across years)
- EIA Form 860 Excel parsing
- eGRID Excel multi-sheet parsing
- Utility name cross-linking (PUC → EIA)
- Emissions calculation accuracy
- Consumer bill impact calculations
- Revenue-per-customer metrics
- Generation mix percentage validation
- Dashboard data queries
- Export format validation

---

## Priority Order
1. **EIA Form 861** — Customer counts + prices fill the biggest data gap
2. **eGRID Emissions** — Environmental dimension adds unique analytical value
3. **Rate Case Impact Calculations** — Built on #1, pure computation
4. **EIA Form 860 Capacity** — Infrastructure context, lower priority

---

## Expected Outcome
- From "rate case docket list" → "utility performance and consumer impact platform"
- First free tool combining: rate case decisions + utility financials + emissions + consumer impact
- Enables analysis: "Which utilities charge the most but pollute the most?"
- Enables: "How much did the average customer's bill change after this rate case?"
- High-value for: consumer advocates, utility commissions, energy researchers, clean energy firms, media
