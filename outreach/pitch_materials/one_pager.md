# State PUC Rate Case Tracker
### The First Free Cross-State Utility Rate Case Database

## What It Is
A structured, searchable database tracking **1,294 utility rate case filings** across **4 state public utility commissions** — Missouri, Oregon, Connecticut, and Georgia. The database links docket numbers, utility identities, filing and decision dates, utility types, and case classifications into a single normalized schema.

## Why It Matters
- **S&P Global / Regulatory Research Associates** charges six-figure subscriptions for rate case tracking
- **Individual state PUC websites** have inconsistent interfaces and no cross-state search
- **No free tool exists** that aggregates and normalizes rate case data across states

## Key Numbers
| Metric | Value |
|--------|-------|
| Total rate cases tracked | 1,294 |
| States covered | 4 (MO, OR, CT, GA) |
| Unique utilities | 544 |
| Live-scraped sources | 4 (OR PUC, MO PSC, CT PURA, GA PSC) |
| Date range | 1990-2026 |
| Quality score average | 0.668 |
| Records above quality threshold | 100% |

## Sample Insights
- Missouri leads with 590 rate cases from the PSC EFIS system
- Oregon has the deepest docket history (582 cases back to 1990)
- Connecticut PURA contributes 114 rate cases from its Lotus Notes docket system
- PacifiCorp and Cascade Natural Gas appear in multiple state jurisdictions

## Technical Highlights
- **Zero LLM dependency** — all extraction is deterministic/rule-based
- **Entity resolution** across state boundaries using fuzzy matching + canonical name mapping
- **Quality scoring** on every record (average: 0.668/1.0, 100% above threshold)
- **Interactive Streamlit dashboard** with 11 analytical sections
- **Machine-readable exports:** CSV, JSON, Excel, and Markdown

## Use Cases
- **Energy consultants:** Benchmark rate case outcomes and timelines across states
- **Utility companies:** Track competitor filings and regulatory trends
- **Consumer advocates:** Monitor rate increase patterns by utility and state
- **Academic researchers:** Study regulatory outcomes, approval rates, and ROE trends
- **Investment analysts:** Track rate base growth and regulatory climate

---

Built by Nathan Goldberg | nathanmauricegoldberg@gmail.com | [LinkedIn](https://www.linkedin.com/in/nathan-goldberg-62a44522a/)
