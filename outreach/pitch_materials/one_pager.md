# State PUC Rate Case Tracker
### The First Free Cross-State Utility Rate Case Database

## What It Is
A structured, searchable database tracking **621 utility rate case filings** across **5 state public utility commissions** — Oregon, Pennsylvania, California, Indiana, and Washington. The database links docket numbers, utility identities, filing and decision dates, revenue requests vs. approvals, utility types, and case classifications into a single normalized schema.

## Why It Matters
- **S&P Global / Regulatory Research Associates** charges six-figure subscriptions for rate case tracking
- **Individual state PUC websites** have inconsistent interfaces and no cross-state search
- **No free tool exists** that aggregates and normalizes rate case data across states

## Key Numbers
| Metric | Value |
|--------|-------|
| Total rate cases tracked | 621 |
| States covered | 5 (OR, PA, CA, IN, WA) |
| Unique utilities | 467 |
| Date range | 1990–2024 |
| Cases with financial data | 39 |
| Total revenue requested | $23.0 billion |
| Total revenue approved | $10.5 billion |
| Average ROE | 10.39% |

## Sample Insights
- Oregon has the deepest docket history (582 cases back to 1990)
- Multi-service dockets (UM prefix) are the most common filing type in Oregon
- Average utility regulatory ROE across tracked decided cases: 10.39%
- PacifiCorp and Cascade Natural Gas appear in multiple state jurisdictions

## Technical Highlights
- **Zero LLM dependency** — all extraction is deterministic/rule-based
- **Entity resolution** across state boundaries using fuzzy matching + canonical name mapping
- **Quality scoring** on every record (average: 0.670/1.0, 100% above threshold)
- **Interactive Streamlit dashboard** with 7 analytical sections
- **Machine-readable exports:** CSV, JSON, Excel, and Markdown

## Use Cases
- **Energy consultants:** Benchmark rate case outcomes and timelines across states
- **Utility companies:** Track competitor filings and regulatory trends
- **Consumer advocates:** Monitor rate increase patterns by utility and state
- **Academic researchers:** Study regulatory outcomes, approval rates, and ROE trends
- **Investment analysts:** Track rate base growth and regulatory climate

---

Built by Nathan Goldberg | nathanmauricegoldberg@gmail.com | [LinkedIn](https://linkedin.com/in/nathanmauricegoldberg)
