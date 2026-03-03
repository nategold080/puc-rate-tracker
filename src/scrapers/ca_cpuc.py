"""California CPUC scraper stub.

Scrapes rate case data from the California CPUC proceedings search.
Currently uses seed data; live scraping to be implemented.
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Optional

from rich.console import Console

console = Console()
PROJECT_ROOT = Path(__file__).parent.parent.parent
CACHE_DIR = PROJECT_ROOT / "data" / "raw" / "ca_cpuc"


def scrape_ca_cpuc(
    start_year: int = 2015,
    end_year: Optional[int] = None,
    force: bool = False,
) -> list[dict]:
    """Scrape California CPUC rate case data. Uses seed data as fallback."""
    if end_year is None:
        end_year = datetime.now().year

    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache_file = CACHE_DIR / f"ca_cpuc_cases_{start_year}_{end_year}.json"

    if cache_file.exists() and not force:
        console.print("[dim]Loading cached CA CPUC data...[/dim]")
        with open(cache_file) as f:
            records = json.load(f)
        console.print(f"[green]Loaded {len(records)} cached CA CPUC records[/green]")
        return records

    from scripts.seed_data import get_ca_cpuc_seed_data
    records = get_ca_cpuc_seed_data()

    filtered = []
    for r in records:
        fd = r.get("filing_date", "")
        if fd:
            try:
                y = int(fd[:4])
                if start_year <= y <= end_year:
                    filtered.append(r)
            except (ValueError, IndexError):
                filtered.append(r)
        else:
            filtered.append(r)

    with open(cache_file, "w") as f:
        json.dump(filtered, f, indent=2, default=str)

    console.print(f"[green]Loaded {len(filtered)} CA CPUC rate cases (seed data)[/green]")
    return filtered
