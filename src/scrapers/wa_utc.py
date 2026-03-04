"""Washington UTC scraper stub.

Scrapes rate case data from the Washington UTC docket system.
Live scraping not yet implemented.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

from rich.console import Console

console = Console()
logger = logging.getLogger(__name__)
PROJECT_ROOT = Path(__file__).parent.parent.parent
CACHE_DIR = PROJECT_ROOT / "data" / "raw" / "wa_utc"


def scrape_wa_utc(
    start_year: int = 2015,
    end_year: Optional[int] = None,
    force: bool = False,
) -> list[dict]:
    """Scrape Washington UTC rate case data.

    Live scraping not yet implemented for Washington UTC. Returns cached
    data if available, otherwise returns an empty list.

    Args:
        start_year: Earliest year to scrape.
        end_year: Latest year to scrape.
        force: If True, re-scrape even if cached data exists.

    Returns:
        List of raw rate case records (dicts), or empty list.
    """
    if end_year is None:
        end_year = datetime.now().year

    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache_file = CACHE_DIR / f"wa_utc_cases_{start_year}_{end_year}.json"

    if cache_file.exists() and not force:
        console.print("[dim]Loading cached WA UTC data...[/dim]")
        with open(cache_file) as f:
            records = json.load(f)
        console.print(f"[green]Loaded {len(records)} cached WA UTC records[/green]")
        return records

    console.print(
        "[yellow]Live scraping not yet implemented for Washington UTC. "
        "Skipping.[/yellow]"
    )
    logger.info("Live scraping not yet implemented for Washington UTC. Skipping.")
    return []
