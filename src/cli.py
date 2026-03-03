"""CLI interface for State PUC Rate Case Tracker.

Commands: scrape, extract, normalize, validate, export, dashboard, stats, pipeline, seed
"""

import click
from pathlib import Path
from rich.console import Console

console = Console()
PROJECT_ROOT = Path(__file__).parent.parent


@click.group()
@click.version_option(version="0.1.0")
def cli():
    """State PUC Rate Case Tracker

    Cross-linked database of utility rate case filings and decisions across
    state public utility commissions. Tracks docket numbers, filing/decision
    dates, revenue requests vs. approvals, case types, and utility entities
    across PA, OR, CA, IN, and WA.
    """
    pass


@cli.command()
@click.option("--source", "-s", help="Specific state PUC to scrape (e.g., pennsylvania_puc, oregon_puc, california_cpuc)")
@click.option("--force", "-f", is_flag=True, help="Re-scrape even if cached data exists")
@click.option("--start-year", type=int, default=2015, help="Earliest year to scrape (default: 2015)")
@click.option("--end-year", type=int, default=None, help="Latest year to scrape (default: current year)")
def scrape(source, force, start_year, end_year):
    """Scrape rate case docket data from state PUC websites."""
    from src.scrapers.pa_puc import scrape_pa_puc
    from src.scrapers.or_puc import scrape_or_puc
    from src.scrapers.ca_cpuc import scrape_ca_cpuc
    from src.scrapers.in_iurc import scrape_in_iurc
    from src.scrapers.wa_utc import scrape_wa_utc
    from src.scrapers.ct_pura import scrape_ct_pura
    from src.scrapers.mo_psc import scrape_mo_psc
    from src.scrapers.ga_psc import scrape_ga_psc

    scrapers = {
        "pennsylvania_puc": scrape_pa_puc,
        "oregon_puc": scrape_or_puc,
        "california_cpuc": scrape_ca_cpuc,
        "indiana_iurc": scrape_in_iurc,
        "washington_utc": scrape_wa_utc,
        "connecticut_pura": scrape_ct_pura,
        "missouri_psc": scrape_mo_psc,
        "georgia_psc": scrape_ga_psc,
    }

    if source:
        if source not in scrapers:
            console.print(f"[red]Unknown source: {source}. Available: {', '.join(scrapers.keys())}[/red]")
            raise SystemExit(1)
        console.print(f"[bold blue]Scraping {source}...[/bold blue]")
        scrapers[source](start_year=start_year, end_year=end_year, force=force)
    else:
        console.print("[bold blue]Scraping all PUC sources...[/bold blue]")
        for name, scraper_fn in scrapers.items():
            console.print(f"\n[bold]Scraping {name}...[/bold]")
            scraper_fn(start_year=start_year, end_year=end_year, force=force)


@cli.command()
@click.option("--source", "-s", help="Extract from specific source only")
def extract(source):
    """Extract and parse structured rate case records from raw scraped data."""
    from src.extractors.rate_case_parser import extract_all, extract_source
    if source:
        records = extract_source(source)
        console.print(f"[green]Extracted {len(records)} records from {source}[/green]")
    else:
        all_records = extract_all()
        total = sum(len(r) for r in all_records.values())
        console.print(f"[green]Extracted {total} records total[/green]")


@cli.command()
def normalize():
    """Normalize utility names, classify case types, and resolve entities."""
    from src.normalization.utilities import normalize_utilities
    normalize_utilities()


@cli.command()
def validate():
    """Run data quality validation and compute quality scores."""
    from src.validation.quality import validate_all
    validate_all()


@cli.command()
@click.option("--format", "-f", "fmt",
              type=click.Choice(["csv", "json", "excel", "markdown", "all"]),
              default="all")
@click.option("--output-dir", "-o", default="data/exports", help="Output directory")
def export(fmt, output_dir):
    """Export rate case data in various formats."""
    from src.export.exporter import export_data
    export_data(fmt, output_dir)


@cli.command()
@click.option("--port", "-p", type=int, default=8501, help="Dashboard port")
def dashboard(port):
    """Launch the Streamlit dashboard."""
    import subprocess
    subprocess.run([
        "streamlit", "run", "src/dashboard/app.py",
        "--server.port", str(port)
    ])


@cli.command()
def stats():
    """Show database statistics."""
    from src.storage.database import get_stats
    get_stats()


@cli.command()
def seed():
    """Load seed data into the database.

    Initializes the database and loads realistic sample rate case records
    from 5 state PUCs (PA, CA, OR, IN, WA). Use this to populate the
    database without requiring live web scraping.
    """
    from src.storage.database import init_db, store_records
    from scripts.seed_data import get_all_seed_data

    console.print("[bold blue]Loading seed data...[/bold blue]")

    conn = init_db()

    all_data = get_all_seed_data()
    total_created = 0
    total_updated = 0

    for source_key, records in all_data.items():
        created, updated = store_records(source_key, records, conn=conn)
        total_created += created
        total_updated += updated

    conn.close()

    console.print(f"\n[bold green]Seed data loaded: {total_created} created, {total_updated} updated[/bold green]")

    # Run normalization and validation
    console.print("\n[bold]Running normalization...[/bold]")
    from src.normalization.utilities import normalize_utilities
    normalize_utilities()

    console.print("\n[bold]Running quality validation...[/bold]")
    from src.validation.quality import validate_all
    validate_all()

    console.print("\n[bold green]Database ready![/bold green]")


@cli.command()
@click.option("--skip-scrape", is_flag=True, help="Skip scrape step (use cached data)")
@click.option("--start-year", type=int, default=2015, help="Earliest year to scrape (default: 2015)")
def pipeline(skip_scrape, start_year):
    """Run the full pipeline: scrape -> extract -> normalize -> validate -> store."""
    console.print("[bold blue]Starting full pipeline...[/bold blue]")
    from src.scrapers.pa_puc import scrape_pa_puc
    from src.scrapers.or_puc import scrape_or_puc
    from src.scrapers.ca_cpuc import scrape_ca_cpuc
    from src.scrapers.in_iurc import scrape_in_iurc
    from src.scrapers.wa_utc import scrape_wa_utc
    from src.scrapers.ct_pura import scrape_ct_pura
    from src.scrapers.mo_psc import scrape_mo_psc
    from src.scrapers.ga_psc import scrape_ga_psc
    from src.extractors.rate_case_parser import extract_all
    from src.normalization.utilities import normalize_utilities
    from src.validation.quality import validate_all
    from src.storage.database import init_db, store_records

    # Initialize database
    console.print("\n[bold]Initializing database...[/bold]")
    init_db()

    if not skip_scrape:
        console.print("\n[bold]Step 1/6: Scraping PUC docket data...[/bold]")
        for name, scraper_fn in [
            ("Pennsylvania PUC", scrape_pa_puc),
            ("Oregon PUC", scrape_or_puc),
            ("California CPUC", scrape_ca_cpuc),
            ("Indiana IURC", scrape_in_iurc),
            ("Washington UTC", scrape_wa_utc),
            ("Connecticut PURA", scrape_ct_pura),
            ("Missouri PSC", scrape_mo_psc),
            ("Georgia PSC", scrape_ga_psc),
        ]:
            console.print(f"  Scraping {name}...")
            scraper_fn(start_year=start_year)
    else:
        console.print("\n[bold]Step 1/6: Skipping scrape (using cached data)...[/bold]")

    console.print("\n[bold]Step 2/6: Extracting rate case records...[/bold]")
    all_records = extract_all()

    console.print("\n[bold]Step 3/6: Storing records...[/bold]")
    for source_key, records in all_records.items():
        if records:
            store_records(source_key, records)

    console.print("\n[bold]Step 4/6: Normalizing utility entities...[/bold]")
    normalize_utilities()

    console.print("\n[bold]Step 5/6: Validating data quality...[/bold]")
    validate_all()

    console.print("\n[bold]Step 6/6: Generating summary statistics...[/bold]")
    from src.storage.database import get_stats
    get_stats()

    console.print("\n[bold green]Pipeline complete![/bold green]")


if __name__ == "__main__":
    cli()
