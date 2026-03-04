"""CLI interface for State PUC Rate Case Tracker.

Commands: scrape, extract, normalize, validate, export, dashboard, stats, pipeline
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
    across MO, OR, CT, and GA.
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
@click.option("--years", "-y", default="2020,2021,2022", help="Comma-separated years to fetch")
@click.option("--force", "-f", is_flag=True, help="Re-download even if cached")
def enrich(years, force):
    """Run all data enrichment: EIA 861 + eGRID + EIA 860 + impact calculations."""
    from src.storage.database import (
        get_all_rate_cases, get_all_utilities, get_connection, get_utility_eia_links,
        get_utility_operations, init_db,
        upsert_utility_operations_batch, upsert_utility_eia_links_batch,
        upsert_utility_emissions_batch, upsert_utility_capacity_batch,
        upsert_rate_case_impacts_batch,
    )
    from src.scrapers.eia_861 import fetch_eia_861
    from src.scrapers.egrid import fetch_egrid
    from src.scrapers.eia_860 import fetch_eia_860
    from src.normalization.cross_linker import (
        cross_link_utilities, cross_link_emissions, compute_rate_case_impacts,
    )

    year_list = [int(y.strip()) for y in years.split(",")]

    conn = init_db()

    # Phase 1: EIA Form 861
    console.print("\n[bold blue]Phase 1: EIA Form 861 — Utility Operations[/bold blue]")
    eia_records = fetch_eia_861(years=year_list, force=force)
    if eia_records:
        created, updated = upsert_utility_operations_batch(eia_records, conn=conn)
        console.print(f"[green]Stored {created} new, {updated} updated EIA 861 records[/green]")

    # Cross-link PUC utilities to EIA IDs
    console.print("\n[bold]Cross-linking utilities to EIA IDs...[/bold]")
    puc_utilities = get_all_utilities(conn=conn)
    links = cross_link_utilities(puc_utilities, eia_records)
    if links:
        upsert_utility_eia_links_batch(links, conn=conn)

    # Phase 2: eGRID emissions
    console.print("\n[bold blue]Phase 2: EPA eGRID — Emissions[/bold blue]")
    egrid_years = [y for y in year_list if y >= 2020]
    if not egrid_years:
        egrid_years = [2022]
    emissions = fetch_egrid(years=egrid_years, force=force)
    if emissions:
        # Link emissions to EIA IDs
        eia_links = get_utility_eia_links(conn=conn)
        cross_link_emissions(eia_links, emissions)
        created, updated = upsert_utility_emissions_batch(emissions, conn=conn)
        console.print(f"[green]Stored {created} new, {updated} updated emissions records[/green]")

    # Phase 3: EIA Form 860
    console.print("\n[bold blue]Phase 3: EIA Form 860 — Generation Capacity[/bold blue]")
    capacity_records = fetch_eia_860(years=egrid_years, force=force)
    if capacity_records:
        created, updated = upsert_utility_capacity_batch(capacity_records, conn=conn)
        console.print(f"[green]Stored {created} new, {updated} updated capacity records[/green]")

    # Phase 4: Rate case impact calculations
    console.print("\n[bold blue]Phase 4: Rate Case Consumer Impact Calculations[/bold blue]")
    rate_cases = get_all_rate_cases(limit=10000, conn=conn)
    eia_links = get_utility_eia_links(conn=conn)
    operations = get_utility_operations(conn=conn)
    impacts = compute_rate_case_impacts(rate_cases, eia_links, operations)
    if impacts:
        count = upsert_rate_case_impacts_batch(impacts, conn=conn)
        console.print(f"[green]Stored {count} rate case impact records[/green]")

    conn.close()
    console.print("\n[bold green]Enrichment complete![/bold green]")


@cli.command(name="enrich-eia861")
@click.option("--years", "-y", default="2020,2021,2022", help="Comma-separated years")
@click.option("--force", "-f", is_flag=True, help="Re-download even if cached")
def enrich_eia861(years, force):
    """Download and store EIA Form 861 utility operational data."""
    from src.storage.database import (
        get_all_utilities, get_connection, init_db,
        upsert_utility_operations_batch, upsert_utility_eia_links_batch,
    )
    from src.scrapers.eia_861 import fetch_eia_861
    from src.normalization.cross_linker import cross_link_utilities

    year_list = [int(y.strip()) for y in years.split(",")]
    conn = init_db()

    records = fetch_eia_861(years=year_list, force=force)
    if records:
        created, updated = upsert_utility_operations_batch(records, conn=conn)
        console.print(f"[green]Stored {created} new, {updated} updated[/green]")

        # Cross-link
        puc_utilities = get_all_utilities(conn=conn)
        links = cross_link_utilities(puc_utilities, records)
        if links:
            upsert_utility_eia_links_batch(links, conn=conn)

    conn.close()


@cli.command(name="enrich-egrid")
@click.option("--years", "-y", default="2022", help="Comma-separated years")
@click.option("--force", "-f", is_flag=True, help="Re-download even if cached")
def enrich_egrid(years, force):
    """Download and store EPA eGRID emissions data."""
    from src.storage.database import (
        get_connection, get_utility_eia_links, init_db,
        upsert_utility_emissions_batch,
    )
    from src.scrapers.egrid import fetch_egrid
    from src.normalization.cross_linker import cross_link_emissions

    year_list = [int(y.strip()) for y in years.split(",")]
    conn = init_db()

    emissions = fetch_egrid(years=year_list, force=force)
    if emissions:
        eia_links = get_utility_eia_links(conn=conn)
        cross_link_emissions(eia_links, emissions)
        created, updated = upsert_utility_emissions_batch(emissions, conn=conn)
        console.print(f"[green]Stored {created} new, {updated} updated[/green]")

    conn.close()


@cli.command(name="enrich-eia860")
@click.option("--years", "-y", default="2022", help="Comma-separated years")
@click.option("--force", "-f", is_flag=True, help="Re-download even if cached")
def enrich_eia860(years, force):
    """Download and store EIA Form 860 generation capacity data."""
    from src.storage.database import (
        get_connection, init_db, upsert_utility_capacity_batch,
    )
    from src.scrapers.eia_860 import fetch_eia_860

    year_list = [int(y.strip()) for y in years.split(",")]
    conn = init_db()

    records = fetch_eia_860(years=year_list, force=force)
    if records:
        created, updated = upsert_utility_capacity_batch(records, conn=conn)
        console.print(f"[green]Stored {created} new, {updated} updated[/green]")

    conn.close()


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
