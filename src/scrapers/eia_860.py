"""EIA Form 860 downloader and parser.

Downloads annual EIA Form 860 ZIP files containing generator-level data:
capacity by fuel type, plant ages, planned additions/retirements.

Data source: https://www.eia.gov/electricity/data/eia860/
Format: ZIP containing Excel files
Key file: 3_1_Generator_Y{year}.xlsx
"""

from __future__ import annotations

import io
import re
import time
import zipfile
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

import httpx
from rich.console import Console

console = Console()

PROJECT_ROOT = Path(__file__).parent.parent.parent
CACHE_DIR = PROJECT_ROOT / "data" / "raw" / "eia_860"

EIA_860_URL = "https://www.eia.gov/electricity/data/eia860/zip/eia860{year}.zip"

USER_AGENT = (
    "PUC-Rate-Tracker/1.0 (Data research project; "
    "contact: nathanmauricegoldberg@gmail.com)"
)

# Technology type → fuel category mapping
TECH_TO_FUEL = {
    # Coal
    "conventional steam coal": "coal",
    "coal integrated gasification combined cycle": "coal",
    # Gas
    "natural gas fired combined cycle": "gas",
    "natural gas fired combustion turbine": "gas",
    "natural gas steam turbine": "gas",
    "natural gas internal combustion engine": "gas",
    "natural gas with compressed air storage": "gas",
    # Nuclear
    "nuclear": "nuclear",
    # Hydro
    "conventional hydroelectric": "hydro",
    "hydroelectric pumped storage": "hydro",
    # Wind
    "onshore wind turbine": "wind",
    "offshore wind turbine": "wind",
    # Solar
    "solar photovoltaic": "solar",
    "solar thermal with energy storage": "solar",
    "solar thermal without energy storage": "solar",
    # Other
    "petroleum liquids": "other",
    "petroleum coke": "other",
    "other gases": "other",
    "other waste biomass": "other",
    "wood/wood waste biomass": "other",
    "geothermal": "other",
    "landfill gas": "other",
    "municipal solid waste": "other",
    "batteries": "other",
    "flywheels": "other",
    "all other": "other",
}


def download_eia_860(year: int, force: bool = False) -> Optional[Path]:
    """Download EIA Form 860 ZIP for a given year.

    Args:
        year: Data year (e.g., 2022).
        force: Re-download even if cached.

    Returns:
        Path to the downloaded ZIP file, or None on failure.
    """
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    zip_path = CACHE_DIR / f"eia860{year}.zip"

    if zip_path.exists() and not force:
        console.print(f"[dim]Using cached EIA 860 for {year}[/dim]")
        return zip_path

    url = EIA_860_URL.format(year=year)
    console.print(f"[blue]Downloading EIA Form 860 for {year}...[/blue]")

    try:
        with httpx.Client(
            follow_redirects=True, timeout=120.0,
            headers={"User-Agent": USER_AGENT}
        ) as client:
            resp = client.get(url)
            resp.raise_for_status()

        zip_path.write_bytes(resp.content)
        console.print(f"[green]Downloaded {len(resp.content) / 1024 / 1024:.1f} MB[/green]")
        return zip_path

    except Exception as e:
        console.print(f"[red]Error downloading EIA 860 for {year}: {e}[/red]")
        return None


def _find_generator_file(zip_path: Path) -> Optional[str]:
    """Find the generator data file inside the ZIP."""
    with zipfile.ZipFile(zip_path) as zf:
        for name in zf.namelist():
            lower = name.lower()
            if "generator" in lower and ("3_1" in lower or "operable" in lower.replace(" ", "")):
                if lower.endswith((".xlsx", ".xls")):
                    return name
        # Broader fallback
        for name in zf.namelist():
            lower = name.lower()
            if "generator" in lower and lower.endswith((".xlsx", ".xls")):
                return name
    return None


def parse_eia_860(zip_path: Path, year: int) -> list[dict[str, Any]]:
    """Parse EIA Form 860 generator data and aggregate by utility.

    Args:
        zip_path: Path to the downloaded ZIP.
        year: Data year.

    Returns:
        List of utility capacity dicts (one per utility).
    """
    import openpyxl

    gen_file = _find_generator_file(zip_path)
    if not gen_file:
        console.print(f"[red]Could not find generator file in {zip_path.name}[/red]")
        return []

    console.print(f"[dim]Parsing {gen_file}...[/dim]")

    with zipfile.ZipFile(zip_path) as zf:
        data = zf.read(gen_file)

    wb = openpyxl.load_workbook(io.BytesIO(data), read_only=True, data_only=True)
    ws = wb.active

    rows = list(ws.iter_rows(values_only=True))
    wb.close()

    if not rows:
        return []

    # Find header row
    header_idx = 0
    for i, row in enumerate(rows):
        row_str = " ".join(str(c) for c in row if c).lower()
        if "utility" in row_str and ("nameplate" in row_str or "capacity" in row_str or "generator" in row_str):
            header_idx = i
            break

    headers = [str(c).strip().lower() if c else "" for c in rows[header_idx]]

    # Find column indices
    def _find_col(keywords):
        for i, h in enumerate(headers):
            if all(kw in h for kw in keywords):
                return i
        return None

    def _find_col_fallback(*keyword_lists):
        """Try multiple keyword lists, returning the first match (handles index 0 correctly)."""
        for keywords in keyword_lists:
            result = _find_col(keywords)
            if result is not None:
                return result
        return None

    col_utility_id = _find_col_fallback(["utility", "id"], ["utility", "number"])
    col_plant_id = _find_col_fallback(["plant", "code"], ["plant", "id"])
    col_technology = _find_col_fallback(["technology"], ["prime", "mover"], ["energy", "source"])
    col_capacity = _find_col_fallback(["nameplate", "capacity"], ["capacity", "mw"])
    col_op_year = _find_col_fallback(["operating", "year"], ["online", "year"])
    col_status = _find_col(["status"])
    col_retire_year = _find_col_fallback(["planned", "retirement"], ["retire"])

    if col_utility_id is None or col_capacity is None:
        console.print(f"[red]Missing required columns in generator file[/red]")
        return []

    # Aggregate by utility
    utility_data: dict[int, dict] = defaultdict(lambda: {
        "coal_mw": 0.0, "gas_mw": 0.0, "nuclear_mw": 0.0,
        "hydro_mw": 0.0, "wind_mw": 0.0, "solar_mw": 0.0,
        "other_mw": 0.0, "plants": set(), "generators": 0,
        "op_years": [], "planned_add_mw": 0.0, "planned_retire_mw": 0.0,
    })

    current_year = datetime.now().year

    for row in rows[header_idx + 1:]:
        if not row or col_utility_id >= len(row):
            continue

        try:
            uid = int(float(str(row[col_utility_id]).strip()))
        except (ValueError, TypeError):
            continue

        try:
            capacity = float(str(row[col_capacity]).strip().replace(",", ""))
        except (ValueError, TypeError):
            continue

        if capacity <= 0:
            continue

        # Determine fuel type
        tech_str = ""
        if col_technology is not None and col_technology < len(row) and row[col_technology]:
            tech_str = str(row[col_technology]).strip().lower()

        fuel = "other"
        for tech_pattern, fuel_type in TECH_TO_FUEL.items():
            if tech_pattern in tech_str:
                fuel = fuel_type
                break

        data = utility_data[uid]
        data[f"{fuel}_mw"] += capacity
        data["generators"] += 1

        if col_plant_id is not None and col_plant_id < len(row) and row[col_plant_id]:
            try:
                data["plants"].add(int(float(str(row[col_plant_id]).strip())))
            except (ValueError, TypeError):
                pass

        # Operating year for fleet age
        if col_op_year is not None and col_op_year < len(row) and row[col_op_year]:
            try:
                op_yr = int(float(str(row[col_op_year]).strip()))
                if 1900 <= op_yr <= current_year:
                    data["op_years"].append(op_yr)
            except (ValueError, TypeError):
                pass

        # Status-based classification
        status = ""
        if col_status is not None and col_status < len(row) and row[col_status]:
            status = str(row[col_status]).strip().lower()

        if "proposed" in status or "planned" in status or status in ("ts", "p", "l", "t"):
            data["planned_add_mw"] += capacity

        # Planned retirement
        if col_retire_year is not None and col_retire_year < len(row) and row[col_retire_year]:
            try:
                retire_yr = int(float(str(row[col_retire_year]).strip()))
                if retire_yr >= current_year:
                    data["planned_retire_mw"] += capacity
            except (ValueError, TypeError):
                pass

    # Convert to records
    records = []
    for uid, d in utility_data.items():
        total_mw = sum(d[f"{f}_mw"] for f in ["coal", "gas", "nuclear", "hydro", "wind", "solar", "other"])

        avg_age = None
        if d["op_years"]:
            avg_age = round(current_year - (sum(d["op_years"]) / len(d["op_years"])), 1)

        rec = {
            "eia_utility_id": uid,
            "year": year,
            "coal_capacity_mw": round(d["coal_mw"], 2),
            "gas_capacity_mw": round(d["gas_mw"], 2),
            "nuclear_capacity_mw": round(d["nuclear_mw"], 2),
            "hydro_capacity_mw": round(d["hydro_mw"], 2),
            "wind_capacity_mw": round(d["wind_mw"], 2),
            "solar_capacity_mw": round(d["solar_mw"], 2),
            "other_capacity_mw": round(d["other_mw"], 2),
            "total_capacity_mw": round(total_mw, 2),
            "num_plants": len(d["plants"]),
            "num_generators": d["generators"],
            "avg_generator_age": avg_age,
            "planned_additions_mw": round(d["planned_add_mw"], 2),
            "planned_retirements_mw": round(d["planned_retire_mw"], 2),
        }

        # Quality score
        rec["quality_score"] = _score_capacity(rec)
        records.append(rec)

    console.print(f"[green]Parsed capacity for {len(records)} utilities for {year}[/green]")
    return records


def _score_capacity(rec: dict) -> float:
    """Score quality of a capacity record."""
    score = 0.0
    if rec.get("eia_utility_id"):
        score += 0.15
    if rec.get("total_capacity_mw"):
        score += 0.20
    if rec.get("num_generators"):
        score += 0.15
    if rec.get("num_plants"):
        score += 0.10
    if rec.get("avg_generator_age"):
        score += 0.15
    if any(rec.get(f"{f}_capacity_mw") is not None for f in ["coal", "gas", "nuclear", "hydro", "wind", "solar"]):
        score += 0.15
    if rec.get("planned_additions_mw") is not None or rec.get("planned_retirements_mw") is not None:
        score += 0.10
    return round(score, 3)


def fetch_eia_860(
    years: Optional[list[int]] = None,
    force: bool = False,
) -> list[dict[str, Any]]:
    """Download and parse EIA 860 data for specified years.

    Args:
        years: List of years. Defaults to [2022].
        force: Re-download even if cached.

    Returns:
        Combined list of utility capacity records.
    """
    if years is None:
        years = [2022]

    all_records = []
    for year in years:
        zip_path = download_eia_860(year, force=force)
        if zip_path:
            records = parse_eia_860(zip_path, year)
            all_records.extend(records)
            time.sleep(2)

    console.print(f"[bold green]Total EIA 860 records: {len(all_records)}[/bold green]")
    return all_records
