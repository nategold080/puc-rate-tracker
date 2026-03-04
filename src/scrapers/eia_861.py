"""EIA Form 861 downloader and parser.

Downloads annual EIA Form 861 ZIP files containing utility operational data:
customer counts, revenue, sales (MWh), and average electricity prices.

Data source: https://www.eia.gov/electricity/data/eia861/
Format: ZIP containing Excel/CSV files
Key file: Sales_Ult_Cust_{year}.xlsx
"""

from __future__ import annotations

import io
import re
import time
import zipfile
from pathlib import Path
from typing import Any, Optional

import httpx
from rich.console import Console

console = Console()

PROJECT_ROOT = Path(__file__).parent.parent.parent
CACHE_DIR = PROJECT_ROOT / "data" / "raw" / "eia_861"

EIA_861_URL = "https://www.eia.gov/electricity/data/eia861/zip/f861{year}.zip"

USER_AGENT = (
    "PUC-Rate-Tracker/1.0 (Data research project; "
    "contact: nathanmauricegoldberg@gmail.com)"
)

# Column name variations across years
SALES_COLUMN_MAPS = {
    "utility_number": ["Utility Number", "Utility_Number", "UTILITY_NUMBER"],
    "utility_name": ["Utility Name", "Utility_Name", "UTILITY_NAME"],
    "state": ["State", "STATE"],
    "ownership": ["Ownership", "OWNERSHIP", "Ownership Type"],
    "part": ["Part", "PART"],
    "service_type": ["Service Type", "SERVICE_TYPE"],
    # Residential
    "res_customers": [
        "Residential Customers", "RESIDENTIAL.Customers",
        "Customers.Residential", "Number of Customers.Residential",
    ],
    "res_revenue": [
        "Residential Revenue (Thousands Dollars)",
        "RESIDENTIAL.Revenue (Thousands Dollars)",
        "Revenue (Thousands Dollars).Residential",
        "Revenues (Thousands Dollars).Residential",
    ],
    "res_sales": [
        "Residential Sales (Megawatthours)",
        "RESIDENTIAL.Sales (Megawatthours)",
        "Sales (Megawatthours).Residential",
        "Megawatthours Sold.Residential",
    ],
    "res_avg_price": [
        "Residential Average Price (Cents/kWh)",
        "RESIDENTIAL.Average Price (Cents/kWh)",
        "Average Price (Cents/kWh).Residential",
    ],
    # Commercial
    "com_customers": [
        "Commercial Customers", "COMMERCIAL.Customers",
        "Customers.Commercial", "Number of Customers.Commercial",
    ],
    "com_revenue": [
        "Commercial Revenue (Thousands Dollars)",
        "COMMERCIAL.Revenue (Thousands Dollars)",
        "Revenue (Thousands Dollars).Commercial",
        "Revenues (Thousands Dollars).Commercial",
    ],
    "com_sales": [
        "Commercial Sales (Megawatthours)",
        "COMMERCIAL.Sales (Megawatthours)",
        "Sales (Megawatthours).Commercial",
        "Megawatthours Sold.Commercial",
    ],
    "com_avg_price": [
        "Commercial Average Price (Cents/kWh)",
        "COMMERCIAL.Average Price (Cents/kWh)",
        "Average Price (Cents/kWh).Commercial",
    ],
    # Industrial
    "ind_customers": [
        "Industrial Customers", "INDUSTRIAL.Customers",
        "Customers.Industrial", "Number of Customers.Industrial",
    ],
    "ind_revenue": [
        "Industrial Revenue (Thousands Dollars)",
        "INDUSTRIAL.Revenue (Thousands Dollars)",
        "Revenue (Thousands Dollars).Industrial",
        "Revenues (Thousands Dollars).Industrial",
    ],
    "ind_sales": [
        "Industrial Sales (Megawatthours)",
        "INDUSTRIAL.Sales (Megawatthours)",
        "Sales (Megawatthours).Industrial",
        "Megawatthours Sold.Industrial",
    ],
    "ind_avg_price": [
        "Industrial Average Price (Cents/kWh)",
        "INDUSTRIAL.Average Price (Cents/kWh)",
        "Average Price (Cents/kWh).Industrial",
    ],
    # Total
    "total_customers": [
        "Total Customers", "TOTAL.Customers",
        "Customers.Total", "Number of Customers.Total",
    ],
    "total_revenue": [
        "Total Revenue (Thousands Dollars)",
        "TOTAL.Revenue (Thousands Dollars)",
        "Revenue (Thousands Dollars).Total",
        "Revenues (Thousands Dollars).Total",
    ],
    "total_sales": [
        "Total Sales (Megawatthours)",
        "TOTAL.Sales (Megawatthours)",
        "Sales (Megawatthours).Total",
        "Megawatthours Sold.Total",
    ],
    "total_avg_price": [
        "Total Average Price (Cents/kWh)",
        "TOTAL.Average Price (Cents/kWh)",
        "Average Price (Cents/kWh).Total",
    ],
}


def _find_column(columns: list[str], aliases: list[str]) -> Optional[str]:
    """Find the matching column name from a list of aliases."""
    col_lower = {c.strip().lower(): c for c in columns}
    for alias in aliases:
        if alias.strip().lower() in col_lower:
            return col_lower[alias.strip().lower()]
    # Fuzzy substring match as fallback
    for alias in aliases:
        alias_lower = alias.strip().lower()
        for cl, orig in col_lower.items():
            if alias_lower in cl or cl in alias_lower:
                return orig
    return None


def download_eia_861(year: int, force: bool = False) -> Optional[Path]:
    """Download EIA Form 861 ZIP for a given year.

    Args:
        year: Data year (e.g., 2022).
        force: Re-download even if cached.

    Returns:
        Path to the downloaded ZIP file, or None on failure.
    """
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    zip_path = CACHE_DIR / f"f861{year}.zip"

    if zip_path.exists() and not force:
        console.print(f"[dim]Using cached EIA 861 for {year}[/dim]")
        return zip_path

    url = EIA_861_URL.format(year=year)
    console.print(f"[blue]Downloading EIA Form 861 for {year}...[/blue]")

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

    except httpx.HTTPStatusError as e:
        console.print(f"[red]HTTP error downloading EIA 861 for {year}: {e.response.status_code}[/red]")
        return None
    except Exception as e:
        console.print(f"[red]Error downloading EIA 861 for {year}: {e}[/red]")
        return None


def _find_sales_file(zip_path: Path) -> Optional[str]:
    """Find the sales to ultimate customers file inside the ZIP."""
    with zipfile.ZipFile(zip_path) as zf:
        for name in zf.namelist():
            lower = name.lower()
            if "sales" in lower and "ult" in lower and "cust" in lower:
                if lower.endswith((".xlsx", ".xls", ".csv")):
                    return name
        # Fallback: try broader patterns
        for name in zf.namelist():
            lower = name.lower()
            if "sales" in lower and lower.endswith((".xlsx", ".xls", ".csv")):
                return name
    return None


def parse_eia_861(zip_path: Path, year: int) -> list[dict[str, Any]]:
    """Parse EIA Form 861 sales data from a ZIP file.

    Args:
        zip_path: Path to the downloaded ZIP.
        year: Data year.

    Returns:
        List of utility operations dicts.
    """
    import openpyxl

    sales_file = _find_sales_file(zip_path)
    if not sales_file:
        console.print(f"[red]Could not find sales file in {zip_path.name}[/red]")
        return []

    console.print(f"[dim]Parsing {sales_file}...[/dim]")

    with zipfile.ZipFile(zip_path) as zf:
        data = zf.read(sales_file)

    if sales_file.lower().endswith(".csv"):
        return _parse_csv_sales(data, year)

    # Parse Excel
    wb = openpyxl.load_workbook(io.BytesIO(data), read_only=True, data_only=True)
    ws = wb.active

    rows = list(ws.iter_rows(values_only=True))
    wb.close()

    if not rows:
        return []

    # Find header row (first row with "Utility" in it)
    header_idx = 0
    for i, row in enumerate(rows):
        row_str = " ".join(str(c) for c in row if c)
        if "utility" in row_str.lower() and ("number" in row_str.lower() or "name" in row_str.lower()):
            header_idx = i
            break

    headers = [str(c).strip() if c else "" for c in rows[header_idx]]
    # Handle merged header rows: check if next row has sub-headers
    if header_idx + 1 < len(rows):
        next_row = rows[header_idx + 1]
        next_str = " ".join(str(c) for c in next_row if c)
        if "customer" in next_str.lower() or "revenue" in next_str.lower():
            # Merge: parent.child
            sub_headers = [str(c).strip() if c else "" for c in next_row]
            merged = []
            last_parent = ""
            for h, s in zip(headers, sub_headers):
                if h:
                    last_parent = h
                if s:
                    merged.append(f"{last_parent}.{s}" if last_parent else s)
                elif h:
                    merged.append(h)
                else:
                    merged.append("")
            headers = merged
            header_idx += 1

    # Map columns
    col_map = {}
    for field, aliases in SALES_COLUMN_MAPS.items():
        col = _find_column(headers, aliases)
        if col:
            col_map[field] = headers.index(col)

    if "utility_number" not in col_map:
        console.print(f"[red]Could not find utility_number column in {sales_file}[/red]")
        return []

    records = []
    for row in rows[header_idx + 1:]:
        if not row or not row[col_map["utility_number"]]:
            continue

        try:
            eia_id = _safe_int(row[col_map["utility_number"]])
        except (ValueError, TypeError):
            continue

        if not eia_id:
            continue

        rec = {
            "eia_utility_id": eia_id,
            "year": year,
            "utility_name": _safe_str(row, col_map.get("utility_name")),
            "state": _safe_str(row, col_map.get("state")),
            "ownership_type": _safe_str(row, col_map.get("ownership")),
            "residential_customers": _safe_int(row[col_map["res_customers"]]) if "res_customers" in col_map else None,
            "commercial_customers": _safe_int(row[col_map["com_customers"]]) if "com_customers" in col_map else None,
            "industrial_customers": _safe_int(row[col_map["ind_customers"]]) if "ind_customers" in col_map else None,
            "total_customers": _safe_int(row[col_map["total_customers"]]) if "total_customers" in col_map else None,
            "residential_revenue": _safe_float(row[col_map["res_revenue"]]) if "res_revenue" in col_map else None,
            "commercial_revenue": _safe_float(row[col_map["com_revenue"]]) if "com_revenue" in col_map else None,
            "industrial_revenue": _safe_float(row[col_map["ind_revenue"]]) if "ind_revenue" in col_map else None,
            "total_revenue": _safe_float(row[col_map["total_revenue"]]) if "total_revenue" in col_map else None,
            "residential_sales_mwh": _safe_float(row[col_map["res_sales"]]) if "res_sales" in col_map else None,
            "commercial_sales_mwh": _safe_float(row[col_map["com_sales"]]) if "com_sales" in col_map else None,
            "industrial_sales_mwh": _safe_float(row[col_map["ind_sales"]]) if "ind_sales" in col_map else None,
            "total_sales_mwh": _safe_float(row[col_map["total_sales"]]) if "total_sales" in col_map else None,
            "residential_avg_price": _safe_float(row[col_map["res_avg_price"]]) if "res_avg_price" in col_map else None,
            "commercial_avg_price": _safe_float(row[col_map["com_avg_price"]]) if "com_avg_price" in col_map else None,
            "industrial_avg_price": _safe_float(row[col_map["ind_avg_price"]]) if "ind_avg_price" in col_map else None,
            "avg_price": _safe_float(row[col_map["total_avg_price"]]) if "total_avg_price" in col_map else None,
        }

        # Compute revenue per customer
        total_rev = rec.get("total_revenue")
        total_cust = rec.get("total_customers")
        if total_rev and total_cust and total_cust > 0:
            rec["revenue_per_customer"] = round(total_rev * 1000 / total_cust, 2)

        # Quality score
        rec["quality_score"] = _score_operations(rec)

        records.append(rec)

    console.print(f"[green]Parsed {len(records)} utility records for {year}[/green]")
    return records


def _parse_csv_sales(data: bytes, year: int) -> list[dict[str, Any]]:
    """Parse CSV-format sales data."""
    import csv

    text = data.decode("utf-8", errors="replace")
    reader = csv.DictReader(io.StringIO(text))

    records = []
    for row in reader:
        # Map columns
        eia_id = None
        for alias in SALES_COLUMN_MAPS["utility_number"]:
            if alias in row:
                eia_id = _safe_int(row[alias])
                break

        if not eia_id:
            continue

        def _get(field):
            for alias in SALES_COLUMN_MAPS.get(field, []):
                if alias in row:
                    return row[alias]
            return None

        rec = {
            "eia_utility_id": eia_id,
            "year": year,
            "utility_name": _get("utility_name"),
            "state": _get("state"),
            "ownership_type": _get("ownership"),
            "residential_customers": _safe_int(_get("res_customers")),
            "commercial_customers": _safe_int(_get("com_customers")),
            "industrial_customers": _safe_int(_get("ind_customers")),
            "total_customers": _safe_int(_get("total_customers")),
            "residential_revenue": _safe_float(_get("res_revenue")),
            "commercial_revenue": _safe_float(_get("com_revenue")),
            "industrial_revenue": _safe_float(_get("ind_revenue")),
            "total_revenue": _safe_float(_get("total_revenue")),
            "residential_sales_mwh": _safe_float(_get("res_sales")),
            "commercial_sales_mwh": _safe_float(_get("com_sales")),
            "industrial_sales_mwh": _safe_float(_get("ind_sales")),
            "total_sales_mwh": _safe_float(_get("total_sales")),
            "residential_avg_price": _safe_float(_get("res_avg_price")),
            "commercial_avg_price": _safe_float(_get("com_avg_price")),
            "industrial_avg_price": _safe_float(_get("ind_avg_price")),
            "avg_price": _safe_float(_get("total_avg_price")),
        }

        total_rev = rec.get("total_revenue")
        total_cust = rec.get("total_customers")
        if total_rev and total_cust and total_cust > 0:
            rec["revenue_per_customer"] = round(total_rev * 1000 / total_cust, 2)

        rec["quality_score"] = _score_operations(rec)
        records.append(rec)

    console.print(f"[green]Parsed {len(records)} utility records for {year}[/green]")
    return records


def _safe_int(value: Any) -> Optional[int]:
    """Safely convert to int."""
    if value is None:
        return None
    try:
        s = str(value).strip().replace(",", "")
        if not s or s in (".", "-", "None", "NA", "N/A"):
            return None
        return int(float(s))
    except (ValueError, TypeError):
        return None


def _safe_float(value: Any) -> Optional[float]:
    """Safely convert to float."""
    if value is None:
        return None
    try:
        s = str(value).strip().replace(",", "")
        if not s or s in (".", "-", "None", "NA", "N/A"):
            return None
        return round(float(s), 4)
    except (ValueError, TypeError):
        return None


def _safe_str(row: tuple, idx: Optional[int]) -> Optional[str]:
    """Safely get string from a row tuple."""
    if idx is None or idx >= len(row):
        return None
    val = row[idx]
    if val is None:
        return None
    return str(val).strip() or None


def _score_operations(rec: dict) -> float:
    """Score quality of an operations record."""
    score = 0.0
    if rec.get("eia_utility_id"):
        score += 0.15
    if rec.get("utility_name"):
        score += 0.10
    if rec.get("state"):
        score += 0.10
    if rec.get("total_customers"):
        score += 0.20
    if rec.get("total_revenue"):
        score += 0.15
    if rec.get("total_sales_mwh"):
        score += 0.10
    if rec.get("residential_avg_price"):
        score += 0.10
    if rec.get("ownership_type"):
        score += 0.10
    return round(score, 3)


def fetch_eia_861(
    years: Optional[list[int]] = None,
    force: bool = False,
) -> list[dict[str, Any]]:
    """Download and parse EIA 861 data for specified years.

    Args:
        years: List of years to fetch. Defaults to [2020, 2021, 2022].
        force: Re-download even if cached.

    Returns:
        Combined list of all utility operations records.
    """
    if years is None:
        years = [2020, 2021, 2022]

    all_records = []

    for year in years:
        zip_path = download_eia_861(year, force=force)
        if zip_path:
            records = parse_eia_861(zip_path, year)
            all_records.extend(records)
            time.sleep(2)  # Rate limit

    console.print(f"[bold green]Total EIA 861 records: {len(all_records)}[/bold green]")
    return all_records
