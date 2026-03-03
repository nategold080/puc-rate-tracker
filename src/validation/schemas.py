"""Pydantic v2 schemas for PUC Rate Case Tracker.

Defines validated models for rate cases, utilities, case documents,
and pipeline runs. All dollar amounts in millions. All percentages
stored as floats (e.g., 10.5 for 10.5%).
"""

from __future__ import annotations

import re
from datetime import date, datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field, field_validator, model_validator


# --- Enums ---


class UtilityType(str, Enum):
    ELECTRIC = "electric"
    GAS = "gas"
    WATER = "water"
    WASTEWATER = "wastewater"
    TELECOMMUNICATIONS = "telecommunications"
    MULTI_SERVICE = "multi_service"
    UNKNOWN = "unknown"


class CaseType(str, Enum):
    GENERAL_RATE_CASE = "general_rate_case"
    DISTRIBUTION_RATE_CASE = "distribution_rate_case"
    TRANSMISSION_RATE_CASE = "transmission_rate_case"
    FUEL_COST_ADJUSTMENT = "fuel_cost_adjustment"
    INFRASTRUCTURE_RIDER = "infrastructure_rider"
    DECOUPLING_MECHANISM = "decoupling_mechanism"
    RATE_DESIGN = "rate_design"
    OTHER = "other"
    UNKNOWN = "unknown"


class CaseStatus(str, Enum):
    FILED = "filed"
    ACTIVE = "active"
    SETTLED = "settled"
    DECIDED = "decided"
    WITHDRAWN = "withdrawn"
    SUSPENDED = "suspended"
    UNKNOWN = "unknown"


class OwnershipType(str, Enum):
    INVESTOR_OWNED = "investor_owned"
    MUNICIPAL = "municipal"
    COOPERATIVE = "cooperative"
    UNKNOWN = "unknown"


class DocumentType(str, Enum):
    APPLICATION = "application"
    TESTIMONY = "testimony"
    STAFF_REPORT = "staff_report"
    ORDER = "order"
    SETTLEMENT = "settlement"
    BRIEF = "brief"
    MOTION = "motion"
    NOTICE = "notice"
    OTHER = "other"


# --- Utility Schema ---


class Utility(BaseModel):
    """Utility company profile."""

    name: str = Field(..., min_length=1, max_length=500, description="Canonical utility name")
    canonical_name: Optional[str] = Field(None, description="Resolved canonical name after normalization")
    state: Optional[str] = Field(None, min_length=2, max_length=2, description="Two-letter state code")
    utility_type: UtilityType = Field(default=UtilityType.UNKNOWN, description="Primary service type")
    ownership_type: OwnershipType = Field(default=OwnershipType.UNKNOWN)
    parent_company: Optional[str] = Field(None, description="Parent holding company name")
    customer_count: Optional[int] = Field(None, ge=0, description="Approximate number of customers")
    eia_utility_id: Optional[int] = Field(None, description="EIA-861 utility identifier")
    ferc_respondent_id: Optional[int] = Field(None, description="FERC respondent identifier")

    @field_validator("state")
    @classmethod
    def validate_state(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return v
        v = v.upper().strip()
        valid_states = {
            "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
            "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
            "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
            "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
            "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
            "DC",
        }
        if v not in valid_states:
            raise ValueError(f"Invalid state code: {v}")
        return v

    @field_validator("name")
    @classmethod
    def validate_name(cls, v: str) -> str:
        return v.strip()


# --- Rate Case Schema ---


class RateCase(BaseModel):
    """A single rate case filing/proceeding at a state PUC."""

    docket_number: str = Field(..., min_length=1, max_length=100, description="PUC docket/case number")
    utility_name: str = Field(..., min_length=1, max_length=500, description="Utility company name as filed")
    canonical_utility_name: Optional[str] = Field(None, description="Resolved canonical utility name")
    state: str = Field(..., description="Two-letter state code")
    source: str = Field(..., description="Source PUC identifier (e.g., pennsylvania_puc)")

    # Classification
    case_type: CaseType = Field(default=CaseType.UNKNOWN, description="Type of rate case")
    utility_type: UtilityType = Field(default=UtilityType.UNKNOWN, description="Utility service type")
    status: CaseStatus = Field(default=CaseStatus.UNKNOWN, description="Current case status")

    # Dates
    filing_date: Optional[date] = Field(None, description="Date the case was filed")
    decision_date: Optional[date] = Field(None, description="Date the final decision was issued")
    effective_date: Optional[date] = Field(None, description="Date new rates take effect")

    # Financial - all dollar amounts in millions
    requested_revenue_change: Optional[float] = Field(
        None, description="Requested annual revenue change in $M"
    )
    approved_revenue_change: Optional[float] = Field(
        None, description="Approved annual revenue change in $M"
    )
    rate_base: Optional[float] = Field(None, description="Rate base in $M")
    return_on_equity: Optional[float] = Field(
        None, ge=0.0, le=25.0, description="Return on equity as percentage (e.g., 10.5)"
    )

    # Rate change percentages
    requested_rate_change_pct: Optional[float] = Field(
        None, description="Requested rate change as percentage"
    )
    approved_rate_change_pct: Optional[float] = Field(
        None, description="Approved rate change as percentage"
    )

    # Metadata
    source_url: Optional[str] = Field(None, description="URL of the docket page")
    description: Optional[str] = Field(None, max_length=5000, description="Case description or title")
    quality_score: Optional[float] = Field(None, ge=0.0, le=1.0, description="Data quality score 0.0-1.0")

    scraped_at: Optional[datetime] = Field(None, description="When data was last scraped")
    created_at: Optional[datetime] = Field(default_factory=datetime.utcnow)
    updated_at: Optional[datetime] = Field(default_factory=datetime.utcnow)

    @field_validator("state")
    @classmethod
    def validate_state(cls, v: str) -> str:
        v = v.upper().strip()
        valid_states = {
            "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
            "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
            "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
            "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
            "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
            "DC",
        }
        if v not in valid_states:
            raise ValueError(f"Invalid state code: {v}")
        return v

    @field_validator("docket_number")
    @classmethod
    def validate_docket_number(cls, v: str) -> str:
        return v.strip()

    @field_validator("utility_name")
    @classmethod
    def validate_utility_name(cls, v: str) -> str:
        return v.strip()

    @field_validator("filing_date", "decision_date", "effective_date")
    @classmethod
    def validate_dates(cls, v: Optional[date]) -> Optional[date]:
        if v is None:
            return v
        # Reasonable range: 1990-2030
        if v.year < 1990 or v.year > 2030:
            raise ValueError(f"Date {v} is outside reasonable range (1990-2030)")
        return v

    @field_validator("requested_revenue_change", "approved_revenue_change", "rate_base")
    @classmethod
    def validate_dollar_amounts(cls, v: Optional[float]) -> Optional[float]:
        if v is None:
            return v
        # Sanity check: no single rate case should involve more than $50B
        if abs(v) > 50000.0:
            raise ValueError(f"Dollar amount ${v}M seems unreasonably large (>$50B)")
        return round(v, 3)

    @field_validator("return_on_equity")
    @classmethod
    def validate_roe(cls, v: Optional[float]) -> Optional[float]:
        if v is None:
            return v
        return round(v, 2)

    @model_validator(mode="after")
    def validate_dates_order(self) -> "RateCase":
        if self.filing_date and self.decision_date:
            if self.decision_date < self.filing_date:
                raise ValueError(
                    f"Decision date {self.decision_date} cannot be before "
                    f"filing date {self.filing_date}"
                )
        if self.decision_date and self.effective_date:
            if self.effective_date < self.decision_date:
                # This can happen but is unusual — allow but don't error
                pass
        return self


# --- Case Document Schema ---


class CaseDocument(BaseModel):
    """A document linked to a rate case docket."""

    docket_number: str = Field(..., min_length=1, max_length=100)
    document_type: DocumentType = Field(default=DocumentType.OTHER)
    title: Optional[str] = Field(None, max_length=1000, description="Document title")
    filed_by: Optional[str] = Field(None, max_length=500, description="Entity that filed the document")
    filing_date: Optional[date] = Field(None, description="Date the document was filed")
    url: Optional[str] = Field(None, description="URL to the document")
    source: str = Field(..., description="Source PUC identifier")
    state: str = Field(..., min_length=2, max_length=2)

    @field_validator("state")
    @classmethod
    def validate_state(cls, v: str) -> str:
        return v.upper().strip()

    @field_validator("filing_date")
    @classmethod
    def validate_filing_date(cls, v: Optional[date]) -> Optional[date]:
        if v is None:
            return v
        if v.year < 1990 or v.year > 2030:
            raise ValueError(f"Date {v} is outside reasonable range (1990-2030)")
        return v

    @field_validator("url")
    @classmethod
    def validate_url(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return v
        v = v.strip()
        if v and not v.startswith(("http://", "https://")):
            raise ValueError(f"URL must start with http:// or https://: {v}")
        return v


# --- Pipeline Run Schema ---


class PipelineRun(BaseModel):
    """Record of a pipeline execution for auditing."""

    run_id: Optional[str] = Field(None, description="Unique run identifier")
    source: str = Field(..., description="PUC source key")
    stage: str = Field(..., description="Pipeline stage (scrape, extract, normalize, validate)")
    status: str = Field(..., description="Run status (running, completed, failed)")
    records_processed: int = Field(default=0, ge=0)
    records_created: int = Field(default=0, ge=0)
    records_updated: int = Field(default=0, ge=0)
    errors: int = Field(default=0, ge=0)
    started_at: datetime = Field(default_factory=datetime.utcnow)
    completed_at: Optional[datetime] = None
    notes: Optional[str] = None


# --- Helper functions ---


def parse_date_flexible(text: str) -> Optional[date]:
    """Parse dates in multiple common formats.

    Supports: YYYY-MM-DD, MM/DD/YYYY, M/D/YYYY, Month DD YYYY, etc.
    Returns None if parsing fails.
    """
    if not text or not text.strip():
        return None

    text = text.strip()

    # Standard formats to try
    formats = [
        "%Y-%m-%d",
        "%m/%d/%Y",
        "%m/%d/%y",
        "%m-%d-%Y",
        "%B %d, %Y",    # January 15, 2024
        "%b %d, %Y",    # Jan 15, 2024
        "%B %d %Y",     # January 15 2024
        "%b %d %Y",     # Jan 15 2024
        "%d %B %Y",     # 15 January 2024
        "%d %b %Y",     # 15 Jan 2024
        "%Y%m%d",        # 20240115
    ]

    for fmt in formats:
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue

    return None


def parse_dollar_amount(text: str) -> Optional[float]:
    """Parse dollar amount from text, returning value in millions.

    Handles: $25.3 million, $25,300,000, $25.3M, $25.3B, etc.
    Returns float in millions or None if parsing fails.
    """
    if not text or not text.strip():
        return None

    text = text.strip()

    # Pattern: $X.X million/billion/M/B
    match = re.search(
        r'\$\s*([\d,]+(?:\.\d+)?)\s*(million|billion|M|B|m|b)',
        text, re.IGNORECASE
    )
    if match:
        amount = float(match.group(1).replace(",", ""))
        unit = match.group(2).lower()
        if unit in ("billion", "b"):
            return round(amount * 1000.0, 3)  # Convert to millions
        elif unit in ("million", "m"):
            return round(amount, 3)

    # Pattern: plain dollar amount $X,XXX,XXX or $X,XXX,XXX.XX
    match = re.search(r'\$\s*([\d,]+(?:\.\d{1,2})?)', text)
    if match:
        amount = float(match.group(1).replace(",", ""))
        # Convert to millions
        result = round(amount / 1_000_000, 3)
        if result < 0.001:
            return None  # Too small to be a meaningful rate case amount
        return result

    return None


def parse_percentage(text: str) -> Optional[float]:
    """Parse percentage from text, returning as float (e.g., 10.5 for 10.5%).

    Handles: 10.5%, 10.5 percent, etc.
    """
    if not text or not text.strip():
        return None

    text = text.strip()

    match = re.search(r'([\d]+(?:\.\d+)?)\s*%', text)
    if match:
        return round(float(match.group(1)), 2)

    match = re.search(r'([\d]+(?:\.\d+)?)\s*percent', text, re.IGNORECASE)
    if match:
        return round(float(match.group(1)), 2)

    return None
