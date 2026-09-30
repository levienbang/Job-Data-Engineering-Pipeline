"""Deterministic Silver text cleaning, with source values retained separately."""

import unicodedata
from dataclasses import asdict

import polars as pl

from job_pipeline.contracts import CanonicalJob

TEXT_FIELDS = (
    "source_job_id",
    "source_url",
    "raw_title",
    "raw_company",
    "raw_location",
    "raw_employment_type",
    "raw_description",
    "raw_requirements",
    "raw_experience",
    "raw_technical_skills",
    "raw_soft_skills",
    "raw_qualifications",
    "raw_languages",
    "raw_benefits",
    "raw_salary",
    "raw_working_hours",
    "raw_category",
    "raw_country",
)


def clean_text(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = unicodedata.normalize("NFC", value)
    lines = [" ".join(line.split()) for line in normalized.splitlines()]
    result = "\n".join(line for line in lines if line).strip()
    return result or None


def clean_jobs(jobs: list[CanonicalJob]) -> pl.DataFrame:
    """Use Polars for the Silver table while preserving raw source JSON elsewhere."""
    if not jobs:
        return pl.DataFrame()
    frame = pl.DataFrame([asdict(job) for job in jobs], infer_schema_length=None)
    return frame.with_columns(
        pl.col(field).map_elements(clean_text, return_dtype=pl.String)
        for field in TEXT_FIELDS
    )
