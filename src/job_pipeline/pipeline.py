"""Standalone Bronze → Silver → Gold pipeline for the two supported sources."""

import json
from collections.abc import Iterable, Mapping
from dataclasses import asdict, dataclass, replace
from datetime import datetime
from pathlib import Path

import polars as pl

from job_pipeline.contracts import CanonicalJob
from job_pipeline.sources.fake_jobs import FakeJobRecord, map_fake_job
from job_pipeline.sources.vietjobs import VietJobsDatasetSource, map_vietjobs_row
from job_pipeline.storage import sha256_file

VIETJOBS_COLUMNS = frozenset(
    {
        "job_title",
        "location",
        "country",
        "qualifications",
        "technical_skills",
        "soft_skills",
        "languages_required",
        "experience_required",
        "salary",
        "contract_type",
        "working_hours",
        "benefits",
        "description",
        "requirements_text",
        "category",
        "salary_min",
        "salary_max",
        "salary_avg",
    }
)
FAKE_JOBS_COLUMNS = frozenset(
    {"title", "company", "location", "description", "source_url", "posted_at"}
)

CANONICAL_SCHEMA = {
    "source_name": pl.String,
    "source_job_id": pl.String,
    "source_url": pl.String,
    "raw_title": pl.String,
    "raw_company": pl.String,
    "raw_location": pl.String,
    "raw_employment_type": pl.String,
    "raw_description": pl.String,
    "raw_requirements": pl.String,
    "raw_experience": pl.String,
    "raw_technical_skills": pl.String,
    "raw_soft_skills": pl.String,
    "raw_qualifications": pl.String,
    "raw_languages": pl.String,
    "raw_benefits": pl.String,
    "raw_salary": pl.String,
    "raw_working_hours": pl.String,
    "raw_category": pl.String,
    "raw_country": pl.String,
    "posted_at": pl.Datetime("us", "UTC"),
    "collected_at": pl.Datetime("us", "UTC"),
    "source_record_hash": pl.String,
    "schema_version": pl.Int64,
    "bronze_path": pl.String,
    "bronze_row_number": pl.Int64,
}


def _empty_jobs(*, silver: bool) -> pl.DataFrame:
    schema = dict(CANONICAL_SCHEMA)
    if silver:
        schema["raw_record_json"] = pl.String
    return pl.DataFrame(schema=schema)


class SchemaDriftError(ValueError):
    """The source file does not have the published schema."""


class CorruptInputError(ValueError):
    """An input file cannot be decoded or parsed."""


@dataclass(frozen=True)
class PipelineResult:
    bronze_path: Path
    silver_path: Path
    gold_path: Path
    quarantine_path: Path
    bronze_checksum: str
    input_count: int
    valid_count: int
    invalid_count: int
    duplicate_count: int
    filtered_count: int
    input_checksum: str | None = None


def _json(value: Mapping[str, object]) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)


def _map_job(
    source: str, raw: dict[str, object], collected_at: datetime
) -> CanonicalJob:
    if source == "vietjobs":
        return map_vietjobs_row(raw, collected_at=collected_at)
    if source == "fake_jobs":
        posted_at = raw.get("posted_at")
        if isinstance(posted_at, str):
            posted_at = datetime.fromisoformat(posted_at)
        return map_fake_job(
            FakeJobRecord(
                title=raw["title"],
                company=raw.get("company"),
                location=raw.get("location"),
                description=raw["description"],
                source_url=raw["source_url"],
                posted_at=posted_at,
            ),
            collected_at=collected_at,
        )
    raise ValueError(f"unsupported source: {source}")


def run_pipeline(
    source: str,
    raw_records: Iterable[Mapping[str, object]],
    *,
    collected_at: datetime,
    root: Path = Path("data"),
) -> PipelineResult:
    """Run the same discrete stages used by the Airflow DAG."""
    from job_pipeline.stages import (
        deduplicate_gold,
        publish_silver,
        snapshot_bronze,
        validate_clean_bronze,
    )

    bronze = snapshot_bronze(source, raw_records, collected_at=collected_at, root=root)
    clean = validate_clean_bronze(bronze, root=root)
    silver_path = publish_silver(clean, root=root)
    gold_path, duplicates = deduplicate_gold(silver_path, source=source, root=root)
    return PipelineResult(
        bronze.path,
        silver_path,
        gold_path,
        clean.quarantine_path,
        bronze.checksum,
        bronze.input_count,
        clean.valid_count,
        clean.invalid_count,
        duplicates,
        clean.filtered_count,
    )


def read_vietjobs_rows(csv_path: Path) -> list[dict[str, str | None]]:
    """Validate the published CSV header and decode source rows for acquisition."""
    import csv

    try:
        with csv_path.open(newline="", encoding="utf-8-sig") as stream:
            header = next(csv.reader(stream), [])
        if set(header) != VIETJOBS_COLUMNS or len(header) != len(VIETJOBS_COLUMNS):
            raise SchemaDriftError("VietJobs CSV columns differ from published schema")
        return list(VietJobsDatasetSource(csv_path).iter_rows())
    except (UnicodeError, csv.Error) as exc:
        raise CorruptInputError(f"cannot parse VietJobs CSV: {csv_path}") from exc


def run_vietjobs(
    csv_path: Path, *, collected_at: datetime, root: Path = Path("data")
) -> PipelineResult:
    """Run the published VietJobs CSV through all standalone stages."""
    result = run_pipeline(
        "vietjobs", read_vietjobs_rows(csv_path), collected_at=collected_at, root=root
    )
    return replace(result, input_checksum=sha256_file(csv_path))


def run_fake_jobs(
    records: Iterable[FakeJobRecord],
    *,
    collected_at: datetime,
    root: Path = Path("data"),
) -> PipelineResult:
    return run_pipeline(
        "fake_jobs",
        (asdict(record) for record in records),
        collected_at=collected_at,
        root=root,
    )
