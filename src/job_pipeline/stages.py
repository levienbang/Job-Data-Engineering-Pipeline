"""Standalone stage functions shared by direct runs and Airflow."""

import hashlib
import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import polars as pl
from pydantic import ValidationError

from job_pipeline.clean import clean_jobs
from job_pipeline.contracts import CanonicalJob
from job_pipeline.deduplicate import identity
from job_pipeline.pipeline import (
    CANONICAL_SCHEMA,
    FAKE_JOBS_COLUMNS,
    MAPPERS,
    VIETJOBS_COLUMNS,
    CorruptInputError,
    SchemaDriftError,
    _empty_jobs,
    _json,
)
from job_pipeline.sources.technology import is_technology_job
from job_pipeline.storage import read_parquet, sha256_file, write_parquet
from job_pipeline.validate import ValidatedJob


@dataclass(frozen=True)
class BronzeStage:
    path: Path
    checksum: str
    input_count: int
    source: str


@dataclass(frozen=True)
class CleanStage:
    prepared_path: Path
    quarantine_path: Path
    valid_count: int
    invalid_count: int
    filtered_count: int


def snapshot_bronze(
    source: str,
    raw_records: Iterable[Mapping[str, object]],
    *,
    collected_at: datetime,
    root: Path = Path("data"),
) -> BronzeStage:
    if source not in {"vietjobs", "fake_jobs"}:
        raise ValueError(f"unsupported source: {source}")
    if collected_at.tzinfo is None:
        raise ValueError("collected_at needs a timezone")
    raw_json = [_json(dict(record)) for record in raw_records]
    fingerprint = hashlib.sha256("\n".join(raw_json).encode("utf-8")).hexdigest()
    path = (
        root
        / "bronze"
        / f"source={source}"
        / f"ingestion_date={collected_at.date().isoformat()}"
        / f"jobs-{fingerprint}.parquet"
    )
    frame = pl.DataFrame(
        {
            "source_name": [source] * len(raw_json),
            "row_number": list(range(1, len(raw_json) + 1)),
            "collected_at": [collected_at.isoformat()] * len(raw_json),
            "raw_record_json": raw_json,
        },
        schema={
            "source_name": pl.String,
            "row_number": pl.Int64,
            "collected_at": pl.String,
            "raw_record_json": pl.String,
        },
    )
    write_parquet(frame, path, immutable=True)
    if read_parquet(path)["raw_record_json"].to_list() != raw_json:
        raise CorruptInputError("Bronze snapshot differs from its input fingerprint")
    return BronzeStage(path, sha256_file(path), len(raw_json), source)


def validate_clean_bronze(
    bronze: BronzeStage, *, root: Path = Path("data")
) -> CleanStage:
    """Validate each row; write staged Silver candidates and explicit quarantine."""
    source = bronze.source
    if source not in MAPPERS:
        raise ValueError(f"unsupported source: {source}")
    frame = read_parquet(bronze.path)
    mapped: list[CanonicalJob] = []
    lineage: list[dict[str, object]] = []
    quarantine: list[dict[str, object]] = []
    filtered = 0
    for row in frame.to_dicts():
        raw = json.loads(row["raw_record_json"])
        if not isinstance(raw, dict):
            quarantine.append(
                {
                    "source_name": source,
                    "row_number": row["row_number"],
                    "raw_record_json": row["raw_record_json"],
                    "error_code": "SCHEMA_DRIFT",
                    "error_detail": "record is not an object",
                }
            )
            continue
        try:
            expected = VIETJOBS_COLUMNS if source == "vietjobs" else FAKE_JOBS_COLUMNS
            if set(raw) != expected:
                raise SchemaDriftError(
                    "source record fields differ from expected schema"
                )
            if source == "vietjobs" and not is_technology_job(raw):
                filtered += 1
                continue
            job = MAPPERS[source](
                raw, collected_at=datetime.fromisoformat(row["collected_at"])
            )
            for field in (
                "raw_title",
                "raw_description",
                "source_url",
                "source_job_id",
            ):
                value = getattr(job, field)
                if value is not None and not isinstance(value, str):
                    raise TypeError(f"{field} must be text")
        except (ValueError, KeyError, TypeError, AttributeError) as exc:
            quarantine.append(
                {
                    "source_name": source,
                    "row_number": row["row_number"],
                    "raw_record_json": row["raw_record_json"],
                    "error_code": "SCHEMA_DRIFT"
                    if isinstance(exc, SchemaDriftError)
                    else "MAPPING_ERROR",
                    "error_detail": str(exc),
                }
            )
            continue
        mapped.append(job)
        lineage.append(
            {
                "bronze_path": str(bronze.path),
                "bronze_row_number": row["row_number"],
                "raw_record_json": row["raw_record_json"],
            }
        )

    valid: list[dict[str, object]] = []
    for row, trace in zip(clean_jobs(mapped).to_dicts(), lineage, strict=True):
        try:
            validated = ValidatedJob.model_validate(row)
        except ValidationError as exc:
            quarantine.append(
                {
                    "source_name": source,
                    "row_number": trace["bronze_row_number"],
                    "raw_record_json": trace["raw_record_json"],
                    "error_code": "VALIDATION_ERROR",
                    "error_detail": str(exc),
                }
            )
            continue
        valid.append({**validated.model_dump(), **trace})

    partition = (
        f"source={source}/ingestion_date={bronze.path.parent.name.split('=', 1)[1]}"
    )
    prepared_path = root / "staging" / partition / bronze.path.name
    quarantine_path = root / "quarantine" / partition / bronze.path.name
    prepared = (
        pl.DataFrame(valid, infer_schema_length=None)
        if valid
        else _empty_jobs(silver=True)
    )
    write_parquet(prepared, prepared_path)
    quarantine_frame = (
        pl.DataFrame(quarantine)
        if quarantine
        else pl.DataFrame(
            schema={
                "source_name": pl.String,
                "row_number": pl.Int64,
                "raw_record_json": pl.String,
                "error_code": pl.String,
                "error_detail": pl.String,
            }
        )
    )
    write_parquet(quarantine_frame, quarantine_path)
    return CleanStage(
        prepared_path, quarantine_path, len(valid), len(quarantine), filtered
    )


def publish_silver(clean: CleanStage, *, root: Path = Path("data")) -> Path:
    """Publish validated candidates as the stable Silver Parquet artifact."""
    path = root / "silver" / clean.prepared_path.relative_to(root / "staging")
    write_parquet(read_parquet(clean.prepared_path), path)
    return path


def deduplicate_gold(
    silver_path: Path, *, source: str, root: Path = Path("data")
) -> tuple[Path, int]:
    gold_path = root / "gold" / f"source={source}" / "jobs.parquet"
    existing = []
    if gold_path.exists():
        old = read_parquet(gold_path)
        if (
            set(CANONICAL_SCHEMA) - set(old.columns)
            or old.filter(pl.col("schema_version") != 2).height
        ):
            raise ValueError(
                f"Gold schema v1 detected at {gold_path}; rerun the pipeline"
            )
        existing = old.to_dicts()
    seen = {identity(row) for row in existing}
    duplicates = 0
    silver = read_parquet(silver_path)
    if (
        set(CANONICAL_SCHEMA) - set(silver.columns)
        or silver.filter(pl.col("schema_version") != 2).height
    ):
        raise ValueError(
            f"Silver schema v1 detected at {silver_path}; rerun the pipeline"
        )
    for row in silver.to_dicts():
        key = identity(row)
        if key in seen:
            duplicates += 1
            continue
        seen.add(key)
        existing.append(
            {key: value for key, value in row.items() if key != "raw_record_json"}
        )
    gold = (
        pl.DataFrame(existing, infer_schema_length=None)
        if existing
        else _empty_jobs(silver=False)
    )
    write_parquet(gold, gold_path)
    return gold_path, duplicates
