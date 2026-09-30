"""Read the published VietJobs CSV and map rows to the DE contract."""

import csv
import hashlib
import json
from collections.abc import Iterator, Mapping
from datetime import datetime
from pathlib import Path

from job_pipeline.contracts import CanonicalJob

REQUIRED_COLUMNS = frozenset({"job_title", "description"})
SOURCE_FIELDS = {
    "raw_requirements": "requirements_text",
    "raw_experience": "experience_required",
    "raw_technical_skills": "technical_skills",
    "raw_soft_skills": "soft_skills",
    "raw_qualifications": "qualifications",
    "raw_languages": "languages_required",
    "raw_benefits": "benefits",
    "raw_salary": "salary",
    "raw_working_hours": "working_hours",
    "raw_category": "category",
    "raw_country": "country",
}


def _optional(value: str | None) -> str | None:
    return value if value and value.strip() else None


def compose_vietjobs_description(row: Mapping[str, str | None]) -> str:
    """Compatibility wrapper for the published CSV shape."""
    from job_pipeline.delivery import compose_import_description

    return compose_import_description(
        {
            "raw_description": row.get("description"),
            **{target: row.get(source) for target, source in SOURCE_FIELDS.items()},
        }
    )


def map_vietjobs_row(
    row: Mapping[str, str | None], *, collected_at: datetime
) -> CanonicalJob:
    """Map one CSV row without inventing source identity or timestamps."""
    title = row.get("job_title")
    description = row.get("description")
    if not title or not title.strip():
        raise ValueError("VietJobs row has no job_title")
    if not description or not description.strip():
        raise ValueError("VietJobs row has no description")

    # Hash every published source field so a change in source content is visible.
    # Sorted keys make the hash independent of CSV column order.
    content = json.dumps(
        {"source_name": "vietjobs", "record": dict(row)},
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return CanonicalJob(
        source_name="vietjobs",
        source_job_id=None,
        source_url=None,
        raw_title=title,
        raw_company=None,
        raw_location=_optional(row.get("location")),
        raw_employment_type=_optional(row.get("contract_type")),
        raw_description=description,
        posted_at=None,
        collected_at=collected_at,
        source_record_hash=hashlib.sha256(content.encode("utf-8")).hexdigest(),
        **{
            target: _optional(row.get(source))
            for target, source in SOURCE_FIELDS.items()
        },
    )


class VietJobsDatasetSource:
    """Stream raw or canonical records from a local copy of the published CSV."""

    def __init__(self, csv_path: Path) -> None:
        self.csv_path = csv_path

    def iter_rows(self) -> Iterator[dict[str, str | None]]:
        """Keep acquisition independent from optional downstream selection."""
        with self.csv_path.open(newline="", encoding="utf-8-sig") as source:
            reader = csv.DictReader(source)
            missing = REQUIRED_COLUMNS - set(reader.fieldnames or ())
            if missing:
                raise ValueError(
                    f"VietJobs CSV missing columns: {', '.join(sorted(missing))}"
                )
            yield from reader

    def iter_jobs(self, *, collected_at: datetime) -> Iterator[CanonicalJob]:
        for row in self.iter_rows():
            yield map_vietjobs_row(row, collected_at=collected_at)
