import csv
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from job_pipeline.sources.base import SourceAdapter
from job_pipeline.sources.vietjobs import (
    VietJobsDatasetSource,
    compose_vietjobs_description,
    map_vietjobs_row,
)

FIXTURE = Path(__file__).parent / "fixtures" / "vietjobs_small.csv"
COLLECTED_AT = datetime(2026, 9, 29, tzinfo=UTC)


def test_vietjobs_subset_maps_to_canonical_contract() -> None:
    source: SourceAdapter = VietJobsDatasetSource(FIXTURE)
    jobs = list(source.iter_jobs(collected_at=COLLECTED_AT))

    assert len(jobs) == 2
    first = jobs[0]
    assert first.source_name == "vietjobs"
    assert first.raw_title == "Kỹ Sư Môi Trường"
    assert first.raw_description == "Vận hành hệ thống xử lý nước thải."
    assert first.raw_experience == "1 năm"
    assert first.raw_location == "hồ chí minh"
    assert first.raw_employment_type == "Toàn thời gian"
    assert first.raw_company is None
    assert first.source_job_id is None
    assert first.source_url is None
    assert first.posted_at is None
    assert first.collected_at == COLLECTED_AT
    assert first.schema_version == 2
    assert jobs[1].raw_employment_type is None


def test_source_hash_is_stable_across_collection_time_and_column_order() -> None:
    with FIXTURE.open(newline="", encoding="utf-8") as source:
        row = next(csv.DictReader(source))
    later = COLLECTED_AT + timedelta(days=1)

    original = map_vietjobs_row(row, collected_at=COLLECTED_AT)
    reordered = map_vietjobs_row(dict(reversed(list(row.items()))), collected_at=later)
    changed = map_vietjobs_row({**row, "salary": "20 triệu"}, collected_at=later)

    assert original.source_record_hash == reordered.source_record_hash
    assert original.source_record_hash != changed.source_record_hash
    assert len(original.source_record_hash) == 64


def test_identical_source_rows_share_a_hash_on_rerun() -> None:
    with FIXTURE.open(newline="", encoding="utf-8") as source:
        row = next(csv.DictReader(source))

    first_run = map_vietjobs_row(row, collected_at=COLLECTED_AT)
    second_run = map_vietjobs_row(row, collected_at=COLLECTED_AT + timedelta(days=7))

    assert first_run.source_record_hash == second_run.source_record_hash


@pytest.mark.parametrize("missing_field", ["job_title", "description"])
def test_missing_required_content_is_rejected(missing_field: str) -> None:
    row = {"job_title": "Kỹ Sư", "description": "Mô tả công việc"}
    row[missing_field] = "  "

    with pytest.raises(ValueError, match=missing_field):
        map_vietjobs_row(row, collected_at=COLLECTED_AT)


def test_missing_required_column_is_rejected(tmp_path: Path) -> None:
    csv_path = tmp_path / "missing.csv"
    csv_path.write_text("job_title,location\nKỹ Sư,hà nội\n", encoding="utf-8")

    with pytest.raises(ValueError, match="description"):
        list(VietJobsDatasetSource(csv_path).iter_jobs(collected_at=COLLECTED_AT))


def test_composition_with_description_only_preserves_source_text() -> None:
    row = {"description": "  Build APIs.  ", "requirements_text": " "}

    assert compose_vietjobs_description(row) == "Build APIs."


def test_composition_adds_labelled_requirements() -> None:
    row = {"description": "Build APIs.", "requirements_text": "Know Python."}

    assert compose_vietjobs_description(row) == (
        "Description:\nBuild APIs.\n\nRequirements:\nKnow Python."
    )


def test_composition_preserves_all_distinct_source_sections() -> None:
    row = {
        "description": "Build APIs.",
        "requirements_text": "Know Python.",
        "experience_required": "2 years",
        "technical_skills": "['Python', 'SQL']",
        "qualifications": "['Bachelor degree']",
    }

    assert compose_vietjobs_description(row) == (
        "Description:\nBuild APIs.\n\nRequirements:\nKnow Python."
        "\n\nExperience:\n2 years\n\nTechnical skills:\n['Python', 'SQL']"
        "\n\nQualifications:\n['Bachelor degree']"
    )
    assert compose_vietjobs_description(dict(reversed(list(row.items())))) == (
        compose_vietjobs_description(row)
    )


def test_composition_omits_null_empty_and_identical_sections() -> None:
    row = {
        "description": "Build APIs.",
        "requirements_text": "  build   APIs.  ",
        "experience_required": None,
        "technical_skills": "  ",
        "qualifications": "Bachelor degree",
    }

    assert compose_vietjobs_description(row) == (
        "Description:\nBuild APIs.\n\nQualifications:\nBachelor degree"
    )


def test_source_hash_changes_with_new_requirements_but_not_collection_time() -> None:
    row = {"job_title": "Backend Engineer", "description": "Build APIs."}
    first = map_vietjobs_row(row, collected_at=COLLECTED_AT)
    same = map_vietjobs_row(row, collected_at=COLLECTED_AT + timedelta(days=1))
    changed = map_vietjobs_row(
        {**row, "requirements_text": "Know Python."}, collected_at=COLLECTED_AT
    )

    assert first.source_record_hash == same.source_record_hash
    assert first.source_record_hash != changed.source_record_hash
    assert changed.raw_description == "Build APIs."
    assert changed.raw_requirements == "Know Python."


def test_raw_rows_are_available_for_separate_selection() -> None:
    rows = list(VietJobsDatasetSource(FIXTURE).iter_rows())

    assert len(rows) == 2
    assert rows[0]["job_title"] == "Kỹ Sư Môi Trường"
