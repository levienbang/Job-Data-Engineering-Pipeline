"""Offline Milestone 3/4 pipeline tests."""

import csv
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx
import polars as pl
import pytest

from job_pipeline.clean import clean_text
from job_pipeline.pipeline import (
    CorruptInputError,
    SchemaDriftError,
    run_fake_jobs,
    run_pipeline,
    run_vietjobs,
)
from job_pipeline.quality import quality_report
from job_pipeline.sources.fake_jobs import LIST_URL, FakeJobRecord, FakeJobsCrawler
from job_pipeline.storage import read_parquet

NOW = datetime(2026, 9, 30, tzinfo=UTC)


def fake_record(**changes: object) -> FakeJobRecord:
    values = {
        "title": "  Cafe\u0301   Engineer  ",
        "company": " Example   Labs ",
        "location": " New   York ",
        "description": " Build   APIs.\n\n  Test   them. ",
        "source_url": "https://realpython.github.io/fake-jobs/job-1.html",
        "posted_at": datetime(2026, 9, 1, tzinfo=UTC),
    }
    values.update(changes)
    return FakeJobRecord(**values)


def test_fake_jobs_round_trip_cleaning_quality_and_rerun(tmp_path: Path) -> None:
    first = run_fake_jobs(
        [fake_record(), fake_record()], collected_at=NOW, root=tmp_path
    )
    assert first.input_count == 2
    assert first.valid_count == 2
    assert first.duplicate_count == 1
    assert len(first.bronze_checksum) == 64
    bronze = read_parquet(first.bronze_path)
    silver = read_parquet(first.silver_path)
    gold = read_parquet(first.gold_path)
    assert bronze.height == 2
    assert "Cafe\u0301" in bronze["raw_record_json"][0]
    assert silver.height == 2
    assert silver["raw_title"][0] == "Café Engineer"
    assert gold.height == 1
    assert gold["raw_description"][0] == "Build APIs.\nTest them."
    assert gold["bronze_path"][0] == str(first.bronze_path)
    assert quality_report(first.gold_path)["duplicates"] == 0
    assert quality_report(first.gold_path)["top_locations"] == {"New York": 1}
    assert quality_report(first.gold_path)["nulls"]["source_job_id"] == 1

    repeated = run_fake_jobs(
        [fake_record(), fake_record()],
        collected_at=NOW + timedelta(hours=1),
        root=tmp_path,
    )
    assert repeated.bronze_path == first.bronze_path
    assert repeated.bronze_checksum == first.bronze_checksum
    assert repeated.duplicate_count == 2

    second = run_fake_jobs(
        [fake_record()], collected_at=NOW + timedelta(hours=1), root=tmp_path
    )
    assert second.duplicate_count == 1
    assert read_parquet(second.gold_path).height == 1
    assert first.bronze_path.exists()
    assert read_parquet(first.bronze_path).height == 2


def test_vietjobs_scope_raw_bronze_dedup_and_quarantine(tmp_path: Path) -> None:
    fixture = Path(__file__).parent / "fixtures" / "vietjobs_small.csv"
    with fixture.open(newline="", encoding="utf-8") as stream:
        rows = list(csv.DictReader(stream))
    tech = {
        **rows[0],
        "job_title": "Data Engineer",
        "category": "công_nghệ_thông_tin_kỹ_thuật_số",
    }
    invalid = {**tech, "description": "  "}
    result = run_pipeline(
        "vietjobs", [rows[0], tech, tech, invalid], collected_at=NOW, root=tmp_path
    )
    assert result.input_count == 4
    assert result.filtered_count == 1
    assert result.invalid_count == 1
    assert result.duplicate_count == 1
    assert read_parquet(result.bronze_path).height == 4
    assert read_parquet(result.silver_path).height == 2
    assert read_parquet(result.gold_path).height == 1
    quarantine = read_parquet(result.quarantine_path)
    assert quarantine["error_code"][0] == "MAPPING_ERROR"
    assert "description" in quarantine["error_detail"][0]


def test_vietjobs_file_schema_drift_and_corrupt_csv(tmp_path: Path) -> None:
    drift = tmp_path / "drift.csv"
    drift.write_text("job_title,description\nA,B\n", encoding="utf-8")
    with pytest.raises(SchemaDriftError):
        run_vietjobs(drift, collected_at=NOW, root=tmp_path)
    broken = tmp_path / "broken.csv"
    broken.write_bytes(b"\xff\xfe\xfd")
    with pytest.raises(CorruptInputError):
        run_vietjobs(broken, collected_at=NOW, root=tmp_path)


def test_vietjobs_csv_entrypoint_and_empty_gold(tmp_path: Path) -> None:
    fixture = Path(__file__).parent / "fixtures" / "vietjobs_small.csv"
    result = run_vietjobs(fixture, collected_at=NOW, root=tmp_path)
    assert result.input_count == 2
    assert result.filtered_count == 2
    assert read_parquet(result.gold_path).height == 0
    assert quality_report(result.gold_path)["count"] == 0


def test_bad_fake_record_is_quarantined_without_losing_neighbor(tmp_path: Path) -> None:
    result = run_fake_jobs(
        [fake_record(source_url="bad-url"), fake_record()],
        collected_at=NOW,
        root=tmp_path,
    )
    assert result.invalid_count == 1
    assert result.valid_count == 1
    assert read_parquet(result.quarantine_path)["error_code"][0] == "VALIDATION_ERROR"
    assert read_parquet(result.gold_path).height == 1


def test_corrupt_parquet_reader_and_clean_nulls(tmp_path: Path) -> None:
    corrupt = tmp_path / "corrupt.parquet"
    corrupt.write_bytes(b"not parquet")
    with pytest.raises(ValueError, match="corrupt Parquet"):
        read_parquet(corrupt)
    assert clean_text("  \u00a0 ") is None
    assert clean_text(" A\u0301  \t B ") == "Á B"


def test_source_id_beats_hash_for_dedup(tmp_path: Path) -> None:
    from job_pipeline.deduplicate import identity

    first = {
        "source_name": "fake_jobs",
        "source_job_id": "123",
        "source_record_hash": "a" * 64,
    }
    second = {**first, "source_record_hash": "b" * 64}
    assert identity(first) == identity(second)
    assert pl.DataFrame([first, second]).height == 2


def test_crawler_fixture_records_reach_bronze_and_gold(tmp_path: Path) -> None:
    fixture = Path(__file__).parent / "fixtures" / "fake_jobs"
    listing = (fixture / "list.html").read_text(encoding="utf-8")
    details = {
        name: (fixture / name).read_text(encoding="utf-8")
        for name in ("senior-python-developer-0.html", "data-engineer-1.html")
    }

    def respond(request: httpx.Request) -> httpx.Response:
        if str(request.url) == LIST_URL:
            return httpx.Response(200, text=listing)
        name = str(request.url).rsplit("/", 1)[-1]
        if name in details:
            return httpx.Response(200, text=details[name])
        return httpx.Response(404)

    with httpx.Client(transport=httpx.MockTransport(respond)) as client:
        with FakeJobsCrawler(
            client=client,
            raw_html_dir=tmp_path / "raw_html",
            parsed_records_dir=tmp_path / "parsed",
            min_request_interval_seconds=0,
        ) as crawler:
            result = run_fake_jobs(
                crawler.iter_records(), collected_at=NOW, root=tmp_path
            )
    assert result.input_count == 2
    assert read_parquet(result.gold_path).height == 2
    assert len(list((tmp_path / "raw_html").rglob("*.html"))) == 3
