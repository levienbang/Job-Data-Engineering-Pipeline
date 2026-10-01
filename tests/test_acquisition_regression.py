"""Fixed Gold output captured before the Milestone 8.1 refactor."""

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from job_pipeline.pipeline import run_pipeline
from job_pipeline.storage import read_parquet

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.mark.parametrize("source", ["vietjobs", "fake_jobs"])
def test_acquisition_gold_snapshot(source: str, tmp_path: Path) -> None:
    raw = json.loads((FIXTURES / "acquisition_raw.json").read_text())[source]
    expected = json.loads((FIXTURES / "acquisition_gold.json").read_text())[source]
    result = run_pipeline(
        source, raw, collected_at=datetime(2026, 9, 30, tzinfo=UTC), root=tmp_path
    )
    actual = (
        read_parquet(result.gold_path).drop("collected_at", "bronze_path").to_dicts()
    )
    assert json.loads(json.dumps(actual, default=str)) == expected


def test_fake_jobs_acquisition_matches_pre_refactor_fixture(tmp_path: Path) -> None:
    import httpx

    from job_pipeline.pipeline import FAKE_JOBS_COLUMNS
    from job_pipeline.sources.base import RawSource
    from job_pipeline.sources.fake_jobs import LIST_URL, FakeJobsCrawler

    def respond(request: httpx.Request) -> httpx.Response:
        name = (
            "list.html"
            if str(request.url) == LIST_URL
            else request.url.path.rsplit("/", 1)[-1]
        )
        return httpx.Response(200, text=(FIXTURES / "fake_jobs" / name).read_text())

    with httpx.Client(transport=httpx.MockTransport(respond)) as client:
        with FakeJobsCrawler(
            client=client, raw_html_dir=tmp_path, min_request_interval_seconds=0
        ) as source:
            assert isinstance(source, RawSource)
            raw = list(source.iter_raw())
    assert (
        raw == json.loads((FIXTURES / "acquisition_raw.json").read_text())["fake_jobs"]
    )
    assert all(set(row) == FAKE_JOBS_COLUMNS for row in raw)
    assert isinstance(raw[0]["posted_at"], str)
    assert raw[1]["posted_at"] is None


def test_unknown_source_is_rejected_during_validation(tmp_path: Path) -> None:
    from job_pipeline.stages import BronzeStage, validate_clean_bronze

    with pytest.raises(ValueError, match="unsupported source: unknown"):
        validate_clean_bronze(
            BronzeStage(tmp_path / "missing.parquet", "", 0, "unknown")
        )


def test_invalid_posted_at_is_quarantined_without_losing_neighbor(
    tmp_path: Path,
) -> None:
    raw = json.loads((FIXTURES / "acquisition_raw.json").read_text())["fake_jobs"]
    result = run_pipeline(
        "fake_jobs",
        [{**raw[0], "posted_at": "invalid"}, raw[1]],
        collected_at=datetime(2026, 9, 30, tzinfo=UTC),
        root=tmp_path,
    )
    assert result.invalid_count == result.valid_count == 1
    assert read_parquet(result.quarantine_path)["error_code"][0] == "MAPPING_ERROR"
