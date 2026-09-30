"""Offline metadata, migration, and import delivery checks."""

import json
from datetime import UTC, datetime
from pathlib import Path

import httpx
import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import inspect, select

from job_pipeline.checkpoints import (
    finish_run,
    record_source_asset,
    start_run,
)
from job_pipeline.delivery import (
    DeliveryMapper,
    MissingCompanyError,
    OversizeRecordError,
    deliver_gold,
    serialize_batches,
)
from job_pipeline.metadata import (
    DeliveryBatch,
    PipelineRun,
    SourceAsset,
    make_session_factory,
)
from job_pipeline.pipeline import run_fake_jobs
from job_pipeline.service import execute_fake_jobs
from job_pipeline.sources.fake_jobs import FakeJobRecord

NOW = datetime(2026, 9, 30, tzinfo=UTC)


@pytest.fixture
def sessions(tmp_path: Path):
    config = Config(str(Path(__file__).parents[1] / "alembic.ini"))
    config.set_main_option(
        "script_location", str(Path(__file__).parents[1] / "migrations")
    )
    config.set_main_option("sqlalchemy.url", f"sqlite:///{tmp_path / 'metadata.db'}")
    command.upgrade(config, "head")
    factory = make_session_factory(f"sqlite:///{tmp_path / 'metadata.db'}")
    assert set(inspect(factory.kw["bind"]).get_table_names()) >= {
        "pipeline_runs",
        "source_assets",
        "delivery_batches",
    }
    return factory


def _gold(tmp_path: Path, *, company: str | None = "Example Labs") -> Path:
    record = FakeJobRecord(
        title="Engineer",
        company=company,
        location="Paris",
        description="Build APIs",
        source_url="https://realpython.github.io/fake-jobs/job-1.html",
        posted_at=None,
    )
    return run_fake_jobs([record], collected_at=NOW, root=tmp_path).gold_path


def test_metadata_transaction_and_migration_round_trip(
    sessions, tmp_path: Path
) -> None:
    start_run(sessions, run_id="run-1", source_name="fake_jobs")
    start_run(sessions, run_id="run-1", source_name="fake_jobs")
    asset = record_source_asset(
        sessions,
        source_name="fake_jobs",
        source_url_or_path="https://example.test/list",
        content_hash="a" * 64,
        run_id="run-1",
    )
    record_source_asset(
        sessions,
        source_name="fake_jobs",
        source_url_or_path="https://example.test/list",
        content_hash="b" * 64,
        run_id="run-1",
    )
    with sessions() as session:
        assert session.get(PipelineRun, "run-1").status == "running"
        assert session.get(SourceAsset, asset.asset_id).content_hash == "b" * 64
        assert len(session.scalars(select(SourceAsset)).all()) == 1
    finish_run(sessions, "run-1", status="succeeded", delivered_count=1)
    with sessions() as session:
        assert session.get(PipelineRun, "run-1").delivered_count == 1

    config = Config(str(Path(__file__).parents[1] / "alembic.ini"))
    config.set_main_option(
        "script_location", str(Path(__file__).parents[1] / "migrations")
    )
    config.set_main_option("sqlalchemy.url", f"sqlite:///{tmp_path / 'metadata.db'}")
    command.check(config)
    command.downgrade(config, "base")
    assert "pipeline_runs" not in inspect(sessions.kw["bind"]).get_table_names()
    command.upgrade(config, "head")


def test_byte_batching_and_oversize_rejection() -> None:
    mapped = [
        (str(i), {"title": "T" * 20, "company": "C", "description": "D"})
        for i in range(3)
    ]
    batches = serialize_batches(mapped, max_bytes=110)
    assert len(batches) == 3
    assert all(len(batch.payload) < 110 for batch in batches)
    assert sum(len(json.loads(batch.payload)) for batch in batches) == 3
    with pytest.raises(OversizeRecordError):
        serialize_batches(mapped[:1], max_bytes=20)


def test_mapping_keeps_exact_contract_and_missing_company(tmp_path: Path) -> None:
    gold_path = _gold(tmp_path)
    import polars as pl

    row = pl.read_parquet(gold_path).to_dicts()[0]
    mapped = DeliveryMapper.map_gold(row)
    assert set(mapped) == {
        "title",
        "company",
        "location",
        "employment_type",
        "description",
        "source_url",
    }
    assert mapped["company"] == "Example Labs"
    with pytest.raises(MissingCompanyError):
        DeliveryMapper.map_gold({**row, "raw_company": None})


def test_successful_delivery_is_not_repeated(sessions, tmp_path: Path) -> None:
    gold_path = _gold(tmp_path)
    start_run(sessions, run_id="run-1", source_name="fake_jobs")
    requests = []

    def respond(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        assert request.url.path == "/jobs/import"
        assert b'name="file"' in request.content
        assert b"jobs.json" in request.content
        return httpx.Response(
            200,
            json={"processed": 1, "inserted": 1, "skipped_duplicates": 0, "failed": 0},
        )

    with httpx.Client(transport=httpx.MockTransport(respond)) as client:
        first = deliver_gold(
            gold_path,
            run_id="run-1",
            sessions=sessions,
            endpoint="https://example.test/jobs/import",
            client=client,
        )
        second = deliver_gold(
            gold_path,
            run_id="run-1",
            sessions=sessions,
            endpoint="https://example.test/jobs/import",
            client=client,
        )
    assert first.delivered_records == 1
    assert second.skipped_records == 1
    assert len(requests) == 1
    with sessions() as session:
        batch = session.scalar(select(DeliveryBatch))
        assert batch.status == "succeeded"
        assert batch.attempt_count == 1
        assert batch.inserted_count == 1


def test_timeout_retry_and_failed_batch_remains_retryable(
    sessions, tmp_path: Path
) -> None:
    gold_path = _gold(tmp_path)
    start_run(sessions, run_id="run-1", source_name="fake_jobs")
    calls = 0

    def timeout(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        raise httpx.ReadTimeout("timeout", request=request)

    with httpx.Client(transport=httpx.MockTransport(timeout)) as client:
        report = deliver_gold(
            gold_path,
            run_id="run-1",
            sessions=sessions,
            endpoint="https://example.test/jobs/import",
            client=client,
            max_attempts=2,
            backoff_seconds=0,
            sleep=lambda _: None,
        )
    assert report.failed_batches == 1
    assert calls == 2
    with sessions() as session:
        batch = session.scalar(select(DeliveryBatch))
        assert batch.status == "failed"
        assert batch.attempt_count == 2

    with httpx.Client(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(
                200,
                json={
                    "processed": 1,
                    "inserted": 1,
                    "skipped_duplicates": 0,
                    "failed": 0,
                },
            )
        )
    ) as client:
        retry = deliver_gold(
            gold_path,
            run_id="run-1",
            sessions=sessions,
            endpoint="https://example.test/jobs/import",
            client=client,
        )
    assert retry.delivered_records == 1
    with sessions() as session:
        assert session.scalar(select(DeliveryBatch)).attempt_count == 3


def test_missing_company_is_held_without_http(sessions, tmp_path: Path) -> None:
    gold_path = _gold(tmp_path, company=None)
    start_run(sessions, run_id="run-1", source_name="fake_jobs")

    def no_request(request: httpx.Request) -> httpx.Response:
        pytest.fail(f"unexpected request: {request.url}")

    with httpx.Client(transport=httpx.MockTransport(no_request)) as client:
        report = deliver_gold(
            gold_path,
            run_id="run-1",
            sessions=sessions,
            endpoint="https://example.test/jobs/import",
            client=client,
        )
    assert report.blocked_records == 1
    with sessions() as session:
        batch = session.scalar(select(DeliveryBatch))
        assert batch.status == "blocked"
        assert batch.attempt_count == 0


def test_standalone_service_tracks_run_and_source_checkpoint(
    sessions, tmp_path: Path
) -> None:
    record = FakeJobRecord(
        title="Engineer",
        company="Example Labs",
        location="Paris",
        description="Build APIs",
        source_url="https://realpython.github.io/fake-jobs/job-1.html",
        posted_at=None,
    )
    calls = 0

    def respond(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(
            200,
            json={"processed": 1, "inserted": 1, "skipped_duplicates": 0, "failed": 0},
        )

    with httpx.Client(transport=httpx.MockTransport(respond)) as client:
        first, first_delivery = execute_fake_jobs(
            [record],
            run_id="run-1",
            collected_at=NOW,
            sessions=sessions,
            endpoint="https://example.test/jobs/import",
            client=client,
            asset_location="https://realpython.github.io/fake-jobs/",
            root=tmp_path,
        )
        second, second_delivery = execute_fake_jobs(
            [record],
            run_id="run-2",
            collected_at=NOW,
            sessions=sessions,
            endpoint="https://example.test/jobs/import",
            client=client,
            asset_location="https://realpython.github.io/fake-jobs/",
            root=tmp_path,
        )
    assert first.bronze_checksum == second.bronze_checksum
    assert first_delivery.delivered_records == 1
    assert second_delivery.skipped_records == 1
    assert calls == 1
    with sessions() as session:
        assert session.get(PipelineRun, "run-1").status == "succeeded"
        assert session.get(PipelineRun, "run-2").duplicate_count == 1
        assert session.scalar(select(SourceAsset)).last_run_id == "run-2"


def test_failed_count_without_error_details_fails_batch(
    sessions, tmp_path: Path
) -> None:
    gold_path = _gold(tmp_path)
    start_run(sessions, run_id="run-1", source_name="fake_jobs")
    with httpx.Client(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(
                200,
                json={
                    "processed": 1,
                    "inserted": 0,
                    "skipped_duplicates": 0,
                    "failed": 1,
                },
            )
        )
    ) as client:
        report = deliver_gold(
            gold_path,
            run_id="run-1",
            sessions=sessions,
            endpoint="https://example.test/jobs/import",
            client=client,
        )
    assert report.failed_batches == 1
    with sessions() as session:
        batch = session.scalar(select(DeliveryBatch))
        assert batch.status == "failed"
        assert batch.failed_count is None


def test_new_gold_record_does_not_redeliver_successful_record(
    sessions, tmp_path: Path
) -> None:
    first_path = _gold(tmp_path)
    start_run(sessions, run_id="run-1", source_name="fake_jobs")
    posted_payloads = []

    def respond(request: httpx.Request) -> httpx.Response:
        posted_payloads.append(request.content)
        return httpx.Response(
            200,
            json={"processed": 1, "inserted": 1, "skipped_duplicates": 0, "failed": 0},
        )

    with httpx.Client(transport=httpx.MockTransport(respond)) as client:
        deliver_gold(
            first_path,
            run_id="run-1",
            sessions=sessions,
            endpoint="https://example.test/jobs/import",
            client=client,
        )
        second = FakeJobRecord(
            title="Second Engineer",
            company="Example Labs",
            location="Paris",
            description="Test APIs",
            source_url="https://realpython.github.io/fake-jobs/job-2.html",
            posted_at=None,
        )
        run_fake_jobs([second], collected_at=NOW, root=tmp_path)
        start_run(sessions, run_id="run-2", source_name="fake_jobs")
        report = deliver_gold(
            first_path,
            run_id="run-2",
            sessions=sessions,
            endpoint="https://example.test/jobs/import",
            client=client,
        )
    assert report.skipped_records == 1
    assert report.delivered_records == 1
    assert len(posted_payloads) == 2


def test_retryable_http_status_retries_but_bad_request_does_not(
    sessions, tmp_path: Path
) -> None:
    gold_path = _gold(tmp_path)
    start_run(sessions, run_id="run-1", source_name="fake_jobs")
    statuses = iter([503, 400])
    calls = 0

    def respond(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(next(statuses))

    with httpx.Client(transport=httpx.MockTransport(respond)) as client:
        report = deliver_gold(
            gold_path,
            run_id="run-1",
            sessions=sessions,
            endpoint="https://example.test/jobs/import",
            client=client,
            max_attempts=3,
            backoff_seconds=0,
            sleep=lambda _: None,
        )
    assert report.failed_batches == 1
    assert calls == 2
    with sessions() as session:
        batch = session.scalar(select(DeliveryBatch))
        assert batch.response_status == 400
        assert batch.attempt_count == 2
