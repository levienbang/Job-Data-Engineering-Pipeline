"""Milestone 8 contract, export, and response behavior using local fixtures."""

import csv
import hashlib
import json
from dataclasses import asdict, replace
from datetime import UTC, datetime
from pathlib import Path

import httpx
import pytest
from alembic import command
from alembic.config import Config
from pydantic import ValidationError
from sqlalchemy import select

from job_pipeline.__main__ import main
from job_pipeline.checkpoints import (
    get_or_create_batch,
    record_statuses,
    start_run,
)
from job_pipeline.clean import clean_text
from job_pipeline.config import DeliveryConfig
from job_pipeline.delivery import (
    DeliveryMapper,
    OversizeRecordError,
    compose_import_description,
    deliver_gold,
    export_batches,
    serialize_batches,
)
from job_pipeline.metadata import DeliveryRecord, PipelineRun, make_session_factory
from job_pipeline.pipeline import run_fake_jobs, run_pipeline
from job_pipeline.quality import quality_report
from job_pipeline.sources.vietjobs import (
    SOURCE_FIELDS,
    map_vietjobs_row,
)
from job_pipeline.storage import read_parquet, write_parquet
from job_pipeline.validate import ValidatedJob

NOW = datetime(2026, 9, 30, tzinfo=UTC)
FIXTURE = Path(__file__).parent / "fixtures" / "vietjobs_small.csv"


@pytest.fixture
def sessions(tmp_path: Path):
    url = f"sqlite:///{tmp_path / 'metadata.db'}"
    config = Config(str(Path(__file__).parents[1] / "alembic.ini"))
    config.set_main_option(
        "script_location", str(Path(__file__).parents[1] / "migrations")
    )
    config.set_main_option("sqlalchemy.url", url)
    command.upgrade(config, "head")
    command.check(config)
    return make_session_factory(url)


def _gold(tmp_path: Path, count: int = 3) -> Path:
    records = [
        dict(
            title=f"Engineer {n}",
            company="Acme",
            location="Paris",
            description=f"Build APIs {n}",
            source_url=f"https://realpython.github.io/fake-jobs/{n}.html",
            posted_at=None,
        )
        for n in range(count)
    ]
    return run_fake_jobs(records, collected_at=NOW, root=tmp_path).gold_path


def test_vietjobs_v2_fields_hash_and_description() -> None:
    with FIXTURE.open(newline="", encoding="utf-8") as stream:
        row = next(csv.DictReader(stream))
    job = map_vietjobs_row(row, collected_at=NOW)
    source_json = json.dumps(
        {"source_name": "vietjobs", "record": dict(row)},
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    assert job.source_record_hash == hashlib.sha256(source_json.encode()).hexdigest()
    assert job.schema_version == 2
    assert job.raw_description == row["description"]
    for target, source in SOURCE_FIELDS.items():
        assert getattr(job, target) == (row[source] if row[source].strip() else None)
    cleaned = {
        field: clean_text(value) if isinstance(value, str) else value
        for field, value in asdict(job).items()
    }
    expected = "Description:\nVận hành hệ thống xử lý nước thải.\n\nExperience:\n1 năm"
    assert compose_import_description(cleaned) == expected


def test_v2_validation_and_old_gold_error(tmp_path: Path) -> None:
    gold = _gold(tmp_path, 1)
    row = read_parquet(gold).to_dicts()[0]
    with pytest.raises(ValidationError):
        ValidatedJob.model_validate({**row, "schema_version": 1})
    with pytest.raises(ValidationError):
        ValidatedJob.model_validate({**row, "unexpected": "x"})
    old = read_parquet(gold).drop("raw_salary")
    write_parquet(old, gold)
    with pytest.raises(ValueError, match="rerun the pipeline"):
        quality_report(gold)


def test_quality_reports_new_metrics_without_removing_near_duplicates(
    tmp_path: Path,
) -> None:
    first = dict(
        title="Same",
        company="Acme",
        location="Paris",
        description="Shared",
        source_url="https://realpython.github.io/fake-jobs/1.html",
        posted_at=None,
    )
    second = {**first, "source_url": "https://realpython.github.io/fake-jobs/2.html"}
    gold = run_fake_jobs([first, second], collected_at=NOW, root=tmp_path).gold_path
    report = quality_report(gold)
    assert report["count"] == 2
    assert report["near_duplicates"] == 1
    assert report["schema_versions"] == {2: 2}
    assert report["missing_company"] == 0
    assert report["description_bytes"] == {"min": 6, "p50": 6, "max": 6}
    assert "raw_salary" in report["nulls"]


@pytest.mark.parametrize("profile", ["ai_platform_v1", "extended"])
@pytest.mark.parametrize("policy", ["block", "send_null"])
def test_mapper_profiles_and_company_policy(
    tmp_path: Path, profile: str, policy: str
) -> None:
    row = read_parquet(_gold(tmp_path, 1)).to_dicts()[0]
    row["raw_company"] = None
    if policy == "block":
        with pytest.raises(ValueError, match="company"):
            DeliveryMapper.map_gold(row, profile=profile, missing_company=policy)
        return
    payload = DeliveryMapper.map_gold(row, profile=profile, missing_company=policy)
    assert payload["company"] is None
    assert len(payload) == (20 if profile == "extended" else 6)
    assert payload["description"] == row["raw_description"]


def test_batch_limits_and_config_validation() -> None:
    mapped = [(str(n), {"title": str(n), "company": "A"}) for n in range(3)]
    assert len(serialize_batches(mapped, max_records=2)) == 2
    assert all(
        len(batch.payload) < 40 for batch in serialize_batches(mapped, max_bytes=40)
    )
    with pytest.raises(OversizeRecordError):
        serialize_batches(mapped, max_bytes=10)
    with pytest.raises(ValueError, match="MAX_RECORDS"):
        DeliveryConfig(max_records=0)


def test_export_idempotent_hash_guard_partial_retry_and_legacy_checkpoint(
    sessions, tmp_path: Path
) -> None:
    gold = _gold(tmp_path)
    start_run(sessions, run_id="run-1", source_name="fake_jobs")
    config = DeliveryConfig(max_records=3, export_root=tmp_path / "export")
    first = export_batches(
        gold,
        run_id="run-1",
        sessions=sessions,
        out_dir=config.export_root,
        config=config,
    )
    payload = first.path.parent / first.batches[0]["file"]
    original = payload.read_bytes()
    mtime = payload.stat().st_mtime_ns
    second = export_batches(
        gold,
        run_id="run-1",
        sessions=sessions,
        out_dir=config.export_root,
        config=config,
    )
    assert first.data == second.data
    assert payload.stat().st_mtime_ns == mtime
    payload.write_bytes(b"tampered")
    calls = 0

    def respond(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(
            200,
            json={
                "processed": 3,
                "inserted": 2,
                "skipped_duplicates": 0,
                "failed": 1,
                "errors": [{"record": 2, "error": "x"}],
            },
        )

    with httpx.Client(transport=httpx.MockTransport(respond)) as client:
        with pytest.raises(ValueError, match="payload hash"):
            deliver_gold(
                sessions=sessions,
                endpoint="https://example.test/jobs/import",
                client=client,
                manifest=first.path,
                config=config,
            )
        assert calls == 0
        payload.write_bytes(original)
        report = deliver_gold(
            sessions=sessions,
            endpoint="https://example.test/jobs/import",
            client=client,
            manifest=first.path,
            config=config,
        )
        assert report.delivered_records == 2
        assert report.rejected_records == 1
        assert report.failed_batches == 0
        assert calls == 1
        again = deliver_gold(
            gold,
            run_id="run-1",
            sessions=sessions,
            endpoint="https://example.test/jobs/import",
            client=client,
            config=config,
        )
        assert again.sent_batches == 0
        assert calls == 1
    with sessions() as session:
        statuses = record_statuses(session)
        assert list(statuses.values()).count("accepted") == 2
        assert list(statuses.values()).count("rejected") == 1
        rejected_key = session.scalar(
            select(DeliveryRecord.record_key).where(DeliveryRecord.status == "rejected")
        )
        assert rejected_key is not None

    retry = export_batches(
        gold,
        run_id="run-1",
        sessions=sessions,
        out_dir=config.export_root,
        config=config,
        retry_rejected=True,
    )
    assert retry.batches[0]["record_keys"] == [rejected_key]
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
        retried = deliver_gold(
            sessions=sessions,
            endpoint="https://example.test/jobs/import",
            client=client,
            manifest=retry.path,
            config=config,
        )
    assert retried.delivered_records == 1
    with sessions() as session:
        assert record_statuses(session)[rejected_key] == "accepted"


def test_cli_run_and_export(tmp_path: Path, sessions, monkeypatch, capsys) -> None:
    with FIXTURE.open(newline="", encoding="utf-8") as stream:
        rows = list(csv.DictReader(stream))
    rows[0]["job_title"] = "Data Engineer"
    rows[0]["category"] = "công_nghệ_thông_tin_kỹ_thuật_số"
    csv_path = tmp_path / "input.csv"
    with csv_path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    monkeypatch.setenv("PIPELINE_DATABASE_URL", str(sessions.kw["bind"].url))
    monkeypatch.setenv("PIPELINE_EXPORT_ROOT", str(tmp_path / "export"))
    monkeypatch.setenv("DELIVERY_MISSING_COMPANY", "send_null")
    root = tmp_path / "data"
    assert (
        main(
            ["run", "--source", "vietjobs", "--csv", str(csv_path), "--root", str(root)]
        )
        == 0
    )
    assert json.loads(capsys.readouterr().out)["schema_versions"] == {"2": 1}
    assert main(["export", "--source", "vietjobs", "--root", str(root)]) == 0
    summary = json.loads(capsys.readouterr().out)
    assert summary["batches"] == 1
    assert summary["blocked"] == 0
    batch = json.loads(next((tmp_path / "export").rglob("batch-*.json")).read_text())
    assert batch[0]["company"] is None
    assert len(batch[0]) == 6


def test_413_is_not_retried_and_503_is_retried(sessions, tmp_path: Path) -> None:
    gold = _gold(tmp_path, 1)
    start_run(sessions, run_id="run-1", source_name="fake_jobs")
    calls = 0

    def too_large(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(413)

    with httpx.Client(transport=httpx.MockTransport(too_large)) as client:
        result = deliver_gold(
            gold,
            run_id="run-1",
            sessions=sessions,
            endpoint="https://example.test/jobs/import",
            client=client,
            backoff_seconds=0,
        )
    assert result.failed_batches == 1
    assert calls == 1

    statuses = iter([503, 200])

    def recover(request: httpx.Request) -> httpx.Response:
        status = next(statuses)
        if status == 503:
            return httpx.Response(status)
        return httpx.Response(
            200,
            json={
                "processed": 1,
                "inserted": 1,
                "skipped_duplicates": 0,
                "failed": 0,
            },
        )

    with httpx.Client(transport=httpx.MockTransport(recover)) as client:
        result = deliver_gold(
            gold,
            run_id="run-1",
            sessions=sessions,
            endpoint="https://example.test/jobs/import",
            client=client,
            backoff_seconds=0,
            sleep=lambda _: None,
        )
    assert result.delivered_records == 1


def test_cli_deliver_and_disabled_workflow(
    tmp_path: Path, sessions, monkeypatch, capsys
) -> None:
    from job_pipeline import workflow

    gold = _gold(tmp_path, 1)
    start_run(sessions, run_id="run-1", source_name="fake_jobs")
    config = DeliveryConfig(export_root=tmp_path / "export")
    manifest = export_batches(
        gold,
        run_id="run-1",
        sessions=sessions,
        out_dir=config.export_root,
        config=config,
    )
    monkeypatch.setenv("PIPELINE_DATABASE_URL", str(sessions.kw["bind"].url))
    monkeypatch.setenv("PIPELINE_EXPORT_ROOT", str(config.export_root))
    real_client = httpx.Client
    monkeypatch.setattr(
        httpx,
        "Client",
        lambda: real_client(
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
        ),
    )
    assert (
        main(
            [
                "deliver",
                "--source",
                "fake_jobs",
                "--endpoint",
                "https://example.test/jobs/import",
                "--manifest",
                str(manifest.path),
            ]
        )
        == 0
    )
    assert json.loads(capsys.readouterr().out)["accepted"] == 1
    monkeypatch.setenv("DELIVERY_ENABLED", "false")
    monkeypatch.setattr(workflow, "_sessions", lambda: sessions)
    monkeypatch.setattr(workflow, "_run_id", lambda context: "run-1")

    class TaskInstance:
        def xcom_pull(self, *, task_ids: str) -> str:
            assert task_ids == "export_payload"
            return str(manifest.path)

    assert workflow.deliver_task(ti=TaskInstance())["delivered_records"] == 0


def test_legacy_success_and_retry_single_rejected_record(
    sessions, tmp_path: Path
) -> None:
    gold = _gold(tmp_path, 1)
    row = read_parquet(gold).to_dicts()[0]
    key = f"{row['source_name']}:{row['source_record_hash']}"
    start_run(sessions, run_id="legacy", source_name="fake_jobs")
    config = DeliveryConfig(export_root=tmp_path / "export")
    first = export_batches(
        gold,
        run_id="legacy",
        sessions=sessions,
        out_dir=config.export_root,
        config=config,
    )
    with sessions.begin() as session:
        old = get_or_create_batch(
            session,
            run_id="legacy",
            payload_hash=first.batches[0]["payload_hash"],
            record_keys=[key],
            schema_version=1,
        )
        old.status = "succeeded"
    with sessions() as session:
        assert record_statuses(session)[key] == "accepted"

    # An explicit rejected outcome supersedes the old whole-batch checkpoint.
    from job_pipeline.checkpoints import upsert_record_status

    with sessions.begin() as session:
        upsert_record_status(session, key, "rejected", error="x")
    manifest = export_batches(
        gold,
        run_id="legacy",
        sessions=sessions,
        out_dir=config.export_root,
        config=config,
        retry_rejected=True,
    )
    calls = 0

    def respond(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(
            200,
            json={
                "processed": 1,
                "inserted": 1,
                "skipped_duplicates": 0,
                "failed": 0,
            },
        )

    with httpx.Client(transport=httpx.MockTransport(respond)) as client:
        result = deliver_gold(
            sessions=sessions,
            endpoint="https://example.test/jobs/import",
            client=client,
            manifest=manifest.path,
            config=config,
        )
    assert result.delivered_records == 1
    assert calls == 1


def test_v1_record_is_quarantined_and_csv_e2e_does_not_redeliver(
    sessions, tmp_path: Path, monkeypatch
) -> None:
    from job_pipeline import stages

    original = stages.MAPPERS["fake_jobs"]

    def old_version(raw, *, collected_at):
        return replace(original(raw, collected_at=collected_at), schema_version=1)

    monkeypatch.setitem(stages.MAPPERS, "fake_jobs", old_version)
    fake = dict(
        title="Engineer",
        company="Acme",
        location="Paris",
        description="Build APIs",
        source_url="https://realpython.github.io/fake-jobs/1.html",
        posted_at=None,
    )
    invalid = run_fake_jobs([fake], collected_at=NOW, root=tmp_path / "invalid")
    quarantine = read_parquet(invalid.quarantine_path)
    assert quarantine["error_code"][0] == "VALIDATION_ERROR"
    monkeypatch.setitem(stages.MAPPERS, "fake_jobs", original)

    with FIXTURE.open(newline="", encoding="utf-8") as stream:
        rows = list(csv.DictReader(stream))
    rows[0]["job_title"] = "Data Engineer"
    rows[0]["category"] = "công_nghệ_thông_tin_kỹ_thuật_số"
    result = run_pipeline("vietjobs", rows, collected_at=NOW, root=tmp_path / "e2e")
    start_run(sessions, run_id="e2e", source_name="vietjobs")
    config = DeliveryConfig(
        missing_company="send_null", export_root=tmp_path / "export"
    )
    manifest = export_batches(
        result.gold_path,
        run_id="e2e",
        sessions=sessions,
        out_dir=config.export_root,
        config=config,
    )
    calls = 0

    def respond(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(
            200,
            json={
                "processed": 1,
                "inserted": 1,
                "skipped_duplicates": 0,
                "failed": 0,
            },
        )

    with httpx.Client(transport=httpx.MockTransport(respond)) as client:
        first = deliver_gold(
            sessions=sessions,
            endpoint="https://example.test/jobs/import",
            client=client,
            manifest=manifest.path,
            config=config,
        )
        second = deliver_gold(
            result.gold_path,
            run_id="e2e",
            sessions=sessions,
            endpoint="https://example.test/jobs/import",
            client=client,
            config=config,
        )
    assert first.delivered_records == 1
    assert second.sent_batches == 0
    assert calls == 1


_REAL_CLIENT = httpx.Client


def _ok_client(counter: list[int]) -> httpx.Client:
    def handler(request: httpx.Request) -> httpx.Response:
        counter.append(1)
        jobs = json.loads(
            request.content.split(b"\r\n\r\n", 1)[1].rsplit(b"\r\n--", 1)[0]
        )
        return httpx.Response(
            200,
            json={
                "processed": len(jobs),
                "inserted": len(jobs) - 1,
                "skipped_duplicates": 0,
                "failed": 1,
                "errors": [{"record": 1, "error": "unknown skill"}],
            },
        )

    return _REAL_CLIENT(transport=httpx.MockTransport(handler))


def test_cli_rerun_keeps_run_counts_and_export_run_is_closed(
    tmp_path: Path, sessions, monkeypatch, capsys
) -> None:
    gold = _gold(tmp_path, 3)
    monkeypatch.setenv("PIPELINE_DATABASE_URL", str(sessions.kw["bind"].url))
    monkeypatch.setenv("PIPELINE_EXPORT_ROOT", str(tmp_path / "export"))
    assert main(["export", "--source", "fake_jobs", "--root", str(tmp_path)]) == 0
    manifest = Path(json.loads(capsys.readouterr().out)["manifest"])
    run_id = json.loads(manifest.read_text())["run_id"]
    with sessions() as session:
        assert session.get(PipelineRun, run_id).status == "succeeded"

    counter: list[int] = []
    monkeypatch.setattr(httpx, "Client", lambda: _ok_client(counter))
    args = [
        "deliver",
        "--source",
        "fake_jobs",
        "--endpoint",
        "https://example.test/jobs/import",
        "--manifest",
        str(manifest),
    ]
    assert main(args) == 0
    assert main(args) == 0
    assert len(counter) == 1
    with sessions() as session:
        run = session.get(PipelineRun, run_id)
        assert (run.status, run.delivered_count, run.rejected_count) == (
            "succeeded",
            2,
            1,
        )
    assert gold.exists()


def test_send_null_releases_records_blocked_by_missing_company(
    tmp_path: Path, sessions
) -> None:
    gold = run_fake_jobs(
        [
            dict(
                title="Engineer",
                company=None,
                location="Paris",
                description="Build APIs",
                source_url="https://realpython.github.io/fake-jobs/1.html",
                posted_at=None,
            )
        ],
        collected_at=NOW,
        root=tmp_path,
    ).gold_path
    start_run(sessions, run_id="run-1", source_name="fake_jobs")
    blocked = export_batches(
        gold,
        run_id="run-1",
        sessions=sessions,
        out_dir=tmp_path / "export",
        config=DeliveryConfig(export_root=tmp_path / "export"),
    )
    assert (blocked.data["blocked_count"], blocked.batches) == (1, [])

    start_run(sessions, run_id="run-2", source_name="fake_jobs")
    released = export_batches(
        gold,
        run_id="run-2",
        sessions=sessions,
        out_dir=tmp_path / "export",
        config=DeliveryConfig(
            missing_company="send_null", export_root=tmp_path / "export"
        ),
    )
    assert released.data["blocked_count"] == 0
    assert len(released.batches) == 1
    payload = json.loads(
        (released.path.parent / released.batches[0]["file"]).read_text()
    )
    assert payload[0]["company"] is None


@pytest.mark.parametrize("api_key", [None, "s3cret"])
def test_delivery_sends_platform_api_key_only_when_configured(
    sessions, tmp_path: Path, api_key
) -> None:
    gold = _gold(tmp_path, 1)
    start_run(sessions, run_id="run-1", source_name="fake_jobs")
    seen = []

    def respond(request: httpx.Request) -> httpx.Response:
        seen.append(request.headers.get("X-API-Key"))
        return httpx.Response(
            200,
            json={"processed": 1, "inserted": 1, "skipped_duplicates": 0, "failed": 0},
        )

    config = DeliveryConfig(export_root=tmp_path / "export", api_key=api_key)
    with httpx.Client(transport=httpx.MockTransport(respond)) as client:
        result = deliver_gold(
            gold,
            run_id="run-1",
            sessions=sessions,
            endpoint="https://example.test/jobs/import",
            client=client,
            config=config,
        )
    assert result.failed_batches == 0
    assert seen == [api_key]
    assert "s3cret" not in repr(config)
