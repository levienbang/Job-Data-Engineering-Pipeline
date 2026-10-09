"""Airflow DAG structure without network, metadata DB, or scheduler services."""

import csv
import importlib.util
from datetime import timedelta
from pathlib import Path

import httpx
import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import select

from job_pipeline import workflow
from job_pipeline.metadata import (
    DeliveryBatch,
    PipelineRun,
    SourceAsset,
    make_session_factory,
)


def test_airflow_dag_import_and_dependencies(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("AIRFLOW_HOME", str(tmp_path / "airflow"))
    pytest.importorskip("airflow")
    path = Path(__file__).parents[1] / "dags" / "job_pipeline.py"
    spec = importlib.util.spec_from_file_location("job_pipeline_dag_test", path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    expected = [
        "acquire",
        "bronze",
        "validate_clean",
        "silver",
        "deduplicate_gold",
        "quality_check",
        "export_payload",
        "deliver",
    ]
    assert set(module.dags) == {"vietjobs", "arbeitnow"}
    for source, dag in module.dags.items():
        assert dag.dag_id == f"job_pipeline_{source}"
        assert getattr(module, dag.dag_id) is dag
        assert dag.params["source"] == source
        assert set(dag.task_ids) == set(expected)
        assert dag.catchup is False
        assert dag.max_active_runs == 1
        for before, after in zip(expected, expected[1:], strict=False):
            assert dag.get_task(before).downstream_task_ids == {after}
        for task in dag.tasks:
            assert task.retries == 2
            assert task.execution_timeout == timedelta(minutes=30)
    assert module.dags["vietjobs"].schedule_interval is None
    assert module.dags["arbeitnow"].schedule_interval == "@daily"


def test_dag_params_choose_source_over_environment(monkeypatch) -> None:
    monkeypatch.setenv("PIPELINE_SOURCE", "vietjobs")
    context = {"run_id": "manual__1", "params": {"source": "arbeitnow"}}
    assert workflow._source(context) == "arbeitnow"
    assert workflow._source({"run_id": "manual__1"}) == "vietjobs"
    # The same Airflow run id under two DAGs maps to two pipeline runs.
    assert workflow._run_id(context) != workflow._run_id(
        {**context, "params": {"source": "vietjobs"}}
    )
    with pytest.raises(ValueError, match="unsupported"):
        workflow._source({"params": {"source": "unknown"}})


def test_workflow_stages_and_missing_company_hold(monkeypatch, tmp_path: Path) -> None:
    fixture = Path(__file__).parent / "fixtures" / "vietjobs_small.csv"
    with fixture.open(newline="", encoding="utf-8") as stream:
        rows = list(csv.DictReader(stream))
    rows[0]["job_title"] = "Data Engineer"
    rows[0]["category"] = "công_nghệ_thông_tin_kỹ_thuật_số"
    source_path = tmp_path / "vietjobs.csv"
    with source_path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)

    config = Config(str(Path(__file__).parents[1] / "alembic.ini"))
    config.set_main_option(
        "script_location", str(Path(__file__).parents[1] / "migrations")
    )
    config.set_main_option("sqlalchemy.url", f"sqlite:///{tmp_path / 'metadata.db'}")
    command.upgrade(config, "head")
    sessions = make_session_factory(f"sqlite:///{tmp_path / 'metadata.db'}")
    monkeypatch.setattr(workflow, "_sessions", lambda: sessions)
    monkeypatch.setenv("PIPELINE_SOURCE", "vietjobs")
    monkeypatch.setenv("VIETJOBS_CSV_PATH", str(source_path))
    monkeypatch.setenv("PIPELINE_DATA_ROOT", str(tmp_path / "data"))
    monkeypatch.setenv("AI_PLATFORM_IMPORT_URL", "https://example.test/jobs/import")

    class TaskInstance:
        values: dict[str, object] = {}

        def xcom_pull(self, *, task_ids: str) -> object:
            return self.values[task_ids]

    ti = TaskInstance()
    context = {"ti": ti, "run_id": "scheduled__2026-09-30", "logical_date": None}
    stages = (
        ("acquire", workflow.acquire_task),
        ("bronze", workflow.bronze_task),
        ("validate_clean", workflow.validate_clean_task),
        ("silver", workflow.silver_task),
        ("deduplicate_gold", workflow.deduplicate_gold_task),
        ("quality_check", workflow.quality_check_task),
    )
    for name, function in stages:
        ti.values[name] = function(**context)
    assert ti.values["quality_check"]["count"] == 1
    ti.values["export_payload"] = workflow.export_payload_task(**context)
    assert Path(ti.values["export_payload"]).is_relative_to(
        tmp_path / "data" / "export"
    )
    assert workflow.deliver_task(**context)["delivered_records"] == 0
    with sessions() as session:
        assert session.scalar(select(PipelineRun)).status == "succeeded"
        batch = session.scalar(select(DeliveryBatch))
        assert batch.status == "blocked"
        assert batch.attempt_count == 0


REAL_HTTPX_CLIENT = httpx.Client


def _vietjobs_csv(path: Path, title: str = "Data Engineer") -> Path:
    fixture = Path(__file__).parent / "fixtures" / "vietjobs_small.csv"
    with fixture.open(newline="", encoding="utf-8") as stream:
        rows = list(csv.DictReader(stream))
    rows[0]["job_title"] = title
    rows[0]["category"] = "công_nghệ_thông_tin_kỹ_thuật_số"
    with path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    return path


def _dag_environment(monkeypatch, tmp_path: Path):
    config = Config(str(Path(__file__).parents[1] / "alembic.ini"))
    config.set_main_option(
        "script_location", str(Path(__file__).parents[1] / "migrations")
    )
    config.set_main_option("sqlalchemy.url", f"sqlite:///{tmp_path / 'metadata.db'}")
    command.upgrade(config, "head")
    sessions = make_session_factory(f"sqlite:///{tmp_path / 'metadata.db'}")
    monkeypatch.setattr(workflow, "_sessions", lambda: sessions)
    monkeypatch.setenv("PIPELINE_SOURCE", "vietjobs")
    monkeypatch.setenv("PIPELINE_DATA_ROOT", str(tmp_path / "data"))
    monkeypatch.setenv("AI_PLATFORM_IMPORT_URL", "https://example.test/jobs/import")
    monkeypatch.setenv("DELIVERY_MISSING_COMPANY", "send_null")
    return sessions


def _pipeline_run(session, airflow_run_id: str) -> PipelineRun:
    return session.get(PipelineRun, workflow._run_id({"run_id": airflow_run_id}))


def _run_dag(monkeypatch, run_id: str, csv_path: Path, respond) -> dict[str, int]:
    """Run every DAG task in order, as the scheduler would, with a fake endpoint."""
    monkeypatch.setenv("VIETJOBS_CSV_PATH", str(csv_path))
    monkeypatch.setattr(
        workflow.httpx,
        "Client",
        lambda **kwargs: REAL_HTTPX_CLIENT(transport=httpx.MockTransport(respond)),
    )

    class TaskInstance:
        def __init__(self) -> None:
            self.values: dict[str, object] = {}

        def xcom_pull(self, *, task_ids: str) -> object:
            return self.values[task_ids]

    ti = TaskInstance()
    context = {"ti": ti, "run_id": run_id, "logical_date": None}
    for name in (
        "acquire",
        "bronze",
        "validate_clean",
        "silver",
        "deduplicate_gold",
        "quality_check",
        "export_payload",
    ):
        ti.values[name] = getattr(workflow, f"{name}_task")(**context)
    return workflow.deliver_task(**context)


def test_dag_rerun_tracks_run_and_does_not_redeliver(
    monkeypatch, tmp_path: Path
) -> None:
    sessions = _dag_environment(monkeypatch, tmp_path)
    source = _vietjobs_csv(tmp_path / "vietjobs.csv")
    calls = 0

    def respond(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(
            200,
            json={"processed": 1, "inserted": 1, "skipped_duplicates": 0, "failed": 0},
        )

    first = _run_dag(monkeypatch, "run-1", source, respond)
    second = _run_dag(monkeypatch, "run-2", source, respond)
    assert first["delivered_records"] == 1
    assert second["delivered_records"] == 0
    assert calls == 1
    with sessions() as session:
        assert _pipeline_run(session, "run-1").status == "succeeded"
        assert _pipeline_run(session, "run-2").duplicate_count == 1
        assert session.scalar(select(SourceAsset)).last_run_id == workflow._run_id(
            {"run_id": "run-2"}
        )


def test_dag_run_status_for_rejection_and_failed_batch(
    monkeypatch, tmp_path: Path
) -> None:
    sessions = _dag_environment(monkeypatch, tmp_path)
    rejected = {
        "processed": 1,
        "inserted": 0,
        "skipped_duplicates": 0,
        "failed": 1,
        "errors": [{"record": 1, "error": "company rejected"}],
    }
    _run_dag(
        monkeypatch,
        "rejected",
        _vietjobs_csv(tmp_path / "first.csv"),
        lambda request: httpx.Response(200, json=rejected),
    )
    with sessions() as session:
        run = _pipeline_run(session, "rejected")
        assert (run.status, run.rejected_count) == ("succeeded", 1)

    changed = _vietjobs_csv(tmp_path / "second.csv", title="Senior Data Engineer")
    with pytest.raises(RuntimeError, match="delivery batches failed"):
        _run_dag(monkeypatch, "failed", changed, lambda request: httpx.Response(413))
    with sessions() as session:
        assert _pipeline_run(session, "failed").status == "failed"


def test_arbeitnow_workflow_through_quality_twice(monkeypatch, tmp_path):
    import json
    from datetime import UTC, datetime

    from job_pipeline.sources.arbeitnow import ArbeitnowSource

    sessions = _dag_environment(monkeypatch, tmp_path)
    monkeypatch.setenv("PIPELINE_SOURCE", "arbeitnow")
    monkeypatch.setenv("ARBEITNOW_MAX_PAGES", "5")
    fixture = Path(__file__).parent / "fixtures" / "arbeitnow"

    def respond(request):
        body = json.loads(
            (fixture / f"page-{request.url.params['page']}.json").read_text()
        )
        return httpx.Response(200, json=body)

    with httpx.Client(transport=httpx.MockTransport(respond)) as client:
        monkeypatch.setattr(
            workflow,
            "ArbeitnowSource",
            lambda **kwargs: ArbeitnowSource(client, sleep=lambda _: None, **kwargs),
        )

        class TI:
            def __init__(self):
                self.values = {}

            def xcom_pull(self, *, task_ids):
                return self.values[task_ids]

        for number in (1, 2):
            ti = TI()
            context = {
                "ti": ti,
                "run_id": f"arbeitnow-{number}",
                "logical_date": datetime(2026, 10, 9, 12, tzinfo=UTC),
            }
            for name in (
                "acquire",
                "bronze",
                "validate_clean",
                "silver",
                "deduplicate_gold",
                "quality_check",
            ):
                ti.values[name] = getattr(workflow, f"{name}_task")(**context)
            assert ti.values["quality_check"]["count"] == 6
            assert ti.values["validate_clean"]["filtered_count"] == 9
            with sessions() as session:
                run = _pipeline_run(session, context["run_id"])
                assert run.input_count == 15
                assert run.valid_count == 6
                assert run.invalid_count == 0
                assert run.duplicate_count == (0 if number == 1 else 6)
