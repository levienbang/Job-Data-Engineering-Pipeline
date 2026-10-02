"""Airflow DAG structure without network, metadata DB, or scheduler services."""

import csv
import importlib.util
from datetime import timedelta
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import select

from job_pipeline import workflow
from job_pipeline.metadata import DeliveryBatch, PipelineRun, make_session_factory


def test_airflow_dag_import_and_dependencies(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("AIRFLOW_HOME", str(tmp_path / "airflow"))
    pytest.importorskip("airflow")
    path = Path(__file__).parents[1] / "dags" / "job_pipeline.py"
    spec = importlib.util.spec_from_file_location("job_pipeline_dag_test", path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    dag = module.dag
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
    assert set(dag.task_ids) == set(expected)
    assert dag.catchup is True
    assert dag.max_active_runs == 1
    for before, after in zip(expected, expected[1:], strict=False):
        assert dag.get_task(before).downstream_task_ids == {after}
    for task in dag.tasks:
        assert task.retries == 2
        assert task.execution_timeout == timedelta(minutes=30)


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
