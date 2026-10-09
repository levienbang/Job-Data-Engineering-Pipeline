"""Airflow orchestration for the standalone Job Data Engineering Pipeline.

One DAG per source, built from the same tasks. Each DAG passes its source in
``params`` so sources run independently; delivery still follows
``DELIVERY_ENABLED``.
"""

import os
from datetime import UTC, datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from job_pipeline.workflow import (
    acquire_task,
    bronze_task,
    deduplicate_gold_task,
    deliver_task,
    export_payload_task,
    log_failure,
    quality_check_task,
    silver_task,
    validate_clean_task,
)

DEFAULT_ARGS = {
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=30),
    "on_failure_callback": log_failure,
}
TASKS = (
    ("acquire", acquire_task),
    ("bronze", bronze_task),
    ("validate_clean", validate_clean_task),
    ("silver", silver_task),
    ("deduplicate_gold", deduplicate_gold_task),
    ("quality_check", quality_check_task),
    ("export_payload", export_payload_task),
    ("deliver", deliver_task),
)
# VietJobs is a static published CSV: run it by hand. Arbeitnow is a live API
# that only returns current jobs, so past intervals cannot be backfilled.
SOURCES = {
    "vietjobs": {"schedule": None, "catchup": False},
    "arbeitnow": {
        "schedule": os.getenv("ARBEITNOW_SCHEDULE", "@daily"),
        "catchup": False,
    },
}


def build_dag(source: str, *, schedule: str | None, catchup: bool) -> DAG:
    with DAG(
        dag_id=f"job_pipeline_{source}",
        start_date=datetime(2026, 10, 1, tzinfo=UTC),
        schedule=schedule,
        catchup=catchup,
        max_active_runs=1,
        default_args=DEFAULT_ARGS,
        params={"source": source},
        tags=["job-pipeline", source],
    ) as dag:
        previous = None
        for task_id, function in TASKS:
            task = PythonOperator(task_id=task_id, python_callable=function)
            if previous is not None:
                previous >> task
            previous = task
    return dag


dags = {source: build_dag(source, **settings) for source, settings in SOURCES.items()}
# Airflow discovers DAG objects in module globals.
globals().update({dag.dag_id: dag for dag in dags.values()})
