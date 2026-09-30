"""Airflow orchestration for the standalone Job Data Engineering Pipeline."""

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

with DAG(
    dag_id="job_data_pipeline",
    start_date=datetime(2026, 9, 30, tzinfo=UTC),
    schedule=os.getenv("PIPELINE_SCHEDULE", "@daily"),
    catchup=True,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["job-pipeline"],
) as dag:
    acquire = PythonOperator(task_id="acquire", python_callable=acquire_task)
    bronze = PythonOperator(task_id="bronze", python_callable=bronze_task)
    validate_clean = PythonOperator(
        task_id="validate_clean", python_callable=validate_clean_task
    )
    silver = PythonOperator(task_id="silver", python_callable=silver_task)
    deduplicate_gold = PythonOperator(
        task_id="deduplicate_gold", python_callable=deduplicate_gold_task
    )
    quality_check = PythonOperator(
        task_id="quality_check", python_callable=quality_check_task
    )
    export_payload = PythonOperator(
        task_id="export_payload", python_callable=export_payload_task
    )
    deliver = PythonOperator(task_id="deliver", python_callable=deliver_task)

    (
        acquire
        >> bronze
        >> validate_clean
        >> silver
        >> deduplicate_gold
        >> quality_check
        >> export_payload
        >> deliver
    )
