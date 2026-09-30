"""Standalone source-to-delivery service with operational run metadata."""

import logging
import os
from collections.abc import Callable, Iterable
from dataclasses import replace
from datetime import datetime
from pathlib import Path

import httpx
from sqlalchemy.orm import sessionmaker

from job_pipeline.checkpoints import (
    finish_delivery_run,
    finish_run,
    record_source_asset,
    start_run,
)
from job_pipeline.config import DeliveryConfig
from job_pipeline.delivery import DeliveryReport, deliver_gold
from job_pipeline.pipeline import PipelineResult, run_fake_jobs, run_vietjobs
from job_pipeline.quality import quality_report
from job_pipeline.sources.fake_jobs import FakeJobRecord

LOGGER = logging.getLogger(__name__)


def _run_and_deliver(
    source: str,
    *,
    run_id: str,
    sessions: sessionmaker,
    endpoint: str,
    client: httpx.Client,
    asset_location: str,
    execute: Callable[[], PipelineResult],
) -> tuple[PipelineResult, DeliveryReport]:
    start_run(sessions, run_id=run_id, source_name=source)
    result = None
    try:
        result = execute()
        record_source_asset(
            sessions,
            source_name=source,
            source_url_or_path=asset_location,
            content_hash=result.input_checksum or result.bronze_checksum,
            run_id=run_id,
        )
        report = quality_report(result.gold_path)
        if (
            report["duplicates"]
            or report["missing_title"]
            or report["missing_description"]
        ):
            raise ValueError("Gold quality checks failed")
        config = DeliveryConfig.from_env()
        if "PIPELINE_EXPORT_ROOT" not in os.environ:
            config = replace(config, export_root=result.gold_path.parents[2] / "export")
        delivery = deliver_gold(
            result.gold_path,
            run_id=run_id,
            sessions=sessions,
            endpoint=endpoint,
            client=client,
            config=config,
        )
        complete = delivery.failed_batches == 0
        if delivery.blocked_records or delivery.rejected_records:
            LOGGER.warning(
                "Delivery held %s blocked and %s rejected records",
                delivery.blocked_records,
                delivery.rejected_records,
            )
        finish_delivery_run(
            sessions,
            run_id,
            result=result,
            status="succeeded" if complete else "failed",
            blocked_count=delivery.blocked_records,
            error_message=None if complete else "delivery batches failed",
        )
        if not complete:
            raise RuntimeError("delivery batches failed")
        return result, delivery
    except Exception as exc:
        finish_run(
            sessions, run_id, result=result, status="failed", error_message=str(exc)
        )
        raise


def execute_vietjobs(
    csv_path: Path,
    *,
    run_id: str,
    collected_at: datetime,
    sessions: sessionmaker,
    endpoint: str,
    client: httpx.Client,
    root: Path = Path("data"),
) -> tuple[PipelineResult, DeliveryReport]:
    return _run_and_deliver(
        "vietjobs",
        run_id=run_id,
        sessions=sessions,
        endpoint=endpoint,
        client=client,
        asset_location=str(csv_path),
        execute=lambda: run_vietjobs(csv_path, collected_at=collected_at, root=root),
    )


def execute_fake_jobs(
    records: Iterable[FakeJobRecord],
    *,
    run_id: str,
    collected_at: datetime,
    sessions: sessionmaker,
    endpoint: str,
    client: httpx.Client,
    asset_location: str,
    root: Path = Path("data"),
) -> tuple[PipelineResult, DeliveryReport]:
    return _run_and_deliver(
        "fake_jobs",
        run_id=run_id,
        sessions=sessions,
        endpoint=endpoint,
        client=client,
        asset_location=asset_location,
        execute=lambda: run_fake_jobs(records, collected_at=collected_at, root=root),
    )
