"""Transactional run, source asset, and delivery checkpoint operations."""

from datetime import UTC, datetime
from uuid import uuid4

from sqlalchemy import func, select
from sqlalchemy.orm import Session, sessionmaker

from job_pipeline.metadata import (
    DeliveryBatch,
    DeliveryRecord,
    PipelineRun,
    SourceAsset,
    utcnow,
)
from job_pipeline.pipeline import PipelineResult


def start_run(
    sessions: sessionmaker,
    *,
    run_id: str,
    source_name: str,
    code_version: str = "0.1.0",
) -> None:
    """An existing successful run remains the checkpoint for the same run ID."""
    with sessions.begin() as session:
        existing = session.get(PipelineRun, run_id)
        if existing is not None:
            if existing.source_name != source_name:
                raise ValueError("run ID belongs to a different source")
            return
        session.add(
            PipelineRun(
                run_id=run_id,
                pipeline_name="job_pipeline",
                source_name=source_name,
                started_at=utcnow(),
                status="running",
                code_version=code_version,
            )
        )


def finish_run(
    sessions: sessionmaker,
    run_id: str,
    *,
    result: PipelineResult | None = None,
    status: str,
    error_message: str | None = None,
    delivered_count: int | None = None,
    blocked_count: int | None = None,
    rejected_count: int | None = None,
) -> None:
    with sessions.begin() as session:
        run = session.get(PipelineRun, run_id)
        if run is None:
            raise ValueError(f"unknown run ID: {run_id}")
        run.status = status
        run.finished_at = utcnow()
        run.error_message = error_message
        if result is not None:
            run.input_count = result.input_count
            run.valid_count = result.valid_count
            run.invalid_count = result.invalid_count
            run.duplicate_count = result.duplicate_count
        if delivered_count is not None:
            run.delivered_count = delivered_count
        if blocked_count is not None:
            run.blocked_count = blocked_count
        if rejected_count is not None:
            run.rejected_count = rejected_count


def run_record_counts(sessions: sessionmaker, run_id: str) -> dict[str, int]:
    """Count record outcomes stored for a run's batches, so reruns stay accurate."""
    counts = {"accepted": 0, "rejected": 0, "blocked": 0}
    with sessions() as session:
        rows = session.execute(
            select(DeliveryRecord.status, func.count())
            .join(DeliveryBatch, DeliveryRecord.last_batch_id == DeliveryBatch.batch_id)
            .where(DeliveryBatch.run_id == run_id)
            .group_by(DeliveryRecord.status)
        )
        counts.update({status: count for status, count in rows})
    return counts


def finish_delivery_run(
    sessions: sessionmaker,
    run_id: str,
    *,
    status: str,
    blocked_count: int,
    result: PipelineResult | None = None,
    error_message: str | None = None,
) -> dict[str, int]:
    """Persist run outcome with delivered/rejected totals read from checkpoints."""
    counts = run_record_counts(sessions, run_id)
    finish_run(
        sessions,
        run_id,
        result=result,
        status=status,
        error_message=error_message,
        delivered_count=counts["accepted"],
        blocked_count=blocked_count,
        rejected_count=counts["rejected"],
    )
    return counts


def update_run_counts(
    sessions: sessionmaker,
    run_id: str,
    *,
    input_count: int,
    valid_count: int,
    invalid_count: int,
    duplicate_count: int,
) -> None:
    with sessions.begin() as session:
        run = session.get(PipelineRun, run_id)
        if run is None:
            raise ValueError(f"unknown run ID: {run_id}")
        run.input_count = input_count
        run.valid_count = valid_count
        run.invalid_count = invalid_count
        run.duplicate_count = duplicate_count


def record_source_asset(
    sessions: sessionmaker,
    *,
    source_name: str,
    source_url_or_path: str,
    content_hash: str,
    run_id: str,
    status: str = "processed",
    http_status: int | None = None,
) -> SourceAsset:
    with sessions.begin() as session:
        asset = session.scalar(
            select(SourceAsset).where(
                SourceAsset.source_name == source_name,
                SourceAsset.source_url_or_path == source_url_or_path,
            )
        )
        if asset is None:
            asset = SourceAsset(
                asset_id=str(uuid4()),
                source_name=source_name,
                source_url_or_path=source_url_or_path,
                content_hash=content_hash,
                first_seen_at=utcnow(),
                last_seen_at=utcnow(),
                last_run_id=run_id,
                status=status,
                http_status=http_status,
            )
            session.add(asset)
        else:
            asset.content_hash = content_hash
            asset.last_seen_at = utcnow()
            asset.last_run_id = run_id
            asset.status = status
            asset.http_status = http_status
        session.flush()
        session.expunge(asset)
        return asset


def successful_record_keys(session: Session) -> set[str]:
    keys: set[str] = set()
    for stored in session.scalars(
        select(DeliveryBatch.record_keys).where(DeliveryBatch.status == "succeeded")
    ):
        keys.update(stored)
    return keys


def record_statuses(session: Session) -> dict[str, str]:
    """Explicit record outcomes override legacy whole-batch successes."""
    statuses = {key: "accepted" for key in successful_record_keys(session)}
    statuses.update(
        {row.record_key: row.status for row in session.scalars(select(DeliveryRecord))}
    )
    return statuses


def upsert_record_status(
    session: Session,
    key: str,
    status: str,
    *,
    batch_id: str | None = None,
    error: str | None = None,
) -> None:
    row = session.get(DeliveryRecord, key)
    if row is None:
        row = DeliveryRecord(record_key=key, source_name=key.split(":", 1)[0])
        session.add(row)
    row.status = status
    row.last_batch_id = batch_id
    row.error = error
    row.updated_at = utcnow()


def get_or_create_batch(
    session: Session,
    *,
    run_id: str,
    payload_hash: str,
    record_keys: list[str],
    schema_version: int = 2,
) -> DeliveryBatch:
    batch = session.scalar(
        select(DeliveryBatch)
        .where(DeliveryBatch.payload_hash == payload_hash)
        .with_for_update()
    )
    if batch is None:
        batch = DeliveryBatch(
            batch_id=str(uuid4()),
            run_id=run_id,
            payload_hash=payload_hash,
            record_count=len(record_keys),
            record_keys=record_keys,
            schema_version=schema_version,
            attempt_count=0,
            status="pending",
        )
        session.add(batch)
        session.flush()
    return batch


def mark_batch_attempt(session: Session, batch: DeliveryBatch) -> None:
    batch.attempt_count += 1
    batch.status = "sending"
    batch.last_error = None
    batch.last_attempt_at = utcnow()


def mark_batch_result(
    session: Session,
    batch: DeliveryBatch,
    *,
    status: str,
    response_status: int | None = None,
    error: str | None = None,
    response: dict[str, object] | None = None,
) -> None:
    batch.status = status
    batch.response_status = response_status
    batch.last_error = error
    if status == "succeeded":
        batch.delivered_at = datetime.now(UTC)
    if response is not None:
        batch.processed_count = int(response.get("processed", 0))
        batch.inserted_count = int(response.get("inserted", 0))
        batch.skipped_duplicate_count = int(response.get("skipped_duplicates", 0))
        batch.failed_count = int(response.get("failed", 0))
