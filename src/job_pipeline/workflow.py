"""Small orchestration callables; all transformations live in standalone modules."""

import hashlib
import json
import logging
import os
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from uuid import NAMESPACE_URL, uuid5

import httpx

from job_pipeline.checkpoints import (
    finish_delivery_run,
    finish_run,
    record_source_asset,
    start_run,
    update_run_counts,
)
from job_pipeline.config import DeliveryConfig
from job_pipeline.delivery import deliver_gold, export_batches
from job_pipeline.metadata import make_session_factory
from job_pipeline.pipeline import read_vietjobs_rows
from job_pipeline.quality import quality_report
from job_pipeline.sources.fake_jobs import LIST_URL, FakeJobsCrawler
from job_pipeline.stages import (
    BronzeStage,
    CleanStage,
    deduplicate_gold,
    publish_silver,
    snapshot_bronze,
    validate_clean_bronze,
)
from job_pipeline.storage import sha256_file

LOGGER = logging.getLogger(__name__)


def _root() -> Path:
    return Path(os.getenv("PIPELINE_DATA_ROOT", "data"))


def _source() -> str:
    source = os.getenv("PIPELINE_SOURCE", "vietjobs")
    if source not in {"vietjobs", "fake_jobs"}:
        raise ValueError(f"unsupported PIPELINE_SOURCE: {source}")
    return source


def _sessions():
    url = os.environ["PIPELINE_DATABASE_URL"]
    if not url.startswith("postgresql+psycopg2://"):
        raise ValueError("Airflow workflow requires PostgreSQL metadata URL")
    return make_session_factory(url)


def _run_id(context: dict) -> str:
    return str(uuid5(NAMESPACE_URL, f"job_pipeline:{_source()}:{context['run_id']}"))


def _collected_at(context: dict) -> datetime:
    value = context.get("logical_date") or datetime.now(UTC)
    return value if value.tzinfo is not None else value.replace(tzinfo=UTC)


def acquire_task(**context: object) -> dict[str, str]:
    """Check local VietJobs input or crawl and persist Fake Jobs source records."""
    source = _source()
    start_run(_sessions(), run_id=_run_id(context), source_name=source)
    if source == "vietjobs":
        path = Path(os.getenv("VIETJOBS_CSV_PATH", "data/VietJobs.csv"))
        read_vietjobs_rows(path)
        return {"source": source, "path": str(path), "checksum": sha256_file(path)}

    with FakeJobsCrawler() as crawler:
        records = list(crawler.iter_raw())
    content = json.dumps(
        records, ensure_ascii=False, sort_keys=True, default=str
    ).encode()
    digest = hashlib.sha256(content).hexdigest()
    date = _collected_at(context).date().isoformat()
    path = (
        _root()
        / "acquired"
        / f"source={source}"
        / f"ingestion_date={date}"
        / f"records-{digest}.json"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_bytes(content)
        try:
            os.link(temporary, path)
        except FileExistsError:
            if path.read_bytes() != content:
                raise ValueError(
                    "acquired record snapshot differs from content hash"
                ) from None
    finally:
        temporary.unlink(missing_ok=True)
    return {"source": source, "path": str(path), "checksum": digest}


def bronze_task(**context: object) -> dict[str, object]:
    acquired = context["ti"].xcom_pull(task_ids="acquire")
    source = acquired["source"]
    path = Path(acquired["path"])
    records = (
        read_vietjobs_rows(path)
        if source == "vietjobs"
        else json.loads(path.read_text())
    )
    bronze = snapshot_bronze(
        source, records, collected_at=_collected_at(context), root=_root()
    )
    record_source_asset(
        _sessions(),
        source_name=source,
        source_url_or_path=str(path) if source == "vietjobs" else LIST_URL,
        content_hash=acquired["checksum"],
        run_id=_run_id(context),
    )
    return {
        "path": str(bronze.path),
        "checksum": bronze.checksum,
        "input_count": bronze.input_count,
        "source": source,
    }


def validate_clean_task(**context: object) -> dict[str, object]:
    state = context["ti"].xcom_pull(task_ids="bronze")
    bronze = BronzeStage(
        Path(state["path"]), state["checksum"], state["input_count"], state["source"]
    )
    clean = validate_clean_bronze(bronze, root=_root())
    return {
        "prepared_path": str(clean.prepared_path),
        "quarantine_path": str(clean.quarantine_path),
        "valid_count": clean.valid_count,
        "invalid_count": clean.invalid_count,
        "filtered_count": clean.filtered_count,
    }


def silver_task(**context: object) -> str:
    state = context["ti"].xcom_pull(task_ids="validate_clean")
    clean = CleanStage(
        Path(state["prepared_path"]),
        Path(state["quarantine_path"]),
        state["valid_count"],
        state["invalid_count"],
        state["filtered_count"],
    )
    return str(publish_silver(clean, root=_root()))


def deduplicate_gold_task(**context: object) -> str:
    silver_path = Path(context["ti"].xcom_pull(task_ids="silver"))
    gold_path, duplicates = deduplicate_gold(
        silver_path, source=_source(), root=_root()
    )
    bronze = context["ti"].xcom_pull(task_ids="bronze")
    clean = context["ti"].xcom_pull(task_ids="validate_clean")
    update_run_counts(
        _sessions(),
        _run_id(context),
        input_count=bronze["input_count"],
        valid_count=clean["valid_count"],
        invalid_count=clean["invalid_count"],
        duplicate_count=duplicates,
    )
    return str(gold_path)


def quality_check_task(**context: object) -> dict[str, object]:
    path = Path(context["ti"].xcom_pull(task_ids="deduplicate_gold"))
    report = quality_report(path)
    if report["duplicates"] or report["missing_title"] or report["missing_description"]:
        raise ValueError("Gold quality checks failed")
    LOGGER.info("Gold quality report: %s", report)
    return {
        "count": report["count"],
        "duplicates": report["duplicates"],
        "missing_title": report["missing_title"],
        "missing_description": report["missing_description"],
    }


def export_payload_task(**context: object) -> str:
    path = Path(context["ti"].xcom_pull(task_ids="deduplicate_gold"))
    config = DeliveryConfig.from_env()
    if "PIPELINE_EXPORT_ROOT" not in os.environ:
        # Keep exports with the run's data, not under the cwd.
        config = replace(config, export_root=_root() / "export")
    manifest = export_batches(
        path,
        run_id=_run_id(context),
        sessions=_sessions(),
        out_dir=config.export_root,
        config=config,
    )
    return str(manifest.path)


def deliver_task(**context: object) -> dict[str, int]:
    config = DeliveryConfig.from_env()
    if not config.enabled:
        manifest_path = Path(context["ti"].xcom_pull(task_ids="export_payload"))
        blocked = json.loads(manifest_path.read_text())["blocked_count"]
        finish_delivery_run(
            _sessions(), _run_id(context), status="succeeded", blocked_count=blocked
        )
        return {"delivered_records": 0, "skipped_records": 0}
    path = Path(context["ti"].xcom_pull(task_ids="export_payload"))
    endpoint = os.environ["AI_PLATFORM_IMPORT_URL"]
    with httpx.Client() as client:
        report = deliver_gold(
            manifest=path,
            run_id=_run_id(context),
            sessions=_sessions(),
            endpoint=endpoint,
            client=client,
            config=config,
        )
    if report.blocked_records or report.rejected_records:
        LOGGER.warning(
            "Delivery held %s blocked and %s rejected records",
            report.blocked_records,
            report.rejected_records,
        )
    complete = report.failed_batches == 0
    finish_delivery_run(
        _sessions(),
        _run_id(context),
        status="succeeded" if complete else "failed",
        blocked_count=report.blocked_records,
        error_message=None if complete else "delivery batches failed",
    )
    if not complete:
        raise RuntimeError("delivery batches failed")
    return {
        "delivered_records": report.delivered_records,
        "skipped_records": report.skipped_records,
    }


def log_failure(context: dict) -> None:
    """Airflow callback records terminal task failure without hiding its cause."""
    error = context.get("exception")
    LOGGER.error("Job pipeline task failed: %s", error)
    try:
        finish_run(
            _sessions(), _run_id(context), status="failed", error_message=str(error)
        )
    except Exception:
        LOGGER.exception("Could not persist failed run status")
