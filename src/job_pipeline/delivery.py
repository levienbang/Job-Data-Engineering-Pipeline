"""Deterministic payload export and checkpointed HTTP delivery."""

import hashlib
import json
import os
import time
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx
from sqlalchemy.orm import sessionmaker

from job_pipeline.checkpoints import (
    get_or_create_batch,
    mark_batch_attempt,
    mark_batch_result,
    record_statuses,
    upsert_record_status,
)
from job_pipeline.config import DeliveryConfig
from job_pipeline.metadata import DeliveryBatch
from job_pipeline.pipeline import CANONICAL_SCHEMA
from job_pipeline.storage import read_parquet

DEFAULT_MAX_IMPORT_BYTES = 1_000_000
RETRYABLE_STATUSES = frozenset({429, 500, 502, 503, 504})
DESCRIPTION_SECTIONS = (
    ("raw_description", "Description"),
    ("raw_requirements", "Requirements"),
    ("raw_experience", "Experience"),
    ("raw_technical_skills", "Technical skills"),
    ("raw_qualifications", "Qualifications"),
)
EXTENDED_FIELDS = {
    "requirements": "raw_requirements",
    "experience": "raw_experience",
    "technical_skills": "raw_technical_skills",
    "soft_skills": "raw_soft_skills",
    "qualifications": "raw_qualifications",
    "languages": "raw_languages",
    "benefits": "raw_benefits",
    "salary": "raw_salary",
    "working_hours": "raw_working_hours",
    "category": "raw_category",
    "country": "raw_country",
}


class OversizeRecordError(ValueError):
    """One serialized record cannot fit in an import file."""


class MissingCompanyError(ValueError):
    """The configured company policy blocks this record."""


@dataclass(frozen=True)
class PayloadBatch:
    payload: bytes
    record_keys: list[str]

    @property
    def payload_hash(self) -> str:
        return hashlib.sha256(self.payload).hexdigest()


@dataclass(frozen=True)
class ExportManifest:
    path: Path
    data: dict[str, Any]

    @property
    def batches(self) -> list[dict[str, Any]]:
        return self.data["batches"]


@dataclass(frozen=True)
class DeliveryReport:
    sent_batches: int
    delivered_records: int
    skipped_records: int
    blocked_records: int
    failed_batches: int
    rejected_records: int = 0


def compose_import_description(row: Mapping[str, Any]) -> str:
    sections: list[tuple[str, str]] = []
    seen: set[str] = set()
    for field, label in DESCRIPTION_SECTIONS:
        value = row.get(field)
        if not isinstance(value, str) or not value.strip():
            continue
        value = value.strip()
        comparison = " ".join(value.split()).casefold()
        if comparison in seen:
            continue
        seen.add(comparison)
        sections.append((label, value))
    if not sections:
        raise ValueError("Gold row has no description")
    if len(sections) == 1 and sections[0][0] == "Description":
        return sections[0][1]
    return "\n\n".join(f"{label}:\n{value}" for label, value in sections)


class DeliveryMapper:
    """Map Gold to the selected import profile."""

    @staticmethod
    def map_gold(
        row: Mapping[str, Any],
        *,
        profile: str = "ai_platform_v1",
        missing_company: str = "block",
    ) -> dict[str, Any]:
        if profile not in {"ai_platform_v1", "extended"}:
            raise ValueError(f"unknown payload profile: {profile}")
        if missing_company not in {"block", "send_null"}:
            raise ValueError(f"unknown missing company policy: {missing_company}")
        company = row.get("raw_company")
        if not isinstance(company, str) or not company.strip():
            if missing_company == "block":
                raise MissingCompanyError("source has no company; delivery held")
            company = None
        mapped = {
            "title": row["raw_title"],
            "company": company,
            "location": row.get("raw_location"),
            "employment_type": row.get("raw_employment_type"),
            "description": compose_import_description(row),
            "source_url": row.get("source_url"),
        }
        if profile == "extended":
            posted = row.get("posted_at")
            mapped.update(
                {
                    "source_name": row.get("source_name"),
                    "source_record_hash": row.get("source_record_hash"),
                    "posted_at": posted.isoformat()
                    if isinstance(posted, datetime)
                    else posted,
                }
            )
            mapped.update(
                {target: row.get(source) for target, source in EXTENDED_FIELDS.items()}
            )
        return mapped


def record_key(row: Mapping[str, Any]) -> str:
    return f"{row['source_name']}:{row['source_record_hash']}"


def serialize_batches(
    mapped: list[tuple[str, dict[str, Any]]],
    *,
    max_bytes: int = DEFAULT_MAX_IMPORT_BYTES,
    max_records: int = 20,
) -> list[PayloadBatch]:
    """Pack JSON arrays strictly under the byte limit and at the record limit."""
    if max_bytes < 3 or max_records < 1:
        raise ValueError("invalid batch limits")
    result: list[PayloadBatch] = []
    members: list[bytes] = []
    keys: list[str] = []
    size = 2
    for key, record in mapped:
        encoded = json.dumps(
            record, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
        if len(encoded) + 2 >= max_bytes:
            raise OversizeRecordError(f"record {key} exceeds {max_bytes} bytes")
        if members and (
            len(members) >= max_records or size + len(encoded) + 1 >= max_bytes
        ):
            result.append(PayloadBatch(b"[" + b",".join(members) + b"]", keys))
            members, keys, size = [], [], 2
        members.append(encoded)
        keys.append(key)
        size += len(encoded) + (1 if len(members) > 1 else 0)
    if members:
        result.append(PayloadBatch(b"[" + b",".join(members) + b"]", keys))
    return result


def _write_unchanged(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and path.read_bytes() == payload:
        return
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_bytes(payload)
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def _gold_rows(path: Path) -> list[dict[str, Any]]:
    frame = read_parquet(path)
    if (
        set(CANONICAL_SCHEMA) - set(frame.columns)
        or frame.filter(frame["schema_version"] != 2).height
    ):
        raise ValueError(f"Gold schema v1 detected at {path}; rerun the pipeline")
    return frame.to_dicts()


def export_batches(
    gold_path: Path,
    *,
    run_id: str,
    sessions: sessionmaker,
    out_dir: Path,
    config: DeliveryConfig,
    retry_rejected: bool = False,
    retry_blocked: bool = False,
) -> ExportManifest:
    """Write deterministic batches and a manifest, without HTTP."""
    rows = _gold_rows(gold_path)
    with sessions() as session:
        statuses = record_statuses(session)
    # Records are only blocked for a missing company, so send_null releases them.
    release_blocked = retry_blocked or config.missing_company == "send_null"
    mapped: list[tuple[str, dict[str, Any]]] = []
    blocked = skipped = 0
    for row in rows:
        key = record_key(row)
        status = statuses.get(key)
        if (
            status == "accepted"
            or (status == "rejected" and not retry_rejected)
            or (status == "blocked" and not release_blocked)
        ):
            skipped += 1
            blocked += status == "blocked"
            continue
        try:
            payload_row = DeliveryMapper.map_gold(
                row, profile=config.profile, missing_company=config.missing_company
            )
        except MissingCompanyError as exc:
            with sessions.begin() as session:
                digest = hashlib.sha256(f"blocked:{key}".encode()).hexdigest()
                batch = get_or_create_batch(
                    session, run_id=run_id, payload_hash=digest, record_keys=[key]
                )
                batch.status = "blocked"
                batch.last_error = str(exc)
                upsert_record_status(
                    session, key, "blocked", batch_id=batch.batch_id, error=str(exc)
                )
            blocked += 1
            continue
        mapped.append((key, payload_row))
    batches = serialize_batches(
        mapped, max_bytes=config.max_bytes, max_records=config.max_records
    )
    source = (
        rows[0]["source_name"]
        if rows
        else gold_path.parent.name.removeprefix("source=")
    )
    directory = out_dir / f"source={source}" / f"run_id={run_id}"
    entries = []
    for number, batch in enumerate(batches, 1):
        filename = f"batch-{number:04d}.json"
        _write_unchanged(directory / filename, batch.payload)
        entries.append(
            {
                "file": filename,
                "payload_hash": batch.payload_hash,
                "record_count": len(batch.record_keys),
                "bytes": len(batch.payload),
                "record_keys": batch.record_keys,
            }
        )
    path = directory / "manifest.json"
    previous = json.loads(path.read_text()) if path.exists() else None
    data = {
        "run_id": run_id,
        "source_name": source,
        "gold_path": str(gold_path),
        "profile": config.profile,
        "missing_company_policy": config.missing_company,
        "max_bytes": config.max_bytes,
        "max_records": config.max_records,
        "created_at": datetime.now(UTC).isoformat(),
        "blocked_count": blocked,
        "skipped_count": skipped,
        "retry_rejected": retry_rejected,
        "retry_blocked": retry_blocked,
        "batches": entries,
    }
    if previous is not None and {
        k: v for k, v in previous.items() if k != "created_at"
    } == {k: v for k, v in data.items() if k != "created_at"}:
        data["created_at"] = previous["created_at"]
    _write_unchanged(
        path,
        json.dumps(
            data, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        ).encode("utf-8"),
    )
    return ExportManifest(path, data)


def _response_result(
    response: httpx.Response, expected: int
) -> tuple[dict[str, Any], dict[int, str]]:
    try:
        body = response.json()
    except ValueError as exc:
        raise ValueError("import response is not JSON") from exc
    if not isinstance(body, dict):
        raise ValueError("import response is not an object")
    for field in ("processed", "inserted", "skipped_duplicates", "failed"):
        if type(body.get(field)) is not int or body[field] < 0:
            raise ValueError(f"import response missing {field}")
    if (
        body["processed"] != expected
        or sum(body[field] for field in ("inserted", "skipped_duplicates", "failed"))
        != expected
    ):
        raise ValueError("import response counts differ from batch size")
    errors = body.get("errors", [])
    if not isinstance(errors, list) or len(errors) != body["failed"]:
        raise ValueError("import response errors do not match failed count")
    rejected: dict[int, str] = {}
    for item in errors:
        if (
            not isinstance(item, dict)
            or type(item.get("record")) is not int
            or not 1 <= item["record"] <= expected
            or not isinstance(item.get("error"), str)
            or item["record"] in rejected
        ):
            raise ValueError("import response has invalid error index")
        rejected[item["record"]] = item["error"]
    return body, rejected


def deliver_gold(
    gold_path: Path | None = None,
    *,
    run_id: str | None = None,
    sessions: sessionmaker,
    endpoint: str,
    client: httpx.Client,
    manifest: Path | ExportManifest | None = None,
    config: DeliveryConfig | None = None,
    retry_rejected: bool = False,
    retry_blocked: bool = False,
    max_bytes: int | None = None,
    max_records: int | None = None,
    timeout_seconds: float | None = None,
    max_attempts: int | None = None,
    backoff_seconds: float = 0.5,
    sleep: Any = time.sleep,
) -> DeliveryReport:
    """Verify exported bytes, POST, and checkpoint each record outcome."""
    if config is None:
        config = DeliveryConfig(
            max_bytes=max_bytes or DEFAULT_MAX_IMPORT_BYTES,
            max_records=max_records or 20,
            timeout_seconds=timeout_seconds or 300,
            max_attempts=max_attempts or 3,
            export_root=gold_path.parents[2] / "export"
            if gold_path
            else Path("data/export"),
        )
    if backoff_seconds < 0:
        raise ValueError("backoff_seconds must be nonnegative")
    if manifest is None:
        if gold_path is None or run_id is None:
            raise ValueError("gold_path and run_id required without manifest")
        export = export_batches(
            gold_path,
            run_id=run_id,
            sessions=sessions,
            out_dir=config.export_root,
            config=config,
            retry_rejected=retry_rejected,
            retry_blocked=retry_blocked,
        )
    elif isinstance(manifest, ExportManifest):
        export = manifest
    else:
        export = ExportManifest(manifest, json.loads(manifest.read_text()))
    data = export.data
    run_id = run_id or data["run_id"]
    retry_rejected = retry_rejected or data.get("retry_rejected", False)
    retry_blocked = retry_blocked or data.get("retry_blocked", False)
    # A concurrent sender may legitimately spend every attempt at full timeout.
    stale_after = timedelta(
        seconds=max(600, config.max_attempts * config.timeout_seconds + 60)
    )
    sent = accepted = rejected = failed = 0
    for item in export.batches:
        payload = (export.path.parent / item["file"]).read_bytes()
        if (
            hashlib.sha256(payload).hexdigest() != item["payload_hash"]
            or len(payload) != item["bytes"]
        ):
            raise ValueError(f"payload hash mismatch: {item['file']}")
        keys = item["record_keys"]
        with sessions.begin() as session:
            checkpoint = get_or_create_batch(
                session,
                run_id=run_id,
                payload_hash=item["payload_hash"],
                record_keys=keys,
            )
            if checkpoint.status == "succeeded":
                statuses = record_statuses(session)
                requested_retry = (
                    retry_rejected
                    and any(statuses.get(key) == "rejected" for key in keys)
                ) or (
                    retry_blocked
                    and any(statuses.get(key) == "blocked" for key in keys)
                )
                if not requested_retry:
                    continue
            last_attempt = checkpoint.last_attempt_at
            if last_attempt is not None and last_attempt.tzinfo is None:
                last_attempt = last_attempt.replace(tzinfo=UTC)
            if (
                checkpoint.status == "sending"
                and last_attempt is not None
                and datetime.now(UTC) - last_attempt < stale_after
            ):
                continue
            batch_id = checkpoint.batch_id
        succeeded = False
        for attempt in range(config.max_attempts):
            with sessions.begin() as session:
                mark_batch_attempt(session, session.get(DeliveryBatch, batch_id))
            response_status = None
            response_body = None
            try:
                response = client.post(
                    endpoint,
                    files={"file": ("jobs.json", payload, "application/json")},
                    headers={"X-API-Key": config.api_key} if config.api_key else None,
                    timeout=config.timeout_seconds,
                )
                response_status = response.status_code
                response.raise_for_status()
                response_body, errors = _response_result(response, len(keys))
            except (httpx.HTTPError, ValueError) as exc:
                error = (
                    "upload exceeds import limit (413)"
                    if response_status == 413
                    else str(exc)
                )
                with sessions.begin() as session:
                    mark_batch_result(
                        session,
                        session.get(DeliveryBatch, batch_id),
                        status="failed",
                        response_status=response_status,
                        error=error,
                        response=response_body,
                    )
                if (
                    response_status is not None
                    and response_status not in RETRYABLE_STATUSES
                ) or attempt == config.max_attempts - 1:
                    break
                sleep(backoff_seconds * 2**attempt)
            else:
                with sessions.begin() as session:
                    mark_batch_result(
                        session,
                        session.get(DeliveryBatch, batch_id),
                        status="succeeded",
                        response_status=response_status,
                        response=response_body,
                    )
                    for index, key in enumerate(keys, 1):
                        status = "rejected" if index in errors else "accepted"
                        upsert_record_status(
                            session,
                            key,
                            status,
                            batch_id=batch_id,
                            error=errors.get(index),
                        )
                sent += 1
                rejected += len(errors)
                accepted += len(keys) - len(errors)
                succeeded = True
                break
        if not succeeded:
            failed += 1
    return DeliveryReport(
        sent,
        accepted,
        data.get("skipped_count", 0),
        data["blocked_count"],
        failed,
        rejected,
    )
