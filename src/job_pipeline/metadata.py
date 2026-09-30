"""Operational metadata only; application job rows remain in Gold Parquet."""

from datetime import UTC, datetime
from uuid import uuid4

from sqlalchemy import (
    JSON,
    CheckConstraint,
    Column,
    DateTime,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
    create_engine,
)
from sqlalchemy.orm import declarative_base, sessionmaker

Base = declarative_base()


def utcnow() -> datetime:
    return datetime.now(UTC)


class PipelineRun(Base):
    __tablename__ = "pipeline_runs"

    run_id = Column(String(36), primary_key=True)
    pipeline_name = Column(String(100), nullable=False)
    source_name = Column(String(50), nullable=False)
    started_at = Column(DateTime(timezone=True), nullable=False)
    finished_at = Column(DateTime(timezone=True))
    status = Column(String(20), nullable=False)
    input_count = Column(Integer, nullable=False, default=0)
    valid_count = Column(Integer, nullable=False, default=0)
    invalid_count = Column(Integer, nullable=False, default=0)
    duplicate_count = Column(Integer, nullable=False, default=0)
    delivered_count = Column(Integer, nullable=False, default=0)
    blocked_count = Column(Integer, nullable=False, default=0)
    rejected_count = Column(Integer, nullable=False, default=0)
    error_message = Column(Text)
    code_version = Column(String(40), nullable=False)

    __table_args__ = (CheckConstraint("status IN ('running', 'succeeded', 'failed')"),)


class SourceAsset(Base):
    __tablename__ = "source_assets"

    asset_id = Column(String(36), primary_key=True)
    source_name = Column(String(50), nullable=False)
    source_url_or_path = Column(Text, nullable=False)
    content_hash = Column(String(64), nullable=False)
    http_status = Column(Integer)
    first_seen_at = Column(DateTime(timezone=True), nullable=False)
    last_seen_at = Column(DateTime(timezone=True), nullable=False)
    last_run_id = Column(String(36), ForeignKey("pipeline_runs.run_id"))
    status = Column(String(20), nullable=False)

    __table_args__ = (
        UniqueConstraint("source_name", "source_url_or_path", name="uq_source_asset"),
        CheckConstraint("status IN ('seen', 'processed', 'failed')"),
    )


class DeliveryBatch(Base):
    __tablename__ = "delivery_batches"

    batch_id = Column(String(36), primary_key=True)
    run_id = Column(String(36), ForeignKey("pipeline_runs.run_id"), nullable=False)
    payload_hash = Column(String(64), unique=True, nullable=False)
    record_count = Column(Integer, nullable=False)
    record_keys = Column(JSON, nullable=False)
    schema_version = Column(Integer, nullable=False, default=1)
    attempt_count = Column(Integer, nullable=False, default=0)
    last_attempt_at = Column(DateTime(timezone=True))
    status = Column(String(20), nullable=False)
    last_error = Column(Text)
    delivered_at = Column(DateTime(timezone=True))
    response_status = Column(Integer)
    processed_count = Column(Integer)
    inserted_count = Column(Integer)
    skipped_duplicate_count = Column(Integer)
    failed_count = Column(Integer)

    __table_args__ = (
        CheckConstraint(
            "status IN ('pending', 'sending', 'succeeded', 'failed', 'blocked')"
        ),
    )


class DeliveryRecord(Base):
    __tablename__ = "delivery_records"

    record_key = Column(String, primary_key=True)
    source_name = Column(String(50), nullable=False)
    last_batch_id = Column(String(36), ForeignKey("delivery_batches.batch_id"))
    status = Column(String(20), nullable=False)
    error = Column(Text)
    updated_at = Column(DateTime(timezone=True), nullable=False)

    __table_args__ = (CheckConstraint("status IN ('accepted', 'rejected', 'blocked')"),)


def make_session_factory(database_url: str) -> sessionmaker:
    """Require migrations before use; never create metadata tables implicitly."""
    engine = create_engine(database_url, pool_pre_ping=True)
    return sessionmaker(engine, expire_on_commit=False)


def new_run_id() -> str:
    return str(uuid4())
