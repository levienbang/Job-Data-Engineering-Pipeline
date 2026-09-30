"""Create pipeline operational metadata tables.

Revision ID: 0001_operational_metadata
Revises:
"""

import sqlalchemy as sa
from alembic import op

revision = "0001_operational_metadata"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "pipeline_runs",
        sa.Column("run_id", sa.String(36), primary_key=True),
        sa.Column("pipeline_name", sa.String(100), nullable=False),
        sa.Column("source_name", sa.String(50), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True)),
        sa.Column("status", sa.String(20), nullable=False),
        sa.Column("input_count", sa.Integer(), nullable=False),
        sa.Column("valid_count", sa.Integer(), nullable=False),
        sa.Column("invalid_count", sa.Integer(), nullable=False),
        sa.Column("duplicate_count", sa.Integer(), nullable=False),
        sa.Column("delivered_count", sa.Integer(), nullable=False),
        sa.Column("error_message", sa.Text()),
        sa.Column("code_version", sa.String(40), nullable=False),
        sa.CheckConstraint("status IN ('running', 'succeeded', 'failed')"),
    )
    op.create_table(
        "source_assets",
        sa.Column("asset_id", sa.String(36), primary_key=True),
        sa.Column("source_name", sa.String(50), nullable=False),
        sa.Column("source_url_or_path", sa.Text(), nullable=False),
        sa.Column("content_hash", sa.String(64), nullable=False),
        sa.Column("http_status", sa.Integer()),
        sa.Column("first_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_run_id", sa.String(36), sa.ForeignKey("pipeline_runs.run_id")),
        sa.Column("status", sa.String(20), nullable=False),
        sa.UniqueConstraint(
            "source_name", "source_url_or_path", name="uq_source_asset"
        ),
        sa.CheckConstraint("status IN ('seen', 'processed', 'failed')"),
    )
    op.create_table(
        "delivery_batches",
        sa.Column("batch_id", sa.String(36), primary_key=True),
        sa.Column(
            "run_id",
            sa.String(36),
            sa.ForeignKey("pipeline_runs.run_id"),
            nullable=False,
        ),
        sa.Column("payload_hash", sa.String(64), nullable=False, unique=True),
        sa.Column("record_count", sa.Integer(), nullable=False),
        sa.Column("record_keys", sa.JSON(), nullable=False),
        sa.Column("schema_version", sa.Integer(), nullable=False),
        sa.Column("attempt_count", sa.Integer(), nullable=False),
        sa.Column("last_attempt_at", sa.DateTime(timezone=True)),
        sa.Column("status", sa.String(20), nullable=False),
        sa.Column("last_error", sa.Text()),
        sa.Column("delivered_at", sa.DateTime(timezone=True)),
        sa.Column("response_status", sa.Integer()),
        sa.Column("processed_count", sa.Integer()),
        sa.Column("inserted_count", sa.Integer()),
        sa.Column("skipped_duplicate_count", sa.Integer()),
        sa.Column("failed_count", sa.Integer()),
        sa.CheckConstraint(
            "status IN ('pending', 'sending', 'succeeded', 'failed', 'blocked')"
        ),
    )


def downgrade() -> None:
    op.drop_table("delivery_batches")
    op.drop_table("source_assets")
    op.drop_table("pipeline_runs")
