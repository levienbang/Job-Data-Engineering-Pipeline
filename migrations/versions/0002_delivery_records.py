"""Record delivery checkpoints and run outcome counts."""

import sqlalchemy as sa
from alembic import op

revision = "0002_delivery_records"
down_revision = "0001_operational_metadata"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("pipeline_runs") as batch:
        batch.add_column(
            sa.Column("blocked_count", sa.Integer(), nullable=False, server_default="0")
        )
        batch.add_column(
            sa.Column(
                "rejected_count", sa.Integer(), nullable=False, server_default="0"
            )
        )
    op.create_table(
        "delivery_records",
        sa.Column("record_key", sa.String(), primary_key=True),
        sa.Column("source_name", sa.String(50), nullable=False),
        sa.Column(
            "last_batch_id", sa.String(36), sa.ForeignKey("delivery_batches.batch_id")
        ),
        sa.Column("status", sa.String(20), nullable=False),
        sa.Column("error", sa.Text()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint("status IN ('accepted', 'rejected', 'blocked')"),
    )


def downgrade() -> None:
    op.drop_table("delivery_records")
    with op.batch_alter_table("pipeline_runs") as batch:
        batch.drop_column("rejected_count")
        batch.drop_column("blocked_count")
