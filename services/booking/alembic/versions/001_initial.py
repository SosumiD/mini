"""Bookings and a durable reconciliation journal in Booking Service PostgreSQL."""
from alembic import op
import sqlalchemy as sa

revision = "booking_001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "bookings",
        sa.Column("id", sa.Uuid(as_uuid=True), primary_key=True),
        sa.Column("user_id", sa.String(128), nullable=False),
        sa.Column("flight_id", sa.Uuid(as_uuid=True), nullable=False), # cross-service reference: no foreign key
        sa.Column("passenger_name", sa.String(200), nullable=False),
        sa.Column("passenger_email", sa.String(320), nullable=False),
        sa.Column("seat_count", sa.Integer, nullable=False),
        sa.Column("unit_price_cents", sa.BigInteger, nullable=False),
        sa.Column("total_price_cents", sa.BigInteger, nullable=False),
        sa.Column("status", sa.String(16), nullable=False, server_default="CONFIRMED"),
        sa.Column("cancellation_pending", sa.Boolean, nullable=False, server_default=sa.text("false")),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.CheckConstraint("seat_count > 0", name="ck_booking_count"),
        sa.CheckConstraint("unit_price_cents > 0 AND total_price_cents > 0", name="ck_booking_prices"),
        sa.CheckConstraint("total_price_cents = unit_price_cents * seat_count", name="ck_booking_snapshot_total"),
        sa.CheckConstraint("status IN ('CONFIRMED','CANCELLED')", name="ck_booking_status"),
    )
    op.create_index("ix_bookings_user_id", "bookings", ["user_id"])
    op.create_table(
        "booking_operations",
        sa.Column("id", sa.Uuid(as_uuid=True), primary_key=True),
        sa.Column("flight_id", sa.Uuid(as_uuid=True), nullable=False),
        sa.Column("state", sa.String(16), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.CheckConstraint("state IN ('RESERVING','RESERVED','CONFIRMED','COMPENSATING','RELEASED','COMPENSATED')", name="ck_op_state"),
    )
    op.create_index("ix_booking_operations_state_updated", "booking_operations", ["state", "updated_at"])


def downgrade():
    op.drop_table("booking_operations")
    op.drop_table("bookings")
