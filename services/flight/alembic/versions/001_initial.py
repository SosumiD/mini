"""Flights and seat reservations; each PostgreSQL belongs to Flight Service."""
from alembic import op
import sqlalchemy as sa

revision = "flight_001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "flights",
        sa.Column("id", sa.Uuid(as_uuid=True), primary_key=True),
        sa.Column("airline", sa.String(128), nullable=False),
        sa.Column("flight_number", sa.String(16), nullable=False),
        sa.Column("origin", sa.String(3), nullable=False),
        sa.Column("destination", sa.String(3), nullable=False),
        sa.Column("departure_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("arrival_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("total_seats", sa.Integer, nullable=False),
        sa.Column("available_seats", sa.Integer, nullable=False),
        sa.Column("price_cents", sa.BigInteger, nullable=False),
        sa.Column("status", sa.String(16), nullable=False, server_default="SCHEDULED"),
        sa.CheckConstraint("char_length(origin) = 3 AND origin = upper(origin)", name="ck_origin_iata"),
        sa.CheckConstraint("char_length(destination) = 3 AND destination = upper(destination)", name="ck_dest_iata"),
        sa.CheckConstraint("origin <> destination", name="ck_distinct_airports"),
        sa.CheckConstraint("arrival_at > departure_at", name="ck_arrival_later"),
        sa.CheckConstraint("total_seats > 0", name="ck_total_seats_positive"),
        sa.CheckConstraint("available_seats >= 0 AND available_seats <= total_seats", name="ck_available_seats_bounds"),
        sa.CheckConstraint("price_cents > 0", name="ck_price_positive"),
        sa.CheckConstraint("status IN ('SCHEDULED','DEPARTED','CANCELLED','COMPLETED')", name="ck_flight_status"),
    )
    # Departure date is the UTC calendar date. Derived index avoids storing redundant data.
    op.execute("CREATE UNIQUE INDEX uq_number_departure_date ON flights (flight_number, ((departure_at AT TIME ZONE 'UTC')::date))")
    op.create_index("ix_flights_route_date", "flights", ["origin", "destination", "departure_at"])
    op.create_table(
        "seat_reservations",
        sa.Column("id", sa.Uuid(as_uuid=True), primary_key=True),
        sa.Column("booking_id", sa.Uuid(as_uuid=True), nullable=False, unique=True),
        sa.Column("flight_id", sa.Uuid(as_uuid=True), sa.ForeignKey("flights.id", ondelete="RESTRICT"), nullable=False),
        sa.Column("seat_count", sa.Integer, nullable=False),
        sa.Column("status", sa.String(16), nullable=False, server_default="ACTIVE"),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.CheckConstraint("seat_count > 0", name="ck_reservation_count_positive"),
        sa.CheckConstraint("status IN ('ACTIVE','RELEASED','EXPIRED')", name="ck_reservation_status"),
    )
    op.create_index("ix_seat_reservations_flight", "seat_reservations", ["flight_id"])
    op.execute("""
        INSERT INTO flights (id,airline,flight_number,origin,destination,departure_at,arrival_at,total_seats,available_seats,price_cents,status)
        VALUES
          ('11111111-1111-4111-8111-111111111111','Aeroflot','SU1234','SVO','LED','2026-12-01 08:30:00+00','2026-12-01 10:05:00+00',120,120,850000,'SCHEDULED'),
          ('22222222-2222-4222-8222-222222222222','Aeroflot','SU1235','SVO','LED','2026-12-01 19:30:00+00','2026-12-01 21:05:00+00',100,100,990000,'SCHEDULED'),
          ('33333333-3333-4333-8333-333333333333','Rossiya','FV6001','LED','SVO','2026-12-02 11:00:00+00','2026-12-02 12:30:00+00',90,90,720000,'SCHEDULED'),
          ('44444444-4444-4444-8444-444444444444','Aeroflot','SU0001','SVO','KZN','2026-12-03 08:00:00+00','2026-12-03 09:30:00+00',1,1,510000,'SCHEDULED')
    """)


def downgrade():
    op.drop_table("seat_reservations")
    op.drop_table("flights")
