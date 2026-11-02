from datetime import datetime
from uuid import UUID
from sqlalchemy import BigInteger, CheckConstraint, DateTime, ForeignKey, Integer, String, Uuid, func
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class Flight(Base):
    __tablename__ = "flights"
    id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True)
    airline: Mapped[str] = mapped_column(String(128))
    flight_number: Mapped[str] = mapped_column(String(16))
    origin: Mapped[str] = mapped_column(String(3))
    destination: Mapped[str] = mapped_column(String(3))
    departure_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    arrival_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    total_seats: Mapped[int] = mapped_column(Integer)
    available_seats: Mapped[int] = mapped_column(Integer)
    price_cents: Mapped[int] = mapped_column(BigInteger)
    status: Mapped[str] = mapped_column(String(16))


class SeatReservation(Base):
    __tablename__ = "seat_reservations"
    id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True)
    booking_id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), unique=True)
    flight_id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), ForeignKey("flights.id"))
    seat_count: Mapped[int] = mapped_column(Integer)
    status: Mapped[str] = mapped_column(String(16))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
