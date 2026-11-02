from datetime import datetime
from uuid import UUID
from sqlalchemy import BigInteger, Boolean, DateTime, Integer, String, Uuid, func
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class Booking(Base):
    __tablename__ = "bookings"
    id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True)
    user_id: Mapped[str] = mapped_column(String(128))
    flight_id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True))
    passenger_name: Mapped[str] = mapped_column(String(200))
    passenger_email: Mapped[str] = mapped_column(String(320))
    seat_count: Mapped[int] = mapped_column(Integer)
    unit_price_cents: Mapped[int] = mapped_column(BigInteger)
    total_price_cents: Mapped[int] = mapped_column(BigInteger)
    status: Mapped[str] = mapped_column(String(16), default="CONFIRMED")
    cancellation_pending: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class BookingOperation(Base):
    __tablename__ = "booking_operations"
    id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True)
    flight_id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True))
    state: Mapped[str] = mapped_column(String(16))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
