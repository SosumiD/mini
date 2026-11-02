"""FastAPI Booking Service: REST gateway, bookings and compensation journal."""
from contextlib import asynccontextmanager
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
import logging
import os
import threading
import uuid

import grpc
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, EmailStr, Field
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from services.booking.app.grpc_client import CircuitOpenError, FlightClient
from services.booking.app.models import Booking, BookingOperation

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger(__name__)
engine = create_engine(os.environ["BOOKING_DATABASE_URL"], pool_pre_ping=True)
Session = sessionmaker(engine, expire_on_commit=False)
flight_client = FlightClient()
stop_reconciler = threading.Event()


def money(cents):
    return str((Decimal(cents) / Decimal(100)).quantize(Decimal("0.01")))


def api_flight(flight):
    return {
        "id": flight.id, "airline": flight.airline,
        "flight_number": flight.flight_number, "origin": flight.origin,
        "destination": flight.destination,
        "departure_at": flight.departure_at.ToDatetime(tzinfo=timezone.utc).isoformat(),
        "arrival_at": flight.arrival_at.ToDatetime(tzinfo=timezone.utc).isoformat(),
        "total_seats": flight.total_seats, "available_seats": flight.available_seats,
        "price": money(flight.price_cents), "currency": "RUB",
        "status": flight.__class__.DESCRIPTOR.fields_by_name["status"].enum_type.values_by_number[flight.status].name,
    }


def api_booking(row):
    return {
        "id": str(row.id), "user_id": row.user_id, "flight_id": str(row.flight_id),
        "passenger_name": row.passenger_name, "passenger_email": row.passenger_email,
        "seat_count": row.seat_count, "unit_price": money(row.unit_price_cents),
        "total_price": money(row.total_price_cents), "currency": "RUB",
        "status": row.status, "cancellation_pending": row.cancellation_pending,
        "created_at": row.created_at.isoformat() if row.created_at else None,
    }


class CreateBooking(BaseModel):
    user_id: str = Field(min_length=1, max_length=128)
    flight_id: uuid.UUID
    passenger_name: str = Field(min_length=1, max_length=200)
    passenger_email: EmailStr
    seat_count: int = Field(gt=0, le=10000)


def grpc_error(exc):
    if isinstance(exc, CircuitOpenError):
        return HTTPException(503, str(exc))
    status = exc.code()
    code = {
        grpc.StatusCode.INVALID_ARGUMENT: 400,
        grpc.StatusCode.NOT_FOUND: 404,
        grpc.StatusCode.RESOURCE_EXHAUSTED: 409,
        grpc.StatusCode.ALREADY_EXISTS: 409,
        grpc.StatusCode.FAILED_PRECONDITION: 409,
        grpc.StatusCode.UNAUTHENTICATED: 502,
        grpc.StatusCode.PERMISSION_DENIED: 502,
        grpc.StatusCode.UNAVAILABLE: 503,
        grpc.StatusCode.DEADLINE_EXCEEDED: 503,
    }.get(status, 502)
    return HTTPException(code, f"Flight Service: {status.name}: {exc.details()}")


def set_op_state(op_id, state):
    with Session.begin() as db:
        op = db.get(BookingOperation, op_id)
        if op:
            op.state = state
            op.updated_at = datetime.now(timezone.utc)


def compensate(booking_id):
    """Best-effort immediate compensation; a durable worker retries after failure."""
    set_op_state(booking_id, "COMPENSATING")
    try:
        flight_client.release(booking_id)
    except grpc.RpcError as exc:
        if exc.code() != grpc.StatusCode.NOT_FOUND:
            log.error("Release pending for booking_id=%s: %s", booking_id, exc)
            return False
    except CircuitOpenError:
        log.warning("Release queued: circuit open for %s", booking_id)
        return False
    set_op_state(booking_id, "RELEASED")
    return True


def finish_cancellation(booking_id):
    try:
        flight_client.release(booking_id)
    except grpc.RpcError as exc:
        if exc.code() != grpc.StatusCode.NOT_FOUND:
            raise
    with Session.begin() as db:
        row = db.get(Booking, booking_id, with_for_update=True)
        if row and row.cancellation_pending:
            row.status = "CANCELLED"
            row.cancellation_pending = False
            row.updated_at = datetime.now(timezone.utc)


def reconcile_once():
    """Finish interrupted sagas. Uses a grace period longer than max RPC retries.

    This is eventual compensation across independent DBs, not distributed ACID.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(seconds=45)
    with Session() as db:
        stale_ops = db.scalars(select(BookingOperation).where(
            BookingOperation.state.in_(("RESERVING", "RESERVED", "COMPENSATING", "RELEASED")),
            BookingOperation.updated_at < cutoff,
        ).limit(100)).all()
        pending_cancel = db.scalars(select(Booking.id).where(
            Booking.cancellation_pending.is_(True)).limit(100)).all()
        # Do not keep a DB session checked out during network calls.
        op_states = [(r.id, r.state) for r in stale_ops]
    for op_id, op_state in op_states:
        try:
            with Session() as db:
                saved = db.get(Booking, op_id)
            if saved is not None:
                set_op_state(op_id, "CONFIRMED")
            elif op_state == "RELEASED":
                # Verify again after the RPC-deadline grace period: an earlier
                # Release could have raced with a timed-out ReserveSeats.
                try:
                    flight_client.release(op_id)
                except grpc.RpcError as exc:
                    if exc.code() != grpc.StatusCode.NOT_FOUND:
                        raise
                set_op_state(op_id, "COMPENSATED")
            else:
                compensate(op_id)
        except Exception:
            log.exception("Reconciliation creation failed for %s", op_id)
    for booking_id in pending_cancel:
        try:
            finish_cancellation(booking_id)
        except Exception:
            log.exception("Reconciliation cancellation failed for %s", booking_id)


def reconciliation_loop():
    while not stop_reconciler.wait(10):
        try:
            reconcile_once()
        except Exception:
            log.exception("Reconciliation cycle failed")


@asynccontextmanager
async def lifespan(_app):
    stop_reconciler.clear()
    worker = threading.Thread(target=reconciliation_loop, daemon=True, name="saga-reconciler")
    worker.start()
    yield
    stop_reconciler.set()
    worker.join(timeout=3)
    flight_client.channel.close()
    engine.dispose()


app = FastAPI(title="Flight Booking REST API", version="1.0.0", lifespan=lifespan)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/flights")
def search_flights(origin: str, destination: str, date: date | None = Query(None)):
    try:
        result = flight_client.search_flights(origin, destination, date)
        return [api_flight(r) for r in result.flights]
    except (grpc.RpcError, CircuitOpenError) as exc:
        raise grpc_error(exc) from exc


@app.get("/flights/{flight_id}")
def get_flight(flight_id: uuid.UUID):
    try:
        return api_flight(flight_client.get_flight(flight_id))
    except (grpc.RpcError, CircuitOpenError) as exc:
        raise grpc_error(exc) from exc


@app.post("/bookings", status_code=201)
def create_booking(data: CreateBooking):
    try:
        flight = flight_client.get_flight(data.flight_id)
    except (grpc.RpcError, CircuitOpenError) as exc:
        raise grpc_error(exc) from exc

    booking_id = uuid.uuid4()
    # Durable operation log before crossing the service boundary. No Booking row yet.
    with Session.begin() as db:
        db.add(BookingOperation(id=booking_id, flight_id=data.flight_id, state="RESERVING"))

    try:
        flight_client.reserve(data.flight_id, booking_id, data.seat_count)
    except (grpc.RpcError, CircuitOpenError) as exc:
        # A timed-out ReserveSeats may actually have succeeded: always compensate.
        compensate(booking_id)
        raise grpc_error(exc) from exc

    # Operation state is committed; if subsequent local DB commit fails, we can recover.
    try:
        with Session.begin() as db:
            op = db.get(BookingOperation, booking_id)
            op.state = "CONFIRMED"
            op.updated_at = datetime.now(timezone.utc)
            db.add(Booking(
                id=booking_id, user_id=data.user_id, flight_id=data.flight_id,
                passenger_name=data.passenger_name, passenger_email=str(data.passenger_email),
                seat_count=data.seat_count, unit_price_cents=flight.price_cents,
                total_price_cents=flight.price_cents * data.seat_count,
                status="CONFIRMED", cancellation_pending=False,
            ))
    except Exception as exc:
        log.exception("Booking DB commit failed/ambiguous for %s", booking_id)
        # If the DB commit was ambiguous, a background worker will check both states
        # and compensate only if the booking row is absent.
        raise HTTPException(503, "Booking finalization uncertain; reconciliation scheduled") from exc

    with Session() as db:
        booking = db.get(Booking, booking_id)
        return api_booking(booking)


@app.get("/bookings")
def list_bookings(user_id: str = Query(..., min_length=1)):
    with Session() as db:
        rows = db.scalars(select(Booking).where(
            Booking.user_id == user_id).order_by(Booking.created_at.desc())).all()
        return [api_booking(row) for row in rows]


@app.get("/bookings/{booking_id}")
def get_booking(booking_id: uuid.UUID):
    with Session() as db:
        row = db.get(Booking, booking_id)
        if not row:
            raise HTTPException(404, "Booking not found")
        return api_booking(row)


@app.post("/bookings/{booking_id}/cancel")
def cancel_booking(booking_id: uuid.UUID):
    with Session.begin() as db:
        row = db.get(Booking, booking_id, with_for_update=True)
        if row is None:
            raise HTTPException(404, "Booking not found")
        if row.status != "CONFIRMED":
            raise HTTPException(409, "Only CONFIRMED bookings can be cancelled")
        if row.cancellation_pending:
            raise HTTPException(409, "Cancellation already in progress")
        row.cancellation_pending = True
        row.updated_at = datetime.now(timezone.utc)
    try:
        finish_cancellation(booking_id)
    except (grpc.RpcError, CircuitOpenError) as exc:
        # Leave pending flag for background retry; never mark CANCELLED prematurely.
        raise grpc_error(exc) from exc
    with Session() as db:
        return api_booking(db.get(Booking, booking_id))
