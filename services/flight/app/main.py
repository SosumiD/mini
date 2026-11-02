"""gRPC Flight Service. All seat mutations are transactional and idempotent."""
import hmac
import logging
import os
import re
import uuid
from concurrent import futures
from datetime import datetime, time, timedelta, timezone

import grpc
from google.protobuf.timestamp_pb2 import Timestamp
from sqlalchemy import create_engine, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker

import flight_pb2 as pb
import flight_pb2_grpc as rpc
from services.flight.app.cache import FlightCache
from services.flight.app.models import Flight as FlightRow, SeatReservation as ReservationRow

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger(__name__)
STATUS_TO_PB = {"SCHEDULED": pb.SCHEDULED, "DEPARTED": pb.DEPARTED,
                "CANCELLED": pb.CANCELLED, "COMPLETED": pb.COMPLETED}
RES_TO_PB = {"ACTIVE": pb.ACTIVE, "RELEASED": pb.RELEASED, "EXPIRED": pb.EXPIRED}


def parse_uuid(value, context, field):
    try:
        return uuid.UUID(value)
    except (ValueError, AttributeError, TypeError):
        context.abort(grpc.StatusCode.INVALID_ARGUMENT, f"Invalid {field}: UUID required")


def timestamp(value):
    out = Timestamp()
    out.FromDatetime(value)
    return out


def to_proto(row):
    return pb.Flight(
        id=str(row.id), airline=row.airline, flight_number=row.flight_number,
        origin=row.origin, destination=row.destination,
        departure_at=timestamp(row.departure_at), arrival_at=timestamp(row.arrival_at),
        total_seats=row.total_seats, available_seats=row.available_seats,
        price_cents=row.price_cents, status=STATUS_TO_PB[row.status],
    )


def reservation_proto(row):
    return pb.SeatReservation(
        id=str(row.id), booking_id=str(row.booking_id), flight_id=str(row.flight_id),
        seat_count=row.seat_count, status=RES_TO_PB[row.status],
    )


class ApiKeyInterceptor(grpc.ServerInterceptor):
    def __init__(self, secret):
        if not secret:
            raise ValueError("GRPC_API_KEY must not be empty")
        self.secret = secret

    def intercept_service(self, continuation, handler_call_details):
        metadata = dict(handler_call_details.invocation_metadata or ())
        incoming = metadata.get("x-api-key", "")
        if hmac.compare_digest(incoming, self.secret):
            return continuation(handler_call_details)

        def reject(_request, context):
            context.abort(grpc.StatusCode.UNAUTHENTICATED, "Missing or invalid service API key")

        return grpc.unary_unary_rpc_method_handler(reject)


class FlightService(rpc.FlightServiceServicer):
    def __init__(self, session_factory, cache):
        self.Session = session_factory
        self.cache = cache

    def GetFlight(self, request, context):
        flight_id = parse_uuid(request.flight_id, context, "flight_id")
        cached = self.cache.get_flight(flight_id)
        if cached:
            return cached
        with self.Session() as db:
            row = db.get(FlightRow, flight_id)
            if row is None:
                context.abort(grpc.StatusCode.NOT_FOUND, "Flight not found")
            result = to_proto(row)
        self.cache.set_flight(flight_id, result)
        return result

    def SearchFlights(self, request, context):
        origin, destination = request.origin.strip().upper(), request.destination.strip().upper()
        if not all(re.fullmatch(r"[A-Z]{3}", p) for p in (origin, destination)) or origin == destination:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Use distinct 3-letter IATA airport codes")
        day = request.date.ToDatetime(tzinfo=timezone.utc).date() if request.HasField("date") else None
        cached = self.cache.get_search(origin, destination, day)
        if cached is not None:
            return cached
        with self.Session() as db:
            query = select(FlightRow).where(
                FlightRow.origin == origin, FlightRow.destination == destination,
                FlightRow.status == "SCHEDULED",
            )
            if day:
                start = datetime.combine(day, time.min, tzinfo=timezone.utc)
                query = query.where(FlightRow.departure_at >= start,
                                    FlightRow.departure_at < start + timedelta(days=1))
            flights = db.scalars(query.order_by(FlightRow.departure_at)).all()
            result = pb.SearchFlightsResponse(flights=[to_proto(row) for row in flights])
        self.cache.set_search(origin, destination, day, result)
        return result

    def ReserveSeats(self, request, context):
        flight_id = parse_uuid(request.flight_id, context, "flight_id")
        booking_id = parse_uuid(request.booking_id, context, "booking_id")
        if request.seat_count <= 0:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "seat_count must be > 0")
        changed_flight = None
        try:
            with self.Session() as db:
                with db.begin():
                    # Serialize seat mutations for this flight, including duplicate booking_id.
                    flight = db.execute(select(FlightRow).where(
                        FlightRow.id == flight_id).with_for_update()).scalar_one_or_none()
                    if flight is None:
                        context.abort(grpc.StatusCode.NOT_FOUND, "Flight not found")
                    existing = db.scalar(select(ReservationRow).where(
                        ReservationRow.booking_id == booking_id))
                    if existing is not None:
                        if existing.flight_id != flight_id or existing.seat_count != request.seat_count:
                            context.abort(grpc.StatusCode.ALREADY_EXISTS, "booking_id belongs to a different reservation")
                        if existing.status != "ACTIVE":
                            context.abort(grpc.StatusCode.FAILED_PRECONDITION,
                                          "Released booking_id cannot be reserved again")
                        return reservation_proto(existing)  # idempotent retry, no second decrement
                    if flight.status != "SCHEDULED":
                        context.abort(grpc.StatusCode.FAILED_PRECONDITION, "Flight is not SCHEDULED")
                    if flight.available_seats < request.seat_count:
                        context.abort(grpc.StatusCode.RESOURCE_EXHAUSTED, "Not enough seats")
                    flight.available_seats -= request.seat_count
                    reservation = ReservationRow(
                        id=uuid.uuid4(), booking_id=booking_id, flight_id=flight_id,
                        seat_count=request.seat_count, status="ACTIVE",
                    )
                    db.add(reservation)
                    db.flush()
                    result = reservation_proto(reservation)
                    changed_flight = flight
        except IntegrityError:  # unique booking_id also protects concurrent reservations on different flights
            context.abort(grpc.StatusCode.ALREADY_EXISTS, "booking_id is already reserved")
        if changed_flight is not None:
            self.cache.invalidate(changed_flight)
        return result

    def ReleaseReservation(self, request, context):
        booking_id = parse_uuid(request.booking_id, context, "booking_id")
        changed_flight = None
        with self.Session() as db:
            with db.begin():
                # Read the stable FK, then lock flight first (same order as ReserveSeats).
                existing = db.scalar(select(ReservationRow).where(ReservationRow.booking_id == booking_id))
                if existing is None:
                    context.abort(grpc.StatusCode.NOT_FOUND, "Reservation not found")
                flight = db.execute(select(FlightRow).where(
                    FlightRow.id == existing.flight_id).with_for_update()).scalar_one()
                existing = db.scalar(select(ReservationRow).where(
                    ReservationRow.booking_id == booking_id).with_for_update())
                if existing.status == "ACTIVE":
                    flight.available_seats += existing.seat_count
                    existing.status = "RELEASED"
                    existing.updated_at = datetime.now(timezone.utc)
                    changed_flight = flight
                result = reservation_proto(existing)  # repeated release is idempotent
        if changed_flight is not None:
            self.cache.invalidate(changed_flight)
        return result

    def UpdateFlight(self, request, context):
        flight_id = parse_uuid(request.flight_id, context, "flight_id")
        if not request.HasField("status") and not request.HasField("price_cents"):
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Supply status and/or price_cents")
        changed_flight = None
        with self.Session() as db:
            with db.begin():
                flight = db.execute(select(FlightRow).where(
                    FlightRow.id == flight_id).with_for_update()).scalar_one_or_none()
                if flight is None:
                    context.abort(grpc.StatusCode.NOT_FOUND, "Flight not found")
                if request.HasField("price_cents"):
                    if request.price_cents <= 0:
                        context.abort(grpc.StatusCode.INVALID_ARGUMENT, "price_cents must be positive")
                    flight.price_cents = request.price_cents
                if request.HasField("status"):
                    if request.status not in STATUS_TO_PB.values():
                        context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Unknown flight status")
                    flight.status = next(k for k, v in STATUS_TO_PB.items() if v == request.status)
                changed_flight = flight
                result = to_proto(flight)
        self.cache.invalidate(changed_flight)
        return result


def main():
    engine = create_engine(os.environ["FLIGHT_DATABASE_URL"], pool_pre_ping=True)
    Session = sessionmaker(engine, expire_on_commit=False)
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=16),
        interceptors=(ApiKeyInterceptor(os.environ["GRPC_API_KEY"]),),
    )
    rpc.add_FlightServiceServicer_to_server(FlightService(Session, FlightCache()), server)
    server.add_insecure_port("0.0.0.0:50051")  # private Docker network only; use mTLS outside Docker
    server.start()
    log.info("Flight gRPC server listening on 50051")
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        server.stop(grace=3)


if __name__ == "__main__":
    main()
