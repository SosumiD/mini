"""Run from booking-service container to check service auth, retries and atomic seats."""
import os
import uuid
from concurrent.futures import ThreadPoolExecutor

import grpc
import flight_pb2 as pb
import flight_pb2_grpc as service

TARGET = os.getenv("FLIGHT_GRPC_TARGET", "flight-service:50051")
API_KEY = os.environ["GRPC_API_KEY"]
SINGLE_SEAT_ID = "44444444-4444-4444-8444-444444444444"


def main():
    with grpc.insecure_channel(TARGET) as channel:
        stub = service.FlightServiceStub(channel)
        try:
            stub.GetFlight(pb.GetFlightRequest(flight_id=SINGLE_SEAT_ID), timeout=3)
            raise AssertionError("Unauthenticated RPC unexpectedly accepted")
        except grpc.RpcError as exc:
            assert exc.code() == grpc.StatusCode.UNAUTHENTICATED, exc

        metadata = (("x-api-key", API_KEY),)
        original = stub.GetFlight(pb.GetFlightRequest(flight_id=SINGLE_SEAT_ID), metadata=metadata, timeout=3)
        assert original.available_seats == 1, "Expected clean one-seat demo fixture"

        def attempt(_):
            booking_id = str(uuid.uuid4())
            try:
                result = stub.ReserveSeats(
                    pb.ReserveSeatsRequest(flight_id=SINGLE_SEAT_ID, seat_count=1, booking_id=booking_id),
                    metadata=metadata, timeout=3)
                repeat = stub.ReserveSeats(
                    pb.ReserveSeatsRequest(flight_id=SINGLE_SEAT_ID, seat_count=1, booking_id=booking_id),
                    metadata=metadata, timeout=3)
                assert result.id == repeat.id
                return booking_id
            except grpc.RpcError as exc:
                assert exc.code() == grpc.StatusCode.RESOURCE_EXHAUSTED, exc
                return None

        with ThreadPoolExecutor(max_workers=8) as pool:
            winners = [value for value in pool.map(attempt, range(8)) if value]
        assert len(winners) == 1, f"Expected exactly 1 successful reservation: {winners}"
        try:
            current = stub.GetFlight(pb.GetFlightRequest(flight_id=SINGLE_SEAT_ID), metadata=metadata, timeout=3)
            assert current.available_seats == 0
        finally:
            for winner in winners:
                stub.ReleaseReservation(pb.ReleaseReservationRequest(booking_id=winner), metadata=metadata, timeout=3)
                stub.ReleaseReservation(pb.ReleaseReservationRequest(booking_id=winner), metadata=metadata, timeout=3)
        restored = stub.GetFlight(pb.GetFlightRequest(flight_id=SINGLE_SEAT_ID), metadata=metadata, timeout=3)
        assert restored.available_seats == 1
        print("PASS: UNAUTHENTICATED, one-seat lock, idempotent reserve/release")


if __name__ == "__main__":
    main()
