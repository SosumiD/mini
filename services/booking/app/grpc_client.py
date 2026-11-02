"""Authenticated gRPC client with transparent retries and circuit breaker interceptor."""
import logging
import os
import threading
import time
from collections import deque
from datetime import datetime, timezone

import grpc
from google.protobuf.timestamp_pb2 import Timestamp

import flight_pb2 as pb
import flight_pb2_grpc as rpc

log = logging.getLogger(__name__)
RETRYABLE = (grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED)


class CircuitOpenError(Exception):
    """No RPC was attempted because Circuit Breaker is OPEN/busy HALF_OPEN."""


class CircuitBreaker:
    CLOSED, OPEN, HALF_OPEN = "CLOSED", "OPEN", "HALF_OPEN"

    def __init__(self, failure_threshold=5, open_seconds=15, window_seconds=30, clock=None):
        if failure_threshold <= 0 or open_seconds <= 0 or window_seconds <= 0:
            raise ValueError("Circuit breaker settings must be positive")
        self.threshold = failure_threshold
        self.open_seconds = open_seconds
        self.window_seconds = window_seconds
        self.clock = clock or time.monotonic
        self.state = self.CLOSED
        self.failures = deque()
        self.opened_at = 0.0
        self.probe_in_flight = False
        self.lock = threading.Lock()

    def _transition(self, new_state):
        old = self.state
        self.state = new_state
        log.warning("Circuit Breaker transition %s -> %s", old, new_state)

    def before_call(self):
        with self.lock:
            now = self.clock()
            if self.state == self.OPEN:
                if now - self.opened_at < self.open_seconds:
                    raise CircuitOpenError("Flight Service temporarily unavailable (circuit open)")
                self._transition(self.HALF_OPEN)
                self.probe_in_flight = False
            if self.state == self.HALF_OPEN:
                if self.probe_in_flight:
                    raise CircuitOpenError("Flight Service probe already in progress")
                self.probe_in_flight = True
                return True  # this call owns the only probe slot
            return False

    def complete(self, was_probe, is_transport_failure):
        with self.lock:
            now = self.clock()
            if was_probe:
                self.probe_in_flight = False
                if self.state == self.HALF_OPEN:
                    if is_transport_failure:
                        self.opened_at = now
                        self._transition(self.OPEN)
                    else:
                        self.failures.clear()
                        self._transition(self.CLOSED)
                return
            if self.state != self.CLOSED:
                return  # ignore responses from pre-OPEN in-flight calls
            if not is_transport_failure:
                self.failures.clear()
                return
            self.failures.append(now)
            while self.failures and self.failures[0] < now - self.window_seconds:
                self.failures.popleft()
            if len(self.failures) >= self.threshold:
                self.opened_at = now
                self._transition(self.OPEN)


class CircuitBreakerInterceptor(grpc.UnaryUnaryClientInterceptor):
    """Intercepts every unary RPC, including GetFlight/SearchFlights and mutations."""

    def __init__(self, breaker):
        self.breaker = breaker

    def intercept_unary_unary(self, continuation, client_call_details, request):
        probe = self.breaker.before_call()
        try:
            call = continuation(client_call_details, request)
            call.result()  # wait for RPC completion before recording outcome
        except grpc.RpcError as exc:
            self.breaker.complete(probe, exc.code() in RETRYABLE)
            raise
        except Exception:
            self.breaker.complete(probe, True)
            raise
        else:
            self.breaker.complete(probe, False)
            return call


class FlightClient:
    def __init__(self, target=None, api_key=None, breaker=None, deadline=None):
        self.target = target or os.environ["FLIGHT_GRPC_TARGET"]
        self.api_key = api_key or os.environ["GRPC_API_KEY"]
        if not self.api_key:
            raise ValueError("GRPC_API_KEY is required")
        self.deadline = float(deadline or os.environ.get("GRPC_DEADLINE_SECONDS", "2"))
        if self.deadline <= 0:
            raise ValueError("GRPC_DEADLINE_SECONDS must be positive")
        self.breaker = breaker or CircuitBreaker(
            int(os.environ.get("CB_FAILURE_THRESHOLD", "5")),
            float(os.environ.get("CB_OPEN_SECONDS", "15")),
            float(os.environ.get("CB_WINDOW_SECONDS", "30")),
        )
        base_channel = grpc.insecure_channel(self.target)
        self.channel = grpc.intercept_channel(base_channel, CircuitBreakerInterceptor(self.breaker))
        self.stub = rpc.FlightServiceStub(self.channel)

    def call(self, method_name, request):
        method = getattr(self.stub, method_name)
        for attempt in range(3):
            try:
                return method(request, metadata=(("x-api-key", self.api_key),), timeout=self.deadline)
            except CircuitOpenError:
                raise  # do not retry rejected requests: no gRPC call was made
            except grpc.RpcError as exc:
                if exc.code() not in RETRYABLE or attempt == 2:
                    raise
                delay = 0.1 * (2 ** attempt)
                log.warning("Retry RPC %s attempt=%s status=%s backoff=%.3fs",
                            method_name, attempt + 2, exc.code().name, delay)
                time.sleep(delay)
        raise AssertionError("unreachable")

    def get_flight(self, flight_id):
        return self.call("GetFlight", pb.GetFlightRequest(flight_id=str(flight_id)))

    def search_flights(self, origin, destination, day=None):
        request = pb.SearchFlightsRequest(origin=origin, destination=destination)
        if day:
            value = Timestamp()
            value.FromDatetime(datetime.combine(day, datetime.min.time(), tzinfo=timezone.utc))
            request.date.CopyFrom(value)
        return self.call("SearchFlights", request)

    def reserve(self, flight_id, booking_id, seats):
        return self.call("ReserveSeats", pb.ReserveSeatsRequest(
            flight_id=str(flight_id), booking_id=str(booking_id), seat_count=seats))

    def release(self, booking_id):
        return self.call("ReleaseReservation", pb.ReleaseReservationRequest(booking_id=str(booking_id)))
