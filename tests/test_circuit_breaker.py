import pytest
from services.booking.app.grpc_client import CircuitBreaker, CircuitOpenError


def test_open_half_open_closed_and_single_probe():
    now = [0.0]
    cb = CircuitBreaker(failure_threshold=2, open_seconds=10, window_seconds=30, clock=lambda: now[0])
    assert cb.before_call() is False
    cb.complete(False, True)
    now[0] = 1.0
    assert cb.before_call() is False
    cb.complete(False, True)
    assert cb.state == "OPEN"
    with pytest.raises(CircuitOpenError):
        cb.before_call()
    now[0] = 11.0
    assert cb.before_call() is True
    assert cb.state == "HALF_OPEN"
    with pytest.raises(CircuitOpenError):
        cb.before_call()
    cb.complete(True, False)
    assert cb.state == "CLOSED"
    assert cb.before_call() is False


def test_half_open_failure_reopens():
    now = [0.0]
    cb = CircuitBreaker(failure_threshold=1, open_seconds=3, clock=lambda: now[0])
    cb.before_call()
    cb.complete(False, True)
    now[0] = 4.0
    assert cb.before_call() is True
    cb.complete(True, True)
    assert cb.state == "OPEN"


def test_business_error_is_not_transport_failure():
    cb = CircuitBreaker(failure_threshold=1)
    cb.before_call()
    cb.complete(False, False)
    assert cb.state == "CLOSED"
