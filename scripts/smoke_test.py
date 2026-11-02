"""End-to-end integration check against running compose (stdlib only)."""
import json
import os
from urllib.request import Request, urlopen

BASE = os.environ.get("BOOKING_REST_URL", "http://localhost:8000")
FID = "11111111-1111-4111-8111-111111111111"


def send(path, method="GET", data=None):
    body = json.dumps(data).encode() if data is not None else None
    request = Request(BASE + path, data=body, method=method)
    if body:
        request.add_header("Content-Type", "application/json")
    with urlopen(request, timeout=15) as r:
        return r.status, json.load(r)


def main():
    status, initial = send("/flights/" + FID)
    assert status == 200
    n = initial["available_seats"]
    status, booking = send("/bookings", "POST", {
        "user_id": "test-user", "flight_id": FID,
        "passenger_name": "Test Passenger", "passenger_email": "test@example.org",
        "seat_count": 2,
    })
    assert status == 201 and booking["status"] == "CONFIRMED"
    assert booking["total_price"] == "17000.00"
    _, flight = send("/flights/" + FID)
    assert flight["available_seats"] == n - 2, (n, flight)
    _, results = send("/flights?origin=SVO&destination=LED&date=2026-12-01")
    assert len(results) >= 1
    _, bookings = send("/bookings?user_id=test-user")
    assert booking["id"] in [b["id"] for b in bookings]
    _, cancelled = send(f'/bookings/{booking["id"]}/cancel', "POST")
    assert cancelled["status"] == "CANCELLED"
    _, flight = send("/flights/" + FID)
    assert flight["available_seats"] == n
    print("PASS: search -> reserve -> snapshot -> list -> cancel -> seats restored")


if __name__ == "__main__":
    main()
