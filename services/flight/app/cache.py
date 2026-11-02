"""Cache-Aside backed by Redis Sentinel. A cache outage must not stop flight queries."""
import json
import logging
import os
from datetime import datetime, timezone
from redis.sentinel import Sentinel
from redis.exceptions import RedisError
from google.protobuf.json_format import MessageToDict, ParseDict, ParseError
import flight_pb2 as pb

log = logging.getLogger(__name__)


class FlightCache:
    def __init__(self):
        endpoints = [
            (host, int(port))
            for endpoint in os.environ.get("REDIS_SENTINELS", "redis-sentinel-1:26379").split(",")
            for host, port in [endpoint.rsplit(":", 1)]
        ]
        sentinel = Sentinel(endpoints, socket_timeout=0.4, socket_connect_timeout=0.4)
        self.client = sentinel.master_for(
            os.environ.get("REDIS_MASTER_NAME", "mymaster"),
            decode_responses=True,
            socket_timeout=0.4,
            socket_connect_timeout=0.4,
        )
        self.ttl = int(os.environ.get("CACHE_TTL_SECONDS", "360"))
        if not 1 <= self.ttl <= 600:
            raise ValueError("CACHE_TTL_SECONDS must be between 1 and 600")

    def _get(self, key, message_type):
        try:
            data = self.client.get(key)
            if data is None:
                log.info("cache MISS %s", key)
                return None
            log.info("cache HIT %s", key)
            return ParseDict(json.loads(data), message_type())
        except (RedisError, ParseError, ValueError, TypeError) as exc:
            log.warning("cache unavailable/corrupt on GET %s: %s", key, exc)
            return None

    def _set(self, key, message):
        try:
            data = json.dumps(MessageToDict(message, preserving_proto_field_name=True))
            self.client.set(key, data, ex=self.ttl)
        except RedisError as exc:
            log.warning("cache unavailable on SET %s: %s", key, exc)

    def get_flight(self, flight_id):
        return self._get(f"flight:{flight_id}", pb.Flight)

    def set_flight(self, flight_id, flight):
        self._set(f"flight:{flight_id}", flight)

    @staticmethod
    def search_key(origin, destination, date):
        return f"search:{origin}:{destination}:{date.isoformat() if date else '*'}"

    def get_search(self, origin, destination, date):
        return self._get(self.search_key(origin, destination, date), pb.SearchFlightsResponse)

    def set_search(self, origin, destination, date, response):
        self._set(self.search_key(origin, destination, date), response)

    def invalidate(self, flight):
        """Remove detail and both potentially affected search caches after DB commit.

        Uses known exact route/date keys, avoiding SCAN/KEYS and also clears no-date.
        """
        day = flight.departure_at.astimezone(timezone.utc).date()
        keys = [
            f"flight:{flight.id}",
            self.search_key(flight.origin, flight.destination, day),
            self.search_key(flight.origin, flight.destination, None),
        ]
        try:
            self.client.delete(*keys)
            log.info("cache INVALIDATE %s", keys)
        except RedisError as exc:
            log.error("cache invalidation failed; keys will expire by TTL: %s", exc)
