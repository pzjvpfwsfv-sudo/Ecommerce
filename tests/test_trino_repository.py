import unittest

import httpx

from app.trino_repository import (
    EVENT_COUNT_SQL,
    EVENT_TYPE_COUNTS_SQL,
    LATEST_EVENT_TIME_SQL,
    TrinoAnalyticsRepository,
)


class TrinoAnalyticsRepositoryTest(unittest.TestCase):
    def test_fetch_summary_follows_next_uri_and_maps_rows(self):
        requests: list[httpx.Request] = []

        def handler(request: httpx.Request) -> httpx.Response:
            requests.append(request)
            if request.method == "POST" and request.content == EVENT_COUNT_SQL.encode():
                return httpx.Response(200, json={"data": [[1000]]})
            if request.method == "POST" and request.content == EVENT_TYPE_COUNTS_SQL.encode():
                return httpx.Response(200, json={"data": [["view", 700]], "nextUri": "http://trino:8080/v1/next/1"})
            if request.method == "POST" and request.content == LATEST_EVENT_TIME_SQL.encode():
                return httpx.Response(200, json={"data": [["2026-07-18T10:00:00Z"]]})
            if request.method == "GET" and str(request.url) == "http://trino:8080/v1/next/1":
                return httpx.Response(200, json={"data": [["click", 200], ["purchase", 100]]})
            self.fail(f"Unexpected Trino request: {request.method} {request.url} {request.content!r}")

        transport = httpx.MockTransport(handler)
        repository = TrinoAnalyticsRepository(
            base_url="http://trino:8080",
            user="test-user",
            catalog="test-catalog",
            schema="test-schema",
            timeout_seconds=5,
            client_factory=lambda: httpx.Client(transport=transport),
        )

        result = repository.fetch_summary()

        self.assertEqual(1000, result.event_count)
        self.assertEqual({"view": 700, "click": 200, "purchase": 100}, result.event_type_counts)
        self.assertEqual("2026-07-18T10:00:00+00:00", result.latest_event_time.isoformat())
        post_requests = [request for request in requests if request.method == "POST"]
        self.assertEqual(
            [EVENT_COUNT_SQL.encode(), EVENT_TYPE_COUNTS_SQL.encode(), LATEST_EVENT_TIME_SQL.encode()],
            [request.content for request in post_requests],
        )
        initial_post = post_requests[0]
        self.assertEqual("test-user", initial_post.headers["X-Trino-User"])
        self.assertEqual("test-catalog", initial_post.headers["X-Trino-Catalog"])
        self.assertEqual("test-schema", initial_post.headers["X-Trino-Schema"])
        self.assertEqual(
            {"connect": 5, "read": 5, "write": 5, "pool": 5},
            initial_post.extensions["timeout"],
        )
        next_request = next(request for request in requests if request.method == "GET")
        self.assertEqual("http://trino:8080/v1/next/1", str(next_request.url))
        self.assertEqual(
            {"connect": 5, "read": 5, "write": 5, "pool": 5},
            next_request.extensions["timeout"],
        )

    def test_trino_error_response_raises_runtime_error(self):
        transport = httpx.MockTransport(
            lambda request: httpx.Response(200, json={"error": {"message": "catalog unavailable"}})
        )
        repository = TrinoAnalyticsRepository(
            base_url="http://trino:8080",
            client_factory=lambda: httpx.Client(transport=transport),
        )

        with self.assertRaisesRegex(RuntimeError, "catalog unavailable"):
            repository.fetch_summary()
