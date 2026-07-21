import unittest

import httpx

from app.trino_repository import (
    SUMMARY_SQL,
    TrinoAnalyticsRepository,
)


class TrinoAnalyticsRepositoryTest(unittest.TestCase):
    def test_latest_event_time_is_parsed_as_iso_timestamp_before_max(self):
        normalized_sql = " ".join(SUMMARY_SQL.split()).lower()

        self.assertIn("try(from_iso8601_timestamp(event_time))", normalized_sql)
        self.assertIn("max(parsed_event_time)", normalized_sql)
        self.assertIn("to_iso8601(summary.latest_event_time)", normalized_sql)

    def test_fetch_summary_follows_next_uri_and_maps_rows(self):
        requests: list[httpx.Request] = []

        def handler(request: httpx.Request) -> httpx.Response:
            requests.append(request)
            if request.method == "POST" and b"WITH parsed_events AS" in request.content:
                return httpx.Response(
                    200,
                    json={
                        "data": [[1000, "view", 700, "2026-07-18T10:00:00Z"]],
                        "nextUri": "http://trino:8080/v1/next/1",
                    },
                )
            if request.method == "GET" and str(request.url) == "http://trino:8080/v1/next/1":
                return httpx.Response(
                    200,
                    json={
                        "data": [
                            [1000, "click", 200, "2026-07-18T10:00:00Z"],
                            [1000, "purchase", 100, "2026-07-18T10:00:00Z"],
                        ]
                    },
                )
            self.fail(f"Unexpected Trino request: {request.method} {request.url} {request.content!r}")

        transport = httpx.MockTransport(handler)
        repository = TrinoAnalyticsRepository(
            base_url="http://trino:8080",
            user="test-user",
            catalog="test_catalog",
            schema="test_schema",
            timeout_seconds=5,
            client_factory=lambda: httpx.Client(transport=transport),
        )

        result = repository.fetch_summary()

        self.assertEqual(1000, result.event_count)
        self.assertEqual({"view": 700, "click": 200, "purchase": 100}, result.event_type_counts)
        self.assertEqual("2026-07-18T10:00:00+00:00", result.latest_event_time.isoformat())
        post_requests = [request for request in requests if request.method == "POST"]
        self.assertEqual(1, len(post_requests))
        self.assertIn(b"WITH parsed_events AS", post_requests[0].content)
        self.assertIn(b'FROM "test_catalog"."test_schema"."user_behavior_detail"', post_requests[0].content)
        self.assertNotIn(b"lakehouse.analytics", post_requests[0].content)
        initial_post = post_requests[0]
        self.assertEqual("test-user", initial_post.headers["X-Trino-User"])
        self.assertEqual("test_catalog", initial_post.headers["X-Trino-Catalog"])
        self.assertEqual("test_schema", initial_post.headers["X-Trino-Schema"])
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

    def test_fetch_summary_rejects_inconsistent_or_negative_counts(self):
        cases = (
            [[2, "view", 2, "2026-07-18T10:00:00Z"], [2, "click", 1, "2026-07-18T10:00:00Z"]],
            [[-1, None, None, None]],
            [[1, "view", -1, "2026-07-18T10:00:00Z"]],
        )
        for rows in cases:
            with self.subTest(rows=rows):
                def handler(request: httpx.Request) -> httpx.Response:
                    return httpx.Response(200, json={"data": rows})

                repository = TrinoAnalyticsRepository(
                    base_url="http://trino:8080",
                    client_factory=lambda: httpx.Client(transport=httpx.MockTransport(handler)),
                )

                with self.assertRaises(ValueError):
                    repository.fetch_summary()

    def test_constructor_rejects_unsafe_catalog_or_schema_identifiers(self):
        unsafe_values = ("", "lake.house", 'lake"house', "analytics; DROP TABLE users", "two words")
        for value in unsafe_values:
            with self.subTest(catalog=value):
                with self.assertRaisesRegex(ValueError, "Trino catalog"):
                    TrinoAnalyticsRepository(base_url="http://trino:8080", catalog=value)
            with self.subTest(schema=value):
                with self.assertRaisesRegex(ValueError, "Trino schema"):
                    TrinoAnalyticsRepository(base_url="http://trino:8080", schema=value)

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
