import unittest

import httpx

from app.trino_repository import TrinoAnalyticsRepository


class TrinoAnalyticsRepositoryTest(unittest.TestCase):
    def test_fetch_summary_follows_next_uri_and_maps_rows(self):
        requests: list[str] = []

        def handler(request: httpx.Request) -> httpx.Response:
            requests.append(str(request.url))
            if request.method == "POST" and request.content == b"SELECT COUNT(*) AS event_count FROM lakehouse.analytics.user_behavior_detail":
                return httpx.Response(200, json={"data": [[1000]]})
            if request.method == "POST" and request.content == b"SELECT MAX(event_time) AS latest_event_time FROM lakehouse.analytics.user_behavior_detail":
                return httpx.Response(200, json={"data": [["2026-07-18T10:00:00Z"]]})
            if request.method == "POST":
                return httpx.Response(200, json={"data": [["view", 700]], "nextUri": "http://trino:8080/v1/next/1"})
            return httpx.Response(200, json={"data": [["click", 200], ["purchase", 100]]})

        transport = httpx.MockTransport(handler)
        repository = TrinoAnalyticsRepository(
            base_url="http://trino:8080",
            timeout_seconds=5,
            client_factory=lambda: httpx.Client(transport=transport),
        )

        result = repository.fetch_summary()

        self.assertEqual(1000, result.event_count)
        self.assertEqual({"view": 700, "click": 200, "purchase": 100}, result.event_type_counts)
        self.assertEqual("2026-07-18T10:00:00+00:00", result.latest_event_time.isoformat())
        self.assertTrue(any("/v1/next/1" in url for url in requests))

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
