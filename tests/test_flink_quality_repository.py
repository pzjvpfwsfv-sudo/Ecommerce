import unittest

import httpx

from app.flink_quality_repository import FlinkQualityRepository, QUALITY_COUNTERS


JOB_ID = "a" * 32
JOB_NAME = "chapter-9-datastream-quality-production"


def job(state: str) -> dict[str, str]:
    return {"jid": JOB_ID, "name": JOB_NAME, "state": state}


def valid_flink_responses() -> dict[str, object]:
    return {
        "/jobs/overview": {"jobs": [job("RUNNING")]},
        f"/jobs/{JOB_ID}/checkpoints": {
            "counts": {"completed": 3, "failed": 0},
            "latest": {"completed": {"latest_ack_timestamp": 1785686400000}},
        },
        f"/jobs/{JOB_ID}": {"vertices": [{"id": "v1"}, {"id": "v2"}]},
        f"/jobs/{JOB_ID}/vertices/v1/metrics": [
            {"id": "operator.valid_events_total"},
            {"id": "operator.dlq_events_total"},
            {"id": "operator.late_events_total"},
            {"id": "unknown_metric"},
        ],
        f"/jobs/{JOB_ID}/vertices/v2/metrics": [
            {"id": "operator.duplicate_events_total"},
            {"id": "operator.parse_errors_total"},
            {"id": "operator.validation_errors_total"},
        ],
    }


def metric_values(vertex_id: str, counters: list[str]) -> list[dict[str, str]]:
    values = {
        "v1": {
            "operator.valid_events_total": "2",
            "operator.dlq_events_total": "0",
            "operator.late_events_total": "0",
        },
        "v2": {
            "operator.duplicate_events_total": "0",
            "operator.parse_errors_total": "0",
            "operator.validation_errors_total": "0",
        },
    }
    return [{"id": counter, "sum": values[vertex_id][counter]} for counter in counters]


def repository_for(responses: dict[str, object]):
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        payload = responses.get(request.url.path)
        if payload is None:
            return httpx.Response(404, text="unexpected Flink endpoint")
        if request.url.path.endswith("/metrics") and request.url.params.get_list("get"):
            if isinstance(payload, list) and payload and "sum" in payload[0]:
                return httpx.Response(200, json=payload)
            vertex_id = request.url.path.split("/")[-2]
            return httpx.Response(200, json=metric_values(vertex_id, request.url.params.get_list("get")))
        return httpx.Response(200, json=payload)

    repository = FlinkQualityRepository(
        base_url="http://flink:8081/",
        production_job_name=JOB_NAME,
        timeout_seconds=5,
        client_factory=lambda: httpx.Client(transport=httpx.MockTransport(handler)),
    )
    return repository, requests


def repository_for_jobs(jobs: list[dict[str, str]]) -> FlinkQualityRepository:
    responses = valid_flink_responses()
    responses["/jobs/overview"] = {"jobs": jobs}
    return repository_for(responses)[0]


class FlinkQualityRepositoryTest(unittest.TestCase):
    def test_fetch_health_requires_one_running_job_and_maps_checkpoints_and_counters(self):
        repository, requests = repository_for(valid_flink_responses())

        evidence = repository.fetch_health()

        self.assertEqual(JOB_ID, evidence.job_id)
        self.assertEqual("RUNNING", evidence.job_state)
        self.assertEqual(3, evidence.completed_checkpoints)
        self.assertEqual(0, evidence.failed_checkpoints)
        self.assertEqual(2, evidence.counters["valid_events_total"])
        self.assertNotIn("unknown_metric", evidence.counters)
        self.assertEqual(set(QUALITY_COUNTERS), set(evidence.counters))
        self.assertTrue(all(request.method == "GET" for request in requests))
        self.assertTrue(all(request.url.host == "flink" for request in requests))
        self.assertEqual(
            [
                "/jobs/overview",
                f"/jobs/{JOB_ID}/checkpoints",
                f"/jobs/{JOB_ID}",
                f"/jobs/{JOB_ID}/vertices/v1/metrics",
                f"/jobs/{JOB_ID}/vertices/v1/metrics",
                f"/jobs/{JOB_ID}/vertices/v2/metrics",
                f"/jobs/{JOB_ID}/vertices/v2/metrics",
            ],
            [request.url.path for request in requests],
        )
        metric_requests = [request for request in requests if request.url.path.endswith("/metrics")]
        self.assertTrue(all(request.url.params["agg"] == "sum" for request in metric_requests[1::2]))

    def test_fetch_health_rejects_zero_duplicate_or_non_running_jobs(self):
        for jobs in ([], [job("FAILED")], [job("RUNNING"), job("RUNNING")]):
            with self.subTest(jobs=jobs), self.assertRaises(ValueError):
                repository_for_jobs(jobs).fetch_health()

    def test_fetch_health_rejects_malformed_checkpoint_counts(self):
        invalid_counts = (
            {},
            {"completed": True, "failed": 0},
            {"completed": -1, "failed": 0},
            {"completed": 1, "failed": "0"},
        )
        for counts in invalid_counts:
            with self.subTest(counts=counts):
                responses = valid_flink_responses()
                responses[f"/jobs/{JOB_ID}/checkpoints"] = {"counts": counts, "latest": {}}
                with self.assertRaises(ValueError):
                    repository_for(responses)[0].fetch_health()

    def test_fetch_health_rejects_invalid_job_id_vertices_and_counter_values(self):
        invalid_responses = []

        bad_job_id = valid_flink_responses()
        bad_job_id["/jobs/overview"] = {"jobs": [{"jid": "A" * 32, "name": JOB_NAME, "state": "RUNNING"}]}
        invalid_responses.append(bad_job_id)

        missing_vertex = valid_flink_responses()
        missing_vertex[f"/jobs/{JOB_ID}"] = {"vertices": [{"id": ""}]}
        invalid_responses.append(missing_vertex)

        missing_counter = valid_flink_responses()
        missing_counter[f"/jobs/{JOB_ID}/vertices/v2/metrics"] = [
            {"id": "operator.duplicate_events_total"},
            {"id": "operator.parse_errors_total"},
        ]
        invalid_responses.append(missing_counter)

        for responses in invalid_responses:
            with self.subTest(responses=responses), self.assertRaises(ValueError):
                repository_for(responses)[0].fetch_health()

        for value in ("-1", "2.0", True):
            with self.subTest(value=value):
                responses = valid_flink_responses()
                values = metric_values("v1", [
                    "operator.valid_events_total",
                    "operator.dlq_events_total",
                    "operator.late_events_total",
                ])
                values[0]["sum"] = value
                responses[f"/jobs/{JOB_ID}/vertices/v1/metrics"] = values
                with self.assertRaises(ValueError):
                    repository_for(responses)[0].fetch_health()

    def test_fetch_health_scrubs_upstream_errors_and_timeouts(self):
        def error_handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(500, text="internal Flink body")

        error_repository = FlinkQualityRepository(
            base_url="http://flink:8081",
            production_job_name=JOB_NAME,
            timeout_seconds=5,
            client_factory=lambda: httpx.Client(transport=httpx.MockTransport(error_handler)),
        )
        with self.assertRaisesRegex(RuntimeError, r"^Flink quality upstream request failed$") as error:
            error_repository.fetch_health()
        self.assertNotIn("internal Flink body", str(error.exception))

        def timeout_handler(request: httpx.Request) -> httpx.Response:
            raise httpx.ReadTimeout("internal timeout detail", request=request)

        timeout_repository = FlinkQualityRepository(
            base_url="http://flink:8081",
            production_job_name=JOB_NAME,
            timeout_seconds=5,
            client_factory=lambda: httpx.Client(transport=httpx.MockTransport(timeout_handler)),
        )
        with self.assertRaisesRegex(RuntimeError, r"^Flink quality request timed out$") as timeout:
            timeout_repository.fetch_health()
        self.assertNotIn("internal timeout detail", str(timeout.exception))


if __name__ == "__main__":
    unittest.main()
