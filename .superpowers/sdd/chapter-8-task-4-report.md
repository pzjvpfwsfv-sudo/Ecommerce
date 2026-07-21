# Chapter 8 Task 4 Report

## RED/GREEN

- RED 1: `tests.test_openai_compatible_analyzer` failed because the adapter and injectable settings did not exist.
- GREEN 1: adapter request/response parsing and settings defaults passed (2 tests).
- RED 2: dependency-construction tests failed because `app.dependencies` did not exist.
- GREEN 2: analyzer selection, fallback wiring, Trino settings, and unknown-mode rejection passed (4 tests).

## Verification

- `python -m unittest tests.test_openai_compatible_analyzer tests.test_analysis_service -v`: 16 passed.
- `PYTHONPATH=services/api; python -m unittest discover -s tests -v`: 72 passed.
- `git diff --check`: passed.

## Files

- Modified `services/api/app/analyzers.py` and `services/api/app/config.py`.
- Added `services/api/app/dependencies.py` and `tests/test_openai_compatible_analyzer.py`.

## Self-Review And Concerns

- The system prompt limits responses to supplied evidence, prohibits SQL and outside data, requires an insufficient-evidence statement, and permits Chinese or English only.
- HTTP, response-status, JSON, and response-structure errors propagate to `AnalysisService`, preserving its existing safe fallback.
- API keys are neither logged nor included in responses or repository configuration; tests use the dummy value `secret`.
- No live provider call was made because this task intentionally has no real credentials; mocked HTTP verifies the request contract without network access.

## Review Fix: P1/P2

### RED/GREEN

- RED: the focused suite failed only for `missing_narrative_key`; an incomplete inner narrative produced no degradation warning because Pydantic defaults filled absent list fields.
- GREEN: the adapter now requires a JSON object with explicit `summary`, `insights`, `risks`, and `actions` keys before Pydantic validation. Extra keys are allowed and field types remain strictly validated by Pydantic.
- The MockTransport matrix covers 11 offline failure scenarios: non-2xx, invalid outer JSON, invalid `choices`, invalid `message`, invalid `content`, invalid inner JSON, each of four missing narrative keys, and an invalid narrative field type.

### Review Verification

- `python -B -m unittest tests.test_openai_compatible_analyzer -v`: 5 passed after the RED failure was fixed.
- `python -B -m unittest tests.test_openai_compatible_analyzer tests.test_analysis_service -v`: 17 passed.
- `PYTHONPATH=services/api; python -B -m unittest discover -s tests -v`: 73 passed.
- The happy-path contract asserts the exact URL, configured timeout, payload model, evidence, authorization header, and system-prompt boundaries.
- Every failure-matrix case asserts a degradation warning and the actual `rule_based` analyzer. No external request or real API key is used.
