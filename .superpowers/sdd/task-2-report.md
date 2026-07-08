# Task 2 Report: Add Trino service and catalog configuration

## Scope

- Updated `infra/.env.example` with `TRINO_PORT=8088` and `TRINO_CONTAINER_NAME=ecom-trino`.
- Updated `infra/docker-compose.yml` to add the `trino` service with the required `lakehouse` profile, MinIO dependencies, port mapping, catalog mount, and `platform-net` network.
- Created `infra/compose/trino/catalog/lakehouse.properties` with the exact Iceberg-on-MinIO catalog properties from the task brief.
- Extended `tests/test_chapter_6_trino_artifacts.py` with the focused compose assertion required by the brief.

## TDD Evidence

1. Added `test_compose_mounts_trino_catalog_and_port`.
2. Ran:

```powershell
python -m unittest tests.test_chapter_6_trino_artifacts.Chapter6TrinoArtifactsTest.test_compose_mounts_trino_catalog_and_port -v
```

Observed expected RED failure because `infra/docker-compose.yml` did not yet contain `- "${TRINO_PORT}:8080"`.

3. Implemented the Trino env, compose, and catalog changes.
4. Re-ran:

```powershell
python -m unittest tests.test_chapter_6_trino_artifacts.Chapter6TrinoArtifactsTest.test_compose_mounts_trino_catalog_and_port -v
```

Observed GREEN pass.

## Verification

Focused single test:

```powershell
python -m unittest tests.test_chapter_6_trino_artifacts.Chapter6TrinoArtifactsTest.test_compose_mounts_trino_catalog_and_port -v
```

Result: `OK`

Focused Chapter 6 artifact suite:

```powershell
python -m unittest tests.test_chapter_6_trino_artifacts -v
```

Result:

- Passed:
  - `test_compose_mounts_trino_catalog_and_port`
  - `test_env_and_compose_define_trino_service`
  - `test_trino_catalog_points_to_minio_iceberg`
- Failed:
  - `test_docs_mention_chapter6_trino_validation`
  - `test_query_sql_covers_count_and_group_by`
  - `test_verification_script_runs_chapter5_then_queries_trino`

## Constraints and Concerns

- I did not modify `README.md`, `jobs/sql/11_trino_read_iceberg_user_behavior.sql`, or `scripts/verify_chapter_6_trino_queries.ps1` because they are outside the ownership boundary for Task 2.
- The existing stricter docs expectation (`Trino + Iceberg only`) in `tests/test_chapter_6_trino_artifacts.py` was preserved as requested.

## Review Fix Round 2

### Review Scope Applied

- Updated `infra/compose/trino/catalog/lakehouse.properties` for Trino 458 native S3 support using `fs.s3.enabled=true` and `s3.*` properties while keeping the same MinIO target and Iceberg warehouse.
- Strengthened `tests/test_chapter_6_trino_artifacts.py` so the catalog assertion now requires the native S3 property names and rejects legacy `hive.s3.endpoint`, `hive.s3.path-style-access`, and `hive.s3.ssl.enabled`.
- Kept runtime verification out of scope for Task 2; proof of live Trino-to-MinIO behavior belongs to the later Chapter 6 verification task and its dedicated runtime assets.

### TDD Evidence For Review Fix

1. Tightened `test_trino_catalog_points_to_minio_iceberg` first.
2. Ran:

```powershell
python -m unittest tests.test_chapter_6_trino_artifacts.Chapter6TrinoArtifactsTest.test_trino_catalog_points_to_minio_iceberg -v
```

Observed RED failure because the catalog still contained legacy `hive.s3.*` properties and `fs.native-s3.enabled=true` instead of `fs.s3.enabled=true`.

3. Updated only `infra/compose/trino/catalog/lakehouse.properties` to the native S3 form required for Trino 458.
4. Re-ran the focused Task 2 suite:

```powershell
python -m unittest tests.test_chapter_6_trino_artifacts -v
```

Result:

- Passed:
  - `test_compose_mounts_trino_catalog_and_port`
  - `test_env_and_compose_define_trino_service`
  - `test_trino_catalog_points_to_minio_iceberg`
- Failed, still out of Task 2 ownership:
  - `test_docs_mention_chapter6_trino_validation`
  - `test_query_sql_covers_count_and_group_by`
  - `test_verification_script_runs_chapter5_then_queries_trino`

## Review Fix Round 3

### Scope Applied

- Reworked `test_compose_mounts_trino_catalog_and_port` so it extracts the `trino:` service block from `infra/docker-compose.yml` and asserts the profile, port mapping, container name, and catalog mount only within that slice.

### Verification

Focused Task 2 artifact suite:

```powershell
python -m unittest tests.test_chapter_6_trino_artifacts -v
```

Result:

- Passed:
  - `test_compose_mounts_trino_catalog_and_port`
  - `test_env_and_compose_define_trino_service`
  - `test_trino_catalog_points_to_minio_iceberg`
- Failed, still outside Task 2 ownership:
  - `test_docs_mention_chapter6_trino_validation`
  - `test_query_sql_covers_count_and_group_by`
  - `test_verification_script_runs_chapter5_then_queries_trino`
