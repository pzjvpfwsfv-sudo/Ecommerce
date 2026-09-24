# G2-E Olist Order Domain Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build an attributable Olist historical-order domain from the official nine-file bundle through bounded Iceberg ingestion, anti-fanout curated facts, immutable Doris metrics, and strict `/api/v1/orders/*` FastAPI contracts.

**Architecture:** A standard-library Python pipeline verifies the official archive and extracted files, streams each CSV into canonical JSONL, and assigns one full-bundle identity. Flink SQL ingests one bounded entity at a time into `lakehouse.olist`; Trino builds snapshot-pinned facts/dimensions and recomputes DAY, MONTH, and FULL metrics; Doris exposes candidates only after a publication row is written last. The order API is a separate typed domain and never joins Olist identities to REES46 identities.

**Tech Stack:** Python 3.12 standard library, Python `unittest`, PowerShell 7, Apache Flink SQL 1.19.2, Apache Iceberg 1.6.1, Hive Metastore, MinIO, Trino 458, Apache Doris 2.1.9, FastAPI 0.115, Pydantic v2, PyMySQL, Docker Compose.

**Spec:** `docs/superpowers/specs/2026-09-21-graduation-olist-order-domain-design.md`

## Global Constraints

- Dataset identity, domain, schema, metric version, and API prefix are exactly `olist-brazilian-ecommerce-v2`, `orders`, `lakehouse.olist`, `orders-v1`, and `/api/v1/orders`.
- The formal input is the official Kaggle Version 2 archive and exactly the nine registered CSV filenames; mirrors, augmented copies, fixtures, and pre-cleaned derivatives cannot become formal business evidence.
- `source_bundle_sha256` uses filename-sorted `name + NUL + bytes + NUL + row_count + NUL + sha256`; `metric_run_id` is `orders-v1-b` followed by exactly 64 lowercase hexadecimal characters and is never truncated in storage or API output.
- Raw files, normalized files, runtime reports, and credentials stay outside Git on D drive; the default root is `D:\EcommerceData\olist`, and Kaggle secrets never appear in source, arguments recorded in Git, logs, or reports.
- CSV processing is streaming and standard-library based; the formal path cannot load a complete table into Pandas or memory.
- Business IDs and postal prefixes remain strings; money uses exact non-negative decimals; source timestamps remain timezone-unspecified `TIMESTAMP(3)` values and are never relabeled UTC.
- Each Flink invocation processes one bounded source entity, and each Trino invocation builds or measures one stage at a time for the 16 GB workstation.
- Order, item, payment, and review facts are aggregated separately before any order-level join; raw one-to-many facts cannot be wide-joined and summed.
- `item_value`, `freight_value`, and `payment_value` are not profit, net revenue, audited GMV, or refund-adjusted revenue; currency stays null until verified from current official documentation.
- Olist and REES46 IDs are unrelated; no code may infer a shared user, product, session, or order journey.
- Candidate Doris rows remain invisible until all six families pass readback and the `PUBLISHED` row is inserted last; history is immutable and a conflicting same-run retry fails closed.
- API SQL is fixed and parameterized. Window, date range, dimension, sort, and limit are whitelisted; invalid publication identity or catalog data returns safe `503`.
- Docker programs, images, temporary files, and data remain on D drive. No task may delete Docker volumes, Iceberg tables, Doris rows, historical publications, or user files.
- G2-E does not implement the G3 frontend or G4 RAG; it only supplies their stable versioned contracts and definitions.

## Review Focus

- **Malformed or adversarial bundle:** Task 1 tests quoted multiline rows, over-wide records, archive traversal, symlink escape, missing/extra CSVs, archive/extraction mismatch, and verifies that no formal output survives failure.
- **Mixed or partially ingested source state:** Task 2 tests that an existing table is accepted only when bundle, row count, unique row IDs, row-ID digest, and snapshot evidence all match; every other state fails without appending.
- **One-to-many fanout:** Task 3 fixtures include two items, two payments, and two reviews for one order and require item, payment, and review totals to remain independent after curated construction.
- **Ambiguous business semantics:** Task 4 tests multi-payment grouping, multi-review order averaging, seller-state non-additivity, unknown groups, temporal exclusions, and independently recomputed DAY/MONTH/FULL totals.
- **Stale or injectable serving requests:** Tasks 5 and 7 test publication-last failure, conflicting retries, mismatched snapshot maps, cross-paired definition versions, invalid date ranges, unsupported sorts, boolean limits, SQL-like inputs, and safe `503` behavior.

---

## File Map

### Data preparation and source ingestion

- Create `generators/olist_data/__init__.py`: package boundary and version constants.
- Create `generators/olist_data/schemas.py`: immutable nine-file registry, field kinds, keys, known enums, and Iceberg column types.
- Create `generators/olist_data/csv_io.py`: bounded logical CSV reader with a per-record character budget.
- Create `generators/olist_data/file_io.py`: streaming SHA-256 and atomic no-overwrite writers.
- Create `generators/olist_data/normalization.py`: field normalization, validation reason codes, canonical rows, and `source_row_id`.
- Create `generators/olist_data/bundle.py`: archive/directory verification, formal manifest, bundle digest, and JSONL publication.
- Create `generators/olist_data/flink_sql.py`: fixed-registry rendering for one bounded source entity.
- Create `generators/olist_data/__main__.py`: `prepare-bundle` and `render-flink` CLI commands.
- Create `jobs/sql/19_olist_source_ingest.sql.template`: fixed bounded filesystem-to-Iceberg statement shell.
- Create `jobs/sql/20_olist_source_verify.sql.template`: row, identity, bundle, and snapshot checks.
- Create `scripts/run_g2e_olist_source.ps1`: safe one-entity submission/resume runner.
- Create `scripts/verify_g2e_olist_source.ps1`: Trino/Flink source-table verifier.
- Modify `infra/.env.example`: add D-drive Olist root and order catalog path.
- Modify `infra/docker-compose.yml`: read-only Olist bind mount for the three Flink services and order catalog environment for API.

### Curated model and metric publication

- Create `jobs/sql/21_olist_curated_model.sql.template`: nine staged snapshot-pinned CTAS statements.
- Create `jobs/sql/22_olist_curated_verify.sql.template`: hard gates, reportable quality, anti-fanout, and curated snapshot checks.
- Create `scripts/build_g2e_olist_curated.ps1`: source-gated, resumable one-stage curated builder.
- Create `scripts/verify_g2e_olist_curated.ps1`: complete curated-domain verifier and evidence writer.
- Create `configs/metrics/orders-v1.json`: versioned formulas, denominators, exclusions, limitations, and forbidden claims.
- Create `jobs/sql/23_g2e_order_metrics.sql.template`: source identity plus six fixed metric result families.
- Create `infra/compose/doris/init/03_create_order_metrics.sql`: seven immutable run-scoped Doris tables.
- Create `scripts/lib/G2e.OrderMetrics.psm1`: pure parsing, identity, reconciliation, digest, and candidate-export functions.
- Create `scripts/refresh_g2e_order_metrics.ps1`: fail-closed Trino-to-Doris refresh with publication last.

### API, acceptance, and documentation

- Create `services/api/app/order_models.py`: strict order metadata, points, quality, publication, and definition models.
- Create `services/api/app/order_metric_definitions.py`: exact `orders-v1` catalog loader.
- Create `services/api/app/order_repository.py`: fixed parameterized Doris queries and whitelist mapping.
- Create `services/api/app/order_service.py`: publication identity, snapshot, reconciliation, range, and response validation.
- Modify `services/api/app/config.py`: order-definition path setting.
- Modify `services/api/app/dependencies.py`: order service factory.
- Modify `services/api/app/main.py`: seven order routes and dual-domain definition dispatch.
- Create `scripts/verify_g2e_order_domain.ps1`: end-to-end Trino/Doris/API verifier.
- Create `docs/graduation/olist-source-acquisition.md`: official acquisition and preparation runbook.
- Create `docs/graduation/olist-order-domain-runbook.md`: bounded ingestion, curation, publication, API, and recovery runbook.
- Modify `README.md`: measured G2-E status and honest next step.
- Modify `docs/graduation/data-readiness.md`: measured bundle/snapshot/metric evidence and remaining G3/G4/G5 work.
- Create `tests/test_olist_data.py`, `tests/test_olist_data_cli.py`, `tests/test_g2e_olist_lakehouse.py`, `tests/test_g2e_olist_curated.py`, `tests/test_g2e_order_metrics.py`, `tests/test_order_metrics_api.py`, and `tests/test_g2e_order_domain.py`.

### Task 1: Verify and Normalize the Official Nine-File Bundle

**Files:**
- Create: `generators/olist_data/__init__.py`
- Create: `generators/olist_data/schemas.py`
- Create: `generators/olist_data/csv_io.py`
- Create: `generators/olist_data/file_io.py`
- Create: `generators/olist_data/normalization.py`
- Create: `generators/olist_data/bundle.py`
- Create: `generators/olist_data/__main__.py`
- Create: `tests/test_olist_data.py`
- Create: `tests/test_olist_data_cli.py`
- Create: `docs/graduation/olist-source-acquisition.md`

**Interfaces:**
- Consumes: an official ZIP path, an extracted directory, current acquisition time, Kaggle version text, and current displayed license name/URL supplied by the operator.
- Produces: `TABLE_SPECS: Mapping[str, TableSpec]`; `normalize_row(spec: TableSpec, row: Mapping[str, str], row_number: int) -> dict[str, object]`; `prepare_bundle(archive_path: Path, input_dir: Path, output_root: Path, acquisition: Acquisition) -> dict[str, object]`; and `$outputRoot/prepared/$sourceBundleSha256/source-bundle.json` plus nine `$outputRoot/prepared/$sourceBundleSha256/normalized/$entity.jsonl` files.
- CLI: `python -m generators.olist_data prepare-bundle --archive $archivePath --input-dir $inputDir --output-root $outputRoot --acquired-at $acquiredAt --license-name $licenseName --license-url $licenseUrl`; paths and license values are runtime inputs and are not persisted in Git.

- [x] **Step 1: Write failing schema, parser, identity, and bundle tests**

Use in-memory fixture rows clearly labeled synthetic. Pin the exact official file/header registry:

```python
EXPECTED_HEADERS = {
    "orders": ("order_id", "customer_id", "order_status", "order_purchase_timestamp", "order_approved_at", "order_delivered_carrier_date", "order_delivered_customer_date", "order_estimated_delivery_date"),
    "order_items": ("order_id", "order_item_id", "product_id", "seller_id", "shipping_limit_date", "price", "freight_value"),
    "order_payments": ("order_id", "payment_sequential", "payment_type", "payment_installments", "payment_value"),
    "order_reviews": ("review_id", "order_id", "review_score", "review_comment_title", "review_comment_message", "review_creation_date", "review_answer_timestamp"),
    "customers": ("customer_id", "customer_unique_id", "customer_zip_code_prefix", "customer_city", "customer_state"),
    "products": ("product_id", "product_category_name", "product_name_lenght", "product_description_lenght", "product_photos_qty", "product_weight_g", "product_length_cm", "product_height_cm", "product_width_cm"),
    "sellers": ("seller_id", "seller_zip_code_prefix", "seller_city", "seller_state"),
    "geolocation": ("geolocation_zip_code_prefix", "geolocation_lat", "geolocation_lng", "geolocation_city", "geolocation_state"),
    "category_translation": ("product_category_name", "product_category_name_english"),
}
```

Add tests that prove:

```python
def test_money_zip_and_timestamp_are_canonical_without_invented_timezone(self):
    row = normalize_row(TABLE_SPECS["order_items"], SYNTHETIC_ITEM, row_number=1)
    self.assertEqual("01001", normalize_row(TABLE_SPECS["customers"], SYNTHETIC_CUSTOMER, 1)["customer_zip_code_prefix"])
    self.assertEqual("0.10", row["price"])
    self.assertEqual("0.10", row["price_decimal"])
    self.assertNotIn("Z", row["shipping_limit_date"])

def test_identity_changes_with_row_number_or_canonical_content(self):
    first = normalize_row(TABLE_SPECS["order_items"], SYNTHETIC_ITEM, 1)
    second = normalize_row(TABLE_SPECS["order_items"], SYNTHETIC_ITEM, 2)
    self.assertRegex(first["source_row_id"], r"^[0-9a-f]{64}$")
    self.assertNotEqual(first["source_row_id"], second["source_row_id"])
```

Also test exact headers, UTF-8 with optional BOM only at file start, quoted multiline comments, blank-to-null conversion, 65,536-character logical-record bound, decimal exponent/negative/NaN rejection, required ID rejection, invalid/nonexistent dates, stable canonical JSON, ZIP `../` and absolute member rejection, ZIP/extracted byte mismatch, extra/missing CSVs, duplicate case-insensitive filenames, symlinked files/directories, output inside input, existing target, interrupted staging cleanup, and independent recomputation of the bundle digest.

- [x] **Step 2: Run the focused tests and confirm RED**

Run:

```powershell
$env:TEMP='D:\EcommerceDev\temp'
$env:TMP=$env:TEMP
python -m unittest tests.test_olist_data tests.test_olist_data_cli -v
```

Expected: FAIL because `generators.olist_data` does not exist.

- [x] **Step 3: Implement the immutable registry and streaming primitives**

Use frozen records and preserve the source spelling `lenght`:

```python
@dataclass(frozen=True)
class FieldSpec:
    name: str
    kind: Literal["id", "text", "zip", "uint", "decimal", "coordinate", "timestamp"]
    required: bool = False

@dataclass(frozen=True)
class TableSpec:
    entity: str
    source_file: str
    fields: tuple[FieldSpec, ...]
    key_fields: tuple[str, ...]
    target_table: str
```

Registry rules are exact:

| Entity | Source file | Required key | Typed fields |
| --- | --- | --- | --- |
| `orders` | `olist_orders_dataset.csv` | `order_id`; required FK `customer_id` | required purchase timestamp; four optional timestamps; status text |
| `order_items` | `olist_order_items_dataset.csv` | `(order_id, order_item_id)`; required product/seller IDs | positive item ID; required shipping timestamp; non-negative `price`, `freight_value` |
| `order_payments` | `olist_order_payments_dataset.csv` | `(order_id, payment_sequential)` | positive sequence; non-negative installments; required payment type; non-negative `payment_value` |
| `order_reviews` | `olist_order_reviews_dataset.csv` | stable `source_row_id`; `review_id` is reportable, not unique | required order ID; integer score; optional text/timestamps |
| `customers` | `olist_customers_dataset.csv` | `customer_id` | required unique ID; zip string; city/state text |
| `products` | `olist_products_dataset.csv` | `product_id` | optional category; optional non-negative numeric dimensions/counts |
| `sellers` | `olist_sellers_dataset.csv` | `seller_id` | zip string; city/state text |
| `geolocation` | `olist_geolocation_dataset.csv` | stable `source_row_id` | zip string; signed coordinates; city/state text |
| `category_translation` | `product_category_name_translation.csv` | `product_category_name` | required English label |

Known order statuses are exactly `created`, `approved`, `invoiced`, `processing`, `shipped`, `delivered`, `unavailable`, and `canceled`. Known payment types are exactly `credit_card`, `boleto`, `voucher`, `debit_card`, and `not_defined`. Other nonblank values remain in facts as reportable unknowns; review scores outside 1 through 5 remain reportable and are excluded from score-dependent metrics.

`bounded_csv_reader()` must reset one shared budget per logical record. Atomic writers create a same-directory `.part` file, flush and `fsync`, then use a no-overwrite link/rename publication that fails if a concurrent writer won.

Money keeps two representations in normalized JSON: the trimmed source text under the official column name and a canonical two-decimal string under that name plus the `_decimal` suffix. For example, source `price="0.1"` remains `price="0.1"` while `price_decimal="0.10"`; validation rejects exponents, signs, NaN/infinity, more than two fractional digits, and overflow. Stable row identity uses only normalized official-header values, not derived convenience fields.

- [x] **Step 4: Implement canonical normalization and bundle publication**

Canonical identity is byte-defined:

```python
canonical_row = json.dumps(
    [normalized[field.name] for field in spec.fields],
    ensure_ascii=False,
    separators=(",", ":"),
)
identity = "\0".join((DATASET_ID, spec.source_file, str(row_number), canonical_row))
source_row_id = sha256(identity.encode("utf-8")).hexdigest()
```

The manifest contains only attributable metadata: schema version, dataset ID, Kaggle version, fixed official URL, acquisition time, displayed license name/URL, archive filename/hash/bytes, sorted nine-file entries with raw hash/bytes/row count/header, normalized filename/hash/bytes/row count/row-ID-sequence hash, and full bundle hash. It never contains an API token, cookie, absolute username path, or fixture scope.

Validate every archive member and extracted file before creating staging output. Write each JSON object with sorted keys and one LF. Append `source_file`, one-based `source_row_number`, `source_row_id`, `source_bundle_sha256`, and `schema_version=1`; because the bundle digest is known only after raw verification, compute all raw identities first and normalize in a second streaming pass. Publish the whole prepared directory by one no-overwrite directory rename only after all nine output reconciliations pass.

- [x] **Step 5: Implement the safe CLI and acquisition guide**

`prepare-bundle` accepts only an absolute D-drive `--output-root`, rejects a C-drive output, rejects output nested in the input/archive directory, and maps `OSError`, `UnicodeError`, `csv.Error`, `zipfile.BadZipFile`, and validation errors to a stable reason-code suffix on stderr, such as `olist-data error: invalid_header`, with exit code 1. It prints the completed manifest JSON only after publication.

The guide uses browser/manual Kaggle authentication, records the currently displayed license values interactively, and runs without embedding credentials:

```powershell
$licenseName = Read-Host 'Kaggle 页面当前显示的许可名称'
$licenseUrl = Read-Host 'Kaggle 页面当前显示的许可链接'
python -m generators.olist_data prepare-bundle `
  --archive 'D:\EcommerceData\olist\downloads\olist-brazilian-ecommerce.zip' `
  --input-dir 'D:\EcommerceData\olist\raw\v2' `
  --output-root 'D:\EcommerceData\olist' `
  --acquired-at ((Get-Date).ToUniversalTime().ToString('o')) `
  --license-name $licenseName `
  --license-url $licenseUrl
```

- [x] **Step 6: Run tests and confirm GREEN**

Run the command from Step 2.

Expected: all Olist preparation tests PASS; each failure fixture leaves no published manifest, JSONL, or `.part` file.

- [x] **Step 7: Commit**

```powershell
git add generators/olist_data tests/test_olist_data.py tests/test_olist_data_cli.py docs/graduation/olist-source-acquisition.md
git commit -m "feat: prepare attributable Olist source bundles"
```

### Task 2: Ingest and Verify One Bounded Iceberg Source at a Time

**Files:**
- Create: `generators/olist_data/flink_sql.py`
- Modify: `generators/olist_data/__main__.py`
- Create: `jobs/sql/19_olist_source_ingest.sql.template`
- Create: `jobs/sql/20_olist_source_verify.sql.template`
- Create: `scripts/run_g2e_olist_source.ps1`
- Create: `scripts/verify_g2e_olist_source.ps1`
- Modify: `infra/.env.example`
- Modify: `infra/docker-compose.yml`
- Create: `tests/test_g2e_olist_lakehouse.py`

**Interfaces:**
- Consumes: a Task 1 manifest path and one entity from the exact nine-value registry.
- Produces: `render_flink_ingest_sql(entity: str, source_bundle_sha256: str) -> str`; one fixed `lakehouse.olist.${entity}_src_v1` table selected from the registry; `Get-G2eSourceDeployment`; and a verification report at `tmp/graduation/g2e/$sourceBundleSha256/sources/$entity.json`.
- Runner: `scripts/run_g2e_olist_source.ps1 -ManifestPath $manifestPath -Entity $entity` where both values have already passed the D-drive/registry checks.
- Verifier: `scripts/verify_g2e_olist_source.ps1 -ManifestPath $manifestPath -Entity $entity -JobId $jobId`; `-JobId` is omitted only for an exact `already_ingested` verification.

- [x] **Step 1: Write failing SQL, Compose, runner, and verifier tests**

Assert the renderer accepts only registry entities and lowercase 64-hex bundle IDs, emits one `INSERT INTO`, one fixed target, `execution.runtime-mode=batch`, parallelism 1, strict JSON parsing, the derived `/data/olist/prepared/$sourceBundleSha256/normalized/$entity.jsonl` path, all source coordinates, and no Kafka connector. Assert only fixed registry values can fill these template tokens:

```text
__PIPELINE_NAME__
__SOURCE_PATH__
__SOURCE_COLUMNS__
__TARGET_TABLE__
__TARGET_COLUMNS__
__SELECT_COLUMNS__
__SOURCE_BUNDLE_SHA256__
```

Test Compose YAML parsing to require the same read-only `${OLIST_DATA_DIR}:/data/olist` long-syntax bind for `flink-jobmanager`, `flink-taskmanager`, and `flink-sql-client`. Test `OLIST_DATA_DIR=D:/EcommerceData/olist` in `.env.example`.

Dot-source the scripts with `-FunctionsOnly` and test:

```powershell
$deployment = Get-G2eSourceDeployment -Manifest $manifest -Entity 'orders'
Assert-G2eSourceState -Observed $observed -Expected $deployment
```

Negative cases cover non-D-drive manifests, manifests outside `OLIST_DATA_DIR`, altered normalized hashes, unresolved SQL tokens, unknown entities, active duplicate pipeline jobs, a target containing a different bundle, partial nonzero rows, duplicate IDs, row gaps, wrong source filename, digest mismatch, no Iceberg snapshot, and a supplied Job ID that is not the finished bounded job.

- [x] **Step 2: Run the focused test and confirm RED**

Run:

```powershell
$env:TEMP='D:\EcommerceDev\temp'
$env:TMP=$env:TEMP
python -m unittest tests.test_g2e_olist_lakehouse -v
```

Expected: FAIL because the renderer, templates, scripts, and mounts do not exist.

- [x] **Step 3: Implement fixed-registry SQL rendering and the read-only mount**

The template creates `lakehouse.olist` and one temporary filesystem JSON source. Source fields match Task 1 JSON types; target business columns use `VARCHAR`, `BIGINT`, `DECIMAL`, or timezone-free `TIMESTAMP(3)`. Each monetary source field becomes both a `_raw`-suffixed `VARCHAR` preserving the official text and a typed `DECIMAL(38,2)` under the official field name for downstream use. These are followed by:

```sql
source_file VARCHAR NOT NULL,
source_row_number BIGINT NOT NULL,
source_row_id VARCHAR NOT NULL,
source_bundle_sha256 VARCHAR NOT NULL,
schema_version INTEGER NOT NULL
```

All nine source tables are unpartitioned at this dataset size; date partitioning begins only in the curated order fact where the query pattern justifies it. Every `SELECT` compares the row bundle to the already-validated literal, and the final row-count reconciliation prevents filtered or foreign rows from passing. The Python renderer pulls every identifier, type, cast expression, and filename from `TABLE_SPECS`; no CLI value becomes an SQL identifier or expression.

Add this Compose mount to all three Flink services:

```yaml
- type: bind
  source: ${OLIST_DATA_DIR}
  target: /data/olist
  read_only: true
```

- [x] **Step 4: Implement safe submit/resume behavior**

Before submission, the runner verifies the formal manifest and normalized file against Task 1 hashes, checks the physical path remains below `OLIST_DATA_DIR`, starts only Flink/MinIO/Metastore/Trino, and queries the fixed target name.

State transitions are exact:

| Observed target | Action |
| --- | --- |
| absent | render and submit one bounded job |
| present with zero rows and no snapshot | submit to resume table creation failure |
| present and exact verified identity | return `already_ingested` without submitting |
| present with any row/identity/digest discrepancy | fail; never append or replace |

Implement the branch before any submission side effect:

```powershell
$state = Get-G2eSourceState -Deployment $deployment
switch ($state.Kind) {
    'Absent' { $shouldSubmit = $true }
    'EmptyWithoutSnapshot' { $shouldSubmit = $true }
    'Verified' { return New-G2eSourceResult -Status 'already_ingested' -State $state }
    default { throw "Unsafe existing Olist source state: $($state.Kind)" }
}
```

Wait for the exact pipeline job to reach `FINISHED`; reject `FAILED`, `CANCELED`, timeout, or multiple matching jobs. A completed checkpoint is recorded when present, but a short batch job may prove atomic completion through `FINISHED` plus the committed Iceberg snapshot.

- [x] **Step 5: Implement Trino verification**

`20_olist_source_verify.sql.template` returns fixed named results for table existence, row summary, ordered row-ID digest, business-key uniqueness, bundle identity, source filename, and latest snapshot. The digest must match Task 1's LF-delimited `source_row_id_sequence_sha256`:

```sql
lower(to_hex(sha256(to_utf8(
    array_join(array_agg(source_row_id ORDER BY source_row_number), chr(10)) || chr(10)
)))) AS source_row_id_sequence_sha256
```

Require `row_count = manifest row_count = count(distinct source_row_id)`, `min(source_row_number)=1`, `max(source_row_number)=row_count`, one bundle value, one source filename, zero duplicate strong/composite keys, and a positive decimal-string Snapshot ID. Write the report atomically and never overwrite an existing contradictory report.

- [x] **Step 6: Run tests and confirm GREEN**

Run the command from Step 2.

Expected: all source-ingestion contract tests PASS without requiring Docker.

- [x] **Step 7: Commit**

```powershell
git add generators/olist_data/flink_sql.py generators/olist_data/__main__.py jobs/sql/19_olist_source_ingest.sql.template jobs/sql/20_olist_source_verify.sql.template scripts/run_g2e_olist_source.ps1 scripts/verify_g2e_olist_source.ps1 infra/.env.example infra/docker-compose.yml tests/test_g2e_olist_lakehouse.py
git commit -m "feat: ingest bounded Olist sources into Iceberg"
```

### Task 3: Build Snapshot-Pinned Curated Facts and Dimensions

**Files:**
- Create: `jobs/sql/21_olist_curated_model.sql.template`
- Create: `jobs/sql/22_olist_curated_verify.sql.template`
- Create: `scripts/build_g2e_olist_curated.ps1`
- Create: `scripts/verify_g2e_olist_curated.ps1`
- Create: `tests/test_g2e_olist_curated.py`

**Interfaces:**
- Consumes: nine Task 2 reports for one bundle and the exact positive Snapshot ID map `Mapping[str, str]`, keyed only by the nine registered source table names.
- Produces: nine fixed curated tables, one Snapshot ID per curated table, a hard-gate result, a reportable-quality result, and `tmp/graduation/g2e/$sourceBundleSha256/curated/verification.json`.
- Builder: `scripts/build_g2e_olist_curated.ps1 -ManifestPath $manifestPath`; it discovers and validates source reports rather than accepting caller-supplied table names or SQL.
- Source tokens: `__ORDERS_SRC_SNAPSHOT__`, `__ORDER_ITEMS_SRC_SNAPSHOT__`, `__ORDER_PAYMENTS_SRC_SNAPSHOT__`, `__ORDER_REVIEWS_SRC_SNAPSHOT__`, `__CUSTOMERS_SRC_SNAPSHOT__`, `__PRODUCTS_SRC_SNAPSHOT__`, `__SELLERS_SRC_SNAPSHOT__`, `__GEOLOCATION_SRC_SNAPSHOT__`, and `__CATEGORY_TRANSLATION_SRC_SNAPSHOT__`; curated dependency tokens use the matching singular table name plus `_SNAPSHOT__`.

- [x] **Step 1: Write failing source-gate, grain, anti-fanout, and resume tests**

Create a small synthetic SQL fixture with one order that has two items, two payments, and two reviews. Tests inspect executable query results or pure assertion functions and require:

```text
order_count = 1
item_row_count = 2
payment_row_count = 2
review_row_count = 2
item_value_sum = item1 + item2
payment_value_sum = payment1 + payment2
```

Add rejection tests for duplicate order/customer/product/seller/category keys; duplicate item/payment composite keys; every required FK orphan class; blank required IDs; mixed bundle values; negative amounts; invalid required purchase time; and unparseable optional time. Add report-only tests for duplicate `review_id`, unknown status/payment type, missing category/translation, multiple reviews, optional time absence, lifecycle ordering anomalies, and payment-versus-item-plus-freight mismatch.

Resume tests require an existing curated table to match bundle identity, exact grain count, source Snapshot map, and its recorded latest curated Snapshot; a conflicting table fails without `DROP`, `DELETE`, `TRUNCATE`, or replacement.

- [x] **Step 2: Run the focused test and confirm RED**

Run:

```powershell
$env:TEMP='D:\EcommerceDev\temp'
$env:TMP=$env:TEMP
python -m unittest tests.test_g2e_olist_curated -v
```

Expected: FAIL because curated SQL and scripts do not exist.

- [x] **Step 3: Define exact curated grains and columns**

The template has nine statements in this exact dependency order: `customer_dim`, `category_dim`, `product_dim`, `seller_dim`, `geolocation_dim`, `order_fact`, `order_item_fact`, `payment_fact`, and `review_fact`. Every source reference uses a `FOR VERSION AS OF` token that the renderer replaces only with a validated positive integer:

| Curated table | Grain / key | Required semantic columns |
| --- | --- | --- |
| `order_fact_v1` | `order_id` | customer IDs, status, five timestamps, purchase day/month, temporal flags, bundle |
| `order_item_fact_v1` | `(order_id, order_item_id)` | product, seller, translated category, shipping limit, item/freight decimals, bundle |
| `payment_fact_v1` | `(order_id, payment_sequential)` | type, installments, payment decimal, known-type flag, bundle |
| `review_fact_v1` | `source_row_id` | review ID, order ID, score, creation/answer timestamps, valid-score flag, bundle; no comment text |
| `customer_dim_v1` | `customer_id` | unique customer ID, zip prefix, city, state, bundle |
| `product_dim_v1` | `product_id` | category source/English names, product attributes, unknown flags, bundle |
| `seller_dim_v1` | `seller_id` | zip prefix, city, state, bundle |
| `category_dim_v1` | `product_category_name` | one English name and unknown-translation flag, bundle |
| `geolocation_dim_v1` | zip prefix | average lat/lng, source point count, city/state variant counts, deterministic representative city/state, bundle |

Every curated row also carries `source_bundle_sha256` and `source_snapshot_set_sha256`. The latter is SHA-256 over UTF-8 canonical JSON for the complete source Snapshot object with table names sorted and Snapshot IDs encoded as decimal strings. This compact provenance avoids repeating the full nine-entry object on one million geolocation rows while allowing resume and publication code to prove which exact source set built the table.

For geolocation representatives, rank `(city,state)` by descending source row count then ascending city/state and take rank 1. Coordinates are arithmetic means of valid source coordinates and are labeled aggregate centers, never precise addresses.

`order_item_fact_v1` joins only one-row product/category dimensions. It does not join payments or reviews. Facts retain rows with reportable quality issues and expose flags so dependent metrics can exclude them explicitly.

The item fact follows this restricted join shape; it cannot reference payment or review sources:

```sql
CREATE TABLE lakehouse.olist.order_item_fact_v1 AS
SELECT i.order_id, i.order_item_id, i.product_id, i.seller_id,
       p.product_category_name, p.product_category_name_english,
       i.shipping_limit_date, i.price AS item_value, i.freight_value,
       i.source_bundle_sha256, '__SOURCE_SNAPSHOT_SET_SHA256__' AS source_snapshot_set_sha256
FROM lakehouse.olist.order_items_src_v1 FOR VERSION AS OF __ORDER_ITEMS_SNAPSHOT__ AS i
JOIN lakehouse.olist.product_dim_v1 FOR VERSION AS OF __PRODUCT_DIM_SNAPSHOT__ AS p
  ON i.product_id = p.product_id;
```

The renderer replaces only positive integer Snapshot tokens and the validated 64-hex snapshot-set digest. Production SQL uses `CREATE TABLE IF NOT EXISTS` plus pre/post state checks so interruption can resume without replacing an existing table.

- [x] **Step 4: Implement hard gates before CTAS and reportable quality after CTAS**

Split `22_olist_curated_verify.sql.template` into named single-row statements. `hard_gate` returns every structural count with fixed aliases and must be all zero before the first CTAS. `reportable_quality` returns non-blocking counts. `grain_reconciliation` proves each curated key unique and source-to-curated counts exact. `anti_fanout` separately compares item, payment, and review source sums/counts to their facts. `snapshot_identity` returns sorted source and curated table/Snapshot pairs.

The builder runs the gate first, executes one stage, waits for Trino visibility, verifies that stage, writes a stage report, then proceeds. This preserves low peak memory and makes interruption resumable without destructive cleanup.

```powershell
$gate = Invoke-G2eNamedTrinoResult -Name 'hard_gate' -Sql $verificationSql
Assert-G2eZeroHardGate -Row $gate
foreach ($stage in Get-G2eCuratedStageOrder) {
    if (Test-G2eCuratedStageExact -Stage $stage -Identity $identity) { continue }
    Invoke-G2eCuratedStage -Stage $stage -RenderedSql $rendered[$stage]
    Assert-G2eCuratedStageExact -Stage $stage -Identity $identity
}
```

- [x] **Step 5: Run tests and confirm GREEN**

Run the command from Step 2.

Expected: all curated-model tests PASS; text-contract tests also prove every source table is Snapshot-pinned and forbidden destructive SQL is absent.

- [x] **Step 6: Commit**

```powershell
git add jobs/sql/21_olist_curated_model.sql.template jobs/sql/22_olist_curated_verify.sql.template scripts/build_g2e_olist_curated.ps1 scripts/verify_g2e_olist_curated.ps1 tests/test_g2e_olist_curated.py
git commit -m "feat: build anti-fanout Olist curated facts"
```

### Task 4: Define Versioned Metrics, Trino Results, Doris Tables, and Pure Validators

**Files:**
- Create: `configs/metrics/orders-v1.json`
- Create: `jobs/sql/23_g2e_order_metrics.sql.template`
- Create: `infra/compose/doris/init/03_create_order_metrics.sql`
- Create: `scripts/lib/G2e.OrderMetrics.psm1`
- Create: `tests/test_g2e_order_metrics.py`

**Interfaces:**
- Consumes: the exact nine curated table names and positive Snapshot IDs from Task 3, plus the Task 1 manifest and Task 3 quality report.
- Produces: six ordered metric result sets named `overview`, `delivery`, `payment`, `ranking`, `review`, and `quality`; seven Doris tables; `Get-G2eMetricIdentity`, `Split-G2eNamedSql`, `ConvertFrom-G2eCsv`, `Merge-G2eQualityEvidence`, `Assert-G2eMetricBundle`, `Get-G2eCanonicalDigest`, and `Export-G2eCandidateCsv`.
- Metric identity object: `MetricRunId`, `DatasetId`, `MetricVersion`, `SourceBundleSha256`, sorted `SourceSnapshots`, sorted `CuratedSnapshots`, `SourceOrderCount`, `WindowStart`, `WindowEnd`, `CalculatedAt`, and `ImplementationRevision`.
- Curated tokens: `__ORDER_FACT_SNAPSHOT__`, `__ORDER_ITEM_FACT_SNAPSHOT__`, `__PAYMENT_FACT_SNAPSHOT__`, `__REVIEW_FACT_SNAPSHOT__`, `__CUSTOMER_DIM_SNAPSHOT__`, `__PRODUCT_DIM_SNAPSHOT__`, `__SELLER_DIM_SNAPSHOT__`, `__CATEGORY_DIM_SNAPSHOT__`, and `__GEOLOCATION_DIM_SNAPSHOT__`.

- [x] **Step 1: Write failing catalog, SQL, DDL, and pure-function tests**

Pin the catalog to this exact public set:

```python
REQUIRED_ORDER_METRICS = {
    "order_count", "delivered_order_count", "canceled_order_count",
    "unavailable_order_count", "status_eligible_order_count", "status_excluded_order_count",
    "delivered_rate", "canceled_rate", "unique_customer_count", "repeat_customer_count",
    "repeat_customer_rate", "item_value_sum", "freight_value_sum",
    "payment_value_sum", "items_per_order_avg", "delivery_eligible_order_count",
    "delivery_days_avg", "delivery_days_p50", "delivery_days_p90",
    "late_delivery_order_count", "late_delivery_rate", "payment_row_count",
    "payment_order_count", "installment_order_count", "payment_type_order_count",
    "payment_type_value_sum", "ranking_order_count", "ranking_item_row_count",
    "ranking_customer_count", "ranking_item_value_sum", "ranking_freight_value_sum",
    "ranking_payment_value_sum", "ranking_late_delivery_order_count",
    "ranking_late_delivery_rate", "review_row_count", "reviewed_order_count",
    "review_coverage_rate", "review_score_avg", "low_score_order_count",
    "low_score_rate", "multi_review_order_count", "source_row_count",
    "iceberg_row_count", "duplicate_key_count", "orphan_key_count",
    "invalid_value_count", "temporal_anomaly_count", "amount_comparable_order_count",
    "amount_reconciled_order_count", "amount_mismatch_order_count",
    "amount_reconciliation_rate", "payment_item_freight_abs_difference_avg",
    "payment_item_freight_abs_difference_p50", "payment_item_freight_abs_difference_p90"
}
```

Every definition must include `metric_name`, Chinese `display_name`, formula, numerator, denominator, source fields, allowed windows, additive flag, null policy, exclusions, limitations, and forbidden claims. Amount definitions forbid at least `利润`, `净收入`, `审计 GMV`, `退款后收入`, and currency conversion claims. Seller-state payment definitions explicitly say groups are non-additive.

Artifact tests require seven non-destructive Doris tables, `VARCHAR(128)` run IDs, six family row-count/hash pairs in the publication table, no caller-controlled source identifier, exactly six result markers, and every curated source reference pinned by a numeric Snapshot token.

Pure-function fixtures exercise decimal canonicalization, LF line endings, column order, JSON map key order, duplicate candidate keys, null handling, booleans, dates, and digest stability. Include synthetic business tests:

```python
def test_two_payments_do_not_multiply_two_items(self):
    self.assertEqual("30.00", metrics["item_value_sum"])
    self.assertEqual("35.00", metrics["payment_value_sum"])

def test_order_level_review_average_gives_each_order_one_vote(self):
    # Order A scores 1 and 5 => 3; order B scores 5 => 5; final average => 4.
    self.assertEqual("4.000000", metrics["review_score_avg"])
```

Also pin a multi-payment order into two payment-type groups while retaining one global order count; prove grouped order counts are not summed as a total. Pin one order touching two seller states and prove each state may show the whole order payment value while the catalog and API mark seller-state payment totals non-additive.

- [x] **Step 2: Run the focused test and confirm RED**

Run:

```powershell
$env:TEMP='D:\EcommerceDev\temp'
$env:TMP=$env:TEMP
python -m unittest tests.test_g2e_order_metrics -v
```

Expected: FAIL because the catalog, SQL, DDL, and module do not exist.

- [x] **Step 3: Create the complete `orders-v1` definition catalog**

The document identity is fixed:

```json
{
  "domain": "orders",
  "dataset_id": "olist-brazilian-ecommerce-v2",
  "metric_version": "orders-v1",
  "definitions": []
}
```

Use `DAY`, `MONTH`, and `FULL` only. Status-rate denominator `status_eligible_order_count` contains orders with one of the eight known statuses; unknown nonblank statuses remain in `order_count` but enter `status_excluded_order_count`. Delivered and canceled rates expose their count numerator and this denominator. Define `repeat_customer_rate` as unique customers with at least two orders in that independently computed window divided by unique customers with at least one order. Define `items_per_order_avg` from order-item row count and label it a row-count proxy because Olist has no quantity column. Define `delivery_days_*` only for delivered orders with usable purchase/delivery times. Define late rate as late eligible delivered orders divided by all late-eligible delivered orders. Define low score from the per-order average `<= 2`, followed by one-vote-per-reviewed-order aggregation.

Window bounds are deterministic and remain inside the observed publication range: DAY uses the purchase date for both bounds; MONTH groups by calendar month but uses that group's minimum and maximum observed purchase dates; FULL uses the global minimum and maximum observed purchase dates. This makes first/last partial months explicit instead of pretending they cover unobserved calendar days.

- [x] **Step 4: Create six snapshot-pinned Trino metric statements**

Every statement independently builds DAY, MONTH, and FULL windows; FULL is recomputed from facts, not summed from smaller windows. Use three one-row-per-order CTEs before overview joins:

```sql
item_per_order AS (
  SELECT order_id, count(*) AS item_row_count,
         sum(item_value) AS item_value_sum, sum(freight_value) AS freight_value_sum
  FROM lakehouse.olist.order_item_fact_v1 FOR VERSION AS OF __ORDER_ITEM_FACT_SNAPSHOT__
  GROUP BY order_id
),
payment_per_order AS (
  SELECT order_id, count(*) AS payment_row_count,
         sum(payment_value) AS payment_value_sum
  FROM lakehouse.olist.payment_fact_v1 FOR VERSION AS OF __PAYMENT_FACT_SNAPSHOT__
  GROUP BY order_id
),
review_per_order AS (
  SELECT order_id, avg(CAST(review_score AS DECIMAL(18,6))) AS order_review_score,
         count(*) AS valid_review_row_count
  FROM lakehouse.olist.review_fact_v1 FOR VERSION AS OF __REVIEW_FACT_SNAPSHOT__
  WHERE valid_review_score
  GROUP BY order_id
)
```

Metric-family contracts are exact:

| Family | Grain and required behavior |
| --- | --- |
| `overview` | one row per window; status numerator/eligible/excluded counts and rates, repeat customers, independent item/freight/payment sums, item rows per order |
| `delivery` | one row per window; eligible/excluded counts, average and approximate p50/p90 days, late numerator/denominator/exclusions |
| `payment` | one `__ALL__` row plus one row per source payment type/window; each row includes global order count; installment means `payment_installments > 1` |
| `ranking` | product/category/seller/customer_state/seller_state rows; stable dimension ID; unknown groups retained; allowed measures nullable when semantically inapplicable |
| `review` | one row per window; valid review rows, reviewed orders, all-order denominator, coverage, multi-review orders, order-weighted score, low-score numerator/rate |
| `quality` | one FULL SQL row containing duplicate/orphan/invalid/temporal counts, fact counts, exact amount-comparable/reconciled/mismatch counts, absolute payment-versus-item-plus-freight difference average/approximate p50/p90, and `PASS`; the pure module adds manifest/source-report evidence before export |

Product/category/seller ranks derive only from item facts. Customer-state payment sums use one order/customer-state row. Seller-state payment values first deduplicate `(order_id, seller_state)`, then attach the whole order payment total; they are deliberately non-additive across states. State late rates deduplicate `(order_id, state)` before numerator and denominator counts. Amount reconciliation compares only orders having both an item aggregate and a payment aggregate; equality is exact at two decimals, and the reported distribution uses the absolute signed-difference magnitude without hiding unmatched orders.

- [x] **Step 5: Define immutable Doris contracts**

Create these tables with one local bucket and no destructive statement:

```text
order_metric_publications
order_metric_overview
order_metric_delivery
order_metric_payment
order_metric_ranking
order_metric_review
order_metric_quality
```

`order_metric_publications` uses `UNIQUE KEY(metric_run_id)` and stores dataset/version, full bundle, canonical source/curated Snapshot JSON, implementation revision, source order count, window, calculated/published timestamps, each of six candidate row counts and SHA-256 values, and `status`.

Family keys are:

```text
overview: (metric_run_id, window_type, window_start)
delivery: (metric_run_id, window_type, window_start)
payment:  (metric_run_id, window_type, window_start, payment_type)
ranking:  (metric_run_id, window_type, window_start, dimension_type, dimension_id)
review:   (metric_run_id, window_type, window_start)
quality:  (metric_run_id)
```

Use `DECIMAL(38,2)` for money and `DECIMAL(18,6)` for rates/averages. Use nullable money/rate columns only where a denominator or metric is semantically unavailable. Store canonical quality/Snapshot JSON as bounded `VARCHAR` and validate it before and after Doris load.

- [x] **Step 6: Implement pure PowerShell reconciliation and canonical digest functions**

`Get-G2eMetricIdentity` accepts already parsed manifest/reports and rejects unknown/missing Snapshot keys, nonpositive IDs, a bundle mismatch, an empty order range, source-order count mismatch, a dirty/invalid implementation revision, or a run ID other than:

```powershell
$expectedRunId = "orders-v1-b$($Manifest.source_bundle_sha256)"
```

`Merge-G2eQualityEvidence` accepts exactly one SQL quality row, the formal manifest, nine source reports, and the curated report. It adds canonical sorted JSON maps for raw/normalized/Iceberg row counts, normalized file SHA-256 values, source/curated Snapshot IDs, and fact reconciliations; every overlapping count must agree before it returns one final quality candidate.

`Assert-G2eMetricBundle` enforces unique family keys, exact identity on every row, DAY/MONTH/FULL reconciliation, FULL source order count, nonnegative values, rates matching numerator/denominator to six decimals, valid null cases, stable ranking order inputs, the merged reportable quality maps, and `PASS`. `Export-G2eCandidateCsv` writes a fixed header and canonical rows atomically; `Get-G2eCanonicalDigest` hashes those exact UTF-8/LF bytes.

- [x] **Step 7: Run tests and confirm GREEN**

Run the command from Step 2.

Expected: all metric contract and pure-function tests PASS.

- [x] **Step 8: Commit**

```powershell
git add configs/metrics/orders-v1.json jobs/sql/23_g2e_order_metrics.sql.template infra/compose/doris/init/03_create_order_metrics.sql scripts/lib/G2e.OrderMetrics.psm1 tests/test_g2e_order_metrics.py
git commit -m "feat: define orders-v1 metric contracts"
```

### Task 5: Publish Metric Candidates to Doris Fail-Closed

**Files:**
- Create: `scripts/refresh_g2e_order_metrics.ps1`
- Modify: `tests/test_g2e_order_metrics.py`

**Interfaces:**
- Consumes: one formal manifest, exact verified source/curated reports, fixed SQL/DDL, current clean Git revision, and existing platform credentials from `infra/.env`.
- Produces: `Get-G2eRefreshPlan`, `Enter-G2eRunLock`, `Invoke-G2ePublicationSequence`, `New-G2ePublicationRecord`, `Invoke-G2eRefresh`; six candidate CSVs; a report at `tmp/graduation/g2e/$sourceBundleSha256/metrics/refresh.json`; and either `published` or `already_published`.
- Entrypoint: `scripts/refresh_g2e_order_metrics.ps1 -ManifestPath $manifestPath`; table names, SQL, run IDs, and Snapshot IDs are discovered from verified evidence rather than accepted as free-form arguments.

- [x] **Step 1: Write failing publication-order, retry, path, and readback tests**

Dot-source with `-FunctionsOnly` and inject callbacks into `Invoke-G2ePublicationSequence`. Pin this order:

```text
overview -> delivery -> payment -> ranking -> review -> quality -> publication
```

For each candidate stage, simulate a load failure, row-count mismatch, digest mismatch, identity mismatch, or readback exception and assert the publication callback is never invoked. Assert a successful run invokes publication exactly once and last.

```python
self.assertEqual(
    ["overview", "delivery", "payment", "ranking", "review", "quality", "publication"],
    invoke_sequence(successful_callbacks),
)
self.assertNotIn("publication", invoke_sequence(callbacks_failing_at("ranking")))
```

Retry tests cover:

| Existing state | Expected result |
| --- | --- |
| no publication | calculate, validate, load, read back, publish |
| exact publication and six exact families | `already_published`; no Stream Load and no insert |
| publication identity matches but any hash/count/Snapshot/range differs | fail closed; a different current Git revision does not rewrite the existing publication |
| candidate rows exist without a publication | validate exact candidate rows and continue only when all expected hashes match |
| another published run exists | preserve it and publish the new immutable run |

Path tests reject C-drive output, symlink/junction escape, a report outside fixed `tmp/graduation/g2e/$sourceBundleSha256/metrics`, existing conflicting files, and unresolved SQL tokens. A held run lock rejects a concurrent refresh before either process queries or loads candidates.

- [x] **Step 2: Run the refresh tests and confirm RED**

Run:

```powershell
$env:TEMP='D:\EcommerceDev\temp'
$env:TMP=$env:TEMP
python -m unittest tests.test_g2e_order_metrics.G2eRefreshTests -v
```

Expected: FAIL because `refresh_g2e_order_metrics.ps1` does not exist.

- [x] **Step 3: Implement dependency, identity, and SQL execution gates**

Reuse the repository's process-local Docker fallback `D:\DockerProgram\Docker\resources\bin`; never persist a PATH change. Acquire an exclusive create-new file handle for this full run ID and hold it through final readback so two local refreshes cannot race. Start only MinIO/Metastore/Trino and Doris for refresh. Require a clean tracked implementation state and capture `git rev-parse HEAD` as a 40-hex revision. Query every source and curated latest Snapshot, compare it to verified reports, then render only numeric `FOR VERSION AS OF` tokens in the fixed metric template.

```powershell
function Enter-G2eRunLock {
    param([string]$Path)
    $parent = Split-Path -Parent $Path
    [System.IO.Directory]::CreateDirectory($parent) | Out-Null
    return [System.IO.File]::Open($Path, 'CreateNew', 'Write', 'None')
}

if ($snapshotId -notmatch '^[1-9][0-9]*$') { throw 'Invalid Olist Snapshot ID.' }
$rendered = $rendered.Replace('__ORDER_FACT_SNAPSHOT__', $snapshotId)
```

Split named SQL deterministically and execute each statement through the Trino CLI `CSV_HEADER_UNQUOTED` format. Parse with fixed columns, call `Assert-G2eMetricBundle`, and export each family into the run directory before touching Doris.

- [x] **Step 4: Implement candidate load and publication-last readback**

Run the fixed DDL, assert all seven table schemas, and Stream Load six candidate files with strict JSON/CSV options and zero tolerated filtering. After each load, query the rows back by parameter-escaped fixed run ID, canonicalize with the same module, and require exact row count and digest.

Construct the publication row only from verified values. Insert it with a fixed column list after all six readbacks. Immediately read it back, compare every field, and write the refresh report atomically. Never issue `UPDATE`, `DELETE`, `DROP`, `TRUNCATE`, or table replacement.

```powershell
foreach ($family in @('overview', 'delivery', 'payment', 'ranking', 'review', 'quality')) {
    Invoke-G2eCandidateLoad -Family $family -Candidate $candidates[$family]
    Assert-G2eCandidateReadback -Family $family -Expected $candidates[$family]
}
Publish-G2eMetadata -Record (New-G2ePublicationRecord -Identity $identity -Candidates $candidates)
Assert-G2ePublicationReadback -Expected $identity
```

- [x] **Step 5: Run focused and neighboring regression tests**

Run:

```powershell
$env:TEMP='D:\EcommerceDev\temp'
$env:TMP=$env:TEMP
python -m unittest tests.test_g2e_order_metrics tests.test_g2d_behavior_metrics -v
```

Expected: G2-E publication tests and unchanged G2-D behavior publication tests PASS.

- [x] **Step 6: Commit**

```powershell
git add scripts/refresh_g2e_order_metrics.ps1 tests/test_g2e_order_metrics.py
git commit -m "feat: publish immutable Olist order metrics"
```

### Task 6: Add Strict Order Models, Definition Loading, and Dependency Wiring

**Files:**
- Create: `services/api/app/order_models.py`
- Create: `services/api/app/order_metric_definitions.py`
- Modify: `services/api/app/config.py`
- Modify: `services/api/app/dependencies.py`
- Modify: `infra/.env.example`
- Modify: `infra/docker-compose.yml`
- Create: `tests/test_order_metrics_api.py`

**Interfaces:**
- Consumes: `configs/metrics/orders-v1.json` and existing `ApiSettings` Doris configuration.
- Produces: `OrderMetricMeta`, six point/quality models, seven response models, `OrderMetricDefinition`, `OrderMetricDefinitionsResponse`, `OrderMetricDefinitionCatalog.load(path)`, `ApiSettings.order_metric_definitions_path`, and `build_order_metrics_service(settings)`.

- [x] **Step 1: Write failing strict-model, catalog, settings, and factory tests**

Require `extra="forbid"` on every model and pin the metadata contract:

```python
class OrderMetricMeta(StrictOrderModel):
    dataset_id: Literal["olist-brazilian-ecommerce-v2"]
    metric_version: Literal["orders-v1"]
    metric_run_id: str = Field(pattern=r"^orders-v1-b[0-9a-f]{64}$")
    source_bundle_sha256: Sha256String
    source_snapshots: dict[str, PositiveSnapshotId]
    curated_snapshots: dict[str, PositiveSnapshotId]
    window_start: date
    window_end: date
    source_timezone: Literal["unspecified"]
    source_currency: NonBlankString | None
    source_order_count: int = Field(gt=0)
    calculated_at: datetime
    implementation_revision: str = Field(pattern=r"^[0-9a-f]{40}$")
    warnings: list[NonBlankString]
```

Test full run ID length, lowercase digest, decimal Snapshot strings, exact source/curated key sets, null currency with a warning, money as fixed decimal strings, rates with six decimals, nonnegative counts, supported literals, publication hashes, and quality JSON maps with nonnegative integer values. Reject booleans as integer counts.

Catalog tests require exact identity and the Task 4 metric set, no duplicate names, valid source fields/windows, nonempty limitations, exact forbidden monetary claims, and seller-state non-additivity text. Settings tests require a nonblank `ORDER_METRIC_DEFINITIONS_PATH`; factory tests inject a temporary catalog and patched repository without making a network connection.

- [x] **Step 2: Run the focused API-contract tests and confirm RED**

Run:

```powershell
$env:TEMP='D:\EcommerceDev\temp'
$env:TMP=$env:TEMP
$env:PYTHONPATH='services/api'
python -m unittest tests.test_order_metrics_api.OrderModelAndCatalogTests -v
```

Expected: FAIL because the order model/catalog modules do not exist.

- [x] **Step 3: Implement exact typed responses**

Create these response shapes:

```text
OrderPublicationResponse(meta, data)
OrderOverviewResponse(meta, data: list[OrderOverviewPoint])
OrderDeliveryResponse(meta, data: list[OrderDeliveryPoint])
OrderPaymentsResponse(meta, data: list[OrderPaymentPoint])
OrderRankingsResponse(meta, data: list[OrderRankingPoint])
OrderReviewsResponse(meta, data: list[OrderReviewPoint])
OrderQualityResponse(meta, data: OrderQualityMetrics)
```

Point fields exactly mirror Task 4 Doris columns. Payment points include `payment_type`, `is_all`, `global_order_count`, group order count, row count, installment order count, and value. Ranking points include nullable item/payment/late measures plus `payment_value_is_additive: bool | None`; customer-state rows set it true, seller-state rows set it false, and item dimensions set it null. Delivery and review points expose numerator, denominator, and excluded counts in addition to rates. Publication data exposes six family row-count/hash pairs and `PUBLISHED`.

```python
class OrderPaymentPoint(StrictOrderModel):
    window_type: Literal["DAY", "MONTH", "FULL"]
    window_start: date
    window_end: date
    payment_type: NonBlankString
    is_all: bool
    global_order_count: int = Field(ge=0)
    payment_order_count: int = Field(ge=0)
    payment_row_count: int = Field(ge=0)
    installment_order_count: int = Field(ge=0)
    payment_value_sum: MoneyString
```

- [x] **Step 4: Implement the independent definition catalog and settings wiring**

Do not broaden the existing behavior-only `MetricDefinitionCatalog`. `OrderMetricDefinitionCatalog` owns the order source-field allowlist and exact required set. Resolve the default `configs/metrics/orders-v1.json` using the same repository/container search strategy as behavior definitions.

Add:

```python
order_metric_definitions_path: Path = _DEFAULT_ORDER_METRIC_DEFINITIONS_PATH
```

Load `ORDER_METRIC_DEFINITIONS_PATH` in `load_settings`, validate it in `__post_init__`, expose `/app/configs/metrics/orders-v1.json` in `.env.example`, and pass it to the API container. `build_order_metrics_service` loads the order catalog and creates `OrderMetricsRepository.from_settings(settings)`.

- [x] **Step 5: Run tests and confirm GREEN**

Run the command from Step 2 plus:

```powershell
$env:PYTHONPATH='services/api'
python -m unittest tests.test_behavior_metrics_api tests.test_api_service -v
```

Expected: new order model/catalog tests and unchanged behavior/core API tests PASS.

- [x] **Step 6: Commit**

```powershell
git add services/api/app/order_models.py services/api/app/order_metric_definitions.py services/api/app/config.py services/api/app/dependencies.py infra/.env.example infra/docker-compose.yml tests/test_order_metrics_api.py
git commit -m "feat: add typed orders-v1 API contracts"
```

### Task 7: Implement the Doris Repository, Service Validation, and Eight Routes

**Files:**
- Create: `services/api/app/order_repository.py`
- Create: `services/api/app/order_service.py`
- Modify: `services/api/app/main.py`
- Modify: `tests/test_order_metrics_api.py`

**Interfaces:**
- Consumes: Task 6 models/catalog, Task 5 published tables, and existing PyMySQL settings.
- Produces: `OrderMetricsRepository`, `OrderMetricsService`, `OrderMetricsUnavailableError`, seven `/api/v1/orders/*` endpoints, and dual-domain `/api/v1/metrics/definitions` dispatch.
- Service signatures:

```python
get_publication() -> OrderPublicationResponse
get_overview(window: str, start_date: date | None, end_date: date | None) -> OrderOverviewResponse
get_delivery(window: str, start_date: date | None, end_date: date | None) -> OrderDeliveryResponse
get_payments(window: str, start_date: date | None, end_date: date | None) -> OrderPaymentsResponse
get_rankings(dimension: str, window: str, start_date: date | None, end_date: date | None, sort_by: str, limit: int) -> OrderRankingsResponse
get_reviews(window: str, start_date: date | None, end_date: date | None) -> OrderReviewsResponse
get_quality() -> OrderQualityResponse
get_definitions() -> OrderMetricDefinitionsResponse
```

Repository integrity interface: `fetch_family_row_count(metric_run_id: str, family: Literal["overview", "delivery", "payment", "ranking", "review", "quality"]) -> int`; the family selects one fixed query from a constant map and never becomes a raw table name.

- [x] **Step 1: Write failing repository, service, and route tests**

Use fake DB connections to assert exact fixed query text and parameter tuples. Repository tests reject unknown windows/dimensions/sorts, strings/booleans for limit, unpaired dates, date filters on FULL, reversed ranges, and SQL-like values. Require only these maps:

```python
WINDOWS = {"day": "DAY", "month": "MONTH", "full": "FULL"}
ITEM_SORTS = {"order_count", "item_value", "freight_value"}
STATE_SORTS = {"order_count", "payment_value", "late_rate"}
DIMENSIONS = {"product", "category", "seller", "customer_state", "seller_state"}
```

Service tests reject no publication; wrong status/dataset/version/run digest; noncanonical or incomplete Snapshot JSON; bundle/run mismatch; zero source orders; reversed publication window; family row identity mismatch; quality not `PASS`; publication/metric row-count mismatch; out-of-range request dates; malformed decimals; and unexpected database values. Every such repository/data failure becomes `OrderMetricsUnavailableError` without leaking raw SQL or row content.

Route tests call all eight contracts with injected fake services, verify exact response models, and require:

```text
422: malformed dates, unpaired dates, FULL with dates, limit outside 1..100,
     unsupported dimension/sort pairing, behavior/orders definition cross-pair
503: missing/corrupt publication, metric identity mismatch, invalid definition catalog
```

Also assert existing behavior definitions and all existing `/metrics/*`, `/analysis/realtime`, and `/analysis/tools` tests remain unchanged.

- [x] **Step 2: Run the focused tests and confirm RED**

Run:

```powershell
$env:TEMP='D:\EcommerceDev\temp'
$env:TMP=$env:TEMP
$env:PYTHONPATH='services/api'
python -m unittest tests.test_order_metrics_api -v
```

Expected: FAIL because repository, service, and routes do not exist.

- [x] **Step 3: Implement parameterized fixed-table repository methods**

Use one private `_window_and_range()` validator. Date-filtered queries use `window_start >= %s AND window_end <= %s`; FULL uses no date predicate. Sort expressions are selected only from nested constant maps keyed by validated dimension and public sort name:

```python
RANK_SORT_COLUMNS = {
    "product": {"order_count": "order_count", "item_value": "item_value_sum", "freight_value": "freight_value_sum"},
    "category": {"order_count": "order_count", "item_value": "item_value_sum", "freight_value": "freight_value_sum"},
    "seller": {"order_count": "order_count", "item_value": "item_value_sum", "freight_value": "freight_value_sum"},
    "customer_state": {"order_count": "order_count", "payment_value": "payment_value_sum", "late_rate": "late_delivery_rate"},
    "seller_state": {"order_count": "order_count", "payment_value": "payment_value_sum", "late_rate": "late_delivery_rate"},
}
```

Ordering uses the selected constant column descending, followed by `dimension_id ASC, window_start ASC`; for example, item-value sorting becomes `item_value_sum DESC, dimension_id ASC, window_start ASC`. All values remain DB parameters. `fetch_latest_publication()` reads only `status='PUBLISHED'` and orders by publication time/run ID. Quality reads one FULL row. `fetch_family_row_count()` selects among six complete fixed `COUNT(*)` queries so limited/ranged responses can still prove the published family has not lost or gained rows. No method accepts a table, column, SQL fragment, or raw order expression.

- [x] **Step 4: Implement fail-closed service validation**

`_load_publication()` derives the expected run ID from `source_bundle_sha256`, parses canonical sorted Snapshot objects, requires the exact nine source and nine curated names, verifies every positive decimal-string ID, and builds metadata. The source-order count must equal the `orders` manifest/source count represented by quality and overview FULL.

Validate requested dates before repository access: both absent or both present; present dates require DAY/MONTH, `start <= end`, and inclusion in the publication window. Convert Doris system timestamps to UTC-aware datetimes, but leave source business time semantics represented only by `source_timezone="unspecified"`. Until verified documentation supplies currency, set `source_currency=None` and include a Chinese warning that no symbol or conversion is applied.

```python
def _validate_range(window, start_date, end_date, publication_start, publication_end):
    if (start_date is None) != (end_date is None):
        raise ValueError("start_date and end_date must be provided together")
    if window == "full" and start_date is not None:
        raise ValueError("full window does not accept a date range")
    if start_date is not None and not (publication_start <= start_date <= end_date <= publication_end):
        raise ValueError("date range is outside the publication window")
    return start_date, end_date
```

Each response validates row identity, requested family/window/range, row counts against publication metadata, stable ordering, denominator/rate consistency, and allowed nulls. Parse quality JSON with exact expected map keys and nonnegative integer values.

- [x] **Step 5: Add routes without breaking behavior definitions**

Add an `order_response()` wrapper that logs only stage and exception type and returns generic `503`. Add:

```text
GET /api/v1/orders/publication
GET /api/v1/orders/overview
GET /api/v1/orders/delivery
GET /api/v1/orders/payments
GET /api/v1/orders/rankings
GET /api/v1/orders/reviews
GET /api/v1/orders/quality
```

Change the existing definitions route to accept only the two valid pairs. A behavior request calls the existing behavior service and returns its existing model; an order request calls the order service. A crossed pair receives 422 before either service is called. Inject `order_service` as an optional keyword in `create_app` so existing test construction stays compatible.

```python
@app.get("/api/v1/orders/overview", response_model=OrderOverviewResponse)
def get_order_overview(
    window: Literal["day", "month", "full"] = "full",
    start_date: date | None = None,
    end_date: date | None = None,
) -> OrderOverviewResponse:
    return order_response(
        "order_overview",
        lambda: order_service.get_overview(window, start_date, end_date),
    )
```

- [x] **Step 6: Run focused and full API regressions**

Run:

```powershell
$env:PYTHONPATH='services/api'
python -m unittest tests.test_order_metrics_api tests.test_behavior_metrics_api tests.test_api_service tests.test_analysis_api tests.test_tool_analysis_api -v
```

Expected: all listed suites PASS.

- [x] **Step 7: Commit**

```powershell
git add services/api/app/order_repository.py services/api/app/order_service.py services/api/app/main.py tests/test_order_metrics_api.py
git commit -m "feat: expose immutable Olist order metrics API"
```

### Task 8: Run Real-Data Acceptance, Document Measured Evidence, Review, and Back Up

**Files:**
- Create: `scripts/verify_g2e_order_domain.ps1`
- Create: `tests/test_g2e_order_domain.py`
- Create: `docs/graduation/olist-order-domain-runbook.md`
- Modify: `README.md`
- Modify: `docs/graduation/data-readiness.md`
- Modify: `docs/superpowers/plans/2026-09-21-graduation-olist-order-domain.md`

**Interfaces:**
- Consumes: the official locally downloaded archive, all Tasks 1-7 artifacts, running Docker Desktop, and the same formal bundle identity throughout.
- Produces: one immutable `tmp/graduation/g2e/$sourceBundleSha256/acceptance.json`, measured documentation, completed plan checkboxes/record, full regression evidence, whole-branch review, and a verified remote branch backup.
- Verifier: `scripts/verify_g2e_order_domain.ps1 -ManifestPath $manifestPath -ApiBaseUrl http://localhost:8000`.

- [x] **Step 1: Write failing verifier contract tests**

Dot-source the verifier with `-FunctionsOnly`. Test `Assert-G2eAcceptanceIdentity`, `Assert-G2eApiMeta`, `Assert-G2eApiRows`, and `Assert-G2eAcceptanceEvidence` against synthetic evidence. Require exact agreement across manifest, nine source reports, nine curated Snapshot entries, refresh report, Doris publication, six family digests, and all eight API responses.

Negative fixtures alter one field at a time: bundle, run ID, source or curated Snapshot, source order count, window, implementation revision, row count, digest, quality status, source timezone/currency warning, endpoint list, and response metadata. Test report-path containment and no-overwrite behavior. Artifact tests prohibit destructive SQL and any committed real row, token, cookie, or absolute user-profile path.

```python
def test_acceptance_rejects_one_changed_curated_snapshot(self):
    evidence = valid_synthetic_acceptance()
    evidence["api"]["overview"]["meta"]["curated_snapshots"]["order_fact_v1"] = "999"
    with self.assertRaisesRegex(RuntimeError, "curated snapshot"):
        assert_acceptance_evidence(evidence)
```

- [x] **Step 2: Run all G2-E offline tests and confirm GREEN before real data**

Run:

```powershell
$env:TEMP='D:\EcommerceDev\temp'
$env:TMP=$env:TEMP
$env:PYTHONPATH='services/api'
python -m unittest tests.test_olist_data tests.test_olist_data_cli tests.test_g2e_olist_lakehouse tests.test_g2e_olist_curated tests.test_g2e_order_metrics tests.test_order_metrics_api tests.test_g2e_order_domain -v
```

Expected: all G2-E offline tests PASS. If Kaggle authentication is not already valid, pause only at the browser/account boundary so the user can log in; do not request or handle their password, token, or cookie.

- [x] **Step 3: Prepare the official bundle and record only measured provenance**

Download from `https://www.kaggle.com/olistbr/brazilian-ecommerce/home`, verify the current page metadata/license, and run Task 1 against `D:\EcommerceData\olist`. Record the actual archive/file hashes, bytes, logical rows, headers, normalized hashes, bundle identity, acquisition time, and elapsed time in the ignored manifest/report. Do not copy raw paths containing the Windows username into committed docs.

- [x] **Step 4: Ingest and verify all nine source tables sequentially**

For each registry entity in filename order, run Task 2's runner and verifier, wait for `FINISHED`, then stop before the next entity. Require exact source/normalized/Iceberg row counts, distinct row IDs, ordered row-ID digest, bundle identity, business-key uniqueness, and positive Snapshot ID. Capture peak container memory where available; do not run frontend builds or a local language model concurrently.

- [x] **Step 5: Build curated tables, publish metrics, and start the API**

Run Task 3 builder/verifier, then Task 5 refresh. Start only the serving profile needed for the API after metric publication. Require hard-gate zeros, separately reported quality counts, exact curated grains, anti-fanout sums, six candidate readbacks, publication last, and one full run identity.

- [x] **Step 6: Verify Trino, Doris, and all eight API contracts end to end**

Call:

```text
/api/v1/orders/publication
/api/v1/orders/overview?window=full
/api/v1/orders/delivery?window=full
/api/v1/orders/payments?window=full
/api/v1/orders/rankings?dimension=category&window=full&sort_by=item_value&limit=20
/api/v1/orders/reviews?window=full
/api/v1/orders/quality
/api/v1/metrics/definitions?domain=orders&version=orders-v1
```

Then exercise one measured DAY range and one MONTH range from inside the publication window. The verifier independently queries fixed Trino facts and Doris candidates, recomputes response expectations, verifies row counts/digests and metadata, confirms invalid parameter requests return 422, and confirms an unavailable fake publication path returns 503 in offline tests rather than mutating live data.

- [x] **Step 7: Write only measured documentation and an honest status**

The runbook records reproducible commands, table grains, metric definitions, identities, actual counts, nonzero reportable quality findings, durations, observed resource peaks, restart/resume behavior, and safe failure recovery. README and data-readiness may say G2-E is dynamically accepted only if Step 6 passes. If Docker or data acquisition blocks dynamic execution, state exactly “代码与离线契约完成，真实动态验收未完成” and preserve the remaining commands; never insert expected-looking numbers.

- [x] **Step 8: Run repository-wide verification**

Run:

```powershell
$env:TEMP='D:\EcommerceDev\temp'
$env:TMP=$env:TEMP
$env:PYTHONPATH='services/api'
python -m unittest discover -s tests -v
docker compose --env-file infra/.env.example -f infra/docker-compose.yml config --quiet
git diff --check
git status --short
```

Expected: every Python test passes, Compose validates, diff check is clean, and no raw/normalized Olist data, runtime report, credential, or unrelated workspace file is staged. Run the existing Java DataStream Maven tests only if a Java/DataStream file changed; otherwise rely on the unchanged full Python/API/artifact regression plus Compose validation.

- [x] **Step 9: Request one whole-branch review and resolve findings**

Use `superpowers:requesting-code-review` against the merge base through current HEAD. Review priority is data authenticity, key/FK gates, fanout, Snapshot identity, immutable publication, SQL injection, API semantics, secrets, path containment, and destructive operations. Fix every confirmed finding, rerun the affected focused suite, then rerun Step 8 before claiming completion.

- [x] **Step 10: Commit evidence and verify GitHub backup**

Update this plan's checkboxes and append exact measured acceptance/test records. Commit only source, tests, and documentation:

```powershell
git add README.md docs/graduation/data-readiness.md docs/graduation/olist-order-domain-runbook.md docs/superpowers/plans/2026-09-21-graduation-olist-order-domain.md scripts/verify_g2e_order_domain.ps1 tests/test_g2e_order_domain.py
git commit -m "docs: record G2-E Olist order acceptance"
```

Push `codex/chapter-10-controlled-tools` using the current command-scoped proxy only when direct GitHub access needs it. Compare `git rev-parse HEAD` with `git ls-remote origin refs/heads/codex/chapter-10-controlled-tools`. Do not merge `main` without a separate explicit user request.

### Task 8 Measured Acceptance Record

- 2026-09-24 formal verifier status: `PASS`; evidence written once to ignored `tmp/graduation/g2e/3e0119b83f4ae47a992a6b2dcf30a6ed57403ef798413209ed52d2d4e624c3c3/acceptance.json`.
- Official ZIP: 44,717,580 bytes, SHA-256 `967e41e04fc306fe604e2a693f488995a8b41e5047418f8a5c8e4abd6deca784`; nine source files: 1,550,922 logical rows and bundle `3e0119b83f4ae47a992a6b2dcf30a6ed57403ef798413209ed52d2d4e624c3c3`.
- All nine source tables and nine curated tables passed exact row, key, digest and Snapshot gates. The order fact has 99,441 rows over `2016-09-04` through `2018-10-17`; all 18 hard gates are zero and nine reportable quality measures remain visible.
- Immutable run `orders-v1-b3e0119b83f4ae47a992a6b2dcf30a6ed57403ef798413209ed52d2d4e624c3c3` is `PUBLISHED`. Six family row counts are `660/660/3,031/316,153/660/1`; all candidate and Doris readback SHA-256 values agree.
- Eight full API contracts, one DAY range (`2016-09-04`), one MONTH range (`2016-09-04` through `2016-09-15`) and invalid-request 422 behavior passed. The final read-only acceptance took about 9 minutes 30 seconds.
- Docker was capped at 7.66 GiB. Staged services eliminated the reproduced Trino exit 137, and the 316,153-row one-pass ranking parser used about 1.30 GiB in 35.5 seconds instead of about 5 GiB. No Docker volume, Iceberg table, Doris publication or user file was deleted.
- Exact Task 8 offline command passed 100 tests in 48.739 seconds with one expected Windows symbolic-link capability skip.
- The first repository-wide run found the new Compose resume variable missing from the Chapter 10.5 isolated environment. TDD fixed isolation to force `HIVE_METASTORE_IS_RESUME=false`; focused 2/2 and cold-start 34/34 tests passed.
- Repository-wide verification after the cold-start fix passed 658 tests in 237.357 seconds; the post-review final rerun passed the same 658 tests in 235.567 seconds, both with the same one host-capability skip. Compose config and `git diff --check` passed; no Java/DataStream file changed.
- Final review: self-review (no subagent tool), range `7b66f9051ceb80a68dcb3c9d575bc94fe9da93c1..0068296466d5bd0c85e884f8404a9239a4953cef`. No Critical or Important findings. Three documentation-consistency findings were fixed: source replay order now matches the filename-ordered acceptance run, Tasks 1-7 reflect their committed completion, and the Chapter 10.5 runbook has no trailing whitespace.
- Review boundary: production HA/capacity/SLA and roadmap stages G3-G5 were not judged because G2-E specifies a single-machine bounded acceptance. Upstream collection authenticity beyond the official Kaggle archive, recorded hashes, license metadata, and operator acquisition evidence cannot be independently proven from this repository.
- GitHub backup was verified on branch `codex/chapter-10-controlled-tools`: local HEAD and `refs/heads/codex/chapter-10-controlled-tools` both resolved to `00bb3ae8abc983d3a140815a4f263b8ca97f4b14` before this final evidence-only commit.

## Self-Review

- **Spec coverage:** Tasks 1-8 map every design section to acquisition, streaming normalization, bounded ingestion, curated grains, semantics, metrics, immutable publication, API, security/RAG boundaries, resource limits, tests, and real acceptance.
- **Unfinished-marker scan:** passed; executable SQL tokens and runtime path arguments are explicit interfaces, not missing design decisions.
- **Type consistency:** full bundle/run identities, decimal-string Snapshot IDs, nine source and nine curated table names, six metric families, endpoint names, window literals, dimensions, sort names, and service signatures are consistent across tasks.
- **Review Focus:** all five high-risk classes have named failing tests in their owning tasks.
- **Scope:** G2-E ends at stable data/API contracts. G3 visualization, G4 knowledge-base/RAG, and G5 capacity work remain separate roadmap stages.
