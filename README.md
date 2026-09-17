# Oracle and SQL Server to Databricks Migration Accelerator

A metadata-driven accelerator for migrating Oracle and Microsoft SQL Server tables to Databricks Delta tables governed by Unity Catalog. Shared notebooks route every control-table row through a source-specific adapter, while target provisioning, audit, reconciliation, and incremental orchestration remain common.

## Repository layout

```text
config/
  type_rules.yaml
  type_rules_oracle.yaml
  type_rules_sqlserver.yaml
notebooks/
  deployment/
    NB_CreateRunContext.ipynb
    NB_GetFullLoadWorklist.ipynb
    NB_GetDeltaWorklist.ipynb
    NB_MigrateSourceTableIdentityV2.py
  shared/                       # source-neutral; identical for every source
    _common.py
    NB00_ControlTableInit.py
    NB01B_RegisterSelectedTables.py
    NB02_TypeNormalization.py
    NB03_MappingRulesGeneration.py
    NB04_MappingValidation.py
    NB07_TableDecisionGeneration.py
    NB08_TargetProvisioning.py
    NB09_FullLoad.py
    NB10_PostFullLoadState.py
    NB11a_DeltaSyncPrep.py
    NB11b_DeltaSyncApply.py
    NB12_ValidationAndReconciliation.py
    NB14_RetryFailedTables.py
    NB15_BronzeToSilverETL.py
    NB16_NotifyFailures.py
    NB17_DashboardViews.py
  sources/                      # thin, dialect-specific operational notebooks
    oracle/
      NB00A_UpsertAndValidateConnection.py
      NB01_SourceInventory.py
      NB01A_SourceAssessment.py
      NB13_SQLObjectAssessmentAndConversion.py
      TEST_CONNECTION.py
    sqlserver/
      NB00A_UpsertAndValidateConnection.py
      NB01_SourceInventory.py
      NB01A_SourceAssessment.py
      NB13_SQLObjectAssessmentAndConversion.py
      TEST_CONNECTION.py
src/
  source_adapters/
    base.py
    factory.py
    oracle.py
    sqlserver.py
  type_mappers/
    base.py
    factory.py
    oracle.py
    sqlserver.py
  assessment_common.py
  control_repository.py
  crosssourcetypemapper.py
  ddl_builder.py
  dq_rules.py
  failure_classifier.py
  identifiers.py
  inventory_common.py
  partitioning.py
  reconciliation.py
  source_identity.py
  source_registry.py
  sql_builder.py
  sql_object_assessment_common.py
  sql_object_converter.py
  sqlserver_sql_builder.py
  strategy.py
docs/
  adding_a_new_source.md
  installation.md
  supported_features_and_limitations.md
  databricks_job_task_mapping.md
  production_readiness_checklist.md
  source_identity_v2_migration.md
artifacts/
  test-results/                  # captured repository-test evidence
tests/
  _fakes.py
  _nbsource.py
  test_connection_registry.py
  test_assessment.py
  test_reconciliation.py
  test_failure_retry.py
  test_sql_object_converter.py
  test_etl_dq.py
  test_defect_fixes.py
  test_inventory_idempotency.py
  test_multi_connection.py
  test_modularity.py
  test_notebook_wiring.py
  test_release_documentation.py
  test_type_mappers.py
requirements-dev.txt
```

## Modular source architecture

Logic that is identical for every source lives in `notebooks/shared/` and
`src/`. Logic that changes with the source dialect, catalog, metadata,
connectivity, or SQL lives in `notebooks/sources/<source>/` and in that source's
adapter. Shared notebooks never compare `source_system` to a source literal - an
AST-based test parses the shared code and fails the build if a source branch, a
dialect catalog string, a source credential key, a dialect query-builder import,
or a direct concrete-adapter construction appears.

The adapter owns, and shared code asks for:

- its **connection probe** (`connection_probe_query()`) - shared code never
  hard-codes `SELECT 1 FROM DUAL` or `SELECT 1`
- its **type-rules file** (`type_rules_file()`) - shared code locates the named
  file under `config/` and fails loudly if it is missing, instead of inferring a
  filename from the source system
- its **type mapper** (`load_type_mapper()`) - Oracle and SQL Server mapping
  algorithms live in `src/type_mappers/oracle.py` and
  `src/type_mappers/sqlserver.py`; `src/type_mappers/base.py` contains only the
  source-neutral result, status/fidelity constants, interface, and table-level
  compatibility classifier
- its **column policy** (`apply_column_policy()`) - hidden, generated,
  non-writable, and version columns are expressed as canonical policy codes
  (`SOURCE_HIDDEN_COLUMN`, `SOURCE_GENERATED_COLUMN`,
  `SOURCE_NON_WRITABLE_COLUMN`, `SOURCE_BINARY_VERSION_COLUMN`), so shared
  mapping and validation consume `include_column` / `is_writable` /
  `requires_review` / `policy_code` and never interpret a dialect flag
- its **connection metadata rules** (`validate_connection_metadata()`)

The registered `source_connection.secret_scope` is authoritative; shared code
never infers a scope from the source system, and an unregistered source fails
explicitly rather than falling through to Oracle or SQL Server.

Each source folder provides exactly five thin notebooks: connection validation,
broad assessment, metadata inventory, SQL-object assessment, and a connection
diagnostic. Everything else - Bronze provisioning and loading, reconciliation,
checkpoints, ETL, quarantine, retries, dashboards, notifications, and
control-table DDL - is shared and never duplicated per source.

Adding a future source requires a new adapter, one factory registration, one
`src/source_registry.py` entry, its own mapper plus one explicit mapper-factory
registration, a type-rules YAML, and those five notebooks. No existing Oracle
or SQL Server mapper and no shared mapping notebook changes. The legacy
`src/crosssourcetypemapper.py` module is a compatibility-only constructor
facade. See `docs/adding_a_new_source.md`.

Source notebooks bootstrap with `%run ../../shared/_common`; shared notebooks
use `%run ./_common`. Every notebook exists in exactly one place - there are no
duplicate or wrapper copies - so Databricks job definitions must reference the
`shared/` or `sources/<source>/` path directly.

## Architecture: two pipelines (INGEST and ETL)

The accelerator is organized as two Databricks pipelines over shared control and
audit tables. They share metadata, run IDs, failure classification, and
reporting views, but never duplicate responsibilities or checkpoints.

**INGEST pipeline (source -> Bronze).** Owns connection selection/validation,
source assessment and inventory, object discovery, metadata mapping and Bronze
provisioning, full and incremental loads into Bronze, source-to-Bronze
reconciliation, ingest checkpoints, failed-ingest recovery, and SQL-object
assessment/conversion drafts.

**ETL pipeline (Bronze -> Silver).** Starts only after Bronze ingestion
succeeds. Owns Bronze-to-Silver transformation, data-quality checks and
cleansing, quarantine, Bronze-to-Silver reconciliation, a separate ETL
checkpoint, and failed-ETL recovery. It never connects to a source and never
reads a source secret scope.

### Multi-connection onboarding
- `source_connection` stores non-secret connection metadata; credentials stay in
  a per-connection Databricks secret scope.
- One physical endpoint may have multiple connection IDs and secret scopes. Each
  source-table registration belongs to exactly one connection; the same physical
  table registered through two connections has two independent IDs, targets,
  checkpoints, retries, reconciliations, and audit histories.
- `NB00A_UpsertAndValidateConnection` upserts metadata and validates
  connectivity (`SELECT 1 FROM DUAL` / `SELECT 1`), setting the connection
  active/`VALID` or inactive/`FAILED` with a sanitized error. Onboarding remains
  one connection per execution.
- Operational worklists are discovered globally from eligible registrations and
  contain only `run_id`, `connection_id`, and `source_table_id`. Connections
  without eligible registrations are not resolved, connected, or updated.

### Source assessment and registration
- `NB01A_SourceAssessment` discovers objects via data-dictionary/catalog views
  and classifies table compatibility (`COMPATIBLE` / `REVIEW` / `MANUAL` /
  `UNABLE_TO_ASSESS`) without per-table `COUNT(*)`. Mandatory schema/table
  discovery failures fail the task. Optional discovery failures return
  `execution_status=SUCCEEDED` and `business_status=PARTIAL`; full coverage is
  `business_status=COMPLETE`.
- Oracle `SIZE_MB` is labelled `ESTIMATED_8K_BLOCKS`: it uses
  `ALL_TABLES.BLOCKS` with an 8 KiB block assumption because the current
  least-privilege adapter contract has no reliable database block-size source.
- `NB01B_RegisterSelectedTables` registers only explicitly selected COMPATIBLE/
  REVIEW tables as **inactive** rows, computes the deterministic
  `source_table_id`, blocks target collisions, and preserves existing state.
  Newly registered rows are never auto-activated.

### Source-to-Bronze full and delta, reconciliation, and checkpoint order
- Full load is overwrite-only (`NB09`); delta sync uses frozen intervals
  (`NB11a`/`NB11b`).
- The mandatory delta order is **extract -> apply -> reconcile the exact work
  unit -> commit checkpoint -> finalize queue**. `target_count >= source_count`
  is never an automatic pass; named checks (`FULL_SNAPSHOT_COUNT`,
  `DELTA_INTERVAL_COUNT`, `STAGE_COUNT`, `DUPLICATE_PRIMARY_KEY`,
  `MERGED_KEY_EXISTENCE`) decide PASS/WARN/FAIL. A reconciliation failure leaves
  the ingest watermark unchanged.

### Failed-ingest handling
- `table_run_log` carries `attempt_number`, `failure_stage`, `error_category`,
  `retry_eligible`, and `parent_run_id`. `NB14_RetryFailedTables` is a selector
  that returns a per-table retry worklist with a safe `recovery_action`
  (checkpoint-only and finalization-only retries never reapply data).

### SQL objects
- `NB13_SQLObjectAssessmentAndConversion` (ASSESS/CONVERT) captures Oracle
  `ALL_VIEWS`/`ALL_SOURCE` and SQL Server `sys.sql_modules` definitions,
  classifies complexity, and produces limited deterministic Databricks SQL drafts
  for simple views. Every draft is `PENDING_REVIEW`; nothing is executed.

### Bronze-to-Silver ETL and data quality
- Enable per table with `etl_is_active`, a Silver target, and `dq_rule` rows.
- `NB15_BronzeToSilverETL` applies cleansing then validation, quarantines
  rejects (with column redaction), reconciles the processed set before a
  **separate** ETL checkpoint, and is retry-safe. Source-ingest and ETL
  watermarks are tracked independently.

### Notifications and dashboards
- `NB16_NotifyFailures` builds a sanitized per-run failure summary and optionally
  posts a Teams webhook (read only from a secret, never logged).
- `NB17_DashboardViews` creates `vw_assessment_summary`, `vw_ingest_status`,
  `vw_etl_status`, and `vw_validation_status` (history preserved in base tables).

### Security requirements
- No credential, token, secret value, or credential-bearing JDBC URL is stored in
  Delta, returned by a task, or logged. SQL Server uses `encrypt=true` with
  `trustServerCertificate=false` by default. Errors and URLs are sanitized.

See `docs/installation.md`, `docs/supported_features_and_limitations.md`,
`docs/databricks_job_task_mapping.md`, and `docs/adding_a_new_source.md` for
details, validation status, and manual validation steps.

Pipeline JSON files may be maintained separately. Confirm notebook paths, task order, compute, and parameters before importing any job definition.

## Source registration

Every source row is identified by:

```text
connection_id
source_system
source_server
source_database
source_schema
source_table
```

A deterministic v2 `source_table_id` is the SHA-256 digest of a version marker
and that normalized six-part identity. `connection_id` is trimmed; source
system, server, and database retain their existing normalization; schema and
table casing remains significant. Thus the same physical object registered via
two connection IDs receives two distinct IDs.

`source_system` is mandatory on every registered connection and operational
row. A missing, blank, or unknown value fails explicitly before identity
generation or adapter routing; it never defaults to Oracle or SQL Server.

Supported `source_system` values:

```text
oracle
sqlserver
sql_server
mssql
```

Aliases normalize to `sqlserver`. Unknown systems fail explicitly.

Existing five-part IDs are never recalculated during ordinary initialization or
processing. Before enabling global operational worklists on an upgraded
installation, run `notebooks/deployment/NB_MigrateSourceTableIdentityV2.py`
first with `dry_run=true`, resolve every blocker, then rerun with
`dry_run=false`. The migration preserves the old ID in
`legacy_source_table_id`, marks `source_identity_version=2`, and updates child
history by `connection_id + old_source_table_id`. It is never invoked
automatically. Repair missing source ownership only after reviewing the real
source; do not infer it from names or secret keys. Example:

See `docs/source_identity_v2_migration.md` for the complete dry-run, execution,
and verification procedure.

```sql
UPDATE <catalog>.<control_schema>.source_table_control
SET source_system = 'oracle'
WHERE source_table_id = '<reviewed legacy source_table_id>'
  AND (source_system IS NULL OR trim(source_system) = '');
```

## Unity Catalog organization

The default control namespace is:

```text
da_accelerators.control
```

Target location is selected per control row using:

```text
target_catalog
target_schema
target_table
```

If multiple active source rows resolve to the same target fully qualified name, target provisioning marks a configuration error instead of allowing an overwrite collision.

## Secret scopes

Default scopes:

```text
Oracle:     oracle-migration
SQL Server: sqlserver-migration
```

Oracle keys:

```text
oracle-user
oracle-password
oracle-jdbc-url
```

If `oracle-jdbc-url` is absent, the adapter uses:

```text
oracle-host
oracle-port
oracle-service
```

SQL Server keys:

```text
sqlserver-user
sqlserver-password
sqlserver-jdbc-url
```

If `sqlserver-jdbc-url` is absent, the adapter uses:

```text
sqlserver-host
sqlserver-port
```

`source_database` is required for SQL Server control rows. Credentials must remain in secret scopes and must not be committed to Git.

## Connection validation

Run the matching connection notebook before onboarding tables:

```text
notebooks/sources/oracle/TEST_CONNECTION.py
notebooks/sources/sqlserver/TEST_CONNECTION.py
```

The SQL Server adapter uses the Microsoft JDBC driver:

```text
com.microsoft.sqlserver.jdbc.SQLServerDriver
```

The generated direct JDBC URL enables encryption and does not trust the server certificate by default.

Diagnostics do not display sampled source values by default. They report only
sample-query success, sampled row count, and column names. The
`show_sample_values` widget defaults to `false`; set it to `true` only for
approved non-sensitive test data.

## INGEST onboarding workflow

Run in this order (source-specific tasks pick the matching `sources/<source>/`
notebook; everything else is shared):

```text
shared/NB00_ControlTableInit
sources/<source>/NB00A_UpsertAndValidateConnection
sources/<source>/NB01A_SourceAssessment
shared/NB01B_RegisterSelectedTables
sources/<source>/NB01_SourceInventory
shared/NB02_TypeNormalization
shared/NB03_MappingRulesGeneration
shared/NB04_MappingValidation
shared/NB07_TableDecisionGeneration
shared/NB08_TargetProvisioning
sources/<source>/NB13_SQLObjectAssessmentAndConversion
```

NB02-NB08 are **bulk** metadata tasks for one onboarding `connection_id`.

Full Load is a separate global operational workflow:

```text
deployment/NB_CreateRunContext
deployment/NB_GetFullLoadWorklist
shared/NB09_FullLoad            (per item inside a ForEach)
shared/NB12_ValidationAndReconciliation with mode=full
shared/NB10_PostFullLoadState
```

The worklist discovers registrations across all active, `VALID` connections;
optional connection/table filters narrow it. Each NB09 iteration requires and
resolves exactly `run_id + connection_id + source_table_id`.

Source inventory is retry-safe at the exact
`run_id + connection_id + source_table_id` scope.
Each successfully discovered table is validated as one complete incoming
snapshot, duplicate
`(run_id, connection_id, source_table_id, column_name)` keys fail before
any write, then that table's existing same-run snapshot is deleted and replaced.
A dropped source column therefore disappears on retry, while older run history
and every other table remain untouched. The table is marked `INVENTORIED` only
after persistence succeeds. All intended tables are attempted; if any table
fails, the notebook safely reports succeeded/failed counts and raises
`RuntimeError` so downstream tasks cannot treat a partial inventory as success.

See `docs/databricks_job_task_mapping.md` for the exact task keys and parameters.

## INGEST recurring synchronization workflow

Run in this order:

```text
shared/NB11a_DeltaSyncPrep
deployment/NB_GetDeltaWorklist
shared/NB11b_DeltaSyncApply
shared/NB12_ValidationAndReconciliation with mode=delta
```

Supported strategies:

```text
FULL_LOAD
WATERMARK
PRIMARY_KEY
HYBRID
```

NB11a discovers initialized registrations globally, creates adapters lazily only
for participating connections, and freezes each interval independently. The
Delta worklist emits only `run_id`, `connection_id`, and `source_table_id`.

Behavior:

- `FULL_LOAD`: complete source extract and complete target overwrite.
- `WATERMARK`: bounded temporal interval replacement and append.
- `PRIMARY_KEY`: complete source extract and Delta MERGE by primary key.
- `HYBRID`: bounded temporal extract and Delta MERGE by primary key.

Only supported temporal source types are eligible as automatic watermarks. Numeric IDs, quantities, strings, SQL Server `timestamp`, and SQL Server `rowversion` are not temporal watermarks.

## SQL Server datetime2 policy

Databricks Delta `TIMESTAMP` uses microsecond precision. SQL Server `datetime2(7)` contains a seventh fractional digit, so this accelerator applies an explicit automatic lossy policy:

```text
SQL Server datetime2
-> source-side datetime2(6)
-> Databricks TIMESTAMP
-> mapping status AUTO
-> fidelity LOSSY
```

The SQL Server full-load projection, upper-watermark query, incremental predicate, and returned incremental watermark column all use the same six-digit policy for a selected `datetime2` watermark.

Other supported SQL Server temporal families retain their family-specific query behavior. SQL Server `timestamp` and `rowversion` map to `BINARY` and are never temporal watermarks.

## Empty temporal tables

A successfully loaded empty `WATERMARK` or `HYBRID` table receives the adapter-defined canonical bootstrap checkpoint:

```text
1900-01-01T00:00:00.000000Z
```

This permits later inserts to be discovered by the INGEST recurring
synchronization workflow instead of leaving the table permanently outside
incremental processing.

## Column safety policy

SQL Server metadata records identity, computed, hidden, rowversion, and source type schema properties.

```text
Hidden column:   BLOCKED
Computed column: REVIEW
Identity column: retained as metadata
Rowversion:      BINARY, never temporal
```

Only approved `AUTO` mappings are selected for automatic extraction and target provisioning.

## Type mapping highlights

Oracle examples:

```text
NUMBER(p,0) -> SMALLINT, INT, BIGINT, or DECIMAL(p,0)
Unconstrained NUMBER -> DECIMAL(38,0), AUTO
DATE -> TIMESTAMP, WIDENED
TIMESTAMP WITH TIME ZONE -> TIMESTAMP, REVIEW/LOSSY
BFILE, ANYDATA, SDO_GEOMETRY -> BLOCKED
```

SQL Server examples:

```text
int -> INT
bigint -> BIGINT
decimal(p,s) -> DECIMAL(p,s), up to precision 38
money -> DECIMAL(19,4)
datetime2 -> TIMESTAMP, AUTO/LOSSY at six digits
uniqueidentifier -> STRING
rowversion/timestamp -> BINARY
sql_variant, hierarchyid, geometry, geography -> BLOCKED
```

The source-qualified YAML and concrete mapper are the source of truth for each
source. Shared NB03 calls `adapter.load_type_mapper().map_column(...)` and has no
dialect branch. `crosssourcetypemapper.py` is retained only for compatible
construction through `CrossSourceTypeMapper(rules, dialect=...)`.

## Delete policy

Default behavior keeps target rows that no longer exist at the source.

```text
delete_policy = IGNORE_DELETES
```

For a `PRIMARY_KEY` strategy processing a complete source snapshot, `HARD_DELETE` can request:

```sql
WHEN NOT MATCHED BY SOURCE THEN DELETE
```

This is not applied to bounded `WATERMARK` or `HYBRID` slices.

## Testing

Install development dependencies and run:

```bash
python -m pip install -r requirements-dev.txt
python -m compileall -q src tests
python -m pytest tests -q
python -m unittest discover -s tests -v
```

The repository unit tests are pure Python (no Spark or JDBC). They cover the
connection registry and input validation, source-assessment discovery-query
generation and compatibility classification, source-to-Bronze and Bronze-to-Silver
reconciliation rules, failure classification and retry recovery mapping, SQL-object
classification/conversion, and DQ-rule parsing. `tests/_fakes.py` provides a Spark
double for query-building assertions. Always run the suite after changing adapter
contracts, query builders, mappings, reconciliation, or notebook wiring.

Final command output and machine-readable status are written to:

```text
artifacts/test-results/compileall-output.txt
artifacts/test-results/pytest-output.txt
artifacts/test-results/unittest-output.txt
artifacts/test-results/test-summary.json
```

These files report repository checks only. They do not prove live Spark, JDBC,
Delta, Oracle, SQL Server, Unity Catalog, or Databricks Job readiness. The
release checklist uses `NOT_EXECUTED`, `PASSED`, `FAILED`, `BLOCKED`, and
`NOT_APPLICABLE`; no runtime item is treated as passed without live evidence.

## Deployment validation

Before production use, complete live validation against the intended source platform:

1. Run the matching connection notebook.
2. Onboard one small table for each intended strategy.
3. Validate initial row counts and target schema.
4. Insert and update controlled source rows.
5. Run the INGEST recurring synchronization workflow and verify data, queue state, audit state, and checkpoint movement.
6. Restart compute and repeat the relevant connection and synchronization checks.
7. Validate the final SQL Server deployment against the chosen Azure SQL, Cloud SQL for SQL Server, VM-hosted SQL Server, or on-premises network path.

Unit tests validate pure logic and static wiring. They do not replace live JDBC, permissions, TLS, networking, source-dialect, or Databricks job validation.
Track those results separately in
`docs/production_readiness_checklist.md`. Production readiness must not be
claimed while required live checks remain `NOT_EXECUTED`.

Repository changes do not update Databricks Job YAML. Deployed Jobs must be
updated separately to use the run-only context, global worklist tasks, and the
three-field ForEach payload.
