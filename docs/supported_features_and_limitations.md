# Supported features and limitations

## Supported

### Sources
- Oracle and Microsoft SQL Server only, via shared source adapters.
- Secret-backed JDBC; one Databricks secret scope per registered connection.
- Deterministic connection-owned identity v2 over `connection_id`, source
  system, server, database, schema, and table.
- Multiple connection IDs may reference one physical endpoint, including
  different secret scopes. The same physical table under different connection
  IDs has separate control state, targets, history, checkpoints, retries,
  reconciliation, inventory, mappings, and decisions.
- **Endpoint change and secret-scope rotation policy:** Material endpoint changes
  (`source_server` or `source_database`) on an existing `connection_id` with existing
  dependent `source_table_control` registrations are blocked to preserve identity
  integrity and prevent checkpoint rebinding. Operators must create a new
  `connection_id` for a new endpoint. Material changes with zero dependent registrations
  are allowed and reset connection status to `REGISTERED` (requiring revalidation).
  `secret_scope` rotation and `trust_server_certificate` updates on an existing
  endpoint are allowed and set status to `REGISTERED` for revalidation.
  `connection_name` updates are allowed and preserve existing validation status.
  `source_system` mutations are always rejected.
- Mandatory explicit `source_system`; missing, blank, and unknown values fail
  before routing or identity generation.

### INGEST pipeline (source -> Bronze)
- Multi-connection validation (`source_connection` registry, `NB00A`).
  Connection metadata is populated separately; NB00A validates an existing
  registered connection by `connection_id` and updates its status to `VALID` or
  `FAILED`. It does not upsert connection metadata.
- Broad source assessment (`NB01A`, one notebook per source) using
  data-dictionary / catalog views: schemas, tables, views, procedures,
  functions, packages. Row counts are labelled `CATALOG` (SQL Server catalog
  metadata from `sys.partitions`, heap/clustered partitions only), `ESTIMATED`
  (Oracle `ALL_TABLES.NUM_ROWS` optimizer statistics), or `UNAVAILABLE`. A broad
  assessment never executes a per-table `COUNT(*)` / `COUNT_BIG(*)`, so no label
  claims an executed exact count. Assessment publishes `assessment_id` as a task
  value, including on `business_status=PARTIAL`.
- Schema and table discovery are mandatory. A failure in either stage fails the
  assessment task after a sanitized report. Optional view, routine, package,
  column, or statistics errors are counted and return
  `execution_status=SUCCEEDED`, `business_status=PARTIAL`; no errors returns
  `business_status=COMPLETE`.
- **Oracle `SIZE_MB` is an estimate**, labelled
  `SIZE_MB_METHOD=ESTIMATED_8K_BLOCKS`. It multiplies optimizer
  `ALL_TABLES.BLOCKS` metadata by an assumed 8 KiB block size. The current
  adapter contract has no reliable block-size metadata source that avoids new
  DBA-level privileges, so this value must not be represented as exact.
- **SQL Server `SIZE_MB` is the total reserved size** of the table: heap or
  clustered base storage plus all nonclustered indexes, summing `total_pages`
  across every allocation-unit type - IN_ROW_DATA (type 1) and ROW_OVERFLOW_DATA
  (type 3) reached via `container_id = partitions.hobt_id`, and LOB_DATA
  (type 2) reached via `container_id = partitions.partition_id`. The two
  relationships are combined with `UNION ALL` so each allocation unit is counted
  once, and row counts are aggregated in an independent CTE so they are never
  multiplied by the allocation join. **Indexes are included.**
  The query structure is covered by unit tests; the resulting numbers have
  **not** been compared against a live SQL Server instance (see "Validation
  status" below).
- Source assessment and SQL-object inventory are retry-safe: re-running the
  same `assessment_id` updates its own rows instead of duplicating them.
- Assessment-based, collision-safe registration (`NB01B`). New rows are created
  inactive (`is_active = false`, `current_status = 'REGISTERED'`).
  Target collisions are checked across registrations by normalized target FQN.
  Supports `selection_mode = 'ASSESSMENT_FLAGS'` (control-table driven) and `WIDGETS` (legacy).
- **Control-table-driven connection validation and assessment (Job 1A):**
  Discovers configured candidate connections (`connection_mode = 'CONFIGURED'`), validates existing registered connections using source-specific `NB00A` (passing only `connection_id`), discovers active `VALID` connections (`connection_mode = 'VALID'`), and executes connection-scoped assessments (`NB01A`) in For Each tasks, followed by `NB_AssessmentSummary`. Connection worklist items contain strictly `{"connection_id": "..."}`. No notification task is included.
- **Control-table-driven selected-table onboarding (future Job 1B):**
  Discovers eligible batches via `NB_GetSelectedAssessmentWorklist` emitting strictly
  `{"connection_id": "...", "assessment_id": "..."}`. Detects and blocks overlapping selected
  tables across assessments (`AMBIGUOUS_SELECTED_ASSESSMENT`). `NB01B` in `ASSESSMENT_FLAGS` mode
  resolves routing from `accelerator_target_config` without manual schema or table parameters.
- **Target routing configuration (`accelerator_target_config`):**
  Resolves targets by 3-tier precedence: connection-specific override -> source-specific default
  -> global default. *Affects new registrations only.* Existing registrations retain stored
  target identities permanently; differing proposed targets are reported as `TARGET_CONFIG_CHANGED`.
- **Selection lifecycle:**
  `source_assessment` tracks `selection_status`: `NOT_SELECTED`, `SELECTED`, `ONBOARDING`, `REGISTERED`, `ONBOARDED`, `FAILED` (plus terminal states `REVIEW_REQUIRED`, `BLOCKED`). Exact rows transition `NOT_SELECTED -> SELECTED -> ONBOARDING -> REGISTERED -> ONBOARDED`. Failure transitions move owned rows from `SELECTED / ONBOARDING / REGISTERED -> FAILED`.
  - `NB01B_RegisterSelectedTables` performs atomic claims (`SELECTED -> ONBOARDING`) with run/attempt ownership, registers tables in `source_table_control`, and ends at `REGISTERED` (never marking `ONBOARDED`).
  - Caught registration failures move owned rows to `FAILED` with stage `REGISTRATION`.
  - Finalization (`NB_FinalizeSelectedTableOnboarding`) executes after successful `NB08_TargetProvisioning`, verifies active `PROVISIONED` state and collision-free target, and marks exact rows `ONBOARDED`.
  - Downstream failure handler (`NB_MarkSelectedOnboardingFailed`) marks owned incomplete rows `FAILED` when a downstream task fails.
  - REGISTERED rows need continuation/finalization, not registration rediscovery.
  - Stale `ONBOARDING` recovery is manual and evidence-based (`NB_RecoverSelectedOnboardingState`); rows are never reset automatically based on elapsed time.
  - Worklists use a unified task-value payload guard (`TASK_VALUE_LIMIT_BYTES = 40_000` bytes) measuring UTF-8 bytes.
  - Inactive/non-default `accelerator_target_config` history does not participate in routing and does not block initialization.
  - Reserved targets include all non-retired registrations in `source_table_control` across onboarding and operational states.
- **Job YAML unchanged:**
  Databricks Job YAML files remain unchanged in this task and will be updated separately.
  Deployed jobs should not be represented as zero-input before YAML is updated.
- **Onboarding activation flow:** Downstream metadata tasks-Source Inventory
  (`NB01`), Type Normalization (`NB02`), Mapping Generation (`NB03`), Mapping
  Validation (`NB04`), and Table Decision Generation (`NB07`)-operate on registered
  tables in the current onboarding scope (`include_onboarding=True`). Target
  Provisioning (`NB08`) re-verifies target collisions against all registrations,
  provisions target Delta tables, and activates approved `AUTO_MIGRATE` tables
  (`is_active = true`, `current_status = 'PROVISIONED'`). Unapproved, review, or
  blocked tables remain inactive.
- Source inventory is an exact replacement per
  `run_id + connection_id + source_table_id`.
  Incoming duplicate
  `(run_id, connection_id, source_table_id, column_name)` keys fail before
  persistence. A same-run retry replaces the full table snapshot, so a dropped
  source column is removed; older run history and unrelated tables remain.
  `INVENTORIED` is set only after the inventory write succeeds. The notebook
  attempts every intended table, then raises `RuntimeError` when any table
  failed so a partial inventory cannot return task success.
- Full load (`NB09`, overwrite) and delta sync (`NB11a`/`NB11b`) with
  `FULL_LOAD`, `WATERMARK`, `PRIMARY_KEY`, `HYBRID` strategies and frozen
  incremental intervals. Full Load revalidates target ownership before overwrite
  to prevent collision even if control rows were modified out-of-band.
- Full Load and Delta worklists discover eligible registrations across active,
  `VALID` connections. They contain only `run_id`, `connection_id`, and
  `source_table_id`; adapters and secrets are resolved lazily, so a connection
  with no eligible registration is not contacted.
- Correct source-to-Bronze reconciliation performed on the exact work unit
  **before** the checkpoint is committed (extract -> apply -> reconcile ->
  checkpoint -> finalize). `target_count >= source_count` is never an automatic
  pass.
- Failure classification and retry worklists (`NB14`) select the latest failed
  attempt independently per `connection_id + source_table_id + operation`.
  INGEST and ETL own
  explicit operation sets, so blank operation filtering returns all applicable
  failed operations without mixing pipelines. One table may yield multiple
  recovery items. Attempts and `max_retries` are operation-specific;
  non-executable `MANUAL_REVIEW` items are returned separately and never enter
  the ForEach. Checkpoint-only / finalization-only recovery remains no-reapply.
- **Frozen retry boundaries.** `RETRY_DELTA_APPLY` copies the parent run's queue
  row unchanged into a child row, so the source MAX is never recaptured and the
  interval is never widened. `RETRY_ETL` replays the lower/upper bounds recorded
  by the failed attempt rather than recomputing the Bronze MAX. A single
  immutable work unit drives Bronze filtering, interval replacement,
  reconciliation, the success audit, **every** failure audit, and the
  checkpoint - so a second retry receives exactly the same interval and the
  recorded interval always equals the processed one.
- State-only recoveries write a child audit row and never read the source or
  reapply data.
- `table_run_log.status` is normalized to `SUCCEEDED` / `FAILED`; the detailed
  operational state lives on `source_table_control` / `delta_sync_queue`, and
  `failure_stage` + `error_category` carry the precise meaning.
- Source SQL-object inventory (`NB13`): original source-definition extraction
  only. Definitions are stored unchanged in `sql_object_assessment` (historical
  table name retained). Discovery query failures are counted separately from
  inaccessible definitions, which persist with an explicit `error_message`;
  coverage is `COMPLETE`, `PARTIAL`, or `FAILED`.
- **SQL object artifact materialization (`NB18`):** Preserves original Oracle (VIEW, PROCEDURE, FUNCTION, PACKAGE, PACKAGE_BODY) and SQL Server (VIEW, PROCEDURE, FUNCTION) non-table database object definitions as raw `.sql` files in Unity Catalog Volumes (`_source_artifacts`).
  - Stored under deterministic path `/Volumes/<target_catalog>/<target_schema>/_source_artifacts/<safe_connection_id>/<safe_source_database>/<safe_source_schema>/<type_directory>/<safe_object_name>.sql`.
  - Content comes strictly from `sql_object_assessment.source_definition` unchanged.
  - NB18 preserves exact raw source SQL definitions without conversion, does not execute source SQL, does not classify source SQL for deployment, and does not deploy converted SQL objects.
  - Handles raw-definition artifact ownership, collision prevention (using deterministic SHA-256 owner hashing for lossy/special names), idempotency, and missing/corrupt artifact repair (`REPAIRED_MISSING`, `REPAIRED_HASH_MISMATCH`) via atomic `.tmp` rewrite.
  - Live Unity Catalog Volume privileges (`CREATE VOLUME`, `READ VOLUME`, `WRITE VOLUME`) must be validated.
- **SQL Artifact Migration workflow (`ACCELERATOR_SQL_ARTIFACT_MIGRATION`):** An independent selection-driven analyze, convert, and store workflow operating separately from Full Load and Delta Sync. It analyzes, transpiles, and stores; does not deploy, does not execute converted SQL, and never runs `CREATE VIEW` or `CREATE PROCEDURE`.
  - Executable scope is **VIEW and PROCEDURE only**, selected after Assessment. Routine types `FUNCTION`, Oracle `PACKAGE`, `PACKAGE_BODY`, and `TRIGGER` are out of scope for the executable artifact job.
  - Job graph: `T00_Create_Run_Context` -> `T03_Init_SQL_Artifact_Control` -> `T23_Fetch_Selected_SQL_Artifacts` -> `T24_Lakebridge_Analyze_And_Transpile` -> `T06_SQL_Artifact_Summary`.
  - `NB21_SQLArtifactInit` initializes and additively manages dedicated control structures (`sql_artifact_control`, `sql_artifact_execution_log`) without modifying table pipeline state.
  - `NB23_FetchSelectedSQLArtifacts` performs selected-only Oracle and SQL Server JDBC definition fetches from registered source databases and stores raw definitions under `_source_artifacts` Volumes.
  - `NB24_LakebridgeAnalyzeAndTranspile` executes Lakebridge Analyzer and BladeBridge transpilation via an isolated Python environment:
    - Bootstrapped via `uv venv --seed --python 3.12` containing `databricks-labs-lakebridge`, `databricks-bb-plugin`, and `databricks-bb-analyzer` (eliminating the interactive-only Databricks CLI dependency in automated workflow runs).
    - Runs Analyzer via `from databricks.labs.bladespector.analyzer import Analyzer` (`Analyzer.analyze(...)` with source tech `"MS SQL Server"` / `"Oracle"`), extracting structured metrics primarily from JSON (`analyzer_report.json`) with deterministic XLSX fallback (`analyzer_report.xlsx`).
    - Runs BladeBridge transpilation via `from databricks.labs.bladebridge.transpiler import Transpiler` (`source_tech="MSSQL"/"ORACLE"`, `target_tech="SQL"`).
    - Stores converted clean definitions centrally under `/Volumes/da_accelerators/ConvertedArtifacts/converted_artifacts/<database>/<schema>/<object_type>/<object>.sql` and JSON/XLSX reports/error logs under `_lakebridge_reports` Volumes. Folder and filename whitespace is normalized, source metadata remains unchanged, deployment target routing remains separate, and converted files are not executed automatically.
    - Uses unique local staging per artifact attempt (`/local_disk0/sql_artifact_lakebridge/<run_id>/<artifact_id>/<attempt>/<uuid>/`).
    - Staging directory is cleaned up only on complete success; retained on failure for diagnostics.
    - `CREATE SCHEMA IF NOT EXISTS` and `CREATE VOLUME IF NOT EXISTS` are run idempotently on `da_accelerators.ConvertedArtifacts.converted_artifacts`.
    - Converted SQL is never executed or deployed.
  - Classifications:
    - `AUTO` / `AUTO_CANDIDATE`: Allowed only for `VIEW` when transpilation succeeds with zero errors, zero warnings/fixmes, no risky constructs, no unresolved source syntax/references, and `object_map_applied = true`. Because `object_map_applied` is false in this release, no artifact is classified `AUTO`; never auto-deployed; `deployment_status = NOT_DEPLOYED`.
    - `MANUAL_REVIEW`: Successfully converted `PROCEDURE`, or successfully converted `VIEW` while `object_map_applied = false`, or any artifact with high complexity, unknown statements, fixmes, risk constructs, or validation errors; reason recorded; converted definition stored if available; never auto-deployed; `deployment_status = NOT_DEPLOYED`.
    - `UNSUPPORTED`: Missing or blank source definition, Analyzer failure, parsing/generation errors, transpile failure, or missing/blank output; non-migratable constructs or out-of-scope objects such as `TRIGGER`; reason recorded; converted definition stored if available; never auto-deployed; `deployment_status = NOT_DEPLOYED`.
  - Historical reference: Legacy prototype `NB22_SQLArtifactMigrate` converts, classifies, and stores; does not deploy and does not connect to source databases. The active executable artifact pipeline uses Lakebridge Analyzer and BladeBridge Python APIs in an isolated environment via `NB24_LakebridgeAnalyzeAndTranspile`.
  - Assessment, Onboarding, Full Load, Delta Sync, ETL, Retry, and reconciliation remain unchanged.
  - Lakebridge, BladeBridge, JDBC, Unity Catalog, Volume, and Databricks Job behavior require live validation.
  - Dedicated artifact control tables are updated; existing configuration and table pipeline tables remain strictly read-only.

### Datatype mapper ownership
- Shared mapping calls `adapter.load_type_mapper().map_column(...)` and consumes
  the adapter's source-neutral column policy result.
- `src/type_mappers/base.py` contains only the mapper interface, immutable
  result, common status/fidelity constants, YAML contract validation, and table
  compatibility classification.
- Oracle rules and precision/scale behavior live only in
  `src/type_mappers/oracle.py`; SQL Server rules and decimal behavior live only
  in `src/type_mappers/sqlserver.py`.
- `src/type_mappers/factory.py` uses an explicit registry. There is no dynamic
  discovery or directory scan. `src/crosssourcetypemapper.py` is a deprecated
  compatibility facade and contains no dialect rules.

### Oracle mapping policy
- Oracle `NUMBER` uses Oracle-owned precision/scale resolution. Whole-number
  precisions select `SMALLINT`, `INT`, `BIGINT`, or `DECIMAL`.
- Unconstrained `NUMBER` does not default to AUTO/EXACT; it maps to `DECIMAL(38,0)`
  only as a proposed fallback with status `REVIEW` and fidelity `UNKNOWN`, requiring
  profiling of observed values and precision/scale before migration.
- `NUMBER(p,s)` validates metadata (1 <= p <= 38). Scale > precision (such as
  `NUMBER(4,5)`) fails closed to `REVIEW` with `LOSSY` fidelity. Valid negative
  scale maps with status `REVIEW` and `WIDENED` fidelity. Precision > 38 remains
  `BLOCKED`.
- Oracle `DATE`, `TIMESTAMP`, LOB, JSON, BOOLEAN, VECTOR, interval, spatial, and
  user-defined behavior remains in `OracleTypeMapper` and the Oracle adapter.
  JSON and timezone-sensitive mappings retain their existing review policy;
  VECTOR and unsupported types remain BLOCKED.

### SQL Server mapping and column policy
- SQL Server `decimal`/`numeric` precision and scale, unsigned `tinyint`
  widening, money families, `uniqueidentifier`, CLR/unsupported types, and
  temporal mappings are owned by `SqlServerTypeMapper`.
- SQL Server `decimal`/`numeric` metadata is validated fail-closed: precision
  must be between 1 and 38, scale between 0 and precision, and values must be
  integers. Corrupt or synthetic metadata (such as `DECIMAL(0,0)`, scale > precision,
  negative scale, or non-numeric precision/scale) produces `BLOCKED`/`UNKNOWN`.
- `datetime2` maps to Delta `TIMESTAMP` with the existing AUTO/LOSSY
  microsecond policy. Source projection and watermark predicates normalize the
  selected `datetime2` value to six fractional digits in the SQL Server query
  builder; `datetime2(7)` loses its seventh digit.
- SQL Server `timestamp` and `rowversion` are binary change tokens, map to
  `BINARY`, and are never temporal watermarks.
- The SQL Server adapter marks computed columns `REVIEW`, hidden/system columns
  `BLOCKED` and excluded, and rowversion columns non-writable while preserving
  their binary type mapping. Identity metadata is retained.

### ETL pipeline (Bronze -> Silver)
- Processes only successfully ingested Bronze tables; never connects to a source
  or reads a source secret.
- MVP data-quality rules only: `NOT_NULL`, `DUPLICATE_KEY`, `DATA_TYPE`,
  `ALLOWED_VALUES`, `DEFAULT_VALUE`, `TRIM_STRING`, `STANDARDIZE_CASE`.
- **DUPLICATE_KEY policy:** every record in a duplicate-key group is rejected.
  No survivor is kept, because there is no deterministic survivor-ordering rule.
  Key columns come from the rule's `column_name` (comma-separated for a
  composite key), otherwise from the table's registered primary key.
- An **invalid active** DQ rule fails the table before any transform, Silver
  write, or quarantine write (`DQ_CONFIG_ERROR`); it is never silently skipped.
  An inactive invalid rule does not block ETL.
- Incremental ETL compares a DATE/TIMESTAMP Bronze watermark using explicitly
  cast typed bounds - never a lexical string comparison. Any other watermark
  type is a configuration error.
- Quarantine is idempotent per `run_id` + `source_table_id`: re-running the same
  ETL run replaces that run's quarantine rows instead of duplicating them.
  Other runs' history is untouched. With `quarantine_enabled=false` rejected
  rows are still counted for reconciliation but are not persisted.
- Valid rows to Silver; rejected rows to `dq_quarantine` (with column
  redaction). Bronze-to-Silver reconciliation before a **separate** ETL
  checkpoint. Retry-safe FULL overwrite, MERGE, and interval replacement.
- Duplicate primary keys in the valid ETL input fail **before** the MERGE runs.

### Shared
- Failure classification, retry metadata, notifications (`NB16`, Teams webhook
  from a secret, never logged), and dashboard views (`NB17`).

## Limitations / non-goals
- No generalized workflow engine or custom scheduler (Databricks Jobs orchestrate).
- In the dedicated SQL Artifact Migration workflow, executable scope is strictly VIEW and PROCEDURE only, analyze/transpile and store only. Converted SQL is never executed, and `CREATE VIEW` or `CREATE PROCEDURE` is never run. Nothing is deployed. NB23 connects to registered source databases via JDBC only to fetch selected definitions. Raw definitions are stored in `_source_artifacts` Volumes. NB24 executes Databricks Labs Lakebridge and BladeBridge Python APIs in an isolated runtime to analyze and transpile, storing outputs centrally in `/Volumes/da_accelerators/ConvertedArtifacts/converted_artifacts/<database>/<schema>/<object_type>/<object>.sql` and reports in `_lakebridge_reports` Volumes. Folder and filename whitespace is normalized, source metadata is unchanged, deployment target routing remains separate, and converted files are not executed automatically. `CREATE SCHEMA IF NOT EXISTS` and `CREATE VOLUME IF NOT EXISTS` may be used to support required Volumes.
- Routine types `FUNCTION`, Oracle `PACKAGE`, `PACKAGE_BODY`, and `TRIGGER` are out of scope for the active artifact migration job; any encountered trigger or unhandled routine is classified `UNSUPPORTED` and never deployed or executed.
- Raw source definitions materialized by NB18 are preserved unchanged without conversion under `_source_artifacts` volumes.
- No AI conversion of source SQL objects; transpilation uses deterministic rule-based transformations.
- No Gold layer, no reporting warehouse, no cloud-billing ingestion, no
  automatic cost optimization, no anomaly detection, no ServiceNow/paging.
- DQ rules are the fixed MVP set only; arbitrary SQL expressions are rejected.
- No new source systems beyond Oracle and SQL Server.
- Existing physical-source IDs require the explicit dry-run-first
  `deployment/NB_MigrateSourceTableIdentityV2` upgrade. Initialization never
  rewrites them automatically.

## Security
- No username, password, token, secret value, or credential-bearing JDBC URL is
  ever stored in Delta, returned by a task, or written to logs.
- The registered `source_connection.secret_scope` is **authoritative**. Shared
  code never infers a scope from the source system; a blank registered scope
  fails clearly. Source operations require an active `VALID` registered
  connection and verify that queue/control rows retain the same `connection_id`.
  Legacy global scope widgets remain only for explicit diagnostic compatibility
  and are named by the adapter, not by shared operational code.
- Connection diagnostics report sample-query success, row count, and column
  names only. `show_sample_values` defaults to `false`; enabling it prints a
  warning and is permitted only with approved non-sensitive test data.
- An unregistered source fails explicitly. It never falls through to Oracle or
  SQL Server behavior.
- Legacy control rows with missing ownership no longer run. NB00 reports and
  blocks them without changing their IDs; an operator must classify each
  reviewed row explicitly before the identity-v2 migration. For
  example, only after confirming the source is Oracle:

  ```sql
  UPDATE <catalog>.<control_schema>.source_table_control
  SET source_system = 'oracle'
  WHERE source_table_id = '<reviewed legacy source_table_id>'
    AND (source_system IS NULL OR trim(source_system) = '');
  ```

  The accelerator never executes this repair or guesses the source.
- SQL Server uses `encrypt=true`; `trustServerCertificate` defaults to `false`.
- Error messages are sanitized **before** printing and before every persistence
  path (`update_control`, `update_connection_status`, `log_job_run`,
  `log_table_run`) and before notification. Coverage includes `password`/`pwd`,
  `user`/`username`, `token`/`access_token`/`refresh_token`, `secret`/
  `client_secret`, SAS `sig`, `Authorization: Bearer`/`Basic`, URL and JDBC
  userinfo, and webhook path tokens. Safe identifiers (schema, table, database,
  host, error codes) are deliberately preserved.

## Validation status

Repository command evidence is captured under `artifacts/test-results/`. The
machine-readable `test-summary.json` is authoritative for command, timestamp,
exit code, and runner-reported counts. Pure tests cover query construction,
policy, reconciliation, retry boundaries, sanitization, inventory identities,
mapping regressions, and static notebook/modularity contracts. They do not
establish runtime production readiness.

**NOT executed** (requires a live environment; nothing below is claimed as
passing): Spark execution, JDBC authentication/networking, Delta operations,
Unity Catalog permissions, Oracle and SQL Server behavior/permissions,
Databricks Job/ForEach orchestration, task-value propagation, secret-scope
resolution, and live validation of reported metadata values.

Use `docs/production_readiness_checklist.md` for repository and live evidence.
Allowed statuses are `NOT_EXECUTED`, `PASSED`, `FAILED`, `BLOCKED`, and
`NOT_APPLICABLE`. Required live rows default to `NOT_EXECUTED`; do not claim
production readiness from repository tests alone or SQL Server `SIZE_MB`
accuracy until the live comparison passes.
