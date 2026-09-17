# Supported features and limitations

## Supported

### Sources
- Oracle and Microsoft SQL Server only, via shared source adapters.
- Secret-backed JDBC; one Databricks secret scope per registered connection.
- Deterministic five-part `source_table_id` identity.
- Mandatory explicit `source_system`; missing, blank, and unknown values fail
  before routing or identity generation.

### INGEST pipeline (source -> Bronze)
- Pipeline-driven multi-connection onboarding (`source_connection` registry,
  `NB00A`). Only non-secret metadata is stored.
- Broad source assessment (`NB01A`, one notebook per source) using
  data-dictionary / catalog views: schemas, tables, views, procedures,
  functions, packages. Row counts are labelled `CATALOG` (SQL Server catalog
  metadata from `sys.partitions`, heap/clustered partitions only), `ESTIMATED`
  (Oracle `ALL_TABLES.NUM_ROWS` optimizer statistics), or `UNAVAILABLE`. A broad
  assessment never executes a per-table `COUNT(*)` / `COUNT_BIG(*)`, so no label
  claims an executed exact count.
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
- Source assessment and SQL-object assessment are retry-safe: re-running the
  same `assessment_id` updates its own rows instead of duplicating them, and an
  existing `APPROVED`/`REJECTED` review decision is preserved.
- Source inventory is an exact replacement per `run_id + source_table_id`.
  Incoming duplicate `(run_id, source_table_id, column_name)` keys fail before
  persistence. A same-run retry replaces the full table snapshot, so a dropped
  source column is removed; older run history and unrelated tables remain.
  `INVENTORIED` is set only after the inventory write succeeds. The notebook
  attempts every intended table, then raises `RuntimeError` when any table
  failed so a partial inventory cannot return task success.
- Assessment-based, collision-safe registration (`NB01B`). New rows are inactive
  (`REGISTERED`); `MANUAL` / `UNABLE_TO_ASSESS` are never auto-activated.
- Full load (`NB09`, overwrite) and delta sync (`NB11a`/`NB11b`) with
  `FULL_LOAD`, `WATERMARK`, `PRIMARY_KEY`, `HYBRID` strategies and frozen
  incremental intervals.
- Correct source-to-Bronze reconciliation performed on the exact work unit
  **before** the checkpoint is committed (extract -> apply -> reconcile ->
  checkpoint -> finalize). `target_count >= source_count` is never an automatic
  pass.
- Failure classification and retry worklists (`NB14`) select the latest failed
  attempt independently per `source_table_id + operation`. INGEST and ETL own
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
- SQL-object assessment and limited deterministic conversion drafts (`NB13`);
  every generated draft is `PENDING_REVIEW` and is never executed. Discovery
  query failures are counted separately from inaccessible definitions, which
  persist as `UNABLE_TO_ASSESS`; coverage is `COMPLETE`, `PARTIAL`, or `FAILED`.

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
  precisions select `SMALLINT`, `INT`, `BIGINT`, or `DECIMAL`; unconstrained
  `NUMBER` retains the approved `DECIMAL(38,0)` AUTO policy; precision over 38
  remains BLOCKED.
- Oracle `DATE`, `TIMESTAMP`, LOB, JSON, BOOLEAN, VECTOR, interval, spatial, and
  user-defined behavior remains in `OracleTypeMapper` and the Oracle adapter.
  JSON and timezone-sensitive mappings retain their existing review policy;
  VECTOR and unsupported types remain BLOCKED.

### SQL Server mapping and column policy
- SQL Server `decimal`/`numeric` precision and scale, unsigned `tinyint`
  widening, money families, `uniqueidentifier`, CLR/unsupported types, and
  temporal mappings are owned by `SqlServerTypeMapper`.
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
- No third pipeline, no generalized workflow engine, no custom scheduler
  (Databricks Jobs orchestrate).
- No general SQL parser; conversion uses limited, literal/comment-safe textual
  replacements for simple views only. Procedures/functions/packages produce
  non-executable redesign guidance.
- Converted views, procedures, and functions are never executed or deployed
  automatically.
- No Gold layer, no reporting warehouse, no cloud-billing ingestion, no
  automatic cost optimization, no anomaly detection, no ServiceNow/paging.
- DQ rules are the fixed MVP set only; arbitrary SQL expressions are rejected.
- Optional AI conversion requires a pre-approved, configured endpoint; without
  one, routine conversion is `NOT_CONFIGURED` (never fabricated).
- No new source systems beyond Oracle and SQL Server.

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
- Legacy control rows with missing `source_system` no longer run. NB00 skips
  and counts them; an operator must classify each reviewed row explicitly. For
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
