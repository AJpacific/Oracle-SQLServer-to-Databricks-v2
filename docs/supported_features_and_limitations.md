# Supported features and limitations

## Supported

### Sources
- Oracle and Microsoft SQL Server only, via shared source adapters.
- Secret-backed JDBC; one Databricks secret scope per registered connection.
- Deterministic five-part `source_table_id` identity.

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
- **SQL Server `SIZE_MB` is a conservative lower bound**, not a verified
  complete reserved size: it sums `total_pages` over allocation units reached
  from each partition through `container_id = partition_id`, which covers
  IN_ROW_DATA and ROW_OVERFLOW_DATA but not LOB_DATA units keyed by an
  allocation-unit id. Validating the complete relationship requires a live SQL
  Server check (see the manual validation checklist); it has not been verified
  by the pure unit tests.
- Source assessment and SQL-object assessment are retry-safe: re-running the
  same `assessment_id` updates its own rows instead of duplicating them, and an
  existing `APPROVED`/`REJECTED` review decision is preserved.
- Assessment-based, collision-safe registration (`NB01B`). New rows are inactive
  (`REGISTERED`); `MANUAL` / `UNABLE_TO_ASSESS` are never auto-activated.
- Full load (`NB09`, overwrite) and delta sync (`NB11a`/`NB11b`) with
  `FULL_LOAD`, `WATERMARK`, `PRIMARY_KEY`, `HYBRID` strategies and frozen
  incremental intervals.
- Correct source-to-Bronze reconciliation performed on the exact work unit
  **before** the checkpoint is committed (extract -> apply -> reconcile ->
  checkpoint -> finalize). `target_count >= source_count` is never an automatic
  pass.
- Failed-ingest classification and retry worklists (`NB14`) with no-reapply
  checkpoint-only / finalization-only recovery. `max_retries` means the number
  of **additional** attempts allowed after the first.
- `table_run_log.status` is normalized to `SUCCEEDED` / `FAILED`; the detailed
  operational state lives on `source_table_control` / `delta_sync_queue`, and
  `failure_stage` + `error_category` carry the precise meaning.
- SQL-object assessment and limited deterministic conversion drafts (`NB13`);
  every generated draft is `PENDING_REVIEW` and is never executed.

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
- SQL Server uses `encrypt=true`; `trustServerCertificate` defaults to `false`.
- Error messages and JDBC URLs are sanitized/redacted before logging.
- The Teams webhook is read only from a secret scope and is never logged.
