# Supported features and limitations

## Supported

### Sources
- Oracle and Microsoft SQL Server only, via shared source adapters.
- Secret-backed JDBC; one Databricks secret scope per registered connection.
- Deterministic five-part `source_table_id` identity.

### INGEST pipeline (source -> Bronze)
- Pipeline-driven multi-connection onboarding (`source_connection` registry,
  `NB00A`). Only non-secret metadata is stored.
- Broad source assessment (`NB01A`) using data-dictionary / catalog views:
  schemas, tables, views, procedures, functions, packages. Row counts are
  labelled `EXACT` (SQL Server), `ESTIMATED` (Oracle), or `UNAVAILABLE`. No
  per-table `COUNT(*)` during a broad assessment.
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
  checkpoint-only / finalization-only recovery.
- SQL-object assessment and limited deterministic conversion drafts (`NB13`);
  every generated draft is `PENDING_REVIEW` and is never executed.

### ETL pipeline (Bronze -> Silver)
- Processes only successfully ingested Bronze tables; never connects to a source
  or reads a source secret.
- MVP data-quality rules only: `NOT_NULL`, `DUPLICATE_KEY`, `DATA_TYPE`,
  `ALLOWED_VALUES`, `DEFAULT_VALUE`, `TRIM_STRING`, `STANDARDIZE_CASE`.
- Valid rows to Silver; rejected rows to `dq_quarantine` (with column
  redaction). Bronze-to-Silver reconciliation before a **separate** ETL
  checkpoint. Retry-safe FULL overwrite, MERGE, and interval replacement.

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
