# Databricks job and task mapping

Real Databricks job definitions are **not** committed to this repository:
workspace paths, cluster/compute identifiers, and schedules are
deployment-specific. This document is the authoritative task mapping to build
those jobs against.

Notebook paths below are relative to `notebooks/`. Shared tasks are identical
for every source; source-specific tasks live under `sources/<source>/`. The job
definition chooses which source-specific task to run - no notebook dynamically
invokes another notebook.

Rules that apply to every workflow:

- No secret is ever passed as a task parameter. Downstream tasks receive only
  `connection_id`, `source_table_id`, and `run_id`.
- Every registered and operational row must carry a nonblank, registered
  `source_system`. A missing or unknown value fails and is never routed to a
  default source.
- Every task in one workflow receives the **same** `run_id`, passed explicitly
  as a task parameter (`get_run_id()` prefers the widget over a task value).
- Every shared task from `T03` onward receives `connection_id` where relevant,
  so one connection's onboarding never consumes another connection's rows.
- A retry workflow receives a **child** `run_id` plus the `parent_run_id` of the
  original run, and the original frozen watermark bounds.
- The ETL workflow never receives a source secret scope or a `connection_id`
  used for connectivity.
- Every notebook exists in exactly one place (`shared/` or
  `sources/<source>/`); there are no wrapper copies, so tasks must reference
  those paths directly.

---

## INGEST — onboarding workflow

| Task key | Notebook | Parameters | Output |
|---|---|---|---|
| `T00_Init_Control` | `shared/NB00_ControlTableInit` | – | `run_id` |
| `T01_Validate_Oracle_Connection` *or* `T01_Validate_SQLServer_Connection` | `sources/oracle/NB00A_UpsertAndValidateConnection` *or* `sources/sqlserver/NB00A_UpsertAndValidateConnection` | `run_id`, `connection_id`, `connection_name`, `source_server`, `source_database`, `secret_scope`, `trust_server_certificate` | `status`, `connection_id`, `source_system`, `source_database` |
| `T02_Oracle_Source_Assessment` *or* `T02_SQLServer_Source_Assessment` | `sources/oracle/NB01A_SourceAssessment` *or* `sources/sqlserver/NB01A_SourceAssessment` | `run_id`, `connection_id`, `assessment_id`, `include_schemas`, `exclude_schemas`, `include_object_types` | `assessment_id`, `objects`, `summary` |
| `T03_Register_Selected_Tables` | `shared/NB01B_RegisterSelectedTables` | `assessment_id`, `connection_id`, `selected_schemas`, `selected_tables`, `target_catalog`, `target_schema_mode`, `target_schema` | worklist, conflicts |
| `T04_Oracle_Source_Inventory` *or* `T04_SQLServer_Source_Inventory` | `sources/oracle/NB01_SourceInventory` *or* `sources/sqlserver/NB01_SourceInventory` | `run_id`, `connection_id` | tables, columns |
| `T05_Type_Normalization` | `shared/NB02_TypeNormalization` | `run_id`, `connection_id` | – |
| `T06_Mapping` | `shared/NB03_MappingRulesGeneration` | `run_id`, `connection_id` | – |
| `T07_Mapping_Validation` | `shared/NB04_MappingValidation` | `run_id`, `connection_id` | – |
| `T08_Table_Decision` | `shared/NB07_TableDecisionGeneration` | `run_id`, `connection_id` | – |
| `T09_Provision_Bronze` | `shared/NB08_TargetProvisioning` | `run_id`, `connection_id` | – |
| `T10_Get_Auto_Migrate_Worklist` | shared query on `source_table_control` | `connection_id` | `{connection_id, source_table_id, run_id}` |
| `T11_ForEach_Full_Load` | `shared/NB09_FullLoad` | `connection_id`, `source_table_id`, `run_id`, `attempt_number` | – |
| `T12_Full_Reconciliation` | `shared/NB12_ValidationAndReconciliation` | `run_id`, `mode=full` | – |
| `T13_Commit_Initial_State` | `shared/NB10_PostFullLoadState` | `run_id` | – |
| `T14_Oracle_SQL_Object_Assessment` *or* `T14_SQLServer_SQL_Object_Assessment` | `sources/oracle/NB13_SQLObjectAssessmentAndConversion` *or* `sources/sqlserver/NB13_SQLObjectAssessmentAndConversion` | `run_id`, `connection_id`, `assessment_id`, `mode`, `include_schemas`, `include_object_types`, `use_ai` | `assessment_id`, `objects`, `summary` |
| `T15_Notify_Ingest_Failures` | `shared/NB16_NotifyFailures` (run_if: ALL_DONE) | `run_id`, `pipeline_name=INGEST` | – |

**Orchestration model.** `T05`–`T09` are **bulk** shared metadata tasks: each
processes the selected rows for the run, scoped by `connection_id`. They are
*not* placed inside the per-table ForEach. Only the load stage (`T11`) is
per-table; `shared/NB09_FullLoad` fails if a supplied `source_table_id` does not
resolve to exactly one eligible AUTO_MIGRATE table.

`T04` persists each table independently. Repeating it with the same `run_id`
replaces that table's exact inventory snapshot after duplicate-key validation;
it does not append duplicate columns or delete another run/table. A new
`run_id` creates new history.

---

## INGEST — recurring synchronization workflow

| Task key | Notebook | Parameters |
|---|---|---|
| `T20_Delta_Prep` | `shared/NB11a_DeltaSyncPrep` | `run_id`, `connection_id` (optional scope) |
| `T21_Delta_Apply` | `shared/NB11b_DeltaSyncApply` | `run_id`, `source_table_id`, `attempt_number`, `parent_run_id`, `recovery_action` |
| `T22_Delta_Summary` | `shared/NB12_ValidationAndReconciliation` | `run_id`, `mode=delta` |
| `T23_Notify_Delta_Failures` | `shared/NB16_NotifyFailures` (run_if: ALL_DONE) | `run_id`, `pipeline_name=INGEST_DELTA` |

`NB11b` performs extract → apply → reconcile → checkpoint → finalize per queue
item, so reconciliation always precedes the checkpoint. When `source_table_id`
is supplied the task must resolve exactly one QUEUED row.

---

## ETL — Bronze-to-Silver workflow

| Task key | Notebook | Parameters |
|---|---|---|
| `T30_Get_ETL_Eligible_Tables` | shared query on `source_table_control` | – |
| `T31_ForEach_Bronze_Table` | `shared/NB15_BronzeToSilverETL` | `source_table_id`, `run_id`, `etl_mode`, `attempt_number`, `quarantine_enabled`, `exclude_quarantine_columns` |
| `T32_Notify_ETL_Failures` | `shared/NB16_NotifyFailures` (run_if: ALL_DONE) | `run_id`, `pipeline_name=ETL` |

`NB15` performs transform → validate → quarantine → reconcile → ETL checkpoint
in one per-table task. It never builds a source adapter and never reads a source
secret scope.

---

## Retry workflow (either pipeline)

| Task key | Notebook | Parameters |
|---|---|---|
| `T40_Select_Retries` | `shared/NB14_RetryFailedTables` | `pipeline_name`, `original_run_id`, `operation`, `source_table_id`, `max_retries`, `include_non_retryable` |
| `T41_ForEach_Retry` | routed by `recovery_action` | `run_id` (child), `parent_run_id`, `connection_id`, `source_table_id`, `pipeline_name`, `operation`, `previous_attempt_number`, `attempt_number`, `failure_stage`, `error_category`, `recovery_action`, `retry_lower_watermark`, `retry_upper_watermark` |

Retry selection identity is `source_table_id + operation`. NB14 selects the
latest failed row independently for every operation using the greatest attempt
number, then ended timestamp, started timestamp, and run ID. A table may
therefore produce more than one recovery item when distinct operations failed;
one operation never contributes attempt history to another.

`pipeline_name=INGEST` admits only the documented ingest operations, while
`pipeline_name=ETL` admits only `ETL`, `ETL_FULL`, and `ETL_INCREMENTAL`. A
nonblank `operation` is an exact, case-normalized filter and must belong to the
selected pipeline. Unknown pipelines and pipeline/operation mismatches fail
before a worklist is produced.

Routing for `recovery_action`:

| Recovery action | Target notebook | Reads source? | Reapplies data? |
|---|---|---|---|
| `RETRY_FULL_LOAD` | `shared/NB09_FullLoad` | yes | yes (idempotent overwrite) |
| `RETRY_DELTA_APPLY` | `shared/NB11b_DeltaSyncApply` | yes | yes, but only over the **parent's frozen interval** copied to a child queue row |
| `RETRY_CHECKPOINT_ONLY` | `shared/NB11b_DeltaSyncApply` | **no** | **no** |
| `RETRY_QUEUE_FINALIZATION_ONLY` | `shared/NB11b_DeltaSyncApply` | **no** | **no** |
| `RETRY_ETL` | `shared/NB15_BronzeToSilverETL` | no (Bronze only) | yes, over the **original frozen ETL bounds** |
| `MANUAL_REVIEW` | none | – | no automatic execution |

`max_retries` means the number of **additional** attempts allowed after the
first. With `max_retries=3`, an initial `attempt_number=1` may be retried as
attempts 2, 3, and 4; beyond that the operation becomes `MANUAL_REVIEW`.
Attempts and retry limits are evaluated independently per
`source_table_id + operation`.

NB14 returns two collections. `worklist` contains only executable recovery
actions. `manual_review_items` contains non-retryable, exhausted, unknown, or
otherwise unsafe operation failures and is never routed into the ForEach. The
ForEach routes with both `source_table_id` and `recovery_action` and retains
`operation` for audit and diagnostics.

Both collections are set as task values only when their serialized values fit
the Databricks task-value limit. The complete collections remain in the
notebook result when reasonably sized; no list is silently truncated. For a
larger result, scope NB14 with `operation` or `source_table_id`, or use the same
documented `table_run_log` query pattern keyed by original run, pipeline-owned
operation, source table, and operation.

State-only recoveries (`RETRY_CHECKPOINT_ONLY`,
`RETRY_QUEUE_FINALIZATION_ONLY`) write a child `table_run_log` row with
operation `CHECKPOINT_RECOVERY` / `QUEUE_FINALIZATION_RECOVERY` carrying
`parent_run_id` and `attempt_number`. They never read the source, never MERGE,
and never replace an interval.

---

## Reporting

`shared/NB17_DashboardViews` is run once (or after schema changes) to create
`vw_assessment_summary`, `vw_ingest_status`, `vw_etl_status`, and
`vw_validation_status`. It takes no pipeline parameters.

Optional connection diagnostics use the current modular paths:

```text
sources/oracle/TEST_CONNECTION
sources/sqlserver/TEST_CONNECTION
```

Before release, verify the deployed Job's exact notebook paths, dependencies,
task parameters, ForEach isolation, retry routing, child lineage, retry policy,
and `ALL_DONE` failure-notification conditions. Record actual workspace evidence
in `docs/production_readiness_checklist.md`; repository tests cannot validate a
deployed Databricks Job.

---

## Adding a future source (template only — not implemented)

Adding, for example, PostgreSQL would require:

```text
src/source_adapters/postgresql.py        # new adapter
src/source_adapters/factory.py           # one registration line
src/source_registry.py                   # one manifest entry
src/type_mappers/postgresql.py           # source-owned mapping policy
src/type_mappers/factory.py              # one explicit mapper registration
config/type_rules_postgresql.yaml        # source-qualified rules
notebooks/sources/postgresql/
    NB00A_UpsertAndValidateConnection.py
    NB01_SourceInventory.py
    NB01A_SourceAssessment.py
    NB13_SQLObjectAssessmentAndConversion.py
    TEST_CONNECTION.py
```

Nothing under `notebooks/shared/` changes: Bronze loading, ETL, audit, retry,
reconciliation, dashboards, notifications, and control-table DDL are shared. The
job definition then points `T01`, `T02`, `T04`, and `T14` at the new source
folder. PostgreSQL is **not** implemented in this repository.
