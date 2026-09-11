# Databricks job and task mapping

Real Databricks job definitions are **not** committed to this repository:
workspace paths, cluster/compute identifiers, and schedules are
deployment-specific. This document is the authoritative task mapping to build
those jobs against.

Rules that apply to every workflow below:

- No secret is ever passed as a task parameter. Downstream tasks receive only
  `connection_id`, `source_table_id`, and `run_id`.
- Every task in one workflow receives the **same** `run_id`. Pass it explicitly
  as a task parameter; `get_run_id()` prefers the widget over a task value.
- A retry workflow receives a **child** `run_id` plus the `parent_run_id` of the
  original run.
- The ETL workflow never receives a source secret scope or a `connection_id`
  used for connectivity (only for reporting).

---

## INGEST — onboarding workflow

| Task key | Notebook | Parameters | Output |
|---|---|---|---|
| `T00_Init_Control` | `NB00_ControlTableInit` | – | `run_id` |
| `T01_Upsert_Validate_Connection` | `NB00A_UpsertAndValidateConnection` | `run_id`, `connection_id`, `connection_name`, `source_system`, `source_server`, `source_database`, `secret_scope`, `trust_server_certificate` | `connection_id`, `status` |
| `T02_Source_Assessment` | `NB01A_SourceAssessment` | `run_id`, `connection_id`, `include_schemas`, `exclude_schemas`, `include_object_types` | `assessment_id` |
| `T03_Register_Selected_Tables` | `NB01B_RegisterSelectedTables` | `assessment_id`, `connection_id`, `selected_schemas`, `selected_tables`, `target_catalog`, `target_schema_mode`, `target_schema` | worklist, conflicts |
| `T04_Source_Inventory` | `NB01_SourceInventory` | `run_id`, `connection_id` | – |
| `T05_Type_Normalization` | `NB02_TypeNormalization` | `run_id` | – |
| `T06_Mapping` | `NB03_MappingRulesGeneration` | `run_id` | – |
| `T07_Mapping_Validation` | `NB04_MappingValidation` | `run_id` | – |
| `T08_Table_Decision` | `NB07_TableDecisionGeneration` | `run_id` | – |
| `T09_Provision_Bronze` | `NB08_TargetProvisioning` | `run_id` | – |
| `T10_Get_Auto_Migrate_Worklist` | query `source_table_control` | `connection_id` | list of `{connection_id, source_table_id, run_id}` |
| `T11_ForEach_Full_Load` | `NB09_FullLoad` | `connection_id`, `source_table_id`, `run_id`, `attempt_number` | – |
| `T12_Full_Reconciliation` | `NB12_ValidationAndReconciliation` | `run_id`, `mode=full` | – |
| `T13_Commit_Initial_State` | `NB10_PostFullLoadState` | `run_id` | – |
| `T14_SQL_Object_Assessment` | `NB13_SQLObjectAssessmentAndConversion` | `run_id`, `connection_id`, `mode` | `assessment_id` |
| `T15_Notify_Ingest_Failures` | `NB16_NotifyFailures` | `run_id`, `pipeline_name=INGEST` (run_if: ALL_DONE) | – |

**Orchestration model.** `T04`–`T09` are **bulk** metadata tasks: each one
processes the selected active tables for the run, scoped by `connection_id` when
supplied. They are *not* placed inside the per-table ForEach. Only the load
stage (`T11`) is per-table. `NB09_FullLoad` fails if a supplied
`source_table_id` does not resolve to exactly one eligible AUTO_MIGRATE table.

---

## INGEST — recurring synchronization workflow

| Task key | Notebook | Parameters |
|---|---|---|
| `T20_Delta_Prep` | `NB11a_DeltaSyncPrep` | `run_id`, `connection_id` (optional scope) |
| `T21_Delta_Apply` | `NB11b_DeltaSyncApply` | `run_id`, `attempt_number`, `parent_run_id`, `recovery_action` |
| `T22_Delta_Summary` | `NB12_ValidationAndReconciliation` | `run_id`, `mode=delta` |
| `T23_Notify_Delta_Failures` | `NB16_NotifyFailures` | `run_id`, `pipeline_name=INGEST_DELTA` |

`NB11b` performs extract → apply → reconcile → checkpoint → finalize per queue
item, so reconciliation always precedes the checkpoint.

---

## ETL — Bronze-to-Silver workflow

| Task key | Notebook | Parameters |
|---|---|---|
| `T30_Get_ETL_Eligible_Tables` | query `source_table_control` | – |
| `T31_ForEach_Bronze_Table` | `NB15_BronzeToSilverETL` | `source_table_id`, `run_id`, `etl_mode`, `attempt_number`, `quarantine_enabled`, `exclude_quarantine_columns` |
| `T32_Notify_ETL_Failures` | `NB16_NotifyFailures` | `run_id`, `pipeline_name=ETL` |

`NB15` performs transform → validate → quarantine → reconcile → ETL checkpoint
in one per-table task. It never builds a source adapter and never reads a source
secret scope.

---

## Retry workflow (either pipeline)

| Task key | Notebook | Parameters |
|---|---|---|
| `T40_Select_Retries` | `NB14_RetryFailedTables` | `pipeline_name`, `original_run_id`, `operation`, `source_table_id`, `max_retries`, `include_non_retryable` |
| `T41_ForEach_Retry` | routed by `recovery_action` | `run_id` (child), `parent_run_id`, `connection_id`, `source_table_id`, `attempt_number`, `recovery_action` |

Routing for `recovery_action`:

| Recovery action | Target notebook | Reapplies data? |
|---|---|---|
| `RETRY_FULL_LOAD` | `NB09_FullLoad` | yes (idempotent overwrite) |
| `RETRY_DELTA_APPLY` | `NB11b_DeltaSyncApply` | yes (frozen interval / MERGE) |
| `RETRY_CHECKPOINT_ONLY` | `NB11b_DeltaSyncApply` | **no** |
| `RETRY_QUEUE_FINALIZATION_ONLY` | `NB11b_DeltaSyncApply` | **no** |
| `RETRY_ETL` | `NB15_BronzeToSilverETL` | yes (retry-safe) |
| `MANUAL_REVIEW` | none | no automatic execution |

`max_retries` means the number of **additional** attempts allowed after the
first. With `max_retries=3`, an initial `attempt_number=1` may be retried as
attempts 2, 3, and 4; beyond that the item becomes `MANUAL_REVIEW`.

---

## Reporting

`NB17_DashboardViews` is run once (or after schema changes) to create
`vw_assessment_summary`, `vw_ingest_status`, `vw_etl_status`, and
`vw_validation_status`. It takes no pipeline parameters.
