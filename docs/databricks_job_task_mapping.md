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

- No secret is ever passed as a task parameter. Normal Full Load and Delta
  ForEach items contain only `run_id`, `connection_id`, and `source_table_id`.
- Every registered and operational row must carry a nonblank, registered
  `source_system`. A missing or unknown value fails and is never routed to a
  default source.
- Every task in one workflow receives the **same** `run_id`, passed explicitly
  as a task parameter (`get_run_id()` prefers the widget over a task value).
- Onboarding tasks receive one required `connection_id`; operational worklist
  tasks discover eligible registrations globally and may be narrowed by
  `only_connection_ids` or `only_source_table_ids`.
- A retry workflow receives a **child** `run_id` plus the `parent_run_id` of the
  original run, and the original frozen watermark bounds.
- The ETL workflow receives `connection_id` only as ownership lineage. It never
  receives a source secret scope or uses the connection for source access.
- Every notebook exists in exactly one place (`shared/` or
  `sources/<source>/`); there are no wrapper copies, so tasks must reference
  those paths directly.

---

## Job 1A - Automated Connection Validation and Source Assessment Workflow (Target Architecture)

Job 1A orchestrates end-to-end connection validation and assessment for an internally fixed `source_system` parameter (`oracle` or `sqlserver`). Connection metadata is maintained authoritatively in `da_accelerators.control.source_connection` and populated by an operator or trusted configuration process prior to execution. Job 1A never passes connection metadata (server, database, secret scope, or TLS settings) or credentials through Job parameters; it passes only `run_id`, `connection_id`, `catalog`, and `control_schema`.

### Operator Contract

**SQL Server:**
- `source_database` populated: assess only the configured database.
- `source_database` blank (NULL, empty, or whitespace): discover and assess all accessible online non-system databases using the registered SQL Server server and credentials.

**Oracle:**
- Database/service behavior remains unchanged. Oracle always requires its configured database/service value; multi-database discovery is not supported for Oracle.

**Both Oracle and SQL Server:**
- `include_schemas` (optional Job 1A parameter, default `""`): blank means all accessible schemas; populated limits assessment to those schemas.
- `exclude_schemas` (optional Job 1A parameter, default `""`): populated removes those schemas from the discovered or included set.
- Neither include nor exclude schema columns are added to `source_connection`.

**Operational Guarantees:**
- System SQL Server databases (`master`, `model`, `msdb`, `tempdb`) are strictly excluded.
- Credentials and TLS settings are reused through the same parent secret-scope reference for every discovered database.
- Discovered databases are **never** written back into the parent `source_connection.source_database` row; the registered connection row remains blank.
- `source_database` remains an explicit part of object identity across assessment and SQL-object inventories (`connection_id`, `source_database`, `source_schema`, `object_name`), Delta MERGE keys, and artifact volume paths (`<connection_id>/<source_database>/<source_schema>/<object_type>/<object_name>.sql`).
- Multi-database discovery applies only to Job 1A assessment. Downstream onboarding (Job 1B, Full Load, Delta Sync) continues to require database-qualified assessment and registration identities.

No notification task (`NB16_NotifyFailures`) is included in Job 1A.

### Oracle Job 1A Workflow

```text
T00 Create Run Context
    |
    v
T01 Initialize Control Tables
    |
    v
T02 Get CONFIGURED Connection Worklist
    |
    v
T03 ForEach Validate Existing Connection (sources/oracle/NB00A)
    |
    v
T04 Get VALID Connection Worklist
    |
    +---------------------------------------+
    |                                       |
    v                                       v
T05 ForEach Assessment (NB01A)        T06 ForEach SQL Object (NB13)
    |                                       |
    +-------------------+-------------------+
                        |
                        v
              T07 Assessment Summary
```

| Task key | Notebook | Parameters | Output |
|---|---|---|---|
| `T00_Create_Run_Context` | `deployment/NB_CreateRunContext` | optional `run_id`, `run_prefix` | `run_id` only |
| `T01_Init_Control` | `shared/NB00_ControlTableInit` | `run_id` | `run_id` |
| `T02_Get_Configured_Connection_Worklist` | `deployment/NB_GetConnectionWorklist` | `run_id`, `source_system: oracle`, `connection_mode: CONFIGURED`, optional `max_connections`, optional `only_connection_ids`, optional `exclude_connection_ids` | `worklist`, `worklist_count` |
| `T03_ForEach_Validate_Connection` | `sources/oracle/NB00A_UpsertAndValidateConnection` (For Each `{{tasks.T02_Get_Configured_Connection_Worklist.values.worklist}}`) | `run_id: {{tasks.T00_Create_Run_Context.values.run_id}}`, `connection_id: {{input.connection_id}}`, `catalog: da_accelerators`, `control_schema: control` | `status`, `connection_status`, `run_id`, `connection_id`, `source_system`, `source_database` |
| `T04_Get_Valid_Connection_Worklist` | `deployment/NB_GetConnectionWorklist` (depends_on: `T03_ForEach_Validate_Connection`) | `run_id`, `source_system: oracle`, `connection_mode: VALID`, optional `max_connections`, optional `only_connection_ids`, optional `exclude_connection_ids` | `worklist`, `worklist_count` |
| `T05_ForEach_Connection_Assessment` | `sources/oracle/NB01A_SourceAssessment` (For Each `{{tasks.T04_Get_Valid_Connection_Worklist.values.worklist}}`) | `run_id`, `connection_id: {{input.connection_id}}`, `catalog: da_accelerators`, `control_schema: control`, `include_schemas: {{job.parameters.include_schemas}}`, `exclude_schemas: {{job.parameters.exclude_schemas}}` | `assessment_id`, `objects`, `summary` |
| `T06_ForEach_Source_SQL_Object_Inventory` | `sources/oracle/NB13_SQLObjectAssessmentAndConversion` (For Each `{{tasks.T04_Get_Valid_Connection_Worklist.values.worklist}}`) | `run_id`, `connection_id: {{input.connection_id}}`, `catalog: da_accelerators`, `control_schema: control`, `include_object_types: VIEW,PROCEDURE,FUNCTION,PACKAGE`, `include_schemas: {{job.parameters.include_schemas}}`, `exclude_schemas: {{job.parameters.exclude_schemas}}` | `assessment_id`, `objects`, `summary` |
| `T07_Assessment_Summary` | `deployment/NB_AssessmentSummary` (run_if: ALL_DONE, depends_on: `T05_ForEach_Connection_Assessment`, `T06_ForEach_Source_SQL_Object_Inventory`) | `run_id`, `source_system: oracle` | `connections_assessed`, `databases_assessed`, `assessments`, `objects_assessed`, `selected_table_count`, `business_status` |

### SQL Server Job 1A Workflow (Multi-Database Discovery)

```text
T00 Create Run Context
    |
    v
T01 Initialize Control Tables
    |
    v
T02 Get CONFIGURED Connection Worklist
    |
    v
T03 ForEach Validate Existing Connection (sources/sqlserver/NB00A)
    |
    v
T04 Get VALID Connection Worklist
    |
    v
T04a Get Assessment Database Worklist (deployment/NB_GetAssessmentDatabaseWorklist)
    |
    +---------------------------------------+
    |                                       |
    v                                       v
T05 ForEach Assessment (NB01A)        T06 ForEach SQL Object (NB13)
    |                                       |
    +-------------------+-------------------+
                        |
                        v
              T07 Assessment Summary
```

| Task key | Notebook | Parameters | Output |
|---|---|---|---|
| `T00_Create_Run_Context` | `deployment/NB_CreateRunContext` | optional `run_id`, `run_prefix` | `run_id` only |
| `T01_Init_Control` | `shared/NB00_ControlTableInit` | `run_id` | `run_id` |
| `T02_Get_Configured_Connection_Worklist` | `deployment/NB_GetConnectionWorklist` | `run_id`, `source_system: sqlserver`, `connection_mode: CONFIGURED`, optional `max_connections`, optional `only_connection_ids`, optional `exclude_connection_ids` | `worklist`, `worklist_count` |
| `T03_ForEach_Validate_Connection` | `sources/sqlserver/NB00A_UpsertAndValidateConnection` (For Each `{{tasks.T02_Get_Configured_Connection_Worklist.values.worklist}}`) | `run_id: {{tasks.T00_Create_Run_Context.values.run_id}}`, `connection_id: {{input.connection_id}}`, `catalog: da_accelerators`, `control_schema: control` | `status`, `connection_status`, `run_id`, `connection_id`, `source_system`, `source_database` |
| `T04_Get_Valid_Connection_Worklist` | `deployment/NB_GetConnectionWorklist` (depends_on: `T03_ForEach_Validate_Connection`) | `run_id`, `source_system: sqlserver`, `connection_mode: VALID`, optional `max_connections`, optional `only_connection_ids`, optional `exclude_connection_ids` | `worklist`, `worklist_count` |
| `T04a_Get_Assessment_Database_Worklist` | `deployment/NB_GetAssessmentDatabaseWorklist` (depends_on: `T04_Get_Valid_Connection_Worklist`) | `run_id`, `catalog: da_accelerators`, `control_schema: control`, optional `max_connections`, optional `only_connection_ids`, optional `exclude_connection_ids` | `worklist`, `worklist_count`, `connections_processed`, `databases_emitted`, `business_status` |
| `T05_ForEach_Connection_Assessment` | `sources/sqlserver/NB01A_SourceAssessment` (For Each `{{tasks.T04a_Get_Assessment_Database_Worklist.values.worklist}}`) | `run_id`, `connection_id: {{input.connection_id}}`, `source_database: {{input.source_database}}`, `catalog: da_accelerators`, `control_schema: control`, `include_schemas: {{job.parameters.include_schemas}}`, `exclude_schemas: {{job.parameters.exclude_schemas}}` | `assessment_id`, `objects`, `summary` |
| `T06_ForEach_Source_SQL_Object_Inventory` | `sources/sqlserver/NB13_SQLObjectAssessmentAndConversion` (For Each `{{tasks.T04a_Get_Assessment_Database_Worklist.values.worklist}}`) | `run_id`, `connection_id: {{input.connection_id}}`, `source_database: {{input.source_database}}`, `catalog: da_accelerators`, `control_schema: control`, `include_object_types: VIEW,PROCEDURE,FUNCTION`, `include_schemas: {{job.parameters.include_schemas}}`, `exclude_schemas: {{job.parameters.exclude_schemas}}` | `assessment_id`, `objects`, `summary` |
| `T07_Assessment_Summary` | `deployment/NB_AssessmentSummary` (run_if: ALL_DONE, depends_on: `T05_ForEach_Connection_Assessment`, `T06_ForEach_Source_SQL_Object_Inventory`) | `run_id`, `source_system: sqlserver` | `connections_assessed`, `databases_assessed`, `assessments`, `objects_assessed`, `selected_table_count`, `business_status` |

*Note on orchestration:*
- `source_connection.is_active` is the authoritative operator-controlled switch for Job 1A consideration:
  - `is_active = true`: Connection is considered for validation and processing across all statuses (`REGISTERED`, `VALID`, `FAILED`).
  - `is_active = false` or `is_active IS NULL`: Job 1A completely ignores the connection. It does not appear in CONFIGURED or VALID connection worklists, is not validated, does not enter SQL Server database discovery, and does not enter assessment or SQL-object extraction.
- Connection registry validation vs. operational table validation:
  - In `NB00_ControlTableInit` (`ACTIVE_CONNECTION_INVALID_STATUS`): Active connection registry rows may have `REGISTERED`, `VALID`, or `FAILED` status so Job 1A can perform initial validation and retries. An active connection with a NULL, blank, or unsupported status fails initialization. Inactive connections (`is_active = false` or `NULL`) are ignored by this check.
  - In operational table validation (`ACTIVE_TABLE_INVALID_CONNECTION`): Active operational `source_table_control` rows under an active parent connection strictly require a `VALID` parent connection with a nonblank secret scope (`is_active = true AND connection_status = 'VALID' AND secret_scope IS NOT NULL`). An inactive parent connection (`is_active = false`) with active child tables represents an intentional parked state and is not flagged as structural corruption.
- `connection_status` records the result of the latest validation attempt (`REGISTERED`, `VALID`, `FAILED`), while `is_active` controls operator eligibility.
- Validation outcome transitions:
  - Validation success: `connection_status = 'VALID'`, `is_active = true`, `error_message = NULL`, `last_validated_ts = current_timestamp()`, `updated_ts = current_timestamp()`.
  - Validation failure: `connection_status = 'FAILED'`, `is_active = false`, `error_message = <sanitized failure reason>`, `updated_ts = current_timestamp()`.
- Operator retry procedure after validation failure:
  Because validation failure sets `is_active = false`, Job 1A does not automatically retry failed inactive connections. An operator must explicitly re-enable the connection after correcting metadata, network, TLS, or secret settings:
  ```sql
  UPDATE <catalog>.<control_schema>.source_connection
  SET
      is_active = true,
      connection_status = 'REGISTERED',
      error_message = NULL,
      last_validated_ts = NULL,
      updated_ts = current_timestamp()
  WHERE connection_id = '<connection_id>';
  ```
- `source_connection` is pre-populated by an operator or trusted configuration process; NB00A validates an existing row and never inserts or updates connection configuration metadata.
- For SQL Server connections with a blank `source_database`, NB00A connects through `master` temporarily for credential and connectivity validation, but never stores `master` in `source_connection.source_database`.
- Job 1A passes only `connection_id` to NB00A; no metadata or secrets are passed through Job parameters.
- `source_system` is an internal fixed task parameter in Job YAML (`oracle` or `sqlserver`), not a user-entered runtime parameter.
- Configured and valid connection worklists emit strictly `[{"connection_id": "..."}]`.
- For SQL Server, `T04a_Get_Assessment_Database_Worklist` defensively requires `coalesce(is_active, false) = true AND connection_status = 'VALID'`. When `source_database` is populated, it emits 1 work item without querying sys.databases. When blank, it discovers all accessible online non-system databases via a temporary `master` bootstrap connection and emits one item per database.
- Assessment (`T05`) and SQL Object extraction (`T06`) run concurrently for each database work item and pass `source_database` explicitly.
- Original non-table SQL object definitions (Views, Procedures, Functions, Packages, Package Bodies) extracted by `NB13` are stored in `da_accelerators.control.sql_object_assessment`.
- `NB18_MaterializeSourceArtifacts` materializes these exact raw source definitions into governed Unity Catalog Volumes (`_source_artifacts`) under `/Volumes/<target_catalog>/<target_schema>/_source_artifacts/<safe_connection_id>/<safe_source_database>/<safe_source_schema>/<type_directory>/<safe_object_name>.sql`.
- Raw source definitions are stored unchanged. They are never converted, rewritten, executed, or deployed.
- No notification task is included.

---

## Operator Selection Procedure (Between Job 1A and Job 1B)

Between Job 1A and Job 1B, the operator reviews persisted assessment rows in `<catalog>.<control_schema>.source_assessment` and selects required tables for onboarding.

### Selection by schemas:
```sql
UPDATE <catalog>.<control_schema>.source_assessment
SET is_selected = true,
    selection_status = 'SELECTED',
    selected_ts = current_timestamp()
WHERE connection_id = '<connection_id>'
  AND assessment_id = '<assessment_id>'
  AND object_type = 'TABLE'
  AND source_schema IN ('HR', 'SALES')
  AND compatibility_status IN ('COMPATIBLE', 'REVIEW')
  AND coalesce(selection_status, 'NOT_SELECTED')
      IN ('NOT_SELECTED', 'SELECTED');
```

### Selection by explicit tables:
```sql
UPDATE <catalog>.<control_schema>.source_assessment
SET is_selected = true,
    selection_status = 'SELECTED',
    selected_ts = current_timestamp()
WHERE connection_id = '<connection_id>'
  AND assessment_id = '<assessment_id>'
  AND object_type = 'TABLE'
  AND concat(source_schema, '.', object_name) IN (
      'HR.EMPLOYEES',
      'HR.DEPARTMENTS',
      'SALES.ORDERS'
  )
  AND compatibility_status IN ('COMPATIBLE', 'REVIEW')
  AND coalesce(selection_status, 'NOT_SELECTED')
      IN ('NOT_SELECTED', 'SELECTED');
```

### Deselection before onboarding:
```sql
UPDATE <catalog>.<control_schema>.source_assessment
SET is_selected = false,
    selection_status = 'NOT_SELECTED',
    selected_ts = NULL
WHERE connection_id = '<connection_id>'
  AND assessment_id = '<assessment_id>'
  AND source_schema = 'HR'
  AND object_type = 'TABLE'
  AND object_name = 'AUDIT_LOG'
  AND coalesce(selection_status, 'NOT_SELECTED')
      IN ('NOT_SELECTED', 'SELECTED');
```

- Selection occurs after Job 1A and before Job 1B.
- Never modify rows while `selection_status = 'ONBOARDING'`.
- Deselection does not deactivate already `ONBOARDED` tables in `source_table_control`.
- Overlapping selected tables across multiple assessment IDs must be deselected before Job 1B.
- Stale `ONBOARDING` rows (e.g. from an abort or crash) are not reset automatically; operators should investigate the prior run and use `NB_RecoverSelectedOnboardingState` to choose reviewed recovery (`RESUME_REGISTERED`, `MARK_FAILED`, `RESET_TO_SELECTED`, or `FINALIZE_ONBOARDED`).

---

## Job 1B - Automated Selected-Table Onboarding Workflow (Optimized Architecture)

Job 1B automatically discovers selected assessment batches and onboards tables without requiring selected schema, selected table, or target routing parameters. All target routing is resolved from `accelerator_target_config`.

### Execution-Scope Separation:

To eliminate duplicate JDBC metadata queries, duplicate inventory writes, and unnecessary orchestration overhead when a single connection owns multiple assessment batches (e.g., 8 assessment batches under 1 connection with 137 registered tables):
- **Assessment-scoped stages:** Execute per `assessment_id` (T00, T01, T02, T09, T10).
- **Connection-scoped stages:** Execute exactly once per distinct `connection_id` (T02B, T03, T04, T05, T06, T07, T08).
- Concurrency (`concurrency: 3`) is applied after deduplication.
- SQL Server and Oracle Job 1B definitions are managed directly in Databricks and are not deployed from repository YAML.

> [!NOTE]
> Job 1B definitions are maintained directly in Databricks. Export the live Databricks Job configuration before major structural changes if an external backup or review artifact is required.

### Logical Sequence:
```text
J1B_T00_Create_Run_Context
    |
    v
J1B_T01_Get_Selected_Assessments (source_system: sqlserver or oracle)
    |
    v
J1B_T02_Register_Selected_Tables (ForEach assessment worklist from T01, concurrency: 3)
    |
    v
J1B_T02B_Get_Registered_Connections (NB_GetRegisteredConnectionWorklist: deduplicated connection worklist)
    |
    v
J1B_T03_Source_Inventory (ForEach connection worklist from T02B, concurrency: 3)
    |
    v
J1B_T04_Type_Normalization (ForEach connection worklist from T02B, concurrency: 3)
    |
    v
J1B_T05_Mapping_Generation (ForEach connection worklist from T02B, concurrency: 3)
    |
    v
J1B_T06_Mapping_Validation (ForEach connection worklist from T02B, concurrency: 3)
    |
    v
J1B_T07_Table_Decision (ForEach connection worklist from T02B, concurrency: 3)
    |
    v
J1B_T08_Target_Provisioning (ForEach connection worklist from T02B, concurrency: 3)
    |
    +-----------------------------------------------+
    |                                               |
    v                                               v
J1B_T09_Finalize_Onboarding            J1B_T10_Mark_Downstream_Failure
(ForEach original assessment worklist, (ForEach original assessment worklist,
 concurrency: 3)                       concurrency: 3, run_if: AT_LEAST_ONE_FAILED)
```

| Task key | Execution Scope | Notebook | Inputs / Parameters | Output Task Values |
|---|---|---|---|---|
| `J1B_T00_Create_Run_Context` | Run-level | `deployment/NB_CreateRunContext` | `run_id`, `run_prefix`, `catalog`, `control_schema` | `run_id` |
| `J1B_T01_Get_Selected_<Source>_Assessments` | Assessment | `deployment/NB_GetSelectedAssessmentWorklist` | `run_id`, `source_system`, `catalog`, `control_schema`, `max_batches`, `only_connection_ids`, `only_assessment_ids`, `exclude_connection_ids`, `include_failed_retries` | `worklist`, `worklist_count`, `selected_table_count`, `connection_count` |
| `J1B_T02_Register_Selected_Tables` | Assessment (ForEach `T01.worklist`, concurrency: 3) | `shared/NB01B_RegisterSelectedTables` | `run_id`, `connection_id: {{input.connection_id}}`, `assessment_id: {{input.assessment_id}}`, `selection_mode: ASSESSMENT_FLAGS`, `catalog`, `control_schema`, `include_failed_retries` | `status`, `business_status`, `selected_count`, `validated_count`, `claimed_count`, `registered_count`, `already_registered_count`, `failed_count`, `skipped_count`, `conflict_count` |
| `J1B_T02B_Get_Registered_<Source>_Connections` | Connection Worklist Generation | `deployment/NB_GetRegisteredConnectionWorklist` | `run_id`, `source_system`, `catalog`, `control_schema`, `only_connection_ids`, `exclude_connection_ids` | `worklist` (`[{"connection_id": "..."}]`), `connection_count`, `registration_owner_count`, `status`, `business_status` |
| `J1B_T03_<Source>_Source_Inventory` | Connection (ForEach `T02B.worklist`, concurrency: 3) | `sources/<source>/NB01_SourceInventory` | `run_id`, `connection_id: {{input.connection_id}}`, `catalog`, `control_schema`, `include_onboarding: true` | `candidate_table_count`, `inventoried_table_count`, `failed_table_count`, `columns_written`, `databases_processed`, `status`, `business_status` |
| `J1B_T04_Type_Normalization` | Connection (ForEach `T02B.worklist`, concurrency: 3) | `shared/NB02_TypeNormalization` | `run_id`, `connection_id: {{input.connection_id}}`, `catalog`, `control_schema` | `status`, `columns` |
| `J1B_T05_Mapping_Generation` | Connection (ForEach `T02B.worklist`, concurrency: 3) | `shared/NB03_MappingRulesGeneration` | `run_id`, `connection_id: {{input.connection_id}}`, `catalog`, `control_schema` | `status`, `columns` |
| `J1B_T06_Mapping_Validation` | Connection (ForEach `T02B.worklist`, concurrency: 3) | `shared/NB04_MappingValidation` | `run_id`, `connection_id: {{input.connection_id}}`, `catalog`, `control_schema` | `status`, `findings` |
| `J1B_T07_Table_Decision` | Connection (ForEach `T02B.worklist`, concurrency: 3) | `shared/NB07_TableDecisionGeneration` | `run_id`, `connection_id: {{input.connection_id}}`, `catalog`, `control_schema` | `status`, `tables` |
| `J1B_T08_Target_Provisioning` | Connection (ForEach `T02B.worklist`, concurrency: 3) | `shared/NB08_TargetProvisioning` | `run_id`, `connection_id: {{input.connection_id}}`, `catalog`, `control_schema` | `status`, `business_status`, `provisioning_candidates`, `provisioned`, `failed` |
| `J1B_T09_Finalize_Onboarding` | Assessment (ForEach `T01.worklist`, concurrency: 3) | `deployment/NB_FinalizeSelectedTableOnboarding` | `run_id`, `connection_id: {{input.connection_id}}`, `assessment_id: {{input.assessment_id}}`, `catalog`, `control_schema` | `status`, `business_status`, `onboarded_count`, `review_required_count`, `blocked_count`, `failed_count` |
| `J1B_T10_Mark_Downstream_Failure` | Assessment (ForEach `T01.worklist`, concurrency: 3, run_if: AT_LEAST_ONE_FAILED) | `deployment/NB_MarkSelectedOnboardingFailed` | `run_id`, `connection_id: {{input.connection_id}}`, `assessment_id: {{input.assessment_id}}`, `failed_stage: FINALIZATION`, `error_message`, `catalog`, `control_schema` | `status`, `business_status`, `target_count`, `failed_count` |

### Orchestration and Parameter Isolation Guarantees:
- **Scope Contract:** T02 may run $N$ times for $N$ assessment IDs under 1 connection, but T03 through T08 execute strictly once for that distinct connection. T09 and T10 return to the original $N$ assessment batches.
- **Candidate Isolation:** Connection-level stages process strictly current-run registered tables (`repo.registered_tables_for_onboarding_run(connection_id, run_id)`), isolating against historical tables owned by the same connection.
- **Metadata Batching:** Inventory stages batch column and primary key JDBC queries at the `(connection_id, source_database)` level, avoiding individual JDBC queries per table.
- **Internal values only:** Tasks pass strictly `run_id`, `connection_id`, and `assessment_id`. No secrets, passwords, JDBC URLs, or user-entered connection parameters are passed across task parameters.
- **Downstream failure recording (`J1B_T10`):** Marks only still-incomplete rows owned by the current `run_id`, preserving already `ONBOARDED` tables and rows owned by other runs.
- **Worklist payload safety:** All worklists enforce `TASK_VALUE_LIMIT_BYTES = 40_000` bytes via `validate_task_value_payload()`. Zero credentials or endpoints are present in emitted worklists.

---

## INGEST - Legacy Onboarding Workflow (Widget-Driven Backward Compatibility)

| Task key | Notebook | Parameters | Output |
|---|---|---|---|
| `T00_Init_Control` | `shared/NB00_ControlTableInit` | - | `run_id` |
| `T01_Validate_Oracle_Connection` *or* `T01_Validate_SQLServer_Connection` | `sources/oracle/NB00A_UpsertAndValidateConnection` *or* `sources/sqlserver/NB00A_UpsertAndValidateConnection` | `run_id`, `connection_id`, `catalog`, `control_schema` | `status`, `connection_status`, `run_id`, `connection_id`, `source_system`, `source_database` |
| `T02_Oracle_Source_Assessment` *or* `T02_SQLServer_Source_Assessment` | `sources/oracle/NB01A_SourceAssessment` *or* `sources/sqlserver/NB01A_SourceAssessment` | `run_id`, `connection_id`, `assessment_id`, `include_schemas`, `exclude_schemas`, `include_object_types` | `assessment_id`, `objects`, `summary` |
| `T03_Register_Selected_Tables` | `shared/NB01B_RegisterSelectedTables` | `assessment_id`, `connection_id`, `selected_schemas`, `selected_tables`, `target_catalog`, `target_schema_mode`, `target_schema` | worklist, conflicts |
| `T04_Oracle_Source_Inventory` *or* `T04_SQLServer_Source_Inventory` | `sources/oracle/NB01_SourceInventory` *or* `sources/sqlserver/NB01_SourceInventory` | `run_id`, `connection_id` | tables, columns |
| `T05_Type_Normalization` | `shared/NB02_TypeNormalization` | `run_id`, `connection_id` | - |
| `T06_Mapping` | `shared/NB03_MappingRulesGeneration` | `run_id`, `connection_id` | - |
| `T07_Mapping_Validation` | `shared/NB04_MappingValidation` | `run_id`, `connection_id` | - |
| `T08_Table_Decision` | `shared/NB07_TableDecisionGeneration` | `run_id`, `connection_id` | - |
| `T09_Provision_Bronze` | `shared/NB08_TargetProvisioning` | `run_id`, `connection_id` | - |
| `T10_Get_Auto_Migrate_Worklist` | `deployment/NB_GetFullLoadWorklist` | `run_id`, `catalog`, `control_schema`, `max_tables`, optional `only_connection_ids`, optional `only_source_table_ids` | `worklist`, `worklist_count`, `connection_count` |
| `T11_ForEach_Full_Load` | `shared/NB09_FullLoad` | `connection_id`, `source_table_id`, `run_id`, `attempt_number` | - |
| `T12_Full_Reconciliation` | `shared/NB12_ValidationAndReconciliation` | `run_id`, `mode=full` | - |
| `T13_Commit_Initial_State` | `shared/NB10_PostFullLoadState` | `run_id` | - |
| `T14_Oracle_Source_SQL_Object_Inventory` *or* `T14_SQLServer_Source_SQL_Object_Inventory` | `sources/oracle/NB13_SQLObjectAssessmentAndConversion` *or* `sources/sqlserver/NB13_SQLObjectAssessmentAndConversion` | `run_id`, `connection_id`, `assessment_id`, `include_schemas`, `include_object_types` | `assessment_id`, `objects`, `summary` |
| `T15_Notify_Ingest_Failures` | `shared/NB16_NotifyFailures` (run_if: ALL_DONE) | `run_id`, `pipeline_name=INGEST` | - |

**Orchestration model.** `T05`-`T09` are **bulk** shared metadata tasks: each
processes the selected rows for the run, scoped by `connection_id`. They are
*not* placed inside the per-table ForEach. Only the load stage (`T11`) is
per-table; `shared/NB09_FullLoad` fails if a supplied `source_table_id` does not
resolve to exactly one eligible AUTO_MIGRATE table.

**Onboarding activation flow.** Registration (`T03`) creates candidate table rows
in `source_table_control` as inactive (`is_active = false`, `current_status = 'REGISTERED'`).
Tasks `T04`-`T08` operate across the selected onboarding registrations with
`include_onboarding=True`. In `T09`, `NB08_TargetProvisioning` independently verifies
that no target collision exists against any other registration, provisions the
Delta tables, and activates approved `AUTO_MIGRATE` tables (`is_active = true`,
`current_status = 'PROVISIONED'`). Tables with decisions other than `AUTO_MIGRATE`
(e.g. `MANUAL_REVIEW`, `BLOCKED`) remain inactive (`is_active = false`). Next,
`T10` discovers only active `AUTO_MIGRATE` tables for Full Load. In `T11`,
`NB09_FullLoad` revalidates target ownership before overwriting data, providing
end-to-end protection against target collisions. `T02` assessment notebooks always
publish `assessment_id` as a task value (even on `PARTIAL` business status), allowing
`T03` registration to dynamically receive `{{tasks.T02_Oracle_Source_Assessment.values.assessment_id}}`
(or SQL Server equivalent).

`T04` persists each table independently. Repeating it with the same `run_id`
replaces that table's exact `run_id + connection_id + source_table_id`
inventory snapshot after duplicate-key validation;
it does not append duplicate columns or delete another run/table. A new
`run_id` creates new history.

### Full Load Artifact Materialization Task
After Full Load initial state commit succeeds (`T13_Commit_Initial_State` / `NB10_PostFullLoadState`), run one run-level artifact materialization task:
- Task Key: `J2_T05_Materialize_Source_Artifacts`
- Notebook: `notebooks/shared/NB18_MaterializeSourceArtifacts`
- Parameters: `run_id: {{tasks.J2_T00_Create_Run_Context.values.run_id}}`, `connection_id: ""`, `only_source_system: ""`, `only_assessment_id: ""`, `volume_name: "_source_artifacts"`, `catalog: "da_accelerators"`, `control_schema: "control"`.
- Runs only after `J2_T04_Commit_Initial_State` succeeds. Does not run if reconciliation or state commit failed.

---

## INGEST - recurring synchronization workflow

| Task key | Notebook | Parameters |
|---|---|---|
| `T19_Create_Run_Context` | `deployment/NB_CreateRunContext` | optional `run_id`, `run_prefix` | `run_id` only |
| `T20_Delta_Prep` | `shared/NB11a_DeltaSyncPrep` | `run_id`, optional `only_connection_ids`, optional `only_source_table_ids` |
| `T20B_Get_Delta_Worklist` | `deployment/NB_GetDeltaWorklist` | `run_id`, `catalog`, `control_schema`, `max_tables`, optional `only_connection_ids`, optional `only_source_table_ids` | `worklist`, `worklist_count`, `connection_count` |
| `T21_Delta_Apply` | `shared/NB11b_DeltaSyncApply` | `run_id`, `connection_id`, `source_table_id`, `attempt_number`, `parent_run_id`, `recovery_action` |
| `T22_Delta_Summary` | `shared/NB12_ValidationAndReconciliation` | `run_id`, `mode=delta` |
| `T23_Get_Valid_Oracle_Connections` | `deployment/NB_GetConnectionWorklist` (depends_on: `T22_Delta_Summary`) | `run_id`, `source_system: oracle`, `connection_mode: VALID` |
| `T24_ForEach_Refresh_Oracle_NB13` | `sources/oracle/NB13_SQLObjectAssessmentAndConversion` (For Each `{{tasks.T23_Get_Valid_Oracle_Connections.values.worklist}}`) | `run_id`, `connection_id: {{input.connection_id}}`, `include_object_types: VIEW,PROCEDURE,FUNCTION,PACKAGE` |
| `T25_Get_Valid_SQLServer_Connections` | `deployment/NB_GetConnectionWorklist` (depends_on: `T22_Delta_Summary`) | `run_id`, `source_system: sqlserver`, `connection_mode: VALID` |
| `T26_ForEach_Refresh_SQLServer_NB13` | `sources/sqlserver/NB13_SQLObjectAssessmentAndConversion` (For Each `{{tasks.T25_Get_Valid_SQLServer_Connections.values.worklist}}`) | `run_id`, `connection_id: {{input.connection_id}}`, `include_object_types: VIEW,PROCEDURE,FUNCTION` |
| `T27_Materialize_Source_Artifacts` | `shared/NB18_MaterializeSourceArtifacts` (depends_on: `T24_ForEach_Refresh_Oracle_NB13`, `T26_ForEach_Refresh_SQLServer_NB13`) | `run_id: {{tasks.T19_Create_Run_Context.values.run_id}}`, `connection_id: ""`, `only_source_system: ""`, `only_assessment_id: ""`, `volume_name: "_source_artifacts"`, `catalog: "da_accelerators"`, `control_schema: "control"` |
| `T28_Notify_Delta_Failures` | `shared/NB16_NotifyFailures` (run_if: ALL_DONE) | `run_id`, `pipeline_name=INGEST_DELTA` |

`NB11a` discovers work across active, valid connections and creates adapters
lazily only for registrations it processes. `NB11b` performs extract -> apply ->
reconcile -> checkpoint -> finalize per queue item, so reconciliation always
precedes the checkpoint. Each task resolves exactly one
`run_id + connection_id + source_table_id` queue row.
At the end of Delta Sync, active valid connections are refreshed via NB13 and NB18 materializes updated artifacts idempotently. Changed definitions overwrite the same deterministic file path; unchanged definitions are skipped without rewriting.

---

## ETL - Bronze-to-Silver workflow

| Task key | Notebook | Parameters |
|---|---|---|
| `T30_Get_ETL_Eligible_Tables` | shared query on `source_table_control` | - |
| `T31_ForEach_Bronze_Table` | `shared/NB15_BronzeToSilverETL` | `connection_id`, `source_table_id`, `run_id`, `etl_mode`, `attempt_number`, `quarantine_enabled`, `exclude_quarantine_columns` |
| `T32_Notify_ETL_Failures` | `shared/NB16_NotifyFailures` (run_if: ALL_DONE) | `run_id`, `pipeline_name=ETL` |

`NB15` performs transform -> validate -> quarantine -> reconcile -> ETL checkpoint
in one per-table task. It never builds a source adapter and never reads a source
secret scope.

---

## Retry workflow (either pipeline)

| Task key | Notebook | Parameters |
|---|---|---|
| `T40_Select_Retries` | `shared/NB14_RetryFailedTables` | `pipeline_name`, `original_run_id`, `operation`, `source_table_id`, `max_retries`, `include_non_retryable` |
| `T41_ForEach_Retry` | routed by `recovery_action` | `run_id` (child), `parent_run_id`, `connection_id`, `source_table_id`, `pipeline_name`, `operation`, `previous_attempt_number`, `attempt_number`, `failure_stage`, `error_category`, `recovery_action`, `retry_lower_watermark`, `retry_upper_watermark` |

Retry selection identity is `connection_id + source_table_id + operation`.
NB14 selects the
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
| `MANUAL_REVIEW` | none | - | no automatic execution |

`max_retries` means the number of **additional** attempts allowed after the
first. With `max_retries=3`, an initial `attempt_number=1` may be retried as
attempts 2, 3, and 4; beyond that the operation becomes `MANUAL_REVIEW`.
Attempts and retry limits are evaluated independently per
`connection_id + source_table_id + operation`.

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
operation, connection, source table, and operation.

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

The repository changes do not update Databricks Job YAML. Existing deployments
must separately replace any global `connection_id` run context with the global
Full Load/Delta worklist tasks and pass each three-field item into its ForEach.

---

## Adding a future source (template only - not implemented)

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
