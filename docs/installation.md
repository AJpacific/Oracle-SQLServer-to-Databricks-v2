# Installation and setup

This accelerator runs as two Databricks pipelines (INGEST and ETL) over shared
control/audit tables in Unity Catalog. It has no runtime pip dependencies beyond
what Databricks provides (Spark, `dbutils`) plus the Oracle and Microsoft SQL
Server JDBC drivers on the cluster. Only the pure unit tests use dev tooling.

## 1. Prerequisites

- A Unity Catalog catalog for the control schema and target data (default
  `da_accelerators`). The catalog must already exist.
- Cluster libraries: Oracle JDBC driver (`oracle.jdbc.OracleDriver`) and the
  Microsoft SQL Server JDBC driver (`com.microsoft.sqlserver.jdbc.SQLServerDriver`).
- Permission to create schemas/tables in the target catalog and to read the
  source secret scopes.

## 2. Import the repository

Import this repo as a Databricks Git folder (Repos) so the `src/` package and
the `notebooks/` tree are available. `notebooks/shared/_common.py` discovers the
repo root from the running notebook path (no hard-coded workspace path). If you
run a notebook outside a Git folder, set the `src_path` widget to the absolute
path of the `src/` directory.

Notebook layout:

- `notebooks/shared/` - source-neutral notebooks (bootstrap with `%run ./_common`)
- `notebooks/deployment/` - run context, global worklists, and manual upgrades
- `notebooks/sources/<source>/` - dialect-specific notebooks
  (bootstrap with `%run ../../shared/_common`)

Every notebook exists in exactly one place; job definitions must reference the
`shared/` or `sources/<source>/` path. Verify the relative `%run` paths resolve
after import before running any job.

Datatype mapping follows the same ownership boundary:

- `src/type_mappers/base.py` - source-neutral result and mapper contract
- `src/type_mappers/oracle.py` - Oracle mapping policy
- `src/type_mappers/sqlserver.py` - SQL Server mapping policy
- `src/type_mappers/factory.py` - explicit registrations only
- `src/crosssourcetypemapper.py` - deprecated compatibility facade only

## 3. Create secret scopes (per connection)

Credentials never live in Delta or in Git. Create one Databricks secret scope per
source connection and store only credentials there.

Oracle keys: `oracle-user`, `oracle-password`, and either `oracle-jdbc-url` or
`oracle-host` / `oracle-port` / `oracle-service`.

SQL Server keys: `sqlserver-user`, `sqlserver-password`, and either
`sqlserver-jdbc-url` or `sqlserver-host` / `sqlserver-port`.

SQL Server connections use `encrypt=true` and `trustServerCertificate=false` by
default; enable trust only per connection via `trust_server_certificate=true`.

## 4. Initialize control tables

Run `shared/NB00_ControlTableInit`. It is idempotent and never drops tables. It
creates the control schema, all control/audit tables, the `source_connection`,
`source_assessment`, `sql_object_assessment`, `dq_rule`, `dq_result`, and
`dq_quarantine` tables, and idempotently adds the `connection_id`, identity-v2,
ETL, retry, and reconciliation columns to existing tables.

NB00 never rewrites a legacy `source_table_id`. It reports legacy or inconsistent
ownership and directs operators to the explicit migration. Existing
installations must run
`notebooks/deployment/NB_MigrateSourceTableIdentityV2.py` with `dry_run=true`,
resolve all blockers, and then run it with `dry_run=false` before global Full
Load or Delta workflows. The migration is restart-safe, updates history by
`connection_id + old_source_table_id`, and is not called automatically.
See `docs/source_identity_v2_migration.md` for the full procedure and
verification queries.

Rows with missing ownership metadata are blocked. After reviewing the real
source, repair each row explicitly. Example only:

```sql
UPDATE <catalog>.<control_schema>.source_table_control
SET source_system = 'oracle'
WHERE source_table_id = '<reviewed legacy source_table_id>'
  AND (source_system IS NULL OR trim(source_system) = '');
```

Do not run a broad update and do not infer a source from a schema, database,
table name, or secret key.

## 5. Configure target routing (accelerator_target_config)

`NB00` creates `accelerator_target_config` for automatic target routing during selected-table onboarding.
Automatic resolution uses a 3-tier precedence:
1. **Connection-specific override** (`connection_id` matches)
2. **Source-specific default** (`source_system` matches, `connection_id` is null)
3. **Global default** (both `connection_id` and `source_system` are null)

Target configuration applies **only to new table registrations**. Existing registrations retain their stored target identity permanently (`TARGET_CONFIG_CHANGED` guard). Inactive or non-default historical rows (`is_active = false` or `is_default = false`) do not participate in automatic target routing and do not block NB00 initialization.

Do not seed environment-specific configuration automatically. Insert the required defaults according to deployment policy.

### Selection and Onboarding Lifecycle:
Assessment tables follow the lifecycle: `NOT_SELECTED -> SELECTED -> ONBOARDING -> REGISTERED -> ONBOARDED` (with safe failure state `FAILED`).
1. `NB01B_RegisterSelectedTables` atomically claims eligible rows (`SELECTED -> ONBOARDING`) with run and attempt ownership, registers inactive rows in `source_table_control`, and transitions exact rows to `REGISTERED`. NB01B never sets `ONBOARDED`.
2. Downstream tasks operate on `include_onboarding=True`. After `NB08_TargetProvisioning`, `NB_FinalizeSelectedTableOnboarding` verifies the active `PROVISIONED` registration and marks exact rows `ONBOARDED`.
3. If downstream tasks fail, `NB_MarkSelectedOnboardingFailed` marks owned incomplete rows `FAILED`. Stale rows are never reset automatically; use `NB_RecoverSelectedOnboardingState` for reviewed recovery.
4. All worklists enforce `TASK_VALUE_LIMIT_BYTES = 40_000` bytes (measured in UTF-8 bytes) via `validate_task_value_payload()`.

### Global default:
```sql
INSERT INTO <catalog>.<control_schema>.accelerator_target_config (
    config_id, source_system, connection_id, target_catalog,
    target_schema_mode, target_schema, is_default, is_active,
    created_ts, updated_ts
)
VALUES (
    'GLOBAL_DEFAULT', NULL, NULL, 'migration_dev',
    'SOURCE_SCHEMA', NULL, true, true,
    current_timestamp(), current_timestamp()
);
```

### Oracle default:
```sql
INSERT INTO <catalog>.<control_schema>.accelerator_target_config (
    config_id, source_system, connection_id, target_catalog,
    target_schema_mode, target_schema, is_default, is_active,
    created_ts, updated_ts
)
VALUES (
    'ORACLE_DEFAULT', 'oracle', NULL, 'migration_oracle',
    'SOURCE_SCHEMA', NULL, true, true,
    current_timestamp(), current_timestamp()
);
```

### SQL Server default:
```sql
INSERT INTO <catalog>.<control_schema>.accelerator_target_config (
    config_id, source_system, connection_id, target_catalog,
    target_schema_mode, target_schema, is_default, is_active,
    created_ts, updated_ts
)
VALUES (
    'SQLSERVER_DEFAULT', 'sqlserver', NULL, 'migration_sqlserver',
    'PREFIX_WITH_DATABASE', NULL, true, true,
    current_timestamp(), current_timestamp()
);
```

### Connection-specific override:
```sql
INSERT INTO <catalog>.<control_schema>.accelerator_target_config (
    config_id, source_system, connection_id, target_catalog,
    target_schema_mode, target_schema, is_default, is_active,
    created_ts, updated_ts
)
VALUES (
    'ORA_FINANCE_TARGET', 'oracle', 'ORA_FINANCE', 'finance_migration',
    'EXPLICIT', 'bronze_finance', true, true,
    current_timestamp(), current_timestamp()
);
```

## 6. Onboard a connection

Run the connection notebook for your source:
`sources/oracle/NB00A_UpsertAndValidateConnection` or
`sources/sqlserver/NB00A_UpsertAndValidateConnection`. Supply `connection_id`,
`connection_name`, `secret_scope`, and (for SQL Server) `source_database`.
`source_system` is fixed by the notebook, not a widget. It stores only
non-secret metadata and runs the source's own connectivity probe; on success the
connection becomes `VALID`. Missing, blank, or unregistered `source_system`
values fail; shared routing has no Oracle fallback.

Inventory retries replace the complete snapshot for one
`run_id + connection_id + source_table_id`. Duplicate incoming column keys fail
before any
write. A successful retry removes stale columns from that same run/table while
preserving older runs and unrelated tables. Control status becomes
`INVENTORIED` only after the replacement write succeeds.

## 6. Run the pipelines

See the README for the INGEST and ETL workflow order, and
`docs/databricks_job_task_mapping.md` for the exact job task keys, parameters,
and retry routing. Real job definitions are intentionally not committed:
workspace paths, compute, and schedules are deployment-specific.

## 7. Run the unit tests (optional, local)

```bash
python -m pip install -r requirements-dev.txt   # pytest + PyYAML only
python -m compileall -q src tests
python -m pytest tests -q
python -m unittest discover -s tests -v
```

The tests are pure Python (no Spark/JDBC). Do not install Ruff, yamllint,
gitleaks, or other blocked tooling.

Captured output belongs under `artifacts/test-results/` in
`compileall-output.txt`, `pytest-output.txt`, `unittest-output.txt`, and
`test-summary.json`. These results validate repository logic and static wiring
only. Record live Spark, Delta, Oracle, SQL Server, and Databricks Job evidence
separately in `docs/production_readiness_checklist.md`; required live checks
default to `NOT_EXECUTED` and cannot be inferred from unit tests.
