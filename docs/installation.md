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
the `notebooks/` are available. `notebooks/_common.py` discovers the repo root
from the running notebook path (no hard-coded workspace path). If you run a
notebook outside a Git folder, set the `src_path` widget to the absolute path of
the `src/` directory.

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

Run `NB00_ControlTableInit`. It is idempotent and never drops tables. It creates
the control schema, all control/audit tables, the new `source_connection`,
`source_assessment`, `sql_object_assessment`, `dq_rule`, `dq_result`, and
`dq_quarantine` tables, and idempotently adds the `connection_id`, ETL, retry,
and reconciliation columns to existing tables.

## 5. Onboard a connection

Run `NB00A_UpsertAndValidateConnection` with `connection_id`, `connection_name`,
`source_system`, `secret_scope`, and (for SQL Server) `source_database`. It
stores only non-secret metadata and runs a connectivity probe; on success the
connection becomes `VALID`.

## 6. Run the pipelines

See the README for the INGEST and ETL workflow order, and
`docs/databricks_job_task_mapping.md` for the exact job task keys, parameters,
and retry routing. Real job definitions are intentionally not committed:
workspace paths, compute, and schedules are deployment-specific.

## 7. Run the unit tests (optional, local)

```bash
python -m pip install -r requirements-dev.txt   # pytest + PyYAML only
python -m compileall src tests
python -m pytest tests -q
```

The tests are pure Python (no Spark/JDBC). Do not install Ruff, yamllint,
gitleaks, or other blocked tooling.
