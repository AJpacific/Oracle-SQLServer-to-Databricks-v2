# Databricks notebook source
# MAGIC %md
# MAGIC # SQL Server / NB00A_UpsertAndValidateConnection
# MAGIC INGEST task that validates an existing registered **SQL Server** source connection.
# MAGIC `source_system` is fixed by this notebook, not accepted as a widget.
# MAGIC The registered `source_connection` record is the sole authority for connection
# MAGIC metadata; this notebook does not accept metadata widgets, does not insert or
# MAGIC upsert rows, and does not mutate connection configuration.
# MAGIC It opens the real JDBC connection through the SQL Server adapter (credentials stay
# MAGIC in the connection's secret scope) and runs the SQL Server connectivity probe.
# MAGIC On successful probe, connection status is updated to VALID (is_active = True).
# MAGIC On failure, status is updated to FAILED (is_active = False) with sanitized error.
# MAGIC It never stores, returns, or prints a credential or a JDBC URL.

# COMMAND ----------

# MAGIC %run ../../shared/_common

# COMMAND ----------

SOURCE_SYSTEM = "sqlserver"

run_id = get_run_id()
connection_id = require_connection_id(
    CONNECTION_ID, "SQL Server connection validation"
)
repo = control_repo()

connection = repo.get_connection(connection_id)
if connection is None:
    raise ValueError(f"connection_id '{connection_id}' was not found in source_connection")

connection_data = connection.asDict() if hasattr(connection, "asDict") else dict(connection)

conn_system = require_source_system(
    connection_data.get("source_system"), "SQL Server connection validation"
)
assert_source_system_match(SOURCE_SYSTEM, conn_system)

# COMMAND ----------

status = "FAILED"
try:
    if not (connection_data.get("source_server") or "").strip():
        raise ValueError(f"connection {connection_id!r} has no source_server")

    adapter = get_source_adapter_for_connection(connection, require_valid=False)
    probe_connection(adapter, source_server=connection_data.get("source_server"),
                     source_database=connection_data.get("source_database"))
    repo.update_connection_status(connection_id, "VALID", None)
    status = "VALID"
    print(f"Connection {connection_id} validated: VALID")
except Exception as e:
    # Sanitized before printing and before persistence, even when adapter
    # construction itself failed (so no adapter instance is required here).
    safe = failcls.sanitize_message(e)
    repo.update_connection_status(connection_id, "FAILED", safe[:1000])
    print(f"Connection {connection_id} validation FAILED: {safe[:300]}")
    raise

# COMMAND ----------

set_task_value("run_id", run_id)
set_task_value("connection_id", connection_id)
set_task_value("source_system", SOURCE_SYSTEM)
set_task_value("status", status)
set_task_value("connection_status", status)
dbutils.notebook.exit(json.dumps({
    "status": status,
    "connection_status": status,
    "run_id": run_id,
    "connection_id": connection_id,
    "source_system": SOURCE_SYSTEM,
    "source_database": connection_data.get("source_database"),
}))
