# Databricks notebook source
# MAGIC %md
# MAGIC # SQL Server / NB00A_UpsertAndValidateConnection
# MAGIC INGEST task that onboards one **SQL Server** source connection.
# MAGIC `source_system` is fixed by this notebook, not accepted as a widget. It
# MAGIC upserts only non-secret metadata into `source_connection`, then opens the
# MAGIC real JDBC connection through the SQL Server adapter (credentials stay in
# MAGIC the connection's secret scope) and runs the SQL Server connectivity probe.
# MAGIC It never stores, returns, or prints a credential or a JDBC URL.

# COMMAND ----------

# MAGIC %run ../../shared/_common

# COMMAND ----------

SOURCE_SYSTEM = "sqlserver"

dbutils.widgets.text("connection_id", "")
dbutils.widgets.text("connection_name", "")
dbutils.widgets.text("source_server", "")
dbutils.widgets.text("source_database", "")
dbutils.widgets.text("secret_scope", "")
dbutils.widgets.dropdown("trust_server_certificate", "false", ["true", "false"])

run_id = get_run_id()
repo = control_repo()

# normalize_connection_input enforces that SQL Server supplies source_database.
clean = normalize_connection_input({
    "connection_id": dbutils.widgets.get("connection_id"),
    "connection_name": dbutils.widgets.get("connection_name"),
    "source_system": SOURCE_SYSTEM,
    "source_server": dbutils.widgets.get("source_server"),
    "source_database": dbutils.widgets.get("source_database"),
    "secret_scope": dbutils.widgets.get("secret_scope"),
    "trust_server_certificate":
        dbutils.widgets.get("trust_server_certificate").strip() == "true",
})
connection_id = clean["connection_id"]

# COMMAND ----------

repo.upsert_connection({**clean, "connection_status": "REGISTERED",
                        "is_active": True, "error_message": None})
print(f"Upserted SQL Server connection {connection_id} "
      f"(database={clean['source_database']}).")

# COMMAND ----------

status = "FAILED"
try:
    connection = repo.get_connection(connection_id)
    adapter = get_source_adapter_for_connection(connection, require_valid=False)
    probe_connection(adapter, source_server=clean["source_server"],
                     source_database=clean["source_database"])
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

set_task_value("connection_id", connection_id)
set_task_value("status", status)
dbutils.notebook.exit(json.dumps({
    "status": status,
    "run_id": run_id,
    "connection_id": connection_id,
    "source_system": SOURCE_SYSTEM,
    "source_database": clean["source_database"],
}))
