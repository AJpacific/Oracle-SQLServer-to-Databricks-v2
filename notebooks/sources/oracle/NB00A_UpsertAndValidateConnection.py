# Databricks notebook source
# MAGIC %md
# MAGIC # Oracle / NB00A_UpsertAndValidateConnection
# MAGIC INGEST task that onboards one **Oracle** source connection. `source_system`
# MAGIC is fixed by this notebook, not accepted as a widget. It upserts only
# MAGIC non-secret metadata into `source_connection`, then opens the real JDBC
# MAGIC connection through the Oracle adapter (credentials stay in the connection's
# MAGIC secret scope) and runs the Oracle connectivity probe.
# MAGIC It never stores, returns, or prints a credential or a JDBC URL.

# COMMAND ----------

# MAGIC %run ../../shared/_common

# COMMAND ----------

SOURCE_SYSTEM = "oracle"

dbutils.widgets.text("connection_id", "")
dbutils.widgets.text("connection_name", "")
dbutils.widgets.text("source_server", "")
dbutils.widgets.text("source_database", "")
dbutils.widgets.text("secret_scope", "")
dbutils.widgets.dropdown("trust_server_certificate", "false", ["true", "false"])

run_id = get_run_id()
repo = control_repo()

# Oracle identifies its database through the registered JDBC URL or service in
# the secret scope, so source_database is optional metadata here.
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
                        "is_active": False, "error_message": None})
print(f"Upserted Oracle connection {connection_id}.")

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
set_task_value("connection_status", status)
dbutils.notebook.exit(json.dumps({
    "status": status,
    "connection_status": status,
    "run_id": run_id,
    "connection_id": connection_id,
    "source_system": SOURCE_SYSTEM,
    "source_database": clean["source_database"],
}))