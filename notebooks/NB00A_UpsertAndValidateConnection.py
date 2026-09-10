# Databricks notebook source
# MAGIC %md
# MAGIC # NB00A_UpsertAndValidateConnection
# MAGIC INGEST-pipeline task that onboards one source connection. It upserts only
# MAGIC non-secret connection metadata into `source_connection`, then opens the
# MAGIC real JDBC connection through the existing adapter (credentials stay in the
# MAGIC connection's secret scope) and runs a trivial connectivity probe.
# MAGIC It never stores, returns, or prints a username, password, token, or a
# MAGIC credential-bearing JDBC URL.

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

dbutils.widgets.text("connection_id", "")
dbutils.widgets.text("connection_name", "")
dbutils.widgets.text("source_system", "")
dbutils.widgets.text("source_server", "")
dbutils.widgets.text("source_database", "")
dbutils.widgets.text("secret_scope", "")
dbutils.widgets.dropdown("trust_server_certificate", "false", ["true", "false"])

connection_id = dbutils.widgets.get("connection_id").strip()
connection_name = dbutils.widgets.get("connection_name").strip()
raw_system = dbutils.widgets.get("source_system").strip()
source_server = dbutils.widgets.get("source_server").strip() or None
source_database = dbutils.widgets.get("source_database").strip() or None
secret_scope = dbutils.widgets.get("secret_scope").strip()
trust_cert = dbutils.widgets.get("trust_server_certificate").strip() == "true"

run_id = get_run_id()
repo = control_repo()

# COMMAND ----------

# --- validate + normalize inputs (no secrets involved) ----------------------
clean = normalize_connection_input({
    "connection_id": connection_id,
    "connection_name": connection_name,
    "source_system": raw_system,
    "source_server": source_server,
    "source_database": source_database,
    "secret_scope": secret_scope,
    "trust_server_certificate": trust_cert,
})
connection_id = clean["connection_id"]
source_system = clean["source_system"]  # oracle | sqlserver
source_server = clean["source_server"]
source_database = clean["source_database"]

# COMMAND ----------

# --- upsert non-secret metadata as REGISTERED first -------------------------
repo.upsert_connection({
    "connection_id": connection_id,
    "connection_name": clean["connection_name"],
    "source_system": source_system,
    "source_server": source_server,
    "source_database": source_database,
    "secret_scope": clean["secret_scope"],
    "trust_server_certificate": clean["trust_server_certificate"],
    "connection_status": "REGISTERED",
    "is_active": True,
    "error_message": None,
})
print(f"Upserted connection {connection_id} ({source_system}).")

# COMMAND ----------

# --- validate connectivity through the adapter ------------------------------
connection = repo.get_connection(connection_id)
status = "FAILED"
try:
    adapter = get_source_adapter_for_connection(connection, require_valid=False)
    probe = ("(SELECT 1 AS CONNECTION_OK FROM DUAL) q"
             if source_system == "oracle"
             else "(SELECT 1 AS CONNECTION_OK) q")
    rows = read_source_jdbc(
        adapter, probe, source_server=source_server,
        source_database=source_database).collect()
    if not rows or int(rows[0]["CONNECTION_OK"]) != 1:
        raise RuntimeError("connectivity probe returned an unexpected result")
    repo.update_connection_status(connection_id, "VALID", None)
    status = "VALID"
    print(f"Connection {connection_id} validated: VALID")
except Exception as e:
    # Sanitize: never surface a credential-bearing URL or secret in the message.
    safe = adapter.redact_jdbc_url(str(e)) if 'adapter' in dir() else str(e)
    repo.update_connection_status(connection_id, "FAILED", safe[:1000])
    print(f"Connection {connection_id} validation FAILED.")
    raise

# COMMAND ----------

set_task_value("connection_id", connection_id)
set_task_value("status", status)
dbutils.notebook.exit(json.dumps({
    "connection_id": connection_id,
    "source_system": source_system,
    "source_database": source_database,
    "status": status,
}))
