# Databricks notebook source
# MAGIC %md
# MAGIC # SQL Server / TEST_CONNECTION
# MAGIC OPTIONAL diagnostic. The production path validates connections through
# MAGIC `sources/sqlserver/NB00A_UpsertAndValidateConnection`. Run this only to
# MAGIC prove network, TLS, login and SELECT manually.
# MAGIC
# MAGIC When `connection_id` is supplied the **registered connection is the
# MAGIC authority**: `source_server`, `source_database`, the secret scope, and the
# MAGIC TLS trust setting all come from `source_connection`, and no widget can
# MAGIC override them (no separate database widget is required). Without
# MAGIC `connection_id` the documented legacy mode applies and `test_database` is
# MAGIC required. No secret is ever printed.

# COMMAND ----------

# MAGIC %run ../../shared/_common

# COMMAND ----------

SOURCE_SYSTEM = "sqlserver"

dbutils.widgets.text("sqlserver_secret_scope", "sqlserver-migration")
dbutils.widgets.text("test_server", "", "SQL Server host (legacy mode only)")
dbutils.widgets.text("test_database", "", "SQL Server database (legacy mode only)")
dbutils.widgets.text("test_schema", "", "SQL Server schema (optional)")
dbutils.widgets.text("test_table", "", "SQL Server table (optional)")
dbutils.widgets.dropdown("show_sample_values", "false", ["true", "false"])
dbutils.widgets.dropdown("allow_legacy_mode", "false", ["true", "false"])

test_schema = dbutils.widgets.get("test_schema").strip()
test_table = dbutils.widgets.get("test_table").strip()
show_sample_values = (
    dbutils.widgets.get("show_sample_values").strip().lower() == "true")
allow_legacy_mode = (
    dbutils.widgets.get("allow_legacy_mode").strip().lower() == "true")
legacy_secret_scope = dbutils.widgets.get("sqlserver_secret_scope").strip()
if show_sample_values:
    print("WARNING: show_sample_values=true should only be used with approved non-sensitive test data.")

# Optional object diagnostics are skipped when no schema/table is supplied, so
# these counters must always be defined for the final summary.
sample_count = 0
sample_columns = []
sample_query_succeeded = False
meta_count = 0
pk_count = 0

# COMMAND ----------

# MAGIC %md ### 1. Driver validation

# COMMAND ----------

driver_class = (spark._jvm.java.lang.Thread.currentThread()
                .getContextClassLoader()
                .loadClass("com.microsoft.sqlserver.jdbc.SQLServerDriver"))
print("Microsoft SQL Server JDBC driver loaded successfully:",
      driver_class.getName())

# COMMAND ----------

# MAGIC %md ### 2. Resolve the connection (registered connection wins)

# COMMAND ----------

if CONNECTION_ID:
    connection = get_connection(CONNECTION_ID)
    if connection is None:
        raise ValueError(
            f"connection_id {CONNECTION_ID!r} not found in source_connection")
    cd = connection.asDict()
    if normalize_source_system(cd["source_system"]) != SOURCE_SYSTEM:
        raise ValueError(
            f"connection_id {CONNECTION_ID!r} is {cd['source_system']!r}; "
            "this diagnostic requires a SQL Server connection")
    # Registered values are authoritative; the widgets are ignored entirely so a
    # blank or stale widget can never override the registered connection.
    test_server = cd.get("source_server")
    test_database = cd.get("source_database")
    if not test_database:
        raise ValueError(
            f"registered connection {CONNECTION_ID!r} has no source_database")
    adapter = get_source_adapter_for_connection(connection, require_valid=False)
    print(f"Using registered connection {CONNECTION_ID} "
          f"(server/database/secret scope/TLS from source_connection).")
elif allow_legacy_mode:
    if not legacy_secret_scope:
        raise ValueError("legacy diagnostic requires sqlserver_secret_scope")
    test_server = dbutils.widgets.get("test_server").strip() or None
    test_database = dbutils.widgets.get("test_database").strip()
    if not test_database:
        raise ValueError(
            "legacy mode requires the test_database widget; supply "
            "connection_id instead to use a registered connection")
    adapter = get_source_adapter(
        SOURCE_SYSTEM, source_server=test_server, source_database=test_database,
        secret_provider=_secret_provider, secret_scope=legacy_secret_scope)
    print(f"[legacy fallback] no connection_id supplied; using the "
          f"'{legacy_secret_scope}' secret scope. Register a connection and "
          "pass connection_id for production use.")
else:
        raise ValueError(
        "connection_id is required; set allow_legacy_mode=true only for an "
        "approved manual legacy diagnostic")

print("Effective SQL Server target:", f"{test_database}")
print("JDBC URL (redacted):",
      adapter.redact_jdbc_url(
          adapter.get_jdbc_url_and_props(test_server, test_database)[0]))

# COMMAND ----------

# MAGIC %md ### 3. Connectivity probe

# COMMAND ----------

probe_connection(adapter, source_server=test_server, source_database=test_database)
print("SQL Server TLS connection and authentication succeeded.")

# COMMAND ----------

# MAGIC %md ### 4. Optional source-object diagnostics

# COMMAND ----------

if not (test_schema and test_table):
    print("Set test_schema and test_table to run the source-object diagnostics.")
else:
    print("Testing SQL Server object:",
          f"{test_database}.{test_schema}.{test_table}")

    sample_df = read_source_jdbc(
        adapter, adapter.top_n_probe_query(test_database, test_schema, test_table, 5),
        source_server=test_server, source_database=test_database, fetchsize=5)
    sample_count = sample_df.count()
    sample_columns = list(sample_df.columns)
    sample_query_succeeded = True
    print("Sample query succeeded.")
    print("Sample rows returned:", sample_count)
    print("Sample column names:", sample_columns)
    if show_sample_values:
        sample_df.show(n=5, truncate=False)

    meta = read_source_jdbc(
        adapter, adapter.columns_metadata_query(test_database, test_schema, test_table),
        source_server=test_server, source_database=test_database)
    meta.show(truncate=False)
    meta_count = meta.count()
    print("Column metadata rows:", meta_count)
    if meta_count == 0:
        raise RuntimeError(
            "No SQL Server column metadata was returned. Verify the "
            "database/schema/table names and that the login can see the object.")

    pk = read_source_jdbc(
        adapter, adapter.primary_key_query(test_database, test_schema, test_table),
        source_server=test_server, source_database=test_database)
    pk.orderBy("KEY_POSITION").show(truncate=False)
    pk_count = pk.count()
    print("Primary-key columns:", pk_count)
    if pk_count == 0:
        print("No primary key found; the accelerator may select WATERMARK or "
              "FULL_LOAD depending on available watermark columns.")

# COMMAND ----------

dbutils.notebook.exit(json.dumps({
    "status": "SUCCEEDED", "execution_status": "SUCCEEDED",
    "business_status": "COMPLETE", "source_system": SOURCE_SYSTEM,
    "connection_id": CONNECTION_ID or None,
    "sample_query_succeeded": sample_query_succeeded,
    "sample_rows": sample_count, "sample_columns": sample_columns,
    "sample_values_displayed": show_sample_values and sample_query_succeeded,
    "metadata_columns": meta_count,
    "primary_key_columns": pk_count,
}))
