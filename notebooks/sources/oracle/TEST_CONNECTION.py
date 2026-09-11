# Databricks notebook source
# MAGIC %md
# MAGIC # Oracle / TEST_CONNECTION
# MAGIC OPTIONAL diagnostic. The production path validates connections through
# MAGIC `sources/oracle/NB00A_UpsertAndValidateConnection`. Run this only to prove
# MAGIC network, TLS, login and SELECT manually.
# MAGIC
# MAGIC When `connection_id` is supplied the **registered connection is the
# MAGIC authority**: its server, database, and secret scope are used and a widget
# MAGIC cannot override them. Without `connection_id` the documented legacy secret
# MAGIC scope is used. No secret is ever printed.

# COMMAND ----------

# MAGIC %run ../../shared/_common

# COMMAND ----------

SOURCE_SYSTEM = "oracle"

dbutils.widgets.text("test_schema", "", "Oracle source owner (optional)")
dbutils.widgets.text("test_table", "", "Oracle source table (optional)")

test_schema = dbutils.widgets.get("test_schema").strip().upper()
test_table = dbutils.widgets.get("test_table").strip().upper()

# Optional object diagnostics are skipped when no schema/table is supplied, so
# these counters must always be defined for the final summary.
sample_count = 0
meta_count = 0
pk_count = 0

# COMMAND ----------

# MAGIC %md ### 1. Driver validation

# COMMAND ----------

driver_class = (spark._jvm.java.lang.Thread.currentThread()
                .getContextClassLoader().loadClass("oracle.jdbc.OracleDriver"))
print("Oracle JDBC driver loaded successfully:", driver_class.getName())

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
            "this diagnostic requires an Oracle connection")
    # Registered values are authoritative; no widget may override them.
    test_server = cd.get("source_server")
    test_database = cd.get("source_database")
    adapter = get_source_adapter_for_connection(connection, require_valid=False)
    print(f"Using registered connection {CONNECTION_ID} "
          f"(server/database/secret scope from source_connection).")
else:
    test_server = None
    test_database = None
    adapter = get_source_adapter(
        SOURCE_SYSTEM, secret_provider=_secret_provider, secret_scope=SECRET_SCOPE)
    print(f"[legacy fallback] no connection_id supplied; using the "
          f"'{SECRET_SCOPE}' secret scope. Register a connection and pass "
          "connection_id for production use.")

print("JDBC URL (redacted):",
      adapter.redact_jdbc_url(adapter.get_jdbc_url_and_props()[0]))

# COMMAND ----------

# MAGIC %md ### 3. Connectivity probe

# COMMAND ----------

probe_connection(adapter, source_server=test_server, source_database=test_database)
print("Oracle TLS connection and authentication succeeded.")

# COMMAND ----------

# MAGIC %md ### 4. Optional source-object diagnostics

# COMMAND ----------

if not (test_schema and test_table):
    print("Set test_schema and test_table to run the source-object diagnostics.")
else:
    print("Testing Oracle object:", f"{test_schema}.{test_table}")

    sample_df = read_source_jdbc(
        adapter, adapter.top_n_probe_query(test_database, test_schema, test_table, 5),
        source_server=test_server, source_database=test_database, fetchsize=5)
    sample_df.show(n=5, truncate=False)
    sample_count = sample_df.count()
    print("Sample rows returned:", sample_count)

    meta = read_source_jdbc(
        adapter, adapter.columns_metadata_query(test_database, test_schema, test_table),
        source_server=test_server, source_database=test_database)
    meta.show(truncate=False)
    meta_count = meta.count()
    print("Column metadata rows:", meta_count)
    if meta_count == 0:
        raise RuntimeError(
            "No Oracle column metadata was returned. Verify the owner/table "
            "names are uppercase and the Oracle user can see the object.")

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
    "status": "SUCCEEDED", "source_system": SOURCE_SYSTEM,
    "connection_id": CONNECTION_ID or None,
    "sample_rows": sample_count, "metadata_columns": meta_count,
    "primary_key_columns": pk_count,
}))
