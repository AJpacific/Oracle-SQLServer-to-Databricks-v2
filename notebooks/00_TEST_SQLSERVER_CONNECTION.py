# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 00_TEST_SQLSERVER_CONNECTION
# MAGIC OPTIONAL diagnostic. The production path validates connections through
# MAGIC NB00A_UpsertAndValidateConnection. Run this only to manually prove network,
# MAGIC TLS, login and SELECT for a SQL Server source using the shared adapter. It
# MAGIC reads no more than a few rows and prints no secrets.

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

# MAGIC %md
# MAGIC ## SQL Server connection (secret-backed)
# MAGIC
# MAGIC This notebook builds a SQL Server adapter via `get_source_adapter` bound to
# MAGIC the `sqlserver_secret_scope` secret scope (default `sqlserver-migration`).
# MAGIC It never redefines or hard-codes any connection details. Set the
# MAGIC `test_database` / `test_schema` / `test_table` widgets (and optionally
# MAGIC `test_server`) below before running the source-table tests.

# COMMAND ----------

# MAGIC %md ### 1. Module and driver validation

# COMMAND ----------

import sys

modules = [
    "src.identifiers",
    "src.sqlserver_sql_builder",
    "src.ddl_builder",
    "src.strategy",
    "src.crosssourcetypemapper",
    "src.control_repository",
    "src.source_adapters.factory",
]

for m in modules:
    try:
        __import__(m)
        print(f"OK  {m}")
    except Exception as e:
        try:
            __import__(m.replace("src.", ""))
            print(f"OK  {m} (flat)")
        except Exception as e2:
            print(f"FAIL {m}: {type(e2).__name__}: {e2}")
            break

# COMMAND ----------

loader = (
    spark._jvm.java.lang.Thread
    .currentThread()
    .getContextClassLoader()
)

driver_class = loader.loadClass(
    "com.microsoft.sqlserver.jdbc.SQLServerDriver"
)

print(
    "Microsoft SQL Server JDBC driver loaded successfully:",
    driver_class.getName()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Source-table test configuration
# MAGIC
# MAGIC Provide a real SQL Server database, schema and table. SQL Server names
# MAGIC preserve their stored casing (e.g. `dbo`, `Employees`).

# COMMAND ----------

for widget_name in ("test_server", "test_database", "test_schema", "test_table"):
    try:
        dbutils.widgets.remove(widget_name)
    except Exception:
        pass

# COMMAND ----------

dbutils.widgets.text(
    "test_server",
    "",
    "SQL Server host",
)

dbutils.widgets.text(
    "test_database",
    "",
    "SQL Server database",
)

dbutils.widgets.text(
    "test_schema",
    "",
    "SQL Server schema",
)

dbutils.widgets.text(
    "test_table",
    "",
    "SQL Server table",
)

print("SQL Server test widgets created. Fill them in before running the tests.")

# COMMAND ----------

# Read the widget values after any Python restart.
test_server = dbutils.widgets.get("test_server").strip() or None
test_database = dbutils.widgets.get("test_database").strip()
test_schema = dbutils.widgets.get("test_schema").strip()
test_table = dbutils.widgets.get("test_table").strip()

if not test_database:
    raise ValueError(
        "Set the test_database widget (required for SQL Server) before running "
        "this optional diagnostic.")

# Optional table diagnostics are skipped when no schema/table is supplied, so
# these counters must always be defined for the final summary.
sample_count = 0
meta_count = 0
pk_count = 0

print(
    "Effective SQL Server object:",
    f"{test_server}:1433/"
    f"{test_database}.{test_schema}.{test_table}",
)

# A registered connection is preferred: it selects that connection's own secret
# scope and TLS trust setting instead of the global legacy scope.
if CONNECTION_ID:
    connection = get_connection(CONNECTION_ID)
    if connection is None:
        raise ValueError(f"connection_id {CONNECTION_ID!r} not found in source_connection")
    if normalize_source_system(connection["source_system"]) != "sqlserver":
        raise ValueError(
            f"connection_id {CONNECTION_ID!r} is "
            f"{connection['source_system']!r}; this diagnostic requires a "
            "SQL Server connection")
    adapter = get_source_adapter_for_connection(
        connection, source_database=test_database, require_valid=False)
    print(f"Using registered connection {CONNECTION_ID} (scope from source_connection).")
else:
    adapter = get_source_adapter(
        "sqlserver",
        source_server=test_server,
        source_database=test_database,
        secret_provider=_secret_provider,
        secret_scope=SQLSERVER_SECRET_SCOPE,
    )
    print(f"[legacy fallback] no connection_id supplied; using the "
          f"'{SQLSERVER_SECRET_SCOPE}' secret scope.")

print(
    "Adapter:",
    adapter.source_system,
    "| driver:",
    adapter.DRIVER,
)

# Build and inspect only the redacted URL.
_url, _props = adapter.get_jdbc_url_and_props(
    test_server,
    test_database,
)

print(
    "JDBC URL (redacted):",
    adapter.redact_jdbc_url(_url),
)

# COMMAND ----------

# MAGIC %md ### 2. SELECT 1 connectivity test

# COMMAND ----------

ping = read_source_jdbc(
    adapter,
    "(SELECT 1 AS CONNECTION_OK) q",
    source_server=test_server,
    source_database=test_database,
    fetchsize=1,
)

ping_rows = ping.collect()

if (
    not ping_rows
    or int(ping_rows[0]["CONNECTION_OK"]) != 1
):
    raise RuntimeError(
        "SQL Server SELECT 1 returned an unexpected value."
    )

ping.show(
    n=1,
    truncate=False,
)

print(
    "SQL Server TLS connection and authentication succeeded."
)

# COMMAND ----------

# MAGIC %md ### 3. Source table TOP sample test (skipped unless schema/table set)

# COMMAND ----------

if not (test_schema and test_table):
    print("Set test_schema and test_table to run the source-table diagnostics.")
else:
    probe = adapter.top_n_probe_query(test_database, test_schema, test_table, 5)
    print("Testing SQL Server object:", f"{test_database}.{test_schema}.{test_table}")

    sample_df = read_source_jdbc(
        adapter, probe, source_server=test_server,
        source_database=test_database, fetchsize=5)
    sample_df.show(n=5, truncate=False)
    sample_count = sample_df.count()
    print("Sample rows returned:", sample_count)

# COMMAND ----------

# MAGIC %md ### 4. Column metadata test (skipped unless schema/table set)

# COMMAND ----------

if test_schema and test_table:
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

# COMMAND ----------

# MAGIC %md ### 5. Primary-key metadata test (skipped unless schema/table set)

# COMMAND ----------

if test_schema and test_table:
    pk = read_source_jdbc(
        adapter, adapter.primary_key_query(test_database, test_schema, test_table),
        source_server=test_server, source_database=test_database)
    pk.orderBy("KEY_POSITION").show(truncate=False)
    pk_count = pk.count()
    print("Primary-key columns:", pk_count)

if pk_count == 0:
    print("No primary key found. The accelerator may select WATERMARK or "
          "FULL_LOAD depending on available temporal columns.")

# COMMAND ----------

# MAGIC %md ### 6. Final success result

# COMMAND ----------

result = {
    "status": "SUCCEEDED",
    "database": test_database,
    "schema": test_schema,
    "table": test_table,
    "sample_rows": sample_count,
    "metadata_columns": meta_count,
    "primary_key_columns": pk_count,
}

print(json.dumps(result, indent=2))
dbutils.notebook.exit(json.dumps(result))