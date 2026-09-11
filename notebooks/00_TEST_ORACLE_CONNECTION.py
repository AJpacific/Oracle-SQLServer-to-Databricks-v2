# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 00_TEST_ORACLE_CONNECTION
# MAGIC OPTIONAL diagnostic. The production path validates connections through
# MAGIC NB00A_UpsertAndValidateConnection. Run this only to manually prove network,
# MAGIC TLS, login and SELECT for an Oracle source. It reads no more than a few
# MAGIC rows and prints no secrets. Provide a connection_id (preferred) or the
# MAGIC legacy oracle-migration secret scope.

# COMMAND ----------



# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

# MAGIC %md
# MAGIC ## Oracle connection (secret-backed)
# MAGIC
# MAGIC This notebook resolves its Oracle connection from the `connection_id`
# MAGIC widget when one is supplied (using that connection's own secret scope), and
# MAGIC otherwise falls back to the documented legacy `oracle-migration` scope. It
# MAGIC does not redefine or hard-code any connection details. Set the
# MAGIC `test_schema` / `test_table` widgets below to run the optional
# MAGIC source-object tests.

# COMMAND ----------

# MAGIC %md ### 1. Module and driver validation

# COMMAND ----------

import sys

modules = [
    "src.identifiers",
    "src.sql_builder",
    "src.ddl_builder",
    "src.strategy",
    "src.crosssourcetypemapper",
    "src.control_repository"
]

for m in modules:
    try:
        __import__(m)
        print(f"✅ {m}")
    except Exception as e:
        print(f"❌ {m}")
        print(type(e).__name__, ":", e)
        break

# COMMAND ----------

loader = (
    spark._jvm.java.lang.Thread
    .currentThread()
    .getContextClassLoader()
)

driver_class = loader.loadClass(
    "oracle.jdbc.OracleDriver"
)

print(
    "Oracle JDBC driver loaded successfully:",
    driver_class.getName()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Source-table test configuration
# MAGIC
# MAGIC The widgets below must be changed to a real Oracle owner and table before
# MAGIC running the source-table tests. Do not assume `HR.EMPLOYEES` exists.
# MAGIC
# MAGIC Replace `HR` and `EMPLOYEES` with a real Oracle owner and table. Ordinary
# MAGIC unquoted Oracle names should be entered in uppercase.

# COMMAND ----------

# Remove previously cached widget values such as HR.EMPLOYEES
for widget_name in ("test_schema", "test_table"):
    try:
        dbutils.widgets.remove(widget_name)
    except Exception:
        pass

# COMMAND ----------

dbutils.widgets.text(
    "test_schema",
    "",
    "Oracle source owner"
)

dbutils.widgets.text(
    "test_table",
    "",
    "Oracle source table"
)

test_schema = (
    dbutils.widgets
    .get("test_schema")
    .strip()
    .upper()
)

test_table = (
    dbutils.widgets
    .get("test_table")
    .strip()
    .upper()
)

print(
    "Effective Oracle object:",
    f"{test_schema}.{test_table}"
)

# Optional table diagnostics are skipped when no schema/table is supplied, so
# these counters must always be defined for the final summary.
sample_count = 0
meta_count = 0
pk_count = 0

# COMMAND ----------

# MAGIC %md ### 1b. Resolve the adapter (connection_id preferred)

# COMMAND ----------

# A registered connection is preferred: it selects that connection's own secret
# scope instead of the global legacy scope. The legacy scope remains a
# documented fallback for environments not yet onboarded.
if CONNECTION_ID:
    connection = get_connection(CONNECTION_ID)
    if connection is None:
        raise ValueError(f"connection_id {CONNECTION_ID!r} not found in source_connection")
    if normalize_source_system(connection["source_system"]) != "oracle":
        raise ValueError(
            f"connection_id {CONNECTION_ID!r} is "
            f"{connection['source_system']!r}; this diagnostic requires an Oracle connection")
    adapter = get_source_adapter_for_connection(connection, require_valid=False)
    print(f"Using registered connection {CONNECTION_ID} (scope from source_connection).")
else:
    adapter = get_source_adapter(
        "oracle", secret_provider=_secret_provider, secret_scope=SECRET_SCOPE)
    print(f"[legacy fallback] no connection_id supplied; using the "
          f"'{SECRET_SCOPE}' secret scope.")

print("JDBC URL (redacted):",
      adapter.redact_jdbc_url(adapter.get_jdbc_url_and_props()[0]))

# COMMAND ----------

# MAGIC %md ### 2. Oracle DUAL connectivity test

# COMMAND ----------

dual_test = read_source_jdbc(
    adapter,
    "(SELECT 1 AS CONNECTION_OK FROM DUAL) q",
    fetchsize=1
)

dual_rows = dual_test.collect()

if not dual_rows:
    raise RuntimeError(
        "Oracle DUAL query returned no rows."
    )

if int(dual_rows[0]["CONNECTION_OK"]) != 1:
    raise RuntimeError(
        "Oracle DUAL query returned an unexpected value."
    )

dual_test.show(
    n=1,
    truncate=False
)

print(
    "Oracle TLS connection and authentication succeeded."
)

# COMMAND ----------

# MAGIC %md ### 3. Source table sample test (skipped unless test_schema/test_table set)

# COMMAND ----------

if not (test_schema and test_table):
    print("Set test_schema and test_table to run the source-table diagnostics.")
else:
    probe = sqlb.build_top_n_probe(
        test_schema,
        test_table,
        5
    )

    print(
        "Testing Oracle object:",
        f"{test_schema}.{test_table}"
    )

    sample_df = read_source_jdbc(
        adapter,
        probe,
        fetchsize=5
    )

    sample_df.show(
        n=5,
        truncate=False
    )

    sample_count = sample_df.count()

    print(
        "Sample rows returned:",
        sample_count
    )

# COMMAND ----------

# MAGIC %md ### 4. Column metadata test (skipped unless test_schema/test_table set)

# COMMAND ----------

if test_schema and test_table:
    meta = read_source_jdbc(
        adapter,
        sqlb.columns_metadata_query(
            test_schema,
            test_table
        )
    )

    meta.show(
        truncate=False
    )

    meta_count = meta.count()

    print(
        "Column metadata rows:",
        meta_count
    )

    if meta_count == 0:
        raise RuntimeError(
            "No Oracle column metadata was returned. "
            "Verify the owner/table names are uppercase and "
            "the Oracle user can see the source object."
        )

# COMMAND ----------

# MAGIC %md ### 5. Primary-key metadata test (skipped unless test_schema/test_table set)

# COMMAND ----------

if test_schema and test_table:
    pk = read_source_jdbc(
        adapter,
        sqlb.primary_key_query(
            test_schema,
            test_table
        )
    )

    pk.show(
        truncate=False
    )

    pk_count = pk.count()

    print(
        "Primary-key columns:",
        pk_count
    )

    if pk_count == 0:
        print(
            "No primary key was found. "
            "The accelerator may select WATERMARK or FULL_LOAD "
            "depending on available watermark columns."
        )

# COMMAND ----------

# MAGIC %md ### 6. Final success result

# COMMAND ----------

result = {
    "status": "SUCCEEDED",
    "schema": test_schema,
    "table": test_table,
    "sample_rows": sample_count,
    "metadata_columns": meta_count,
    "primary_key_columns": pk_count,
}

print(
    json.dumps(
        result,
        indent=2
    )
)

dbutils.notebook.exit(
    json.dumps(result)
)