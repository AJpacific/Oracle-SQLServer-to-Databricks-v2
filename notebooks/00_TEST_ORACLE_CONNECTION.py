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
# MAGIC This notebook uses the shared `get_jdbc_url_and_props()` from `_common`,
# MAGIC which reads the connection from the `oracle-migration` secret scope. It does
# MAGIC not redefine or hard-code any connection details. Set the `test_schema` /
# MAGIC `test_table` widgets below before running the source-table tests.

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

# COMMAND ----------

# MAGIC %md ### 2. Oracle DUAL connectivity test

# COMMAND ----------

dual_test = read_jdbc(
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

    sample_df = read_jdbc(
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
    meta = read_jdbc(
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
    pk = read_jdbc(
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