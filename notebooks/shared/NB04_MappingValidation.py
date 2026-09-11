# Databricks notebook source
# MAGIC %md
# MAGIC # NB04_MappingValidation
# MAGIC Validates the resolved mappings against safety contracts and writes
# MAGIC mapping_validation_results (ERROR / WARNING / INFO per column).

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

from pyspark.sql import functions as F

run_id = get_run_id()
print("run_id:", run_id)

def ctrl(t):
    return f"{quote_databricks(CATALOG)}.{quote_databricks(CONTROL_SCHEMA)}.{quote_databricks(t)}"

# COMMAND ----------

_conn_filter = (f" AND connection_id = {escape_string_literal(CONNECTION_ID)}"
                if CONNECTION_ID else "")

maps = spark.sql(
    f"SELECT * FROM {ctrl('resolved_column_mappings')} "
    f"WHERE run_id = {escape_string_literal(run_id)}{_conn_filter}"
).collect()
print("Mappings to validate:", len(maps))

# COMMAND ----------

results = []
for r in maps:
    src_id = r["source_table_id"]
    src_system = r["source_system"]
    conn_id = r["connection_id"]
    schema, table, col = r["source_schema"], r["source_table"], r["column_name"]
    status = (r["mapping_status"] or "").upper()
    fidelity = (r["fidelity"] or "").upper()
    dtype = r["databricks_delta_type"] or ""

    def _add(severity, rule, message):
        results.append((run_id, src_id, conn_id, src_system, schema, table, col,
                        severity, rule, message))
    # Source-column safety policy is explicit and source-qualified.
    if bool(r["is_hidden"]):
        _add("ERROR", "SQLSERVER_HIDDEN_COLUMN",
             "SQL Server hidden/system-generated column is blocked from automatic migration")
    elif bool(r["is_computed"]):
        _add("WARNING", "SQLSERVER_COMPUTED_COLUMN",
             "SQL Server computed column requires explicit review before materialization")

    # Contract 1: BLOCKED columns are hard errors - they stop a table migrating.
    if status == "BLOCKED":
        _add("ERROR", "BLOCKED_TYPE",
             f"{r['source_type']} has no safe Delta mapping: {r['notes']}")
    # Contract 2: REVIEW columns need a human sign-off before migration.
    elif status == "REVIEW":
        _add("WARNING", "NEEDS_REVIEW",
             f"{r['source_type']} -> {dtype}: {r['notes']}")
    # Contract 3: lossy conversions are flagged even when AUTO (e.g. DATE->TIMESTAMP).
    elif fidelity in ("LOSSY", "WIDENED") and status == "AUTO":
        sev = "WARNING" if fidelity == "LOSSY" else "INFO"
        _add(sev, f"{fidelity}_CONVERSION",
             f"{r['source_type']} -> {dtype}: {r['notes'] or fidelity.lower()}")
    # Contract 4: sanity - an empty target type is always an error.
    if not dtype:
        _add("ERROR", "EMPTY_TARGET_TYPE",
             "resolved databricks_delta_type is empty")

# COMMAND ----------

if results:
    cols = ["run_id", "source_table_id", "connection_id", "source_system",
            "source_schema", "source_table", "column_name", "severity", "rule",
            "message"]
    df = spark.createDataFrame(results, cols).withColumn("captured_ts", F.current_timestamp())
    df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(
        ctrl("mapping_validation_results").replace("`", ""))
    df.groupBy("severity").count().show()
    print(f"Wrote {len(results)} validation records.")
else:
    print("No validation findings (all AUTO/EXACT).")

dbutils.notebook.exit(json.dumps({"status": "SUCCEEDED", "run_id": run_id,
                                  "findings": len(results)}))