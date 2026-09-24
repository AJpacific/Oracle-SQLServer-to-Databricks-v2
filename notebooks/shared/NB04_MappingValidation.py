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
connection_id = require_connection_id(CONNECTION_ID, "mapping validation")
connection = require_valid_connection(connection_id)
print("run_id:", run_id, "| connection_id:", connection_id)

def ctrl(t):
    return f"{quote_databricks(CATALOG)}.{quote_databricks(CONTROL_SCHEMA)}.{quote_databricks(t)}"

# COMMAND ----------

maps = spark.sql(
    f"SELECT * FROM {ctrl('resolved_column_mappings')} "
    f"WHERE run_id = {escape_string_literal(run_id)} "
    f"AND connection_id = {escape_string_literal(connection_id)}"
).collect()
print("Mappings to validate:", len(maps))

# COMMAND ----------

results = []
from collections import Counter
target_col_counts = Counter(
    (r["connection_id"], r["source_table_id"], str(r.asDict().get("target_column_name") or "").strip().lower())
    for r in maps
    if r.asDict().get("include_column") is not False and r.asDict().get("target_column_name")
)

for r in maps:
    assert_table_connection_match(r, connection_id)
    assert_source_identity_match(r, connection)
    d = r.asDict()
    src_id = r["source_table_id"]
    src_system = require_source_system(
        r["source_system"], "resolved mapping row")
    conn_id = r["connection_id"]
    schema, table, col = r["source_schema"], r["source_table"], r["column_name"]
    target_col = d.get("target_column_name")
    status = (r["mapping_status"] or "").upper()
    fidelity = (r["fidelity"] or "").upper()
    dtype = r["databricks_delta_type"] or ""
    policy_code = d.get("policy_code")
    include_column = d.get("include_column")
    is_writable = d.get("is_writable")
    requires_review = d.get("requires_review")

    def _add(severity, rule, message):
        results.append((run_id, src_id, conn_id, src_system,
                        r["source_server"], r["source_database"],
                        schema, table, col,
                        severity, rule, message))

    # Target column name validation
    if not target_col or not str(target_col).strip():
        _add("ERROR", "BLANK_TARGET_COLUMN_NAME",
             f"target_column_name is blank or missing for source column '{col}'")
    else:
        norm_t_col = str(target_col).strip()
        try:
            validate_identifier(norm_t_col)
        except Exception as exc:
            _add("ERROR", "INVALID_TARGET_COLUMN_NAME",
                 f"target_column_name '{norm_t_col}' is invalid: {exc}")

        if include_column is not False:
            if target_col_counts.get((conn_id, src_id, norm_t_col.lower()), 0) > 1:
                _add("ERROR", "DUPLICATE_TARGET_COLUMN_NAME",
                     f"duplicate target_column_name '{norm_t_col}' in table '{table}'")

    if policy_code == "INVALID_TARGET_COLUMN_NAME":
        _add("ERROR", "INVALID_TARGET_COLUMN_NAME",
             f"target_column_name generation failed for '{col}'")
    elif policy_code == "TARGET_COLUMN_NAME_COLLISION":
        _add("ERROR", "TARGET_COLUMN_NAME_COLLISION",
             f"target_column_name '{target_col}' collision for '{col}'")

    # Canonical policy outcomes decided by the source adapter. This notebook
    # reports them without interpreting any dialect metadata concept.
    if include_column is False:
        if policy_code not in ("INVALID_TARGET_COLUMN_NAME", "TARGET_COLUMN_NAME_COLLISION"):
            _add("ERROR", policy_code or "SOURCE_NON_WRITABLE_COLUMN",
                 "source column policy excludes this column from automatic migration")
    elif is_writable is False:
        severity = "WARNING" if requires_review else "INFO"
        _add(severity, policy_code or "SOURCE_NON_WRITABLE_COLUMN",
             "source column policy marks this column as not writable")
    elif requires_review and policy_code:
        _add("WARNING", policy_code,
             "source column policy requires explicit review before materialization")

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
    if not dtype and include_column is not False:
        _add("ERROR", "EMPTY_TARGET_TYPE",
             "resolved databricks_delta_type is empty")

# COMMAND ----------

spark.sql(f"""
        DELETE FROM {ctrl('mapping_validation_results')}
        WHERE run_id = {escape_string_literal(run_id)}
            AND connection_id = {escape_string_literal(connection_id)}
""")

if results:
    cols = ["run_id", "source_table_id", "connection_id", "source_system",
            "source_server", "source_database",
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
                                  "connection_id": connection_id,
                                  "findings": len(results)}))