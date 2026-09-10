# Databricks notebook source
# MAGIC %md
# MAGIC # NB14_RetryFailedTables
# MAGIC Selector ONLY. It inspects the latest failed attempt per source table for a
# MAGIC prior run and returns a retry worklist (child run_id + parent_run_id +
# MAGIC incremented attempt_number + a safe recovery_action). It never reloads a
# MAGIC table, never reapplies data, and never duplicates the migration logic - the
# MAGIC INGEST/ETL job routes each recovery_action to the existing load notebook.

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

dbutils.widgets.dropdown("pipeline_name", "INGEST", ["INGEST", "ETL"])
dbutils.widgets.text("original_run_id", "")
dbutils.widgets.text("operation", "")          # optional filter, e.g. FULL_LOAD
dbutils.widgets.text("source_table_id", "")    # optional single-table scope
dbutils.widgets.text("max_retries", "3")
dbutils.widgets.dropdown("include_non_retryable", "false", ["true", "false"])

pipeline_name = dbutils.widgets.get("pipeline_name").strip()
original_run_id = dbutils.widgets.get("original_run_id").strip()
operation_filter = dbutils.widgets.get("operation").strip()
only_id = dbutils.widgets.get("source_table_id").strip()
try:
    max_retries = int(dbutils.widgets.get("max_retries").strip() or "3")
except ValueError:
    max_retries = 3
include_non_retryable = dbutils.widgets.get("include_non_retryable") == "true"

if not original_run_id:
    raise ValueError("original_run_id is required")

def ctrl(t):
    return f"{quote_databricks(CATALOG)}.{quote_databricks(CONTROL_SCHEMA)}.{quote_databricks(t)}"

child_run_id = new_run_id("retry")
print(f"Selecting retries for {pipeline_name} run {original_run_id}; "
      f"child run_id={child_run_id}")

# COMMAND ----------

# Latest FAILED attempt per source table (history is preserved; nothing mutated).
where = [f"run_id = {escape_string_literal(original_run_id)}", "status = 'FAILED'"]
if operation_filter:
    where.append(f"operation = {escape_string_literal(operation_filter)}")
if only_id:
    where.append(f"source_table_id = {escape_string_literal(only_id)}")
where_sql = " AND ".join(where)

latest = spark.sql(f"""
    SELECT * FROM (
      SELECT source_table_id, connection_id, operation, failure_stage,
             error_category, retry_eligible, attempt_number,
             ROW_NUMBER() OVER (PARTITION BY source_table_id
                                ORDER BY ended_ts DESC) AS rn
      FROM {ctrl('table_run_log')}
      WHERE {where_sql}
    ) WHERE rn = 1
""").collect()
print(f"Distinct failed tables: {len(latest)}")

# COMMAND ----------

worklist = []
manual = []
for r in latest:
    src_id = r["source_table_id"]
    operation = r["operation"]
    stage = r["failure_stage"]
    eligible = bool(r["retry_eligible"]) if r["retry_eligible"] is not None else False
    prev_attempt = int(r["attempt_number"]) if r["attempt_number"] is not None else 1
    next_attempt = prev_attempt + 1

    action = failcls.recovery_action(operation, stage)
    if not eligible and not include_non_retryable:
        manual.append((src_id, operation, r["error_category"]))
        continue
    if not eligible:
        action = "MANUAL_REVIEW"
    if next_attempt > max_retries:
        action = "MANUAL_REVIEW"

    item = {
        "run_id": child_run_id, "parent_run_id": original_run_id,
        "connection_id": r["connection_id"], "source_table_id": src_id,
        "operation": operation, "attempt_number": next_attempt,
        "recovery_action": action,
    }
    if action == "MANUAL_REVIEW":
        manual.append((src_id, operation, r["error_category"]))
    worklist.append(item)

print(f"Retry worklist: {len(worklist)}; manual review: {len(manual)}")
for m in manual:
    print("  MANUAL_REVIEW", m)

# COMMAND ----------

set_task_value("run_id", child_run_id)
set_task_value("worklist", json.dumps(worklist))
dbutils.notebook.exit(json.dumps({
    "status": "SUCCEEDED", "pipeline_name": pipeline_name,
    "run_id": child_run_id, "parent_run_id": original_run_id,
    "selected": len(worklist), "manual_review": len(manual),
    "worklist": worklist,
}))
