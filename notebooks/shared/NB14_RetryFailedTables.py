# Databricks notebook source
# MAGIC %md
# MAGIC # NB14_RetryFailedTables
# MAGIC Selector ONLY. It inspects the latest failed attempt per source table and
# MAGIC operation for a prior run, then returns separate executable and manual
# MAGIC review collections. It never reloads a table, reapplies data, or duplicates
# MAGIC migration logic; the job routes each recovery_action to an existing notebook.

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

dbutils.widgets.dropdown("pipeline_name", "INGEST", ["INGEST", "ETL"])
dbutils.widgets.text("original_run_ids", "")
dbutils.widgets.text("operation", "")          # optional filter, e.g. FULL_LOAD
dbutils.widgets.text("source_table_id", "")    # optional single-table scope
dbutils.widgets.text("max_retries", "3")
dbutils.widgets.dropdown("include_non_retryable", "false", ["true", "false"])

MAX_ORIGINAL_RUN_IDS = 20


def _parse_original_run_ids(raw_value):
    values = []
    seen = set()

    for item in str(raw_value or "").split(","):
        run_id = item.strip()

        if not run_id:
            continue

        if run_id not in seen:
            values.append(run_id)
            seen.add(run_id)

    if not values:
        raise ValueError(
            "original_run_ids must contain at least one nonblank run_id"
        )

    if len(values) > MAX_ORIGINAL_RUN_IDS:
        raise ValueError(
            "original_run_ids supports at most "
            f"{MAX_ORIGINAL_RUN_IDS} unique values"
        )

    return values


pipeline_name = failcls.normalize_pipeline_name(
    dbutils.widgets.get("pipeline_name"))
original_run_ids = _parse_original_run_ids(
    dbutils.widgets.get("original_run_ids"))
operation_filter = dbutils.widgets.get("operation").strip().upper()
operation_filter = failcls.validate_pipeline_operation(
    pipeline_name, operation_filter)
only_id = dbutils.widgets.get("source_table_id").strip()
try:
    # max_retries = the number of ADDITIONAL attempts allowed after the first.
    # With max_retries=3 an initial attempt_number=1 may be retried as attempts
    # 2, 3, and 4; a further failure becomes MANUAL_REVIEW.
    max_retries = int(dbutils.widgets.get("max_retries").strip() or "3")
except ValueError:
    max_retries = 3
include_non_retryable = dbutils.widgets.get("include_non_retryable") == "true"


def ctrl(t):
    return f"{quote_databricks(CATALOG)}.{quote_databricks(CONTROL_SCHEMA)}.{quote_databricks(t)}"


def _row_run_id(row):
    if hasattr(row, "asDict"):
        return row.asDict().get("run_id")
    if isinstance(row, dict):
        return row.get("run_id")
    if hasattr(row, "__getitem__"):
        try:
            return row["run_id"]
        except Exception:
            pass
    return getattr(row, "run_id", None)


child_run_id = new_run_id("retry")
print(f"Selecting retries for {pipeline_name} runs {original_run_ids}; "
      f"child run_id={child_run_id}")

# COMMAND ----------

# Latest FAILED attempt per source table + operation. Pipeline ownership is an
# explicit exact filter; no source or operation can fall through by default.
escaped_run_ids = ", ".join(
    escape_string_literal(run_id)
    for run_id in original_run_ids
)
where = [f"run_id IN ({escaped_run_ids})", "status = 'FAILED'"]
owned_operations = sorted(failcls.operations_for_pipeline(pipeline_name))
owned_sql = ", ".join(escape_string_literal(op) for op in owned_operations)
where.append(f"operation IN ({owned_sql})")
if operation_filter:
    where.append(f"operation = {escape_string_literal(operation_filter)}")
if CONNECTION_ID:
    where.append(f"connection_id = {escape_string_literal(CONNECTION_ID)}")
if only_id:
    where.append(f"source_table_id = {escape_string_literal(only_id)}")
where_sql = " AND ".join(where)

latest = spark.sql(f"""
    SELECT * FROM (
      SELECT run_id, source_table_id, connection_id, operation, failure_stage,
             error_category, retry_eligible, attempt_number,
             lower_watermark, upper_watermark,
             ROW_NUMBER() OVER (
                 PARTITION BY connection_id, source_table_id, operation, run_id
                 ORDER BY COALESCE(attempt_number, 1) DESC,
                          ended_ts DESC NULLS LAST,
                          started_ts DESC NULLS LAST,
                          run_id DESC
             ) AS rn
      FROM {ctrl('table_run_log')}
      WHERE {where_sql}
    ) WHERE rn = 1
    ORDER BY connection_id, source_table_id, operation
""").collect()
print(f"Distinct failed operations: {len(latest)}")

# COMMAND ----------

# include_non_retryable remains an accepted widget for compatibility. Unsafe
# rows are always returned in manual_review_items and never become executable.
worklist = []
manual_review_items = []
duplicate_keys = []

for p_run_id in original_run_ids:
    p_rows = [
        r for r in latest
        if _row_run_id(r) == p_run_id
    ]
    p_worklist, p_manual, p_dups = failcls.build_retry_collections(
        p_rows, child_run_id, p_run_id, pipeline_name, max_retries)
    for item in p_manual:
        item["parent_run_id"] = p_run_id
    worklist.extend(p_worklist)
    manual_review_items.extend(p_manual)
    duplicate_keys.extend(p_dups)

for connection_id, source_table_id, operation, recovery_action in duplicate_keys:
    print(
        "  [warn] duplicate retry item removed:",
        f"connection_id={connection_id}",
        f"source_table_id={source_table_id}",
        f"operation={operation}",
        f"recovery_action={recovery_action}",
    )

print(f"Retry worklist: {len(worklist)}; "
      f"manual review: {len(manual_review_items)}")
for item in manual_review_items:
    print(
        "  MANUAL_REVIEW",
        f"parent_run_id={item.get('parent_run_id')}",
        f"connection_id={item['connection_id']}",
        f"source_table_id={item['source_table_id']}",
        f"operation={item['operation']}",
        f"reason={item['reason']}",
    )

# COMMAND ----------

NOTEBOOK_EXIT_LIMIT_BYTES = 5 * 1024 * 1024

def _compact_json(value):
    return canonical_task_value_serialization(value)

def _set_json_task_value_if_fits(key, value):
    payload = _compact_json(value)
    # Preserve the existing string-valued task contract. Databricks serializes
    # that string as JSON, so include its quoting/escaping in the size check.
    serialized_size = len(json.dumps(payload, ensure_ascii=False).encode("utf-8"))
    if serialized_size > TASK_VALUE_LIMIT_BYTES:
        print(f"  [warn] {key} exceeds the Databricks task-value limit ({TASK_VALUE_LIMIT_BYTES} bytes); "
              "use the complete notebook result or scope the selector by "
              "operation/source_table_id. No entries were truncated.")
        return False
    set_task_value(key, payload)
    return True

result = {
    "status": "SUCCEEDED", "pipeline_name": pipeline_name,
    "run_id": child_run_id,
    "original_run_ids": original_run_ids,
    "original_run_count": len(original_run_ids),
    "selected": len(worklist),
    "manual_review": len(manual_review_items),
    "worklist": worklist,
    "manual_review_items": manual_review_items,
}
result_payload = _compact_json(result)
if len(result_payload.encode("utf-8")) > NOTEBOOK_EXIT_LIMIT_BYTES:
    raise ValueError(
        "retry selector output exceeds the notebook result limit; rerun with "
        "operation or source_table_id scoping, or query table_run_log using "
        "the documented retry-selection identity")

set_task_value("run_id", child_run_id)
_set_json_task_value_if_fits("original_run_ids", original_run_ids)
set_task_value("original_run_count", len(original_run_ids))
_set_json_task_value_if_fits("worklist", worklist)
_set_json_task_value_if_fits("manual_review_items", manual_review_items)
dbutils.notebook.exit(result_payload)
