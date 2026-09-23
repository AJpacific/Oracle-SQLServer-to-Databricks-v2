# Databricks notebook source
# MAGIC %md
# MAGIC # NB_MarkSelectedOnboardingFailed
# MAGIC Marks incomplete ONBOARDING or REGISTERED assessment rows as FAILED when
# MAGIC a downstream onboarding task fails.
# MAGIC Scoped strictly to the owning run_id, connection_id, and assessment_id.
# MAGIC Never modifies ONBOARDED rows or claims owned by another run.
# MAGIC Performs metadata operations only: no source JDBC or secret access.

# COMMAND ----------

# MAGIC %run ../shared/_common

# COMMAND ----------

import json

dbutils.widgets.text("run_id", "")
dbutils.widgets.text("connection_id", "")
dbutils.widgets.text("assessment_id", "")
dbutils.widgets.text("failed_stage", "")
dbutils.widgets.text("error_message", "")
dbutils.widgets.text("catalog", "da_accelerators")
dbutils.widgets.text("control_schema", "control")

run_id = dbutils.widgets.get("run_id").strip()
if not run_id:
    raise ValueError(
        "run_id is required and must be passed from the original Job run context"
    )

connection_id = dbutils.widgets.get("connection_id").strip()
if not connection_id:
    raise ValueError("connection_id is required")

assessment_id = dbutils.widgets.get("assessment_id").strip()
if not assessment_id:
    raise ValueError("assessment_id is required")

failed_stage_raw = dbutils.widgets.get("failed_stage").strip().upper()
if not failed_stage_raw:
    raise ValueError("failed_stage is required")

if failed_stage_raw not in VALID_DOWNSTREAM_ONBOARDING_STAGES:
    raise ValueError(
        f"Invalid failed_stage {failed_stage_raw!r}; expected one of: "
        f"{', '.join(sorted(VALID_DOWNSTREAM_ONBOARDING_STAGES))}"
    )

error_message_raw = dbutils.widgets.get("error_message").strip()
catalog = dbutils.widgets.get("catalog").strip() or CATALOG
control_schema = dbutils.widgets.get("control_schema").strip() or CONTROL_SCHEMA

repo = ControlRepository(
    spark,
    catalog=catalog,
    control_schema=control_schema,
)

def ctrl(t):
    return f"{quote_databricks(catalog)}.{quote_databricks(control_schema)}.{quote_databricks(t)}"

# COMMAND ----------

# Sanitize error message to prevent credential or secret leakage
safe_error = failcls.sanitize_message(error_message_raw) if error_message_raw else f"Downstream task failure at {failed_stage_raw}"
if len(safe_error) > 2000:
    safe_error = safe_error[:1997] + "..."

# Find incomplete rows owned by this run
candidate_query = f"""
    SELECT source_database, source_schema, object_name, selection_status, onboarding_attempt_id
    FROM {ctrl('source_assessment')}
    WHERE connection_id = {escape_string_literal(connection_id)}
      AND assessment_id = {escape_string_literal(assessment_id)}
      AND object_type = 'TABLE'
      AND onboarding_run_id = {escape_string_literal(run_id)}
      AND upper(trim(coalesce(selection_status, ''))) IN ('ONBOARDING', 'REGISTERED')
"""
candidate_rows = spark.sql(candidate_query).collect()
target_count = len(candidate_rows)

failed_count = 0
errors = []
for r in candidate_rows:
    row_dict = r.asDict(recursive=True)

    s_db = row_dict.get("source_database")
    sch = row_dict["source_schema"]
    tbl = row_dict["object_name"]
    att = row_dict.get("onboarding_attempt_id")
    try:
        succ = repo.mark_assessment_onboarding_failed(
            connection_id=connection_id,
            assessment_id=assessment_id,
            source_schema=sch,
            object_name=tbl,
            run_id=run_id,
            attempt_id=att or None,
            failed_stage=failed_stage_raw,
            error=safe_error,
            source_database=s_db,
        )
        if succ:
            failed_count += 1
        else:
            errors.append(f"{sch}.{tbl}: State was not updated (row may not be in ONBOARDING/REGISTERED or ownership mismatch)")
    except Exception as exc:
        safe_msg = failcls.sanitize_message(exc)
        errors.append(f"{sch}.{tbl}: {safe_msg}")
        print(f"[warn] Failed to mark failure for {sch}.{tbl}: {safe_msg}")

# COMMAND ----------

if target_count == 0:
    business_status = "NO_MATCHING_ROWS"
    execution_status = "SUCCEEDED"
elif failed_count == target_count:
    business_status = "RECORDED"
    execution_status = "SUCCEEDED"
else:
    business_status = "PARTIAL"
    execution_status = "FAILED"

bounded_errors = [
    str(error)[:500]
    for error in errors[:20]
]

exit_payload = {
    "status": execution_status,
    "business_status": business_status,
    "run_id": run_id,
    "connection_id": connection_id,
    "assessment_id": assessment_id,
    "failed_stage": failed_stage_raw,
    "target_count": target_count,
    "failed_count": failed_count,
    "errors": bounded_errors,
}

set_task_value("run_id", run_id)
set_task_value("connection_id", connection_id)
set_task_value("assessment_id", assessment_id)
set_task_value("failed_stage", failed_stage_raw)
set_task_value("failed_count", failed_count)
set_task_value("business_status", business_status)
set_task_value("status", execution_status)

payload_str = json.dumps(exit_payload)
print(f"Downstream failure recording complete: marked {failed_count}/{target_count} row(s) as FAILED "
      f"(stage={failed_stage_raw}, status={execution_status})")

if target_count > 0 and failed_count != target_count:
    raise RuntimeError(
        f"Downstream failure recording incomplete: marked {failed_count} of {target_count} row(s) "
        f"as FAILED (stage={failed_stage_raw})"
    )

dbutils.notebook.exit(payload_str)