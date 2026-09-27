# Databricks notebook source
# MAGIC %md
# MAGIC # NB_SQLArtifactSummary
# MAGIC Summarizes execution and persistence metrics for an independent SQL Artifact
# MAGIC Migration run. Queries `sql_artifact_control` and `sql_artifact_execution_log`.
# MAGIC Purely read-only; never mutates or writes to existing control tables.

# COMMAND ----------

# MAGIC %run ../shared/_common

# COMMAND ----------

import json
from pyspark.sql import functions as F

try:
    from src.identifiers import quote_databricks, escape_string_literal
    from src.sql_artifact_control_common import (
        SQL_ARTIFACT_CONTROL_TABLE,
        SQL_ARTIFACT_EXECUTION_LOG_TABLE,
        decide_summary,
    )
except ModuleNotFoundError:
    from identifiers import quote_databricks, escape_string_literal
    from sql_artifact_control_common import (
        SQL_ARTIFACT_CONTROL_TABLE,
        SQL_ARTIFACT_EXECUTION_LOG_TABLE,
        decide_summary,
    )

dbutils.widgets.text("run_id", "")
dbutils.widgets.text("catalog", "da_accelerators")
dbutils.widgets.text("control_schema", "control")
dbutils.widgets.text("t24_status", "")
dbutils.widgets.text("t24_business_status", "")
dbutils.widgets.text("t05_status", "")

run_id = dbutils.widgets.get("run_id").strip() or get_run_id()
if not run_id:
    raise ValueError("Blank run_id rejected: current run_id scope is required")

catalog = dbutils.widgets.get("catalog").strip() or CATALOG
control_schema = dbutils.widgets.get("control_schema").strip() or CONTROL_SCHEMA
t24_status = dbutils.widgets.get("t24_status").strip()
t24_business_status = dbutils.widgets.get("t24_business_status").strip()
t05_status = dbutils.widgets.get("t05_status").strip()

if not t24_status:
    try:
        t24_status = get_task_value("T24_Lakebridge_Analyze_And_Transpile", "status")
    except Exception:
        try:
            t24_status = get_task_value("T05_Migrate_SQL_Artifacts", "status")
        except Exception:
            t24_status = t05_status

if not t24_business_status:
    try:
        t24_business_status = get_task_value("T24_Lakebridge_Analyze_And_Transpile", "business_status")
    except Exception:
        t24_business_status = ""

print(f"Generating SQL Artifact Migration Summary for run_id={run_id} (t24_status={t24_status})")

control_fqn = f"{quote_databricks(catalog)}.{quote_databricks(control_schema)}.{quote_databricks(SQL_ARTIFACT_CONTROL_TABLE)}"
log_fqn = f"{quote_databricks(catalog)}.{quote_databricks(control_schema)}.{quote_databricks(SQL_ARTIFACT_EXECUTION_LOG_TABLE)}"

# COMMAND ----------

# Query run metrics from control table
ctrl_metrics = spark.sql(f"""
    SELECT
        count(*) as selected_candidate_count,
        count(CASE WHEN source_definition IS NOT NULL AND trim(source_definition) <> '' THEN 1 END) as fetched_definition_count,
        count(CASE WHEN error_code = 'DEFINITION_MISSING' OR manual_review_reason LIKE '%DEFINITION_MISSING%' THEN 1 END) as definition_missing_count,
        count(CASE WHEN lakebridge_analyzed_ts IS NOT NULL THEN 1 END) as analyzed_count,
        count(CASE WHEN (lakebridge_transpiled_definition IS NOT NULL AND trim(lakebridge_transpiled_definition) <> '')
                     OR (converted_definition IS NOT NULL AND trim(converted_definition) <> '') THEN 1 END) as transpiled_count,
        count(CASE WHEN lakebridge_classification = 'AUTO_CANDIDATE'
                     OR (lakebridge_classification IS NULL AND conversion_classification = 'AUTO') THEN 1 END) as auto_candidate_count,
        count(CASE WHEN lakebridge_classification = 'MANUAL_REVIEW'
                     OR (lakebridge_classification IS NULL AND conversion_classification = 'MANUAL_REVIEW') THEN 1 END) as manual_review_count,
        count(CASE WHEN lakebridge_classification = 'UNSUPPORTED'
                     OR (lakebridge_classification IS NULL AND conversion_classification = 'UNSUPPORTED') THEN 1 END) as unsupported_count,
        coalesce(sum(lakebridge_parsing_error_count), 0) as parsing_error_count,
        coalesce(sum(lakebridge_validation_error_count), 0) as validation_error_count,
        coalesce(sum(lakebridge_generation_error_count), 0) as generation_error_count,
        count(CASE WHEN coalesce(lakebridge_fixme_count, 0) > 0 THEN 1 END) as fixme_artifact_count,
        count(CASE WHEN conversion_status = 'FAILED' AND error_code <> 'DEFINITION_MISSING' AND lakebridge_analyzed_ts IS NULL THEN 1 END) as source_fetch_failure_count,
        count(CASE WHEN error_code = 'ARTIFACT_WRITE_FAILED' THEN 1 END) as persistent_store_failure_count
    FROM {control_fqn}
    WHERE run_id = {escape_string_literal(run_id)}
""").collect()[0].asDict()

# Query log counts
log_counts = spark.sql(f"""
    SELECT
        count(*) as total_logs,
        count(CASE WHEN status = 'SUCCEEDED' THEN 1 END) as successful_stages,
        count(CASE WHEN status = 'FAILED' THEN 1 END) as failed_stages,
        count(CASE WHEN status = 'SKIPPED' THEN 1 END) as skipped_stages
    FROM {log_fqn}
    WHERE run_id = {escape_string_literal(run_id)}
""").collect()[0].asDict()

selected_candidate_count = ctrl_metrics.get("selected_candidate_count") or 0
fetched_definition_count = ctrl_metrics.get("fetched_definition_count") or 0
definition_missing_count = ctrl_metrics.get("definition_missing_count") or 0
analyzed_count = ctrl_metrics.get("analyzed_count") or 0
transpiled_count = ctrl_metrics.get("transpiled_count") or 0
auto_candidate_count = ctrl_metrics.get("auto_candidate_count") or 0
manual_review_count = ctrl_metrics.get("manual_review_count") or 0
unsupported_count = ctrl_metrics.get("unsupported_count") or 0
parsing_error_count = int(ctrl_metrics.get("parsing_error_count") or 0)
validation_error_count = int(ctrl_metrics.get("validation_error_count") or 0)
generation_error_count = int(ctrl_metrics.get("generation_error_count") or 0)
fixme_artifact_count = ctrl_metrics.get("fixme_artifact_count") or 0
source_fetch_failure_count = ctrl_metrics.get("source_fetch_failure_count") or 0
persistent_store_failure_count = ctrl_metrics.get("persistent_store_failure_count") or 0

# COMMAND ----------

# Determine business status using shared decide_summary
completed_count = auto_candidate_count + manual_review_count
total_failures = (
    definition_missing_count
    + unsupported_count
    + source_fetch_failure_count
    + persistent_store_failure_count
)

total_failed_stages = int(log_counts.get("failed_stages") or 0)

status, business_status = decide_summary(
    ctrl_metrics=ctrl_metrics,
    failed_log_stages=total_failed_stages,
    t05_status=t05_status,
    t24_status=t24_status,
    t24_business_status=t24_business_status,
)

if t05_status and str(t05_status).strip().upper() != "SUCCEEDED":
    status = "FAILED"

if total_failed_stages > 0:
    status = "FAILED"

print(f"Workflow Status: {status}, Business Status: {business_status}")
print("Control Metrics:", json.dumps(ctrl_metrics, indent=2))
print("Execution Log Counts:", json.dumps(log_counts, indent=2))

# COMMAND ----------

# Publish Task Values for Orchestrator (ALWAYS before raising on failure)
set_task_value("run_id", run_id)
set_task_value("status", status)
set_task_value("business_status", business_status)
set_task_value("selected_candidate_count", selected_candidate_count)
set_task_value("fetched_definition_count", fetched_definition_count)
set_task_value("definition_missing_count", definition_missing_count)
set_task_value("analyzed_count", analyzed_count)
set_task_value("transpiled_count", transpiled_count)
set_task_value("auto_candidate_count", auto_candidate_count)
set_task_value("manual_review_count", manual_review_count)
set_task_value("unsupported_count", unsupported_count)
set_task_value("parsing_error_count", parsing_error_count)
set_task_value("validation_error_count", validation_error_count)
set_task_value("generation_error_count", generation_error_count)
set_task_value("fixme_artifact_count", fixme_artifact_count)
set_task_value("source_fetch_failure_count", source_fetch_failure_count)
set_task_value("persistent_store_failure_count", persistent_store_failure_count)

final_result = {
    "status": status,
    "business_status": business_status,
    "run_id": run_id,
    "selected_candidate_count": selected_candidate_count,
    "fetched_definition_count": fetched_definition_count,
    "definition_missing_count": definition_missing_count,
    "analyzed_count": analyzed_count,
    "transpiled_count": transpiled_count,
    "auto_candidate_count": auto_candidate_count,
    "manual_review_count": manual_review_count,
    "unsupported_count": unsupported_count,
    "parsing_error_count": parsing_error_count,
    "validation_error_count": validation_error_count,
    "generation_error_count": generation_error_count,
    "fixme_artifact_count": fixme_artifact_count,
    "source_fetch_failure_count": source_fetch_failure_count,
    "persistent_store_failure_count": persistent_store_failure_count,
    "log_summary": log_counts,
}

if status == "FAILED":
    raise RuntimeError(
        f"NB_SQLArtifactSummary: Artifact workflow run {run_id} failed: "
        f"business_status={business_status}, failures={total_failures}, completed={completed_count}"
    )

dbutils.notebook.exit(json.dumps(final_result))
