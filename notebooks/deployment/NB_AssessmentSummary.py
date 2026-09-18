# Databricks notebook source
# MAGIC %md
# MAGIC # NB_AssessmentSummary
# MAGIC Summarizes persisted assessment results across all connection-level assessment
# MAGIC iterations sharing the same run_id and normalized source system.
# MAGIC Performs metadata reads only: no adapter, secret, or JDBC access.

# COMMAND ----------

# MAGIC %run ../shared/_common

# COMMAND ----------

import json
from collections import defaultdict

try:
    from src.identifiers import escape_string_literal, quote_databricks
    from src.source_identity import require_source_system, canonical_source_system_sql
except ModuleNotFoundError:
    from identifiers import escape_string_literal, quote_databricks
    from source_identity import require_source_system, canonical_source_system_sql

dbutils.widgets.text("run_id", "")
dbutils.widgets.text("source_system", "")
dbutils.widgets.text("catalog", "da_accelerators")
dbutils.widgets.text("control_schema", "control")

run_id = dbutils.widgets.get("run_id").strip()
source_system_raw = dbutils.widgets.get("source_system").strip()
catalog = dbutils.widgets.get("catalog").strip() or CATALOG
control_schema = dbutils.widgets.get("control_schema").strip() or CONTROL_SCHEMA

if not run_id:
    raise ValueError("run_id is required")
if not source_system_raw:
    raise ValueError("source_system is required")
if not catalog or not control_schema:
    raise ValueError("catalog and control_schema are required")

source_system = require_source_system(source_system_raw, "NB_AssessmentSummary")

def _fqn(t):
    return f"{quote_databricks(catalog)}.{quote_databricks(control_schema)}.{quote_databricks(t)}"

# Query persisted assessments scoped to run_id, joining source_connection to validate ownership
query = f"""
    SELECT
        sa.connection_id,
        sa.assessment_id,
        sa.object_type,
        sa.compatibility_status,
        sa.is_selected,
        sa.selection_status
    FROM {_fqn('source_assessment')} sa
    JOIN {_fqn('source_connection')} sc
      ON sa.connection_id = sc.connection_id
    WHERE sa.run_id = {escape_string_literal(run_id)}
      AND {canonical_source_system_sql('sc.source_system')} = {escape_string_literal(source_system)}
      AND {canonical_source_system_sql('sa.source_system')} = {canonical_source_system_sql('sc.source_system')}
"""

rows = spark.sql(query).collect()

if not rows:
    business_status = "NO_RESULTS"
    exit_payload = {
        "status": "SUCCEEDED",
        "business_status": "NO_RESULTS",
        "run_id": run_id,
        "source_system": source_system,
        "connections_assessed": 0,
        "assessments": 0,
        "objects_assessed": 0,
        "selected_table_count": 0,
    }
    dbutils.jobs.taskValues.set(key="run_id", value=run_id)
    dbutils.jobs.taskValues.set(key="source_system", value=source_system)
    dbutils.jobs.taskValues.set(key="connections_assessed", value=0)
    dbutils.jobs.taskValues.set(key="assessments", value=0)
    dbutils.jobs.taskValues.set(key="objects_assessed", value=0)
    dbutils.jobs.taskValues.set(key="selected_table_count", value=0)
    dbutils.jobs.taskValues.set(key="business_status", value=business_status)
    print(f"Assessment summary: NO_RESULTS for run_id={run_id}, source_system={source_system}")
    dbutils.notebook.exit(json.dumps(exit_payload))

# Compute aggregate metrics safely
connections_assessed = len({r["connection_id"] for r in rows})
assessments_set = {(r["connection_id"], r["assessment_id"]) for r in rows}
assessments_count = len(assessments_set)
objects_assessed = len(rows)

table_count = sum(1 for r in rows if str(r.get("object_type") or "").upper() == "TABLE")
view_count = sum(1 for r in rows if str(r.get("object_type") or "").upper() == "VIEW")
procedure_count = sum(1 for r in rows if str(r.get("object_type") or "").upper() == "PROCEDURE")
function_count = sum(1 for r in rows if str(r.get("object_type") or "").upper() == "FUNCTION")
package_count = sum(1 for r in rows if str(r.get("object_type") or "").upper() in ("PACKAGE", "PACKAGE_BODY"))

compatible_count = sum(1 for r in rows if str(r.get("compatibility_status") or "").upper() == "COMPATIBLE")
review_count = sum(1 for r in rows if str(r.get("compatibility_status") or "").upper() == "REVIEW")
manual_count = sum(1 for r in rows if str(r.get("compatibility_status") or "").upper() == "MANUAL")
unable_count = sum(1 for r in rows if str(r.get("compatibility_status") or "").upper() in ("UNABLE_TO_ASSESS", "UNABLE"))

selected_table_count = sum(
    1 for r in rows
    if str(r.get("object_type") or "").upper() == "TABLE" and r.get("is_selected") is True
)

business_status = "PARTIAL" if (unable_count > 0 or manual_count > 0) else "COMPLETE"

# Per-connection and per-assessment deterministic breakdown
grouped = defaultdict(list)
for r in rows:
    grouped[(r["connection_id"], r["assessment_id"])].append(r)

connection_summaries = []
for (cid, aid) in sorted(grouped.keys()):
    batch_rows = grouped[(cid, aid)]
    b_tables = sum(1 for r in batch_rows if str(r.get("object_type") or "").upper() == "TABLE")
    b_views = sum(1 for r in batch_rows if str(r.get("object_type") or "").upper() == "VIEW")
    b_compat = sum(1 for r in batch_rows if str(r.get("compatibility_status") or "").upper() == "COMPATIBLE")
    b_review = sum(1 for r in batch_rows if str(r.get("compatibility_status") or "").upper() == "REVIEW")
    b_manual = sum(1 for r in batch_rows if str(r.get("compatibility_status") or "").upper() == "MANUAL")
    b_unable = sum(1 for r in batch_rows if str(r.get("compatibility_status") or "").upper() in ("UNABLE_TO_ASSESS", "UNABLE"))
    b_selected = sum(
        1 for r in batch_rows
        if str(r.get("object_type") or "").upper() == "TABLE" and r.get("is_selected") is True
    )
    b_status = "PARTIAL" if (b_unable > 0 or b_manual > 0) else "COMPLETE"
    connection_summaries.append({
        "connection_id": cid,
        "assessment_id": aid,
        "objects_assessed": len(batch_rows),
        "table_count": b_tables,
        "view_count": b_views,
        "compatible_count": b_compat,
        "review_count": b_review,
        "manual_count": b_manual,
        "unable_count": b_unable,
        "selected_table_count": b_selected,
        "business_status": b_status,
    })

dbutils.jobs.taskValues.set(key="run_id", value=run_id)
dbutils.jobs.taskValues.set(key="source_system", value=source_system)
dbutils.jobs.taskValues.set(key="connections_assessed", value=connections_assessed)
dbutils.jobs.taskValues.set(key="assessments", value=assessments_count)
dbutils.jobs.taskValues.set(key="objects_assessed", value=objects_assessed)
dbutils.jobs.taskValues.set(key="selected_table_count", value=selected_table_count)
dbutils.jobs.taskValues.set(key="business_status", value=business_status)

print(
    f"Assessment summary: connections={connections_assessed}, assessments={assessments_count}, "
    f"objects={objects_assessed}, selected_tables={selected_table_count}, status={business_status}"
)

exit_payload = {
    "status": "SUCCEEDED",
    "business_status": business_status,
    "run_id": run_id,
    "source_system": source_system,
    "connections_assessed": connections_assessed,
    "assessments": assessments_count,
    "objects_assessed": objects_assessed,
    "table_count": table_count,
    "view_count": view_count,
    "procedure_count": procedure_count,
    "function_count": function_count,
    "package_count": package_count,
    "compatible_count": compatible_count,
    "review_count": review_count,
    "manual_count": manual_count,
    "unable_count": unable_count,
    "selected_table_count": selected_table_count,
    "summaries": connection_summaries,
}

dbutils.notebook.exit(json.dumps(exit_payload))
