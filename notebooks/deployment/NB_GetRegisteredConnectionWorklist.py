# Databricks notebook source
# MAGIC %md
# MAGIC # NB_GetRegisteredConnectionWorklist
# MAGIC Finds distinct active, valid connection IDs that have successfully registered tables
# MAGIC owned by the current Job 1B run.
# MAGIC Output items contain strictly `{"connection_id": "<id>"}`.
# MAGIC Emits each distinct connection_id exactly once.
# MAGIC Performs metadata reads only: no adapter, secret, or JDBC access.

# COMMAND ----------

import json
from pyspark.sql import functions as F

try:
    from src.identifiers import escape_string_literal, quote_databricks
    from src.source_identity import require_source_system, canonical_source_system_sql
    from src.worklist_utils import TASK_VALUE_LIMIT_BYTES, validate_task_value_payload
except ModuleNotFoundError:
    from identifiers import escape_string_literal, quote_databricks
    from source_identity import require_source_system, canonical_source_system_sql
    from worklist_utils import TASK_VALUE_LIMIT_BYTES, validate_task_value_payload

# COMMAND ----------

dbutils.widgets.text("run_id", "")
dbutils.widgets.text("source_system", "")
dbutils.widgets.text("catalog", "da_accelerators")
dbutils.widgets.text("control_schema", "control")
dbutils.widgets.text("only_connection_ids", "")
dbutils.widgets.text("exclude_connection_ids", "")

run_id = dbutils.widgets.get("run_id").strip()
source_system_raw = dbutils.widgets.get("source_system").strip()
catalog = dbutils.widgets.get("catalog").strip() or "da_accelerators"
control_schema = dbutils.widgets.get("control_schema").strip() or "control"

if not run_id:
    raise ValueError("run_id is required")
if not source_system_raw:
    raise ValueError("source_system is required")
if not catalog:
    raise ValueError("catalog is required")
if not control_schema:
    raise ValueError("control_schema is required")

source_system = require_source_system(source_system_raw, "NB_GetRegisteredConnectionWorklist")

raw_only_conns = [
    value.strip() for value in
    dbutils.widgets.get("only_connection_ids").split(",") if value.strip()
]
if len(raw_only_conns) != len(set(raw_only_conns)):
    raise ValueError("only_connection_ids contains duplicate values")

raw_exclude_conns = [
    value.strip() for value in
    dbutils.widgets.get("exclude_connection_ids").split(",") if value.strip()
]
if len(raw_exclude_conns) != len(set(raw_exclude_conns)):
    raise ValueError("exclude_connection_ids contains duplicate values")

def ctrl(t):
    return f"{quote_databricks(catalog)}.{quote_databricks(control_schema)}.{quote_databricks(t)}"

# COMMAND ----------

assessments = spark.table(ctrl("source_assessment")).alias("sa")
connections = spark.table(ctrl("source_connection")).alias("sc")

# Statuses indicating registration completed successfully or advanced downstream in current/repair run
ELIGIBLE_SELECTION_STATUSES = [
    "REGISTERED",
    "INVENTORIED",
    "MAPPED",
    "READY_FOR_PROVISIONING",
    "PROVISIONED",
    "ONBOARDED",
    "REVIEW_REQUIRED",
]

base_registered = (
    assessments.join(
        connections,
        F.col("sa.connection_id") == F.col("sc.connection_id"),
        "inner",
    )
    .filter(F.col("sa.object_type") == F.lit("TABLE"))
    .filter(F.col("sa.is_selected") == F.lit(True))
    .filter(F.col("sa.onboarding_run_id") == F.lit(run_id))
    .filter(F.col("sa.registration_completed_ts").isNotNull())
    .filter(F.upper(F.trim(F.coalesce(F.col("sa.selection_status"), F.lit("")))).isin(ELIGIBLE_SELECTION_STATUSES))
    .filter(F.col("sc.is_active") == F.lit(True))
    .filter(F.upper(F.trim(F.col("sc.connection_status"))) == F.lit("VALID"))
    .filter(F.col("sc.secret_scope").isNotNull() & (F.trim(F.col("sc.secret_scope")) != ""))
    .filter(F.expr(f"{canonical_source_system_sql('sc.source_system')} = {escape_string_literal(source_system)}"))
    .filter(F.expr(f"{canonical_source_system_sql('sa.source_system')} = {canonical_source_system_sql('sc.source_system')}"))
)

# Apply inclusion and exclusion filters (exclusion takes precedence over inclusion)
if raw_only_conns:
    base_registered = base_registered.filter(F.col("sa.connection_id").isin(raw_only_conns))

if raw_exclude_conns:
    base_registered = base_registered.filter(~F.col("sa.connection_id").isin(raw_exclude_conns))

# Distinct connections sorted deterministically
conn_df = (
    base_registered
    .select(F.col("sa.connection_id").alias("connection_id"))
    .distinct()
    .orderBy("connection_id")
)

conn_rows = conn_df.collect()
worklist = [{"connection_id": row["connection_id"]} for row in conn_rows]

# Verify payload strictness
for item in worklist:
    if set(item.keys()) != {"connection_id"}:
        raise ValueError(f"Worklist item contains unexpected keys: {list(item.keys())}")

validate_task_value_payload(worklist, key="worklist", limit_bytes=TASK_VALUE_LIMIT_BYTES)

connection_count = len(worklist)

if worklist:
    conn_keys = [item["connection_id"] for item in worklist]
    registration_owner_count = (
        base_registered
        .filter(F.col("sa.connection_id").isin(conn_keys))
        .count()
    )
else:
    registration_owner_count = 0

# Zero-work validation:
# Do not report success with an empty worklist when successful current-run registrations exist.
raw_current_registrations_count = (
    spark.table(ctrl("source_assessment"))
    .filter(F.col("onboarding_run_id") == F.lit(run_id))
    .filter(F.col("object_type") == F.lit("TABLE"))
    .filter(F.col("is_selected") == F.lit(True))
    .filter(F.col("registration_completed_ts").isNotNull())
    .filter(F.expr(f"{canonical_source_system_sql('source_system')} = {escape_string_literal(source_system)}"))
    .count()
)

if connection_count == 0 and raw_current_registrations_count > 0:
    err_msg = (
        f"EMPTY_REGISTERED_CONNECTION_WORKLIST: Current-run successful registrations exist "
        f"({raw_current_registrations_count} table(s) registered for run_id={run_id!r}, source_system={source_system!r}), "
        f"but no active, valid parent connections matched eligibility or filtering criteria."
    )
    raise RuntimeError(err_msg)

if connection_count == 0:
    business_status = "NO_REGISTRATIONS"
    status = "SUCCEEDED"
else:
    business_status = "READY"
    status = "SUCCEEDED"

dbutils.jobs.taskValues.set(key="run_id", value=run_id)
dbutils.jobs.taskValues.set(key="source_system", value=source_system)
dbutils.jobs.taskValues.set(key="worklist", value=worklist)
dbutils.jobs.taskValues.set(key="connection_count", value=connection_count)
dbutils.jobs.taskValues.set(key="registration_owner_count", value=registration_owner_count)
dbutils.jobs.taskValues.set(key="status", value=status)
dbutils.jobs.taskValues.set(key="business_status", value=business_status)

print(
    f"Registered connection worklist: connections={connection_count}, "
    f"registered_tables={registration_owner_count}, status={status}, "
    f"business_status={business_status}"
)

exit_payload = {
    "status": status,
    "business_status": business_status,
    "run_id": run_id,
    "source_system": source_system,
    "connection_count": connection_count,
    "registration_owner_count": registration_owner_count,
    "worklist": worklist,
}
dbutils.notebook.exit(json.dumps(exit_payload))
