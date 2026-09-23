# Databricks notebook source
# MAGIC %md
# MAGIC # NB_GetAssessmentDatabaseWorklist
# MAGIC Post-validation SQL Server database worklist generator for Job 1A.
# MAGIC For each validated SQL Server connection:
# MAGIC - If source_database is populated: emits one work item for that configured database.
# MAGIC - If source_database is blank: connects via master temporarily, discovers all
# MAGIC   online accessible non-system databases, and emits one work item per database.
# MAGIC Publishes a safe task-value worklist for Job 1A assessment and inventory iterations.
# MAGIC Never prints credentials or secret values.

# COMMAND ----------

# MAGIC %run ../shared/_common

# COMMAND ----------

import json
from pyspark.sql import functions as F

try:
    from src.identifiers import escape_string_literal, quote_databricks
    from src.source_identity import require_source_system, canonical_source_system_sql
    from src.worklist_utils import TASK_VALUE_LIMIT_BYTES, validate_task_value_payload
    from src import failure_classifier as failcls
except ModuleNotFoundError:
    from identifiers import escape_string_literal, quote_databricks
    from source_identity import require_source_system, canonical_source_system_sql
    from worklist_utils import TASK_VALUE_LIMIT_BYTES, validate_task_value_payload
    import failure_classifier as failcls

SOURCE_SYSTEM = "sqlserver"
SYSTEM_DATABASES = {"master", "model", "msdb", "tempdb"}

dbutils.widgets.text("run_id", "")
dbutils.widgets.text("catalog", "da_accelerators")
dbutils.widgets.text("control_schema", "control")
dbutils.widgets.text("max_connections", "0")
dbutils.widgets.text("only_connection_ids", "")
dbutils.widgets.text("exclude_connection_ids", "")

run_id = dbutils.widgets.get("run_id").strip() or get_run_id()
catalog = dbutils.widgets.get("catalog").strip() or CATALOG
control_schema = dbutils.widgets.get("control_schema").strip() or CONTROL_SCHEMA

if not run_id:
    raise ValueError("run_id is required")
if not catalog or not control_schema:
    raise ValueError("catalog and control_schema are required")

try:
    max_connections = int(dbutils.widgets.get("max_connections").strip() or "0")
except ValueError as exc:
    raise ValueError("max_connections must be a non-negative integer") from exc
if max_connections < 0:
    raise ValueError("max_connections must be a non-negative integer")

raw_only = [
    v.strip() for v in dbutils.widgets.get("only_connection_ids").split(",") if v.strip()
]
if len(raw_only) != len(set(raw_only)):
    raise ValueError("only_connection_ids contains duplicate values")

raw_exclude = [
    v.strip() for v in dbutils.widgets.get("exclude_connection_ids").split(",") if v.strip()
]
if len(raw_exclude) != len(set(raw_exclude)):
    raise ValueError("exclude_connection_ids contains duplicate values")

clean_exclude = set(raw_exclude)
clean_only = {v for v in raw_only if v not in clean_exclude}

def _fqn(table_name):
    return f"{quote_databricks(catalog)}.{quote_databricks(control_schema)}.{quote_databricks(table_name)}"

connections = spark.table(_fqn("source_connection")).alias("sc")

eligible = (
    connections
    .filter(F.col("sc.connection_id").isNotNull())
    .filter(F.trim(F.col("sc.connection_id")) != "")
    .filter(F.col("sc.source_server").isNotNull() & (F.trim(F.col("sc.source_server")) != ""))
    .filter(F.col("sc.secret_scope").isNotNull() & (F.trim(F.col("sc.secret_scope")) != ""))
    .filter(F.expr(f"{canonical_source_system_sql('sc.source_system')} = {escape_string_literal(SOURCE_SYSTEM)}"))
    .filter(F.col("sc.is_active") == F.lit(True))
    .filter(F.upper(F.col("sc.connection_status")) == F.lit("VALID"))
)

if raw_only and not clean_only:
    eligible = eligible.filter(F.lit(False))
elif clean_only:
    eligible = eligible.filter(F.col("sc.connection_id").isin(list(clean_only)))

if clean_exclude:
    eligible = eligible.filter(~F.col("sc.connection_id").isin(list(clean_exclude)))

eligible = (
    eligible.select(
        F.col("sc.connection_id").alias("connection_id"),
        F.col("sc.source_server").alias("source_server"),
        F.col("sc.source_database").alias("source_database"),
        F.col("sc.secret_scope").alias("secret_scope"),
        F.col("sc.source_system").alias("source_system"),
        F.col("sc.trust_server_certificate").alias("trust_server_certificate"),
        F.col("sc.is_active").alias("is_active"),
        F.col("sc.connection_status").alias("connection_status")
    )
    .orderBy("connection_id")
)

if max_connections > 0:
    eligible = eligible.limit(max_connections)

conn_rows = eligible.collect()

worklist = []
connections_processed = 0
databases_emitted = 0
discovery_connections = 0
configured_database_connections = 0
failed_connections = 0
errors = []

for conn in conn_rows:
    connections_processed += 1
    conn_id = conn["connection_id"]
    configured_db = str(conn["source_database"] or "").strip()

    if configured_db:
        # A. Populated source_database: emit exactly one work item, do not run discovery
        configured_database_connections += 1
        worklist.append({
            "connection_id": conn_id,
            "source_database": configured_db,
        })
        databases_emitted += 1
    else:
        # B. Blank source_database: discover accessible user databases via temporary master bootstrap
        discovery_connections += 1
        try:
            bootstrap_adapter = get_source_adapter_for_connection(
                conn, source_database="master", require_valid=True
            )
            disc_query = bootstrap_adapter.accessible_databases_query()
            db_df = read_source_jdbc(
                bootstrap_adapter,
                disc_query,
                source_server=conn["source_server"],
                source_database="master"
            )
            raw_db_rows = db_df.collect()

            seen_lower = set()
            discovered_dbs = []
            for r in raw_db_rows:
                raw_name = str(r["database_name"] or "").strip()
                if not raw_name:
                    continue
                lower_name = raw_name.lower()
                if lower_name in SYSTEM_DATABASES:
                    continue
                if lower_name not in seen_lower:
                    seen_lower.add(lower_name)
                    discovered_dbs.append(raw_name)

            if not discovered_dbs:
                raise RuntimeError(
                    f"Connection '{conn_id}' discovered 0 accessible user databases"
                )

            for db_name in discovered_dbs:
                worklist.append({
                    "connection_id": conn_id,
                    "source_database": db_name,
                })
                databases_emitted += 1

        except Exception as exc:
            failed_connections += 1
            safe_err = failcls.sanitize_message(exc)
            errors.append(f"Connection '{conn_id}' discovery failed: {safe_err[:400]}")
            print(f"  [error] Connection {conn_id} discovery failed: {safe_err[:300]}")

# Sort work items deterministically by connection_id and source_database
worklist.sort(key=lambda x: (x["connection_id"], x["source_database"].casefold()))

# Validate worklist keys
for item in worklist:
    if set(item.keys()) != {"connection_id", "source_database"}:
        raise ValueError(f"Worklist item contains unexpected keys: {list(item.keys())}")

# Validate task-value payload size
validate_task_value_payload(worklist, key="worklist", limit_bytes=TASK_VALUE_LIMIT_BYTES)

worklist_count = len(worklist)

if connections_processed == 0:
    business_status = "NO_ELIGIBLE_CONNECTIONS"
elif failed_connections > 0 and worklist_count == 0:
    business_status = "FAILED"
elif failed_connections > 0:
    business_status = "PARTIAL"
else:
    business_status = "READY"

execution_status = "FAILED" if business_status == "FAILED" else "SUCCEEDED"

dbutils.jobs.taskValues.set(key="run_id", value=run_id)
dbutils.jobs.taskValues.set(key="source_system", value=SOURCE_SYSTEM)
dbutils.jobs.taskValues.set(key="worklist", value=worklist)
dbutils.jobs.taskValues.set(key="worklist_count", value=worklist_count)
dbutils.jobs.taskValues.set(key="databases_emitted", value=databases_emitted)
dbutils.jobs.taskValues.set(key="connections_processed", value=connections_processed)
dbutils.jobs.taskValues.set(key="business_status", value=business_status)

print(
    f"Assessment database worklist: count={worklist_count}, "
    f"connections={connections_processed}, configured={configured_database_connections}, "
    f"discovery={discovery_connections}, failed={failed_connections}, status={business_status}"
)

exit_payload = {
    "status": execution_status,
    "execution_status": execution_status,
    "business_status": business_status,
    "run_id": run_id,
    "source_system": SOURCE_SYSTEM,
    "connections_processed": connections_processed,
    "databases_emitted": databases_emitted,
    "discovery_connections": discovery_connections,
    "configured_database_connections": configured_database_connections,
    "failed_connections": failed_connections,
    "worklist_count": worklist_count,
    "errors": errors[:20],
}
if worklist_count > 0:
    exit_payload["worklist"] = worklist

if business_status == "FAILED":
    print(json.dumps(exit_payload))
    raise RuntimeError(
        f"Database worklist generation failed: {len(errors)} connection error(s)"
    )

dbutils.notebook.exit(json.dumps(exit_payload))
