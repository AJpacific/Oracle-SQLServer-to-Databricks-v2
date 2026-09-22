# Databricks notebook source
# MAGIC %md
# MAGIC # SQL Server / NB13_SQLObjectAssessmentAndConversion
# MAGIC Extracts and inventories original source SQL-object definitions for
# MAGIC unchanged artifact materialization. Reads **SQL Server** object
# MAGIC definitions from `sys.sql_modules` and stores each definition exactly as
# MAGIC extracted in `sql_object_assessment`. It never classifies, converts,
# MAGIC reviews, executes, or deploys source SQL. An encrypted or inaccessible
# MAGIC module is recorded with an explicit reason - never fabricated - and an
# MAGIC unrecognized module code is skipped rather than relabelled.

# COMMAND ----------

# MAGIC %run ../../shared/_common

# COMMAND ----------

import uuid as _uuid

SOURCE_SYSTEM = "sqlserver"

dbutils.widgets.text("connection_id", "")
dbutils.widgets.text("assessment_id", "")
dbutils.widgets.text("include_schemas", "")
dbutils.widgets.text("include_object_types", "VIEW,PROCEDURE,FUNCTION")

connection_id = dbutils.widgets.get("connection_id").strip() or CONNECTION_ID
assessment_id = dbutils.widgets.get("assessment_id").strip() or _uuid.uuid4().hex
include_schemas = [s.strip() for s in
                   dbutils.widgets.get("include_schemas").split(",") if s.strip()]
include_types = {t.strip().upper() for t in
                 dbutils.widgets.get("include_object_types").split(",") if t.strip()}
run_id = get_run_id()

connection_id = require_connection_id(
    connection_id, "SQL Server SQL-object inventory")

repo = control_repo()

# COMMAND ----------

connection = require_valid_connection(connection_id, SOURCE_SYSTEM)
cd = connection.asDict()
src_server, src_db = cd.get("source_server"), cd.get("source_database")
if not src_db:
    raise ValueError("SQL Server connections require source_database")
adapter = get_source_adapter_for_connection(connection)   # requires VALID
print(f"NB13 source SQL object inventory for SQL Server connection "
      f"{connection_id} (database={src_db}); assessment_id={assessment_id}")


def _q(query):
    return read_source_jdbc(adapter, query, source_server=src_server,
                            source_database=src_db).collect()


discovery_errors = []


def _capture_discovery_error(stage, error, source_schema=None):
    safe_message = failcls.sanitize_message(error)[:400]
    detail = {
        "stage": stage,
        "source_schema": source_schema,
        "exception_type": type(error).__name__,
        "message": safe_message,
    }
    discovery_errors.append(detail)
    location = f" for schema {source_schema}" if source_schema else ""
    print(f"  [warn] {stage}{location}: {detail['exception_type']}: "
          f"{safe_message}")

# COMMAND ----------

schema_discovery_failed = False
try:
    schemas = resolve_assessment_schemas(adapter, src_db, include_schemas, [])
except Exception as e:
    schemas = []
    schema_discovery_failed = True
    _capture_discovery_error("schema_discovery", e)

# ---- SQL Server definition extraction (sys.sql_modules) --------------------
definitions = []       # (schema, object_name, normalized_type, definition_text)
skipped_types = []
discovered_objects = 0
inaccessible_definitions = 0
object_discovery_attempts = 0
object_discovery_successes = 0

for schema in schemas:
    object_discovery_attempts += 1
    try:
        modules = _q(adapter.module_definition_query(src_db, schema))
        object_discovery_successes += 1
    except Exception as e:
        modules = []
        _capture_discovery_error("module_discovery", e, schema)
    for m in modules:
        raw_code = m["OBJECT_TYPE"]
        otype = adapter.normalize_sql_object_type(raw_code)
        if not otype:
            skipped_types.append((schema, m["OBJECT_NAME"], raw_code))
            discovered_objects += 1
            continue
        if otype not in include_types:
            continue
        discovered_objects += 1
        # A NULL definition means encrypted or not visible to this login.
        definition = m["DEFINITION_TEXT"]
        if not definition or not str(definition).strip():
            inaccessible_definitions += 1
        definitions.append((schema, m["OBJECT_NAME"], otype, definition))

print(f"Collected {len(definitions)} SQL Server object definition(s).")
if skipped_types:
    print(f"Skipped {len(skipped_types)} unsupported module type(s).")

# COMMAND ----------

records = [
    sqlobj_common.build_sql_object_record(
        assessment_id=assessment_id, run_id=run_id, connection_id=connection_id,
        source_system=SOURCE_SYSTEM, source_database=src_db,
        source_schema=schema, object_name=name, object_type=otype,
        source_definition=definition)
    for schema, name, otype, definition in definitions
]

summary = persist_sql_object_records(records)
print("Source SQL object inventory summary:", summary)

business_status = sqlobj_common.discovery_business_status(
    schema_discovery_failed=schema_discovery_failed,
    object_discovery_attempts=object_discovery_attempts,
    object_discovery_successes=object_discovery_successes,
    discovery_failures=len(discovery_errors),
    inaccessible_definitions=inaccessible_definitions,
    unsupported_object_types=len(skipped_types))
sql_object_result = {
    "status": "SUCCEEDED",
    "execution_status": "SUCCEEDED",
    "business_status": business_status,
    "run_id": run_id,
    "connection_id": connection_id,
    "assessment_id": assessment_id,
    "discovered_objects": discovered_objects,
    "persisted_objects": len(records),
    "inaccessible_definitions": inaccessible_definitions,
    "unsupported_object_types": len(skipped_types),
    "discovery_failures": len(discovery_errors),
    "errors": discovery_errors[:sqlobj_common.DISCOVERY_ERROR_LIMIT],
    # Backward-compatible aliases.
    "objects": len(records),
    "summary": summary,
}
dbutils.notebook.exit(json.dumps(sql_object_result))
