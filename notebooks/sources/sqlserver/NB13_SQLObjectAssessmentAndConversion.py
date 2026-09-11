# Databricks notebook source
# MAGIC %md
# MAGIC # SQL Server / NB13_SQLObjectAssessmentAndConversion
# MAGIC Extracts **SQL Server** object definitions from `sys.sql_modules`, then
# MAGIC hands each definition to the shared classifier/converter. ASSESS records
# MAGIC complexity; CONVERT also produces a limited deterministic draft. Every
# MAGIC generated draft is PENDING_REVIEW and is NEVER executed or deployed. An
# MAGIC encrypted or inaccessible module is UNABLE_TO_ASSESS with an explicit
# MAGIC reason - never fabricated - and an unrecognized module code is skipped
# MAGIC rather than relabelled.

# COMMAND ----------

# MAGIC %run ../../shared/_common

# COMMAND ----------

import uuid as _uuid

SOURCE_SYSTEM = "sqlserver"

dbutils.widgets.text("connection_id", "")
dbutils.widgets.text("assessment_id", "")
dbutils.widgets.dropdown("mode", "ASSESS", ["ASSESS", "CONVERT"])
dbutils.widgets.text("include_schemas", "")
dbutils.widgets.text("include_object_types", "VIEW,PROCEDURE,FUNCTION")
dbutils.widgets.dropdown("use_ai", "false", ["true", "false"])

connection_id = dbutils.widgets.get("connection_id").strip() or CONNECTION_ID
assessment_id = dbutils.widgets.get("assessment_id").strip() or _uuid.uuid4().hex
mode = dbutils.widgets.get("mode").strip()
include_schemas = [s.strip() for s in
                   dbutils.widgets.get("include_schemas").split(",") if s.strip()]
include_types = {t.strip().upper() for t in
                 dbutils.widgets.get("include_object_types").split(",") if t.strip()}
use_ai = dbutils.widgets.get("use_ai") == "true"
run_id = get_run_id()

if not connection_id:
    raise ValueError("connection_id is required")

repo = control_repo()

# COMMAND ----------

connection = repo.get_connection(connection_id)
if connection is None:
    raise ValueError(f"connection_id {connection_id!r} not found")
cd = connection.asDict()
if normalize_source_system(cd["source_system"]) != SOURCE_SYSTEM:
    raise ValueError(
        f"connection_id {connection_id!r} is {cd['source_system']!r}; "
        "this notebook assesses SQL Server SQL objects only")
src_server, src_db = cd.get("source_server"), cd.get("source_database")
if not src_db:
    raise ValueError("SQL Server connections require source_database")
adapter = get_source_adapter_for_connection(connection)   # requires VALID
print(f"NB13 {mode} for SQL Server connection {connection_id} "
      f"(database={src_db}); assessment_id={assessment_id}")


def _q(query):
    return read_source_jdbc(adapter, query, source_server=src_server,
                            source_database=src_db).collect()

# COMMAND ----------

schemas = resolve_assessment_schemas(adapter, src_db, include_schemas, [])

# ---- SQL Server definition extraction (sys.sql_modules) --------------------
definitions = []       # (schema, object_name, normalized_type, definition_text)
skipped_types = []

for schema in schemas:
    try:
        for m in _q(adapter.module_definition_query(src_db, schema)):
            raw_code = m["OBJECT_TYPE"]
            otype = adapter.normalize_sql_object_type(raw_code)
            if not otype:
                skipped_types.append((schema, m["OBJECT_NAME"], raw_code))
                continue
            if otype not in include_types:
                continue
            # A NULL definition means encrypted or not visible to this login.
            definitions.append((schema, m["OBJECT_NAME"], otype,
                                m["DEFINITION_TEXT"]))
    except Exception as e:
        print(f"  [warn] SQL Server module discovery {schema}: "
              f"{failcls.sanitize_message(e)[:200]}")

print(f"Collected {len(definitions)} SQL Server object definition(s).")
if skipped_types:
    print(f"Skipped {len(skipped_types)} unsupported module type(s); "
          f"examples: {skipped_types[:5]}")

# COMMAND ----------

records = [
    sqlobj_common.build_sql_object_record(
        assessment_id=assessment_id, run_id=run_id, connection_id=connection_id,
        source_system=SOURCE_SYSTEM, source_database=src_db,
        source_schema=schema, object_name=name, object_type=otype,
        source_definition=definition, mode=mode, use_ai=use_ai)
    for schema, name, otype, definition in definitions
]

summary = persist_sql_object_records(records)
print("Complexity summary:", summary)

dbutils.notebook.exit(json.dumps({
    "status": "SUCCEEDED", "run_id": run_id, "connection_id": connection_id,
    "assessment_id": assessment_id, "objects": len(records), "summary": summary,
}))
