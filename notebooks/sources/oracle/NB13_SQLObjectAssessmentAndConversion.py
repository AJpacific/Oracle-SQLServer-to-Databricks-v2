# Databricks notebook source
# MAGIC %md
# MAGIC # Oracle / NB13_SQLObjectAssessmentAndConversion
# MAGIC Extracts **Oracle** view text (`ALL_VIEWS`) and routine/package source
# MAGIC (`ALL_SOURCE`, assembled in line order), then hands each definition to the
# MAGIC shared classifier/converter. ASSESS records complexity; CONVERT also
# MAGIC produces a limited deterministic draft. Every generated draft is
# MAGIC PENDING_REVIEW and is NEVER executed or deployed. An inaccessible
# MAGIC definition is UNABLE_TO_ASSESS with an explicit reason - never fabricated.

# COMMAND ----------

# MAGIC %run ../../shared/_common

# COMMAND ----------

import uuid as _uuid

SOURCE_SYSTEM = "oracle"

dbutils.widgets.text("connection_id", "")
dbutils.widgets.text("assessment_id", "")
dbutils.widgets.dropdown("mode", "ASSESS", ["ASSESS", "CONVERT"])
dbutils.widgets.text("include_schemas", "")
dbutils.widgets.text("include_object_types",
                     "VIEW,PROCEDURE,FUNCTION,PACKAGE")
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

connection_id = require_connection_id(connection_id, "Oracle SQL-object assessment")

repo = control_repo()

# COMMAND ----------

connection = require_valid_connection(connection_id, SOURCE_SYSTEM)
cd = connection.asDict()
src_server, src_db = cd.get("source_server"), cd.get("source_database")
adapter = get_source_adapter_for_connection(connection)   # requires VALID
print(f"NB13 {mode} for Oracle connection {connection_id}; "
      f"assessment_id={assessment_id}")


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

# ---- Oracle definition extraction ------------------------------------------
definitions = []       # (schema, object_name, normalized_type, definition_text)
skipped_types = []
discovered_objects = 0
inaccessible_definitions = 0
object_discovery_attempts = 0
object_discovery_successes = 0

for schema in schemas:
    if "VIEW" in include_types:
        object_discovery_attempts += 1
        try:
            views = _q(adapter.list_views_query(src_db, schema))
            object_discovery_successes += 1
        except Exception as e:
            views = []
            _capture_discovery_error("view_discovery", e, schema)
        for v in views:
            name = v["OBJECT_NAME"]
            discovered_objects += 1
            try:
                text = _q(adapter.view_text_query(src_db, schema, name))
                definition = text[0]["DEFINITION_TEXT"] if text else None
            except Exception as e:
                definition = None
                safe_message = failcls.sanitize_message(e)[:200]
                print(f"  [warn] inaccessible view definition "
                      f"{schema}.{name}: {safe_message}")
            if not definition or not str(definition).strip():
                inaccessible_definitions += 1
            definitions.append((schema, name, "VIEW", definition))

    if include_types & {"PROCEDURE", "FUNCTION", "PACKAGE", "PACKAGE_BODY"}:
        object_discovery_attempts += 1
        try:
            routines = _q(adapter.list_routines_query(src_db, schema))
            object_discovery_successes += 1
        except Exception as e:
            routines = []
            _capture_discovery_error("routine_discovery", e, schema)
        for rt in routines:
            raw_type = rt["OBJECT_TYPE"]
            otype = adapter.normalize_sql_object_type(raw_type)
            if not otype:
                skipped_types.append((schema, rt["OBJECT_NAME"], raw_type))
                discovered_objects += 1
                continue
            requested = (otype in include_types
                         or (otype == "PACKAGE_BODY" and "PACKAGE" in include_types))
            if not requested:
                continue
            discovered_objects += 1
            try:
                # ALL_SOURCE returns one row per line; assemble in line order.
                lines = _q(adapter.object_source_query(
                    src_db, schema, rt["OBJECT_NAME"], raw_type))
                definition = ("".join(l["SOURCE_TEXT"] or "" for l in lines)
                              if lines else None)
            except Exception as e:
                definition = None
                safe_message = failcls.sanitize_message(e)[:200]
                print(f"  [warn] ALL_SOURCE {schema}.{rt['OBJECT_NAME']}: "
                      f"{safe_message}")
            if not definition or not str(definition).strip():
                inaccessible_definitions += 1
            definitions.append((schema, rt["OBJECT_NAME"], otype, definition))

print(f"Collected {len(definitions)} Oracle object definition(s).")
if skipped_types:
    print(f"Skipped {len(skipped_types)} unsupported object type(s).")

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
