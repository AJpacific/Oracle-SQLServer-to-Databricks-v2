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
        "this notebook assesses Oracle SQL objects only")
src_server, src_db = cd.get("source_server"), cd.get("source_database")
adapter = get_source_adapter_for_connection(connection)   # requires VALID
print(f"NB13 {mode} for Oracle connection {connection_id}; "
      f"assessment_id={assessment_id}")


def _q(query):
    return read_source_jdbc(adapter, query, source_server=src_server,
                            source_database=src_db).collect()

# COMMAND ----------

schemas = resolve_assessment_schemas(adapter, src_db, include_schemas, [])

# ---- Oracle definition extraction ------------------------------------------
definitions = []       # (schema, object_name, normalized_type, definition_text)
skipped_types = []

for schema in schemas:
    if "VIEW" in include_types:
        try:
            for v in _q(adapter.list_views_query(src_db, schema)):
                name = v["OBJECT_NAME"]
                text = _q(adapter.view_text_query(src_db, schema, name))
                definitions.append(
                    (schema, name, "VIEW",
                     text[0]["DEFINITION_TEXT"] if text else None))
        except Exception as e:
            print(f"  [warn] Oracle view discovery {schema}: "
                  f"{failcls.sanitize_message(e)[:200]}")

    try:
        for rt in _q(adapter.list_routines_query(src_db, schema)):
            raw_type = rt["OBJECT_TYPE"]
            otype = adapter.normalize_sql_object_type(raw_type)
            if not otype:
                skipped_types.append((schema, rt["OBJECT_NAME"], raw_type))
                continue
            requested = (otype in include_types
                         or (otype == "PACKAGE_BODY" and "PACKAGE" in include_types))
            if not requested:
                continue
            try:
                # ALL_SOURCE returns one row per line; assemble in line order.
                lines = _q(adapter.object_source_query(
                    src_db, schema, rt["OBJECT_NAME"], raw_type))
                definition = ("".join(l["SOURCE_TEXT"] or "" for l in lines)
                              if lines else None)
            except Exception as e:
                definition = None
                print(f"  [warn] ALL_SOURCE {schema}.{rt['OBJECT_NAME']}: "
                      f"{failcls.sanitize_message(e)[:150]}")
            definitions.append((schema, rt["OBJECT_NAME"], otype, definition))
    except Exception as e:
        print(f"  [warn] Oracle routine discovery {schema}: "
              f"{failcls.sanitize_message(e)[:200]}")

print(f"Collected {len(definitions)} Oracle object definition(s).")
if skipped_types:
    print(f"Skipped {len(skipped_types)} unsupported object type(s); "
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
