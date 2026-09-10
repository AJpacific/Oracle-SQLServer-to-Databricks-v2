# Databricks notebook source
# MAGIC %md
# MAGIC # NB13_SQLObjectAssessmentAndConversion
# MAGIC INGEST-pipeline assessment of source views, procedures, functions and
# MAGIC packages. ASSESS captures each definition (Oracle ALL_VIEWS / ALL_SOURCE,
# MAGIC SQL Server sys.sql_modules), classifies conversion complexity, and records
# MAGIC it. CONVERT additionally produces a limited, deterministic Databricks SQL
# MAGIC draft for simple views. Every generated result is PENDING_REVIEW and is
# MAGIC NEVER executed or deployed. Inaccessible/encrypted definitions are
# MAGIC UNABLE_TO_ASSESS - text is never fabricated.

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

import uuid as _uuid

dbutils.widgets.text("connection_id", "")
dbutils.widgets.dropdown("mode", "ASSESS", ["ASSESS", "CONVERT"])
dbutils.widgets.text("include_schemas", "")
dbutils.widgets.text("include_object_types", "VIEW,PROCEDURE,FUNCTION,PACKAGE")
dbutils.widgets.text("assessment_id", "")
dbutils.widgets.text("run_id", "")
dbutils.widgets.dropdown("use_ai", "false", ["true", "false"])

connection_id = dbutils.widgets.get("connection_id").strip()
mode = dbutils.widgets.get("mode").strip()
include_schemas = [s.strip() for s in dbutils.widgets.get("include_schemas").split(",") if s.strip()]
include_types = {t.strip().upper() for t in dbutils.widgets.get("include_object_types").split(",") if t.strip()}
assessment_id = dbutils.widgets.get("assessment_id").strip() or _uuid.uuid4().hex
run_id = dbutils.widgets.get("run_id").strip() or get_run_id()
use_ai = dbutils.widgets.get("use_ai") == "true"

if not connection_id:
    raise ValueError("connection_id is required")

repo = control_repo()

def ctrl(t):
    return f"{quote_databricks(CATALOG)}.{quote_databricks(CONTROL_SCHEMA)}.{quote_databricks(t)}"

# COMMAND ----------

connection = repo.get_connection(connection_id)
if connection is None:
    raise ValueError(f"connection_id {connection_id!r} not found")
cd = connection.asDict()
src_system = cd["source_system"]
src_server = cd.get("source_server")
src_db = cd.get("source_database")
adapter = get_source_adapter_for_connection(connection)   # requires VALID
print(f"NB13 {mode} for {connection_id} ({src_system}); assessment_id={assessment_id}")

def _q(query):
    return read_source_jdbc(adapter, query, source_server=src_server,
                            source_database=src_db).collect()

# COMMAND ----------

# ---- resolve schemas -------------------------------------------------------
all_schemas = [r["SCHEMA_NAME"] for r in _q(adapter.list_schemas_query(src_db))]
schemas = [s for s in all_schemas if (not include_schemas or s in include_schemas)]
print(f"Schemas: {len(schemas)}")

# ---- collect (schema, object, type, definition) ----------------------------
objects = []

_SS_TYPE = {"V": "VIEW", "P": "PROCEDURE", "FN": "FUNCTION", "IF": "FUNCTION",
            "TF": "FUNCTION", "AF": "FUNCTION"}

for schema in schemas:
    if src_system == "oracle":
        if "VIEW" in include_types:
            try:
                for v in _q(adapter.list_views_query(src_db, schema)):
                    name = v["OBJECT_NAME"]
                    txt = _q(adapter.view_text_query(src_db, schema, name))
                    definition = txt[0]["DEFINITION_TEXT"] if txt else None
                    objects.append((schema, name, "VIEW", definition))
            except Exception as e:
                print(f"  [warn] Oracle view discovery {schema}: {str(e)[:200]}")
        try:
            for rt in _q(adapter.list_routines_query(src_db, schema)):
                otype = (rt["OBJECT_TYPE"] or "").upper()
                norm = otype.replace(" ", "_")
                if norm.split("_")[0] not in include_types and norm not in include_types:
                    continue
                try:
                    lines = _q(adapter.object_source_query(src_db, schema,
                                                           rt["OBJECT_NAME"], otype))
                    definition = ("".join(l["SOURCE_TEXT"] or "" for l in lines)
                                  if lines else None)
                except Exception as e:
                    definition = None
                    print(f"  [warn] Oracle source {schema}.{rt['OBJECT_NAME']}: {str(e)[:150]}")
                objects.append((schema, rt["OBJECT_NAME"], norm, definition))
        except Exception as e:
            print(f"  [warn] Oracle routine discovery {schema}: {str(e)[:200]}")
    else:
        try:
            for m in _q(adapter.module_definition_query(src_db, schema)):
                otype = _SS_TYPE.get((m["OBJECT_TYPE"] or "").strip(), "PROCEDURE")
                if otype not in include_types:
                    continue
                objects.append((schema, m["OBJECT_NAME"], otype, m["DEFINITION_TEXT"]))
        except Exception as e:
            print(f"  [warn] SQL Server module discovery {schema}: {str(e)[:200]}")

print(f"Collected {len(objects)} object definition(s).")

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql import functions as F

rows = []
for schema, name, otype, definition in objects:
    complexity, reason = sqlconv.classify_sql_object(src_system, otype, definition)
    converted, language, conv_status = None, None, "NOT_STARTED"
    review_status = "NOT_REVIEWED"
    error_message = None if definition else "definition inaccessible or encrypted"

    if mode == "CONVERT" and definition:
        converted, language, conv_status = sqlconv.convert_sql_object_deterministic(
            src_system, otype, definition)
        if conv_status == "GENERATED":
            # Every generated draft must be human-reviewed before any use.
            review_status = "PENDING_REVIEW"
        elif conv_status == "NOT_SUPPORTED" and use_ai:
            # No approved AI endpoint is configured; never fabricate model output.
            conv_status = "NOT_CONFIGURED"

    rows.append(Row(
        assessment_id=assessment_id, run_id=run_id, connection_id=connection_id,
        source_system=src_system, source_database=src_db, source_schema=schema,
        object_name=name, object_type=otype, source_definition=definition,
        complexity_category=complexity, classification_reason=reason,
        converted_definition=converted, conversion_language=language,
        conversion_status=conv_status, review_status=review_status,
        error_message=error_message))

# COMMAND ----------

if rows:
    df = (spark.createDataFrame(rows)
          .withColumn("captured_ts", F.current_timestamp())
          .withColumn("updated_ts", F.current_timestamp()))
    df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(
        ctrl("sql_object_assessment").replace("`", ""))
    summary = {r["complexity_category"]: r["count"]
               for r in df.groupBy("complexity_category").count().collect()}
    print("Complexity summary:", summary)
else:
    summary = {}
    print("No SQL objects found for the selected filters.")

# COMMAND ----------

dbutils.notebook.exit(json.dumps({
    "status": "SUCCEEDED", "mode": mode, "assessment_id": assessment_id,
    "run_id": run_id, "connection_id": connection_id, "objects": len(rows),
    "summary": summary,
}))
