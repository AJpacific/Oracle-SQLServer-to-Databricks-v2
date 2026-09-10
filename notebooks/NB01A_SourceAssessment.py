# Databricks notebook source
# MAGIC %md
# MAGIC # NB01A_SourceAssessment
# MAGIC INGEST-pipeline broad source assessment. Discovers schemas, tables, views,
# MAGIC procedures, functions and packages for one validated connection using the
# MAGIC source data dictionary / catalog views (never a per-table COUNT(*)), runs
# MAGIC the existing type mapper per table to classify migration compatibility, and
# MAGIC writes one row per object to `source_assessment`.
# MAGIC It does NOT provision, migrate, or modify `source_table_control`.

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

import uuid as _uuid

dbutils.widgets.text("connection_id", "")
dbutils.widgets.text("include_schemas", "")
dbutils.widgets.text("exclude_schemas", "")
dbutils.widgets.text("include_object_types", "TABLE,VIEW,PROCEDURE,FUNCTION,PACKAGE")
dbutils.widgets.text("assessment_id", "")
dbutils.widgets.text("run_id", "")

connection_id = dbutils.widgets.get("connection_id").strip()
include_schemas = [s.strip() for s in dbutils.widgets.get("include_schemas").split(",") if s.strip()]
exclude_schemas = {s.strip().lower() for s in dbutils.widgets.get("exclude_schemas").split(",") if s.strip()}
include_types = {t.strip().upper() for t in dbutils.widgets.get("include_object_types").split(",") if t.strip()}
assessment_id = dbutils.widgets.get("assessment_id").strip() or _uuid.uuid4().hex
run_id = dbutils.widgets.get("run_id").strip() or get_run_id()

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
mapper = load_type_mapper(src_system)
print(f"Assessing connection {connection_id} ({src_system}); assessment_id={assessment_id}")

# COMMAND ----------

def _q(query):
    "Run a discovery query through this connection's adapter."
    return read_source_jdbc(adapter, query, source_server=src_server,
                            source_database=src_db).collect()

# ---- resolve target schemas (apply include/exclude filters) ----------------
all_schemas = [r["SCHEMA_NAME"] for r in _q(adapter.list_schemas_query(src_db))]
schemas = [s for s in all_schemas
           if (not include_schemas or s in include_schemas)
           and s.lower() not in exclude_schemas]
print(f"Schemas to assess: {len(schemas)} of {len(all_schemas)} discovered")

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql import functions as F

rows = []

def _complexity_for_table(row_count, column_count):
    rc = row_count or 0
    cc = column_count or 0
    if rc > 100_000_000 or cc > 200:
        return "HIGH"
    if rc > 1_000_000 or cc > 50:
        return "MEDIUM"
    return "LOW"

for schema in schemas:
    # Statistics lookup for the schema (exact for SQL Server, estimated for Oracle).
    stats = {}
    try:
        for s in _q(adapter.table_statistics_query(src_db, schema)):
            stats[s["OBJECT_NAME"]] = (
                s["ROW_COUNT"], s["SIZE_MB"], s["ROW_COUNT_METHOD"])
    except Exception as e:
        print(f"  [warn] statistics unavailable for schema {schema}: {str(e)[:200]}")

    # ---- TABLES ----
    if "TABLE" in include_types:
        try:
            tables = _q(adapter.list_tables_query(src_db, schema))
        except Exception as e:
            tables = []
            print(f"  [warn] table discovery failed for {schema}: {str(e)[:200]}")
        for t in tables:
            obj = t["OBJECT_NAME"]
            row_count, size_mb, method = stats.get(obj, (t["ROW_COUNT"], None, None))
            if method is None:
                method = "ESTIMATED" if src_system == "oracle" else "EXACT"
            comp, complexity, msg, col_count = "UNABLE_TO_ASSESS", "NOT_APPLICABLE", None, None
            try:
                cols = _q(adapter.columns_metadata_query(src_db, schema, obj))
                statuses = []
                for c in cols:
                    res = mapper.map_column(
                        c["DATA_TYPE"], c["NUMERIC_PRECISION"], c["NUMERIC_SCALE"],
                        c["CHARACTER_MAXIMUM_LENGTH"], c["IS_NULLABLE"] == "YES")
                    statuses.append(res.status)
                col_count = len(cols)
                comp = classify_table_compatibility(statuses)
                complexity = _complexity_for_table(row_count, col_count)
            except Exception as e:
                msg = f"column assessment failed: {str(e)[:400]}"
                print(f"  [warn] {schema}.{obj}: {msg}")
            rows.append(Row(
                assessment_id=assessment_id, run_id=run_id, connection_id=connection_id,
                source_system=src_system, source_server=src_server, source_database=src_db,
                source_schema=schema, object_name=obj, object_type="TABLE",
                row_count=(int(row_count) if row_count is not None else None),
                row_count_method=method,
                size_mb=(float(size_mb) if size_mb is not None else None),
                column_count=col_count, compatibility_status=comp, complexity=complexity,
                assessment_message=msg, is_selected=False))

    # ---- VIEWS ----
    if "VIEW" in include_types:
        try:
            for v in _q(adapter.list_views_query(src_db, schema)):
                rows.append(Row(
                    assessment_id=assessment_id, run_id=run_id, connection_id=connection_id,
                    source_system=src_system, source_server=src_server, source_database=src_db,
                    source_schema=schema, object_name=v["OBJECT_NAME"], object_type="VIEW",
                    row_count=None, row_count_method="UNAVAILABLE", size_mb=None,
                    column_count=None, compatibility_status="REVIEW",
                    complexity="NOT_APPLICABLE",
                    assessment_message="assess conversion in NB13", is_selected=False))
        except Exception as e:
            print(f"  [warn] view discovery failed for {schema}: {str(e)[:200]}")

    # ---- ROUTINES (procedure / function / package) ----
    if include_types & {"PROCEDURE", "FUNCTION", "PACKAGE"}:
        try:
            for rt in _q(adapter.list_routines_query(src_db, schema)):
                otype = (rt["OBJECT_TYPE"] or "").upper().replace(" ", "_")
                if otype not in include_types and otype.split("_")[0] not in include_types:
                    continue
                rows.append(Row(
                    assessment_id=assessment_id, run_id=run_id, connection_id=connection_id,
                    source_system=src_system, source_server=src_server, source_database=src_db,
                    source_schema=schema, object_name=rt["OBJECT_NAME"], object_type=otype,
                    row_count=None, row_count_method="UNAVAILABLE", size_mb=None,
                    column_count=None, compatibility_status="MANUAL",
                    complexity="NOT_APPLICABLE",
                    assessment_message="assess conversion in NB13", is_selected=False))
        except Exception as e:
            print(f"  [warn] routine discovery failed for {schema}: {str(e)[:200]}")

print(f"Assessed {len(rows)} object(s).")

# COMMAND ----------

if rows:
    df = spark.createDataFrame(rows).withColumn("captured_ts", F.current_timestamp())
    df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(
        ctrl("source_assessment").replace("`", ""))
    grouped = {r["compatibility_status"]: r["count"] for r in
               df.groupBy("compatibility_status").count().collect()}
    print("Compatibility summary:", grouped)
else:
    grouped = {}
    print("No objects discovered for the selected filters.")

# COMMAND ----------

dbutils.notebook.exit(json.dumps({
    "status": "SUCCEEDED", "assessment_id": assessment_id, "run_id": run_id,
    "connection_id": connection_id, "objects": len(rows), "summary": grouped,
}))
