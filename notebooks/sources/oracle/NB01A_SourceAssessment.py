# Databricks notebook source
# MAGIC %md
# MAGIC # Oracle / NB01A_SourceAssessment
# MAGIC Broad **Oracle** source assessment. Discovers owners, tables, views and
# MAGIC routines/packages through the Oracle data dictionary (never a per-table
# MAGIC COUNT(*)), reads `ALL_TABLES` estimated statistics, and classifies table
# MAGIC migration compatibility with the shared type mapper. Record building,
# MAGIC compatibility roll-up, and the idempotent MERGE are shared.
# MAGIC It does NOT provision, migrate, or modify `source_table_control`.

# COMMAND ----------

# MAGIC %run ../../shared/_common

# COMMAND ----------

import uuid as _uuid

SOURCE_SYSTEM = "oracle"
# Oracle dictionary NUM_ROWS is optimizer statistics, not an executed count.
ROW_COUNT_METHOD = assess_common.ESTIMATED

dbutils.widgets.text("connection_id", "")
dbutils.widgets.text("assessment_id", "")
dbutils.widgets.text("include_schemas", "")
dbutils.widgets.text("exclude_schemas", "")
dbutils.widgets.text("include_object_types",
                     "TABLE,VIEW,PROCEDURE,FUNCTION,PACKAGE")

connection_id = dbutils.widgets.get("connection_id").strip() or CONNECTION_ID
assessment_id = dbutils.widgets.get("assessment_id").strip() or _uuid.uuid4().hex
include_schemas = [s.strip() for s in
                   dbutils.widgets.get("include_schemas").split(",") if s.strip()]
exclude_schemas = [s.strip() for s in
                   dbutils.widgets.get("exclude_schemas").split(",") if s.strip()]
include_types = {t.strip().upper() for t in
                 dbutils.widgets.get("include_object_types").split(",") if t.strip()}
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
        "this notebook assesses Oracle sources only")
src_server, src_db = cd.get("source_server"), cd.get("source_database")
adapter = get_source_adapter_for_connection(connection)   # requires VALID
mapper = load_type_mapper(SOURCE_SYSTEM)
print(f"Assessing Oracle connection {connection_id}; assessment_id={assessment_id}")


def _q(query):
    return read_source_jdbc(adapter, query, source_server=src_server,
                            source_database=src_db).collect()


def _record(**kwargs):
    return assess_common.build_assessment_record(
        assessment_id=assessment_id, run_id=run_id, connection_id=connection_id,
        source_system=SOURCE_SYSTEM, source_server=src_server,
        source_database=src_db, **kwargs)

# COMMAND ----------

schemas = resolve_assessment_schemas(adapter, src_db, include_schemas,
                                     exclude_schemas)

# COMMAND ----------

records = []
for schema in schemas:
    # ---- Oracle dictionary statistics (estimated NUM_ROWS / BLOCKS) ----
    stats = {}
    try:
        for s in _q(adapter.table_statistics_query(src_db, schema)):
            stats[s["OBJECT_NAME"]] = (s["ROW_COUNT"], s["SIZE_MB"],
                                       s["ROW_COUNT_METHOD"])
    except Exception as e:
        print(f"  [warn] Oracle statistics unavailable for {schema}: "
              f"{failcls.sanitize_message(e)[:200]}")

    # ---- TABLES (ALL_TABLES) ----
    if "TABLE" in include_types:
        try:
            tables = _q(adapter.list_tables_query(src_db, schema))
        except Exception as e:
            tables = []
            print(f"  [warn] Oracle table discovery failed for {schema}: "
                  f"{failcls.sanitize_message(e)[:200]}")
        for t in tables:
            obj = t["OBJECT_NAME"]
            row_count, size_mb, method = stats.get(
                obj, (t["ROW_COUNT"], None, ROW_COUNT_METHOD))
            comp, complexity, msg, col_count = (
                "UNABLE_TO_ASSESS", "NOT_APPLICABLE", None, None)
            try:
                cols = _q(adapter.columns_metadata_query(src_db, schema, obj))
                statuses = [
                    mapper.map_column(
                        c["DATA_TYPE"], c["NUMERIC_PRECISION"], c["NUMERIC_SCALE"],
                        c["CHARACTER_MAXIMUM_LENGTH"],
                        c["IS_NULLABLE"] == "YES").status
                    for c in cols
                ]
                col_count = len(cols)
                comp = assess_common.summarize_table_compatibility(statuses)
                complexity = assess_common.classify_complexity(row_count, col_count)
            except Exception as e:
                msg = f"column assessment failed: {failcls.sanitize_message(e)[:400]}"
                print(f"  [warn] {schema}.{obj}: {msg}")
            records.append(_record(
                source_schema=schema, object_name=obj, object_type="TABLE",
                row_count=row_count, row_count_method=method or ROW_COUNT_METHOD,
                size_mb=size_mb, column_count=col_count,
                compatibility_status=comp, complexity=complexity,
                assessment_message=msg))

    # ---- VIEWS (ALL_VIEWS) ----
    if "VIEW" in include_types:
        try:
            for v in _q(adapter.list_views_query(src_db, schema)):
                records.append(_record(
                    source_schema=schema, object_name=v["OBJECT_NAME"],
                    object_type="VIEW", compatibility_status="REVIEW",
                    assessment_message="assess conversion in NB13"))
        except Exception as e:
            print(f"  [warn] Oracle view discovery failed for {schema}: "
                  f"{failcls.sanitize_message(e)[:200]}")

    # ---- ROUTINES + PACKAGES (ALL_OBJECTS) ----
    if include_types & {"PROCEDURE", "FUNCTION", "PACKAGE", "PACKAGE_BODY"}:
        try:
            for rt in _q(adapter.list_routines_query(src_db, schema)):
                # PACKAGE BODY normalizes to PACKAGE_BODY and stays distinct.
                otype = adapter.normalize_sql_object_type(rt["OBJECT_TYPE"])
                if not otype:
                    continue
                requested = (otype in include_types
                             or (otype == "PACKAGE_BODY" and "PACKAGE" in include_types))
                if not requested:
                    continue
                records.append(_record(
                    source_schema=schema, object_name=rt["OBJECT_NAME"],
                    object_type=otype, compatibility_status="MANUAL",
                    assessment_message="assess conversion in NB13"))
        except Exception as e:
            print(f"  [warn] Oracle routine discovery failed for {schema}: "
                  f"{failcls.sanitize_message(e)[:200]}")

print(f"Assessed {len(records)} Oracle object(s).")

# COMMAND ----------

summary = persist_assessment_records(records)
print("Compatibility summary:", summary)

dbutils.notebook.exit(json.dumps({
    "status": "SUCCEEDED", "run_id": run_id, "connection_id": connection_id,
    "assessment_id": assessment_id, "objects": len(records), "summary": summary,
}))
