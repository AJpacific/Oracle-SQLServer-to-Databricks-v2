# Databricks notebook source
# MAGIC %md
# MAGIC # NB23_FetchSelectedSQLArtifacts
# MAGIC Dedicated source definition fetcher for selected VIEW and PROCEDURE objects.
# MAGIC Discovers candidates strictly from `source_assessment` where `is_selected = true`.
# MAGIC Connects only to source groups that contain selected objects.
# MAGIC Stores exact raw source definitions into `_source_artifacts` Volume and `sql_artifact_control`.
# MAGIC Never executes fetched SQL. Never mutates `source_connection` or `source_assessment`.

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

import os
import json
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Tuple
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    BooleanType,
    TimestampType,
)

try:
    from src.identifiers import (
        quote_databricks,
        escape_string_literal,
        normalize_target_identifier,
        databricks_fqn,
    )
    from src.sql_artifact_control_common import (
        SQL_ARTIFACT_CONTROL_TABLE,
        SQL_ARTIFACT_EXECUTION_LOG_TABLE,
        SQL_ARTIFACT_CONTROL_COLUMNS,
        SQL_ARTIFACT_EXECUTION_LOG_COLUMNS,
        STAGE_SELECTED_SOURCE_FETCH,
        compute_artifact_id,
        compute_definition_hash,
        is_connection_usable,
        validate_registered_source_connection,
        prepare_artifact_rerun_control_row,
        resolve_target_catalog_and_schema,
        sanitize_error,
    )
    from src.sql_artifact_scope import (
        build_selected_candidate_query,
        validate_max_artifacts,
    )
    from src.sql_object_artifact_common import (
        build_artifact_relative_path,
        build_artifact_volume_path,
        normalize_object_type,
    )
    from src.source_adapters.factory import get_source_adapter
    from src.failure_classifier import sanitize_message
except ModuleNotFoundError:
    from identifiers import (
        quote_databricks,
        escape_string_literal,
        normalize_target_identifier,
        databricks_fqn,
    )
    from sql_artifact_control_common import (
        SQL_ARTIFACT_CONTROL_TABLE,
        SQL_ARTIFACT_EXECUTION_LOG_TABLE,
        SQL_ARTIFACT_CONTROL_COLUMNS,
        SQL_ARTIFACT_EXECUTION_LOG_COLUMNS,
        STAGE_SELECTED_SOURCE_FETCH,
        compute_artifact_id,
        compute_definition_hash,
        is_connection_usable,
        validate_registered_source_connection,
        prepare_artifact_rerun_control_row,
        resolve_target_catalog_and_schema,
        sanitize_error,
    )
    from sql_artifact_scope import (
        build_selected_candidate_query,
        validate_max_artifacts,
    )
    from sql_object_artifact_common import (
        build_artifact_relative_path,
        build_artifact_volume_path,
        normalize_object_type,
    )
    from source_adapters.factory import get_source_adapter
    from failure_classifier import sanitize_message

# COMMAND ----------

# --- Widgets ---
dbutils.widgets.text("run_id", "")
dbutils.widgets.text("catalog", "da_accelerators")
dbutils.widgets.text("control_schema", "control")
dbutils.widgets.text("connection_id", "")
dbutils.widgets.text("source_database", "")
dbutils.widgets.text("max_artifacts", "0")

run_id = dbutils.widgets.get("run_id").strip() or get_run_id()
if not run_id:
    raise ValueError("Blank run_id rejected: current run_id scope is required")

catalog = dbutils.widgets.get("catalog").strip() or CATALOG
control_schema = dbutils.widgets.get("control_schema").strip() or CONTROL_SCHEMA
connection_filter = dbutils.widgets.get("connection_id").strip()
source_db_filter = dbutils.widgets.get("source_database").strip()
max_artifacts_raw = dbutils.widgets.get("max_artifacts").strip()

max_artifacts = validate_max_artifacts(max_artifacts_raw)

print(f"=== Starting Selected SQL Artifact Source Fetch run_id={run_id} ===")
print(f"Control Catalog={catalog}, Control Schema={control_schema}")
print(f"Filter Connection='{connection_filter}', Filter Database='{source_db_filter}', Max Artifacts={max_artifacts}")

sa_fqn = f"{quote_databricks(catalog)}.{quote_databricks(control_schema)}.{quote_databricks('source_assessment')}"
control_fqn = f"{quote_databricks(catalog)}.{quote_databricks(control_schema)}.{quote_databricks(SQL_ARTIFACT_CONTROL_TABLE)}"
log_fqn = f"{quote_databricks(catalog)}.{quote_databricks(control_schema)}.{quote_databricks(SQL_ARTIFACT_EXECUTION_LOG_TABLE)}"

# COMMAND ----------

# 1. Query selected candidates from source_assessment
candidate_query = build_selected_candidate_query(
    source_assessment_fqn=sa_fqn,
    connection_id=connection_filter,
    source_database=source_db_filter,
    max_artifacts=max_artifacts,
)

candidate_rows = spark.sql(candidate_query).collect()
candidates = [r.asDict() if hasattr(r, "asDict") else dict(r) for r in candidate_rows]
total_candidates = len(candidates)
print(f"Discovered {total_candidates} selected candidate VIEW/PROCEDURE artifact(s)")

if total_candidates == 0:
    set_task_value("run_id", run_id)
    set_task_value("status", "SUCCEEDED")
    set_task_value("business_status", "NO_CANDIDATES")
    set_task_value("selected_candidate_count", 0)
    set_task_value("fetched_definition_count", 0)
    set_task_value("definition_missing_count", 0)
    set_task_value("source_fetch_failure_count", 0)
    empty_summary = {
        "status": "SUCCEEDED",
        "business_status": "NO_CANDIDATES",
        "run_id": run_id,
        "selected_candidate_count": 0,
        "fetched_definition_count": 0,
        "definition_missing_count": 0,
        "source_fetch_failure_count": 0,
    }
    dbutils.notebook.exit(json.dumps(empty_summary))

# COMMAND ----------

def write_atomic_file(target_path: str, content: str) -> None:
    temp_path = f"{target_path}.tmp.{uuid.uuid4().hex}"
    os.makedirs(os.path.dirname(target_path), exist_ok=True)
    with open(temp_path, "w", encoding="utf-8") as f:
        f.write(content)
    os.replace(temp_path, target_path)

# Group selected candidates by (connection_id, source_system, source_database)
groups: Dict[Tuple[str, str, str], List[Dict[str, Any]]] = {}
for c in candidates:
    cid = str(c.get("connection_id") or "").strip()
    sys_name = str(c.get("source_system") or "").strip().lower()
    db = str(c.get("source_database") or "").strip()
    groups.setdefault((cid, sys_name, db), []).append(c)

print(f"Grouped into {len(groups)} source connection/database group(s)")

repo = control_repo()
verified_volumes: Set[Tuple[str, str, str]] = set()

fetched_definition_count = 0
definition_missing_count = 0
source_fetch_failure_count = 0
now_utc = datetime.now(timezone.utc)

# Read existing control records to detect reruns and determine attempt_count
existing_artifacts: Dict[str, Dict[str, Any]] = {}
try:
    existing_rows = spark.sql(f"SELECT artifact_id, attempt_count, first_seen_ts, created_ts, legacy_artifact_id FROM {control_fqn}").collect()
    for r in existing_rows:
        row_dict = r.asDict() if hasattr(r, "asDict") else dict(r)
        existing_artifacts[row_dict["artifact_id"]] = row_dict
except Exception as e:
    print(f"Note: Could not query existing control table (may not exist yet): {sanitize_message(e)}")

ctrl_struct_fields = []
for cname, ctype, cnull in SQL_ARTIFACT_CONTROL_COLUMNS:
    if ctype == "STRING":
        st = StringType()
    elif ctype == "INT":
        st = IntegerType()
    elif ctype == "BOOLEAN":
        st = BooleanType()
    elif ctype == "TIMESTAMP":
        st = TimestampType()
    else:
        st = StringType()
    ctrl_struct_fields.append(StructField(cname, st, cnull))

control_schema_struct = StructType(ctrl_struct_fields)

log_struct_fields = []
for cname, ctype, cnull in SQL_ARTIFACT_EXECUTION_LOG_COLUMNS:
    if ctype == "STRING":
        st = StringType()
    elif ctype == "INT":
        st = IntegerType()
    elif ctype == "TIMESTAMP":
        st = TimestampType()
    else:
        st = StringType()
    log_struct_fields.append(StructField(cname, st, cnull))

log_schema_struct = StructType(log_struct_fields)

def persist_execution_log(log_entry: Dict[str, Any]) -> None:
    log_df = spark.createDataFrame([log_entry], schema=log_schema_struct)
    log_df.write.format("delta").mode("append").saveAsTable(log_fqn.replace("`", ""))

def persist_control_row(row_entry: Dict[str, Any]) -> None:
    ctrl_df = spark.createDataFrame([row_entry], schema=control_schema_struct)
    ctrl_view = f"_artifact_fetch_merge_{uuid.uuid4().hex}"
    ctrl_df.createOrReplaceTempView(ctrl_view)
    spark.sql(f"""
        MERGE INTO {control_fqn} t
        USING {ctrl_view} s
           ON t.artifact_id = s.artifact_id
        WHEN MATCHED THEN UPDATE SET
          t.connection_id = s.connection_id,
          t.source_system = s.source_system,
          t.source_database = s.source_database,
          t.source_schema = s.source_schema,
          t.object_name = s.object_name,
          t.object_type = s.object_type,
          t.target_catalog = s.target_catalog,
          t.target_schema = s.target_schema,
          t.target_object_name = s.target_object_name,
          t.source_definition = s.source_definition,
          t.source_definition_hash = s.source_definition_hash,
          t.conversion_classification = s.conversion_classification,
          t.conversion_status = s.conversion_status,
          t.deployment_status = s.deployment_status,
          t.manual_review_required = s.manual_review_required,
          t.manual_review_reason = s.manual_review_reason,
          t.error_code = s.error_code,
          t.error_message = s.error_message,
          t.attempt_count = s.attempt_count,
          t.last_seen_ts = s.last_seen_ts,
          t.updated_ts = s.updated_ts,
          t.run_id = s.run_id,
          t.converted_definition = NULL,
          t.converted_definition_hash = NULL,
          t.conversion_ts = NULL,
          t.lakebridge_analyzed_ts = NULL,
          t.lakebridge_complexity = NULL,
          t.lakebridge_statement_count = NULL,
          t.lakebridge_unknown_statement_count = NULL,
          t.lakebridge_unknown_fragments = NULL,
          t.lakebridge_uses_error_handling = NULL,
          t.lakebridge_uses_rowcount = NULL,
          t.lakebridge_uses_cursor = NULL,
          t.lakebridge_uses_dynamic_sql = NULL,
          t.lakebridge_uses_trigger = NULL,
          t.lakebridge_referenced_objects = NULL,
          t.lakebridge_parsing_error_count = NULL,
          t.lakebridge_validation_error_count = NULL,
          t.lakebridge_generation_error_count = NULL,
          t.lakebridge_fixme_count = NULL,
          t.lakebridge_transpiled_definition = NULL,
          t.object_map_applied = false,
          t.lakebridge_classification = NULL,
          t.unsupported_features = NULL
        WHEN NOT MATCHED THEN INSERT (
          artifact_id, connection_id, source_system, source_database,
          source_schema, object_name, object_type, target_catalog,
          target_schema, target_object_name, source_definition,
          converted_definition, source_definition_hash, converted_definition_hash,
          conversion_classification, conversion_status, deployment_status,
          manual_review_required, manual_review_reason, unsupported_features,
          error_code, error_message, attempt_count, first_seen_ts,
          last_seen_ts, conversion_ts, deployment_ts, created_ts,
          updated_ts, run_id, is_active, identity_version, legacy_artifact_id,
          lakebridge_analyzed_ts, lakebridge_complexity, lakebridge_statement_count,
          lakebridge_unknown_statement_count, lakebridge_unknown_fragments,
          lakebridge_uses_error_handling, lakebridge_uses_rowcount, lakebridge_uses_cursor,
          lakebridge_uses_dynamic_sql, lakebridge_uses_trigger, lakebridge_referenced_objects,
          lakebridge_parsing_error_count, lakebridge_validation_error_count,
          lakebridge_generation_error_count, lakebridge_fixme_count,
          lakebridge_transpiled_definition, object_map_applied, lakebridge_classification
        ) VALUES (
          s.artifact_id, s.connection_id, s.source_system, s.source_database,
          s.source_schema, s.object_name, s.object_type, s.target_catalog,
          s.target_schema, s.target_object_name, s.source_definition,
          s.converted_definition, s.source_definition_hash, s.converted_definition_hash,
          s.conversion_classification, s.conversion_status, s.deployment_status,
          s.manual_review_required, s.manual_review_reason, s.unsupported_features,
          s.error_code, s.error_message, s.attempt_count, s.first_seen_ts,
          s.last_seen_ts, s.conversion_ts, s.deployment_ts, s.created_ts,
          s.updated_ts, s.run_id, s.is_active, s.identity_version, s.legacy_artifact_id,
          s.lakebridge_analyzed_ts, s.lakebridge_complexity, s.lakebridge_statement_count,
          s.lakebridge_unknown_statement_count, s.lakebridge_unknown_fragments,
          s.lakebridge_uses_error_handling, s.lakebridge_uses_rowcount, s.lakebridge_uses_cursor,
          s.lakebridge_uses_dynamic_sql, s.lakebridge_uses_trigger, s.lakebridge_referenced_objects,
          s.lakebridge_parsing_error_count, s.lakebridge_validation_error_count,
          s.lakebridge_generation_error_count, s.lakebridge_fixme_count,
          s.lakebridge_transpiled_definition, s.object_map_applied, s.lakebridge_classification
        )
    """)

# COMMAND ----------

for (conn_id, src_sys, src_db), group_candidates in groups.items():
    stage_start_ts = datetime.now(timezone.utc)
    print(f"\nProcessing group: connection_id={conn_id}, source_system={src_sys}, database={src_db} ({len(group_candidates)} objects)")

    # Read and validate connection
    try:
        conn_record = repo.get_connection(conn_id) if hasattr(repo, "get_connection") else None
    except Exception as e_get:
        conn_record = None

    is_conn_valid, conn_err_msg, conn_meta = validate_registered_source_connection(
        conn_record=conn_record,
        candidate_source_system=src_sys,
        candidate_source_database=src_db,
    )

    if not is_conn_valid:
        safe_err = sanitize_error(conn_err_msg)
        print(f"  [error] Connection validation failed for {conn_id}: {safe_err}")
        source_fetch_failure_count += len(group_candidates)
        for cand in group_candidates:
            oname = str(cand.get("object_name") or "").strip()
            otype = normalize_object_type(cand.get("object_type"))
            art_id = compute_artifact_id(conn_id, src_sys, src_db, cand.get("source_schema"), otype, oname)
            existing_art = existing_artifacts.get(art_id)
            ctrl_row = prepare_artifact_rerun_control_row(
                existing_row=existing_art,
                current_run_id=run_id,
                connection_id=conn_id,
                source_system=src_sys,
                source_database=src_db if src_db else None,
                source_schema=cand.get("source_schema"),
                object_name=oname,
                object_type=otype,
                source_definition=None,
                source_definition_hash="0" * 64,
                now_ts=now_utc,
                fetch_error_code="SOURCE_IDENTITY_MISMATCH",
                fetch_error_message=safe_err,
            )
            persist_control_row(ctrl_row)
            persist_execution_log({
                "log_id": uuid.uuid4().hex,
                "run_id": run_id,
                "artifact_id": art_id,
                "processing_stage": STAGE_SELECTED_SOURCE_FETCH,
                "attempt_number": ctrl_row["attempt_count"],
                "start_ts": stage_start_ts,
                "end_ts": datetime.now(timezone.utc),
                "status": "FAILED",
                "source_definition_hash": None,
                "converted_definition_hash": None,
                "classification": "UNSUPPORTED",
                "error_code": "SOURCE_IDENTITY_MISMATCH",
                "error_message": safe_err,
                "execution_metadata": json.dumps({"source_system": src_sys, "connection_id": conn_id}),
                "created_ts": datetime.now(timezone.utc),
            })
        continue

    # Build adapter for this source group
    try:
        conn_dict = conn_record.asDict() if hasattr(conn_record, "asDict") else dict(conn_record)
        extra = {}
        if conn_meta.get("trust_server_certificate") is not None:
            extra["trust_server_certificate"] = bool(conn_meta.get("trust_server_certificate"))
        adapter = get_source_adapter(
            source_system=conn_meta["source_system"],
            source_server=conn_meta["source_server"],
            source_database=conn_meta["source_database"],
            secret_provider=dbutils.secrets.get,
            secret_scope=conn_meta["secret_scope"],
            config=extra if extra else None,
        )
    except Exception as e:
        safe_err = sanitize_error(e)
        print(f"  [error] Adapter creation failed: {safe_err}")
        source_fetch_failure_count += len(group_candidates)
        for cand in group_candidates:
            oname = str(cand.get("object_name") or "").strip()
            otype = normalize_object_type(cand.get("object_type"))
            art_id = compute_artifact_id(conn_id, src_sys, src_db, cand.get("source_schema"), otype, oname)
            existing_art = existing_artifacts.get(art_id)
            ctrl_row = prepare_artifact_rerun_control_row(
                existing_row=existing_art,
                current_run_id=run_id,
                connection_id=conn_id,
                source_system=src_sys,
                source_database=src_db if src_db else None,
                source_schema=cand.get("source_schema"),
                object_name=oname,
                object_type=otype,
                source_definition=None,
                source_definition_hash="0" * 64,
                now_ts=now_utc,
                fetch_error_code="ADAPTER_INIT_FAILED",
                fetch_error_message=safe_err,
            )
            persist_control_row(ctrl_row)
            persist_execution_log({
                "log_id": uuid.uuid4().hex,
                "run_id": run_id,
                "artifact_id": art_id,
                "processing_stage": STAGE_SELECTED_SOURCE_FETCH,
                "attempt_number": ctrl_row["attempt_count"],
                "start_ts": stage_start_ts,
                "end_ts": datetime.now(timezone.utc),
                "status": "FAILED",
                "source_definition_hash": None,
                "converted_definition_hash": None,
                "classification": "UNSUPPORTED",
                "error_code": "ADAPTER_INIT_FAILED",
                "error_message": safe_err,
                "execution_metadata": json.dumps({"source_system": src_sys, "connection_id": conn_id}),
                "created_ts": datetime.now(timezone.utc),
            })
        continue

    # Resolve target catalog and schema
    try:
        tgt_cfg = repo.resolve_target_config(conn_id)
    except Exception:
        tgt_cfg = {"target_catalog": catalog, "target_schema_mode": "SAME_AS_SOURCE"}

    # Fetch each selected definition
    for cand in group_candidates:
        cand_start_ts = datetime.now(timezone.utc)
        sch = cand.get("source_schema")
        oname = cand.get("object_name")
        otype = normalize_object_type(cand.get("object_type"))
        art_id = compute_artifact_id(conn_id, src_sys, src_db, sch, otype, oname)
        existing_art = existing_artifacts.get(art_id)

        try:
            target_cat, target_sch = resolve_target_catalog_and_schema(tgt_cfg, src_db, sch)
        except Exception:
            target_cat, target_sch = catalog, sch.lower()

        src_def: Optional[str] = None
        fetch_err: Optional[str] = None

        try:
            if src_sys in ("sqlserver", "sql_server", "mssql"):
                p = f"[{src_db}].sys." if src_db and str(src_db).strip() else "sys."
                type_filter = "('V')" if otype == "VIEW" else "('P')"
                query = (
                    f"(SELECT s.name AS SCHEMA_NAME, o.name AS OBJECT_NAME, o.type AS OBJECT_TYPE, m.definition AS DEFINITION_TEXT "
                    f"FROM {p}objects o "
                    f"JOIN {p}schemas s ON o.schema_id = s.schema_id "
                    f"LEFT JOIN {p}sql_modules m ON m.object_id = o.object_id "
                    f"WHERE s.name = {escape_string_literal(sch)} "
                    f"AND o.name = {escape_string_literal(oname)} "
                    f"AND o.type IN {type_filter}) q"
                )
                df = read_source_jdbc(adapter, query, source_server=conn_dict.get("source_server"), source_database=src_db)
                rows = df.collect()
                if rows and rows[0]["DEFINITION_TEXT"]:
                    src_def = str(rows[0]["DEFINITION_TEXT"])
            else:  # Oracle
                if otype == "VIEW":
                    query = (
                        f"(SELECT text AS DEFINITION_TEXT FROM ALL_VIEWS "
                        f"WHERE owner = {escape_string_literal(sch)} "
                        f"AND view_name = {escape_string_literal(oname)}) q"
                    )
                    df = read_source_jdbc(adapter, query, source_server=conn_dict.get("source_server"), source_database=src_db)
                    rows = df.collect()
                    if rows and rows[0]["DEFINITION_TEXT"]:
                        src_def = str(rows[0]["DEFINITION_TEXT"])
                else:  # PROCEDURE
                    query = (
                        f"(SELECT line AS LINE_NO, text AS SOURCE_TEXT FROM ALL_SOURCE "
                        f"WHERE owner = {escape_string_literal(sch)} "
                        f"AND name = {escape_string_literal(oname)} "
                        f"AND type = 'PROCEDURE' ORDER BY LINE) q"
                    )
                    df = read_source_jdbc(adapter, query, source_server=conn_dict.get("source_server"), source_database=src_db)
                    rows = df.collect()
                    if rows:
                        src_def = "".join(r["SOURCE_TEXT"] or "" for r in rows)
        except Exception as e:
            fetch_err = sanitize_error(e)
            src_def = None

        if not src_def or not str(src_def).strip():
            definition_missing_count += 1
            err_code = "DEFINITION_MISSING"
            err_msg = fetch_err or "DEFINITION_MISSING: Selected object has NULL or inaccessible source definition"
            print(f"  [warn] Definition missing for {sch}.{oname} ({otype}): {err_msg}")
            ctrl_row = prepare_artifact_rerun_control_row(
                existing_row=existing_art,
                current_run_id=run_id,
                connection_id=conn_id,
                source_system=src_sys,
                source_database=src_db if src_db else None,
                source_schema=sch,
                object_name=oname,
                object_type=otype,
                source_definition=None,
                source_definition_hash="0" * 64,
                now_ts=now_utc,
                target_catalog=target_cat,
                target_schema=target_sch,
                target_object_name=oname.lower(),
                fetch_error_code=err_code,
                fetch_error_message=err_msg,
            )
            persist_control_row(ctrl_row)
            persist_execution_log({
                "log_id": uuid.uuid4().hex,
                "run_id": run_id,
                "artifact_id": art_id,
                "processing_stage": STAGE_SELECTED_SOURCE_FETCH,
                "attempt_number": ctrl_row["attempt_count"],
                "start_ts": cand_start_ts,
                "end_ts": datetime.now(timezone.utc),
                "status": "FAILED",
                "source_definition_hash": None,
                "converted_definition_hash": None,
                "classification": "UNSUPPORTED",
                "error_code": err_code,
                "error_message": err_msg,
                "execution_metadata": json.dumps({"source_system": src_sys, "connection_id": conn_id, "object": f"{sch}.{oname}"}),
                "created_ts": datetime.now(timezone.utc),
            })
            continue

        # Definition successfully fetched from JDBC - now persist to _source_artifacts Volume
        src_hash = compute_definition_hash(src_def)
        persist_vol_failed = False
        persist_vol_err = None
        try:
            rel_path = build_artifact_relative_path(
                connection_id=conn_id,
                source_system=src_sys,
                source_database=src_db,
                source_schema=sch,
                object_type=otype,
                object_name=oname,
            )
            vol_path = build_artifact_volume_path(
                target_catalog=target_cat,
                target_schema=target_sch,
                volume_name="_source_artifacts",
                relative_path=rel_path,
            )
            vol_key = (target_cat, target_sch, "_source_artifacts")
            if vol_key not in verified_volumes:
                spark.sql(f"CREATE SCHEMA IF NOT EXISTS {quote_databricks(target_cat)}.{quote_databricks(target_sch)}")
                spark.sql(f"CREATE VOLUME IF NOT EXISTS {quote_databricks(target_cat)}.{quote_databricks(target_sch)}.`_source_artifacts`")
                verified_volumes.add(vol_key)
            write_atomic_file(vol_path, src_def)
        except Exception as e:
            persist_vol_err = sanitize_error(e)
            print(f"  [error] Failed to write _source_artifacts volume file: {persist_vol_err}")
            persist_vol_failed = True

        if persist_vol_failed:
            source_fetch_failure_count += 1
            err_code = "ARTIFACT_WRITE_FAILED"
            err_msg = f"Failed to persist _source_artifacts volume file: {persist_vol_err}"
            # Do NOT pass artifact to NB24: set source_definition to None
            ctrl_row = prepare_artifact_rerun_control_row(
                existing_row=existing_art,
                current_run_id=run_id,
                connection_id=conn_id,
                source_system=src_sys,
                source_database=src_db if src_db else None,
                source_schema=sch,
                object_name=oname,
                object_type=otype,
                source_definition=None,
                source_definition_hash=src_hash,
                now_ts=now_utc,
                target_catalog=target_cat,
                target_schema=target_sch,
                target_object_name=oname.lower(),
                fetch_error_code=err_code,
                fetch_error_message=err_msg,
            )
            persist_control_row(ctrl_row)
            persist_execution_log({
                "log_id": uuid.uuid4().hex,
                "run_id": run_id,
                "artifact_id": art_id,
                "processing_stage": STAGE_SELECTED_SOURCE_FETCH,
                "attempt_number": ctrl_row["attempt_count"],
                "start_ts": cand_start_ts,
                "end_ts": datetime.now(timezone.utc),
                "status": "FAILED",
                "source_definition_hash": src_hash,
                "converted_definition_hash": None,
                "classification": "UNSUPPORTED",
                "error_code": err_code,
                "error_message": err_msg,
                "execution_metadata": json.dumps({"source_system": src_sys, "connection_id": conn_id, "object": f"{sch}.{oname}"}),
                "created_ts": datetime.now(timezone.utc),
            })
            continue

        fetched_definition_count += 1
        ctrl_row = prepare_artifact_rerun_control_row(
            existing_row=existing_art,
            current_run_id=run_id,
            connection_id=conn_id,
            source_system=src_sys,
            source_database=src_db if src_db else None,
            source_schema=sch,
            object_name=oname,
            object_type=otype,
            source_definition=src_def,
            source_definition_hash=src_hash,
            now_ts=now_utc,
            target_catalog=target_cat,
            target_schema=target_sch,
            target_object_name=oname.lower(),
        )
        persist_control_row(ctrl_row)
        persist_execution_log({
            "log_id": uuid.uuid4().hex,
            "run_id": run_id,
            "artifact_id": art_id,
            "processing_stage": STAGE_SELECTED_SOURCE_FETCH,
            "attempt_number": ctrl_row["attempt_count"],
            "start_ts": cand_start_ts,
            "end_ts": datetime.now(timezone.utc),
            "status": "SUCCEEDED",
            "source_definition_hash": src_hash,
            "converted_definition_hash": None,
            "classification": "PENDING",
            "error_code": None,
            "error_message": None,
            "execution_metadata": json.dumps({
                "source_system": src_sys,
                "connection_id": conn_id,
                "target_fqn": databricks_fqn(target_cat, target_sch, oname.lower()),
            }),
            "created_ts": datetime.now(timezone.utc),
        })

# COMMAND ----------

def evaluate_nb23_fetch_status(
    total_candidates: int,
    fetched_definition_count: int,
    total_failures: int,
) -> Tuple[str, str]:
    if total_candidates == 0:
        business_status = "NO_CANDIDATES"
    elif fetched_definition_count == 0:
        business_status = "FAILED"
    elif total_failures > 0:
        business_status = "PARTIAL"
    else:
        business_status = "COMPLETE"

    status = "FAILED" if business_status == "FAILED" else "SUCCEEDED"
    return status, business_status


# Determine status and publish task values
total_failures = definition_missing_count + source_fetch_failure_count
status, business_status = evaluate_nb23_fetch_status(
    total_candidates=total_candidates,
    fetched_definition_count=fetched_definition_count,
    total_failures=total_failures,
)

set_task_value("run_id", run_id)
set_task_value("status", status)
set_task_value("business_status", business_status)
set_task_value("selected_candidate_count", total_candidates)
set_task_value("fetched_definition_count", fetched_definition_count)
set_task_value("definition_missing_count", definition_missing_count)
set_task_value("source_fetch_failure_count", source_fetch_failure_count)

summary = {
    "status": status,
    "business_status": business_status,
    "run_id": run_id,
    "selected_candidate_count": total_candidates,
    "fetched_definition_count": fetched_definition_count,
    "definition_missing_count": definition_missing_count,
    "source_fetch_failure_count": source_fetch_failure_count,
}
print("Fetch Summary:", json.dumps(summary, indent=2))

if status == "FAILED":
    raise RuntimeError(f"NB23_FetchSelectedSQLArtifacts failed with {total_failures} failure(s) out of {total_candidates} candidates")

dbutils.notebook.exit(json.dumps(summary))
