# Databricks notebook source
# MAGIC %md
# MAGIC # NB22_SQLArtifactMigrate
# MAGIC Independent, idempotent SQL Artifact Migration workflow for non-table objects
# MAGIC (VIEW, PROCEDURE, FUNCTION, PACKAGE, PACKAGE BODY, TRIGGER).
# MAGIC
# MAGIC Converts, classifies, and stores; does not deploy and does not connect to source databases.
# MAGIC Reads only what Assessment (Job 1A) already captured.
# MAGIC
# MAGIC Discovers candidate artifacts from `source_assessment` where `is_selected = true`,
# MAGIC joined to `sql_object_assessment`, resolves target routing from `accelerator_target_config`,
# MAGIC transpiles to Databricks SQL, classifies into AUTO / MANUAL_REVIEW / UNSUPPORTED,
# MAGIC writes converted SQL to Unity Catalog Volumes under `_converted_artifacts`,
# MAGIC and records state strictly in `sql_artifact_control` and `sql_artifact_execution_log`.
# MAGIC Never executes converted SQL. Creates target schema IF NOT EXISTS only to hold the volume. Never connects to source databases.

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

import json
import uuid
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
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
        normalize_target_identifier,
        validate_identifier,
        quote_databricks,
        databricks_fqn,
        escape_string_literal,
    )
    from src.sql_artifact_control_common import (
        SQL_ARTIFACT_CONTROL_TABLE,
        SQL_ARTIFACT_EXECUTION_LOG_TABLE,
        CLASSIFICATION_AUTO,
        CLASSIFICATION_MANUAL_REVIEW,
        CLASSIFICATION_UNSUPPORTED,
        CONVERSION_STATUS_PENDING,
        CONVERSION_STATUS_CONVERTED,
        CONVERSION_STATUS_PARTIAL,
        CONVERSION_STATUS_UNSUPPORTED,
        CONVERSION_STATUS_FAILED,
        CONVERSION_STATUS_SKIPPED,
        DEPLOYMENT_STATUS_NOT_DEPLOYED,
        DEPLOYMENT_STATUS_DEPLOYED,
        DEPLOYMENT_STATUS_FAILED,
        DEPLOYMENT_STATUS_SKIPPED,
        ERR_CONFIG_MISSING,
        ERR_INVALID_IDENTITY,
        ERR_UNSUPPORTED_CONSTRUCT,
        ERR_CONVERSION_FAILED,
        ERR_VALIDATION_FAILED,
        ERR_DEPLOYMENT_FAILED,
        compute_artifact_id,
        compute_definition_hash,
        normalize_artifact_type,
        sanitize_error,
        build_create_artifact_control_ddl,
        build_create_artifact_execution_log_ddl,
        resolve_target_catalog_and_schema,
        is_connection_usable,
    )
    from src.sql_artifact_scope import (
        build_candidate_query,
        build_object_map,
        validate_max_artifacts,
    )
    from src.sql_artifact_converter import (
        SQLArtifactConverter,
        wrap_oracle_view_definition_if_needed,
    )
    from src.sql_object_artifact_common import (
        build_artifact_relative_path,
        build_artifact_volume_path,
        write_atomic_file,
    )
except ModuleNotFoundError:
    from identifiers import (
        normalize_target_identifier,
        validate_identifier,
        quote_databricks,
        databricks_fqn,
        escape_string_literal,
    )
    from sql_artifact_control_common import (
        SQL_ARTIFACT_CONTROL_TABLE,
        SQL_ARTIFACT_EXECUTION_LOG_TABLE,
        CLASSIFICATION_AUTO,
        CLASSIFICATION_MANUAL_REVIEW,
        CLASSIFICATION_UNSUPPORTED,
        CONVERSION_STATUS_PENDING,
        CONVERSION_STATUS_CONVERTED,
        CONVERSION_STATUS_PARTIAL,
        CONVERSION_STATUS_UNSUPPORTED,
        CONVERSION_STATUS_FAILED,
        CONVERSION_STATUS_SKIPPED,
        DEPLOYMENT_STATUS_NOT_DEPLOYED,
        DEPLOYMENT_STATUS_DEPLOYED,
        DEPLOYMENT_STATUS_FAILED,
        DEPLOYMENT_STATUS_SKIPPED,
        ERR_CONFIG_MISSING,
        ERR_INVALID_IDENTITY,
        ERR_UNSUPPORTED_CONSTRUCT,
        ERR_CONVERSION_FAILED,
        ERR_VALIDATION_FAILED,
        ERR_DEPLOYMENT_FAILED,
        compute_artifact_id,
        compute_definition_hash,
        normalize_artifact_type,
        sanitize_error,
        build_create_artifact_control_ddl,
        build_create_artifact_execution_log_ddl,
        resolve_target_catalog_and_schema,
        is_connection_usable,
    )
    from sql_artifact_scope import (
        build_candidate_query,
        build_object_map,
        validate_max_artifacts,
    )
    from sql_artifact_converter import (
        SQLArtifactConverter,
        wrap_oracle_view_definition_if_needed,
    )
    from sql_object_artifact_common import (
        build_artifact_relative_path,
        build_artifact_volume_path,
        write_atomic_file,
    )

# COMMAND ----------

# Widgets and Parameters
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
filter_connection_id = dbutils.widgets.get("connection_id").strip()
filter_source_database = dbutils.widgets.get("source_database").strip()

raw_max_artifacts = dbutils.widgets.get("max_artifacts").strip()
max_artifacts = validate_max_artifacts(raw_max_artifacts)

print(f"=== Starting SQL Artifact Migration run_id={run_id} ===")
print(f"Control Catalog={catalog}, Control Schema={control_schema}")
print(f"Filter Connection={filter_connection_id!r}, Filter Database={filter_source_database!r}, Max Artifacts={max_artifacts}")

repo = control_repo()

# COMMAND ----------

# Ensure dedicated control tables exist (Defensive & Idempotent)
spark.sql(build_create_artifact_control_ddl(catalog, control_schema))
spark.sql(build_create_artifact_execution_log_ddl(catalog, control_schema))

control_fqn = f"{quote_databricks(catalog)}.{quote_databricks(control_schema)}.{quote_databricks(SQL_ARTIFACT_CONTROL_TABLE)}"
log_fqn = f"{quote_databricks(catalog)}.{quote_databricks(control_schema)}.{quote_databricks(SQL_ARTIFACT_EXECUTION_LOG_TABLE)}"
assessment_fqn = f"{quote_databricks(catalog)}.{quote_databricks(control_schema)}.{quote_databricks('sql_object_assessment')}"
source_assessment_fqn = f"{quote_databricks(catalog)}.{quote_databricks(control_schema)}.{quote_databricks('source_assessment')}"

# COMMAND ----------

def _row_to_dict(r: Any) -> Dict[str, Any]:
    return r.asDict() if hasattr(r, "asDict") else dict(r)

# Discover Candidates using Shared Selection-Driven Query
candidate_query = build_candidate_query(
    source_assessment_fqn=source_assessment_fqn,
    sql_object_assessment_fqn=assessment_fqn,
    connection_id=filter_connection_id,
    source_database=filter_source_database,
    max_artifacts=max_artifacts,
)

raw_candidates = spark.sql(candidate_query).collect()
candidates = [_row_to_dict(r) for r in raw_candidates]
if max_artifacts and max_artifacts > 0:
    candidates = candidates[:max_artifacts]

print(f"Discovered {len(candidates)} selected candidate artifact(s)")

# COMMAND ----------

# Pre-fetch existing control records and build target ownership map
# Fail closed if control read fails (re-raise if reading historical control rows raises)
existing_control_records: Dict[str, Dict[str, Any]] = {}
try:
    historical_control_rows = [_row_to_dict(r) for r in spark.sql(f"SELECT * FROM {control_fqn}").collect()]
except Exception:
    raise

seen_historical_aids = set()
for row in historical_control_rows:
    aid = row.get("artifact_id")
    if aid:
        if aid in seen_historical_aids:
            raise RuntimeError(f"Duplicate artifact_id found in historical control rows: {aid}")
        seen_historical_aids.add(aid)
        existing_control_records[aid] = row

# Read source_table_control to build object_map (raise if read fails)
source_table_control_fqn = f"{quote_databricks(catalog)}.{quote_databricks(control_schema)}.{quote_databricks('source_table_control')}"
try:
    stc_rows = [_row_to_dict(r) for r in spark.sql(f"SELECT * FROM {source_table_control_fqn}").collect()]
except Exception as e:
    raise RuntimeError(f"Failed to read source_table_control: {sanitize_error(e)}") from e

# Cache for verified volumes
verified_volumes = set()

converter = SQLArtifactConverter()

# Counters for run summary
total_count = len(candidates)
auto_count = 0
manual_review_count = 0
unsupported_count = 0
failed_count = 0
definition_missing_count = 0
artifact_write_failed_count = 0
skipped_duplicates_count = 0
skipped_connections = set()

control_updates: List[Dict[str, Any]] = []
execution_logs: List[Dict[str, Any]] = []
now_utc = datetime.now(timezone.utc)

# Same-run deduplication on artifact_id: keep latest, log others as skipped duplicate
grouped_candidates: Dict[str, List[Dict[str, Any]]] = {}
candidates_to_process: List[Tuple[str, str, Dict[str, Any]]] = []

for cand in candidates:
    conn_id = str(cand.get("connection_id") or "").strip()
    src_sys = str(cand.get("source_system") or "").strip().lower()
    src_db = cand.get("source_database")
    src_sch = str(cand.get("source_schema") or "").strip()
    obj_name = str(cand.get("object_name") or "").strip()
    obj_type = cand.get("object_type")
    try:
        norm_type = normalize_artifact_type(obj_type)
        aid = compute_artifact_id(
            connection_id=conn_id,
            source_system=src_sys,
            source_database=src_db,
            source_schema=src_sch,
            object_type=norm_type,
            object_name=obj_name,
        )
        grouped_candidates.setdefault(aid, []).append((norm_type, cand))
    except Exception as e:
        failed_count += 1
        err_msg = sanitize_error(e)
        execution_logs.append({
            "log_id": uuid.uuid4().hex,
            "run_id": run_id,
            "artifact_id": "UNKNOWN",
            "processing_stage": "IDENTITY_RESOLUTION",
            "attempt_number": 1,
            "start_ts": now_utc,
            "end_ts": datetime.now(timezone.utc),
            "status": "FAILED",
            "source_definition_hash": None,
            "converted_definition_hash": None,
            "classification": None,
            "error_code": ERR_INVALID_IDENTITY,
            "error_message": err_msg,
            "execution_metadata": json.dumps({"schema": src_sch, "object": obj_name}),
            "created_ts": datetime.now(timezone.utc),
        })

for aid, cand_list in grouped_candidates.items():
    if len(cand_list) == 1:
        norm_type, cand = cand_list[0]
        candidates_to_process.append((aid, norm_type, cand))
    else:
        # Keep latest based on captured_ts
        sorted_cands = sorted(
            cand_list,
            key=lambda item: str(item[1].get("captured_ts") or ""),
            reverse=True,
        )
        kept_norm_type, kept_cand = sorted_cands[0]
        candidates_to_process.append((aid, kept_norm_type, kept_cand))

        # Log skipped duplicates
        for dup_norm_type, dup_cand in sorted_cands[1:]:
            skipped_duplicates_count += 1
            execution_logs.append({
                "log_id": uuid.uuid4().hex,
                "run_id": run_id,
                "artifact_id": aid,
                "processing_stage": "CANDIDATE_DEDUPLICATION",
                "attempt_number": 1,
                "start_ts": now_utc,
                "end_ts": datetime.now(timezone.utc),
                "status": "SKIPPED",
                "source_definition_hash": None,
                "converted_definition_hash": None,
                "classification": CLASSIFICATION_MANUAL_REVIEW,
                "error_code": "SKIPPED_DUPLICATE",
                "error_message": f"Skipped duplicate candidate for artifact_id {aid}",
                "execution_metadata": json.dumps({"reason": "DUPLICATE_CANDIDATE_SAME_RUN"}),
                "created_ts": datetime.now(timezone.utc),
            })

# Cache connection usability results
connection_usable_cache: Dict[str, Tuple[bool, str]] = {}

for artifact_id, norm_type, cand in candidates_to_process:
    conn_id = str(cand.get("connection_id") or "").strip()
    src_sys = str(cand.get("source_system") or "").strip().lower()
    src_db = cand.get("source_database")
    src_sch = str(cand.get("source_schema") or "").strip()
    obj_name = str(cand.get("object_name") or "").strip()
    src_def = cand.get("source_definition")
    captured_ts = cand.get("captured_ts") or now_utc

    stage_start_ts = datetime.now(timezone.utc)
    log_id = uuid.uuid4().hex

    prev_record = existing_control_records.get(artifact_id)
    attempt_num = (prev_record.get("attempt_count") or 0) + 1 if prev_record else 1
    first_seen = prev_record.get("first_seen_ts") if prev_record else (captured_ts or now_utc)

    # 1. Connection Usability Check (Skip connection without JDBC access)
    if conn_id not in connection_usable_cache:
        try:
            if not hasattr(repo, "get_connection"):
                raise RuntimeError("Control repository does not implement get_connection")
            conn_row = repo.get_connection(conn_id)
            if not conn_row:
                connection_usable_cache[conn_id] = (False, f"Connection {conn_id} not found in control repository")
            else:
                if isinstance(conn_row, (list, tuple)):
                    conn_row = conn_row[0]
                cd = conn_row.asDict() if hasattr(conn_row, "asDict") else dict(conn_row)
                usable, reason = is_connection_usable(cd)
                connection_usable_cache[conn_id] = (usable, reason)
        except Exception as e:
            connection_usable_cache[conn_id] = (False, sanitize_error(e))

    conn_usable, conn_reason = connection_usable_cache[conn_id]
    if not conn_usable:
        skipped_connections.add(conn_id)
        execution_logs.append({
            "log_id": log_id,
            "run_id": run_id,
            "artifact_id": artifact_id,
            "processing_stage": "CONNECTION_VALIDATION",
            "attempt_number": attempt_num,
            "start_ts": stage_start_ts,
            "end_ts": datetime.now(timezone.utc),
            "status": "SKIPPED",
            "source_definition_hash": None,
            "converted_definition_hash": None,
            "classification": CLASSIFICATION_MANUAL_REVIEW,
            "error_code": "CONNECTION_NOT_USABLE",
            "error_message": f"Connection {conn_id} is not usable: {conn_reason}",
            "execution_metadata": json.dumps({"connection_id": conn_id, "reason": conn_reason}),
            "created_ts": datetime.now(timezone.utc),
        })
        continue

    # 2. Target Routing Configuration Check
    try:
        target_cfg = repo.resolve_target_config(conn_id)
        target_cat, target_sch = resolve_target_catalog_and_schema(target_cfg, src_db, src_sch)
    except Exception as e:
        err_msg = sanitize_error(e)
        failed_count += 1
        ctrl_fail = {
            "artifact_id": artifact_id,
            "connection_id": conn_id,
            "source_system": src_sys,
            "source_database": src_db,
            "source_schema": src_sch,
            "object_name": obj_name,
            "object_type": norm_type,
            "target_catalog": None,
            "target_schema": None,
            "target_object_name": None,
            "source_definition": src_def,
            "converted_definition": None,
            "source_definition_hash": "0" * 64,
            "converted_definition_hash": None,
            "conversion_classification": CLASSIFICATION_UNSUPPORTED,
            "conversion_status": CONVERSION_STATUS_FAILED,
            "deployment_status": DEPLOYMENT_STATUS_NOT_DEPLOYED,
            "manual_review_required": True,
            "manual_review_reason": f"Target routing resolution failed: {err_msg}",
            "unsupported_features": None,
            "error_code": ERR_CONFIG_MISSING,
            "error_message": err_msg,
            "attempt_count": attempt_num,
            "first_seen_ts": first_seen,
            "last_seen_ts": datetime.now(timezone.utc),
            "conversion_ts": None,
            "deployment_ts": None,
            "created_ts": (prev_record.get("created_ts") if prev_record else None) or now_utc,
            "updated_ts": datetime.now(timezone.utc),
            "run_id": run_id,
            "is_active": False,
            "identity_version": 2,
            "legacy_artifact_id": prev_record.get("legacy_artifact_id") if prev_record else None,
        }
        control_updates.append(ctrl_fail)
        execution_logs.append({
            "log_id": log_id,
            "run_id": run_id,
            "artifact_id": artifact_id,
            "processing_stage": "CONFIG_RESOLUTION",
            "attempt_number": attempt_num,
            "start_ts": stage_start_ts,
            "end_ts": datetime.now(timezone.utc),
            "status": "FAILED",
            "source_definition_hash": None,
            "converted_definition_hash": None,
            "classification": CLASSIFICATION_UNSUPPORTED,
            "error_code": ERR_CONFIG_MISSING,
            "error_message": err_msg,
            "execution_metadata": json.dumps({"connection_id": conn_id}),
            "created_ts": datetime.now(timezone.utc),
        })
        continue

    norm_target_obj = normalize_target_identifier(obj_name, identifier_type=norm_type.lower())

    # 3. Check for Missing or Blank Source Definition
    if src_def is None or not str(src_def).strip():
        definition_missing_count += 1
        failed_count += 1
        err_msg = "DEFINITION_MISSING: Selected object has no row or a blank definition in sql_object_assessment"
        ctrl_missing = {
            "artifact_id": artifact_id,
            "connection_id": conn_id,
            "source_system": src_sys,
            "source_database": src_db,
            "source_schema": src_sch,
            "object_name": obj_name,
            "object_type": norm_type,
            "target_catalog": target_cat,
            "target_schema": target_sch,
            "target_object_name": norm_target_obj,
            "source_definition": None,
            "converted_definition": None,
            "source_definition_hash": "0" * 64,
            "converted_definition_hash": None,
            "conversion_classification": CLASSIFICATION_MANUAL_REVIEW,
            "conversion_status": CONVERSION_STATUS_FAILED,
            "deployment_status": DEPLOYMENT_STATUS_NOT_DEPLOYED,
            "manual_review_required": True,
            "manual_review_reason": "DEFINITION_MISSING",
            "unsupported_features": None,
            "error_code": "DEFINITION_MISSING",
            "error_message": err_msg,
            "attempt_count": attempt_num,
            "first_seen_ts": first_seen,
            "last_seen_ts": datetime.now(timezone.utc),
            "conversion_ts": None,
            "deployment_ts": None,
            "created_ts": (prev_record.get("created_ts") if prev_record else None) or now_utc,
            "updated_ts": datetime.now(timezone.utc),
            "run_id": run_id,
            "is_active": False,
            "identity_version": 2,
            "legacy_artifact_id": prev_record.get("legacy_artifact_id") if prev_record else None,
        }
        control_updates.append(ctrl_missing)
        execution_logs.append({
            "log_id": log_id,
            "run_id": run_id,
            "artifact_id": artifact_id,
            "processing_stage": "DEFINITION_CHECK",
            "attempt_number": attempt_num,
            "start_ts": stage_start_ts,
            "end_ts": datetime.now(timezone.utc),
            "status": "FAILED",
            "source_definition_hash": None,
            "converted_definition_hash": None,
            "classification": CLASSIFICATION_MANUAL_REVIEW,
            "error_code": "DEFINITION_MISSING",
            "error_message": err_msg,
            "execution_metadata": json.dumps({"reason": "DEFINITION_MISSING"}),
            "created_ts": datetime.now(timezone.utc),
        })
        continue

    # Source definition hash
    source_hash = compute_definition_hash(src_def)

    # 4. Convert and Classify (No source access, no deployment)
    # Oracle views: wrap with CREATE OR REPLACE VIEW "<schema>"."<name>" AS <text> before conversion only
    def_for_conversion = wrap_oracle_view_definition_if_needed(
        source_system=src_sys,
        object_type=norm_type,
        source_schema=src_sch,
        object_name=obj_name,
        definition=src_def,
    )

    obj_map = build_object_map(stc_rows, conn_id, src_db)
    conv_res = converter.convert_artifact(
        source_system=src_sys,
        object_type=norm_type,
        object_name=obj_name,
        source_definition=def_for_conversion,
        target_catalog=target_cat,
        target_schema=target_sch,
        object_map=obj_map,
        source_schema=src_sch,
    )

    conv_ts = datetime.now(timezone.utc)

    # For MANUAL_REVIEW and UNSUPPORTED, store converted text if any, otherwise null
    stored_converted_def = conv_res.converted_definition if conv_res.converted_definition else None
    converted_hash = compute_definition_hash(stored_converted_def) if stored_converted_def else None

    # Write converted SQL as a .sql file under /Volumes/<catalog>/<schema>/_converted_artifacts/<relative_path>
    write_failed = False
    write_err_msg = None
    if stored_converted_def:
        try:
            rel_path = build_artifact_relative_path(
                connection_id=conn_id,
                source_system=src_sys,
                source_database=src_db,
                source_schema=src_sch,
                object_type=norm_type,
                object_name=obj_name,
            )
            conv_vol_path = build_artifact_volume_path(
                target_catalog=target_cat,
                target_schema=target_sch,
                volume_name="_converted_artifacts",
                relative_path=rel_path,
            )
            vol_key = (target_cat, target_sch, "_converted_artifacts")
            if vol_key not in verified_volumes:
                spark.sql(f"CREATE SCHEMA IF NOT EXISTS {quote_databricks(target_cat)}.{quote_databricks(target_sch)}")
                spark.sql(f"CREATE VOLUME IF NOT EXISTS {quote_databricks(target_cat)}.{quote_databricks(target_sch)}.`_converted_artifacts`")
                verified_volumes.add(vol_key)
            write_atomic_file(conv_vol_path, stored_converted_def)
        except Exception as e:
            write_failed = True
            write_err_msg = sanitize_error(e)
            failed_count += 1
            artifact_write_failed_count += 1
            print(f"Error: Failed to write converted artifact file: {write_err_msg}")

    # Track classification counts
    if not write_failed:
        if conv_res.classification == CLASSIFICATION_AUTO:
            auto_count += 1
        elif conv_res.classification == CLASSIFICATION_MANUAL_REVIEW:
            manual_review_count += 1
        else:
            unsupported_count += 1

    # Set deployment_status = NOT_DEPLOYED for every row
    deploy_status = DEPLOYMENT_STATUS_NOT_DEPLOYED

    # 5. Prepare Control Record Update
    row_conversion_status = CONVERSION_STATUS_FAILED if write_failed else conv_res.conversion_status
    row_error_code = "ARTIFACT_WRITE_FAILED" if write_failed else conv_res.error_code
    row_error_message = (f"ARTIFACT_WRITE_FAILED: {write_err_msg}" if write_failed else conv_res.error_message)
    row_manual_review = True if write_failed else conv_res.manual_review_required
    row_manual_reason = (f"ARTIFACT_WRITE_FAILED: {write_err_msg}" if write_failed else conv_res.manual_review_reason)

    ctrl_row = {
        "artifact_id": artifact_id,
        "connection_id": conn_id,
        "source_system": src_sys,
        "source_database": src_db,
        "source_schema": src_sch,
        "object_name": obj_name,
        "object_type": norm_type,
        "target_catalog": target_cat,
        "target_schema": target_sch,
        "target_object_name": norm_target_obj,
        "source_definition": src_def,  # Always keep raw source definition
        "converted_definition": stored_converted_def,
        "source_definition_hash": source_hash,
        "converted_definition_hash": converted_hash,
        "conversion_classification": conv_res.classification,
        "conversion_status": row_conversion_status,
        "deployment_status": deploy_status,
        "manual_review_required": row_manual_review,
        "manual_review_reason": row_manual_reason,
        "unsupported_features": conv_res.unsupported_features,
        "error_code": row_error_code,
        "error_message": row_error_message,
        "attempt_count": attempt_num,
        "first_seen_ts": first_seen,
        "last_seen_ts": datetime.now(timezone.utc),
        "conversion_ts": conv_ts,
        "deployment_ts": None,
        "created_ts": (prev_record.get("created_ts") if prev_record else None) or now_utc,
        "updated_ts": datetime.now(timezone.utc),
        "run_id": run_id,
        "is_active": False,
        "identity_version": 2,
        "legacy_artifact_id": prev_record.get("legacy_artifact_id") if prev_record else None,
    }
    control_updates.append(ctrl_row)

    # 6. Append Execution Log Entry
    execution_logs.append({
        "log_id": log_id,
        "run_id": run_id,
        "artifact_id": artifact_id,
        "processing_stage": "CONVERT_AND_STORE",
        "attempt_number": attempt_num,
        "start_ts": stage_start_ts,
        "end_ts": datetime.now(timezone.utc),
        "status": row_conversion_status,
        "source_definition_hash": source_hash,
        "converted_definition_hash": converted_hash,
        "classification": conv_res.classification,
        "error_code": row_error_code,
        "error_message": row_error_message,
        "execution_metadata": json.dumps({
            "target_fqn": databricks_fqn(target_cat, target_sch, norm_target_obj),
            "stored_in_volume": False if write_failed else bool(stored_converted_def),
            **({"write_error": write_err_msg} if write_failed else {}),
        }),
        "created_ts": datetime.now(timezone.utc),
    })

# COMMAND ----------

# Persist State (MERGE Control Table, Append Execution Log)
control_schema_struct = StructType([
    StructField("artifact_id", StringType(), False),
    StructField("connection_id", StringType(), False),
    StructField("source_system", StringType(), False),
    StructField("source_database", StringType(), True),
    StructField("source_schema", StringType(), False),
    StructField("object_name", StringType(), False),
    StructField("object_type", StringType(), False),
    StructField("target_catalog", StringType(), True),
    StructField("target_schema", StringType(), True),
    StructField("target_object_name", StringType(), True),
    StructField("source_definition", StringType(), True),
    StructField("converted_definition", StringType(), True),
    StructField("source_definition_hash", StringType(), False),
    StructField("converted_definition_hash", StringType(), True),
    StructField("conversion_classification", StringType(), False),
    StructField("conversion_status", StringType(), False),
    StructField("deployment_status", StringType(), False),
    StructField("manual_review_required", BooleanType(), False),
    StructField("manual_review_reason", StringType(), True),
    StructField("unsupported_features", StringType(), True),
    StructField("error_code", StringType(), True),
    StructField("error_message", StringType(), True),
    StructField("attempt_count", IntegerType(), False),
    StructField("first_seen_ts", TimestampType(), True),
    StructField("last_seen_ts", TimestampType(), True),
    StructField("conversion_ts", TimestampType(), True),
    StructField("deployment_ts", TimestampType(), True),
    StructField("created_ts", TimestampType(), False),
    StructField("updated_ts", TimestampType(), False),
    StructField("run_id", StringType(), True),
    StructField("is_active", BooleanType(), False),
    StructField("identity_version", IntegerType(), True),
    StructField("legacy_artifact_id", StringType(), True),
])

log_schema_struct = StructType([
    StructField("log_id", StringType(), False),
    StructField("run_id", StringType(), False),
    StructField("artifact_id", StringType(), False),
    StructField("processing_stage", StringType(), False),
    StructField("attempt_number", IntegerType(), False),
    StructField("start_ts", TimestampType(), True),
    StructField("end_ts", TimestampType(), True),
    StructField("status", StringType(), False),
    StructField("source_definition_hash", StringType(), True),
    StructField("converted_definition_hash", StringType(), True),
    StructField("classification", StringType(), True),
    StructField("error_code", StringType(), True),
    StructField("error_message", StringType(), True),
    StructField("execution_metadata", StringType(), True),
    StructField("created_ts", TimestampType(), False),
])

if control_updates:
    ctrl_df = spark.createDataFrame(control_updates, schema=control_schema_struct)
    ctrl_view = f"_artifact_ctrl_updates_{uuid.uuid4().hex}"
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
          t.converted_definition = s.converted_definition,
          t.source_definition_hash = s.source_definition_hash,
          t.converted_definition_hash = s.converted_definition_hash,
          t.conversion_classification = s.conversion_classification,
          t.conversion_status = s.conversion_status,
          t.deployment_status = s.deployment_status,
          t.manual_review_required = s.manual_review_required,
          t.manual_review_reason = s.manual_review_reason,
          t.unsupported_features = s.unsupported_features,
          t.error_code = s.error_code,
          t.error_message = s.error_message,
          t.attempt_count = s.attempt_count,
          t.last_seen_ts = s.last_seen_ts,
          t.conversion_ts = s.conversion_ts,
          t.deployment_ts = s.deployment_ts,
          t.updated_ts = s.updated_ts,
          t.run_id = s.run_id,
          t.is_active = s.is_active,
          t.identity_version = s.identity_version,
          t.legacy_artifact_id = s.legacy_artifact_id
        WHEN NOT MATCHED THEN INSERT (
          artifact_id, connection_id, source_system, source_database,
          source_schema, object_name, object_type, target_catalog,
          target_schema, target_object_name, source_definition,
          converted_definition, source_definition_hash, converted_definition_hash,
          conversion_classification, conversion_status, deployment_status,
          manual_review_required, manual_review_reason, unsupported_features,
          error_code, error_message, attempt_count, first_seen_ts,
          last_seen_ts, conversion_ts, deployment_ts, created_ts,
          updated_ts, run_id, is_active, identity_version, legacy_artifact_id
        ) VALUES (
          s.artifact_id, s.connection_id, s.source_system, s.source_database,
          s.source_schema, s.object_name, s.object_type, s.target_catalog,
          s.target_schema, s.target_object_name, s.source_definition,
          s.converted_definition, s.source_definition_hash, s.converted_definition_hash,
          s.conversion_classification, s.conversion_status, s.deployment_status,
          s.manual_review_required, s.manual_review_reason, s.unsupported_features,
          s.error_code, s.error_message, s.attempt_count, s.first_seen_ts,
          s.last_seen_ts, s.conversion_ts, s.deployment_ts, s.created_ts,
          s.updated_ts, s.run_id, s.is_active, s.identity_version, s.legacy_artifact_id
        )
    """)
    print(f"Updated {len(control_updates)} record(s) in {control_fqn}")

if execution_logs:
    log_df = spark.createDataFrame(execution_logs, schema=log_schema_struct)
    log_df.write.format("delta").mode("append").saveAsTable(log_fqn.replace("`", ""))
    print(f"Appended {len(execution_logs)} log row(s) to {log_fqn}")

# COMMAND ----------

# Determine explicit business status
has_progress = (auto_count + manual_review_count + unsupported_count) > 0
has_problems = failed_count > 0 or len(skipped_connections) > 0

if total_count == 0:
    business_status = "NO_CANDIDATES"
elif (artifact_write_failed_count > 0 or definition_missing_count > 0 or len(skipped_connections) > 0) and not has_progress:
    business_status = "PARTIAL"
elif has_problems and not has_progress:
    business_status = "FAILED"
elif has_problems:
    business_status = "PARTIAL"
elif manual_review_count > 0:
    business_status = "MANUAL_REVIEW_REQUIRED"
elif unsupported_count > 0:
    business_status = "UNSUPPORTED_PRESENT"
else:
    business_status = "COMPLETE"

status = "FAILED" if business_status == "FAILED" else "SUCCEEDED"

# Publish Task Values for Orchestrator
set_task_value("run_id", run_id)
set_task_value("status", status)
set_task_value("business_status", business_status)
set_task_value("candidates_in_scope", total_count)
set_task_value("auto_count", auto_count)
set_task_value("manual_review_count", manual_review_count)
set_task_value("unsupported_count", unsupported_count)
set_task_value("failed_count", failed_count)
set_task_value("definition_missing_count", definition_missing_count)
set_task_value("artifact_write_failed_count", artifact_write_failed_count)
set_task_value("skipped_duplicates_count", skipped_duplicates_count)
set_task_value("skipped_connections_count", len(skipped_connections))

summary = {
    "status": status,
    "business_status": business_status,
    "run_id": run_id,
    "total_candidates": total_count,
    "auto_count": auto_count,
    "manual_review_count": manual_review_count,
    "unsupported_count": unsupported_count,
    "failed_count": failed_count,
    "definition_missing_count": definition_missing_count,
    "artifact_write_failed_count": artifact_write_failed_count,
    "skipped_duplicates_count": skipped_duplicates_count,
    "skipped_connections_count": len(skipped_connections),
    "classifications": {
        "AUTO": auto_count,
        "MANUAL_REVIEW": manual_review_count,
        "UNSUPPORTED": unsupported_count,
    },
}
print("Run Summary:", json.dumps(summary, indent=2))

if failed_count > 0:
    if business_status == "FAILED":
        raise RuntimeError(f"NB22_SQLArtifactMigrate failed: failed={failed_count}, skipped_connections={len(skipped_connections)}")
elif business_status == "FAILED":
    raise RuntimeError(f"NB22_SQLArtifactMigrate failed: business_status=FAILED, skipped_connections={len(skipped_connections)}")

dbutils.notebook.exit(json.dumps(summary))
