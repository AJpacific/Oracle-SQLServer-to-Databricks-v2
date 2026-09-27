# Databricks notebook source
# MAGIC %md
# MAGIC # NB24_LakebridgeAnalyzeAndTranspile
# MAGIC Dedicated notebook for Databricks Labs Lakebridge Analyzer & BladeBridge transpilation.
# MAGIC Analyzes and transpiles selected VIEW and PROCEDURE objects.
# MAGIC Persists Analyzer reports and converted files to Unity Catalog Volumes.
# MAGIC Maintains `sql_artifact_control` and `sql_artifact_execution_log`.
# MAGIC Never executes converted SQL. Never invokes CREATE VIEW or CREATE PROCEDURE.

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

import os
import sys
import shutil
import json
import uuid
import subprocess
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union
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
        databricks_fqn,
        normalize_target_identifier,
    )
    from src.sql_artifact_control_common import (
        SQL_ARTIFACT_CONTROL_TABLE,
        SQL_ARTIFACT_EXECUTION_LOG_TABLE,
        SQL_ARTIFACT_CONTROL_COLUMNS,
        SQL_ARTIFACT_EXECUTION_LOG_COLUMNS,
        STAGE_LAKEBRIDGE_ANALYZE,
        STAGE_LAKEBRIDGE_TRANSPILE,
        STAGE_LAKEBRIDGE_STORE,
        STAGE_LAKEBRIDGE_CLASSIFY,
        LAKEBRIDGE_CLASSIFICATION_AUTO_CANDIDATE,
        LAKEBRIDGE_CLASSIFICATION_MANUAL_REVIEW,
        LAKEBRIDGE_CLASSIFICATION_UNSUPPORTED,
        compute_artifact_id,
        compute_definition_hash,
        resolve_target_catalog_and_schema,
        sanitize_error,
    )
    from src.failure_classifier import sanitize_message
    from src.lakebridge_artifact_common import (
        normalize_source_system,
        normalize_object_type,
        normalize_diagnostic_severity,
        prepare_lakebridge_input_file,
        read_analyzer_workbook,
        read_analyzer_report,
        get_analyzer_platform,
        get_bladebridge_tech,
        extract_sql_from_bladebridge_mime,
        extract_complexity,
        extract_statement_counts,
        extract_unknown_fragments,
        detect_sql_constructs,
        count_fixme_markers,
        detect_remaining_source_syntax,
        extract_referenced_objects,
        build_bounded_json,
        derive_lakebridge_classification,
        sanitize_cli_output,
        build_lakebridge_report_path,
        build_lakebridge_converted_path,
        build_lakebridge_error_path,
        has_usable_analyzer_results,
        CONVERTED_ARTIFACT_CATALOG,
        CONVERTED_ARTIFACT_SCHEMA,
        CONVERTED_ARTIFACT_VOLUME,
        CONVERTED_ARTIFACT_OWNERSHIP_TABLE,
        build_create_converted_artifact_ownership_ddl,
        claim_converted_artifact_path,
    )
    from src.sql_object_artifact_common import (
        build_artifact_relative_path,
        build_artifact_volume_path,
        canonical_artifact_owner_id,
    )
    from src.lakebridge_environment import ensure_lakebridge_environment
    import src.lakebridge_runner as _lb_runner_module
    RUNNER_SCRIPT_PATH = os.path.abspath(_lb_runner_module.__file__)
except ModuleNotFoundError:
    from identifiers import (
        quote_databricks,
        escape_string_literal,
        databricks_fqn,
        normalize_target_identifier,
    )
    from sql_artifact_control_common import (
        SQL_ARTIFACT_CONTROL_TABLE,
        SQL_ARTIFACT_EXECUTION_LOG_TABLE,
        SQL_ARTIFACT_CONTROL_COLUMNS,
        SQL_ARTIFACT_EXECUTION_LOG_COLUMNS,
        STAGE_LAKEBRIDGE_ANALYZE,
        STAGE_LAKEBRIDGE_TRANSPILE,
        STAGE_LAKEBRIDGE_STORE,
        STAGE_LAKEBRIDGE_CLASSIFY,
        LAKEBRIDGE_CLASSIFICATION_AUTO_CANDIDATE,
        LAKEBRIDGE_CLASSIFICATION_MANUAL_REVIEW,
        LAKEBRIDGE_CLASSIFICATION_UNSUPPORTED,
        compute_artifact_id,
        compute_definition_hash,
        resolve_target_catalog_and_schema,
        sanitize_error,
    )
    from failure_classifier import sanitize_message
    from lakebridge_artifact_common import (
        normalize_source_system,
        normalize_object_type,
        normalize_diagnostic_severity,
        prepare_lakebridge_input_file,
        read_analyzer_workbook,
        read_analyzer_report,
        get_analyzer_platform,
        get_bladebridge_tech,
        extract_sql_from_bladebridge_mime,
        extract_complexity,
        extract_statement_counts,
        extract_unknown_fragments,
        detect_sql_constructs,
        count_fixme_markers,
        detect_remaining_source_syntax,
        extract_referenced_objects,
        build_bounded_json,
        derive_lakebridge_classification,
        sanitize_cli_output,
        build_lakebridge_report_path,
        build_lakebridge_converted_path,
        build_lakebridge_error_path,
        has_usable_analyzer_results,
        CONVERTED_ARTIFACT_CATALOG,
        CONVERTED_ARTIFACT_SCHEMA,
        CONVERTED_ARTIFACT_VOLUME,
        CONVERTED_ARTIFACT_OWNERSHIP_TABLE,
        build_create_converted_artifact_ownership_ddl,
        claim_converted_artifact_path,
    )
    from sql_object_artifact_common import (
        build_artifact_relative_path,
        build_artifact_volume_path,
        canonical_artifact_owner_id,
    )
    from lakebridge_environment import ensure_lakebridge_environment
    import lakebridge_runner as _lb_runner_module
    RUNNER_SCRIPT_PATH = os.path.abspath(_lb_runner_module.__file__)

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

print(f"=== Starting Lakebridge Analyze and Transpile run_id={run_id} ===")
print(f"Control Catalog={catalog}, Control Schema={control_schema}")

control_fqn = f"{quote_databricks(catalog)}.{quote_databricks(control_schema)}.{quote_databricks(SQL_ARTIFACT_CONTROL_TABLE)}"
log_fqn = f"{quote_databricks(catalog)}.{quote_databricks(control_schema)}.{quote_databricks(SQL_ARTIFACT_EXECUTION_LOG_TABLE)}"

# COMMAND ----------

# 1. Read candidate artifacts from sql_artifact_control for current run
query = f"""
    SELECT *
    FROM {control_fqn}
    WHERE run_id = {escape_string_literal(run_id)}
"""

ctrl_rows = spark.sql(query).collect()
all_run_candidates = [r.asDict() if hasattr(r, "asDict") else dict(r) for r in ctrl_rows]
total_candidates = len(all_run_candidates)

candidates_to_process = [
    c for c in all_run_candidates
    if c.get("source_definition") and str(c.get("source_definition")).strip()
]

print(f"Total run candidates in control: {total_candidates}; with fetched source definitions: {len(candidates_to_process)}")

if total_candidates == 0:
    set_task_value("run_id", run_id)
    set_task_value("status", "SUCCEEDED")
    set_task_value("business_status", "NO_CANDIDATES")
    set_task_value("analyzed_count", 0)
    set_task_value("transpiled_count", 0)
    set_task_value("auto_candidate_count", 0)
    set_task_value("manual_review_count", 0)
    set_task_value("unsupported_count", 0)
    empty_summary = {
        "status": "SUCCEEDED",
        "business_status": "NO_CANDIDATES",
        "run_id": run_id,
        "analyzed_count": 0,
        "transpiled_count": 0,
        "auto_candidate_count": 0,
        "manual_review_count": 0,
        "unsupported_count": 0,
    }
    dbutils.notebook.exit(json.dumps(empty_summary))

# COMMAND ----------

def write_atomic_file(target_path: str, content: Union[str, bytes]) -> None:
    temp_path = f"{target_path}.tmp.{uuid.uuid4().hex}"
    os.makedirs(os.path.dirname(target_path), exist_ok=True)
    mode = "wb" if isinstance(content, bytes) else "w"
    encoding = None if isinstance(content, bytes) else "utf-8"
    try:
        with open(temp_path, mode, encoding=encoding) as f:
            f.write(content)
        os.replace(temp_path, target_path)
    except Exception:
        if os.path.exists(temp_path):
            try:
                os.remove(temp_path)
            except OSError:
                pass
        raise

# Optional custom runner for testing or dependency injection
custom_lakebridge_runner: Optional[Callable[[Dict[str, Any], str, str], Dict[str, Any]]] = None

# Environment bootstrap and availability check before processing artifacts
try:
    env_ready, venv_python, env_details = ensure_lakebridge_environment()
except Exception as exc_env:
    env_ready = False
    venv_python = None
    env_details = {"error": sanitize_cli_output(exc_env)}

if not env_ready:
    safe_chk_err = sanitize_cli_output(env_details.get("error", "Lakebridge environment unavailable"))
    print(f"FATAL: Lakebridge environment initialization failed: {safe_chk_err}")
    set_task_value("run_id", run_id)
    set_task_value("status", "FAILED")
    set_task_value("business_status", "FAILED")
    set_task_value("candidates_in_scope", total_candidates)
    set_task_value("analyzed_count", 0)
    set_task_value("transpiled_count", 0)
    set_task_value("auto_candidate_count", 0)
    set_task_value("manual_review_count", 0)
    set_task_value("unsupported_count", total_candidates)
    set_task_value("parsing_error_count", 0)
    set_task_value("validation_error_count", 0)
    set_task_value("generation_error_count", 0)
    set_task_value("fixme_artifact_count", 0)
    set_task_value("persistent_store_failure_count", 0)
    raise RuntimeError(f"Lakebridge runtime environment is unavailable: {safe_chk_err}")

print(f"Lakebridge isolated runtime ready using Python: {venv_python}")

def execute_lakebridge_runner(
    request_data: Dict[str, Any],
    attempt_dir: str,
    timeout_sec: int = 180,
) -> Dict[str, Any]:
    """Execute action through isolated runner script using the seeded environment Python."""
    req_path = os.path.join(attempt_dir, f"request_{uuid.uuid4().hex[:8]}.json")
    resp_path = os.path.join(attempt_dir, f"response_{uuid.uuid4().hex[:8]}.json")

    with open(req_path, "w", encoding="utf-8") as f:
        json.dump(request_data, f, indent=2)

    if custom_lakebridge_runner:
        return custom_lakebridge_runner(request_data, req_path, resp_path)

    cmd = [venv_python or sys.executable, RUNNER_SCRIPT_PATH, "--request", req_path, "--response", resp_path]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, check=False, timeout=timeout_sec)
        if os.path.isfile(resp_path):
            try:
                with open(resp_path, "r", encoding="utf-8") as rf:
                    return json.load(rf)
            except Exception:
                pass
        return {
            "status": "FAILED",
            "error": sanitize_cli_output(proc.stderr or proc.stdout or f"Process failed with exit code {proc.returncode}"),
            "exit_code": proc.returncode,
        }
    except subprocess.TimeoutExpired:
        return {
            "status": "FAILED",
            "error": f"Lakebridge runner execution timed out after {timeout_sec}s",
            "exit_code": 124,
        }
    except Exception as exc_run:
        return {
            "status": "FAILED",
            "error": sanitize_message(exc_run),
            "exit_code": 1,
        }

verified_schemas: Set[Tuple[str, str]] = set()
verified_volumes: Set[Tuple[str, str, str]] = set()
run_claimed_converted_paths: Dict[str, str] = {}

analyzed_count = 0
transpiled_count = 0
auto_candidate_count = 0
manual_review_count = 0
unsupported_count = 0
parsing_error_count = 0
validation_error_count = 0
generation_error_count = 0
fixme_artifact_count = 0
store_failure_count = 0

now_utc = datetime.now(timezone.utc)

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
    ctrl_view = f"_ctrl_merge_{uuid.uuid4().hex}"
    ctrl_df.createOrReplaceTempView(ctrl_view)
    spark.sql(f"""
        MERGE INTO {control_fqn} t
        USING {ctrl_view} s
           ON t.artifact_id = s.artifact_id
        WHEN MATCHED THEN UPDATE SET
          t.converted_definition = s.converted_definition,
          t.converted_definition_hash = s.converted_definition_hash,
          t.converted_artifact_path = s.converted_artifact_path,
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
          t.updated_ts = s.updated_ts,
          t.lakebridge_analyzed_ts = s.lakebridge_analyzed_ts,
          t.lakebridge_complexity = s.lakebridge_complexity,
          t.lakebridge_statement_count = s.lakebridge_statement_count,
          t.lakebridge_unknown_statement_count = s.lakebridge_unknown_statement_count,
          t.lakebridge_unknown_fragments = s.lakebridge_unknown_fragments,
          t.lakebridge_uses_error_handling = s.lakebridge_uses_error_handling,
          t.lakebridge_uses_rowcount = s.lakebridge_uses_rowcount,
          t.lakebridge_uses_cursor = s.lakebridge_uses_cursor,
          t.lakebridge_uses_dynamic_sql = s.lakebridge_uses_dynamic_sql,
          t.lakebridge_uses_trigger = s.lakebridge_uses_trigger,
          t.lakebridge_referenced_objects = s.lakebridge_referenced_objects,
          t.lakebridge_parsing_error_count = s.lakebridge_parsing_error_count,
          t.lakebridge_validation_error_count = s.lakebridge_validation_error_count,
          t.lakebridge_generation_error_count = s.lakebridge_generation_error_count,
          t.lakebridge_fixme_count = s.lakebridge_fixme_count,
          t.lakebridge_transpiled_definition = s.lakebridge_transpiled_definition,
          t.object_map_applied = s.object_map_applied,
          t.lakebridge_classification = s.lakebridge_classification
        WHEN NOT MATCHED THEN INSERT (
          artifact_id, connection_id, source_system, source_database,
          source_schema, object_name, object_type, target_catalog,
          target_schema, target_object_name, source_definition,
          converted_definition, source_definition_hash, converted_definition_hash,
          converted_artifact_path,
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
          s.converted_artifact_path,
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

# Process each artifact independently with isolated attempt-staging and immediate logging
for cand in candidates_to_process:
    art_id = cand.get("artifact_id", "unknown")
    attempt_dir = ""
    artifact_counted = False
    try:
        oname = cand.get("object_name")
        otype = normalize_object_type(cand.get("object_type"))
        sch = cand.get("source_schema")
        src_db = cand.get("source_database")
        conn_id = cand.get("connection_id")
        src_def = cand.get("source_definition")
        src_sys = normalize_source_system(cand.get("source_system"))
        target_cat = cand.get("target_catalog") or catalog
        target_sch = cand.get("target_schema") or (sch.lower() if sch else "default")
        attempt_num = cand.get("attempt_count") or 1
        src_hash = cand.get("source_definition_hash") or compute_definition_hash(src_def)

        cand_uuid = uuid.uuid4().hex
        attempt_dir = f"/local_disk0/sql_artifact_lakebridge/{run_id}/{art_id}/{attempt_num}/{cand_uuid}"

        if os.path.exists(attempt_dir):
            raise RuntimeError(f"Unique staging attempt directory already exists: {attempt_dir}")

        input_dir = os.path.join(attempt_dir, "input")
        report_dir = os.path.join(attempt_dir, "report")
        output_dir = os.path.join(attempt_dir, "output")
        error_dir = os.path.join(attempt_dir, "error")

        os.makedirs(input_dir, exist_ok=False)
        os.makedirs(report_dir, exist_ok=False)
        os.makedirs(output_dir, exist_ok=False)
        os.makedirs(error_dir, exist_ok=False)

        print(f"\nProcessing artifact {sch}.{oname} ({otype}) id={art_id}, attempt={attempt_num}")
        tech = "mssql" if src_sys == "sqlserver" else "oracle"

        # 1. Prepare collision-resistant input file
        input_file = prepare_lakebridge_input_file(
            input_base_dir=input_dir,
            connection_id=conn_id,
            source_database=src_db,
            source_schema=sch,
            object_type=otype,
            object_name=oname,
            source_definition=src_def,
            artifact_id=art_id,
            source_system=src_sys,
        )

        # 2. Run Lakebridge Analyzer via isolated Python runner
        report_file = os.path.join(report_dir, f"{art_id}_report.xlsx")
        report_json_file = os.path.join(report_dir, f"{art_id}_report.json")
        an_req = {
            "action": "analyze",
            "input_dir": os.path.abspath(input_dir),
            "report_xlsx_path": os.path.abspath(report_file),
            "report_json_path": os.path.abspath(report_json_file),
            "source_system": src_sys,
            "is_debug": False,
        }
        an_start_ts = datetime.now(timezone.utc)
        an_resp = execute_lakebridge_runner(an_req, attempt_dir)
        an_end_ts = datetime.now(timezone.utc)

        raw_an_status = an_resp.get("status")
        runner_an_err = sanitize_cli_output(an_resp.get("error", ""))
        runner_an_warn = sanitize_cli_output(an_resp.get("warning", ""))

        # 3. Read Analyzer report if available (preferring structured JSON inventory, with XLSX fallback)
        workbook_data: Dict[str, List[Dict[str, Any]]] = {}
        report_exists = (
            (os.path.isfile(report_file) and os.path.getsize(report_file) > 0)
            or (os.path.isfile(report_json_file) and os.path.getsize(report_json_file) > 0)
        )
        report_parse_err: Optional[str] = None
        has_usable_report = False

        if report_exists:
            try:
                workbook_data = read_analyzer_report(
                    report_xlsx_path=report_file if os.path.exists(report_file) else None,
                    report_json_path=report_json_file if os.path.exists(report_json_file) else None,
                )
                has_usable_report = has_usable_analyzer_results(workbook_data)
                if not has_usable_report:
                    report_parse_err = "Analyzer report generated but contains no usable inventory results"
            except Exception as exc_wb:
                report_parse_err = sanitize_message(exc_wb)
                print(f"  [warn] Failed to parse Analyzer report: {report_parse_err}")

        # Decide analyzer_failed status
        if has_usable_report:
            analyzer_failed = False
            an_warning = runner_an_warn or runner_an_err
            safe_an_err = ""
            if an_warning:
                print(f"  [info] Analyzer report usable with non-fatal warning: {an_warning}")
        else:
            analyzer_failed = True
            an_warning = ""
            safe_an_err = runner_an_err or report_parse_err or "Lakebridge Analyzer failed to generate usable report"
            print(f"  [warn] Lakebridge Analyze failed: {safe_an_err}")
            with open(os.path.join(error_dir, f"{art_id}_analyzer_error.txt"), "w", encoding="utf-8") as f:
                f.write(safe_an_err)

        # 4. Run BladeBridge Transpile via isolated Python runner
        error_file = os.path.join(error_dir, f"{art_id}_error.txt")
        converted_file = os.path.join(output_dir, f"{art_id}_converted.sql")
        tr_req = {
            "action": "transpile",
            "source_file": os.path.abspath(input_file),
            "output_file": os.path.abspath(converted_file),
            "source_system": src_sys,
            "object_name": oname,
        }
        tr_start_ts = datetime.now(timezone.utc)
        tr_resp = execute_lakebridge_runner(tr_req, attempt_dir)
        tr_end_ts = datetime.now(timezone.utc)

        tr_diag_errors = int(tr_resp.get("diagnostic_error_count", 0))
        tr_diag_warnings = int(tr_resp.get("diagnostic_warning_count", 0))
        raw_tr_diagnostics = tr_resp.get("diagnostics", [])

        # Sanitize diagnostics: bounded, no source SQL, no secrets
        sanitized_diagnostics = []
        for d in (raw_tr_diagnostics or [])[:20]:
            if isinstance(d, dict):
                raw_m = str(d.get("message", ""))
                sev = normalize_diagnostic_severity(d.get("severity"))
            else:
                raw_m = getattr(d, "message", str(d))
                sev = normalize_diagnostic_severity(getattr(d, "severity", None))
            clean_m = sanitize_message(raw_m)[:500]
            if src_def and len(src_def) > 10 and src_def[:50] in clean_m:
                clean_m = clean_m.replace(src_def[:50], "<source_sql>")
            sanitized_diagnostics.append({"message": clean_m, "severity": sev})

        transpile_failed = (tr_resp.get("status") != "SUCCEEDED" or tr_diag_errors > 0)
        safe_tr_err = sanitize_cli_output(tr_resp.get("error", "")) if transpile_failed else ""
        if tr_diag_errors > 0 and not safe_tr_err:
            safe_tr_err = f"BladeBridge transpiler reported {tr_diag_errors} diagnostic error(s)"
        if transpile_failed:
            print(f"  [warn] Lakebridge Transpile failed: {safe_tr_err}")
            err_payload = f"Error: {safe_tr_err}\n"
            if sanitized_diagnostics:
                err_payload += "Sanitized Diagnostics:\n" + json.dumps(sanitized_diagnostics, indent=2)
            with open(error_file, "w", encoding="utf-8") as f:
                f.write(err_payload[:4000])

        # 5. Discover converted output in output_dir only
        raw_bladebridge_sql: Optional[str] = None
        if not transpile_failed and os.path.isfile(converted_file):
            try:
                with open(converted_file, "r", encoding="utf-8") as cf:
                    content = cf.read().strip()
                    if content:
                        raw_bladebridge_sql = extract_sql_from_bladebridge_mime(content)
            except Exception as exc_rf:
                transpile_failed = True
                safe_tr_err = sanitize_message(exc_rf)

        if not transpile_failed and not raw_bladebridge_sql:
            for root, _, files in os.walk(output_dir):
                for f in files:
                    if f.endswith(".sql"):
                        cand_path = os.path.join(root, f)
                        try:
                            with open(cand_path, "r", encoding="utf-8") as cf:
                                content = cf.read().strip()
                                if content:
                                    raw_bladebridge_sql = extract_sql_from_bladebridge_mime(content)
                                    break
                        except Exception as exc_rf:
                            safe_tr_err = sanitize_message(exc_rf)
                if raw_bladebridge_sql:
                    break

        if not transpile_failed and not raw_bladebridge_sql:
            transpile_failed = True
            safe_tr_err = "No converted SQL output produced in output folder"

        # 6. Extract Analyzer metadata and detect constructs
        staged_fname = os.path.basename(input_file)
        complexity = extract_complexity(workbook_data, oname, staged_filename=staged_fname, artifact_id=art_id)
        stmt_count, unk_stmt_count = extract_statement_counts(workbook_data, oname, staged_filename=staged_fname, artifact_id=art_id)
        unk_fragments = extract_unknown_fragments(workbook_data, oname, staged_filename=staged_fname, artifact_id=art_id)
        constructs = detect_sql_constructs(raw_bladebridge_sql or src_def or "", src_sys)
        fixme_count = count_fixme_markers(raw_bladebridge_sql or "")
        retains_syntax, retained_patterns = detect_remaining_source_syntax(raw_bladebridge_sql or "", src_sys)
        referenced_objects = extract_referenced_objects(workbook_data, raw_bladebridge_sql, oname, staged_filename=staged_fname, artifact_id=art_id)
        bounded_refs_json = build_bounded_json(referenced_objects, max_bytes=8192)
        bounded_fragments_json = build_bounded_json(unk_fragments, max_bytes=8192)

        p_err = 1 if analyzer_failed else 0
        v_err = 1 if retains_syntax else 0
        g_err = tr_diag_errors if tr_diag_errors > 0 else int(transpile_failed)

        parsing_error_count += p_err
        validation_error_count += v_err
        generation_error_count += g_err
        if fixme_count > 0:
            fixme_artifact_count += 1

        cls_res, conv_status, man_req, man_reason, err_code, err_msg = derive_lakebridge_classification(
            source_definition=src_def,
            converted_definition=raw_bladebridge_sql,
            complexity=complexity,
            statement_count=stmt_count,
            unknown_statement_count=unk_stmt_count,
            unknown_fragments=unk_fragments,
            constructs=constructs,
            parsing_error_count=p_err,
            validation_error_count=v_err,
            generation_error_count=g_err,
            fixme_count=fixme_count,
            remaining_source_syntax=retained_patterns,
            analyzer_failed=analyzer_failed,
            transpile_failed=transpile_failed,
            object_map_applied=False,
            require_object_map=False,
            object_type=otype,
            diagnostic_warning_count=tr_diag_warnings,
        )

        if transpile_failed and safe_tr_err:
            err_code = "TRANSPILE_FAILED"
            err_msg = safe_tr_err[:500]
            man_reason = f"Transpilation failed: {safe_tr_err}"[:500]
        elif analyzer_failed and safe_an_err:
            err_code = "ANALYZER_FAILED"
            err_msg = safe_an_err[:500]
            man_reason = f"Analysis failed: {safe_an_err}"[:500]

        if not analyzer_failed:
            analyzed_count += 1
        if raw_bladebridge_sql and not transpile_failed:
            transpiled_count += 1

        conv_hash = compute_definition_hash(raw_bladebridge_sql) if raw_bladebridge_sql else None

        # 7. Truthful persistence to Unity Catalog Volumes
        persistence_failed = False
        persistence_err = ""
        store_start_ts = datetime.now(timezone.utc)

        # 7a. Persist converted SQL if produced
        conv_vol_path: Optional[str] = None
        if raw_bladebridge_sql:
            try:
                owner_id = canonical_artifact_owner_id(
                    connection_id=conn_id,
                    source_system=src_sys,
                    source_database=src_db,
                    source_schema=sch,
                    object_type=otype,
                    object_name=oname,
                )
                rel_path = build_artifact_relative_path(
                    connection_id=conn_id,
                    source_system=src_sys,
                    source_database=src_db,
                    source_schema=sch,
                    object_type=otype,
                    object_name=oname,
                )
                conv_vol_path = build_lakebridge_converted_path(relative_path=rel_path)
                central_vol_key = (CONVERTED_ARTIFACT_CATALOG, CONVERTED_ARTIFACT_SCHEMA, CONVERTED_ARTIFACT_VOLUME)
                if central_vol_key not in verified_volumes:
                    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {quote_databricks(CONVERTED_ARTIFACT_CATALOG)}.{quote_databricks(CONVERTED_ARTIFACT_SCHEMA)}")
                    spark.sql(f"CREATE VOLUME IF NOT EXISTS {quote_databricks(CONVERTED_ARTIFACT_CATALOG)}.{quote_databricks(CONVERTED_ARTIFACT_SCHEMA)}.{quote_databricks(CONVERTED_ARTIFACT_VOLUME)}")
                    spark.sql(build_create_converted_artifact_ownership_ddl())
                    verified_volumes.add(central_vol_key)

                claim_ok, claim_err = claim_converted_artifact_path(
                    spark=spark,
                    artifact_path=conv_vol_path,
                    canonical_owner_id=owner_id,
                    artifact_id=art_id,
                    source_system=src_sys,
                    connection_id=conn_id,
                    run_id=run_id,
                    in_memory_claims=run_claimed_converted_paths,
                )
                if not claim_ok:
                    raise RuntimeError(claim_err)

                write_atomic_file(conv_vol_path, raw_bladebridge_sql)
            except Exception as exc_p:
                persistence_failed = True
                persistence_err = f"Failed to persist converted SQL: {sanitize_error(exc_p)}"
                conv_vol_path = None

        # 7b. Persist report file if produced
        for r_file in (report_file, report_json_file):
            if os.path.exists(r_file) and not persistence_failed:
                try:
                    report_vol_dir = build_lakebridge_report_path(catalog, control_schema, run_id, src_sys)
                    vol_key = (catalog, control_schema, "_lakebridge_reports")
                    if vol_key not in verified_volumes:
                        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {quote_databricks(catalog)}.{quote_databricks(control_schema)}")
                        spark.sql(f"CREATE VOLUME IF NOT EXISTS {quote_databricks(catalog)}.{quote_databricks(control_schema)}.`_lakebridge_reports`")
                        verified_volumes.add(vol_key)
                    dst_rep = os.path.join(report_vol_dir, os.path.basename(r_file))
                    with open(r_file, "rb") as rf:
                        write_atomic_file(dst_rep, rf.read())
                except Exception as exc_p:
                    persistence_failed = True
                    persistence_err = f"Failed to persist Analyzer report: {sanitize_error(exc_p)}"

        # 7c. Persist errors if any error files exist
        if os.path.exists(error_dir) and not persistence_failed:
            for ef in os.listdir(error_dir):
                ef_path = os.path.join(error_dir, ef)
                if os.path.isfile(ef_path):
                    try:
                        error_vol_dir = build_lakebridge_error_path(catalog, control_schema, run_id, src_sys)
                        vol_key = (catalog, control_schema, "_lakebridge_reports")
                        if vol_key not in verified_volumes:
                            spark.sql(f"CREATE SCHEMA IF NOT EXISTS {quote_databricks(catalog)}.{quote_databricks(control_schema)}")
                            spark.sql(f"CREATE VOLUME IF NOT EXISTS {quote_databricks(catalog)}.{quote_databricks(control_schema)}.`_lakebridge_reports`")
                            verified_volumes.add(vol_key)
                        dst_err = os.path.join(error_vol_dir, ef)
                        with open(ef_path, "rb") as rf:
                            write_atomic_file(dst_err, rf.read())
                    except Exception as exc_p:
                        persistence_failed = True
                        persistence_err = f"Failed to persist Lakebridge error: {sanitize_error(exc_p)}"
                        break

        store_end_ts = datetime.now(timezone.utc)

        if persistence_failed:
            store_failure_count += 1
            cls_res = LAKEBRIDGE_CLASSIFICATION_UNSUPPORTED
            conv_status = "FAILED"
            err_code = "ARTIFACT_WRITE_FAILED"
            err_msg = persistence_err
            man_req = True
            man_reason = persistence_err
            conv_vol_path = None

        # Increment classification counters only from the final stored classification
        if cls_res == LAKEBRIDGE_CLASSIFICATION_AUTO_CANDIDATE:
            auto_candidate_count += 1
        elif cls_res == LAKEBRIDGE_CLASSIFICATION_MANUAL_REVIEW:
            manual_review_count += 1
        else:
            unsupported_count += 1
        artifact_counted = True

        # 8. Persist control row immediately
        ctrl_row = {
            "artifact_id": art_id,
            "connection_id": conn_id,
            "source_system": src_sys,
            "source_database": src_db if src_db else None,
            "source_schema": sch,
            "object_name": oname,
            "object_type": otype,
            "target_catalog": target_cat,
            "target_schema": target_sch,
            "target_object_name": (
                cand.get("target_object_name")
                or normalize_target_identifier(oname)
            ),
            "source_definition": src_def,
            "converted_definition": raw_bladebridge_sql,
            "source_definition_hash": src_hash,
            "converted_definition_hash": conv_hash,
            "converted_artifact_path": conv_vol_path if (raw_bladebridge_sql and not persistence_failed) else None,
            "conversion_classification": cls_res,
            "conversion_status": conv_status,
            "deployment_status": "NOT_DEPLOYED",
            "manual_review_required": man_req,
            "manual_review_reason": man_reason,
            "unsupported_features": bounded_fragments_json if unk_fragments else None,
            "error_code": err_code,
            "error_message": err_msg,
            "attempt_count": attempt_num,
            "first_seen_ts": cand.get("first_seen_ts") or now_utc,
            "last_seen_ts": now_utc,
            "conversion_ts": datetime.now(timezone.utc) if (raw_bladebridge_sql and not persistence_failed) else None,
            "deployment_ts": None,
            "created_ts": cand.get("created_ts") or now_utc,
            "updated_ts": now_utc,
            "run_id": run_id,
            "is_active": False,
            "identity_version": 2,
            "legacy_artifact_id": cand.get("legacy_artifact_id"),
            "lakebridge_analyzed_ts": datetime.now(timezone.utc) if not analyzer_failed else None,
            "lakebridge_complexity": complexity,
            "lakebridge_statement_count": stmt_count,
            "lakebridge_unknown_statement_count": unk_stmt_count,
            "lakebridge_unknown_fragments": bounded_fragments_json,
            "lakebridge_uses_error_handling": constructs.get("uses_error_handling"),
            "lakebridge_uses_rowcount": constructs.get("uses_rowcount"),
            "lakebridge_uses_cursor": constructs.get("uses_cursor"),
            "lakebridge_uses_dynamic_sql": constructs.get("uses_dynamic_sql"),
            "lakebridge_uses_trigger": constructs.get("uses_trigger"),
            "lakebridge_referenced_objects": bounded_refs_json,
            "lakebridge_parsing_error_count": p_err,
            "lakebridge_validation_error_count": v_err,
            "lakebridge_generation_error_count": g_err,
            "lakebridge_fixme_count": fixme_count,
            "lakebridge_transpiled_definition": raw_bladebridge_sql,
            "object_map_applied": False,
            "lakebridge_classification": cls_res,
        }
        persist_control_row(ctrl_row)

        # 9. Append stage execution logs immediately
        log_meta = {
            "source_system": src_sys,
            "connection_id": conn_id,
            "source_database": src_db,
            "source_schema": sch,
            "object_name": oname,
            "object_type": otype,
            "converted_artifact_path": conv_vol_path if (raw_bladebridge_sql and not persistence_failed) else None,
            "complexity": complexity,
            "statement_count": stmt_count,
            "unknown_statement_count": unk_stmt_count,
            "fixme_count": fixme_count,
            "parsing_error_count": p_err,
            "validation_error_count": v_err,
            "generation_error_count": g_err,
            "diagnostic_error_count": tr_diag_errors,
            "diagnostic_warning_count": tr_diag_warnings,
            "diagnostics": sanitized_diagnostics,
        }
        if an_warning:
            log_meta["analyzer_warning"] = an_warning

        # Stage: Analyze
        persist_execution_log({
            "log_id": uuid.uuid4().hex,
            "run_id": run_id,
            "artifact_id": art_id,
            "processing_stage": STAGE_LAKEBRIDGE_ANALYZE,
            "attempt_number": attempt_num,
            "start_ts": an_start_ts,
            "end_ts": an_end_ts,
            "status": "FAILED" if analyzer_failed else "SUCCEEDED",
            "source_definition_hash": src_hash,
            "converted_definition_hash": None,
            "classification": cls_res,
            "error_code": "ANALYZER_FAILED" if analyzer_failed else None,
            "error_message": safe_an_err if analyzer_failed else (an_warning if an_warning else None),
            "execution_metadata": build_bounded_json(log_meta, max_bytes=8192),
            "created_ts": datetime.now(timezone.utc),
        })

        # Stage: Transpile
        persist_execution_log({
            "log_id": uuid.uuid4().hex,
            "run_id": run_id,
            "artifact_id": art_id,
            "processing_stage": STAGE_LAKEBRIDGE_TRANSPILE,
            "attempt_number": attempt_num,
            "start_ts": tr_start_ts,
            "end_ts": tr_end_ts,
            "status": "FAILED" if transpile_failed else "SUCCEEDED",
            "source_definition_hash": src_hash,
            "converted_definition_hash": conv_hash,
            "classification": cls_res,
            "error_code": "TRANSPILE_FAILED" if transpile_failed else None,
            "error_message": safe_tr_err if transpile_failed else None,
            "execution_metadata": build_bounded_json(log_meta, max_bytes=8192),
            "created_ts": datetime.now(timezone.utc),
        })

        # Stage: Store
        persist_execution_log({
            "log_id": uuid.uuid4().hex,
            "run_id": run_id,
            "artifact_id": art_id,
            "processing_stage": STAGE_LAKEBRIDGE_STORE,
            "attempt_number": attempt_num,
            "start_ts": store_start_ts,
            "end_ts": store_end_ts,
            "status": "FAILED" if persistence_failed else "SUCCEEDED",
            "source_definition_hash": src_hash,
            "converted_definition_hash": conv_hash,
            "classification": cls_res,
            "error_code": "PERSISTENT_STORE_FAILED" if persistence_failed else None,
            "error_message": persistence_err if persistence_failed else None,
            "execution_metadata": build_bounded_json(log_meta, max_bytes=8192),
            "created_ts": datetime.now(timezone.utc),
        })

        # Stage: Classify
        persist_execution_log({
            "log_id": uuid.uuid4().hex,
            "run_id": run_id,
            "artifact_id": art_id,
            "processing_stage": STAGE_LAKEBRIDGE_CLASSIFY,
            "attempt_number": attempt_num,
            "start_ts": an_start_ts,
            "end_ts": datetime.now(timezone.utc),
            "status": "SUCCEEDED",
            "source_definition_hash": src_hash,
            "converted_definition_hash": conv_hash,
            "classification": cls_res,
            "error_code": None,
            "error_message": None,
            "execution_metadata": build_bounded_json(log_meta, max_bytes=8192),
            "created_ts": datetime.now(timezone.utc),
        })

        # 10. Attempt folder lifecycle: delete only on complete success
        if persistence_failed or analyzer_failed or transpile_failed or (err_code is not None):
            print(f"  [diagnostics] Retaining attempt directory after failure: {attempt_dir}")
        else:
            try:
                shutil.rmtree(attempt_dir, ignore_errors=True)
            except Exception as exc_clean:
                print(f"  [warn] Cleanup warning for {attempt_dir}: {sanitize_message(exc_clean)}")

    except Exception as art_exc:
        safe_exc = sanitize_message(art_exc)
        print(f"  [error] Artifact {art_id} processing failed: {safe_exc}")
        if attempt_dir and os.path.exists(attempt_dir):
            print(f"  [diagnostics] Retaining attempt directory: {attempt_dir}")
        if not artifact_counted:
            unsupported_count += 1
            artifact_counted = True

        try:
            c_sch = cand.get("source_schema") or "default"
            c_oname = cand.get("object_name") or "unnamed"
            c_sdef = cand.get("source_definition")
            c_shash = cand.get("source_definition_hash") or (compute_definition_hash(c_sdef) if c_sdef else None)
            persist_control_row({
                "artifact_id": art_id,
                "connection_id": cand.get("connection_id") or "unknown",
                "source_system": normalize_source_system(cand.get("source_system")) if cand.get("source_system") else "unknown",
                "source_database": cand.get("source_database"),
                "source_schema": c_sch,
                "object_name": c_oname,
                "object_type": cand.get("object_type") or "VIEW",
                "target_catalog": cand.get("target_catalog") or catalog,
                "target_schema": cand.get("target_schema") or c_sch.lower(),
                "target_object_name": (
                    cand.get("target_object_name")
                    or normalize_target_identifier(c_oname)
                ),
                "source_definition": c_sdef,
                "converted_definition": None,
                "source_definition_hash": c_shash,
                "converted_definition_hash": None,
                "converted_artifact_path": None,
                "conversion_classification": LAKEBRIDGE_CLASSIFICATION_UNSUPPORTED,
                "conversion_status": "FAILED",
                "deployment_status": "NOT_DEPLOYED",
                "manual_review_required": True,
                "manual_review_reason": safe_exc,
                "unsupported_features": None,
                "error_code": "ARTIFACT_PROCESSING_FAILED",
                "error_message": safe_exc,
                "attempt_count": cand.get("attempt_count") or 1,
                "first_seen_ts": cand.get("first_seen_ts") or now_utc,
                "last_seen_ts": now_utc,
                "conversion_ts": None,
                "deployment_ts": None,
                "created_ts": cand.get("created_ts") or now_utc,
                "updated_ts": now_utc,
                "run_id": run_id,
                "is_active": False,
                "identity_version": 2,
                "legacy_artifact_id": cand.get("legacy_artifact_id"),
                "lakebridge_analyzed_ts": None,
                "lakebridge_complexity": None,
                "lakebridge_statement_count": None,
                "lakebridge_unknown_statement_count": None,
                "lakebridge_unknown_fragments": None,
                "lakebridge_uses_error_handling": None,
                "lakebridge_uses_rowcount": None,
                "lakebridge_uses_cursor": None,
                "lakebridge_uses_dynamic_sql": None,
                "lakebridge_uses_trigger": None,
                "lakebridge_referenced_objects": None,
                "lakebridge_parsing_error_count": 1,
                "lakebridge_validation_error_count": 0,
                "lakebridge_generation_error_count": 1,
                "lakebridge_fixme_count": 0,
                "lakebridge_transpiled_definition": None,
                "object_map_applied": False,
                "lakebridge_classification": LAKEBRIDGE_CLASSIFICATION_UNSUPPORTED,
            })
        except Exception as ctrl_err:
            print(f"  [error] Failed to persist control row: {sanitize_message(ctrl_err)}")

        try:
            persist_execution_log({
                "log_id": uuid.uuid4().hex,
                "run_id": run_id,
                "artifact_id": art_id,
                "processing_stage": STAGE_LAKEBRIDGE_TRANSPILE,
                "attempt_number": cand.get("attempt_count") or 1,
                "start_ts": now_utc,
                "end_ts": datetime.now(timezone.utc),
                "status": "FAILED",
                "source_definition_hash": c_shash,
                "converted_definition_hash": None,
                "classification": LAKEBRIDGE_CLASSIFICATION_UNSUPPORTED,
                "error_code": "ARTIFACT_PROCESSING_FAILED",
                "error_message": safe_exc,
                "execution_metadata": json.dumps({"source_system": cand.get("source_system"), "artifact_id": art_id}),
                "created_ts": datetime.now(timezone.utc),
            })
        except Exception as log_err:
            print(f"  [error] Failed to persist execution log: {sanitize_message(log_err)}")

# COMMAND ----------

# Determine overall status and business status
completed_count = auto_candidate_count + manual_review_count
def_missing_count = max(0, total_candidates - len(candidates_to_process))
total_failures = unsupported_count + def_missing_count

if total_candidates == 0:
    business_status = "NO_CANDIDATES"
elif len(candidates_to_process) == 0:
    business_status = "FAILED"
elif completed_count == 0:
    business_status = "FAILED"
elif total_failures > 0:
    business_status = "PARTIAL"
elif manual_review_count > 0:
    business_status = "MANUAL_REVIEW_REQUIRED"
else:
    business_status = "COMPLETE"

# A mixed-result batch completed successfully at task level.
# Individual failed artifacts remain FAILED/UNSUPPORTED.
status = "FAILED" if business_status == "FAILED" else "SUCCEEDED"

# Publish Task Values for Orchestrator before raising
set_task_value("run_id", run_id)
set_task_value("status", status)
set_task_value("business_status", business_status)
set_task_value("candidates_in_scope", total_candidates)
set_task_value("analyzed_count", analyzed_count)
set_task_value("transpiled_count", transpiled_count)
set_task_value("auto_candidate_count", auto_candidate_count)
set_task_value("manual_review_count", manual_review_count)
set_task_value("unsupported_count", unsupported_count)
set_task_value("parsing_error_count", parsing_error_count)
set_task_value("validation_error_count", validation_error_count)
set_task_value("generation_error_count", generation_error_count)
set_task_value("fixme_artifact_count", fixme_artifact_count)
set_task_value("persistent_store_failure_count", store_failure_count)

summary = {
    "status": status,
    "business_status": business_status,
    "run_id": run_id,
    "total_candidates": total_candidates,
    "analyzed_count": analyzed_count,
    "transpiled_count": transpiled_count,
    "auto_candidate_count": auto_candidate_count,
    "manual_review_count": manual_review_count,
    "unsupported_count": unsupported_count,
    "parsing_error_count": parsing_error_count,
    "validation_error_count": validation_error_count,
    "generation_error_count": generation_error_count,
    "fixme_artifact_count": fixme_artifact_count,
    "persistent_store_failure_count": store_failure_count,
}
print("Lakebridge Run Summary:", json.dumps(summary, indent=2))

if status == "FAILED":
    raise RuntimeError(f"NB24_LakebridgeAnalyzeAndTranspile completed with failures: business_status={business_status}")

dbutils.notebook.exit(json.dumps(summary))