"""
sql_artifact_control_common.py - Pure Python definitions, constants, and utilities
for the independent SQL Artifact Migration workflow.

Maintains schemas, deterministic identity, hashing, status lifecycles, and DDL
for the two dedicated Delta tables:
  1. sql_artifact_control
  2. sql_artifact_execution_log

Pure Python: NO Spark, NO dbutils, NO Delta APIs. Fully unit-testable.
"""

from __future__ import annotations

import hashlib
import json
import re
from typing import Any, Dict, List, Optional, Tuple

try:
    from src.identifiers import quote_databricks, escape_string_literal, normalize_target_identifier
    from src.failure_classifier import sanitize_message
except ModuleNotFoundError:
    from identifiers import quote_databricks, escape_string_literal, normalize_target_identifier
    from failure_classifier import sanitize_message

# Table Names
SQL_ARTIFACT_CONTROL_TABLE = "sql_artifact_control"
SQL_ARTIFACT_EXECUTION_LOG_TABLE = "sql_artifact_execution_log"

# Classification Values (finite, explicit)
CLASSIFICATION_AUTO = "AUTO"
CLASSIFICATION_MANUAL_REVIEW = "MANUAL_REVIEW"
CLASSIFICATION_UNSUPPORTED = "UNSUPPORTED"
ALLOWED_CLASSIFICATIONS = (
    CLASSIFICATION_AUTO,
    CLASSIFICATION_MANUAL_REVIEW,
    CLASSIFICATION_UNSUPPORTED,
)

# Conversion Statuses
CONVERSION_STATUS_PENDING = "PENDING"
CONVERSION_STATUS_CONVERTED = "CONVERTED"
CONVERSION_STATUS_PARTIAL = "PARTIAL"
CONVERSION_STATUS_UNSUPPORTED = "UNSUPPORTED"
CONVERSION_STATUS_FAILED = "FAILED"
CONVERSION_STATUS_SKIPPED = "SKIPPED"
ALLOWED_CONVERSION_STATUSES = (
    CONVERSION_STATUS_PENDING,
    CONVERSION_STATUS_CONVERTED,
    CONVERSION_STATUS_PARTIAL,
    CONVERSION_STATUS_UNSUPPORTED,
    CONVERSION_STATUS_FAILED,
    CONVERSION_STATUS_SKIPPED,
)

# Deployment Statuses
DEPLOYMENT_STATUS_NOT_DEPLOYED = "NOT_DEPLOYED"
DEPLOYMENT_STATUS_DEPLOYED = "DEPLOYED"
DEPLOYMENT_STATUS_FAILED = "FAILED"
DEPLOYMENT_STATUS_SKIPPED = "SKIPPED"
ALLOWED_DEPLOYMENT_STATUSES = (
    DEPLOYMENT_STATUS_NOT_DEPLOYED,
    DEPLOYMENT_STATUS_DEPLOYED,
    DEPLOYMENT_STATUS_FAILED,
    DEPLOYMENT_STATUS_SKIPPED,
)

# Supported SQL Object Types
SUPPORTED_ARTIFACT_TYPES = (
    "VIEW",
    "PROCEDURE",
    "FUNCTION",
    "PACKAGE",
    "PACKAGE_BODY",
    "TRIGGER",
)

# Error Codes
ERR_CONFIG_MISSING = "CONFIG_MISSING"
ERR_INVALID_IDENTITY = "INVALID_IDENTITY"
ERR_UNSUPPORTED_CONSTRUCT = "UNSUPPORTED_CONSTRUCT"
ERR_CONVERSION_FAILED = "CONVERSION_FAILED"
ERR_VALIDATION_FAILED = "VALIDATION_FAILED"
ERR_DEPLOYMENT_FAILED = "DEPLOYMENT_FAILED"
ERR_CONCURRENT_LOCK = "CONCURRENT_LOCK"
ERR_UNKNOWN = "UNKNOWN_ERROR"

# Lakebridge Classifications
LAKEBRIDGE_CLASSIFICATION_AUTO_CANDIDATE = "AUTO_CANDIDATE"
LAKEBRIDGE_CLASSIFICATION_MANUAL_REVIEW = "MANUAL_REVIEW"
LAKEBRIDGE_CLASSIFICATION_UNSUPPORTED = "UNSUPPORTED"
ALLOWED_LAKEBRIDGE_CLASSIFICATIONS = (
    LAKEBRIDGE_CLASSIFICATION_AUTO_CANDIDATE,
    LAKEBRIDGE_CLASSIFICATION_MANUAL_REVIEW,
    LAKEBRIDGE_CLASSIFICATION_UNSUPPORTED,
)

# Lakebridge Processing Stages
STAGE_SELECTED_SOURCE_FETCH = "SELECTED_SOURCE_FETCH"
STAGE_LAKEBRIDGE_ANALYZE = "LAKEBRIDGE_ANALYZE"
STAGE_LAKEBRIDGE_TRANSPILE = "LAKEBRIDGE_TRANSPILE"
STAGE_LAKEBRIDGE_STORE = "LAKEBRIDGE_STORE"
STAGE_LAKEBRIDGE_CLASSIFY = "LAKEBRIDGE_CLASSIFY"

# Control Table Column Definitions: (column_name, data_type, is_nullable)
SQL_ARTIFACT_CONTROL_COLUMNS: List[Tuple[str, str, bool]] = [
    ("artifact_id", "STRING", False),
    ("connection_id", "STRING", False),
    ("source_system", "STRING", False),
    ("source_database", "STRING", True),
    ("source_schema", "STRING", False),
    ("object_name", "STRING", False),
    ("object_type", "STRING", False),
    ("target_catalog", "STRING", True),
    ("target_schema", "STRING", True),
    ("target_object_name", "STRING", True),
    ("source_definition", "STRING", True),
    ("converted_definition", "STRING", True),
    ("source_definition_hash", "STRING", False),
    ("converted_definition_hash", "STRING", True),
    ("conversion_classification", "STRING", False),
    ("conversion_status", "STRING", False),
    ("deployment_status", "STRING", False),
    ("manual_review_required", "BOOLEAN", False),
    ("manual_review_reason", "STRING", True),
    ("unsupported_features", "STRING", True),
    ("error_code", "STRING", True),
    ("error_message", "STRING", True),
    ("attempt_count", "INT", False),
    ("first_seen_ts", "TIMESTAMP", True),
    ("last_seen_ts", "TIMESTAMP", True),
    ("conversion_ts", "TIMESTAMP", True),
    ("deployment_ts", "TIMESTAMP", True),
    ("created_ts", "TIMESTAMP", False),
    ("updated_ts", "TIMESTAMP", False),
    ("run_id", "STRING", True),
    ("is_active", "BOOLEAN", False),
    ("identity_version", "INT", True),
    ("legacy_artifact_id", "STRING", True),
    ("lakebridge_analyzed_ts", "TIMESTAMP", True),
    ("lakebridge_complexity", "STRING", True),
    ("lakebridge_statement_count", "INT", True),
    ("lakebridge_unknown_statement_count", "INT", True),
    ("lakebridge_unknown_fragments", "STRING", True),
    ("lakebridge_uses_error_handling", "BOOLEAN", True),
    ("lakebridge_uses_rowcount", "BOOLEAN", True),
    ("lakebridge_uses_cursor", "BOOLEAN", True),
    ("lakebridge_uses_dynamic_sql", "BOOLEAN", True),
    ("lakebridge_uses_trigger", "BOOLEAN", True),
    ("lakebridge_referenced_objects", "STRING", True),
    ("lakebridge_parsing_error_count", "INT", True),
    ("lakebridge_validation_error_count", "INT", True),
    ("lakebridge_generation_error_count", "INT", True),
    ("lakebridge_fixme_count", "INT", True),
    ("lakebridge_transpiled_definition", "STRING", True),
    ("object_map_applied", "BOOLEAN", True),
    ("lakebridge_classification", "STRING", True),
]

# Execution Log Column Definitions: (column_name, data_type, is_nullable)
SQL_ARTIFACT_EXECUTION_LOG_COLUMNS: List[Tuple[str, str, bool]] = [
    ("log_id", "STRING", False),
    ("run_id", "STRING", False),
    ("artifact_id", "STRING", False),
    ("processing_stage", "STRING", False),
    ("attempt_number", "INT", False),
    ("start_ts", "TIMESTAMP", True),
    ("end_ts", "TIMESTAMP", True),
    ("status", "STRING", False),
    ("source_definition_hash", "STRING", True),
    ("converted_definition_hash", "STRING", True),
    ("classification", "STRING", True),
    ("error_code", "STRING", True),
    ("error_message", "STRING", True),
    ("execution_metadata", "STRING", True),
    ("created_ts", "TIMESTAMP", False),
]


def normalize_artifact_type(object_type: Any) -> str:
    """Validate, trim, uppercase, and canonicalize object_type token."""
    if object_type is None:
        raise ValueError("object_type cannot be blank")
    s = str(object_type).strip().upper().replace(" ", "_")
    if not s:
        raise ValueError("object_type cannot be blank")
    if s not in SUPPORTED_ARTIFACT_TYPES:
        raise ValueError(f"unsupported artifact object_type: {object_type!r}")
    return s


def make_canonical_owner(
    connection_id: Any,
    source_system: Any = None,
    source_database: Any = None,
    source_schema: Any = None,
    object_type: Any = None,
    object_name: Any = None,
) -> Tuple[str, str, str, str, str, str]:
    """Define canonical source artifact ownership using exact identity."""
    return (
        str(connection_id or "").strip(),
        str(source_system or "").strip().lower(),
        str(source_database or "").strip(),
        str(source_schema or "").strip(),
        normalize_artifact_type(object_type),
        str(object_name or "").strip(),
    )


def compute_artifact_id(
    connection_id: Any,
    source_system: Any = None,
    source_database: Any = None,
    source_schema: Any = None,
    object_type: Any = None,
    object_name: Any = None,
    **kwargs: Any,
) -> str:
    """Derive deterministic v2 SHA-256 artifact ID.

    Identity v2 = SHA-256 over "v2|" + "|".join of these exact values,
    each only .strip()'d, NEVER case-folded:
    connection_id, source_system (lowercased canonical system name only),
    source_database (Oracle: empty string), source_schema, object_type
    (uppercased canonical), object_name.

    compute_artifact_id() requires non-blank source_system.
    """
    if "source_system" in kwargs and source_system is None:
        source_system = kwargs["source_system"]

    if connection_id is None or not str(connection_id).strip():
        raise ValueError("connection_id is required to compute artifact_id")
    if source_system is None or not str(source_system).strip():
        raise ValueError("source_system is required to compute artifact_id")
    if source_schema is None or not str(source_schema).strip():
        raise ValueError("source_schema is required to compute artifact_id")
    if object_name is None or not str(object_name).strip():
        raise ValueError("object_name is required to compute artifact_id")

    conn = str(connection_id).strip()
    sys_name = str(source_system).strip().lower()
    db = str(source_database or "").strip()
    if sys_name == "oracle":
        db = ""
    sch = str(source_schema).strip()
    otype = normalize_artifact_type(object_type)
    obj = str(object_name).strip()

    payload = "v2|" + "|".join([conn, sys_name, db, sch, otype, obj])
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def resolve_target_catalog_and_schema(
    target_config: Dict[str, Any],
    source_database: Optional[str],
    source_schema: str,
) -> Tuple[str, str]:
    """Derive target catalog and target schema using normalize_target_identifier.

    Shared between NB18 and NB22 to guarantee identical routing.
    """
    target_cat = target_config.get("target_catalog")
    if not target_cat or not str(target_cat).strip():
        raise ValueError("Missing target_catalog in target configuration")
    target_cat = str(target_cat).strip()

    target_mode = str(target_config.get("target_schema_mode") or "").upper().strip()
    explicit_sch = target_config.get("target_schema")

    if target_mode == "EXPLICIT":
        if not explicit_sch or not str(explicit_sch).strip():
            raise ValueError("EXPLICIT target_schema_mode requires target_schema")
        target_sch = normalize_target_identifier(str(explicit_sch).strip(), identifier_type="schema")
    elif target_mode == "PREFIX_WITH_DATABASE":
        norm_db = normalize_target_identifier(source_database or "db", identifier_type="schema")
        norm_s = normalize_target_identifier(source_schema, identifier_type="schema")
        target_sch = f"{norm_db}_{norm_s}"
    else:
        target_sch = normalize_target_identifier(source_schema, identifier_type="schema")

    return target_cat, target_sch


def plan_identity_migration(
    rows: List[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Pure decision logic for v1 -> v2 artifact identity migration.

    For each control row where identity_version IS NULL or = 1:
      recomputes v2 id from the row's stored columns.
      - If no other row already has that v2 id:
        UPDATE artifact_id = v2 id, legacy_artifact_id = old id, identity_version = 2.
      - If a row with that v2 id already exists:
        do NOT update. Collect it.
      - If an old artifact_id appears more than once, it is a conflict and NB21 fails closed.
    If any conflicts were collected, returns ([], conflicts) to fail closed.
    Returns (updates, conflicts).
    """
    # Check for duplicate old artifact_ids across rows
    old_id_counts: Dict[str, int] = {}
    for r in rows:
        oid = str(r.get("artifact_id") or "").strip()
        if oid:
            old_id_counts[oid] = old_id_counts.get(oid, 0) + 1

    conflicts: List[Dict[str, Any]] = []
    duplicate_old_ids = {oid for oid, cnt in old_id_counts.items() if cnt > 1}
    for r in rows:
        oid = str(r.get("artifact_id") or "").strip()
        if oid in duplicate_old_ids:
            conflicts.append({
                "old_artifact_id": oid,
                "conflicting_v2_id": "DUPLICATE_OLD_ARTIFACT_ID",
                "reason": f"Old artifact_id {oid!r} appears {old_id_counts[oid]} times in control rows",
                "row": dict(r),
            })

    if conflicts:
        return ([], conflicts)

    existing_v2_ids = set()
    rows_to_migrate = []

    for r in rows:
        d = dict(r)
        ver = d.get("identity_version")
        if ver is not None and str(ver).strip() in ("2", 2):
            existing_v2_ids.add(str(d.get("artifact_id") or "").strip())
        else:
            rows_to_migrate.append(d)

    updates: List[Dict[str, Any]] = []
    planned_v2_ids = set()

    for d in rows_to_migrate:
        old_id = str(d.get("artifact_id") or "").strip()
        v2_id = compute_artifact_id(
            connection_id=d.get("connection_id"),
            source_system=d.get("source_system"),
            source_database=d.get("source_database"),
            source_schema=d.get("source_schema"),
            object_type=d.get("object_type"),
            object_name=d.get("object_name"),
        )
        if v2_id in existing_v2_ids or v2_id in planned_v2_ids:
            conflicts.append({
                "old_artifact_id": old_id,
                "conflicting_v2_id": v2_id,
                "row": d,
            })
        else:
            planned_v2_ids.add(v2_id)
            updates.append({
                "old_artifact_id": old_id,
                "artifact_id": v2_id,
                "legacy_artifact_id": old_id,
                "identity_version": 2,
            })

    if conflicts:
        return ([], conflicts)

    return (updates, conflicts)


def build_target_ownership(control_rows: List[Dict[str, Any]]) -> Dict[str, set]:
    """Build map of lowercased target_fqn -> set of artifact_ids that own it."""
    ownership: Dict[str, set] = {}
    for r in control_rows:
        cat = str(r.get("target_catalog") or "").strip()
        sch = str(r.get("target_schema") or "").strip()
        obj = str(r.get("target_object_name") or "").strip()
        art_id = str(r.get("artifact_id") or "").strip()
        if cat and sch and obj and art_id:
            fqn = f"{cat}.{sch}.{obj}".lower()
            ownership.setdefault(fqn, set()).add(art_id)
    return ownership


def check_target_ownership(
    ownership: Dict[str, set],
    target_fqn: str,
    artifact_id: str,
) -> str:
    """Check target ownership status.

    Returns:
      - 'AMBIGUOUS': target FQN is owned by multiple distinct artifact IDs.
      - 'FREE': target FQN is not owned by any artifact.
      - 'OWNED': target FQN is owned exclusively by this artifact_id.
      - 'CONFLICT': target FQN is owned by another artifact_id.
    """
    fqn = str(target_fqn or "").strip().lower()
    owners = ownership.get(fqn, set())
    if len(owners) > 1:
        return "AMBIGUOUS"
    if not owners:
        return "FREE"
    if artifact_id in owners:
        return "OWNED"
    return "CONFLICT"


def decide_target_deployment_action(
    ownership: Dict[str, set],
    target_fqn: str,
    artifact_id: str,
) -> Tuple[bool, str, Optional[str]]:
    """Decide whether NB22 should deploy or block on TARGET_OWNERSHIP_CONFLICT.

    Returns:
      (can_deploy, ownership_status, error_message)
      can_deploy is True ONLY if status in ('OWNED', 'FREE').
      If status is 'CONFLICT' or 'AMBIGUOUS', can_deploy is False.
    """
    status = check_target_ownership(ownership, target_fqn, artifact_id)
    if status in ("OWNED", "FREE"):
        return True, status, None
    err_msg = f"TARGET_OWNERSHIP_CONFLICT: Target object {target_fqn} ownership check returned {status}"
    return False, status, err_msg


def is_connection_usable(row: Any) -> Tuple[bool, str]:
    """Verify whether a connection record is usable for artifact extraction/migration.

    Usable ONLY when:
      - is_active is True or string equal to 'true' after strip/lower
      - connection_status.strip().upper() == 'VALID'
      - secret_scope is a non-blank string
    Everything else returns (False, reason).
    """
    if row is None:
        return (False, "Connection record is None")
    d = row.asDict() if hasattr(row, "asDict") else (dict(row) if isinstance(row, dict) else getattr(row, "__dict__", {}))

    is_act = d.get("is_active")
    if is_act is True:
        pass
    elif isinstance(is_act, str) and is_act.strip().lower() == "true":
        pass
    else:
        return (False, f"Connection is inactive: is_active={is_act!r}")

    status = str(d.get("connection_status") or "").strip().upper()
    if status != "VALID":
        return (False, f"Connection status is not VALID: {status!r}")

    scope = d.get("secret_scope")
    if scope is None or not str(scope).strip():
        return (False, "Connection is missing required secret_scope")

    return (True, "")


def validate_registered_source_connection(
    conn_record: Any,
    candidate_source_system: str,
    candidate_source_database: Optional[str] = None,
) -> Tuple[bool, str, Dict[str, Any]]:
    """Validate registered connection ownership and endpoint compatibility."""
    if conn_record is None:
        return False, "SOURCE_IDENTITY_MISMATCH: Registered connection does not exist", {}

    usable, unusable_reason = is_connection_usable(conn_record)
    if not usable:
        return False, f"SOURCE_IDENTITY_MISMATCH: Connection is unusable: {unusable_reason}", {}

    d = conn_record.asDict() if hasattr(conn_record, "asDict") else (
        dict(conn_record) if isinstance(conn_record, dict) else getattr(conn_record, "__dict__", {})
    )

    c_status = str(d.get("connection_status") or "").strip().upper()
    if c_status != "VALID":
        return False, f"SOURCE_IDENTITY_MISMATCH: Connection status is not VALID: {c_status}", {}

    scope = d.get("secret_scope")
    if scope is None or not str(scope).strip():
        return False, "SOURCE_IDENTITY_MISMATCH: Connection is missing required secret_scope", {}

    try:
        from src.source_identity import normalize_source_system
    except ModuleNotFoundError:
        from source_identity import normalize_source_system

    try:
        reg_sys_norm = normalize_source_system(d.get("source_system"))
        cand_sys_norm = normalize_source_system(candidate_source_system)
    except Exception as exc_norm:
        return False, f"SOURCE_IDENTITY_MISMATCH: Invalid source system: {sanitize_message(exc_norm)}", {}

    if reg_sys_norm != cand_sys_norm:
        return False, (
            f"SOURCE_IDENTITY_MISMATCH: Candidate source_system '{candidate_source_system}' "
            f"(normalized '{cand_sys_norm}') does not match registered connection source_system "
            f"'{d.get('source_system')}' (normalized '{reg_sys_norm}')"
        ), {}

    reg_server = str(d.get("source_server") or "").strip()
    if not reg_server:
        return False, "SOURCE_IDENTITY_MISMATCH: Registered connection has empty source_server", {}

    reg_db = str(d.get("source_database") or "").strip()

    if cand_sys_norm == "sqlserver":
        cand_db_str = str(candidate_source_database or "").strip()
        if reg_db and cand_db_str and reg_db.lower() != cand_db_str.lower():
            return False, (
                f"SOURCE_IDENTITY_MISMATCH: Selected database '{candidate_source_database}' does not match "
                f"registered database '{reg_db}' for connection '{d.get('connection_id')}'"
            ), {}
        effective_db = cand_db_str or reg_db
        if not effective_db:
            return False, "SOURCE_IDENTITY_MISMATCH: SQL Server connection requires non-blank source_database", {}
    elif cand_sys_norm == "oracle":
        effective_db = reg_db
    else:
        effective_db = candidate_source_database or reg_db

    meta = {
        "connection_id": d.get("connection_id"),
        "source_system": cand_sys_norm,
        "source_server": reg_server,
        "source_database": effective_db,
        "secret_scope": str(scope).strip(),
        "trust_server_certificate": d.get("trust_server_certificate"),
    }
    return True, "", meta


def prepare_artifact_rerun_control_row(
    existing_row: Optional[Dict[str, Any]],
    current_run_id: str,
    connection_id: str,
    source_system: str,
    source_database: Optional[str],
    source_schema: str,
    object_name: str,
    object_type: str,
    source_definition: Optional[str],
    source_definition_hash: Optional[str],
    now_ts: Any,
    target_catalog: Optional[str] = None,
    target_schema: Optional[str] = None,
    target_object_name: Optional[str] = None,
    fetch_error_code: Optional[str] = None,
    fetch_error_message: Optional[str] = None,
) -> Dict[str, Any]:
    """Prepare a fresh control row for an artifact, resetting all 22 stale fields on rerun."""
    art_id = compute_artifact_id(
        connection_id=connection_id,
        source_system=source_system,
        source_database=source_database,
        source_schema=source_schema,
        object_type=object_type,
        object_name=object_name,
    )

    is_rerun = existing_row is not None
    if is_rerun:
        attempt_count = (existing_row.get("attempt_count") or 0) + 1
        first_seen_ts = existing_row.get("first_seen_ts") or now_ts
        created_ts = existing_row.get("created_ts") or now_ts
    else:
        attempt_count = 1
        first_seen_ts = now_ts
        created_ts = now_ts

    if fetch_error_code:
        conversion_status = CONVERSION_STATUS_FAILED
        conversion_classification = CLASSIFICATION_UNSUPPORTED
        man_req = True
        man_reason = fetch_error_message
        err_code = fetch_error_code
        err_msg = fetch_error_message
    else:
        conversion_status = CONVERSION_STATUS_PENDING
        conversion_classification = None
        man_req = False
        man_reason = None
        err_code = None
        err_msg = None

    row = {
        "artifact_id": art_id,
        "connection_id": connection_id,
        "source_system": source_system,
        "source_database": source_database,
        "source_schema": source_schema,
        "object_name": object_name,
        "object_type": object_type,
        "target_catalog": target_catalog,
        "target_schema": target_schema,
        "target_object_name": target_object_name or object_name.lower(),
        "source_definition": source_definition,
        "converted_definition": None,
        "source_definition_hash": source_definition_hash,
        "converted_definition_hash": None,
        "conversion_classification": conversion_classification,
        "conversion_status": conversion_status,
        "deployment_status": DEPLOYMENT_STATUS_NOT_DEPLOYED,
        "manual_review_required": man_req,
        "manual_review_reason": man_reason,
        "unsupported_features": None,
        "error_code": err_code,
        "error_message": err_msg,
        "attempt_count": attempt_count,
        "first_seen_ts": first_seen_ts,
        "last_seen_ts": now_ts,
        "conversion_ts": None,
        "deployment_ts": None,
        "created_ts": created_ts,
        "updated_ts": now_ts,
        "run_id": current_run_id,
        "is_active": False,
        "identity_version": 2,
        "legacy_artifact_id": existing_row.get("legacy_artifact_id") if existing_row else None,
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
        "lakebridge_parsing_error_count": None,
        "lakebridge_validation_error_count": None,
        "lakebridge_generation_error_count": None,
        "lakebridge_fixme_count": None,
        "lakebridge_transpiled_definition": None,
        "object_map_applied": False,
        "lakebridge_classification": None,
    }
    return row



def _normalize_data_type(t: str) -> str:
    token = str(t or "").strip().upper()
    if token in ("INT", "INTEGER"):
        return "INT"
    if token in ("BIGINT", "LONG"):
        return "BIGINT"
    if token in ("STRING", "VARCHAR", "CHAR", "TEXT"):
        return "STRING"
    if token in ("BOOLEAN", "BOOL"):
        return "BOOLEAN"
    if token in ("TIMESTAMP", "TIMESTAMP_NTZ"):
        return "TIMESTAMP"
    if token in ("FLOAT", "DOUBLE"):
        return token
    return token


def verify_schema(
    expected_cols: List[Any],
    actual_fields: List[Any],
) -> List[str]:
    """Verify presence and normalized data type of columns against catalog schema.

    Returns a list of problem strings (empty list if valid).
    """
    problems: List[str] = []
    actual_map: Dict[str, str] = {}
    for f in actual_fields:
        if isinstance(f, dict):
            fname = str(f.get("name") or "").strip().lower()
            ftype = str(f.get("type") or "").strip()
        elif hasattr(f, "name") and hasattr(f, "dataType"):
            fname = str(f.name).strip().lower()
            ftype = str(f.dataType.simpleString() if hasattr(f.dataType, "simpleString") else f.dataType).strip()
        else:
            fname = str(getattr(f, "name", "")).strip().lower()
            ftype = str(getattr(f, "type", "")).strip()
        if fname:
            actual_map[fname] = _normalize_data_type(ftype)

    for item in expected_cols:
        if isinstance(item, (tuple, list)):
            col_name = str(item[0]).strip()
            exp_type = _normalize_data_type(str(item[1]).strip())
        else:
            col_name = str(item).strip()
            exp_type = ""

        c_lower = col_name.lower()
        if c_lower not in actual_map:
            problems.append(f"Missing column {col_name}")
        elif exp_type and actual_map[c_lower] != exp_type:
            problems.append(f"Type mismatch on {col_name}: expected {exp_type}, got {actual_map[c_lower]}")

    return problems


def decide_summary(
    ctrl_metrics: Dict[str, Any],
    failed_log_stages: int = 0,
    t05_status: Optional[str] = None,
    t24_status: Optional[str] = None,
    t24_business_status: Optional[str] = None,
) -> Tuple[str, str]:
    """Determine (status, business_status) for the artifact workflow summary."""
    # Check t05_status for legacy calls
    if t05_status is not None and str(t05_status).strip().upper() not in ("", "SUCCEEDED"):
        return ("FAILED", "FAILED")

    selected_cand = ctrl_metrics.get("selected_candidate_count")
    is_lakebridge_mode = (
        selected_cand is not None
        or t24_status is not None
        or t24_business_status is not None
    )

    if is_lakebridge_mode:
        t24_stat_norm = str(t24_status or "").strip().upper()
        # Fail closed when t24_status is blank, missing, or not SUCCEEDED
        if not t24_status or t24_stat_norm != "SUCCEEDED":
            return ("FAILED", "FAILED")

        sel_count = int(ctrl_metrics.get("selected_candidate_count") or 0)
        fetch_count = int(ctrl_metrics.get("fetched_definition_count") or 0)
        an_count = int(ctrl_metrics.get("analyzed_count") or 0)
        tr_count = int(ctrl_metrics.get("transpiled_count") or 0)
        auto_count = int(ctrl_metrics.get("auto_candidate_count") or 0)
        man_count = int(ctrl_metrics.get("manual_review_count") or 0)
        unsupp_count = int(ctrl_metrics.get("unsupported_count") or 0)
        failed_count = (
            int(ctrl_metrics.get("failed_count") or 0)
            + int(ctrl_metrics.get("definition_missing_count") or 0)
            + int(ctrl_metrics.get("source_fetch_failure_count") or 0)
            + int(ctrl_metrics.get("persistent_store_failure_count") or 0)
        )
        completed_count = auto_count + man_count

        # Fail closed conditions:
        # selected > 0 and fetched == 0 without equivalent terminal failure count
        if sel_count > 0 and fetch_count == 0 and failed_count < sel_count:
            return ("FAILED", "FAILED")
        # fetched > 0 and analyzed == 0
        if fetch_count > 0 and an_count == 0:
            return ("FAILED", "FAILED")
        # fetched > 0 and transpiled == 0
        if fetch_count > 0 and tr_count == 0:
            return ("FAILED", "FAILED")
        # incomplete without terminal outcome
        if (completed_count + failed_count) < sel_count:
            return ("FAILED", "FAILED" if completed_count == 0 else "PARTIAL")

        # Required business-status outcomes:
        # NO_CANDIDATES:
        if sel_count == 0 and failed_count == 0 and (failed_log_stages or 0) == 0:
            return ("SUCCEEDED", "NO_CANDIDATES")

        # PARTIAL: at least one completed, at least one failed (or failed log stage)
        if completed_count > 0 and (failed_count > 0 or (failed_log_stages or 0) > 0):
            return ("FAILED", "PARTIAL")

        # FAILED: failures occurred and zero completed successfully
        if (failed_count > 0 or (failed_log_stages or 0) > 0) and completed_count == 0:
            return ("FAILED", "FAILED")

        # If failed_log_stages > 0, overall status is ALWAYS FAILED
        if (failed_log_stages or 0) > 0:
            return ("FAILED", "PARTIAL" if completed_count > 0 else "FAILED")

        # MANUAL_REVIEW_REQUIRED: all completed, no stage failed, at least one is manual review or unsupported
        if man_count > 0 or unsupp_count > 0:
            return ("SUCCEEDED", "MANUAL_REVIEW_REQUIRED")

        # COMPLETE: every selected candidate completed, no failure
        if completed_count == sel_count and failed_count == 0:
            return ("SUCCEEDED", "COMPLETE")

        return ("FAILED", "PARTIAL" if completed_count > 0 else "FAILED")

    # Legacy workflow mode
    total_artifacts = ctrl_metrics.get("total_artifacts")
    if total_artifacts is None:
        total_artifacts = ctrl_metrics.get("total_candidates", 0)

    total_ctrl_failed = ctrl_metrics.get("failed")
    if total_ctrl_failed is None:
        total_ctrl_failed = ctrl_metrics.get("failed_count", 0)

    total_deployed = ctrl_metrics.get("deployed")
    if total_deployed is None:
        total_deployed = ctrl_metrics.get("deployed_count", 0)

    total_skipped = ctrl_metrics.get("skipped")
    if total_skipped is None:
        total_skipped = ctrl_metrics.get("explicitly_skipped_count", 0)

    total_manual_review = ctrl_metrics.get("manual_review")
    if total_manual_review is None:
        total_manual_review = ctrl_metrics.get("manual_review_count", 0)

    total_unsupported = ctrl_metrics.get("unsupported")
    if total_unsupported is None:
        total_unsupported = ctrl_metrics.get("unsupported_count", 0)

    def_missing = ctrl_metrics.get("definition_missing") or ctrl_metrics.get("definition_missing_count") or 0

    if total_artifacts == 0 and (failed_log_stages or 0) == 0:
        business_status = "NO_OBJECTS"
    elif def_missing > 0:
        business_status = "PARTIAL"
    elif (total_ctrl_failed or 0) > 0 or (failed_log_stages or 0) > 0:
        has_progress = (
            (total_deployed or 0) > 0
            or (total_skipped or 0) > 0
            or (ctrl_metrics.get("auto_classified", 0) > 0)
            or (total_manual_review or 0) > 0
            or (total_unsupported or 0) > 0
        )
        business_status = "PARTIAL" if has_progress else "FAILED"
    elif (total_manual_review or 0) > 0:
        business_status = "MANUAL_REVIEW_REQUIRED"
    elif (total_unsupported or 0) > 0:
        business_status = "UNSUPPORTED_PRESENT"
    elif (total_skipped or 0) > 0 and (total_deployed or 0) == 0:
        business_status = "PARTIAL"
    else:
        business_status = "COMPLETE"

    status = "FAILED" if ((total_ctrl_failed or 0) > 0 or (failed_log_stages or 0) > 0) else "SUCCEEDED"
    return (status, business_status)


def compute_definition_hash(definition: Any) -> str:
    """Calculate deterministic SHA-256 over exact canonical definition text."""
    if definition is None:
        raise ValueError("definition cannot be None")
    s = str(definition)
    if not s.strip():
        raise ValueError("definition cannot be empty or blank")
    return hashlib.sha256(s.encode("utf-8")).hexdigest().lower()


def sanitize_error(error: Any, max_len: int = 500) -> str:
    """Sanitize error message to prevent secrets, tokens, or raw paths from leaking."""
    if error is None:
        return ""
    safe = sanitize_message(str(error))
    safe = re.sub(r"(?i)password\s*=\s*[^;\s]+", "password=***", safe)
    safe = re.sub(r"(?i)token\s*=\s*[^;\s]+", "token=***", safe)
    safe = re.sub(r"(?i)secret\s*=\s*[^;\s]+", "secret=***", safe)
    return safe[:max_len]


def build_create_artifact_control_ddl(catalog: str, control_schema: str) -> str:
    """Generate DDL for the SQL artifact control table with NOT NULL constraints."""
    col_lines = []
    for col_name, col_type, is_nullable in SQL_ARTIFACT_CONTROL_COLUMNS:
        null_clause = "" if is_nullable else " NOT NULL"
        col_lines.append(f"  {quote_databricks(col_name)} {col_type}{null_clause}")
    body = ",\n".join(col_lines)
    fqn = f"{quote_databricks(catalog)}.{quote_databricks(control_schema)}.{quote_databricks(SQL_ARTIFACT_CONTROL_TABLE)}"
    return f"CREATE TABLE IF NOT EXISTS {fqn} (\n{body}\n) USING DELTA"


def build_create_artifact_execution_log_ddl(catalog: str, control_schema: str) -> str:
    """Generate DDL for the append-only SQL artifact execution log table with NOT NULL constraints."""
    col_lines = []
    for col_name, col_type, is_nullable in SQL_ARTIFACT_EXECUTION_LOG_COLUMNS:
        null_clause = "" if is_nullable else " NOT NULL"
        col_lines.append(f"  {quote_databricks(col_name)} {col_type}{null_clause}")
    body = ",\n".join(col_lines)
    fqn = f"{quote_databricks(catalog)}.{quote_databricks(control_schema)}.{quote_databricks(SQL_ARTIFACT_EXECUTION_LOG_TABLE)}"
    return f"CREATE TABLE IF NOT EXISTS {fqn} (\n{body}\n) USING DELTA"


def build_upgrade_artifact_control_ddl(catalog: str, control_schema: str, existing_columns: List[str]) -> List[str]:
    """Generate ALTER TABLE ADD COLUMNS DDL for any missing columns in sql_artifact_control."""
    existing_lower = {c.strip().lower() for c in existing_columns or []}
    missing_cols = []
    for col_name, col_type, is_nullable in SQL_ARTIFACT_CONTROL_COLUMNS:
        if col_name.lower() not in existing_lower:
            missing_cols.append(f"{quote_databricks(col_name)} {col_type}")
    if not missing_cols:
        return []
    fqn = f"{quote_databricks(catalog)}.{quote_databricks(control_schema)}.{quote_databricks(SQL_ARTIFACT_CONTROL_TABLE)}"
    return [f"ALTER TABLE {fqn} ADD COLUMNS ({', '.join(missing_cols)})"]


def build_upgrade_artifact_execution_log_ddl(catalog: str, control_schema: str, existing_columns: List[str]) -> List[str]:
    """Generate ALTER TABLE ADD COLUMNS DDL for any missing columns in sql_artifact_execution_log."""
    existing_lower = {c.strip().lower() for c in existing_columns or []}
    missing_cols = []
    for col_name, col_type, is_nullable in SQL_ARTIFACT_EXECUTION_LOG_COLUMNS:
        if col_name.lower() not in existing_lower:
            missing_cols.append(f"{quote_databricks(col_name)} {col_type}")
    if not missing_cols:
        return []
    fqn = f"{quote_databricks(catalog)}.{quote_databricks(control_schema)}.{quote_databricks(SQL_ARTIFACT_EXECUTION_LOG_TABLE)}"
    return [f"ALTER TABLE {fqn} ADD COLUMNS ({', '.join(missing_cols)})"]


def build_upgrade_sql_object_assessment_ddl(catalog: str, control_schema: str, existing_columns: List[str]) -> List[str]:
    """Generate additive ALTER TABLE ADD COLUMNS for sql_object_assessment."""
    existing_lower = {c.strip().lower() for c in existing_columns or []}
    missing_cols = []
    expected_cols = [
        ("source_database", "STRING"),
        ("error_message", "STRING"),
    ]
    for col_name, col_type in expected_cols:
        if col_name.lower() not in existing_lower:
            missing_cols.append(f"{quote_databricks(col_name)} {col_type}")
    if not missing_cols:
        return []
    fqn = f"{quote_databricks(catalog)}.{quote_databricks(control_schema)}.{quote_databricks('sql_object_assessment')}"
    return [f"ALTER TABLE {fqn} ADD COLUMNS ({', '.join(missing_cols)})"]


def build_upgrade_sql_object_artifact_manifest_ddl(catalog: str, control_schema: str, existing_columns: List[str]) -> List[str]:
    """Generate additive ALTER TABLE ADD COLUMNS for sql_object_artifact_manifest."""
    existing_lower = {c.strip().lower() for c in existing_columns or []}
    missing_cols = []
    expected_cols = [
        ("source_database", "STRING"),
        ("error_message", "STRING"),
    ]
    for col_name, col_type in expected_cols:
        if col_name.lower() not in existing_lower:
            missing_cols.append(f"{quote_databricks(col_name)} {col_type}")
    if not missing_cols:
        return []
    fqn = f"{quote_databricks(catalog)}.{quote_databricks(control_schema)}.{quote_databricks('sql_object_artifact_manifest')}"
    return [f"ALTER TABLE {fqn} ADD COLUMNS ({', '.join(missing_cols)})"]
