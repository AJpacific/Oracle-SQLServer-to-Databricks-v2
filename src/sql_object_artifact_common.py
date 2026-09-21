"""
sql_object_artifact_common.py - Pure Python helpers for SQL object artifact materialization.

Provides supported object types, path sanitization, hashing, relative & volume path
construction, and manifest ownership key helpers.

Pure Python: NO Spark, NO dbutils, NO Delta APIs, NO JDBC code, NO runtime modules.
"""

from __future__ import annotations

import hashlib
import re
from typing import Any, Dict, List, Optional, Tuple

SUPPORTED_OBJECT_TYPES = (
    "VIEW",
    "PROCEDURE",
    "FUNCTION",
    "PACKAGE",
    "PACKAGE_BODY",
)

OBJECT_TYPE_DIRECTORIES = {
    "VIEW": "views",
    "PROCEDURE": "procedures",
    "FUNCTION": "functions",
    "PACKAGE": "packages",
    "PACKAGE_BODY": "package_bodies",
}

DEFAULT_VOLUME_NAME = "_source_artifacts"


def normalize_object_type(object_type: Any) -> str:
    """Validate, trim, uppercase, and canonicalize object_type."""
    if object_type is None:
        raise ValueError("object_type cannot be blank")
    s = str(object_type).strip().upper().replace(" ", "_")
    if not s:
        raise ValueError("object_type cannot be blank")
    if s not in SUPPORTED_OBJECT_TYPES:
        raise ValueError(f"unsupported object_type: {object_type!r}")
    return s


def sanitize_path_component(value: Any, field_name: str) -> str:
    """Sanitize a path component deterministically and prevent path traversal."""
    if value is None:
        raise ValueError(f"{field_name} cannot be blank")
    val = str(value).strip()
    if not val:
        raise ValueError(f"{field_name} cannot be blank")

    if (val in (".", "..")
            or "/.." in val or "\\.." in val
            or "../" in val or "..\\" in val
            or val.startswith("..")):
        raise ValueError(f"path traversal in {field_name}")

    has_leading = val.startswith("_")
    has_trailing = val.endswith("_")

    s = val.lower()
    s = re.sub(r'[^a-z0-9_\-]+', '_', s)
    s = re.sub(r'_+', '_', s)
    if not has_leading:
        s = s.lstrip('_')
    if not has_trailing:
        s = s.rstrip('_')

    if not s or s in (".", ".."):
        raise ValueError(f"invalid sanitized path component for {field_name}")

    return s


def definition_sha256(source_definition: Any) -> str:
    """Calculate SHA-256 over exact UTF-8 encoded definition (without stripping/altering)."""
    if source_definition is None:
        raise ValueError("source_definition cannot be blank")
    s = str(source_definition)
    if not s.strip():
        raise ValueError("source_definition cannot be blank")
    return hashlib.sha256(s.encode("utf-8")).hexdigest().lower()


def build_artifact_relative_path(*args: Any, **kwargs: Any) -> str:
    """Build deterministic connection-owned relative path.

    Supports signatures:
      (connection_id, object_type, object_name, source_schema=None)
      (connection_id, source_schema, object_type, object_name)
      or keyword arguments.
    """
    conn_id = kwargs.get("connection_id")
    schema = kwargs.get("source_schema")
    otype = kwargs.get("object_type")
    oname = kwargs.get("object_name")

    pargs = list(args)
    if pargs:
        conn_id = conn_id or pargs.pop(0)

    if pargs:
        candidate_type = (pargs[0] or "").strip().upper().replace(" ", "_")
        if candidate_type in SUPPORTED_OBJECT_TYPES:
            otype = otype or pargs.pop(0)
            oname = oname or (pargs.pop(0) if pargs else None)
            if pargs:
                schema = schema or pargs.pop(0)
        else:
            schema = schema or pargs.pop(0)
            if pargs:
                otype = otype or pargs.pop(0)
            if pargs:
                oname = oname or pargs.pop(0)

    safe_conn = sanitize_path_component(conn_id, "connection_id")
    norm_type = normalize_object_type(otype)
    type_dir = OBJECT_TYPE_DIRECTORIES[norm_type]
    safe_obj = sanitize_path_component(oname, "object_name")

    if schema and str(schema).strip():
        safe_schema = sanitize_path_component(schema, "source_schema")
        return f"{safe_conn}/{safe_schema}/{type_dir}/{safe_obj}.sql"

    return f"{safe_conn}/{type_dir}/{safe_obj}.sql"


def build_artifact_volume_path(
    target_catalog: Any,
    target_schema: Any,
    volume_name: Any,
    relative_path: Any
) -> str:
    """Build full /Volumes path and validate no path traversal."""
    safe_cat = sanitize_path_component(target_catalog, "target_catalog")
    safe_sch = sanitize_path_component(target_schema, "target_schema")
    safe_vol = sanitize_path_component(volume_name or DEFAULT_VOLUME_NAME, "volume_name")

    if not relative_path or not str(relative_path).strip():
        raise ValueError("relative_path cannot be blank")
    rel_str = str(relative_path).strip()
    if ".." in rel_str or rel_str.startswith("\\"):
        raise ValueError("path traversal in relative_path")

    rel_clean = rel_str.lstrip("/")
    return f"/Volumes/{safe_cat}/{safe_sch}/{safe_vol}/{rel_clean}"


def artifact_owner_key(record: Any) -> Tuple[Any, Any, str, Any]:
    """Return immutable (connection_id, source_schema, normalized_object_type, object_name) tuple."""
    rec = record.asDict() if hasattr(record, "asDict") else dict(record)
    conn_id = rec.get("connection_id")
    schema = rec.get("source_schema")
    otype = rec.get("object_type")
    oname = rec.get("object_name")
    norm_type = normalize_object_type(otype)
    return (conn_id, schema, norm_type, oname)


def select_latest_artifact_records(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Select the latest record per artifact owner key using deterministic ordering."""
    groups: Dict[Tuple[Any, Any, str, Any], List[Dict[str, Any]]] = {}
    for r in records or []:
        key = artifact_owner_key(r)
        groups.setdefault(key, []).append(r)

    latest_list = []
    for key, item_list in groups.items():
        sorted_items = sorted(
            item_list,
            key=lambda x: (
                str(x.get("source_captured_ts") or x.get("captured_ts") or ""),
                str(x.get("updated_ts") or ""),
                str(x.get("assessment_id") or ""),
                str(x.get("run_id") or "")
            ),
            reverse=True
        )
        latest_list.append(sorted_items[0])

    return latest_list
