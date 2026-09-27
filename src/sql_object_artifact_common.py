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


def canonical_artifact_owner_id(
    connection_id: Any,
    source_system: Any = None,
    source_database: Any = None,
    source_schema: Any = None,
    object_type: Any = None,
    object_name: Any = None,
) -> str:
    """Build a deterministic canonical artifact owner identity string."""
    conn = str(connection_id or "").strip()
    sys_name = str(source_system or "").strip().lower()
    db = str(source_database or "").strip().lower() if source_database else ""
    sch = str(source_schema or "").strip().lower()
    otype = normalize_object_type(object_type)
    obj = str(object_name or "").strip()
    return f"{conn}:{sys_name}:{db}:{sch}:{otype}:{obj}"


def canonical_owner_key(record: Any) -> Tuple[str, str, str, str, str, str]:
    """Return 6-tuple canonical owner key:
    (connection_id, source_system, source_database, source_schema, normalized_object_type, object_name)
    """
    rec = record.asDict() if hasattr(record, "asDict") else dict(record)
    conn_id = str(rec.get("connection_id") or "").strip()
    src_sys = str(rec.get("source_system") or "").strip().lower()
    db = str(rec.get("source_database") or "").strip()
    sch = str(rec.get("source_schema") or "").strip()
    otype = normalize_object_type(rec.get("object_type"))
    oname = str(rec.get("object_name") or "").strip()
    return (conn_id, src_sys, db, sch, otype, oname)


def build_artifact_relative_path(
    *args: Any,
    connection_id: Any = None,
    source_system: Any = None,
    source_database: Any = None,
    source_schema: Any = None,
    object_type: Any = None,
    object_name: Any = None,
    append_hash: bool = False,
    **kwargs: Any,
) -> str:
    """Build deterministic connection-owned relative path.

    Supports keyword-only contract:
      build_artifact_relative_path(
          connection_id=..., source_system=..., source_database=...,
          source_schema=..., object_type=..., object_name=...
      )

    Supports backward-compatible positional calls:
      build_artifact_relative_path(conn_id, schema, otype, oname)
      build_artifact_relative_path(conn_id, source_database, schema, otype, oname)

    Collision resistance:
      When normalization of object_name is lossy (e.g. uppercase characters,
      spaces, slashes, or special characters), appends a short stable SHA-256
      hash derived from the full canonical owner identity to ensure distinct
      source objects (such as 'A B', 'A/B', 'A_B', 'Foo', 'FOO') never collide
      on the same physical filename.
    """
    conn_id = connection_id or kwargs.get("connection_id")
    src_db = source_database or kwargs.get("source_database")
    schema = source_schema or kwargs.get("source_schema")
    otype = object_type or kwargs.get("object_type")
    oname = object_name or kwargs.get("object_name")
    src_sys = source_system or kwargs.get("source_system")

    pargs = list(args)
    if len(pargs) == 5:
        conn_id = conn_id or pargs[0]
        src_db = src_db or pargs[1]
        schema = schema or pargs[2]
        otype = otype or pargs[3]
        oname = oname or pargs[4]
    elif len(pargs) == 4:
        conn_id = conn_id or pargs[0]
        cand_type_1 = (pargs[1] or "").strip().upper().replace(" ", "_")
        cand_type_2 = (pargs[2] or "").strip().upper().replace(" ", "_")
        if cand_type_1 in SUPPORTED_OBJECT_TYPES and cand_type_2 in SUPPORTED_OBJECT_TYPES:
            # Ambiguous: e.g. ("c1", "VIEW", "PROCEDURE", "foo")
            raise ValueError(
                "Ambiguous positional arguments: schema and object_type both match "
                "supported object types. Use keyword arguments."
            )
        elif cand_type_1 in SUPPORTED_OBJECT_TYPES:
            otype = otype or pargs[1]
            oname = oname or pargs[2]
            schema = schema or pargs[3]
        else:
            schema = schema or pargs[1]
            otype = otype or pargs[2]
            oname = oname or pargs[3]
    elif len(pargs) == 3:
        conn_id = conn_id or pargs[0]
        cand_type = (pargs[1] or "").strip().upper().replace(" ", "_")
        if cand_type in SUPPORTED_OBJECT_TYPES:
            otype = otype or pargs[1]
            oname = oname or pargs[2]
        else:
            schema = schema or pargs[1]
            otype = otype or pargs[2]
    elif len(pargs) > 0:
        conn_id = conn_id or pargs[0]

    safe_conn = sanitize_path_component(conn_id, "connection_id")
    norm_type = normalize_object_type(otype)
    type_dir = OBJECT_TYPE_DIRECTORIES[norm_type]
    safe_obj = sanitize_path_component(oname, "object_name")

    # Detect lossy normalization (e.g. spaces, slashes, punctuation replaced by _, or explicit append_hash)
    val_str = str(oname)
    is_lossy = (val_str.lower() != safe_obj)
    if is_lossy or append_hash:
        owner_id = canonical_artifact_owner_id(
            connection_id=safe_conn,
            source_system=src_sys,
            source_database=src_db,
            source_schema=schema,
            object_type=norm_type,
            object_name=oname,
        )
        short_hash = hashlib.sha256(owner_id.encode("utf-8")).hexdigest()[:8]
        file_name = f"{safe_obj}_{short_hash}.sql"
    else:
        file_name = f"{safe_obj}.sql"

    parts = [safe_conn]
    if src_db and str(src_db).strip():
        parts.append(sanitize_path_component(src_db, "source_database"))
    if schema and str(schema).strip():
        parts.append(sanitize_path_component(schema, "source_schema"))
    parts.append(type_dir)
    parts.append(file_name)

    return "/".join(parts)


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


def artifact_owner_key(record: Any) -> Tuple[Any, ...]:
    """Return immutable artifact owner key tuple.

    Includes source_database when present to distinguish objects across databases:
    (connection_id, source_database, source_schema, normalized_object_type, object_name)
    or for legacy/unqualified records:
    (connection_id, source_schema, normalized_object_type, object_name).
    """
    rec = record.asDict() if hasattr(record, "asDict") else dict(record)
    conn_id = rec.get("connection_id")
    db = rec.get("source_database")
    schema = rec.get("source_schema")
    otype = rec.get("object_type")
    oname = rec.get("object_name")
    norm_type = normalize_object_type(otype)
    if db is not None and str(db).strip() != "":
        return (conn_id, str(db).strip(), schema, norm_type, oname)
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


def write_atomic_file(filepath: str, content: str) -> None:
    """Write content to filepath atomically using a temporary file and os.replace."""
    import os
    import uuid
    dirname = os.path.dirname(filepath)
    os.makedirs(dirname, exist_ok=True)
    tmp_path = f"{filepath}.tmp.{uuid.uuid4().hex}"
    try:
        with open(tmp_path, "w", encoding="utf-8", newline="") as f:
            f.write(content)
        os.replace(tmp_path, filepath)
    except Exception as exc:
        if os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except Exception:
                pass
        raise exc
