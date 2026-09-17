"""
inventory_common.py - source-neutral pieces of registered-table inventory.

Source-specific inventory notebooks call their adapter for column and
primary-key metadata, then use this module to normalize each column into the
shared `source_inventory` shape and to build the control-table payload that
records the resolved load strategy. Oracle and SQL Server therefore write an
identical inventory schema.

No Oracle or SQL Server SQL belongs in this module. Pure: no Spark, no dbutils.
"""

from __future__ import annotations

# Ordered field list of one normalized inventory row (captured_ts is added by
# the shared persistence helper).
INVENTORY_FIELDS = (
    "run_id", "source_table_id", "connection_id", "source_system",
    "source_server", "source_database", "source_schema", "source_table",
    "column_name", "ordinal_position", "is_nullable", "data_type",
    "character_maximum_length", "numeric_precision", "numeric_scale",
    "datetime_precision", "is_identity", "is_computed", "is_hidden",
    "is_rowversion", "source_type_schema",
)

INVENTORY_MERGE_KEYS = (
    "run_id",
    "connection_id",
    "source_table_id",
    "column_name",
)

INVENTORY_IDENTITY_FIELDS = (
    "connection_id", "source_system", "source_server", "source_database",
    "source_schema", "source_table",
)

# Neutral column aliases every adapter's columns_metadata_query must return.
REQUIRED_COLUMN_ALIASES = (
    "COLUMN_NAME", "ORDINAL_POSITION", "IS_NULLABLE", "DATA_TYPE",
    "CHARACTER_MAXIMUM_LENGTH", "NUMERIC_PRECISION", "NUMERIC_SCALE",
    "DATETIME_PRECISION",
)

# Optional source-specific flags; a source that does not report them yields
# False rather than an error.
OPTIONAL_COLUMN_ALIASES = ("IS_IDENTITY", "IS_COMPUTED", "IS_HIDDEN",
                           "IS_ROWVERSION", "SOURCE_TYPE_SCHEMA")


def inventory_record_dict(record):
    """Return one inventory record as a field-keyed dictionary."""
    if hasattr(record, "asDict"):
        return record.asDict(recursive=True)
    if isinstance(record, dict):
        return dict(record)
    values = tuple(record)
    if len(values) != len(INVENTORY_FIELDS):
        raise ValueError(
            f"inventory record has {len(values)} values; "
            f"expected {len(INVENTORY_FIELDS)}")
    return dict(zip(INVENTORY_FIELDS, values))


def find_duplicate_inventory_keys(records):
    """Return duplicate connection-owned inventory keys."""
    seen = set()
    duplicates = []
    duplicate_set = set()
    for record in records or []:
        item = inventory_record_dict(record)
        key = tuple(item.get(field) for field in INVENTORY_MERGE_KEYS)
        if key in seen and key not in duplicate_set:
            duplicates.append(key)
            duplicate_set.add(key)
        seen.add(key)
    return duplicates


def validate_inventory_batch(records):
    """Validate one complete run/table inventory and return normalized dicts."""
    items = [inventory_record_dict(record) for record in (records or [])]
    if not items:
        return []

    for item in items:
        for field in INVENTORY_MERGE_KEYS:
            if item.get(field) is None or not str(item.get(field)).strip():
                raise ValueError(f"inventory record requires {field}")

    duplicates = find_duplicate_inventory_keys(items)
    if duplicates:
        run_id, connection_id, source_table_id, column_name = duplicates[0]
        raise ValueError(
            "duplicate inventory key: "
            f"run_id={run_id!r}, connection_id={connection_id!r}, "
            f"source_table_id={source_table_id!r}, "
            f"column_name={column_name!r}")

    scopes = {
        (item["run_id"], item["connection_id"], item["source_table_id"])
        for item in items
    }
    if len(scopes) != 1:
        raise ValueError(
            "persist_inventory_rows requires exactly one run_id, connection_id, "
            "and source_table_id scope")

    identity = tuple(items[0].get(field) for field in INVENTORY_IDENTITY_FIELDS)
    for item in items[1:]:
        current = tuple(item.get(field) for field in INVENTORY_IDENTITY_FIELDS)
        if current != identity:
            raise ValueError(
                f"inventory rows for source_table_id "
                f"{items[0]['source_table_id']!r} disagree on connection or "
                "source identity")
    return items


def validate_metadata_aliases(available_aliases):
    """Raise ValueError when an adapter's metadata result is missing an alias."""
    available = {str(a).upper() for a in (available_aliases or [])}
    missing = [a for a in REQUIRED_COLUMN_ALIASES if a not in available]
    if missing:
        raise ValueError(
            "source metadata result is missing required neutral alias(es): "
            + ", ".join(missing))
    return True


def _int_or_none(value):
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _flag(value):
    """Interpret a source flag (int, bool, or string) as a boolean."""
    if value is None:
        return False
    if isinstance(value, str):
        return value.strip().lower() in ("1", "true", "yes", "y")
    return bool(value)


def normalize_inventory_row(column, identity):
    """Normalize one source column result into the shared inventory tuple.

    ``column`` is a dict of the adapter's neutral aliases; ``identity`` carries
    run_id, connection_id, source_table_id, and source object identity.
    """
    c = {str(k).upper(): v for k, v in dict(column).items()}
    return (
        identity["run_id"],
        identity["source_table_id"],
        identity.get("connection_id"),
        identity["source_system"],
        identity.get("source_server"),
        identity.get("source_database"),
        identity["source_schema"],
        identity["source_table"],
        c.get("COLUMN_NAME"),
        _int_or_none(c.get("ORDINAL_POSITION")),
        c.get("IS_NULLABLE"),
        c.get("DATA_TYPE"),
        _int_or_none(c.get("CHARACTER_MAXIMUM_LENGTH")),
        _int_or_none(c.get("NUMERIC_PRECISION")),
        _int_or_none(c.get("NUMERIC_SCALE")),
        _int_or_none(c.get("DATETIME_PRECISION")),
        _flag(c.get("IS_IDENTITY")),
        _flag(c.get("IS_COMPUTED")),
        _flag(c.get("IS_HIDDEN")),
        _flag(c.get("IS_ROWVERSION")),
        c.get("SOURCE_TYPE_SCHEMA"),
    )


def strategy_columns(columns):
    """Build the column dicts the shared watermark/strategy resolver consumes.

    Hidden and computed columns are never eligible for automatic watermark
    selection; their explicit mapping policy is applied later during mapping.
    """
    out = []
    for column in columns or []:
        c = {str(k).upper(): v for k, v in dict(column).items()}
        if _flag(c.get("IS_HIDDEN")) or _flag(c.get("IS_COMPUTED")):
            continue
        out.append({
            "column_name": c.get("COLUMN_NAME"),
            "data_type": c.get("DATA_TYPE"),
            "scale": _int_or_none(c.get("NUMERIC_SCALE")),
            "ordinal_position": _int_or_none(c.get("ORDINAL_POSITION")),
            "datetime_precision": _int_or_none(c.get("DATETIME_PRECISION")),
        })
    return out


def build_strategy_payload(source_table_id, decision, primary_key_columns):
    """Return the source_table_control payload for a resolved load strategy."""
    return {
        "source_table_id": source_table_id,
        "load_strategy": decision["strategy"],
        "primary_key_columns": list(primary_key_columns) if primary_key_columns else None,
        "watermark_column": decision.get("watermark_column"),
        "watermark_data_type": decision.get("watermark_data_type"),
        "current_status": "INVENTORIED",
        "error_message": None,
    }
