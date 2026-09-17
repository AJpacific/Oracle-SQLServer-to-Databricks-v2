"""
assessment_common.py - source-neutral pieces of a broad source assessment.

Source-specific notebooks discover objects with their own dialect SQL (through
the adapter) and then call into this module to build the *normalized* assessment
record, classify complexity, and describe the persistence key. Keeping this
here means Oracle and SQL Server write an identical `source_assessment` shape.

No Oracle or SQL Server SQL belongs in this module. Pure: no Spark, no dbutils.
"""

from __future__ import annotations

try:
    from src.type_mappers.base import classify_table_compatibility
except ModuleNotFoundError:
    from type_mappers.base import classify_table_compatibility

# How a reported row count was obtained. A broad assessment never executes a
# per-table COUNT(*), so EXACT is deliberately not a member of this set.
CATALOG = "CATALOG"          # SQL Server catalog metadata (sys.partitions)
ESTIMATED = "ESTIMATED"      # Oracle optimizer dictionary statistics
UNAVAILABLE = "UNAVAILABLE"  # not applicable / not retrievable
ROW_COUNT_METHODS = (CATALOG, ESTIMATED, UNAVAILABLE)

ASSESSMENT_ERROR_LIMIT = 20
MANDATORY_DISCOVERY_STAGES = ("schema_discovery", "table_discovery")

OBJECT_TYPES = ("TABLE", "VIEW", "PROCEDURE", "FUNCTION", "PACKAGE",
                "PACKAGE_BODY")

COMPATIBILITY_STATUSES = ("COMPATIBLE", "REVIEW", "MANUAL", "UNABLE_TO_ASSESS")

COMPLEXITIES = ("LOW", "MEDIUM", "HIGH", "NOT_APPLICABLE")

# Ordered field list of one normalized assessment record. Both sources must
# produce exactly these fields so the shared MERGE and views stay stable.
ASSESSMENT_FIELDS = (
    "assessment_id", "run_id", "connection_id", "source_system", "source_server",
    "source_database", "source_schema", "object_name", "object_type",
    "row_count", "row_count_method", "size_mb", "column_count",
    "compatibility_status", "complexity", "assessment_message", "is_selected",
)

# Natural key used to MERGE an assessment record idempotently.
ASSESSMENT_MERGE_KEYS = ("assessment_id", "connection_id", "source_schema",
                         "object_type", "object_name")

# Columns the MERGE refreshes on a matched row. is_selected is deliberately
# excluded so a selection made by registration survives a re-assessment.
ASSESSMENT_UPDATE_FIELDS = (
    "run_id", "source_system", "source_server", "source_database", "row_count",
    "row_count_method", "size_mb", "column_count", "compatibility_status",
    "complexity", "assessment_message",
)


def summarize_table_compatibility(column_statuses):
    """Roll per-column mapping statuses up to a table compatibility category.

    Delegates to the single authoritative classifier so source notebooks have an
    explicit, importable dependency instead of relying on a bootstrap global.
    """
    return classify_table_compatibility(column_statuses)


def classify_complexity(row_count, column_count):
    """Size-based complexity band for a table (never a numerical score)."""
    rc = row_count or 0
    cc = column_count or 0
    if rc > 100_000_000 or cc > 200:
        return "HIGH"
    if rc > 1_000_000 or cc > 50:
        return "MEDIUM"
    return "LOW"


def _int_or_none(value):
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _float_or_none(value):
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def build_assessment_record(assessment_id, run_id, connection_id, source_system,
                            source_server, source_database, source_schema,
                            object_name, object_type, compatibility_status,
                            row_count=None, row_count_method=UNAVAILABLE,
                            size_mb=None, column_count=None,
                            complexity="NOT_APPLICABLE",
                            assessment_message=None, is_selected=False):
    """Return one normalized assessment record as a dict.

    Numeric fields are coerced (or left null); the caller's discovery SQL is
    irrelevant here, so Oracle and SQL Server produce an identical shape.
    """
    record = {
        "assessment_id": assessment_id,
        "run_id": run_id,
        "connection_id": connection_id,
        "source_system": source_system,
        "source_server": source_server,
        "source_database": source_database,
        "source_schema": source_schema,
        "object_name": object_name,
        "object_type": (object_type or "").upper().replace(" ", "_"),
        "row_count": _int_or_none(row_count),
        "row_count_method": row_count_method or UNAVAILABLE,
        "size_mb": _float_or_none(size_mb),
        "column_count": _int_or_none(column_count),
        "compatibility_status": compatibility_status,
        "complexity": complexity or "NOT_APPLICABLE",
        "assessment_message": assessment_message,
        "is_selected": bool(is_selected),
    }
    validate_assessment_record(record)
    return record


def validate_assessment_record(record):
    """Raise ValueError when a record would break the shared contract."""
    missing = [f for f in ASSESSMENT_FIELDS if f not in record]
    if missing:
        raise ValueError(f"assessment record is missing fields: {missing}")
    for key in ("assessment_id", "connection_id", "source_schema", "object_name"):
        if not record.get(key):
            raise ValueError(f"assessment record requires {key}")
    if record["object_type"] not in OBJECT_TYPES:
        raise ValueError(f"unsupported object_type: {record['object_type']!r}")
    if record["row_count_method"] not in ROW_COUNT_METHODS:
        raise ValueError(
            f"unsupported row_count_method: {record['row_count_method']!r}")
    if record["compatibility_status"] not in COMPATIBILITY_STATUSES:
        raise ValueError(
            f"unsupported compatibility_status: {record['compatibility_status']!r}")
    if record["complexity"] not in COMPLEXITIES:
        raise ValueError(f"unsupported complexity: {record['complexity']!r}")
    return record


def summarize_compatibility(records):
    """Count records per compatibility status (source-independent summary)."""
    summary = {}
    for r in records or []:
        status = r.get("compatibility_status")
        summary[status] = summary.get(status, 0) + 1
    return summary


def assessment_business_status(errors):
    """Return coverage status from sanitized discovery error records."""
    stages = {error.get("stage") for error in (errors or [])}
    if stages.intersection(MANDATORY_DISCOVERY_STAGES):
        return "FAILED"
    if errors:
        return "PARTIAL"
    return "COMPLETE"
