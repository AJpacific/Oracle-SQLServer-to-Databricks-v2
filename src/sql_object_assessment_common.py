"""
sql_object_assessment_common.py - source-neutral pieces of SQL-object assessment.

Source-specific notebooks extract definition text with their own dialect SQL
(Oracle ALL_VIEWS/ALL_SOURCE, SQL Server sys.sql_modules) and then use this
module to build the normalized `sql_object_assessment` record, apply the review
rules, and describe the idempotent persistence key.

Classification and conversion themselves live in src/sql_object_converter.py and
are NOT duplicated here. Nothing in this module executes SQL.

Pure: no Spark, no dbutils.
"""

from __future__ import annotations

try:
    from src import sql_object_converter as converter
except ModuleNotFoundError:
    import sql_object_converter as converter

OBJECT_TYPES = ("VIEW", "PROCEDURE", "FUNCTION", "PACKAGE", "PACKAGE_BODY")

NOT_REVIEWED = "NOT_REVIEWED"
PENDING_REVIEW = "PENDING_REVIEW"
APPROVED = "APPROVED"
REJECTED = "REJECTED"
# A human decision is never overwritten by a re-assessment.
TERMINAL_REVIEW_STATUSES = (APPROVED, REJECTED)

SQL_OBJECT_FIELDS = (
    "assessment_id", "run_id", "connection_id", "source_system",
    "source_database", "source_schema", "object_name", "object_type",
    "source_definition", "complexity_category", "classification_reason",
    "converted_definition", "conversion_language", "conversion_status",
    "review_status", "error_message",
)

SQL_OBJECT_MERGE_KEYS = ("assessment_id", "connection_id", "source_schema",
                         "object_type", "object_name")

SQL_OBJECT_UPDATE_FIELDS = (
    "run_id", "source_system", "source_database", "source_definition",
    "complexity_category", "classification_reason", "converted_definition",
    "conversion_language", "conversion_status", "error_message",
)

INACCESSIBLE_REASON = (
    "definition text is not accessible (encrypted module, missing "
    "SELECT/VIEW DEFINITION grant, or no source rows)")


def resolve_review_status(conversion_status):
    """Every generated draft requires human review before any use."""
    return PENDING_REVIEW if conversion_status == converter.GENERATED else NOT_REVIEWED


def build_sql_object_record(assessment_id, run_id, connection_id, source_system,
                            source_database, source_schema, object_name,
                            object_type, source_definition, mode="ASSESS",
                            use_ai=False):
    """Return one normalized sql_object_assessment record as a dict.

    Missing definition text is recorded as UNABLE_TO_ASSESS with an explicit
    reason - never labelled MANUAL merely because the text could not be read,
    and never fabricated. CONVERT mode adds a deterministic draft; nothing is
    executed or deployed.
    """
    otype = (object_type or "").upper().replace(" ", "_")
    if otype not in OBJECT_TYPES:
        raise ValueError(f"unsupported SQL object_type: {object_type!r}")

    converted = language = None
    error_message = None

    if not source_definition or not str(source_definition).strip():
        complexity = converter.UNABLE_TO_ASSESS
        reason = "source text unavailable"
        conversion_status = converter.NOT_STARTED
        error_message = INACCESSIBLE_REASON
    else:
        complexity, reason = converter.classify_sql_object(
            source_system, otype, source_definition)
        conversion_status = converter.NOT_STARTED
        if mode == "CONVERT":
            converted, language, conversion_status = (
                converter.convert_sql_object_deterministic(
                    source_system, otype, source_definition))
            if conversion_status == converter.NOT_SUPPORTED and use_ai:
                # No approved AI endpoint is configured; never fabricate output.
                conversion_status = converter.NOT_CONFIGURED

    return {
        "assessment_id": assessment_id,
        "run_id": run_id,
        "connection_id": connection_id,
        "source_system": source_system,
        "source_database": source_database,
        "source_schema": source_schema,
        "object_name": object_name,
        "object_type": otype,
        "source_definition": source_definition,
        "complexity_category": complexity,
        "classification_reason": reason,
        "converted_definition": converted,
        "conversion_language": language,
        "conversion_status": conversion_status,
        "review_status": resolve_review_status(conversion_status),
        "error_message": error_message,
    }


def summarize_complexity(records):
    """Count records per complexity category (source-independent summary)."""
    summary = {}
    for r in records or []:
        key = r.get("complexity_category")
        summary[key] = summary.get(key, 0) + 1
    return summary
