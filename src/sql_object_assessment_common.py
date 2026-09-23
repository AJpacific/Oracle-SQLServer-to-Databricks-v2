"""
sql_object_assessment_common.py - source-neutral pieces of SQL-object inventory.

Source-specific notebooks extract definition text with their own dialect SQL
(Oracle ALL_VIEWS/ALL_SOURCE, SQL Server sys.sql_modules) and then use this
module to build the normalized `sql_object_assessment` record and describe the
idempotent persistence key.

The accelerator only inventories original source definitions so they can be
materialized unchanged as .sql artifacts. Nothing here classifies, converts,
reviews, executes, or deploys SQL.

Pure: no Spark, no dbutils.
"""

from __future__ import annotations

OBJECT_TYPES = ("VIEW", "PROCEDURE", "FUNCTION", "PACKAGE", "PACKAGE_BODY")

SQL_OBJECT_FIELDS = (
    "assessment_id", "run_id", "connection_id", "source_system",
    "source_database", "source_schema", "object_name", "object_type",
    "source_definition", "error_message",
)

SQL_OBJECT_MERGE_KEYS = ("assessment_id", "connection_id", "source_database",
                         "source_schema", "object_type", "object_name")

SQL_OBJECT_UPDATE_FIELDS = (
    "run_id", "source_system", "source_database", "source_definition",
    "error_message",
)

INACCESSIBLE_REASON = (
    "definition text is not accessible (encrypted module, missing "
    "SELECT/VIEW DEFINITION grant, or no source rows)")

DISCOVERY_ERROR_LIMIT = 20


def build_sql_object_record(assessment_id, run_id, connection_id, source_system,
                            source_database, source_schema, object_name,
                            object_type, source_definition):
    """Return one normalized sql_object_assessment record as a dict.

    The definition text is stored exactly as extracted so NB18 can materialize
    it unchanged. A missing, blank, encrypted, or inaccessible definition gets
    an explicit credential-free reason - never fabricated text.
    """
    otype = (object_type or "").upper().replace(" ", "_")
    if otype not in OBJECT_TYPES:
        raise ValueError(f"unsupported SQL object_type: {object_type!r}")

    definition_missing = (
        source_definition is None or not str(source_definition).strip())

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
        "error_message": INACCESSIBLE_REASON if definition_missing else None,
    }


def discovery_business_status(schema_discovery_failed,
                              object_discovery_attempts,
                              object_discovery_successes,
                              discovery_failures=0,
                              inaccessible_definitions=0,
                              unsupported_object_types=0):
    """Return SQL-object assessment coverage without conflating failure types."""
    if schema_discovery_failed:
        return "FAILED"
    if object_discovery_attempts and not object_discovery_successes:
        return "FAILED"
    if (discovery_failures or inaccessible_definitions
            or unsupported_object_types):
        return "PARTIAL"
    return "COMPLETE"
