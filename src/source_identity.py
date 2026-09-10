"""
source_identity.py - deterministic, source-qualified table identity.

A single source table is uniquely identified by the five-part key:

    source_system, source_server, source_database, source_schema, source_table

Oracle and SQL Server can both contain the same ``schema.table`` (e.g.
``dbo.Customers`` / ``sales.Orders``), and the same ``schema.table`` can exist in
two databases or on two servers. Identifying a table only by
``source_schema + source_table`` is therefore unsafe once more than one source is
onboarded. This module derives a stable ``source_table_id`` = SHA-256 over the
normalized five-part key, so every operational control/queue table can carry and
join on a single collision-free identity.

Normalization policy:
  * source_system / source_server / source_database are normalized
    case-insensitively (lower-cased, trimmed). A missing server/database
    (legacy Oracle rows) normalizes to the empty string so historical rows stay
    deterministic and backward compatible.
  * source_schema / source_table casing is preserved, because the source adapter
    is case-sensitive about schema/table names.

Pure module: no Spark, no dbutils. Fully unit-testable.
"""

from __future__ import annotations

import hashlib


# SQL Server appears under several spellings in the wild. All of them normalize
# to the single canonical token 'sqlserver'.
_SQLSERVER_SYNONYMS = {
    "sqlserver",
    "sql_server",
    "sql-server",
    "mssql",
    "sql server",
    "microsoft sql server",
    "microsoft_sql_server",
}

ORACLE = "oracle"
SQLSERVER = "sqlserver"


def normalize_source_system(value) -> str:
    """Normalize a raw ``source_system`` value to a canonical token.

    Returns 'oracle' or 'sqlserver'. Raises ValueError for a null/empty value or
    any unrecognized source system - an unknown source is never silently
    defaulted to Oracle.
    """
    if value is None:
        raise ValueError("source_system is required (received None)")
    token = str(value).strip().lower()
    if not token:
        raise ValueError("source_system is empty")
    if token == ORACLE:
        return ORACLE
    if token in _SQLSERVER_SYNONYMS:
        return SQLSERVER
    raise ValueError(
        f"Unsupported source_system {value!r}; expected one of "
        "'oracle', 'sqlserver', 'sql_server', 'mssql'"
    )


def _normalize_identity_component(value) -> str:
    """Case-insensitive normalization for a system/server/database identity part.

    None and blank both normalize to '' so a legacy Oracle row with NULL
    server/database stays deterministic.
    """
    if value is None:
        return ""
    return str(value).strip().lower()


def _require(value, field: str) -> str:
    if value is None or str(value).strip() == "":
        raise ValueError(f"{field} is required to compute source_table_id")
    return str(value).strip()


def compute_source_table_id(source_system, source_server, source_database,
                            source_schema, source_table) -> str:
    """Return the deterministic SHA-256 ``source_table_id`` for the 5-part key.

    system/server/database are normalized case-insensitively; schema/table casing
    is preserved. source_schema and source_table are mandatory; a missing
    server/database is allowed (normalizes to '').
    """
    system = normalize_source_system(source_system)
    server = _normalize_identity_component(source_server)
    database = _normalize_identity_component(source_database)
    schema = _require(source_schema, "source_schema")
    table = _require(source_table, "source_table")
    payload = "\n".join([system, server, database, schema, table])
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()
