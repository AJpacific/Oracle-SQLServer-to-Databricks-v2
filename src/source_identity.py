"""
source_identity.py - deterministic, connection-owned table identity.

A source-table registration is uniquely identified by the six-part key:

    connection_id, source_system, source_server, source_database,
    source_schema, source_table

Oracle and SQL Server can both contain the same ``schema.table`` (e.g.
``dbo.Customers`` / ``sales.Orders``), and the same physical object can be
registered through different credentials. This module derives a stable
``source_table_id`` = SHA-256 over a versioned, normalized six-part key, so each
connection registration owns independent state and history.

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

# Sources whose connection identity is incomplete without an explicit database.
# Declared here so connection validation stays declarative: a future source
# opts in by adding its token rather than by adding another branch.
SOURCES_REQUIRING_DATABASE = frozenset({SQLSERVER})

LEGACY_SOURCE_IDENTITY_VERSION = 1
SOURCE_IDENTITY_VERSION = 2


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


def require_source_system(value, context="source row") -> str:
    """Return a canonical source token or raise for missing source metadata."""
    if value is None or not str(value).strip():
        raise ValueError(f"{context} requires source_system")
    return normalize_source_system(value)


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


def compute_legacy_source_table_id(source_system, source_server, source_database,
                                   source_schema, source_table) -> str:
    """Return the legacy physical-source ID for explicit migration only."""
    system = require_source_system(source_system, "source table identity")
    server = _normalize_identity_component(source_server)
    database = _normalize_identity_component(source_database)
    schema = _require(source_schema, "source_schema")
    table = _require(source_table, "source_table")
    payload = "\n".join([system, server, database, schema, table])
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def compute_source_table_id(connection_id, source_system, source_server,
                            source_database, source_schema, source_table) -> str:
    """Return the deterministic SHA-256 ``source_table_id`` for identity v2.

    connection_id is mandatory and trimmed. System/server/database are
    normalized case-insensitively; schema/table casing is preserved. A missing
    server/database is allowed (normalizes to '').
    """
    connection = _require(connection_id, "connection_id")
    system = require_source_system(source_system, "source table identity")
    server = _normalize_identity_component(source_server)
    database = _normalize_identity_component(source_database)
    schema = _require(source_schema, "source_schema")
    table = _require(source_table, "source_table")
    payload = "\n".join([
        f"v{SOURCE_IDENTITY_VERSION}", connection, system, server, database,
        schema, table,
    ])
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()
