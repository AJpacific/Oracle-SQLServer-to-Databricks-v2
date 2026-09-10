"""
sqlserver_sql_builder.py - build SQL Server-side SQL strings (metadata + data).

Every query here targets SQL Server's catalog views and T-SQL dialect, which
differ from Oracle in important ways:

  * metadata lives in sys.schemas / sys.tables / sys.columns / sys.types
    (not ALL_TAB_COLUMNS)
  * row limiting uses ``SELECT TOP (n)`` (not Oracle ``FETCH FIRST n ROWS ONLY``)
  * identifiers are bracket-quoted (``[schema].[table]``), ``]`` escaped as ``]]``
  * temporal literals use ``CAST('...' AS <type>)`` (not ``TO_TIMESTAMP``)

Output column *labels* deliberately match the Oracle builder's JDBC-upcased
labels (COLUMN_NAME, ORDINAL_POSITION, IS_NULLABLE, DATA_TYPE,
CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE, DATETIME_PRECISION,
MIN_VAL, MAX_VAL, ROW_COUNT, UPPER_WATERMARK, COLUMN_NAME/KEY_POSITION) so the
shared notebooks read the same neutral names for both sources.

All functions are pure strings so they can be unit tested without a database.
"""

from __future__ import annotations

import re
from typing import List

try:
    from src.identifiers import (
        quote_sqlserver,
        sqlserver_fqn,
        escape_string_literal,
    )
    from src.sql_builder import canonical_watermark_string
except ModuleNotFoundError:
    from identifiers import (
        quote_sqlserver,
        sqlserver_fqn,
        escape_string_literal,
    )
    from sql_builder import canonical_watermark_string


# Eligible SQL Server temporal watermark families (upper-cased, precision
# stripped). NOTE: SQL Server ``timestamp`` is *rowversion*, NOT a datetime, and
# ``time``/``rowversion`` are never temporal watermarks.
SQLSERVER_WATERMARK_CANDIDATE_TYPES = {
    "DATETIMEOFFSET",
    "DATETIME2",
    "DATETIME",
    "SMALLDATETIME",
    "DATE",
}

# Ranking category (lower = higher priority). datetimeoffset carries the most
# fidelity, date the least.
_SQLSERVER_TZ_CATEGORY_RANK = {
    "DATETIMEOFFSET": 0,
    "DATETIME2": 1,
    "DATETIME": 2,
    "SMALLDATETIME": 3,
    "DATE": 4,
}


def normalize_watermark_type(sqlserver_type: str) -> str:
    """Normalize a SQL Server type to its family form (upper, precision stripped)."""
    t = (sqlserver_type or "").strip().upper()
    t = re.sub(r"\((\s*[\dA-Za-z, ]*\s*)\)", "", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t


def is_supported_watermark_type(sqlserver_type: str) -> bool:
    """True only for supported SQL Server temporal watermark families."""
    return normalize_watermark_type(sqlserver_type) in SQLSERVER_WATERMARK_CANDIDATE_TYPES


def watermark_type_rank(sqlserver_type: str) -> int:
    """Category rank for a SQL Server temporal family (lower wins)."""
    return _SQLSERVER_TZ_CATEGORY_RANK.get(normalize_watermark_type(sqlserver_type), 99)


# --------------------------------------------------------------------- metadata
def columns_metadata_query(database: str, owner: str, table: str) -> str:
    """Return a subquery yielding target-neutral column metadata.

    Uses SQL Server catalog views and returns one row per column in ordinal
    order. Output labels match the Oracle builder so NB01/NB02 are unchanged:
      COLUMN_NAME, ORDINAL_POSITION, IS_NULLABLE, DATA_TYPE,
      CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE,
      DATETIME_PRECISION
    plus neutral SQL Server extras (IS_IDENTITY, IS_COMPUTED, IS_HIDDEN,
    IS_ROWVERSION, SOURCE_TYPE_SCHEMA). CHARACTER_MAXIMUM_LENGTH is expressed in characters for
    n[var]char (max_length is bytes there); the SQL Server ``-1`` MAX sentinel is
    preserved so MAX types can be identified downstream.
    """
    s = escape_string_literal(owner)
    t = escape_string_literal(table)
    # sys.* views are database-scoped; qualify with the (validated) database name
    # when provided so the read targets the intended database explicitly.
    if database:
        prefix = f"{quote_sqlserver(database)}.sys."
    else:
        prefix = "sys."
    q = f"""(
        SELECT
            c.name                                        AS COLUMN_NAME,
            c.column_id                                   AS ORDINAL_POSITION,
            CASE WHEN c.is_nullable = 1 THEN 'YES' ELSE 'NO' END AS IS_NULLABLE,
            ty.name                                       AS DATA_TYPE,
            CASE
                WHEN c.max_length = -1 THEN -1
                WHEN ty.name IN ('nchar','nvarchar') THEN c.max_length / 2
                ELSE c.max_length
            END                                           AS CHARACTER_MAXIMUM_LENGTH,
            c.precision                                   AS NUMERIC_PRECISION,
            c.scale                                       AS NUMERIC_SCALE,
            CASE WHEN ty.name IN ('time','datetime2','datetimeoffset')
                 THEN c.scale END                         AS DATETIME_PRECISION,
            CAST(c.is_identity AS INT)                    AS IS_IDENTITY,
            CAST(c.is_computed AS INT)                    AS IS_COMPUTED,
            CAST(c.is_hidden AS INT)                      AS IS_HIDDEN,
            CASE WHEN ty.name IN ('timestamp','rowversion') THEN 1 ELSE 0 END
                                                          AS IS_ROWVERSION,
            tsch.name                                     AS SOURCE_TYPE_SCHEMA
        FROM {prefix}columns c
        JOIN {prefix}tables tb   ON c.object_id = tb.object_id
        JOIN {prefix}schemas sch ON tb.schema_id = sch.schema_id
        JOIN {prefix}types ty    ON c.user_type_id = ty.user_type_id
        JOIN {prefix}schemas tsch ON ty.schema_id = tsch.schema_id
        WHERE sch.name = {s} AND tb.name = {t}
    ) q"""
    return q


def primary_key_query(database: str, owner: str, table: str) -> str:
    """Return a subquery yielding ordered primary-key columns.

    Output labels: COLUMN_NAME, KEY_POSITION. Uses the actual primary key only
    (``is_primary_key = 1``), preserves composite-key order, excludes included
    columns, and never mistakes a plain unique index for a primary key.
    """
    s = escape_string_literal(owner)
    t = escape_string_literal(table)
    if database:
        prefix = f"{quote_sqlserver(database)}.sys."
    else:
        prefix = "sys."
    q = f"""(
        SELECT c.name AS COLUMN_NAME,
               ic.key_ordinal AS KEY_POSITION
        FROM {prefix}indexes i
        JOIN {prefix}index_columns ic
             ON i.object_id = ic.object_id AND i.index_id = ic.index_id
        JOIN {prefix}columns c
             ON ic.object_id = c.object_id AND ic.column_id = c.column_id
        JOIN {prefix}tables tb   ON i.object_id = tb.object_id
        JOIN {prefix}schemas sch ON tb.schema_id = sch.schema_id
        WHERE i.is_primary_key = 1
          AND ic.is_included_column = 0
          AND ic.key_ordinal > 0
          AND sch.name = {s} AND tb.name = {t}
    ) q"""
    return q


# ----------------------------------------------------------------------- probes
def build_top_n_probe(database: str, owner: str, table: str, n: int = 5) -> str:
    """A safe 'read only a few rows' probe using SQL Server ``TOP (n)``."""
    if n < 1:
        n = 1
    return (f"(SELECT TOP ({int(n)}) * "
            f"FROM {sqlserver_fqn(owner, table, database)}) q")


def build_count_query(database: str, owner: str, table: str) -> str:
    """Full row count for reconciliation."""
    return (f"(SELECT COUNT_BIG(*) AS ROW_COUNT "
            f"FROM {sqlserver_fqn(owner, table, database)}) q")


def build_upper_watermark_query(database: str, owner: str, table: str,
                                watermark_col: str, watermark_family: str) -> str:
    """Return a datatype-aware frozen upper watermark."""
    wm = quote_sqlserver(watermark_col)
    fam = normalize_watermark_type(watermark_family)
    if fam not in SQLSERVER_WATERMARK_CANDIDATE_TYPES:
        raise ValueError(f"Unsupported non-temporal watermark type: {watermark_family!r}")
    wm_expr = f"CAST({wm} AS datetime2(6))" if fam == "DATETIME2" else wm
    return (f"(SELECT MAX({wm_expr}) AS UPPER_WATERMARK "
            f"FROM {sqlserver_fqn(owner, table, database)}) q")

def build_min_max_query(database: str, owner: str, table: str, column: str) -> str:
    """MIN/MAX of a column, used to compute JDBC read partition bounds."""
    c = quote_sqlserver(column)
    return (f"(SELECT MIN({c}) AS MIN_VAL, MAX({c}) AS MAX_VAL "
            f"FROM {sqlserver_fqn(owner, table, database)}) q")


# ------------------------------------------------------------- data extraction
def _to_utc_parts(value):
    """Parse a watermark value into a UTC datetime via the shared canonicalizer."""
    from datetime import datetime, timezone
    canonical = canonical_watermark_string(value, strict=True)
    # canonical is ISO-8601 UTC with a trailing 'Z'.
    dt = datetime.fromisoformat(canonical.replace("Z", "+00:00"))
    return dt.astimezone(timezone.utc)


def _format_watermark_literal(value, family: str) -> str:
    """Render a temporal watermark bound as a correct SQL Server literal.

    Only SQL Server temporal families are accepted: DATE, SMALLDATETIME,
    DATETIME, DATETIME2, DATETIMEOFFSET (precision variants normalized). Naive
    families use the accelerator's approved UTC interpretation; DATETIMEOFFSET
    preserves the absolute instant with a ``+00:00`` offset. A null bound or any
    non-temporal family raises ValueError.
    """
    if value is None:
        raise ValueError("Watermark bound value is required")
    fam = normalize_watermark_type(family)
    if fam not in SQLSERVER_WATERMARK_CANDIDATE_TYPES:
        raise ValueError(f"Unsupported non-temporal watermark type: {family!r}")
    dt = _to_utc_parts(value)
    if fam == "DATE":
        body = dt.strftime("%Y-%m-%d")
        return f"CAST('{body}' AS date)"
    if fam == "SMALLDATETIME":
        body = dt.strftime("%Y-%m-%d %H:%M:%S")
        return f"CAST('{body}' AS smalldatetime)"
    if fam == "DATETIME":
        body = dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]  # milliseconds
        return f"CAST('{body}' AS datetime)"
    if fam == "DATETIME2":
        body = dt.strftime("%Y-%m-%d %H:%M:%S.%f")
        return f"CAST('{body}' AS datetime2(6))"
    # DATETIMEOFFSET: preserve the absolute instant with an explicit UTC offset.
    body = dt.strftime("%Y-%m-%dT%H:%M:%S.%f") + "+00:00"
    return f"CAST('{body}' AS datetimeoffset)"


def build_full_extract_query(database: str, owner: str, table: str,
                             columns: List[str] = None,
                             watermark_column: str = None,
                             watermark_type: str = None) -> str:
    """SELECT a full snapshot using the incremental DATETIME2(6) projection."""
    fam = normalize_watermark_type(watermark_type) if watermark_type else None
    if columns:
        projected = []
        for c in columns:
            qc = quote_sqlserver(c)
            if (fam == "DATETIME2" and watermark_column and
                    c.strip().lower() == watermark_column.strip().lower()):
                projected.append(f"CAST({qc} AS datetime2(6)) AS {qc}")
            else:
                projected.append(qc)
        col_list = ", ".join(projected)
    else:
        if fam == "DATETIME2" and watermark_column:
            raise ValueError("SQL Server DATETIME2 full extraction requires an explicit approved column list")
        col_list = "*"
    return f"(SELECT {col_list} FROM {sqlserver_fqn(owner, table, database)}) q"

def build_incremental_extract_query(database, owner, table, watermark_col,
                                    watermark_family, last_watermark_value,
                                    upper_watermark_value,
                                    columns: List[str] = None) -> str:
    """SELECT the bounded incremental slice ``(last_watermark, upper_watermark]``.

    The interval matches the Oracle builder exactly:

        watermark_col > last_watermark_value
        AND watermark_col <= upper_watermark_value

    Both bounds are mandatory. No artificial increment is added.
    """
    if last_watermark_value is None or upper_watermark_value is None:
        raise ValueError("Incremental extraction requires lower and upper watermarks")
    wm = quote_sqlserver(watermark_col)
    fam = normalize_watermark_type(watermark_family)
    if columns:
        projected = []
        for c in columns:
            qc = quote_sqlserver(c)
            if fam == "DATETIME2" and c.strip().lower() == watermark_col.strip().lower():
                projected.append(f"CAST({qc} AS datetime2(6)) AS {qc}")
            else:
                projected.append(qc)
        col_list = ", ".join(projected)
    else:
        # Callers should provide the approved mapping column list for DATETIME2
        # so the watermark can be projected at six digits without duplicate names.
        if fam == "DATETIME2":
            raise ValueError(
                "SQL Server DATETIME2 incremental extraction requires an explicit "
                "approved column list for six-digit watermark projection")
        col_list = "*"
    lower_lit = _format_watermark_literal(last_watermark_value, watermark_family)
    upper_lit = _format_watermark_literal(upper_watermark_value, watermark_family)
    # Apply one consistent six-digit policy to both source values and bounds.
    # This intentionally trades the seventh datetime2 digit for compatibility
    # with Databricks TIMESTAMP and prevents a 7-digit MAX from being stored as
    # a smaller 6-digit checkpoint that would exclude its own boundary row.
    wm_expr = (f"CAST({wm} AS datetime2(6))" if fam == "DATETIME2" else wm)
    where = f"{wm_expr} > {lower_lit} AND {wm_expr} <= {upper_lit}"
    return (f"(SELECT {col_list} FROM {sqlserver_fqn(owner, table, database)} "
            f"WHERE {where}) q")
