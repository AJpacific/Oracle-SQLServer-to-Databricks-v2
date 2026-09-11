"""
sql_builder.py - build Oracle-side SQL strings (metadata + data extraction).

Every query here targets Oracle's data dictionary and SQL dialect, which differs
from SQL Server in important ways:

  * metadata lives in ALL_TAB_COLUMNS / ALL_CONS_COLUMNS (not INFORMATION_SCHEMA)
  * row limiting uses FETCH FIRST n ROWS ONLY (12c+), not TOP n
  * identifiers are quoted with double quotes and are case-sensitive once quoted
  * there is no COL_LENGTH(); lengths come straight from the dictionary

All functions are pure strings so they can be unit tested without a database.
"""

from __future__ import annotations

from typing import List

try:
    from src.identifiers import (
        quote_oracle,
        escape_string_literal,
        oracle_fqn
    )
    from src.watermark import canonical_watermark_string as _canonical_watermark_string
except ModuleNotFoundError:
    from identifiers import (
        quote_oracle,
        escape_string_literal,
        oracle_fqn
    )
    from watermark import canonical_watermark_string as _canonical_watermark_string


# --------------------------------------------------------------------- metadata
def columns_metadata_query(owner: str, table: str) -> str:
    """Return a subquery that yields target-neutral column metadata.

    Output columns are deliberately named to match the SQL Server accelerator so
    downstream notebooks (NB01/NB02) are unchanged:
      column_name, ordinal_position, is_nullable, data_type,
      character_maximum_length, numeric_precision, numeric_scale,
      datetime_precision
    """
    o = escape_string_literal(owner)
    t = escape_string_literal(table)
    q = f"""(
        SELECT
            column_name                                   AS column_name,
            column_id                                     AS ordinal_position,
            CASE WHEN nullable = 'Y' THEN 'YES' ELSE 'NO' END AS is_nullable,
            data_type                                     AS data_type,
            CASE
                WHEN data_type IN ('CHAR','VARCHAR2','NCHAR','NVARCHAR2')
                    THEN char_length
                ELSE data_length
            END                                           AS character_maximum_length,
            data_precision                                AS numeric_precision,
            data_scale                                    AS numeric_scale,
            CASE WHEN data_type LIKE 'TIMESTAMP%' THEN data_scale END
                                                          AS datetime_precision
        FROM all_tab_columns
        WHERE owner = {o} AND table_name = {t}
        ORDER BY column_id
    ) q"""
    return q


def primary_key_query(owner: str, table: str) -> str:
    """Return a subquery yielding ordered primary-key columns: column_name, key_position."""
    o = escape_string_literal(owner)
    t = escape_string_literal(table)
    q = f"""(
        SELECT cols.column_name AS column_name,
               cols.position    AS key_position
        FROM all_constraints cons
        JOIN all_cons_columns cols
             ON cons.owner = cols.owner
            AND cons.constraint_name = cols.constraint_name
        WHERE cons.constraint_type = 'P'
          AND cons.owner = {o}
          AND cons.table_name = {t}
        ORDER BY cols.position
    ) q"""
    return q


# ----------------------------------------------------------------------- probes
def build_top_n_probe(owner: str, table: str, n: int = 5) -> str:
    """A safe 'read only a few rows' probe using Oracle FETCH FIRST."""
    if n < 1:
        n = 1
    return (f"(SELECT * FROM {oracle_fqn(owner, table)} "
            f"FETCH FIRST {int(n)} ROWS ONLY) q")


def build_count_query(owner: str, table: str) -> str:
    """Full row count for reconciliation."""
    return f"(SELECT COUNT(*) AS row_count FROM {oracle_fqn(owner, table)}) q"


def build_max_watermark_select(owner: str, table: str, watermark_col: str) -> str:
    """SELECT MAX(watermark) so incremental prep can advance the high-water mark."""
    return (f"(SELECT MAX({quote_oracle(watermark_col)}) AS max_wm "
            f"FROM {oracle_fqn(owner, table)}) q")


def build_upper_watermark_query(owner: str, table: str, watermark_col: str) -> str:
    """Return the current MAX(watermark) as UPPER_WATERMARK.

    Captured exactly once per delta run in NB11a to fix the upper boundary of the
    extract interval; the value is then frozen for that table's run and never
    recomputed downstream. Structurally:
        SELECT MAX("wm") AS UPPER_WATERMARK FROM "OWNER"."TABLE"
    """
    return (f"(SELECT MAX({quote_oracle(watermark_col)}) AS UPPER_WATERMARK "
            f"FROM {oracle_fqn(owner, table)}) q")


def build_min_max_query(owner: str, table: str, column: str) -> str:
    """MIN/MAX of a column, used to compute JDBC read partition bounds."""
    c = quote_oracle(column)
    return (f"(SELECT MIN({c}) AS min_val, MAX({c}) AS max_val "
            f"FROM {oracle_fqn(owner, table)}) q")


# ------------------------------------------------------------- data extraction
def _iso_to_oracle_datetime(value, keep_fraction=True) -> str:
    """Normalize an ISO-8601 or legacy datetime string to Oracle wall-clock
    'YYYY-MM-DD HH24:MI:SS[.ffffff]' with any timezone offset stripped."""
    import re
    s = str(value).strip().replace("T", " ")
    # Drop a trailing UTC 'Z' or a +/-HH:MM offset (tz handled separately).
    s = re.sub(r"\s*(Z|[+-]\d{2}:?\d{2})\s*$", "", s).strip()
    if not keep_fraction:
        s = s.split(".")[0]
    return s


def _iso_to_oracle_tz(value) -> str:
    """Normalize a timezone-aware value to 'YYYY-MM-DD HH24:MI:SS.FF +TZH:TZM'.

    Accepts ISO-8601 ('T', 'Z', +HH:MM / +HHMM) and legacy space-separated
    inputs. A naive value is treated as UTC per the accelerator policy.
    """
    import re
    s = str(value).strip().replace("T", " ")
    m = re.search(r"(Z|[+-]\d{2}:?\d{2})\s*$", s)
    if m:
        tok = m.group(1)
        body = s[:m.start()].strip()
        off = "+00:00" if tok == "Z" else (tok if ":" in tok else tok[:3] + ":" + tok[3:])
    else:
        body, off = s, "+00:00"
    if "." not in body:
        body = body + ".0"
    return f"{body} {off}"


def canonical_watermark_string(value, strict=False):
    """Deprecated alias: the canonical serializer is source-neutral.

    Kept so existing Oracle-side callers keep working; new code should import
    from src/watermark.py directly.
    """
    return _canonical_watermark_string(value, strict=strict)


def _normalize_family(family) -> str:
    """Strip precision, uppercase, and collapse whitespace for a watermark family."""
    import re
    t = (family or "").strip().upper()
    t = re.sub(r"\((\s*\d+\s*)\)", "", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t


def _format_watermark_literal(value, family: str) -> str:
    """Render a temporal watermark bound as a correct Oracle literal.

    Only Oracle temporal families are accepted: DATE, TIMESTAMP, TIMESTAMP WITH
    TIME ZONE, TIMESTAMP WITH LOCAL TIME ZONE (precision variants normalized).
    Timezone-aware families use TO_TIMESTAMP_TZ and preserve/normalize the
    offset; DATE/plain TIMESTAMP are treated as UTC wall-clock. A null bound or
    any non-temporal family raises ValueError.
    """
    if value is None:
        raise ValueError("Watermark bound value is required")
    fam = _normalize_family(family)
    if fam == "DATE":
        body = _iso_to_oracle_datetime(value, keep_fraction=False).replace("'", "''")
        return f"TO_DATE('{body}', 'YYYY-MM-DD HH24:MI:SS')"
    if fam in ("TIMESTAMP WITH TIME ZONE", "TIMESTAMP WITH LOCAL TIME ZONE"):
        body = _iso_to_oracle_tz(value).replace("'", "''")
        return (
            f"TO_TIMESTAMP_TZ('{body}', "
            f"'YYYY-MM-DD HH24:MI:SS.FF TZH:TZM')"
        )
    if fam == "TIMESTAMP":
        body = _iso_to_oracle_datetime(value, keep_fraction=True).replace("'", "''")
        fmt = "YYYY-MM-DD HH24:MI:SS.FF" if "." in body else "YYYY-MM-DD HH24:MI:SS"
        return f"TO_TIMESTAMP('{body}', '{fmt}')"
    raise ValueError(f"Unsupported non-temporal watermark type: {family!r}")


def build_full_extract_query(owner: str, table: str, columns: List[str] = None) -> str:
    """SELECT the whole table (optionally an explicit column list) for a full load."""
    if columns:
        col_list = ", ".join(quote_oracle(c) for c in columns)
    else:
        col_list = "*"
    return f"(SELECT {col_list} FROM {oracle_fqn(owner, table)}) q"


def build_incremental_extract_query(owner, table, watermark_col, watermark_family,
                                    last_watermark_value, upper_watermark_value,
                                    columns: List[str] = None) -> str:
    """SELECT the bounded incremental slice ``(last_watermark, upper_watermark]``.

    The upper bound is the watermark captured once per run in NB11a and frozen
    for the table's run, so the interval stays fixed:

        watermark_col > last_watermark_value
        AND watermark_col <= upper_watermark_value

    Both bounds are mandatory. Initial-load state must be committed before an
    incremental query can be built. No artificial increment is added.
    """
    if last_watermark_value is None or upper_watermark_value is None:
        raise ValueError("Incremental extraction requires lower and upper watermarks")
    if columns:
        col_list = ", ".join(quote_oracle(c) for c in columns)
    else:
        col_list = "*"
    wm = quote_oracle(watermark_col)
    lower_lit = _format_watermark_literal(last_watermark_value, watermark_family)
    upper_lit = _format_watermark_literal(upper_watermark_value, watermark_family)
    where = f"{wm} > {lower_lit} AND {wm} <= {upper_lit}"
    return (f"(SELECT {col_list} FROM {oracle_fqn(owner, table)} "
            f"WHERE {where}) q")


# ------------------------------------------------------- discovery / assessment
# Owners excluded from a broad Oracle assessment. These are Oracle-maintained
# schemas that never hold user data worth migrating.
ORACLE_SYSTEM_OWNERS = (
    "SYS", "SYSTEM", "OUTLN", "XDB", "CTXSYS", "MDSYS", "ORDSYS", "ORDDATA",
    "ORDPLUGINS", "DBSNMP", "APPQOSSYS", "WMSYS", "LBACSYS", "DVSYS", "AUDSYS",
    "GSMADMIN_INTERNAL", "OJVMSYS", "DBSFWUSER", "REMOTE_SCHEDULER_AGENT",
    "SYS$UMF", "OLAPSYS", "SI_INFORMTN_SCHEMA", "DIP", "ANONYMOUS", "MDDATA",
    "FLOWS_FILES", "APEX_PUBLIC_USER", "SPATIAL_CSW_ADMIN_USR",
    "SPATIAL_WFS_ADMIN_USR", "GGSYS", "SYSBACKUP", "SYSDG", "SYSKM", "SYSRAC",
    "PDBADMIN", "GSMCATUSER", "GSMUSER", "XS$NULL",
)


def _oracle_owner_exclusion():
    "IN-list of quoted Oracle system owners for a WHERE ... NOT IN (...)."
    return ", ".join(escape_string_literal(o) for o in ORACLE_SYSTEM_OWNERS)


def _oracle_owner_filter(owner=None, column="owner"):
    "Restrict to one owner, else exclude Oracle system owners."
    if owner:
        return f"{column} = {escape_string_literal(owner)}"
    return f"{column} NOT IN ({_oracle_owner_exclusion()})"


def list_schemas_query() -> str:
    """Distinct visible, non-system Oracle owners as SCHEMA_NAME."""
    return (
        "(SELECT DISTINCT owner AS SCHEMA_NAME FROM all_objects "
        f"WHERE {_oracle_owner_filter()}) q"
    )


def list_tables_query(owner: str = None) -> str:
    """Oracle tables (estimated NUM_ROWS from the dictionary, never a COUNT(*))."""
    return (
        "(SELECT owner AS SCHEMA_NAME, table_name AS OBJECT_NAME, "
        "num_rows AS ROW_COUNT FROM all_tables "
        f"WHERE {_oracle_owner_filter(owner)}) q"
    )


def list_views_query(owner: str = None) -> str:
    """Oracle views as SCHEMA_NAME / OBJECT_NAME."""
    return (
        "(SELECT owner AS SCHEMA_NAME, view_name AS OBJECT_NAME FROM all_views "
        f"WHERE {_oracle_owner_filter(owner)}) q"
    )


def list_routines_query(owner: str = None) -> str:
    """Oracle routines/packages as SCHEMA_NAME / OBJECT_NAME / OBJECT_TYPE."""
    return (
        "(SELECT owner AS SCHEMA_NAME, object_name AS OBJECT_NAME, "
        "object_type AS OBJECT_TYPE FROM all_objects "
        f"WHERE object_type IN ('PROCEDURE','FUNCTION','PACKAGE','PACKAGE BODY') "
        f"AND {_oracle_owner_filter(owner)}) q"
    )


def table_statistics_query(owner: str = None) -> str:
    """Estimated table statistics from the data dictionary (ROW_COUNT_METHOD=ESTIMATED).

    NUM_ROWS/BLOCKS come from the optimizer dictionary, so they are ESTIMATED
    unless statistics were just gathered; SIZE_MB assumes the common 8 KiB block.
    """
    return (
        "(SELECT owner AS SCHEMA_NAME, table_name AS OBJECT_NAME, "
        "num_rows AS ROW_COUNT, "
        "CAST(NVL(blocks,0) * 8192 / 1048576 AS NUMBER(18,2)) AS SIZE_MB, "
        "'ESTIMATED' AS ROW_COUNT_METHOD FROM all_tables "
        f"WHERE {_oracle_owner_filter(owner)}) q"
    )


def object_source_query(owner: str, object_name: str, object_type: str) -> str:
    """Assemble an Oracle routine/package definition from ALL_SOURCE in line order."""
    o = escape_string_literal(owner)
    n = escape_string_literal(object_name)
    t = escape_string_literal(object_type)
    return (
        "(SELECT line AS LINE_NO, text AS SOURCE_TEXT FROM all_source "
        f"WHERE owner = {o} AND name = {n} AND type = {t} "
        "ORDER BY line) q"
    )


def view_text_query(owner: str, view_name: str) -> str:
    """Oracle view text (LONG column) as DEFINITION_TEXT."""
    o = escape_string_literal(owner)
    v = escape_string_literal(view_name)
    return (
        "(SELECT text AS DEFINITION_TEXT FROM all_views "
        f"WHERE owner = {o} AND view_name = {v}) q"
    )
