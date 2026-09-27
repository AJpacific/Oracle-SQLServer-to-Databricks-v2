"""sql_artifact_scope.py - Pure Python candidate scoping and object mapping functions.

Extracts candidate selection where-clause generation and object map resolution
for the SQL Artifact Migration workflow.
No Spark, no dbutils; fully unit-testable.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Tuple

try:
    from src.identifiers import quote_databricks, escape_string_literal
except ModuleNotFoundError:
    from identifiers import quote_databricks, escape_string_literal


def build_candidate_where_clauses(
    alias: str = "",
    run_id: str = "",
    connection_id: str = "",
    only_assessment_id: str = "",
    only_source_system: str = "",
    object_types: Optional[List[str]] = None,
    allow_global_scope: bool = False,
    source_database: str = "",
) -> List[str]:
    """Build SQL WHERE clauses for candidate selection from sql_object_assessment.

    Qualifies every column reference with {alias}. (or unqualified if alias is empty),
    ensuring function calls are NOT qualified as alias.func(col) but rather
    func(alias.col).
    """
    prefix = f"{alias.strip()}." if alias and alias.strip() else ""

    clauses: List[str] = [
        f"upper(trim({prefix}object_type)) <> 'TABLE'",
        f"{prefix}source_definition IS NOT NULL AND trim({prefix}source_definition) <> ''",
        f"{prefix}connection_id IS NOT NULL AND trim({prefix}connection_id) <> ''",
        f"{prefix}source_schema IS NOT NULL AND trim({prefix}source_schema) <> ''",
        f"{prefix}object_name IS NOT NULL AND trim({prefix}object_name) <> ''",
        f"{prefix}source_system IS NOT NULL AND trim({prefix}source_system) <> ''",
    ]

    if object_types:
        cleaned_types = [t.strip().upper() for t in object_types if t and t.strip()]
        if cleaned_types:
            in_list = ", ".join(f"'{t}'" for t in cleaned_types)
            clauses.append(f"upper(trim(replace({prefix}object_type, ' ', '_'))) IN ({in_list})")

    if not allow_global_scope:
        if not run_id or not run_id.strip():
            raise ValueError("Blank run_id rejected when allow_global_scope=false: current run_id scope is required")
        clauses.append(f"{prefix}run_id = {escape_string_literal(run_id.strip())}")

    if connection_id and connection_id.strip():
        clauses.append(f"{prefix}connection_id = {escape_string_literal(connection_id.strip())}")

    if only_assessment_id and only_assessment_id.strip():
        clauses.append(f"{prefix}assessment_id = {escape_string_literal(only_assessment_id.strip())}")

    if only_source_system and only_source_system.strip():
        canon_sys = only_source_system.strip().lower()
        clauses.append(f"lower(trim({prefix}source_system)) = {escape_string_literal(canon_sys)}")

    if source_database and source_database.strip():
        clauses.append(f"coalesce({prefix}source_database, '') = {escape_string_literal(source_database.strip())}")

    return clauses


def build_object_map(
    control_rows: List[Dict[str, Any]],
    connection_id: str,
    source_database: str = "",
) -> Dict[str, str]:
    """Build source-to-target object map from source_table_control rows.

    Maps source (schema.table and database.schema.table, case-insensitive match)
    -> quoted target_catalog.target_schema.target_table
    using rows where connection_id matches, is_active is true, and
    initial_load_completed is true.

    When source_database is given, blank-database rows are excluded.
    If a two-part key maps to two different targets, that key is dropped.
    """
    conn_req = str(connection_id or "").strip()
    db_req = str(source_database or "").strip()

    valid_entries: List[Tuple[str, str, str, str]] = []
    two_part_targets: Dict[str, set] = {}

    for r in control_rows:
        row_conn = str(r.get("connection_id") or "").strip()
        if row_conn != conn_req:
            continue

        # Check is_active
        is_active = r.get("is_active")
        if is_active is not True and str(is_active).strip().lower() != "true":
            continue

        # Check initial_load_completed
        init_completed = r.get("initial_load_completed")
        if init_completed is not True and str(init_completed).strip().lower() != "true":
            continue

        row_db = str(r.get("source_database") or "").strip()
        # when source_database is given, exclude blank-database rows
        if db_req:
            if not row_db or row_db.lower() != db_req.lower():
                continue

        src_sch = str(r.get("source_schema") or "").strip()
        src_tbl = str(r.get("source_table") or "").strip()
        tgt_cat = str(r.get("target_catalog") or "").strip()
        tgt_sch = str(r.get("target_schema") or "").strip()
        tgt_tbl = str(r.get("target_table") or "").strip()

        if not src_sch or not src_tbl or not tgt_cat or not tgt_sch or not tgt_tbl:
            continue

        quoted_target = f"{quote_databricks(tgt_cat)}.{quote_databricks(tgt_sch)}.{quote_databricks(tgt_tbl)}"
        k2_lower = f"{src_sch}.{src_tbl}".lower()
        two_part_targets.setdefault(k2_lower, set()).add(quoted_target)
        valid_entries.append((row_db, src_sch, src_tbl, quoted_target))

    mapping: Dict[str, str] = {}
    for row_db, src_sch, src_tbl, quoted_target in valid_entries:
        k2 = f"{src_sch}.{src_tbl}"
        k2_lower = k2.lower()

        # If a two-part key maps to two different targets, drop that key
        if len(two_part_targets.get(k2_lower, set())) == 1:
            mapping[k2] = quoted_target
            mapping[k2_lower] = quoted_target

        if row_db:
            k3 = f"{row_db}.{src_sch}.{src_tbl}"
            mapping[k3] = quoted_target
            mapping[k3.lower()] = quoted_target

    return mapping


def validate_max_artifacts(max_artifacts: Any) -> Optional[int]:
    """Validate max_artifacts parameter.

    Returns None if blank or None, int value >= 0 if valid.
    Raises ValueError if invalid or negative.
    """
    if max_artifacts is None:
        return None
    s = str(max_artifacts).strip()
    if not s:
        return None
    try:
        val = int(s)
    except (ValueError, TypeError) as e:
        raise ValueError(f"Invalid max_artifacts: {max_artifacts!r}") from e
    if val < 0:
        raise ValueError(f"Negative max_artifacts: {val}")
    return val


def build_candidate_query(
    source_assessment_fqn: str,
    sql_object_assessment_fqn: str,
    connection_id: Optional[str] = None,
    source_database: Optional[str] = None,
    max_artifacts: Optional[int] = None,
) -> str:
    """Build the authoritative candidate query shared between NB18 and NB22.

    Selection-driven only:
    - Filters strictly non-table rows in source_assessment where is_selected = true.
    - Uses the latest assessment per connection (per connection + source_database for SQL Server).
    - Joined to matching sql_object_assessment definition on connection_id, source_database,
      source_schema, object_type, object_name and the same assessment run (run_id).
    - Normalizes object_type the same way on both sides (e.g. PACKAGE BODY = PACKAGE_BODY).
    - Supports optional filters: connection_id, source_database.
    - Validates max_artifacts (raises if negative or invalid).
    """
    validated_limit = validate_max_artifacts(max_artifacts)

    where_runs = []
    where_sa = []

    if connection_id and str(connection_id).strip():
        cid_esc = escape_string_literal(str(connection_id).strip())
        where_runs.append(f"connection_id = {cid_esc}")
        where_sa.append(f"sa.connection_id = {cid_esc}")

    if source_database and str(source_database).strip():
        sdb_esc = escape_string_literal(str(source_database).strip())
        where_runs.append(f"coalesce(source_database, '') = {sdb_esc}")
        where_sa.append(f"coalesce(sa.source_database, '') = {sdb_esc}")

    where_runs_sql = f"AND {' AND '.join(where_runs)}" if where_runs else ""
    where_sa_sql = f"AND {' AND '.join(where_sa)}" if where_sa else ""

    limit_sql = f"LIMIT {validated_limit}" if (validated_limit is not None and validated_limit > 0) else ""

    query = f"""
    WITH assessment_runs AS (
        SELECT
            connection_id,
            CASE
                WHEN lower(trim(coalesce(source_system, ''))) IN ('sqlserver', 'sql_server', 'mssql')
                THEN coalesce(source_database, '')
                ELSE ''
            END AS partition_db,
            run_id,
            MAX(captured_ts) AS latest_captured_ts
        FROM {source_assessment_fqn}
        WHERE run_id IS NOT NULL AND trim(run_id) <> ''
        {where_runs_sql}
        GROUP BY
            connection_id,
            CASE
                WHEN lower(trim(coalesce(source_system, ''))) IN ('sqlserver', 'sql_server', 'mssql')
                THEN coalesce(source_database, '')
                ELSE ''
            END,
            run_id
    ),
    ranked_runs AS (
        SELECT
            connection_id,
            partition_db,
            run_id,
            ROW_NUMBER() OVER (
                PARTITION BY connection_id, partition_db
                ORDER BY latest_captured_ts DESC NULLS LAST, run_id DESC
            ) AS run_rn
        FROM assessment_runs
    ),
    latest_runs AS (
        SELECT connection_id, partition_db, run_id
        FROM ranked_runs
        WHERE run_rn = 1
    ),
    selected_objects AS (
        SELECT
            sa.connection_id,
            sa.source_system,
            sa.source_database,
            sa.source_schema,
            sa.object_name,
            upper(trim(replace(sa.object_type, ' ', '_'))) AS object_type,
            sa.run_id AS assessment_run_id,
            sa.assessment_id,
            sa.is_selected,
            sa.captured_ts,
            ROW_NUMBER() OVER (
                PARTITION BY sa.connection_id, coalesce(sa.source_database, ''),
                             sa.source_schema, upper(trim(replace(sa.object_type, ' ', '_'))), sa.object_name
                ORDER BY sa.captured_ts DESC NULLS LAST, sa.assessment_id DESC
            ) AS dedupe_rn
        FROM {source_assessment_fqn} sa
        INNER JOIN latest_runs lr
           ON sa.connection_id = lr.connection_id
          AND (CASE WHEN lower(trim(coalesce(sa.source_system, ''))) IN ('sqlserver', 'sql_server', 'mssql') THEN coalesce(sa.source_database, '') ELSE '' END) = lr.partition_db
          AND sa.run_id = lr.run_id
        WHERE upper(trim(replace(sa.object_type, ' ', '_'))) <> 'TABLE'
          AND sa.is_selected = true
          {where_sa_sql}
    ),
    deduped_selected AS (
        SELECT * FROM selected_objects WHERE dedupe_rn = 1
    ),
    soa_ranked AS (
        SELECT
            run_id,
            connection_id,
            coalesce(source_database, '') AS source_database,
            source_schema,
            upper(trim(replace(object_type, ' ', '_'))) AS object_type,
            object_name,
            source_definition,
            error_message AS definition_error_message,
            ROW_NUMBER() OVER (
                PARTITION BY run_id, connection_id, coalesce(source_database, ''),
                             source_schema, upper(trim(replace(object_type, ' ', '_'))), object_name
                ORDER BY captured_ts DESC NULLS LAST, updated_ts DESC NULLS LAST
            ) AS soa_rn
        FROM {sql_object_assessment_fqn}
    )
    SELECT
        ds.connection_id,
        ds.source_system,
        ds.source_database,
        ds.source_schema,
        ds.object_name,
        ds.object_type,
        ds.assessment_run_id,
        ds.assessment_id,
        ds.is_selected,
        ds.captured_ts,
        soa.source_definition,
        soa.definition_error_message
    FROM deduped_selected ds
    LEFT JOIN soa_ranked soa
       ON ds.assessment_run_id = soa.run_id
      AND ds.connection_id = soa.connection_id
      AND coalesce(ds.source_database, '') = soa.source_database
      AND ds.source_schema = soa.source_schema
      AND ds.object_type = soa.object_type
      AND ds.object_name = soa.object_name
      AND soa.soa_rn = 1
    ORDER BY ds.connection_id, coalesce(ds.source_database, ''), ds.source_schema, ds.object_type, ds.object_name
    {limit_sql}
    """
    return query


def build_selected_candidate_query(
    source_assessment_fqn: str,
    connection_id: Optional[str] = None,
    source_database: Optional[str] = None,
    max_artifacts: Optional[int] = None,
) -> str:
    """Build candidate selection query for selected VIEW and PROCEDURE objects only.

    Candidates must come only from source_assessment where:
    - is_selected = true
    - normalized object_type IN ('VIEW', 'PROCEDURE')
    - row belongs to latest Assessment for its connection and database
    - optional connection_id filter matches, when provided
    - optional source_database filter matches, when provided
    - validates max_artifacts: blank or zero = unlimited, positive int = limit, negative/nonnumeric raises ValueError
    - deterministic ordering before applying max_artifacts
    """
    validated_limit = validate_max_artifacts(max_artifacts)

    where_runs = []
    where_sa = []

    if connection_id and str(connection_id).strip():
        cid_esc = escape_string_literal(str(connection_id).strip())
        where_runs.append(f"connection_id = {cid_esc}")
        where_sa.append(f"sa.connection_id = {cid_esc}")

    if source_database and str(source_database).strip():
        sdb_esc = escape_string_literal(str(source_database).strip())
        where_runs.append(f"coalesce(source_database, '') = {sdb_esc}")
        where_sa.append(f"coalesce(sa.source_database, '') = {sdb_esc}")

    where_runs_sql = f"AND {' AND '.join(where_runs)}" if where_runs else ""
    where_sa_sql = f"AND {' AND '.join(where_sa)}" if where_sa else ""

    limit_sql = f"LIMIT {validated_limit}" if (validated_limit is not None and validated_limit > 0) else ""

    query = f"""
    WITH assessment_runs AS (
        SELECT
            connection_id,
            CASE
                WHEN lower(trim(coalesce(source_system, ''))) IN ('sqlserver', 'sql_server', 'mssql')
                THEN coalesce(source_database, '')
                ELSE ''
            END AS partition_db,
            run_id,
            MAX(captured_ts) AS latest_captured_ts
        FROM {source_assessment_fqn}
        WHERE run_id IS NOT NULL AND trim(run_id) <> ''
        {where_runs_sql}
        GROUP BY
            connection_id,
            CASE
                WHEN lower(trim(coalesce(source_system, ''))) IN ('sqlserver', 'sql_server', 'mssql')
                THEN coalesce(source_database, '')
                ELSE ''
            END,
            run_id
    ),
    ranked_runs AS (
        SELECT
            connection_id,
            partition_db,
            run_id,
            ROW_NUMBER() OVER (
                PARTITION BY connection_id, partition_db
                ORDER BY latest_captured_ts DESC NULLS LAST, run_id DESC
            ) AS run_rn
        FROM assessment_runs
    ),
    latest_runs AS (
        SELECT connection_id, partition_db, run_id
        FROM ranked_runs
        WHERE run_rn = 1
    ),
    selected_objects AS (
        SELECT
            sa.assessment_id,
            sa.run_id,
            sa.connection_id,
            sa.source_system,
            sa.source_database,
            sa.source_schema,
            sa.object_name,
            upper(trim(replace(sa.object_type, ' ', '_'))) AS object_type,
            sa.captured_ts,
            ROW_NUMBER() OVER (
                PARTITION BY sa.connection_id, coalesce(sa.source_database, ''),
                             sa.source_schema, upper(trim(replace(sa.object_type, ' ', '_'))), sa.object_name
                ORDER BY sa.captured_ts DESC NULLS LAST, sa.assessment_id DESC
            ) AS dedupe_rn
        FROM {source_assessment_fqn} sa
        INNER JOIN latest_runs lr
           ON sa.connection_id = lr.connection_id
          AND (CASE WHEN lower(trim(coalesce(sa.source_system, ''))) IN ('sqlserver', 'sql_server', 'mssql') THEN coalesce(sa.source_database, '') ELSE '' END) = lr.partition_db
          AND sa.run_id = lr.run_id
        WHERE upper(trim(replace(sa.object_type, ' ', '_'))) IN ('VIEW', 'PROCEDURE')
          AND sa.is_selected = true
          {where_sa_sql}
    )
    SELECT
        assessment_id,
        run_id,
        connection_id,
        source_system,
        source_database,
        source_schema,
        object_name,
        object_type,
        captured_ts
    FROM selected_objects
    WHERE dedupe_rn = 1
    ORDER BY connection_id, coalesce(source_database, ''), source_schema, object_type, object_name
    {limit_sql}
    """
    return query

