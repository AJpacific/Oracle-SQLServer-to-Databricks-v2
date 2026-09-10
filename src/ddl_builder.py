"""
ddl_builder.py - build Databricks (Delta) DDL strings for the target side.

The target of an Oracle migration is still Databricks Delta, so this module is
largely identical to the SQL Server accelerator: it only ever emits Databricks
SQL. Kept here so notebooks import `ddl_builder as ddl` unchanged.

Pure module: no Spark, no dbutils. Fully unit-testable.
"""



from __future__ import annotations

from typing import List, Tuple

try:
    from src.identifiers import (
        quote_databricks,
        databricks_fqn
    )
except ModuleNotFoundError:
    from identifiers import (
        quote_databricks,
        databricks_fqn
    )

def build_create_schema(catalog: str, schema: str, comment: str = "") -> str:
    stmt = (
        f"CREATE SCHEMA IF NOT EXISTS "
        f"{quote_databricks(catalog)}.{quote_databricks(schema)}"
    )

    if comment:
        safe = comment.replace("'", "''")
        stmt += f" COMMENT '{safe}'"

    return stmt


def build_create_table(
    catalog: str,
    schema: str,
    table: str,
    columns: List[Tuple[str, str, bool]]
) -> str:
    """
    Build a CREATE TABLE IF NOT EXISTS ... USING DELTA statement.

    columns: ordered list of
    (column_name, databricks_delta_type, is_nullable)
    """

    lines = []

    for name, dtype, nullable in columns:
        null_sql = "" if nullable else " NOT NULL"
        lines.append(
            f"  {quote_databricks(name)} {dtype}{null_sql}"
        )

    body = ",\n".join(lines)

    return (
        f"CREATE TABLE IF NOT EXISTS "
        f"{databricks_fqn(catalog, schema, table)} "
        f"(\n{body}\n) USING DELTA"
    )


def build_drop_table(
    catalog: str,
    schema: str,
    table: str
) -> str:

    return (
        f"DROP TABLE IF EXISTS "
        f"{databricks_fqn(catalog, schema, table)}"
    )


def build_truncate_table(
    catalog: str,
    schema: str,
    table: str
) -> str:

    return (
        f"TRUNCATE TABLE "
        f"{databricks_fqn(catalog, schema, table)}"
    )


# Alias kept to match SQL Server accelerator naming
def build_truncate(
    catalog: str,
    schema: str,
    table: str
) -> str:

    return build_truncate_table(
        catalog,
        schema,
        table
    )


def build_create_like(
    catalog: str,
    schema: str,
    stage_table: str,
    target_table: str
) -> str:
    """
    Build a staging table with the same structure as the target.
    """

    stage = databricks_fqn(
        catalog,
        schema,
        stage_table
    )

    target = databricks_fqn(
        catalog,
        schema,
        target_table
    )

    return (
        f"CREATE TABLE IF NOT EXISTS {stage} "
        f"LIKE {target}"
    )


def build_merge_condition(primary_keys) -> str:
    """
    Build ON clause for a MERGE using primary keys.
    """

    keys = [k for k in (primary_keys or []) if k]

    if not keys:
        raise ValueError(
            "MERGE requires at least one primary key column"
        )

    parts = []

    for k in keys:
        col = quote_databricks(k)
        parts.append(
            f"t.{col} = s.{col}"
        )

    return " AND ".join(parts)


def build_merge_sql(
    catalog: str,
    schema: str,
    target_table: str,
    stage_table: str,
    primary_keys,
    delete_unmatched: bool = False
) -> str:
    """
    Build full MERGE SQL for PRIMARY_KEY / HYBRID load strategies.
    """

    target = databricks_fqn(
        catalog,
        schema,
        target_table
    )

    stage = databricks_fqn(
        catalog,
        schema,
        stage_table
    )

    on_clause = build_merge_condition(
        primary_keys
    )

    sql = (
        f"MERGE INTO {target} AS t\n"
        f"USING {stage} AS s\n"
        f"ON {on_clause}\n"
        f"WHEN MATCHED THEN UPDATE SET *\n"
        f"WHEN NOT MATCHED THEN INSERT *"
    )

    if delete_unmatched:
        sql += (
            "\nWHEN NOT MATCHED BY SOURCE "
            "THEN DELETE"
        )

    return sql


def build_count_sql(
    catalog: str,
    schema: str,
    table: str
) -> str:

    return (
        f"SELECT COUNT(*) AS row_count "
        f"FROM {databricks_fqn(catalog, schema, table)}"
    )
