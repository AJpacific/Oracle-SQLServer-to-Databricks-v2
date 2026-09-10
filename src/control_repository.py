"""
control_repository.py - thin repository over the control/audit Delta tables.

Wraps the common read/update patterns the notebooks need so they don't sprinkle
raw SQL everywhere. The Spark session is injected, so the *query building* parts
stay pure and testable; only the methods that actually touch Spark require a
live session.

new_run_id() is pure and unit-testable.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone

try:
    from src.identifiers import (
        quote_databricks,
        escape_string_literal
    )
except ModuleNotFoundError:
    from identifiers import (
        quote_databricks,
        escape_string_literal
    )


def new_run_id(prefix: str = "run") -> str:
    """Time-ordered, collision-resistant run id."""
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{prefix}_{ts}_{uuid.uuid4().hex[:8]}"


def _now_utc():
    return datetime.now(timezone.utc)


class ControlRepository:
    def __init__(self, spark, catalog: str, control_schema: str):
        self.spark = spark
        self.catalog = catalog
        self.control_schema = control_schema

    # ------------------------------------------------------ name helpers (pure)

    def ctrl(self, table: str) -> str:
        return (
            f"{quote_databricks(self.catalog)}."
            f"{quote_databricks(self.control_schema)}."
            f"{quote_databricks(table)}"
        )

    # ------------------------------------------------------ reads (need Spark)

    def active_tables(self, decision: str = None):
        """Return active source tables."""
        sql = (
            f"SELECT * FROM {self.ctrl('source_table_control')} "
            f"WHERE is_active = true"
        )

        if decision:
            sql += (
                f" AND table_decision = "
                f"{escape_string_literal(decision)}"
            )

        return self.spark.sql(sql)

    def get_watermark(self, source_table_id: str):
        """Return the last committed watermark value for one source table id."""
        sql = (
            f"SELECT last_watermark_value "
            f"FROM {self.ctrl('source_table_control')} "
            f"WHERE source_table_id = "
            f"{escape_string_literal(source_table_id)}"
        )

        rows = self.spark.sql(sql).collect()

        return (
            rows[0]["last_watermark_value"]
            if rows
            else None
        )

    def count_target(self, target_schema: str, target_table: str) -> int:
        """Row count of a target Delta table."""
        fqn = (
            f"{quote_databricks(self.catalog)}."
            f"{quote_databricks(target_schema)}."
            f"{quote_databricks(target_table)}"
        )

        return (
            self.spark.sql(
                f"SELECT COUNT(*) AS c FROM {fqn}"
            )
            .collect()[0]["c"]
        )

    def get_control_row(self, source_table_id: str):
        sql = (
            f"SELECT * FROM {self.ctrl('source_table_control')} "
            f"WHERE source_table_id = "
            f"{escape_string_literal(source_table_id)}"
        )

        rows = self.spark.sql(sql).collect()

        return rows[0] if rows else None

    # ---------------------------------------------- writes / merges (need Spark)

    def update_control(
        self,
        source_table_id: str,
        fields: dict
    ):
        """Update selected columns of the source_table_control row identified by
        its source-qualified ``source_table_id``.

        A control row is never updated by ``source_schema + source_table`` alone,
        so two sources that share a schema.table can never overwrite each other.
        """

        if not source_table_id:
            raise ValueError(
                "update_control requires a source_table_id (source-qualified "
                "identity); schema+table alone is not accepted")

        assignments = []

        for k, v in fields.items():
            assignments.append(
                f"{quote_databricks(k)} = {self._render_value(v)}"
            )

        assignments.append(
            "`updated_ts` = current_timestamp()"
        )

        set_clause = ", ".join(assignments)

        sql = (
            f"UPDATE {self.ctrl('source_table_control')} "
            f"SET {set_clause} "
            f"WHERE source_table_id = "
            f"{escape_string_literal(source_table_id)}"
        )

        self.spark.sql(sql)

    def update_control_by_identity(self, source_system, source_server,
                                   source_database, source_schema, source_table,
                                   fields: dict):
        """Update a control row matched by its full 5-part source identity.

        Uses null-safe matching for source_server / source_database so legacy
        Oracle rows (NULL server/database) match correctly. Primarily used to set
        source_table_id on rows registered before the id existed; steady-state
        updates use :meth:`update_control` keyed by source_table_id.
        """
        assignments = [
            f"{quote_databricks(k)} = {self._render_value(v)}"
            for k, v in fields.items()
        ]
        assignments.append("`updated_ts` = current_timestamp()")
        set_clause = ", ".join(assignments)
        where = (
            f"source_schema = {escape_string_literal(source_schema)} "
            f"AND source_table = {escape_string_literal(source_table)} "
            f"AND (({self._null_or_eq('source_system', source_system)})) "
            f"AND (({self._null_or_eq('source_server', source_server)})) "
            f"AND (({self._null_or_eq('source_database', source_database)}))"
        )
        self.spark.sql(
            f"UPDATE {self.ctrl('source_table_control')} "
            f"SET {set_clause} WHERE {where}"
        )

    @staticmethod
    def _null_or_eq(column, value):
        """Null-safe equality fragment: matches when both are NULL or equal."""
        lit = escape_string_literal(value)
        if value is None:
            return f"{column} IS NULL"
        return f"{column} = {lit}"

    def log_table_run(self, fields: dict):
        """
        Append one row to table_run_log (carries source identity).
        """

        from pyspark.sql.types import (
            StructType,
            StructField,
            StringType,
            LongType,
            TimestampType,
        )

        def _long(v):
            return int(v) if v is not None else None

        schema = StructType([
            StructField("run_id", StringType(), True),
            StructField("source_table_id", StringType(), True),
            StructField("source_system", StringType(), True),
            StructField("source_server", StringType(), True),
            StructField("source_database", StringType(), True),
            StructField("source_schema", StringType(), True),
            StructField("source_table", StringType(), True),
            StructField("operation", StringType(), True),
            StructField("target_full_name", StringType(), True),
            StructField("source_row_count", LongType(), True),
            StructField("target_row_count", LongType(), True),
            StructField("status", StringType(), True),
            StructField("error_message", StringType(), True),
            StructField("started_ts", TimestampType(), True),
            StructField("ended_ts", TimestampType(), True),
        ])

        row = (
            fields.get("run_id"),
            fields.get("source_table_id"),
            fields.get("source_system"),
            fields.get("source_server"),
            fields.get("source_database"),
            fields.get("source_schema"),
            fields.get("source_table"),
            fields.get("operation"),
            fields.get("target_full_name"),
            _long(fields.get("source_row_count")),
            _long(fields.get("target_row_count")),
            fields.get("status"),
            fields.get("error_message"),
            fields.get("started_ts"),
            fields.get("ended_ts"),
        )

        df = self.spark.createDataFrame([row], schema)

        df.write.format("delta").mode("append").option(
            "mergeSchema", "true").saveAsTable(
            self._plain(self.ctrl("table_run_log"))
        )

    def log_job_run(
        self,
        run_id,
        job_name,
        status,
        message=""
    ):
        self.spark.sql(f"""
            INSERT INTO {self.ctrl('job_run_log')}
            VALUES (
                {escape_string_literal(run_id)},
                {escape_string_literal(job_name)},
                {escape_string_literal(status)},
                current_timestamp(),
                current_timestamp(),
                {escape_string_literal(message)}
            )
        """)

    def commit_watermark(
        self,
        source_table_id,
        watermark_value,
        run_id
    ):
        self.update_control(
            source_table_id,
            {
                "last_watermark_value": watermark_value,
                "last_successful_run_id": run_id,
            }
        )

    # ---------------------------------------------------------------- helpers

    @staticmethod
    def _render_value(v) -> str:
        """
        Convert Python values into valid Databricks SQL literals.
        """

        if v is None:
            return "NULL"

        if isinstance(v, bool):
            return "true" if v else "false"

        if isinstance(v, (int, float)):
            return str(v)

        # Support ARRAY<STRING>
        if isinstance(v, (list, tuple)):
            if not v:
                return "CAST(array() AS ARRAY<STRING>)"

            values = ", ".join(
                escape_string_literal(str(x))
                for x in v
            )

            return f"array({values})"

        return escape_string_literal(v)

    @staticmethod
    def _plain(fqn_with_backticks: str) -> str:
        """
        saveAsTable wants an unquoted dotted name.
        """
        return fqn_with_backticks.replace("`", "")