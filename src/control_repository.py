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

try:
    from src.source_identity import (
        require_source_system, SOURCES_REQUIRING_DATABASE)
except ModuleNotFoundError:
    from source_identity import (
        require_source_system, SOURCES_REQUIRING_DATABASE)

try:
    from src.failure_classifier import sanitize_message as sanitize_error_message
except ModuleNotFoundError:
    from failure_classifier import sanitize_message as sanitize_error_message


# Keys that must never reach a control/audit table. log_table_run() writes an
# explicit column allowlist, so these are structurally excluded; the constant
# makes that intent reviewable and testable.
SECRET_FIELD_KEYS = frozenset({
    "password", "pwd", "token", "access_token", "user", "username",
    "secret", "secret_value", "client_secret", "jdbc_url", "url",
    "webhook_url", "webhook", "sas",
})


# Only these control-table columns may carry free-form text from an exception,
# so only these are sanitized. Identifiers, table/schema/database names, status
# values, and metrics are never rewritten.
SANITIZED_CONTROL_FIELDS = frozenset({"error_message", "etl_error_message"})


# Ownership and identity version fields are immutable under generic updates.
# Only dedicated migration or provisioning flows may set them.
PROTECTED_CONTROL_IDENTITY_FIELDS = frozenset({
    "connection_id",
    "source_table_id",
    "source_identity_version",
    "legacy_source_table_id",
})


def require_connection_id(value, context="operation") -> str:
    """Return a trimmed connection ID or raise with a safe context message."""
    connection_id = str(value or "").strip()
    if not connection_id:
        raise ValueError(f"{context} requires connection_id")
    return connection_id


def _to_long(value):
    """Coerce to int for a BIGINT column; null stays null."""
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _to_int(value):
    """Coerce to int for an INT column; null stays null."""
    return _to_long(value)


def _to_bool(value):
    """Coerce to bool for a BOOLEAN column; null stays null (not False)."""
    if value is None:
        return None
    if isinstance(value, str):
        token = value.strip().lower()
        if token in ("true", "yes", "1"):
            return True
        if token in ("false", "no", "0"):
            return False
        return None
    return bool(value)


# Every column table_run_log supports, with its Spark type token. Declared here
# (not inline) so the audit contract is reviewable and unit-testable without a
# Spark session. Order is the write order.
TABLE_RUN_LOG_COLUMNS = (
    ("run_id", "string"),
    ("source_table_id", "string"),
    ("connection_id", "string"),
    ("source_system", "string"),
    ("source_server", "string"),
    ("source_database", "string"),
    ("source_schema", "string"),
    ("source_table", "string"),
    ("operation", "string"),
    ("target_full_name", "string"),
    ("source_row_count", "bigint"),
    ("target_row_count", "bigint"),
    ("status", "string"),
    ("error_message", "string"),
    ("attempt_number", "int"),
    ("failure_stage", "string"),
    ("error_category", "string"),
    ("retry_eligible", "boolean"),
    ("retry_status", "string"),
    ("parent_run_id", "string"),
    ("lower_watermark", "string"),
    ("upper_watermark", "string"),
    ("extracted_row_count", "bigint"),
    ("staged_row_count", "bigint"),
    ("applied_row_count", "bigint"),
    ("rejected_row_count", "bigint"),
    ("started_ts", "timestamp"),
    ("ended_ts", "timestamp"),
)

_COERCE = {"bigint": _to_long, "int": _to_int, "boolean": _to_bool}


def build_table_run_row(fields: dict) -> tuple:
    """Coerce a table-run field dict into the ordered audit row (pure).

    Only the declared columns are read, so a secret-bearing key can never reach
    the audit table. Values the caller did not supply stay null; nothing is
    derived. ``error_message`` is sanitized as defense in depth.
    """
    fields = fields or {}
    values = []
    for name, type_token in TABLE_RUN_LOG_COLUMNS:
        value = fields.get(name)
        if name == "error_message":
            value = sanitize_error_message(value) if value is not None else None
        elif type_token in _COERCE:
            value = _COERCE[type_token](value)
        values.append(value)
    return tuple(values)


def new_run_id(prefix: str = "run") -> str:
    """Time-ordered, collision-resistant run id."""
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{prefix}_{ts}_{uuid.uuid4().hex[:8]}"


def normalize_connection_input(raw: dict) -> dict:
    """Validate + normalize non-secret connection metadata (pure, no Spark).

    Enforces required fields, normalizes ``source_system`` (raising on an
    unknown system), requires ``source_database`` for SQL Server, and defaults
    ``trust_server_certificate`` to False. Never accepts or returns a username,
    password, token, or credential-bearing URL.
    """
    raw = raw or {}
    connection_id = (raw.get("connection_id") or "").strip()
    connection_name = (raw.get("connection_name") or "").strip()
    secret_scope = (raw.get("secret_scope") or "").strip()
    if not connection_id:
        raise ValueError("connection_id is required")
    if not connection_name:
        raise ValueError("connection_name is required")
    if not secret_scope:
        raise ValueError("secret_scope is required")
    source_system = require_source_system(
        raw.get("source_system"), "registered connection")
    source_server = (raw.get("source_server") or "").strip() or None
    source_database = (raw.get("source_database") or "").strip() or None
    if source_system in SOURCES_REQUIRING_DATABASE and not source_database:
        raise ValueError(
            f"{source_system} connections require source_database")
    return {
        "connection_id": connection_id,
        "connection_name": connection_name,
        "source_system": source_system,
        "source_server": source_server,
        "source_database": source_database,
        "secret_scope": secret_scope,
        "trust_server_certificate": bool(raw.get("trust_server_certificate")),
    }


def assert_source_system_match(row_system, connection_system) -> None:
    """Raise when a control-row source_system conflicts with its connection's."""
    row_token = require_source_system(row_system, "source row")
    connection_token = require_source_system(
        connection_system, "registered connection")
    if row_token != connection_token:
        raise ValueError(
            f"source_system conflict: row={row_system!r} "
            f"connection={connection_system!r}")


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

    def active_tables(self, connection_id: str = None, decision: str = None,
                      include_onboarding: bool = False):
        """Return active source tables.

        When ``connection_id`` is given the result is scoped to that connection;
        when omitted the behavior is unchanged (every active table), so existing
        callers keep working.
        When ``include_onboarding=True``, includes unactivated onboarding registrations
        (`current_status IN ('REGISTERED', 'INVENTORIED', 'PROVISIONED')`).
        """
        if include_onboarding:
            predicate = "(is_active = true OR current_status IN ('REGISTERED', 'INVENTORIED', 'PROVISIONED'))"
        else:
            predicate = "is_active = true"

        sql = (
            f"SELECT * FROM {self.ctrl('source_table_control')} "
            f"WHERE {predicate}"
        )

        if connection_id:
            sql += (
                f" AND connection_id = "
                f"{escape_string_literal(connection_id)}"
            )

        if decision:
            sql += (
                f" AND table_decision = "
                f"{escape_string_literal(decision)}"
            )

        return self.spark.sql(sql)

    def active_tables_for_connection(self, connection_id: str,
                                     decision: str = None,
                                     include_onboarding: bool = False):
        """Return registrations owned by exactly one connection.

        By default, returns operational active tables (`is_active = true`).
        When ``include_onboarding=True``, includes unactivated onboarding registrations
        (`current_status IN ('REGISTERED', 'INVENTORIED', 'PROVISIONED')`).
        """
        connection_id = require_connection_id(
            connection_id, "active_tables_for_connection")
        return self.active_tables(
            connection_id=connection_id,
            decision=decision,
            include_onboarding=include_onboarding,
        )

    # ---------------------------------------------------- connection registry

    def get_connection(self, connection_id: str):
        """Return one source_connection row, or None; reject duplicate IDs."""
        connection_id = require_connection_id(connection_id, "get_connection")
        rows = self.spark.sql(
            f"SELECT * FROM {self.ctrl('source_connection')} "
            f"WHERE connection_id = {escape_string_literal(connection_id)}"
        ).collect()
        if len(rows) > 1:
            raise ValueError(
                f"connection_id {connection_id!r} resolves to {len(rows)} "
                "source_connection rows; expected at most one")
        return rows[0] if rows else None

    def active_connections(self, connection_ids=None):
        """Return active source_connection rows, optionally filtered to a list."""
        sql = (
            f"SELECT * FROM {self.ctrl('source_connection')} "
            f"WHERE is_active = true"
        )
        if connection_ids:
            in_list = ", ".join(escape_string_literal(c) for c in connection_ids)
            sql += f" AND connection_id IN ({in_list})"
        return self.spark.sql(sql)

    def valid_active_connections(self, connection_ids=None):
        """Return operational connections with VALID status and a scope."""
        sql = (
            f"SELECT * FROM {self.ctrl('source_connection')} "
            "WHERE is_active = true "
            "AND connection_status = 'VALID' "
            "AND secret_scope IS NOT NULL AND trim(secret_scope) <> ''"
        )
        if connection_ids:
            normalized = [
                require_connection_id(value, "valid_active_connections")
                for value in connection_ids
            ]
            in_list = ", ".join(
                escape_string_literal(value) for value in normalized)
            sql += f" AND connection_id IN ({in_list})"
        return self.spark.sql(sql)

    def upsert_connection(self, connection: dict):
        """Insert or update one source_connection row by connection_id.

        Only non-secret metadata is stored. A username, password, token, or a
        credential-bearing JDBC URL must never be passed here.
        """
        connection = dict(connection)
        connection_id = require_connection_id(
            connection.get("connection_id"), "upsert_connection")
        connection["connection_id"] = connection_id
        connection["source_system"] = require_source_system(
            connection.get("source_system"), "registered connection")
        if connection.get("error_message") is not None:
            connection["error_message"] = sanitize_error_message(
                connection["error_message"])
        writable = (
            "connection_name", "source_system", "source_server",
            "source_database", "secret_scope", "trust_server_certificate",
            "connection_status", "is_active", "error_message",
            "last_validated_ts",
        )
        existing = self.get_connection(connection_id)
        if existing is not None:
            prior = existing.asDict() if hasattr(existing, "asDict") else dict(existing)
            prior_system = prior.get("source_system")
            if prior_system:
                assert_source_system_match(
                    connection["source_system"], prior_system)
            endpoint_fields = ("source_server", "source_database")
            endpoint_changed = any(
                str(connection.get(f) or "").strip().lower() != str(prior.get(f) or "").strip().lower()
                for f in endpoint_fields
                if f in connection and f in prior
            )
            if endpoint_changed:
                dependent_count = self.spark.sql(
                    f"SELECT count(*) AS c FROM {self.ctrl('source_table_control')} "
                    f"WHERE connection_id = {escape_string_literal(connection_id)}"
                ).collect()[0]["c"]
                if dependent_count > 0:
                    raise ValueError(
                        f"Cannot change material endpoint ('source_server' or 'source_database') for "
                        f"connection_id {connection_id!r} because {dependent_count} dependent table "
                        "registration(s) exist. Create a new connection_id for the new endpoint."
                    )

            revalidation_fields = (
                "source_server", "source_database", "secret_scope",
                "trust_server_certificate",
            )
            changed = any(
                connection.get(field) != prior.get(field)
                for field in revalidation_fields
                if field in connection and field in prior
            )
            if changed:
                connection["connection_status"] = "REGISTERED"
                connection["is_active"] = False
                connection["last_validated_ts"] = None
            assignments = [
                f"{quote_databricks(k)} = {self._render_value(connection[k])}"
                for k in writable if k in connection
            ]
            assignments.append("`updated_ts` = current_timestamp()")
            self.spark.sql(
                f"UPDATE {self.ctrl('source_connection')} "
                f"SET {', '.join(assignments)} "
                f"WHERE connection_id = {escape_string_literal(connection_id)}"
            )
        else:
            connection["connection_status"] = "REGISTERED"
            connection["is_active"] = False
            connection["last_validated_ts"] = None
            cols = ["connection_id"] + [k for k in writable if k in connection]
            vals = [escape_string_literal(connection_id)] + [
                self._render_value(connection[k]) for k in writable
                if k in connection
            ]
            cols += ["created_ts", "updated_ts"]
            vals += ["current_timestamp()", "current_timestamp()"]
            col_sql = ", ".join(quote_databricks(c) for c in cols)
            self.spark.sql(
                f"INSERT INTO {self.ctrl('source_connection')} "
                f"({col_sql}) VALUES ({', '.join(vals)})"
            )

    def update_connection_status(self, connection_id: str, status: str,
                                 error_message: str = None):
        """Set a connection's status (and optional error, sanitized on write)."""
        connection_id = require_connection_id(
            connection_id, "update_connection_status")
        safe_error = (sanitize_error_message(error_message)
                      if error_message is not None else None)
        assignments = [
            f"`connection_status` = {escape_string_literal(status)}",
            f"`is_active` = {'true' if status == 'VALID' else 'false'}",
            f"`error_message` = {escape_string_literal(safe_error)}",
            "`updated_ts` = current_timestamp()",
        ]
        if status == "VALID":
            assignments.append("`last_validated_ts` = current_timestamp()")
        self.spark.sql(
            f"UPDATE {self.ctrl('source_connection')} "
            f"SET {', '.join(assignments)} "
            f"WHERE connection_id = {escape_string_literal(connection_id)}"
        )

    def get_watermark(self, source_table_id: str):
        """[COMPATIBILITY ONLY - DEPRECATED] Return last watermark for a single-key id.

        Deprecated: operational workflows resolve watermarks by connection_id + source_table_id.
        Fails if source_table_id matches multiple registrations across connections.
        """
        sql = (
            f"SELECT last_watermark_value "
            f"FROM {self.ctrl('source_table_control')} "
            f"WHERE source_table_id = "
            f"{escape_string_literal(source_table_id)}"
        )

        rows = self.spark.sql(sql).collect()
        if len(rows) > 1:
            raise ValueError(
                f"source_table_id {source_table_id!r} matches {len(rows)} "
                "registrations; expected at most one")

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
        """[COMPATIBILITY ONLY - DEPRECATED] Return one control row by single-key source_table_id.

        Deprecated: operational workflows must use get_source_table(connection_id, source_table_id).
        Fails if source_table_id matches multiple registrations across connections.
        """
        sql = (
            f"SELECT * FROM {self.ctrl('source_table_control')} "
            f"WHERE source_table_id = "
            f"{escape_string_literal(source_table_id)}"
        )

        rows = self.spark.sql(sql).collect()
        if len(rows) > 1:
            raise ValueError(
                f"source_table_id {source_table_id!r} matches {len(rows)} "
                "registrations; expected at most one (use get_source_table)")

        return rows[0] if rows else None

    def get_source_table(self, connection_id: str, source_table_id: str):
        """Return one table registration by its complete ownership key."""
        connection_id = require_connection_id(
            connection_id, "get_source_table")
        source_table_id = str(source_table_id or "").strip()
        if not source_table_id:
            raise ValueError("get_source_table requires source_table_id")
        rows = self.spark.sql(
            f"SELECT * FROM {self.ctrl('source_table_control')} "
            f"WHERE connection_id = {escape_string_literal(connection_id)} "
            f"AND source_table_id = {escape_string_literal(source_table_id)}"
        ).collect()
        if len(rows) > 1:
            raise ValueError(
                f"connection_id {connection_id!r} and source_table_id "
                f"{source_table_id!r} resolve to {len(rows)} rows; expected "
                "at most one")
        return rows[0] if rows else None

    # ---------------------------------------------- writes / merges (need Spark)

    def update_control(
        self,
        source_table_id: str,
        fields: dict
    ):
        """[COMPATIBILITY ONLY - DEPRECATED] Update selected columns by single-key source_table_id.

        Deprecated: operational workflows must use update_control_for_connection() with
        composite key (connection_id, source_table_id). Fails if source_table_id matches
        multiple registrations across connections.
        """

        if not source_table_id:
            raise ValueError(
                "update_control requires a source_table_id (source-qualified "
                "identity); schema+table alone is not accepted")

        for key in (fields or {}):
            if key in PROTECTED_CONTROL_IDENTITY_FIELDS:
                raise ValueError(
                    f"Cannot update immutable identity field {key!r} via generic update")

        existing = self.spark.sql(
            f"SELECT connection_id FROM {self.ctrl('source_table_control')} "
            f"WHERE source_table_id = {escape_string_literal(source_table_id)}"
        ).collect()
        if len(existing) > 1:
            raise ValueError(
                f"source_table_id {source_table_id!r} matches {len(existing)} "
                "registrations; use update_control_for_connection")

        assignments = []

        for k, v in fields.items():
            if k in SANITIZED_CONTROL_FIELDS and v is not None:
                v = sanitize_error_message(v)
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

    def update_control_for_connection(self, connection_id: str,
                                      source_table_id: str, fields: dict):
        """Update one registration using its complete ownership key."""
        connection_id = require_connection_id(
            connection_id, "update_control_for_connection")
        source_table_id = str(source_table_id or "").strip()
        if not source_table_id:
            raise ValueError(
                "update_control_for_connection requires source_table_id")
        for key in (fields or {}):
            if key in PROTECTED_CONTROL_IDENTITY_FIELDS:
                raise ValueError(
                    f"Cannot update immutable identity field {key!r} via generic update")
        assignments = []
        for key, value in (fields or {}).items():
            if key in SANITIZED_CONTROL_FIELDS and value is not None:
                value = sanitize_error_message(value)
            assignments.append(
                f"{quote_databricks(key)} = {self._render_value(value)}")
        assignments.append("`updated_ts` = current_timestamp()")
        self.spark.sql(
            f"UPDATE {self.ctrl('source_table_control')} "
            f"SET {', '.join(assignments)} "
            f"WHERE connection_id = {escape_string_literal(connection_id)} "
            f"AND source_table_id = {escape_string_literal(source_table_id)}"
        )

    def update_control_by_identity(self, source_system, source_server,
                                   source_database, source_schema, source_table,
                                   fields: dict, connection_id=None):
        """Compatibility helper for a connection-owned physical identity.

        Uses null-safe matching for source_server / source_database so legacy
        Oracle rows (NULL server/database) match correctly, but requires an
        explicit connection owner so the same physical object under another
        connection is never changed. Operational code uses
        :meth:`update_control_for_connection`.
        """
        connection_id = require_connection_id(
            connection_id, "update_control_by_identity")
        source_system = require_source_system(
            source_system, "source table identity")
        assignments = []
        for key, value in fields.items():
            if key in SANITIZED_CONTROL_FIELDS and value is not None:
                value = sanitize_error_message(value)
            assignments.append(
                f"{quote_databricks(key)} = {self._render_value(value)}")
        assignments.append("`updated_ts` = current_timestamp()")
        set_clause = ", ".join(assignments)
        where = (
            f"connection_id = {escape_string_literal(connection_id)} "
            f"AND source_schema = {escape_string_literal(source_schema)} "
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

        Every column declared in TABLE_RUN_LOG_COLUMNS is persisted, including
        the connection, retry-lineage, and row-count metrics the retry selector,
        dashboard views, and notification notebook read back. Operational
        ownership and attempt fields are mandatory; optional metrics stay null.
        """

        for required in (
            "run_id", "connection_id", "source_table_id", "operation",
            "attempt_number"):
            if fields.get(required) is None or not str(fields.get(required)).strip():
                raise ValueError(f"table_run_log requires {required}")

        from pyspark.sql.types import (
            StructType, StructField, StringType, IntegerType, BooleanType,
            LongType, TimestampType,
        )

        spark_types = {
            "string": StringType, "int": IntegerType, "boolean": BooleanType,
            "bigint": LongType, "timestamp": TimestampType,
        }
        schema = StructType([
            StructField(name, spark_types[token](), True)
            for name, token in TABLE_RUN_LOG_COLUMNS
        ])

        normalized_row = build_table_run_row(fields)
        normalized = dict(zip(
            (name for name, _token in TABLE_RUN_LOG_COLUMNS), normalized_row))
        self.spark.sql(
            f"DELETE FROM {self.ctrl('table_run_log')} "
            f"WHERE run_id = {escape_string_literal(normalized['run_id'])} "
            f"AND connection_id = "
            f"{escape_string_literal(normalized['connection_id'])} "
            f"AND source_table_id = "
            f"{escape_string_literal(normalized['source_table_id'])} "
            f"AND operation = {escape_string_literal(normalized['operation'])} "
            f"AND COALESCE(attempt_number, 1) = "
            f"{self._render_value(normalized['attempt_number'])}"
        )
        df = self.spark.createDataFrame([normalized_row], schema)

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
        safe_message = sanitize_error_message(message) if message else message
        self.spark.sql(f"""
            INSERT INTO {self.ctrl('job_run_log')}
            VALUES (
                {escape_string_literal(run_id)},
                {escape_string_literal(job_name)},
                {escape_string_literal(status)},
                current_timestamp(),
                current_timestamp(),
                {escape_string_literal(safe_message)}
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