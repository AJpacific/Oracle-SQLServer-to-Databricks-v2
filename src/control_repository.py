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
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

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

VALID_SELECTION_STATUSES = frozenset({
    "NOT_SELECTED",
    "SELECTED",
    "ONBOARDING",
    "REGISTERED",
    "ONBOARDED",
    "FAILED",
    "REVIEW_REQUIRED",
    "BLOCKED",
})

VALID_ONBOARDING_STAGES = frozenset({
    "REGISTRATION",
    "INVENTORY",
    "TYPE_NORMALIZATION",
    "MAPPING_GENERATION",
    "MAPPING_VALIDATION",
    "TABLE_DECISION",
    "TARGET_PROVISIONING",
    "FINALIZATION",
})

VALID_DOWNSTREAM_ONBOARDING_STAGES = frozenset({
    "INVENTORY",
    "TYPE_NORMALIZATION",
    "MAPPING_GENERATION",
    "MAPPING_VALIDATION",
    "TABLE_DECISION",
    "TARGET_PROVISIONING",
    "FINALIZATION",
})

TERMINAL_SELECTION_STATUSES = frozenset({
    "ONBOARDED",
    "REVIEW_REQUIRED",
    "BLOCKED",
})

CLAIMABLE_SELECTION_STATUSES = frozenset({
    "",
    "SELECTED",
})

RETRYABLE_SELECTION_STATUSES = frozenset({
    "",
    "SELECTED",
    "FAILED",
})


def is_assessment_selection_candidate(
    is_selected,
    selection_status,
    include_failed_retries=False,
) -> bool:
    """Pure production helper to determine if an assessed table is eligible for onboarding."""
    if is_selected is not True:
        return False

    status = str(selection_status or "").strip().upper()

    if status in ("", "SELECTED"):
        return True

    return (
        status == "FAILED"
        and include_failed_retries
    )


def normalize_target_component(value: Any) -> str:
    """Return a trimmed, lowercased target identifier component for comparison."""
    return str(value or "").strip().lower()


def _row_to_dict(row) -> dict:
    if row is None:
        return {}
    if hasattr(row, "asDict"):
        try:
            return row.asDict(recursive=True)
        except TypeError:
            return row.asDict()
    return dict(row)


def _normalize_state(value) -> str:
    return str(value or "").strip().upper()


def _exception_error_class(exc: Exception) -> str:
    for name in ("getErrorClass", "get_error_class"):
        method = getattr(exc, name, None)
        if callable(method):
            try:
                value = method()
                if value:
                    return str(value).strip().upper()
            except Exception:
                pass

    for name in (
        "error_class",
        "errorClass",
        "sql_state",
        "sqlState",
    ):
        value = getattr(exc, name, None)
        if value:
            return str(value).strip().upper()

    return type(exc).__name__.strip().upper()


def is_delta_concurrency_exception(exc: Exception) -> bool:
    error_class = _exception_error_class(exc)

    normalized = (
        error_class
        .replace("_", "")
        .replace(".", "")
    )

    return normalized in {
        "CONCURRENTAPPENDEXCEPTION",
        "CONCURRENTDELETEDELETEEXCEPTION",
        "CONCURRENTDELETEREADEXCEPTION",
        "CONCURRENTTRANSACTIONEXCEPTION",
        "CONCURRENTWRITEEXCEPTION",
        "METADATACHANGEDEXCEPTION",
        "PROTOCOLCHANGEDEXCEPTION",
    }


@dataclass(frozen=True)
class ClaimResult:
    """Result of an assessment row atomic claim attempt."""
    acquired: bool
    reason: str | None = None
    row: dict[str, Any] | None = None

    def __bool__(self) -> bool:
        return self.acquired


def _validate_bounded_identifier(value: str, name: str, max_len: int = 256) -> str:
    s = str(value or "").strip()
    if not s:
        raise ValueError(f"{name} is required")
    if len(s) > max_len:
        raise ValueError(f"{name} exceeds maximum length of {max_len}")
    if any(c in s for c in ("'", '"', ';', '\n', '\r', '\0')):
        raise ValueError(f"{name} contains invalid characters")
    return s


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
            "WHERE coalesce(is_active, false) = true "
            "AND upper(trim(connection_status)) = 'VALID' "
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

    def valid_active_connections_for_source(
        self,
        source_system: str,
        only_connection_ids: list[str] | None = None,
        exclude_connection_ids: list[str] | None = None,
    ):
        """Return lazy DataFrame of eligible connection IDs for a normalized source system.

        Projects ONLY connection_id, ordered deterministically by connection_id.
        Exclusion wins over inclusion. Never returns secret_scope or endpoints.
        """
        norm_system = require_source_system(source_system, "connection discovery")
        sql = (
            f"SELECT connection_id FROM {self.ctrl('source_connection')} "
            f"WHERE coalesce(is_active, false) = true "
            f"AND upper(trim(connection_status)) = 'VALID' "
            f"AND secret_scope IS NOT NULL AND trim(secret_scope) <> '' "
            f"AND connection_id IS NOT NULL AND trim(connection_id) <> '' "
            f"AND lower(trim(source_system)) = {escape_string_literal(norm_system)}"
        )
        clean_exclude = set()
        if exclude_connection_ids:
            clean_exclude = {
                str(c).strip() for c in exclude_connection_ids if str(c).strip()
            }

        clean_only = set()
        if only_connection_ids is not None:
            raw_only = [str(c).strip() for c in only_connection_ids if str(c).strip()]
            clean_only = {c for c in raw_only if c not in clean_exclude}
            if raw_only and not clean_only:
                # Every requested inclusion ID was explicitly excluded
                sql += " AND 1 = 0"
            elif clean_only:
                in_list = ", ".join(
                    escape_string_literal(c) for c in sorted(clean_only)
                )
                sql += f" AND connection_id IN ({in_list})"

        if clean_exclude:
            not_in_list = ", ".join(
                escape_string_literal(c) for c in sorted(clean_exclude)
            )
            sql += f" AND connection_id NOT IN ({not_in_list})"

        sql += " ORDER BY connection_id ASC"
        return self.spark.sql(sql)

    def configured_connections_for_source(
        self,
        source_system: str,
        only_connection_ids: list[str] | None = None,
        exclude_connection_ids: list[str] | None = None,
    ):
        """Return lazy DataFrame of candidate configured connection IDs for validation.

        Includes REGISTERED, VALID, and FAILED status rows requiring is_active=true.
        Rows with is_active = false or is_active IS NULL are ignored completely.
        Requires nonblank connection_id, matching source_system, nonblank source_server,
        nonblank secret_scope, and nonblank source_database for Oracle.
        Projects ONLY connection_id, ordered deterministically by connection_id.
        Exclusion wins over inclusion. Never returns secret_scope or endpoints.
        """
        norm_system = require_source_system(source_system, "connection discovery")
        db_clause = (
            "AND source_database IS NOT NULL AND trim(source_database) <> '' "
            if norm_system in SOURCES_REQUIRING_DATABASE else ""
        )
        sql = (
            f"SELECT connection_id FROM {self.ctrl('source_connection')} "
            f"WHERE coalesce(is_active, false) = true "
            f"AND upper(trim(connection_status)) IN ('REGISTERED', 'VALID', 'FAILED') "
            f"AND source_server IS NOT NULL AND trim(source_server) <> '' "
            f"AND secret_scope IS NOT NULL AND trim(secret_scope) <> '' "
            f"AND connection_id IS NOT NULL AND trim(connection_id) <> '' "
            f"{db_clause}"
            f"AND lower(trim(source_system)) = {escape_string_literal(norm_system)}"
        )
        clean_exclude = set()
        if exclude_connection_ids:
            clean_exclude = {
                str(c).strip() for c in exclude_connection_ids if str(c).strip()
            }

        clean_only = set()
        if only_connection_ids is not None:
            raw_only = [str(c).strip() for c in only_connection_ids if str(c).strip()]
            clean_only = {c for c in raw_only if c not in clean_exclude}
            if raw_only and not clean_only:
                sql += " AND 1 = 0"
            elif clean_only:
                in_list = ", ".join(
                    escape_string_literal(c) for c in sorted(clean_only)
                )
                sql += f" AND connection_id IN ({in_list})"

        if clean_exclude:
            not_in_list = ", ".join(
                escape_string_literal(c) for c in sorted(clean_exclude)
            )
            sql += f" AND connection_id NOT IN ({not_in_list})"

        sql += " ORDER BY connection_id ASC"
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

    # ---------------------------------------------- target routing & assessment selection

    def resolve_target_config(self, connection_id: str) -> dict:
        """Resolve active default target routing for a registered connection.

        Precedence:
          1. Connection-specific (connection_id matches)
          2. Source-specific (connection_id is null/blank, source_system matches)
          3. Global default (both connection_id and source_system are null/blank)

        Returns only safe routing metadata:
          config_id, target_catalog, target_schema_mode, target_schema, effective_scope
        """
        connection_id = require_connection_id(connection_id, "resolve_target_config")
        conn = self.get_connection(connection_id)
        if conn is None:
            raise ValueError(
                f"Connection {connection_id!r} not found in source_connection"
            )

        conn_dict = conn.asDict() if hasattr(conn, "asDict") else dict(conn)
        source_system = require_source_system(
            conn_dict.get("source_system"), "registered connection"
        )
        table = self.ctrl("accelerator_target_config")

        # 1. Connection-specific scope
        sql_conn = (
            f"SELECT * FROM {table} "
            f"WHERE is_active = true AND is_default = true "
            f"AND connection_id = {escape_string_literal(connection_id)}"
        )
        conn_rows = self.spark.sql(sql_conn).collect()
        if len(conn_rows) > 1:
            raise ValueError(
                f"Duplicate active default target configuration found at CONNECTION scope for connection {connection_id!r}"
            )

        if conn_rows:
            matched_row = conn_rows[0]
            effective_scope = "CONNECTION"
        else:
            # 2. Source-specific scope
            sql_source = (
                f"SELECT * FROM {table} "
                f"WHERE is_active = true AND is_default = true "
                f"AND (connection_id IS NULL OR trim(connection_id) = '') "
                f"AND lower(trim(source_system)) = {escape_string_literal(source_system)}"
            )
            source_rows = self.spark.sql(sql_source).collect()
            if len(source_rows) > 1:
                raise ValueError(
                    f"Duplicate active default target configuration found at SOURCE scope for source_system {source_system!r}"
                )
            if source_rows:
                matched_row = source_rows[0]
                effective_scope = "SOURCE"
            else:
                # 3. Global scope
                sql_global = (
                    f"SELECT * FROM {table} "
                    f"WHERE is_active = true AND is_default = true "
                    f"AND (connection_id IS NULL OR trim(connection_id) = '') "
                    f"AND (source_system IS NULL OR trim(source_system) = '')"
                )
                global_rows = self.spark.sql(sql_global).collect()
                if len(global_rows) > 1:
                    raise ValueError(
                        "Duplicate active default target configuration found at GLOBAL scope"
                    )
                if global_rows:
                    matched_row = global_rows[0]
                    effective_scope = "GLOBAL"
                else:
                    raise ValueError(
                        f"No active default target configuration found for connection {connection_id!r} "
                        f"(source_system={source_system!r})"
                    )

        row_dict = (
            matched_row.asDict() if hasattr(matched_row, "asDict") else dict(matched_row)
        )
        config_id = str(row_dict.get("config_id") or "").strip()
        target_catalog = str(row_dict.get("target_catalog") or "").strip()
        target_schema_mode = str(row_dict.get("target_schema_mode") or "").strip().upper()
        target_schema = str(row_dict.get("target_schema") or "").strip() or None

        if effective_scope == "CONNECTION" and row_dict.get("source_system"):
            cfg_sys = require_source_system(row_dict["source_system"], "target_config")
            if cfg_sys != source_system:
                raise ValueError(
                    f"Target configuration {config_id!r} specifies source_system {cfg_sys!r} "
                    f"which does not match connection {connection_id!r} source_system {source_system!r}"
                )

        if not target_catalog:
            raise ValueError(
                f"Target configuration {config_id!r} has blank target_catalog"
            )
        if target_schema_mode not in ("SOURCE_SCHEMA", "PREFIX_WITH_DATABASE", "EXPLICIT"):
            raise ValueError(
                f"Target configuration {config_id!r} has invalid target_schema_mode: {target_schema_mode!r}"
            )
        if target_schema_mode == "EXPLICIT" and not target_schema:
            raise ValueError(
                f"Target configuration {config_id!r} with EXPLICIT mode requires nonblank target_schema"
            )

        return {
            "config_id": config_id,
            "target_catalog": target_catalog,
            "target_schema_mode": target_schema_mode,
            "target_schema": target_schema,
            "effective_scope": effective_scope,
        }

    def selected_assessment_batches(
        self,
        source_system: str,
        only_connection_ids: list[str] | None = None,
        only_assessment_ids: list[str] | None = None,
        include_failed_retries: bool = False,
    ):
        """Return lazy DataFrame of distinct connection_id + assessment_id batches.

        Filters:
          - object_type = 'TABLE'
          - compatibility_status IN ('COMPATIBLE', 'REVIEW')
          - is_selected = true
          - connection is active VALID with nonblank secret_scope
          - matching normalized source_system on both connection and assessment
          - selection_status:
              Default: null, blank, or 'SELECTED'
              If include_failed_retries=True: also includes 'FAILED'
              Never includes 'ONBOARDING' or 'ONBOARDED'
        """
        norm_system = require_source_system(source_system, "selected assessment discovery")
        sa = self.ctrl("source_assessment")
        sc = self.ctrl("source_connection")

        if include_failed_retries:
            status_clause = (
                "(sa.selection_status IS NULL OR trim(sa.selection_status) = '' "
                "OR upper(trim(sa.selection_status)) IN ('SELECTED', 'FAILED'))"
            )
        else:
            status_clause = (
                "(sa.selection_status IS NULL OR trim(sa.selection_status) = '' "
                "OR upper(trim(sa.selection_status)) = 'SELECTED')"
            )

        sql = (
            f"SELECT DISTINCT sa.connection_id, sa.assessment_id "
            f"FROM {sa} sa "
            f"JOIN {sc} sc ON sa.connection_id = sc.connection_id "
            f"WHERE sa.object_type = 'TABLE' "
            f"AND upper(trim(sa.compatibility_status)) IN ('COMPATIBLE', 'REVIEW') "
            f"AND sa.is_selected = true "
            f"AND {status_clause} "
            f"AND sc.is_active = true "
            f"AND upper(trim(sc.connection_status)) = 'VALID' "
            f"AND sc.secret_scope IS NOT NULL AND trim(sc.secret_scope) <> '' "
            f"AND lower(trim(sc.source_system)) = {escape_string_literal(norm_system)} "
            f"AND lower(trim(sa.source_system)) = lower(trim(sc.source_system))"
        )

        clean_conns = {str(c).strip() for c in (only_connection_ids or []) if str(c).strip()}
        if clean_conns:
            in_list = ", ".join(escape_string_literal(c) for c in sorted(clean_conns))
            sql += f" AND sa.connection_id IN ({in_list})"

        clean_assessments = {str(a).strip() for a in (only_assessment_ids or []) if str(a).strip()}
        if clean_assessments:
            in_list = ", ".join(escape_string_literal(a) for a in sorted(clean_assessments))
            sql += f" AND sa.assessment_id IN ({in_list})"

        sql += " ORDER BY sa.connection_id, sa.assessment_id"
        return self.spark.sql(sql)

    def check_overlapping_selected_assessments(
        self,
        source_system: str,
        only_connection_ids: list[str] | None = None,
        only_assessment_ids: list[str] | None = None,
        include_failed_retries: bool = False,
    ) -> list[dict]:
        """Detect if any connection-owned table is selected across multiple assessment IDs.

        Conflict condition:
          - same connection_id
          - same source_schema
          - same TABLE object_name
          - is_selected = true
          - eligible compatibility ('COMPATIBLE', 'REVIEW')
          - occurring under >1 assessment_id

        Returns a list of conflict dicts with safe metadata:
          [{"connection_id": "...", "source_schema": "...", "object_name": "...", "conflicting_assessment_count": N}]
        """
        norm_system = require_source_system(source_system, "overlap check")
        sa = self.ctrl("source_assessment")
        sc = self.ctrl("source_connection")

        if include_failed_retries:
            status_clause = (
                "(sa.selection_status IS NULL OR trim(sa.selection_status) = '' "
                "OR upper(trim(sa.selection_status)) IN ('SELECTED', 'FAILED'))"
            )
        else:
            status_clause = (
                "(sa.selection_status IS NULL OR trim(sa.selection_status) = '' "
                "OR upper(trim(sa.selection_status)) = 'SELECTED')"
            )

        sql = (
            f"SELECT sa.connection_id, sa.source_database, sa.source_schema, sa.object_name, "
            f"count(DISTINCT sa.assessment_id) AS conflicting_assessment_count "
            f"FROM {sa} sa "
            f"JOIN {sc} sc ON sa.connection_id = sc.connection_id "
            f"WHERE sa.object_type = 'TABLE' "
            f"AND upper(trim(sa.compatibility_status)) IN ('COMPATIBLE', 'REVIEW') "
            f"AND sa.is_selected = true "
            f"AND {status_clause} "
            f"AND sc.is_active = true "
            f"AND upper(trim(sc.connection_status)) = 'VALID' "
            f"AND sc.secret_scope IS NOT NULL AND trim(sc.secret_scope) <> '' "
            f"AND lower(trim(sc.source_system)) = {escape_string_literal(norm_system)} "
            f"AND lower(trim(sa.source_system)) = lower(trim(sc.source_system))"
        )

        clean_conns = {str(c).strip() for c in (only_connection_ids or []) if str(c).strip()}
        if clean_conns:
            in_list = ", ".join(escape_string_literal(c) for c in sorted(clean_conns))
            sql += f" AND sa.connection_id IN ({in_list})"

        clean_assessments = {str(a).strip() for a in (only_assessment_ids or []) if str(a).strip()}
        if clean_assessments:
            in_list = ", ".join(escape_string_literal(a) for a in sorted(clean_assessments))
            sql += f" AND sa.assessment_id IN ({in_list})"

        sql += (
            f" GROUP BY sa.connection_id, sa.source_database, sa.source_schema, sa.object_name "
            f"HAVING count(DISTINCT sa.assessment_id) > 1 "
            f"ORDER BY sa.connection_id, sa.source_database, sa.source_schema, sa.object_name"
        )
        rows = self.spark.sql(sql).collect()
        conflicts = []
        for r in rows:
            rd = r.asDict() if hasattr(r, "asDict") else dict(r)
            conflicts.append({
                "connection_id": rd["connection_id"],
                "source_database": rd.get("source_database"),
                "source_schema": rd["source_schema"],
                "object_name": rd["object_name"],
                "conflicting_assessment_count": int(rd["conflicting_assessment_count"]),
            })
        return conflicts

    def _get_assessment_selection_row(
        self,
        connection_id: str,
        assessment_id: str,
        source_schema: str,
        object_name: str,
        source_database: str | None = None,
    ) -> dict[str, Any] | None:
        """Query one exact TABLE assessment row.

        Returns:
            None if zero rows match.
            Dictionary if exactly one row matches.
            Raises ValueError if more than one row matches.
        """
        connection_id = require_connection_id(connection_id, "_get_assessment_selection_row")
        assessment_id = _validate_bounded_identifier(assessment_id, "assessment_id")
        source_schema = str(source_schema or "").strip()
        object_name = str(object_name or "").strip()

        where_predicates = [
            f"connection_id = {escape_string_literal(connection_id)}",
            f"assessment_id = {escape_string_literal(assessment_id)}",
            f"source_schema = {escape_string_literal(source_schema)}",
            "object_type = 'TABLE'",
            f"object_name = {escape_string_literal(object_name)}",
        ]
        if source_database is not None and str(source_database).strip():
            where_predicates.append(
                f"source_database = {escape_string_literal(str(source_database).strip())}"
            )

        sql = (
            f"SELECT connection_id, assessment_id, source_database, source_schema, object_type, "
            f"object_name, is_selected, compatibility_status, selection_status, "
            f"onboarding_run_id, onboarding_attempt_id, onboarding_started_ts, "
            f"registration_completed_ts, onboarding_completed_ts, "
            f"onboarding_failed_stage, onboarding_error_message "
            f"FROM {self.ctrl('source_assessment')} "
            f"WHERE {' AND '.join(where_predicates)}"
        )
        rows = self.spark.sql(sql).collect()
        if not rows:
            return None
        if len(rows) > 1:
            raise ValueError(
                f"Duplicate exact assessment TABLE rows found for connection={connection_id!r}, "
                f"assessment={assessment_id!r}, schema={source_schema!r}, table={object_name!r}"
            )
        return _row_to_dict(rows[0])

    def update_assessment_selection_state(
        self,
        connection_id: str,
        assessment_id: str,
        source_schema: str,
        object_name: str,
        selection_status: str,
        error_message: str | None = None,
        selected_by: str | None = None,
        source_database: str | None = None,
    ):
        """Update the row-level selection state on source_assessment for operator actions.

        Restricted strictly to operator actions: 'SELECTED' and 'NOT_SELECTED'.
        Runtime lifecycle transitions (ONBOARDING, REGISTERED, ONBOARDED, FAILED,
        REVIEW_REQUIRED, BLOCKED) must use dedicated ownership-aware lifecycle methods.
        """
        connection_id = require_connection_id(connection_id, "update_assessment_selection_state")
        assessment_id = _validate_bounded_identifier(assessment_id, "assessment_id")
        source_schema = str(source_schema or "").strip()
        object_name = str(object_name or "").strip()
        status = _normalize_state(selection_status)

        if status not in ("SELECTED", "NOT_SELECTED"):
            raise ValueError(
                f"update_assessment_selection_state() only supports operator actions 'SELECTED' "
                f"and 'NOT_SELECTED', got {selection_status!r}. Callers must use ownership-aware "
                f"lifecycle methods for onboarding runtime transitions."
            )

        assignments = [
            f"`selection_status` = {escape_string_literal(status)}",
            "`onboarding_run_id` = NULL",
            "`onboarding_attempt_id` = NULL",
            "`onboarding_started_ts` = NULL",
            "`registration_completed_ts` = NULL",
            "`onboarding_completed_ts` = NULL",
            "`onboarding_failed_stage` = NULL",
            "`onboarding_error_message` = NULL",
        ]

        if status == "SELECTED":
            assignments.append("`is_selected` = true")
            assignments.append("`selected_ts` = current_timestamp()")
            if selected_by and str(selected_by).strip():
                safe_by = sanitize_error_message(selected_by)[:256]
                assignments.append(f"`selected_by` = {escape_string_literal(safe_by)}")
            else:
                assignments.append("`selected_by` = NULL")
        elif status == "NOT_SELECTED":
            assignments.append("`is_selected` = false")
            assignments.append("`selected_ts` = NULL")
            assignments.append("`selected_by` = NULL")

        where_predicates = [
            f"connection_id = {escape_string_literal(connection_id)}",
            f"assessment_id = {escape_string_literal(assessment_id)}",
            f"source_schema = {escape_string_literal(source_schema)}",
            "object_type = 'TABLE'",
            f"object_name = {escape_string_literal(object_name)}",
            "(selection_status IS NULL OR trim(selection_status) = '' "
            "OR upper(trim(selection_status)) IN ('NOT_SELECTED', 'SELECTED', 'FAILED'))",
            "(onboarding_run_id IS NULL OR trim(onboarding_run_id) = '')",
            "(onboarding_attempt_id IS NULL OR trim(onboarding_attempt_id) = '')",
        ]
        if source_database is not None and str(source_database).strip():
            where_predicates.append(
                f"source_database = {escape_string_literal(str(source_database).strip())}"
            )

        sql = f"UPDATE {self.ctrl('source_assessment')} SET {', '.join(assignments)} WHERE {' AND '.join(where_predicates)}"
        self.spark.sql(sql)

    def claim_assessment_selection_row(
        self,
        connection_id: str,
        assessment_id: str,
        source_schema: str,
        object_name: str,
        run_id: str,
        attempt_id: str,
        allow_failed_retry: bool = False,
        source_database: str | None = None,
    ) -> ClaimResult:
        """Atomically claim an eligible assessment table row for onboarding.

        Under Delta optimistic concurrency, this performs a conditional update
        conditioned on eligible prior state and verified compatibility, followed by
        an immediate post-write ownership verification via _get_assessment_selection_row.
        Only the winning run owns the claim.
        """
        connection_id = require_connection_id(connection_id, "claim_assessment_selection_row")
        assessment_id = _validate_bounded_identifier(assessment_id, "assessment_id")
        source_schema = str(source_schema or "").strip()
        if not source_schema:
            raise ValueError("source_schema is required")
        object_name = str(object_name or "").strip()
        if not object_name:
            raise ValueError("object_name is required")
        run_id = _validate_bounded_identifier(run_id, "run_id")
        attempt_id = _validate_bounded_identifier(attempt_id, "attempt_id")

        if allow_failed_retry:
            prior_condition = (
                "(selection_status IS NULL OR trim(selection_status) = '' "
                "OR upper(trim(selection_status)) IN ('SELECTED', 'FAILED'))"
            )
        else:
            prior_condition = (
                "(selection_status IS NULL OR trim(selection_status) = '' "
                "OR upper(trim(selection_status)) = 'SELECTED')"
            )

        where_predicates = [
            f"connection_id = {escape_string_literal(connection_id)}",
            f"assessment_id = {escape_string_literal(assessment_id)}",
            f"source_schema = {escape_string_literal(source_schema)}",
            "object_type = 'TABLE'",
            f"object_name = {escape_string_literal(object_name)}",
            "coalesce(is_selected, false) = true",
            "upper(trim(coalesce(compatibility_status, ''))) IN ('COMPATIBLE', 'REVIEW')",
            prior_condition,
        ]
        if source_database is not None and str(source_database).strip():
            where_predicates.append(
                f"source_database = {escape_string_literal(str(source_database).strip())}"
            )

        set_clause = (
            f"`selection_status` = 'ONBOARDING', "
            f"`onboarding_run_id` = {escape_string_literal(run_id)}, "
            f"`onboarding_attempt_id` = {escape_string_literal(attempt_id)}, "
            f"`onboarding_started_ts` = current_timestamp(), "
            f"`registration_completed_ts` = NULL, "
            f"`onboarding_completed_ts` = NULL, "
            f"`onboarding_failed_stage` = NULL, "
            f"`onboarding_error_message` = NULL"
        )

        update_sql = (
            f"UPDATE {self.ctrl('source_assessment')} "
            f"SET {set_clause} "
            f"WHERE {' AND '.join(where_predicates)}"
        )
        try:
            self.spark.sql(update_sql)
        except Exception as exc:
            if not is_delta_concurrency_exception(exc):
                raise

        row = self._get_assessment_selection_row(
            connection_id=connection_id,
            assessment_id=assessment_id,
            source_schema=source_schema,
            object_name=object_name,
            source_database=source_database,
        )
        if not row:
            return ClaimResult(acquired=False, reason="CLAIM_NOT_ACQUIRED: row not found", row=None)

        st = _normalize_state(row.get("selection_status"))
        r_id = str(row.get("onboarding_run_id") or "").strip()
        a_id = str(row.get("onboarding_attempt_id") or "").strip()

        if st == "ONBOARDING" and r_id == run_id and a_id == attempt_id:
            return ClaimResult(acquired=True, row=row)

        return ClaimResult(
            acquired=False,
            reason=f"CLAIM_NOT_ACQUIRED: row in state {st!r} owned by run={r_id!r}, attempt={a_id!r}",
            row=row,
        )

    def mark_assessment_preclaim_failed(
        self,
        connection_id: str,
        assessment_id: str,
        source_schema: str,
        object_name: str,
        error: Exception | str | None,
        allow_existing_failed: bool = False,
        source_database: str | None = None,
    ) -> bool:
        """Record pre-claim validation failure before any claim is acquired.

        Only updates unowned claimable rows (run_id and attempt_id are null or blank).
        """
        connection_id = require_connection_id(connection_id, "mark_assessment_preclaim_failed")
        assessment_id = _validate_bounded_identifier(assessment_id, "assessment_id")
        source_schema = str(source_schema or "").strip()
        if not source_schema:
            raise ValueError("source_schema is required")
        object_name = str(object_name or "").strip()
        if not object_name:
            raise ValueError("object_name is required")

        safe_error = sanitize_error_message(error) if error else None
        if safe_error and len(safe_error) > 2000:
            safe_error = safe_error[:1997] + "..."

        if allow_existing_failed:
            prior_condition = (
                "(selection_status IS NULL OR trim(selection_status) = '' "
                "OR upper(trim(selection_status)) IN ('SELECTED', 'FAILED'))"
            )
        else:
            prior_condition = (
                "(selection_status IS NULL OR trim(selection_status) = '' "
                "OR upper(trim(selection_status)) = 'SELECTED')"
            )

        where_predicates = [
            f"connection_id = {escape_string_literal(connection_id)}",
            f"assessment_id = {escape_string_literal(assessment_id)}",
            f"source_schema = {escape_string_literal(source_schema)}",
            "object_type = 'TABLE'",
            f"object_name = {escape_string_literal(object_name)}",
            "coalesce(is_selected, false) = true",
            prior_condition,
            "(onboarding_run_id IS NULL OR trim(onboarding_run_id) = '')",
            "(onboarding_attempt_id IS NULL OR trim(onboarding_attempt_id) = '')",
        ]
        if source_database is not None and str(source_database).strip():
            where_predicates.append(
                f"source_database = {escape_string_literal(str(source_database).strip())}"
            )

        assignments = [
            "`selection_status` = 'FAILED'",
            "`onboarding_failed_stage` = 'REGISTRATION'",
        ]
        if safe_error:
            assignments.append(f"`onboarding_error_message` = {escape_string_literal(safe_error)}")
        else:
            assignments.append("`onboarding_error_message` = NULL")

        update_sql = (
            f"UPDATE {self.ctrl('source_assessment')} "
            f"SET {', '.join(assignments)} "
            f"WHERE {' AND '.join(where_predicates)}"
        )

        try:
            self.spark.sql(update_sql)
        except Exception as exc:
            if not is_delta_concurrency_exception(exc):
                raise

        row = self._get_assessment_selection_row(
            connection_id=connection_id,
            assessment_id=assessment_id,
            source_schema=source_schema,
            object_name=object_name,
            source_database=source_database,
        )
        if not row:
            return False

        st = _normalize_state(row.get("selection_status"))
        r_id = str(row.get("onboarding_run_id") or "").strip()
        a_id = str(row.get("onboarding_attempt_id") or "").strip()

        return st == "FAILED" and not r_id and not a_id

    def mark_assessment_registration_succeeded(
        self,
        connection_id: str,
        assessment_id: str,
        source_schema: str,
        object_name: str,
        run_id: str,
        attempt_id: str,
        source_database: str | None = None,
    ) -> bool:
        """Perform or confirm an idempotently confirmed owned transition of an owned ONBOARDING assessment row to REGISTERED.

        Only updates when the row is currently in ONBOARDING and owned by run_id + attempt_id.
        Does NOT set ONBOARDED (downstream onboarding still required).
        """
        connection_id = require_connection_id(connection_id, "mark_assessment_registration_succeeded")
        assessment_id = _validate_bounded_identifier(assessment_id, "assessment_id")
        run_id = _validate_bounded_identifier(run_id, "run_id")
        attempt_id = _validate_bounded_identifier(attempt_id, "attempt_id")
        source_schema = str(source_schema or "").strip()
        object_name = str(object_name or "").strip()

        where_predicates = [
            f"connection_id = {escape_string_literal(connection_id)}",
            f"assessment_id = {escape_string_literal(assessment_id)}",
            f"source_schema = {escape_string_literal(source_schema)}",
            "object_type = 'TABLE'",
            f"object_name = {escape_string_literal(object_name)}",
            "upper(trim(coalesce(selection_status, ''))) = 'ONBOARDING'",
            f"onboarding_run_id = {escape_string_literal(run_id)}",
            f"onboarding_attempt_id = {escape_string_literal(attempt_id)}",
        ]
        if source_database is not None and str(source_database).strip():
            where_predicates.append(
                f"source_database = {escape_string_literal(str(source_database).strip())}"
            )

        set_clause = (
            "`selection_status` = 'REGISTERED', "
            "`registration_completed_ts` = current_timestamp(), "
            "`onboarding_failed_stage` = NULL, "
            "`onboarding_error_message` = NULL"
        )

        sql = f"UPDATE {self.ctrl('source_assessment')} SET {set_clause} WHERE {' AND '.join(where_predicates)}"
        try:
            self.spark.sql(sql)
        except Exception as exc:
            if not is_delta_concurrency_exception(exc):
                raise

        row = self._get_assessment_selection_row(
            connection_id=connection_id,
            assessment_id=assessment_id,
            source_schema=source_schema,
            object_name=object_name,
            source_database=source_database,
        )
        if not row:
            return False

        st = _normalize_state(row.get("selection_status"))
        r_id = str(row.get("onboarding_run_id") or "").strip()
        a_id = str(row.get("onboarding_attempt_id") or "").strip()

        return st == "REGISTERED" and r_id == run_id and a_id == attempt_id

    def mark_assessment_onboarding_terminal(
        self,
        connection_id: str,
        assessment_id: str,
        source_schema: str,
        object_name: str,
        run_id: str,
        attempt_id: str | None,
        terminal_status: str,
        message: Exception | str | None = None,
        source_database: str | None = None,
    ) -> bool:
        """Perform or confirm an idempotently confirmed owned transition of an owned REGISTERED row to terminal (REVIEW_REQUIRED or BLOCKED)."""
        connection_id = require_connection_id(connection_id, "mark_assessment_onboarding_terminal")
        assessment_id = _validate_bounded_identifier(assessment_id, "assessment_id")
        run_id = _validate_bounded_identifier(run_id, "run_id")
        source_schema = str(source_schema or "").strip()
        object_name = str(object_name or "").strip()

        term_status = str(terminal_status or "").strip().upper()
        if term_status not in ("REVIEW_REQUIRED", "BLOCKED"):
            raise ValueError(
                f"Invalid terminal_status {terminal_status!r}; expected 'REVIEW_REQUIRED' or 'BLOCKED'"
            )

        safe_message = sanitize_error_message(message) if message else None
        if safe_message and len(safe_message) > 2000:
            safe_message = safe_message[:1997] + "..."

        where_predicates = [
            f"connection_id = {escape_string_literal(connection_id)}",
            f"assessment_id = {escape_string_literal(assessment_id)}",
            f"source_schema = {escape_string_literal(source_schema)}",
            "object_type = 'TABLE'",
            f"object_name = {escape_string_literal(object_name)}",
            "upper(trim(coalesce(selection_status, ''))) = 'REGISTERED'",
            f"onboarding_run_id = {escape_string_literal(run_id)}",
        ]
        if attempt_id:
            valid_attempt = _validate_bounded_identifier(attempt_id, "attempt_id")
            where_predicates.append(f"onboarding_attempt_id = {escape_string_literal(valid_attempt)}")
        if source_database is not None and str(source_database).strip():
            where_predicates.append(
                f"source_database = {escape_string_literal(str(source_database).strip())}"
            )

        assignments = [
            f"`selection_status` = {escape_string_literal(term_status)}",
            "`onboarding_completed_ts` = current_timestamp()",
            "`onboarding_failed_stage` = NULL",
        ]
        if safe_message:
            assignments.append(f"`onboarding_error_message` = {escape_string_literal(safe_message)}")
        else:
            assignments.append("`onboarding_error_message` = NULL")

        sql = f"UPDATE {self.ctrl('source_assessment')} SET {', '.join(assignments)} WHERE {' AND '.join(where_predicates)}"
        try:
            self.spark.sql(sql)
        except Exception as exc:
            if not is_delta_concurrency_exception(exc):
                raise

        row = self._get_assessment_selection_row(
            connection_id=connection_id,
            assessment_id=assessment_id,
            source_schema=source_schema,
            object_name=object_name,
            source_database=source_database,
        )
        if not row:
            return False

        st = _normalize_state(row.get("selection_status"))
        r_id = str(row.get("onboarding_run_id") or "").strip()
        a_id = str(row.get("onboarding_attempt_id") or "").strip()
        f_stage = row.get("onboarding_failed_stage")

        matches = (
            st == term_status
            and r_id == run_id
            and (f_stage is None or str(f_stage).strip() == "")
        )
        if attempt_id:
            matches = matches and (a_id == attempt_id)
        return matches

    def mark_assessment_onboarding_completed(
        self,
        connection_id: str,
        assessment_id: str,
        source_schema: str,
        object_name: str,
        run_id: str,
        attempt_id: str | None = None,
        source_database: str | None = None,
    ) -> bool:
        """Perform or confirm an idempotently confirmed owned transition of an owned REGISTERED assessment row to ONBOARDED after target provisioning verification.

        Only updates when the row is currently in REGISTERED and owned by run_id (and attempt_id when supplied).
        """
        connection_id = require_connection_id(connection_id, "mark_assessment_onboarding_completed")
        assessment_id = _validate_bounded_identifier(assessment_id, "assessment_id")
        run_id = _validate_bounded_identifier(run_id, "run_id")
        source_schema = str(source_schema or "").strip()
        object_name = str(object_name or "").strip()

        where_predicates = [
            f"connection_id = {escape_string_literal(connection_id)}",
            f"assessment_id = {escape_string_literal(assessment_id)}",
            f"source_schema = {escape_string_literal(source_schema)}",
            "object_type = 'TABLE'",
            f"object_name = {escape_string_literal(object_name)}",
            "upper(trim(coalesce(selection_status, ''))) = 'REGISTERED'",
            f"onboarding_run_id = {escape_string_literal(run_id)}",
        ]
        if attempt_id:
            valid_attempt = _validate_bounded_identifier(attempt_id, "attempt_id")
            where_predicates.append(f"onboarding_attempt_id = {escape_string_literal(valid_attempt)}")
        if source_database is not None and str(source_database).strip():
            where_predicates.append(
                f"source_database = {escape_string_literal(str(source_database).strip())}"
            )

        set_clause = (
            "`selection_status` = 'ONBOARDED', "
            "`onboarding_completed_ts` = current_timestamp(), "
            "`onboarding_failed_stage` = NULL, "
            "`onboarding_error_message` = NULL"
        )

        sql = f"UPDATE {self.ctrl('source_assessment')} SET {set_clause} WHERE {' AND '.join(where_predicates)}"
        try:
            self.spark.sql(sql)
        except Exception as exc:
            if not is_delta_concurrency_exception(exc):
                raise

        row = self._get_assessment_selection_row(
            connection_id=connection_id,
            assessment_id=assessment_id,
            source_schema=source_schema,
            object_name=object_name,
            source_database=source_database,
        )
        if not row:
            return False

        st = _normalize_state(row.get("selection_status"))
        r_id = str(row.get("onboarding_run_id") or "").strip()
        a_id = str(row.get("onboarding_attempt_id") or "").strip()

        matches = st == "ONBOARDED" and r_id == run_id
        if attempt_id:
            matches = matches and (a_id == attempt_id)
        return matches

    def mark_assessment_onboarding_failed(
        self,
        connection_id: str,
        assessment_id: str,
        source_schema: str,
        object_name: str,
        run_id: str,
        attempt_id: str | None = None,
        failed_stage: str = "REGISTRATION",
        error: Exception | str | None = None,
        source_database: str | None = None,
    ) -> bool:
        """Perform or confirm an idempotently confirmed owned transition of an owned ONBOARDING or REGISTERED assessment row to FAILED.

        Never overwrites another run's claim. Never modifies already ONBOARDED rows.
        """
        connection_id = require_connection_id(connection_id, "mark_assessment_onboarding_failed")
        assessment_id = _validate_bounded_identifier(assessment_id, "assessment_id")
        run_id = _validate_bounded_identifier(run_id, "run_id")
        source_schema = str(source_schema or "").strip()
        object_name = str(object_name or "").strip()

        stage = str(failed_stage or "").strip().upper()
        if stage not in VALID_ONBOARDING_STAGES:
            raise ValueError(
                f"Invalid onboarding failed_stage {failed_stage!r}; expected one of: "
                f"{', '.join(sorted(VALID_ONBOARDING_STAGES))}"
            )

        safe_error = sanitize_error_message(error) if error else None
        if safe_error and len(safe_error) > 2000:
            safe_error = safe_error[:1997] + "..."

        where_predicates = [
            f"connection_id = {escape_string_literal(connection_id)}",
            f"assessment_id = {escape_string_literal(assessment_id)}",
            f"source_schema = {escape_string_literal(source_schema)}",
            "object_type = 'TABLE'",
            f"object_name = {escape_string_literal(object_name)}",
            f"onboarding_run_id = {escape_string_literal(run_id)}",
            "upper(trim(coalesce(selection_status, ''))) IN ('ONBOARDING', 'REGISTERED')",
        ]
        if attempt_id:
            valid_attempt = _validate_bounded_identifier(attempt_id, "attempt_id")
            where_predicates.append(f"onboarding_attempt_id = {escape_string_literal(valid_attempt)}")
        if source_database is not None and str(source_database).strip():
            where_predicates.append(
                f"source_database = {escape_string_literal(str(source_database).strip())}"
            )

        assignments = [
            "`selection_status` = 'FAILED'",
            f"`onboarding_failed_stage` = {escape_string_literal(stage)}",
        ]
        if safe_error:
            assignments.append(f"`onboarding_error_message` = {escape_string_literal(safe_error)}")
        else:
            assignments.append("`onboarding_error_message` = NULL")

        sql = f"UPDATE {self.ctrl('source_assessment')} SET {', '.join(assignments)} WHERE {' AND '.join(where_predicates)}"
        try:
            self.spark.sql(sql)
        except Exception as exc:
            if not is_delta_concurrency_exception(exc):
                raise

        row = self._get_assessment_selection_row(
            connection_id=connection_id,
            assessment_id=assessment_id,
            source_schema=source_schema,
            object_name=object_name,
            source_database=source_database,
        )
        if not row:
            return False

        st = _normalize_state(row.get("selection_status"))
        r_id = str(row.get("onboarding_run_id") or "").strip()
        a_id = str(row.get("onboarding_attempt_id") or "").strip()
        f_stage = _normalize_state(row.get("onboarding_failed_stage"))

        matches = st == "FAILED" and r_id == run_id and f_stage == stage
        if attempt_id:
            matches = matches and (a_id == attempt_id)
        return matches

    def find_target_owners(
        self,
        target_catalog: str,
        target_schema: str,
        target_table: str,
        exclude_owner: tuple[str, str] | None = None,
        include_reserved: bool = True,
    ) -> list[dict]:
        """Find registrations that own or reserve the specified target FQN.

        A target FQN is reserved by any non-retired registration with a complete target.
        When include_reserved=True, reserved states include inactive onboarding rows
        (REGISTERED, INVENTORIED, PROVISIONED, etc.).
        exclude_owner=(connection_id, source_table_id) permits rerun by the exact same owner.
        """
        cat = normalize_target_component(target_catalog)
        sch = normalize_target_component(target_schema)
        tbl = normalize_target_component(target_table)
        if not cat or not sch or not tbl:
            return []

        where_parts = [
            f"lower(trim(coalesce(target_catalog, ''))) = {escape_string_literal(cat)}",
            f"lower(trim(coalesce(target_schema, ''))) = {escape_string_literal(sch)}",
            f"lower(trim(coalesce(target_table, ''))) = {escape_string_literal(tbl)}",
        ]
        if include_reserved:
            where_parts.append(
                "coalesce(current_status, '') NOT IN ('RETIRED', 'DECOMMISSIONED')"
            )
        else:
            where_parts.append("is_active = true")

        sql = (
            f"SELECT connection_id, source_table_id, target_catalog, target_schema, "
            f"target_table, is_active, current_status "
            f"FROM {self.ctrl('source_table_control')} "
            f"WHERE {' AND '.join(where_parts)}"
        )
        rows = self.spark.sql(sql).collect()
        owners = []
        for r in rows:
            rd = r.asDict() if hasattr(r, "asDict") else dict(r)
            owner_key = (rd.get("connection_id"), rd.get("source_table_id"))
            if exclude_owner and owner_key == exclude_owner:
                continue
            owners.append(rd)
        return owners
