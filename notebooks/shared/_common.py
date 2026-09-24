# Databricks notebook source
# MAGIC %md
# MAGIC # _common - shared source bootstrap
# MAGIC
# MAGIC Include this at the top of every accelerator notebook with:
# MAGIC ```
# MAGIC %run ./_common
# MAGIC ```
# MAGIC It puts the src/ package on the path, imports shared logic, selects the
# MAGIC registered adapter for each control-table row, reads credentials from the
# MAGIC connection's Databricks secret scope, and exposes shared helpers.

# COMMAND ----------

import os
import sys
import json
import uuid
from datetime import datetime, timezone

# COMMAND ----------

# Force a deterministic UTC session timezone so every accelerator notebook
# serializes and compares temporal watermarks in UTC, independent of the
# cluster's default timezone. Do not rely on the environment default.
try:
    spark.conf.set("spark.sql.session.timeZone", "UTC")
except Exception:
    pass

# COMMAND ----------

# --- widgets: common configuration for every notebook -----------------------
# Using widgets means the same notebook works interactively and as a Job task.

def _ensure_widget(name, default):
    try:
        dbutils.widgets.text(name, default)
    except Exception:
        pass

_ensure_widget("catalog", "da_accelerators")
_ensure_widget("control_schema", "control")
# Absolute path to the repo src folder, used only as a fallback if the
# package import below fails (e.g. notebook run outside a Git folder).
_ensure_widget("src_path", "")
# Pipeline-driven identifiers. Downstream tasks receive only non-secret ids;
# credentials always stay in the connection's Databricks secret scope.
_ensure_widget("connection_id", "")
_ensure_widget("source_table_id", "")
_ensure_widget("run_id", "")

CATALOG = dbutils.widgets.get("catalog").strip()
CONTROL_SCHEMA = dbutils.widgets.get("control_schema").strip()
_SRC_PATH = dbutils.widgets.get("src_path").strip()
CONNECTION_ID = dbutils.widgets.get("connection_id").strip()
SOURCE_TABLE_ID = dbutils.widgets.get("source_table_id").strip()

# COMMAND ----------

# --- make the src package importable ----------------------------------------

# Discover the repo root from the running notebook / working directory instead of
# a hard-coded personal workspace path. src_path (widget) is the explicit
# override; otherwise the package import and cwd walk below locate src/.
def _discover_repo_root():
    try:
        ctx = (dbutils.notebook.entry_point.getDbutils().notebook()
               .getContext())
        nb_path = ctx.notebookPath().get()
        # /Workspace/<...>/<repo>/notebooks/<name> -> /Workspace/<...>/<repo>
        ws = "/Workspace" + os.path.dirname(os.path.dirname(nb_path))
        if os.path.isdir(ws):
            return ws
    except Exception:
        pass
    return os.getcwd()

repo_root = _discover_repo_root()

if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

def _bootstrap_src():
    try:
        import src.identifiers as identifiers_module
        return os.path.dirname(identifiers_module.__file__)
    except ModuleNotFoundError:
        pass

    candidates = []

    if _SRC_PATH:
        candidates.append(_SRC_PATH)

    candidates.append(os.path.join(repo_root, "src"))

    here = os.getcwd()

    for _ in range(8):
        candidates.append(os.path.join(here, "src"))
        here = os.path.dirname(here)

    for cand in candidates:
        if cand and os.path.isdir(cand):
            if cand not in sys.path:
                sys.path.insert(0, cand)
            return cand

    raise RuntimeError(
        "Could not locate the 'src' folder."
    )

_SRC_LOCATION = _bootstrap_src()
print(f"[_common] src location: {_SRC_LOCATION}")

# COMMAND ----------

# --- import helpers (works whether flat on path or as package) --------------

try:
    from src.identifiers import (
        quote_databricks, quote_oracle, oracle_fqn, databricks_fqn,
        escape_string_literal, validate_identifier, normalize_target_identifier,
        IdentifierError,
    )
    from src.type_mappers.base import (
        ColumnMappingResult, classify_table_compatibility,
    )
    from src.strategy import (
        detect_strategy, pick_watermark_column, is_valid_strategy,
        FULL_LOAD, WATERMARK, PRIMARY_KEY, HYBRID, WATERMARK_CANDIDATE_TYPES,
    )
    from src import ddl_builder as ddl
    from src import watermark as wm
    from src import reconciliation as recon
    from src import failure_classifier as failcls
    from src import dq_rules as dqr
    from src import etl_work_unit as etlwu
    from src import assessment_common as assess_common
    from src import inventory_common as inv_common
    from src import sql_object_assessment_common as sqlobj_common
    from src import source_registry
    from src.control_repository import (
        ControlRepository, new_run_id,
        normalize_connection_input, assert_source_system_match,
        require_connection_id,
        VALID_SELECTION_STATUSES,
        VALID_ONBOARDING_STAGES,
        VALID_DOWNSTREAM_ONBOARDING_STAGES,
        TERMINAL_SELECTION_STATUSES,
        CLAIMABLE_SELECTION_STATUSES,
        RETRYABLE_SELECTION_STATUSES,
        ClaimResult,
        is_assessment_selection_candidate,
        is_delta_concurrency_exception,
        normalize_target_component,
    )
    from src.source_identity import (
        SOURCE_IDENTITY_VERSION, compute_legacy_source_table_id,
        compute_source_table_id, normalize_source_system, require_source_system,
        canonical_source_system_sql,
    )
    from src.worklist_utils import (
        TASK_VALUE_LIMIT_BYTES, validate_task_value_payload,
    )
    from src.source_adapters.factory import get_source_adapter
except ModuleNotFoundError:
    from identifiers import (
        quote_databricks, quote_oracle, oracle_fqn, databricks_fqn,
        escape_string_literal, validate_identifier, normalize_target_identifier,
        IdentifierError,
    )
    from type_mappers.base import (
        ColumnMappingResult, classify_table_compatibility,
    )
    from strategy import (
        detect_strategy, pick_watermark_column, is_valid_strategy,
        FULL_LOAD, WATERMARK, PRIMARY_KEY, HYBRID, WATERMARK_CANDIDATE_TYPES,
    )
    import ddl_builder as ddl
    import watermark as wm
    import reconciliation as recon
    import failure_classifier as failcls
    import dq_rules as dqr
    import etl_work_unit as etlwu
    import assessment_common as assess_common
    import inventory_common as inv_common
    import sql_object_assessment_common as sqlobj_common
    import source_registry
    from control_repository import (
        ControlRepository, new_run_id,
        normalize_connection_input, assert_source_system_match,
        require_connection_id,
        VALID_SELECTION_STATUSES,
        VALID_ONBOARDING_STAGES,
        VALID_DOWNSTREAM_ONBOARDING_STAGES,
        TERMINAL_SELECTION_STATUSES,
        CLAIMABLE_SELECTION_STATUSES,
        RETRYABLE_SELECTION_STATUSES,
        ClaimResult,
        is_assessment_selection_candidate,
        is_delta_concurrency_exception,
        normalize_target_component,
    )
    from source_identity import (
        SOURCE_IDENTITY_VERSION, compute_legacy_source_table_id,
        compute_source_table_id, normalize_source_system, require_source_system,
        canonical_source_system_sql,
    )
    from worklist_utils import (
        TASK_VALUE_LIMIT_BYTES, validate_task_value_payload,
    )
    from source_adapters.factory import get_source_adapter

# COMMAND ----------

def _secret_provider(scope, key):
    "Adapter-facing secret reader: (scope, key) -> value | None."
    try:
        val = dbutils.secrets.get(scope, key)
        return val or None
    except Exception:
        return None


def _config_dir_candidates():
    "Directories that may hold the config/*.yaml type rule files."
    dirs = [os.path.join(repo_root, "config")]
    if _SRC_LOCATION and _SRC_LOCATION != "package":
        dirs.append(os.path.join(os.path.dirname(_SRC_LOCATION), "config"))
    dirs.append("config")
    return dirs


def _resolve_type_rules_path(adapter):
    """Locate the rules file the ADAPTER names; never inferred from the source.

    A future source supplies its own filename and needs no change here. A
    missing or unlocatable file fails loudly with the source token and filename
    (never a credential).
    """
    filename = adapter.type_rules_file()
    if not filename:
        raise ValueError(
            f"source {adapter.source_system!r} does not declare a type-rules file")
    for directory in _config_dir_candidates():
        path = os.path.join(directory, filename)
        if os.path.isfile(path):
            return path
    raise FileNotFoundError(
        f"type-rules file {filename!r} for source {adapter.source_system!r} was "
        f"not found in: {', '.join(_config_dir_candidates())}")


def _build_adapter(source_system, source_server=None, source_database=None,
                   secret_scope=None, extra_config=None):
    """Construct an adapter and attach its own type-rules path.

    normalize_source_system (inside the factory) rejects an unregistered source,
    so an unknown token fails explicitly instead of defaulting to any source.
    """
    adapter = get_source_adapter(
        source_system, source_server=source_server,
        source_database=source_database, secret_provider=_secret_provider,
        secret_scope=secret_scope, config=dict(extra_config or {}))
    config = dict(adapter.config)
    config["type_rules_path"] = _resolve_type_rules_path(adapter)
    adapter.config = config
    return adapter


def build_adapter(source_system, source_server=None, source_database=None,
                  secret_scope=None):
    "Public helper: a configured adapter for a source token (no secrets logged)."
    return _build_adapter(source_system, source_server=source_server,
                          source_database=source_database,
                          secret_scope=secret_scope)


def get_connection(connection_id):
    "Return the source_connection row for an id, or None (no secrets involved)."
    if not connection_id:
        return None
    return control_repo().get_connection(connection_id)


def require_valid_connection(connection_id, expected_source_system=None):
    """Return one active VALID registered connection with a nonblank scope."""
    connection_id = require_connection_id(
        connection_id, "source operation")
    connection = control_repo().get_connection(connection_id)
    if connection is None:
        raise ValueError(
            f"connection_id {connection_id!r} not found in source_connection")
    data = connection.asDict() if hasattr(connection, "asDict") else dict(connection)
    if not data.get("is_active"):
        raise ValueError(f"connection {connection_id!r} is not active")
    if (data.get("connection_status") or "") != "VALID":
        raise ValueError(
            f"connection {connection_id!r} is not VALID "
            f"(status={data.get('connection_status')!r}); validate it first")
    if not str(data.get("secret_scope") or "").strip():
        raise ValueError(
            f"registered connection {connection_id!r} has a blank secret_scope")
    source_system = require_source_system(
        data.get("source_system"), "registered connection")
    if expected_source_system is not None:
        assert_source_system_match(expected_source_system, source_system)
    return connection


def assert_table_connection_match(table_row, connection_id):
    """Reject a table/work row that is not owned by the supplied connection."""
    expected = require_connection_id(connection_id, "table ownership check")
    data = table_row.asDict() if hasattr(table_row, "asDict") else dict(table_row)
    actual = require_connection_id(
        data.get("connection_id"), "source table registration")
    if actual != expected:
        raise ValueError(
            f"source table belongs to connection_id {actual!r}, not {expected!r}")
    return actual


def resolve_effective_source_database(table_row, connection_row):
    """Resolve and validate the effective database across table and connection rows.

    For SQL Server:
      - If connection database is blank (discovery parent): operational row requires
        a nonblank database, which becomes the effective database.
      - If connection database is populated: operational row must match it exactly.
    For Oracle:
      - Connection database/service is required and operational database must match it.
    """
    table = table_row.asDict() if hasattr(table_row, "asDict") else dict(table_row)
    connection = (connection_row.asDict() if hasattr(connection_row, "asDict")
                  else dict(connection_row))
    sys_name = require_source_system(
        connection.get("source_system") or table.get("source_system"),
        "source database identity validation"
    )
    adapter = get_source_adapter(sys_name)
    return adapter.resolve_operational_database(
        connection.get("source_database"), table.get("source_database"))


def assert_source_identity_match(table_row, connection_row):
    """Validate table ownership metadata against the authoritative connection."""
    table = table_row.asDict() if hasattr(table_row, "asDict") else dict(table_row)
    connection = (connection_row.asDict() if hasattr(connection_row, "asDict")
                  else dict(connection_row))
    assert_table_connection_match(table, connection.get("connection_id"))
    assert_source_system_match(
        table.get("source_system"), connection.get("source_system"))

    table_server = str(table.get("source_server") or "").strip().casefold()
    connection_server = str(connection.get("source_server") or "").strip().casefold()
    if table_server and connection_server and table_server != connection_server:
        raise ValueError(
            f"source table source_server does not match registered connection "
            f"{connection.get('connection_id')!r}")

    resolve_effective_source_database(table, connection)
    return True


def assert_current_source_table_identity(table_row, connection_row):
    """Require a registration's stored ID to match connection-owned v2."""
    table = table_row.asDict() if hasattr(table_row, "asDict") else dict(table_row)
    connection = (connection_row.asDict() if hasattr(connection_row, "asDict")
                  else dict(connection_row))
    assert_source_identity_match(table, connection)
    if table.get("source_identity_version") != SOURCE_IDENTITY_VERSION:
        raise ValueError(
            "source table registration requires identity-v2 migration")
    effective_db = resolve_effective_source_database(table, connection)
    expected = compute_source_table_id(
        connection.get("connection_id"), connection.get("source_system"),
        connection.get("source_server"), effective_db,
        table.get("source_schema"), table.get("source_table"))
    if table.get("source_table_id") != expected:
        raise ValueError(
            "source_table_id does not match its connection-owned identity")
    return True


def get_source_adapter_for_connection(connection, source_database=None,
                                      require_valid=True):
    """Build the source adapter described by a source_connection row.

    The registered ``secret_scope`` is authoritative: shared code never replaces
    it and never infers a scope from the source system. A blank registered scope
    fails clearly. No secret value or credential-bearing URL is ever returned.
    """
    c = connection.asDict() if hasattr(connection, "asDict") else dict(connection)
    if require_valid and not c.get("is_active"):
        raise ValueError(
            f"connection {c.get('connection_id')!r} is not active")
    if require_valid and (c.get("connection_status") or "") != "VALID":
        raise ValueError(
            f"connection {c.get('connection_id')!r} is not VALID "
            f"(status={c.get('connection_status')!r}); validate it first")
    source_system = require_source_system(
        c.get("source_system"), "registered connection")
    registered_database = c.get("source_database")
    if (registered_database and source_database is not None
            and str(source_database).strip().casefold()
            != str(registered_database or "").strip().casefold()):
        raise ValueError(
            f"source_database override does not match registered connection "
            f"{c.get('connection_id')!r}")
    database = registered_database or source_database
    secret_scope = (c.get("secret_scope") or "").strip()
    if not secret_scope:
        raise ValueError(
            f"registered connection {c.get('connection_id')!r} has a blank "
            "secret_scope")
    extra = {}
    if c.get("trust_server_certificate") is not None:
        extra["trust_server_certificate"] = bool(c.get("trust_server_certificate"))
    adapter = _build_adapter(
        source_system, source_server=c.get("source_server"),
        source_database=database, secret_scope=secret_scope, extra_config=extra)
    # The source states its own metadata requirements (e.g. a mandatory database).
    adapter.validate_connection_metadata(c)
    return adapter


def get_source_adapter_routed(row, require_valid=True):
    """Route a control/queue row through its authoritative registered connection.

    Source operations require a registered connection. A queue row must still
    belong to the same connection as its source_table_control row, and copied
    server/database metadata must match the registry. Never prints secrets or
    credential-bearing URLs.
    """
    d = row.asDict() if hasattr(row, "asDict") else dict(row)
    conn_id = require_connection_id(
        d.get("connection_id"), "source operation")
    connection = (require_valid_connection(conn_id, d.get("source_system"))
                  if require_valid else get_connection(conn_id))
    if connection is None:
        raise ValueError(f"connection_id {conn_id!r} not found in source_connection")
    cd = connection.asDict() if hasattr(connection, "asDict") else dict(connection)

    src_id = d.get("source_table_id")
    if src_id:
        control_row = control_repo().get_source_table(conn_id, src_id)
        if control_row is None:
            raise ValueError(
                f"connection_id {conn_id!r} and source_table_id {src_id!r} "
                "not found in source_table_control")
        assert_current_source_table_identity(control_row, connection)
    assert_source_identity_match(d, connection)
    op_db = resolve_effective_source_database(d, connection)
    return get_source_adapter_for_connection(
        connection, source_database=op_db, require_valid=require_valid)


def read_source_jdbc(adapter, dbtable, source_server=None, source_database=None,
                     fetchsize=10000, partition_column=None, lower_bound=None,
                     upper_bound=None, num_partitions=None):
    """Read a (sub)query for a row's source via its adapter (per-row connection).

    No notebook may read a SQL Server row through an Oracle-only helper: this is
    the single shared entry point and it always uses the row's adapter.
    """
    return adapter.read_jdbc(
        spark, dbtable,
        source_server=source_server, source_database=source_database,
        fetchsize=fetchsize, partition_column=partition_column,
        lower_bound=lower_bound, upper_bound=upper_bound,
        num_partitions=num_partitions,
    )


def source_table_id_for_row(row):
    "Compute the deterministic source_table_id for a control/queue row."
    d = row.asDict() if hasattr(row, "asDict") else dict(row)
    return compute_source_table_id(
        require_connection_id(d.get("connection_id"), "source table identity"),
        require_source_system(d.get("source_system")),
        d.get("source_server"),
        d.get("source_database"),
        d.get("source_schema"),
        d.get("source_table"),
    )


# Migration note: the former Oracle-only get_jdbc_url_and_props()/read_jdbc()
# helpers were removed from shared code. Shared notebooks must not hold a
# source's credential key names or dialect connection logic. Every caller now
# obtains an adapter (get_source_adapter_routed / get_source_adapter_for_connection)
# and reads through read_source_jdbc(); the adapter owns its own secret keys.


def conform_to_table(df, target_fqn):
    "Select/cast df columns to an existing Delta table's schema (match by name)."
    from pyspark.sql import functions as F
    tgt_fields = spark.table(target_fqn).schema.fields
    have = set(df.columns)
    exprs = []
    for fld in tgt_fields:
        if fld.name in have:
            exprs.append(F.col(f"`{fld.name}`").cast(fld.dataType).alias(fld.name))
        else:
            exprs.append(F.lit(None).cast(fld.dataType).alias(fld.name))
    return df.select(*exprs)


def resolve_target_column_name(source_col: str, mappings: list) -> str:
    """Resolve a source column name to its approved Databricks target column name.

    mappings is an iterable of dicts or Rows with 'column_name' and 'target_column_name'.
    Fails clearly if:
    - No included approved target mapping matches.
    - More than one mapping matches due to case ambiguity.
    - The mapped target identifier is invalid or blank.
    """
    if not source_col or not str(source_col).strip():
        raise ValueError("Source column name cannot be blank")

    raw_s = str(source_col).strip()

    def _get(obj, key):
        if hasattr(obj, "asDict"):
            return obj.asDict().get(key)
        if isinstance(obj, dict):
            return obj.get(key)
        return getattr(obj, key, None) or obj[key]

    # Exact match first
    exact_matches = [m for m in mappings if _get(m, "column_name") == raw_s]
    if len(exact_matches) == 1:
        target_name = _get(exact_matches[0], "target_column_name")
    elif len(exact_matches) > 1:
        raise ValueError(f"Ambiguous mapping: multiple mappings for source column {raw_s!r}")
    else:
        # Case-insensitive match
        ci_matches = [
            m for m in mappings
            if str(_get(m, "column_name") or "").strip().casefold() == raw_s.casefold()
        ]
        if len(ci_matches) == 1:
            target_name = _get(ci_matches[0], "target_column_name")
        elif len(ci_matches) > 1:
            raise ValueError(
                f"Ambiguous mapping: multiple case-insensitive mappings for source column {raw_s!r}"
            )
        else:
            raise ValueError(f"No included approved target mapping found for column {raw_s!r}")

    if not target_name or not str(target_name).strip():
        raise ValueError(f"Mapped target identifier for column {raw_s!r} is invalid or blank")

    return validate_identifier(str(target_name).strip())


def safe_source_column(col_name: str):
    """Return a safe Spark Column reference for source column names.

    Handles spaces, periods, slashes, hyphens, brackets, backticks,
    preventing dots from being parsed as nested struct field access.
    """
    if col_name is None:
        raise ValueError("Source column name cannot be None")
    escaped = str(col_name).replace("`", "``")
    try:
        from pyspark.sql import functions as F
        return F.col(f"`{escaped}`")
    except Exception:
        class MockCol:
            def __init__(self, name):
                self._name = name
            def alias(self, target):
                return MockAliasedCol(self._name, target)
            def __repr__(self):
                return f"col('{self._name}')"
        class MockAliasedCol:
            def __init__(self, src, tgt):
                self.src = src
                self.tgt = tgt
            def __repr__(self):
                return f"col('{self.src}').alias('{self.tgt}')"
        return MockCol(f"`{escaped}`")


def validate_target_identity(target_catalog: str, target_schema: str, target_table: str):
    """Validate authoritative target identity components fail-closed without fallbacks."""
    if not target_catalog or not str(target_catalog).strip():
        raise ValueError("target_catalog is missing; repair registration metadata")
    if not target_schema or not str(target_schema).strip():
        raise ValueError("target_schema is missing; repair registration metadata")
    if not target_table or not str(target_table).strip():
        raise ValueError("target_table is missing; repair registration metadata")

    t_cat = validate_identifier(str(target_catalog).strip())
    t_sch = validate_identifier(str(target_schema).strip())
    t_tbl = validate_identifier(str(target_table).strip())
    return t_cat, t_sch, t_tbl


def get_complete_mapping_snapshot(
    connection_id: str,
    source_table_id: str,
    run_id: str = None,
    spark_session=None,
    catalog: str = None,
    control_schema: str = None,
):
    """Resolve exactly one complete mapping run for a table registration.

    Algorithm:
    1. Query resolved_column_mappings using exact connection_id and source_table_id.
    2. Group by run_id.
    3. Select one complete mapping run using deterministic ordering:
       max(captured_ts) DESC NULLS LAST, run_id DESC (or use explicit run_id if provided).
    4. Retrieve all rows only from the selected run_id.
    5. Order rows by ordinal_position.
    6. Return: (selected_mapping_run_id, complete_ordered_mapping_rows)
    7. Fail clearly when:
       - there is no mapping run
       - the selected run has no rows
       - included mappings have blank target_column_name
       - included mappings have duplicate target_column_name values
       - included mappings are not AUTO
       - included mappings have no databricks_delta_type
    """
    sp = spark_session or spark
    cat = catalog or CATALOG
    csch = control_schema or CONTROL_SCHEMA
    tbl = f"{quote_databricks(cat)}.{quote_databricks(csch)}.{quote_databricks('resolved_column_mappings')}"

    if not connection_id or not str(connection_id).strip():
        raise ValueError("connection_id cannot be blank when retrieving mapping snapshot")
    if not source_table_id or not str(source_table_id).strip():
        raise ValueError("source_table_id cannot be blank when retrieving mapping snapshot")

    conn_id = str(connection_id).strip()
    src_id = str(source_table_id).strip()

    if run_id and str(run_id).strip():
        selected_run_id = str(run_id).strip()
    else:
        run_query = f"""
            SELECT run_id
            FROM (
              SELECT run_id,
                     ROW_NUMBER() OVER (
                       ORDER BY max(captured_ts) DESC NULLS LAST, run_id DESC
                     ) AS rn
              FROM {tbl}
              WHERE connection_id = {escape_string_literal(conn_id)}
                AND source_table_id = {escape_string_literal(src_id)}
              GROUP BY run_id
            )
            WHERE rn = 1
        """
        run_rows = sp.sql(run_query).collect()
        if not run_rows:
            raise ValueError(
                f"No mapping run found in resolved_column_mappings for "
                f"connection_id={conn_id!r}, source_table_id={src_id!r}"
            )
        selected_run_id = run_rows[0]["run_id"]

    data_query = f"""
        SELECT *
        FROM {tbl}
        WHERE connection_id = {escape_string_literal(conn_id)}
          AND source_table_id = {escape_string_literal(src_id)}
          AND run_id = {escape_string_literal(selected_run_id)}
        ORDER BY ordinal_position
    """
    rows = sp.sql(data_query).collect()
    if not rows:
        raise ValueError(
            f"Selected mapping run {selected_run_id!r} has no rows for "
            f"connection_id={conn_id!r}, source_table_id={src_id!r}"
        )

    def _get(obj, key):
        if hasattr(obj, "asDict"):
            return obj.asDict().get(key)
        if isinstance(obj, dict):
            return obj.get(key)
        return getattr(obj, key, None) or obj[key]

    included = [r for r in rows if _get(r, "include_column") is not False]
    if not included:
        raise ValueError(
            f"Selected mapping run {selected_run_id!r} has no included columns for "
            f"connection_id={conn_id!r}, source_table_id={src_id!r}"
        )

    seen_target_names = set()
    for r in included:
        col = _get(r, "column_name")
        t_col = _get(r, "target_column_name")
        if not t_col or not str(t_col).strip():
            raise ValueError(
                f"Included column {col!r} in mapping run {selected_run_id!r} "
                f"has blank target_column_name; regenerate mappings"
            )
        t_col_str = str(t_col).strip()
        validate_identifier(t_col_str)

        t_col_lower = t_col_str.lower()
        if t_col_lower in seen_target_names:
            raise ValueError(
                f"Duplicate target_column_name {t_col_str!r} detected in mapping run "
                f"{selected_run_id!r} for source column {col!r}"
            )
        seen_target_names.add(t_col_lower)

        status = (_get(r, "mapping_status") or "").strip().upper()
        if status != "AUTO":
            raise ValueError(
                f"Included column {col!r} in mapping run {selected_run_id!r} "
                f"has non-AUTO mapping_status: {status}"
            )

        dtype = _get(r, "databricks_delta_type")
        if not dtype or not str(dtype).strip():
            raise ValueError(
                f"Included column {col!r} in mapping run {selected_run_id!r} "
                f"has empty databricks_delta_type"
            )

    return selected_run_id, rows


def _resolve_actual_source_columns(expected_source_columns, actual_source_columns):
    """Resolve expected approved source columns to actual DataFrame columns.

    Precedence:
    1. Exact case-sensitive match first.
    2. If no exact match exists, compare only leading/trailing whitespace using str.strip().
    3. Accept the trimmed fallback only when exactly one actual src_df column matches.
    4. If zero columns match, record as missing (reported with repr() for whitespace visibility).
    5. If multiple actual columns match after trimming, fail explicitly as ambiguous.

    Safety:
    - Never trims internal spaces.
    - Never performs case-insensitive matching.
    - Rejects None, empty, or whitespace-only input.
    """
    actual_set = set(actual_source_columns)
    resolved_map = {}
    missing = []

    for src_c in expected_source_columns:
        if src_c is None or not str(src_c).strip():
            raise ValueError(
                f"Approved source column name is blank or invalid: {src_c!r}"
            )

        # 1. Exact case-sensitive match always wins
        if src_c in actual_set:
            resolved_map[src_c] = src_c
            continue

        # 2. Compare only leading/trailing whitespace via str.strip()
        stripped_target = src_c.strip()
        unique_cands = [c for c in dict.fromkeys(actual_source_columns) if c.strip() == stripped_target]

        if len(unique_cands) == 1:
            resolved_map[src_c] = unique_cands[0]
        elif len(unique_cands) > 1:
            raise ValueError(
                f"Ambiguous source column mapping for approved column {src_c!r}: "
                f"multiple DataFrame columns match after trimming: {', '.join(repr(c) for c in unique_cands)}"
            )
        else:
            missing.append(src_c)

    if missing:
        raise ValueError(
            "Extracted source DataFrame is missing approved columns: "
            + ", ".join(repr(c) for c in missing)
        )

    # Validate that distinct approved mappings did not resolve to the same DataFrame column
    from collections import Counter
    resolved_counts = Counter(resolved_map.values())
    dup_resolved = [c for c, count in resolved_counts.items() if count > 1]
    if dup_resolved:
        raise ValueError(
            f"Multiple approved mappings resolved to the same source column: {', '.join(repr(c) for c in dup_resolved)}"
        )

    return resolved_map


def project_and_validate_dataframe(src_df, approved_mappings):
    """Strictly project and validate source DataFrame to target columns.

    Checks:
    - Every included approved source column appears exactly once in src_df.columns.
    - No duplicate mapping exists for an approved source column.
    - Every target_column_name is nonblank and valid.
    - Every target_column_name is unique case-insensitively.
    - The projected DataFrame contains exactly the expected target columns in ordinal order.
    """
    from collections import Counter

    def _get(obj, key):
        if hasattr(obj, "asDict"):
            return obj.asDict().get(key)
        if isinstance(obj, dict):
            return obj.get(key)
        return getattr(obj, key, None) or obj[key]

    mappings = [m for m in approved_mappings if _get(m, "include_column") is not False]
    expected_source_columns = [_get(m, "column_name") for m in mappings]
    actual_source_columns = list(src_df.columns)

    # Check for duplicate source column mappings
    src_counts = Counter(expected_source_columns)
    dup_src = [c for c, count in src_counts.items() if count > 1]
    if dup_src:
        raise ValueError(
            f"Duplicate mappings found for source columns: {', '.join(repr(c) for c in dup_src)}"
        )

    # Resolve approved source columns to actual DataFrame column labels
    col_map = _resolve_actual_source_columns(expected_source_columns, actual_source_columns)

    # Strict check: duplicate column in src_df among resolved columns
    actual_counts = Counter(actual_source_columns)
    dup_actual = [c for c in col_map.values() if actual_counts[c] > 1]
    if dup_actual:
        raise ValueError(
            f"Extracted source DataFrame has duplicate column instances for: {', '.join(repr(c) for c in dup_actual)}"
        )

    # Validate target columns and build projection expressions
    expected_target_columns = []
    seen_targets = set()
    proj_exprs = []
    for m in mappings:
        src_c = _get(m, "column_name")
        actual_c = col_map[src_c]
        t_c = _get(m, "target_column_name")
        if not t_c or not str(t_c).strip():
            raise ValueError(f"Target column name is blank for source column {src_c!r}")
        t_c_str = str(t_c).strip()
        validate_identifier(t_c_str)
        t_c_lower = t_c_str.lower()
        if t_c_lower in seen_targets:
            raise ValueError(f"Duplicate target column name {t_c_str!r} in approved mappings")
        seen_targets.add(t_c_lower)
        expected_target_columns.append(t_c_str)
        proj_exprs.append(safe_source_column(actual_c).alias(t_c_str))

    projected_df = src_df.select(*proj_exprs)
    if hasattr(projected_df, "columns") and isinstance(projected_df.columns, (list, tuple)):
        actual_target_columns = list(projected_df.columns)
        if actual_target_columns != expected_target_columns:
            raise ValueError(
                f"Projected DataFrame columns mismatch: expected {expected_target_columns}, got {actual_target_columns}"
            )

    return projected_df

# COMMAND ----------

# --- misc helpers -----------------------------------------------------------

def now_utc():
    return datetime.now(timezone.utc)


def get_run_id():
    """Resolve this task's run_id deterministically.

    Priority: the explicit run_id widget (how a job task should pass it), then an
    approved upstream task value, then a newly generated id. A retry child run
    supplies its own run_id widget, so it is never overwritten by the parent's.
    """

    try:
        widget_run_id = dbutils.widgets.get("run_id").strip()
    except Exception:
        widget_run_id = ""

    if widget_run_id:
        return widget_run_id

    upstream_tasks = (
        "T00_Init_Control",
        "T01_Upsert_Validate_Connection",
        "T11a_DeltaSyncPrep",
        "T11a_Delta_Prep",
        "T20_Delta_Prep",
        "T40_Select_Retries",
    )

    for task_key in upstream_tasks:
        try:
            rid = dbutils.jobs.taskValues.get(
                taskKey=task_key,
                key="run_id",
                debugValue=""
            )
        except Exception:
            rid = ""

        if rid:
            return rid

    return new_run_id()


def set_task_value(key, value):
    try:
        dbutils.jobs.taskValues.set(key=key, value=value)
    except Exception:
        # not running inside a Job; ignore
        pass


def control_repo():
    return ControlRepository(spark, CATALOG, CONTROL_SCHEMA)


def load_type_mapper(source_system):
    "Load a source's Delta mapping rules using the file the adapter names."
    adapter = build_adapter(source_system)
    print(f"[_common] loading {adapter.source_system} type rules from "
          f"{adapter.type_rules_file()}")
    return adapter.load_type_mapper()

# COMMAND ----------

# --- shared persistence for source-specific notebooks -----------------------
# Source-specific notebooks own only their dialect discovery/extraction SQL.
# These helpers own the common write paths so Oracle and SQL Server produce an
# identical control-table shape and neither duplicates persistence logic.

def ctrl_table(name):
    "Fully-qualified, quoted control table name."
    return (f"{quote_databricks(CATALOG)}.{quote_databricks(CONTROL_SCHEMA)}."
            f"{quote_databricks(name)}")


def _plain(fqn):
    return fqn.replace("`", "")


def probe_connection(adapter, source_server=None, source_database=None):
    """Run the adapter's own connectivity probe; raise on an unexpected result.

    The probe SQL is dialect-specific and comes from the adapter, so shared code
    never hard-codes a source's syntax.
    """
    rows = read_source_jdbc(
        adapter, adapter.connection_probe_query(),
        source_server=source_server, source_database=source_database,
        fetchsize=1).collect()
    if not rows or int(rows[0]["CONNECTION_OK"]) != 1:
        raise RuntimeError("connectivity probe returned an unexpected result")
    return True


def persist_assessment_records(records):
    """
    Persist normalized assessment records using the target table schema.

    Decimal fields are converted to decimal.Decimal.
    The MERGE inserts only assessment-owned columns, leaving downstream
    selection and onboarding fields unchanged or null.
    """
    from decimal import Decimal

    from pyspark.sql import functions as F
    from pyspark.sql.types import DecimalType

    if not records:
        return {}

    for record in records:
        assess_common.validate_assessment_record(record)

    assessment_fields = list(
        assess_common.ASSESSMENT_FIELDS
    )

    target_schema = (
        spark.table(
            _plain(
                ctrl_table("source_assessment")
            )
        )
        .select(*assessment_fields)
        .schema
    )

    def _coerce_value(value, data_type):
        if value is None:
            return None

        if isinstance(data_type, DecimalType):
            decimal_value = (
                value
                if isinstance(value, Decimal)
                else Decimal(str(value))
            )

            return decimal_value.quantize(
                Decimal(1).scaleb(-data_type.scale)
            )

        return value

    values = [
        tuple(
            _coerce_value(
                record.get(schema_field.name),
                schema_field.dataType,
            )
            for schema_field in target_schema.fields
        )
        for record in records
    ]

    df = (
        spark.createDataFrame(
            values,
            schema=target_schema,
        )
        .withColumn(
            "captured_ts",
            F.current_timestamp(),
        )
    )

    df.createOrReplaceTempView(
        "_assessed_objects"
    )

    on_clause = " AND ".join(
        f"coalesce(t.{key}, '') = coalesce(s.{key}, '')" if key == "source_database" else f"t.{key} = s.{key}"
        for key in assess_common.ASSESSMENT_MERGE_KEYS
    )

    set_clause = ", ".join(
        f"t.{column} = s.{column}"
        for column in assess_common.ASSESSMENT_UPDATE_FIELDS
    )

    insert_columns = assessment_fields + [
        "captured_ts"
    ]

    insert_column_clause = ", ".join(
        insert_columns
    )

    insert_value_clause = ", ".join(
        f"s.{column}"
        for column in insert_columns
    )

    spark.sql(
        f"""
        MERGE INTO {ctrl_table('source_assessment')} t
        USING _assessed_objects s
          ON {on_clause}

        WHEN MATCHED THEN UPDATE SET
          {set_clause},
          t.captured_ts = s.captured_ts

        WHEN NOT MATCHED THEN INSERT (
          {insert_column_clause}
        ) VALUES (
          {insert_value_clause}
        )
        """
    )

    return assess_common.summarize_compatibility(
        records
    )


def persist_inventory_rows(rows):
    """Replace one run/table inventory snapshot after validating the full set."""
    from pyspark.sql import functions as F
    from pyspark.sql.types import (
        StructType, StructField, StringType, IntegerType, BooleanType)
    if not rows:
        return 0
    records = inv_common.validate_inventory_batch(rows)
    run_id = records[0]["run_id"]
    connection_id = records[0]["connection_id"]
    source_table_id = records[0]["source_table_id"]

    control_rows = spark.sql(f"""
        SELECT connection_id
        FROM {ctrl_table('source_table_control')}
        WHERE connection_id = {escape_string_literal(connection_id)}
          AND source_table_id = {escape_string_literal(source_table_id)}
    """).collect()
    if len(control_rows) != 1:
        raise ValueError(
            f"source_table_id {source_table_id!r} resolves to "
            f"{len(control_rows)} control rows; expected exactly one")
    control_connection_id = control_rows[0]["connection_id"]
    if control_connection_id != connection_id:
        raise ValueError(
            f"source_table_id {source_table_id!r} belongs to connection_id "
            f"{control_connection_id!r}; received {connection_id!r}")

    prior_connections = {
        row["connection_id"]
        for row in spark.sql(f"""
            SELECT DISTINCT connection_id
            FROM {ctrl_table('source_inventory')}
                        WHERE connection_id = {escape_string_literal(connection_id)}
                            AND source_table_id = {escape_string_literal(source_table_id)}
              AND connection_id IS NOT NULL
        """).collect()
    }
    if prior_connections and prior_connections != {connection_id}:
        raise ValueError(
            f"source_table_id {source_table_id!r} is already associated with "
            f"connection_id value(s) {sorted(prior_connections)!r}; received "
            f"{connection_id!r}")
    int_cols = {"ordinal_position", "character_maximum_length",
                "numeric_precision", "numeric_scale", "datetime_precision"}
    bool_cols = {"is_identity", "is_computed", "is_hidden", "is_rowversion"}
    schema = StructType([
        StructField(
            name,
            IntegerType() if name in int_cols else
            (BooleanType() if name in bool_cols else StringType()),
            True)
        for name in inv_common.INVENTORY_FIELDS
    ])
    values = [tuple(record.get(name) for name in inv_common.INVENTORY_FIELDS)
              for record in records]
    snapshot = (spark.createDataFrame(values, schema=schema)
                .withColumn("captured_ts", F.current_timestamp()))

    # Exact replacement removes stale columns from a retry while preserving
    # every other run and source table. Validation completes before this write.
    spark.sql(f"""
        DELETE FROM {ctrl_table('source_inventory')}
        WHERE run_id = {escape_string_literal(run_id)}
                    AND connection_id = {escape_string_literal(connection_id)}
          AND source_table_id = {escape_string_literal(source_table_id)}
    """)
    (snapshot.write.format("delta").mode("append").option(
        "mergeSchema", "true").saveAsTable(
            _plain(ctrl_table("source_inventory"))))
    return len(records)


def persist_sql_object_records(records):
    """
    MERGE normalized SQL-object inventory records into `sql_object_assessment`.

    Uses the existing Delta table schema instead of Python inference so an
    inaccessible definition retains its intended STRING type.
    """
    from pyspark.sql import functions as F

    if not records:
        return {}

    sql_object_fields = list(
        sqlobj_common.SQL_OBJECT_FIELDS
    )

    target_schema = (
        spark.table(
            _plain(
                ctrl_table(
                    "sql_object_assessment"
                )
            )
        )
        .select(*sql_object_fields)
        .schema
    )

    values = [
        tuple(
            record.get(field)
            for field in sql_object_fields
        )
        for record in records
    ]

    df = (
        spark.createDataFrame(
            values,
            schema=target_schema,
        )
        .withColumn(
            "captured_ts",
            F.current_timestamp(),
        )
        .withColumn(
            "updated_ts",
            F.current_timestamp(),
        )
    )

    df.createOrReplaceTempView(
        "_sql_objects"
    )

    on_clause = " AND ".join(
        f"coalesce(t.{key}, '') = coalesce(s.{key}, '')" if key == "source_database" else f"t.{key} = s.{key}"
        for key
        in sqlobj_common.SQL_OBJECT_MERGE_KEYS
    )

    set_clause = ", ".join(
        f"t.{column} = s.{column}"
        for column
        in sqlobj_common.SQL_OBJECT_UPDATE_FIELDS
    )

    insert_columns = sql_object_fields + [
        "captured_ts", "updated_ts"
    ]

    insert_column_clause = ", ".join(
        insert_columns
    )

    insert_value_clause = ", ".join(
        f"s.{column}"
        for column in insert_columns
    )

    spark.sql(
        f"""
        MERGE INTO {ctrl_table('sql_object_assessment')} t
        USING _sql_objects s
          ON {on_clause}

        WHEN MATCHED THEN UPDATE SET
          {set_clause},
          t.updated_ts = s.updated_ts

        WHEN NOT MATCHED THEN INSERT (
          {insert_column_clause}
        ) VALUES (
          {insert_value_clause}
        )
        """
    )

    summary = {}
    for record in records:
        object_type = record.get("object_type")
        summary[object_type] = summary.get(object_type, 0) + 1
    return summary


def resolve_assessment_schemas(adapter, source_database, include_schemas,
                               exclude_schemas):
    """Discover schemas through the adapter and apply include/exclude filters."""
    discovered = [
        r["SCHEMA_NAME"] for r in
        read_source_jdbc(adapter, adapter.list_schemas_query(source_database),
                         source_database=source_database).collect()
    ]
    included = {s for s in (include_schemas or [])}
    excluded = {s.lower() for s in (exclude_schemas or [])}
    kept = [s for s in discovered
            if (not included or s in included) and s.lower() not in excluded]
    print(f"[_common] schemas to assess: {len(kept)} of {len(discovered)} discovered")
    return kept


print("[_common] bootstrap complete. CATALOG=%s CONTROL_SCHEMA=%s" % (CATALOG, CONTROL_SCHEMA))