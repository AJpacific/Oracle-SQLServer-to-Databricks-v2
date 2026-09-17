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
        escape_string_literal,
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
    from src import sql_object_converter as sqlconv
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
    )
    from src.source_identity import (
        SOURCE_IDENTITY_VERSION, compute_legacy_source_table_id,
        compute_source_table_id, normalize_source_system, require_source_system,
    )
    from src.source_adapters.factory import get_source_adapter
except ModuleNotFoundError:
    from identifiers import (
        quote_databricks, quote_oracle, oracle_fqn, databricks_fqn,
        escape_string_literal,
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
    import sql_object_converter as sqlconv
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
    )
    from source_identity import (
        SOURCE_IDENTITY_VERSION, compute_legacy_source_table_id,
        compute_source_table_id, normalize_source_system, require_source_system,
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


def assert_source_identity_match(table_row, connection_row):
    """Validate table ownership metadata against the authoritative connection."""
    table = table_row.asDict() if hasattr(table_row, "asDict") else dict(table_row)
    connection = (connection_row.asDict() if hasattr(connection_row, "asDict")
                  else dict(connection_row))
    assert_table_connection_match(table, connection.get("connection_id"))
    assert_source_system_match(
        table.get("source_system"), connection.get("source_system"))
    for field in ("source_server", "source_database"):
        table_value = str(table.get(field) or "").strip().casefold()
        connection_value = str(connection.get(field) or "").strip().casefold()
        if table_value and connection_value and table_value != connection_value:
            raise ValueError(
                f"source table {field} does not match registered connection "
                f"{connection.get('connection_id')!r}")
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
    expected = compute_source_table_id(
        connection.get("connection_id"), connection.get("source_system"),
        connection.get("source_server"), connection.get("source_database"),
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
    if (source_database is not None
            and str(source_database).strip().casefold()
            != str(registered_database or "").strip().casefold()):
        raise ValueError(
            f"source_database override does not match registered connection "
            f"{c.get('connection_id')!r}")
    database = registered_database
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
    return get_source_adapter_for_connection(
        connection, require_valid=require_valid)


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
    """MERGE normalized assessment records; retry-safe for one assessment_id.

    is_selected is never overwritten, so a selection made during registration
    survives a re-assessment of the same assessment_id.
    """
    from pyspark.sql import functions as F
    if not records:
        return {}
    for r in records:
        assess_common.validate_assessment_record(r)
    df = (spark.createDataFrame(records)
          .select(*assess_common.ASSESSMENT_FIELDS)
          .withColumn("captured_ts", F.current_timestamp()))
    df.createOrReplaceTempView("_assessed_objects")
    on_clause = " AND ".join(
        f"t.{k} = s.{k}" for k in assess_common.ASSESSMENT_MERGE_KEYS)
    set_clause = ", ".join(
        f"t.{c} = s.{c}" for c in assess_common.ASSESSMENT_UPDATE_FIELDS)
    spark.sql(f"""
        MERGE INTO {ctrl_table('source_assessment')} t
        USING _assessed_objects s ON {on_clause}
        WHEN MATCHED THEN UPDATE SET {set_clause}, t.captured_ts = s.captured_ts
        WHEN NOT MATCHED THEN INSERT *
    """)
    return assess_common.summarize_compatibility(records)


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
    """MERGE normalized SQL-object records; preserves APPROVED/REJECTED review."""
    from pyspark.sql import functions as F
    if not records:
        return {}
    df = (spark.createDataFrame(records)
          .select(*sqlobj_common.SQL_OBJECT_FIELDS)
          .withColumn("captured_ts", F.current_timestamp())
          .withColumn("updated_ts", F.current_timestamp()))
    df.createOrReplaceTempView("_sql_objects")
    on_clause = " AND ".join(
        f"t.{k} = s.{k}" for k in sqlobj_common.SQL_OBJECT_MERGE_KEYS)
    set_clause = ", ".join(
        f"t.{c} = s.{c}" for c in sqlobj_common.SQL_OBJECT_UPDATE_FIELDS)
    terminal = ", ".join(
        f"'{s}'" for s in sqlobj_common.TERMINAL_REVIEW_STATUSES)
    spark.sql(f"""
        MERGE INTO {ctrl_table('sql_object_assessment')} t
        USING _sql_objects s ON {on_clause}
        WHEN MATCHED THEN UPDATE SET {set_clause},
            t.review_status = CASE
                WHEN t.review_status IN ({terminal}) THEN t.review_status
                ELSE s.review_status END,
            t.updated_ts = s.updated_ts
        WHEN NOT MATCHED THEN INSERT *
    """)
    return sqlobj_common.summarize_complexity(records)


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
