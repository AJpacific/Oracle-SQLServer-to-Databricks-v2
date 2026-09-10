# Databricks notebook source
# MAGIC %md
# MAGIC # _common - shared Oracle and SQL Server bootstrap
# MAGIC
# MAGIC Include this at the top of every accelerator notebook with:
# MAGIC ```
# MAGIC %run ./_common
# MAGIC ```
# MAGIC It puts the src/ package on the path, imports shared logic, selects the
# MAGIC Oracle or SQL Server adapter per control-table row, reads credentials from
# MAGIC source-specific Databricks secret scopes, and exposes shared helpers.

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
_ensure_widget("secret_scope", "oracle-migration")
# Secret scope holding the SQL Server connection secrets (sqlserver-user,
# sqlserver-password, and either sqlserver-jdbc-url or sqlserver-host/port).
_ensure_widget("sqlserver_secret_scope", "sqlserver-migration")
# Absolute path to the repo src folder, used only as a fallback if the
# package import below fails (e.g. notebook run outside a Git folder).
_ensure_widget("src_path", "")

CATALOG = dbutils.widgets.get("catalog").strip()
CONTROL_SCHEMA = dbutils.widgets.get("control_schema").strip()
SECRET_SCOPE = dbutils.widgets.get("secret_scope").strip()
SQLSERVER_SECRET_SCOPE = dbutils.widgets.get("sqlserver_secret_scope").strip()
_SRC_PATH = dbutils.widgets.get("src_path").strip()

# COMMAND ----------

# --- make the src package importable ----------------------------------------

repo_root = "/Workspace/Users/ashutosh.jha1@lumen.com/Oracle-SQLServer-to-Databricks"

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
    from src.crosssourcetypemapper import CrossSourceTypeMapper, ColumnMappingResult
    from src.strategy import (
        detect_strategy, pick_watermark_column, is_valid_strategy,
        FULL_LOAD, WATERMARK, PRIMARY_KEY, HYBRID, WATERMARK_CANDIDATE_TYPES,
    )
    from src import ddl_builder as ddl
    from src import sql_builder as sqlb
    from src.control_repository import ControlRepository, new_run_id
    from src.source_identity import compute_source_table_id, normalize_source_system
    from src.source_adapters.factory import get_source_adapter
except ModuleNotFoundError:
    from identifiers import (
        quote_databricks, quote_oracle, oracle_fqn, databricks_fqn,
        escape_string_literal,
    )
    from crosssourcetypemapper import CrossSourceTypeMapper, ColumnMappingResult
    from strategy import (
        detect_strategy, pick_watermark_column, is_valid_strategy,
        FULL_LOAD, WATERMARK, PRIMARY_KEY, HYBRID, WATERMARK_CANDIDATE_TYPES,
    )
    import ddl_builder as ddl
    import sql_builder as sqlb
    from control_repository import ControlRepository, new_run_id
    from source_identity import compute_source_table_id, normalize_source_system
    from source_adapters.factory import get_source_adapter

# COMMAND ----------

# --- Oracle connection from secrets -----------------------------------------

def _get_secret_or_none(key):
    "Return a secret value, or None if the key isn't present in the scope."
    try:
        val = dbutils.secrets.get(SECRET_SCOPE, key)
        return val or None
    except Exception:
        return None


def _secret_provider(scope, key):
    "Adapter-facing secret reader: (scope, key) -> value | None."
    try:
        val = dbutils.secrets.get(scope, key)
        return val or None
    except Exception:
        return None


def _scope_for_system(source_system):
    "Return the secret scope holding this source system's connection secrets."
    canonical = normalize_source_system(source_system)
    return SECRET_SCOPE if canonical == "oracle" else SQLSERVER_SECRET_SCOPE


def _config_dir_candidates():
    "Directories that may hold the config/*.yaml type rule files."
    dirs = [os.path.join(repo_root, "config")]
    if _SRC_LOCATION and _SRC_LOCATION != "package":
        dirs.append(os.path.join(os.path.dirname(_SRC_LOCATION), "config"))
    dirs.append("config")
    return dirs


def _type_rules_path_for(source_system):
    "Resolve the type-rules YAML path for a source system, else None (adapter default)."
    canonical = normalize_source_system(source_system)
    if canonical == "sqlserver":
        names = ["type_rules_sqlserver.yaml"]
    else:
        names = ["type_rules_oracle.yaml", "type_rules.yaml"]
    for d in _config_dir_candidates():
        for name in names:
            path = os.path.join(d, name)
            if os.path.isfile(path):
                return path
    return None


def get_source_adapter_for_row(row):
    """Return the source adapter for one control-table row.

    The adapter is chosen purely from ``source_system`` and carries the row's
    ``source_server`` / ``source_database`` plus a secret provider bound to the
    correct scope, so callers issue adapter methods with no source-specific
    branching.
    """
    d = row.asDict() if hasattr(row, "asDict") else dict(row)
    source_system = d.get("source_system") or "oracle"
    source_server = d.get("source_server")
    source_database = d.get("source_database")
    scope = _scope_for_system(source_system)
    config = {}
    trp = _type_rules_path_for(source_system)
    if trp:
        config["type_rules_path"] = trp
    return get_source_adapter(
        source_system,
        source_server=source_server,
        source_database=source_database,
        secret_provider=_secret_provider,
        secret_scope=scope,
        config=config,
    )


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
        d.get("source_system") or "oracle",
        d.get("source_server"),
        d.get("source_database"),
        d.get("source_schema"),
        d.get("source_table"),
    )


def get_jdbc_url_and_props():
    "Build the Oracle thin JDBC url + connection properties from secrets."
    user = dbutils.secrets.get(SECRET_SCOPE, "oracle-user")
    password = dbutils.secrets.get(SECRET_SCOPE, "oracle-password")
    # Prefer a complete JDBC URL secret when present; otherwise assemble it from
    # host/port/service. For a SID, put @host:port:SID in the oracle-jdbc-url secret.
    url = _get_secret_or_none("oracle-jdbc-url")
    if not url:
        host = dbutils.secrets.get(SECRET_SCOPE, "oracle-host")
        port = dbutils.secrets.get(SECRET_SCOPE, "oracle-port")
        service = dbutils.secrets.get(SECRET_SCOPE, "oracle-service")
        url = f"jdbc:oracle:thin:@//{host}:{port}/{service}"
    props = {
        "user": user,
        "password": password,
        "driver": "oracle.jdbc.OracleDriver",
    }
    return url, props


def read_jdbc(dbtable, fetchsize=10000, partition_column=None,
              lower_bound=None, upper_bound=None, num_partitions=None):
    "Read an Oracle (sub)query via JDBC into a Spark DataFrame (Oracle compat wrapper)."
    url, props = get_jdbc_url_and_props()
    reader = (
        spark.read.format("jdbc")
        .option("url", url)
        .option("dbtable", dbtable)
        .option("user", props["user"])
        .option("password", props["password"])
        .option("driver", props["driver"])
        .option("fetchsize", str(fetchsize))
        # Read Oracle DATE values using timestamp semantics.
        .option("oracle.jdbc.mapDateToTimestamp", "true")
    )
    # Spread the read across the cluster on a numeric bound column so large
    # tables don't stream through a single connection (driver-OOM risk).
    if (partition_column and num_partitions and int(num_partitions) > 1
            and lower_bound is not None and upper_bound is not None):
        reader = (
            reader
            .option("partitionColumn", partition_column)
            .option("lowerBound", str(lower_bound))
            .option("upperBound", str(upper_bound))
            .option("numPartitions", str(int(num_partitions)))
        )
    return reader.load()


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
    """Reuse a run_id from an upstream job task, otherwise create one."""

    upstream_tasks = (
        "T00_Init_Control",
        "T11a_DeltaSyncPrep",
        "T11a_Delta_Prep",
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


def load_type_mapper(source_system="oracle"):
    "Load the required source->Delta mapping rules for a source system."
    trp = _type_rules_path_for(source_system)
    adapter = get_source_adapter(
        source_system,
        config={"type_rules_path": trp} if trp else None,
    )
    print(f"[_common] loading {normalize_source_system(source_system)} type rules"
          + (f" from {trp}" if trp else " (adapter default path)"))
    return adapter.load_type_mapper()


print("[_common] bootstrap complete. CATALOG=%s CONTROL_SCHEMA=%s" % (CATALOG, CONTROL_SCHEMA))
