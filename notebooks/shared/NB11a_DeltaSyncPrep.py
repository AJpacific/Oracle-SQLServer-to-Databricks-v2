# Databricks notebook source
# MAGIC %md
# MAGIC # NB11a_DeltaSyncPrep
# MAGIC Builds the INGEST recurring-synchronization workload queue for every
# MAGIC eligible table after the initial load. WATERMARK and HYBRID use a bounded
# MAGIC temporal extract, PRIMARY_KEY uses a complete source extract for MERGE, and
# MAGIC FULL_LOAD uses a complete source extract for target refresh. Temporal
# MAGIC precision and predicate policy are supplied by each row's registered
# MAGIC source adapter.

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

from pyspark.sql import functions as F, Row
from pyspark.sql.types import ArrayType, StringType, StructField, StructType

run_id = get_run_id()
set_task_value("run_id", run_id)
print("run_id:", run_id)
repo = control_repo()

dbutils.widgets.text("only_connection_ids", "")
dbutils.widgets.text("only_source_table_ids", "")
only_connection_ids = {
    value.strip()
    for value in dbutils.widgets.get("only_connection_ids").split(",")
    if value.strip()
}
only_source_table_ids = {
    value.strip()
    for value in dbutils.widgets.get("only_source_table_ids").split(",")
    if value.strip()
}

def ctrl(t):
    return f"{quote_databricks(CATALOG)}.{quote_databricks(CONTROL_SCHEMA)}.{quote_databricks(t)}"

# COMMAND ----------

from datetime import datetime, date, timezone


def coerce_watermark(value, watermark_type, adapter):
    """Return a timezone-aware (UTC) datetime for temporal watermark values.

    Any temporal type supported by this row's source adapter parses to an aware
    UTC datetime so ordering is chronological and naive/ISO/offset checkpoints
    compare consistently, never as lexical strings. Unsupported non-temporal
    types are rejected by the adapter contract.
    """
    if value is None:
        return None
    if not adapter.is_supported_watermark_type(watermark_type):
        raise ValueError(f"Unsupported non-temporal watermark type: {watermark_type!r}")
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, date):
        dt = datetime(value.year, value.month, value.day)
    else:
        s = str(value).strip()
        if "T" not in s and " " in s:
            s = s.replace(" ", "T", 1)
        s = s.replace(" ", "")
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = None
        try:
            dt = datetime.fromisoformat(s)
        except ValueError:
            for fmt in ("%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
                try:
                    dt = datetime.strptime(s, fmt)
                    break
                except ValueError:
                    continue
        if dt is None:
            raise ValueError(f"Unparseable temporal watermark {value!r} ({watermark_type})")
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def capture_upper_watermark(adapter, database, schema, table, wm_col, wm_type, server):
    """Capture adapter-normalized MAX once and return (raw, canonical_utc)."""
    rows = read_source_jdbc(
        adapter, adapter.upper_watermark_query(database, schema, table, wm_col, wm_type),
        source_server=server, source_database=database).collect()
    raw = rows[0]["UPPER_WATERMARK"] if rows else None
    canonical = None if raw is None else wm.canonical_watermark_string(raw, strict=True)
    return raw, canonical

# COMMAND ----------

connection_filter = (
    " AND c.connection_id IN (" + ", ".join(
        escape_string_literal(value) for value in sorted(only_connection_ids)
    ) + ")" if only_connection_ids else ""
)
table_filter = (
    " AND c.source_table_id IN (" + ", ".join(
        escape_string_literal(value) for value in sorted(only_source_table_ids)
    ) + ")" if only_source_table_ids else ""
)
eligible = spark.sql(f"""
    SELECT c.*
    FROM {ctrl('source_table_control')} c
    JOIN {ctrl('source_connection')} sc
      ON c.connection_id = sc.connection_id
    WHERE c.is_active = true
      AND c.initial_load_completed = true
      AND c.table_decision = 'AUTO_MIGRATE'
      AND c.load_strategy IN ('WATERMARK','PRIMARY_KEY','HYBRID','FULL_LOAD')
      AND c.source_identity_version = {SOURCE_IDENTITY_VERSION}
      AND sc.is_active = true
      AND sc.connection_status = 'VALID'
      AND sc.secret_scope IS NOT NULL AND trim(sc.secret_scope) <> ''
      AND lower(trim(c.source_system)) = lower(trim(sc.source_system))
      {connection_filter}
      {table_filter}
    ORDER BY c.connection_id, c.source_schema, c.source_table,
             c.source_table_id
""").collect()
print("Eligible tables for delta:", len(eligible))

existing_queue_rows = spark.sql(f"""
    SELECT connection_id, source_table_id
    FROM {ctrl('delta_sync_queue')}
    WHERE run_id = {escape_string_literal(run_id)}
""").collect()
existing_queue_keys = {
    (row["connection_id"], row["source_table_id"])
    for row in existing_queue_rows
}
if len(existing_queue_keys) != len(existing_queue_rows):
    raise ValueError(
        "delta_sync_queue contains duplicate run_id + connection_id + "
        "source_table_id keys")

# COMMAND ----------

queue = []
skipped = []
connection_cache = {}
adapter_cache = {}
for r in eligible:
    d = r.asDict()
    conn_id = require_connection_id(
        d.get("connection_id"), "delta preparation registration")
    src_id = d["source_table_id"]
    src_system = require_source_system(
        d.get("source_system"), "source_table_control row")
    if (conn_id, src_id) in existing_queue_keys:
        skipped.append((conn_id, src_id, "EXISTING_FROZEN_WORK_UNIT"))
        print(f"  SKIP {conn_id}/{src_id}: queue row already exists for run")
        continue

    if conn_id not in connection_cache:
        connection_cache[conn_id] = require_valid_connection(
            conn_id, src_system)
    connection = connection_cache[conn_id]
    assert_current_source_table_identity(r, connection)
    connection_data = connection.asDict()
    src_server = connection_data.get("source_server")
    src_db = resolve_effective_source_database(d, connection_data)
    s_schema, s_table = r["source_schema"], r["source_table"]
    t_catalog, t_schema, t_table = validate_target_identity(
        r["target_catalog"], r["target_schema"], r["target_table"]
    )
    stage_table = normalize_target_identifier(f"{t_table}_stage", identifier_type="stage")
    strategy = r["load_strategy"]
    pk = list(r["primary_key_columns"]) if r["primary_key_columns"] else []
    wm_col = r["watermark_column"]
    wm_type = r["watermark_data_type"]
    last_wm = r["last_watermark_value"]
    upper_wm = None

    try:
        if conn_id not in adapter_cache:
            adapter_cache[conn_id] = get_source_adapter_for_connection(connection)
        adapter = adapter_cache[conn_id]

        # Use only the latest approved AUTO target columns from one complete snapshot.
        latest_mapping_rows = spark.sql(f"""
            SELECT run_id
            FROM (
              SELECT run_id,
                     ROW_NUMBER() OVER (
                       ORDER BY max(captured_ts) DESC NULLS LAST, run_id DESC
                     ) AS rn
              FROM {ctrl('resolved_column_mappings')}
              WHERE connection_id = {escape_string_literal(conn_id)}
                AND source_table_id = {escape_string_literal(src_id)}
              GROUP BY run_id
            )
            WHERE rn = 1
        """).collect()
        if not latest_mapping_rows:
            raise ValueError(
                f"No mapping run found in resolved_column_mappings for "
                f"connection_id={conn_id!r}, source_table_id={src_id!r}"
            )
        selected_run_id = latest_mapping_rows[0]["run_id"]
        selected_run_id, included_mappings = get_complete_mapping_snapshot(
            conn_id, src_id, run_id=selected_run_id
        )
        approved_columns = [row["column_name"] for row in included_mappings]

        if strategy in ("PRIMARY_KEY", "HYBRID") and not pk:
            message = f"{strategy} strategy requires primary_key_columns"
            repo.update_control_for_connection(conn_id, src_id, {
                "current_status": "DELTA_CONFIG_ERROR",
                "error_message": message,
            })
            skipped.append((s_schema, s_table, "NO_PRIMARY_KEY"))
            print(f"  SKIP {s_schema}.{s_table}: {message}")
            continue
        missing_projected_pk = [column for column in pk
                                if column not in approved_columns]
        if strategy in ("PRIMARY_KEY", "HYBRID") and missing_projected_pk:
            raise ValueError(
                "primary-key column(s) are not approved for extraction: "
                + ", ".join(missing_projected_pk))
        # FULL_LOAD refreshes the whole table: complete extract, no PK or
        # watermark required; the queue row carries null watermark fields.
        if strategy == "FULL_LOAD":
            src_query = adapter.full_extract_query(
                src_db, s_schema, s_table, columns=approved_columns,
                watermark_column=wm_col, watermark_type=wm_type)
            wm_col = wm_type = last_wm = upper_wm = None
        # PRIMARY_KEY tables have no watermark: re-extract everything and MERGE by PK.
        elif strategy == "PRIMARY_KEY":
            src_query = adapter.full_extract_query(
                src_db, s_schema, s_table, columns=approved_columns,
                watermark_column=wm_col, watermark_type=wm_type)
            upper_wm = None
        else:
            # WATERMARK / HYBRID uses one frozen interval per table and run.
            if not wm_col or not wm_type:
                message = "watermark column/type missing"
                repo.update_control_for_connection(conn_id, src_id, {
                    "current_status": "DELTA_CONFIG_ERROR",
                    "error_message": message,
                })
                print(f"  SKIP {s_schema}.{s_table}: {message}.")
                skipped.append((s_schema, s_table, "NO_WATERMARK_CONFIG"))
                continue
            # Defensive invariant: even if a control row was hand-edited or a
            # stale non-temporal value survived a prior run, WATERMARK/HYBRID may
            # use only this source's supported temporal watermark types (so SQL
            # Server rowversion/timestamp can never slip through as temporal).
            normalized_wm_type = adapter.normalize_watermark_type(wm_type)
            if not adapter.is_supported_watermark_type(normalized_wm_type):
                message = (
                    "WATERMARK/HYBRID requires a supported temporal "
                    f"watermark type; received {wm_type!r} "
                    f"for column {wm_col!r}"
                )
                repo.update_control_for_connection(conn_id, src_id, {
                    "current_status": "DELTA_CONFIG_ERROR",
                    "error_message": message,
                })
                skipped.append((s_schema, s_table, "NON_TEMPORAL_WATERMARK"))
                print(f"  SKIP {s_schema}.{s_table}: {message}")
                continue
            if last_wm is None:
                message = "no committed last_watermark_value after initial load"
                repo.update_control_for_connection(conn_id, src_id, {
                    "current_status": "DELTA_CONFIG_ERROR",
                    "error_message": message,
                })
                print(f"  SKIP {s_schema}.{s_table}: {message}.")
                skipped.append((s_schema, s_table, "NO_LAST_WATERMARK"))
                continue
            # Capture the upper bound exactly once here.
            upper_raw, upper_wm = capture_upper_watermark(
                adapter, src_db, s_schema, s_table, wm_col, wm_type, src_server)
            if upper_raw is None:
                repo.update_control_for_connection(conn_id, src_id, {
                    "current_status": "NO_SOURCE_WATERMARK",
                    "error_message": None,
                })
                print(f"  SKIP {s_schema}.{s_table}: MAX({wm_col}) is null.")
                skipped.append((s_schema, s_table, "NO_SOURCE_WATERMARK"))
                continue
            last_cmp = coerce_watermark(last_wm, wm_type, adapter)
            upper_cmp = coerce_watermark(upper_raw, wm_type, adapter)
            if not (upper_cmp > last_cmp):
                repo.update_control_for_connection(conn_id, src_id, {
                    "current_status": "NO_CHANGES",
                    "error_message": None,
                })
                print(f"  SKIP {s_schema}.{s_table}: no change "
                      f"(upper={upper_wm} last={last_wm}).")
                skipped.append((s_schema, s_table, "NO_CHANGES"))
                continue
            src_query = adapter.incremental_extract_query(
                src_db, s_schema, s_table, wm_col, wm_type,
                last_wm, upper_wm, columns=approved_columns)

        queue_row = Row(
            run_id=run_id, source_table_id=src_id,
            connection_id=conn_id, source_system=src_system,
            source_server=src_server, source_database=src_db,
            source_schema=s_schema, source_table=s_table,
            target_catalog=t_catalog, target_schema=t_schema, target_table=t_table,
            stage_table=stage_table, load_strategy=strategy,
            delete_policy=d.get("delete_policy"),
            primary_key_columns=pk if pk else None,
            watermark_column=wm_col, watermark_data_type=wm_type,
            last_watermark_value=last_wm, upper_watermark_value=upper_wm,
            source_query=src_query, status="QUEUED",
        )
        queued_status = ("DELTA_FULL_REFRESH_QUEUED"
                         if strategy == "FULL_LOAD" else "DELTA_QUEUED")
        repo.update_control_for_connection(conn_id, src_id, {
            "current_status": queued_status,
            "error_message": None,
        })
        queue.append(queue_row)
        existing_queue_keys.add((conn_id, src_id))
    except Exception as e:
        safe_error = failcls.sanitize_message(e)
        try:
            repo.update_control_for_connection(conn_id, src_id, {
                "current_status": "DELTA_PREP_FAILED",
                "error_message": safe_error[:1000],
            })
        except Exception as update_error:
            safe_update_error = failcls.sanitize_message(update_error)
            print(f"  [warn] failed to record prep error: {safe_update_error[:300]}")
        skipped.append((s_schema, s_table, "PREP_FAILED"))
        print(f"  FAILED prep [{src_system}] {s_schema}.{s_table}: "
              f"{safe_error[:300]}")

print(f"Prepared {len(queue)} work item(s); skipped {len(skipped)}.")

# COMMAND ----------

if queue:
    queue_schema = StructType([
        StructField("run_id", StringType(), False),
        StructField("source_table_id", StringType(), False),
        StructField("connection_id", StringType(), False),
        StructField("source_system", StringType(), False),
        StructField("source_server", StringType(), True),
        StructField("source_database", StringType(), True),
        StructField("source_schema", StringType(), False),
        StructField("source_table", StringType(), False),
        StructField("target_catalog", StringType(), False),
        StructField("target_schema", StringType(), False),
        StructField("target_table", StringType(), False),
        StructField("stage_table", StringType(), False),
        StructField("load_strategy", StringType(), False),
        StructField("delete_policy", StringType(), True),
        StructField("primary_key_columns", ArrayType(StringType()), True),
        StructField("watermark_column", StringType(), True),
        StructField("watermark_data_type", StringType(), True),
        StructField("last_watermark_value", StringType(), True),
        StructField("upper_watermark_value", StringType(), True),
        StructField("source_query", StringType(), False),
        StructField("status", StringType(), False),
    ])
    df = (spark.createDataFrame(
              [row.asDict() for row in queue],
              schema=queue_schema)
          .withColumn("captured_ts", F.current_timestamp()))
    df.write.format("delta").mode("append").option(
        "mergeSchema", "true").saveAsTable(
        ctrl("delta_sync_queue").replace("`", ""))
    print(f"Queued {len(queue)} tables.")
else:
    print("No tables eligible for delta sync.")

dbutils.notebook.exit(json.dumps({"status": "SUCCEEDED", "run_id": run_id,
                                  "queued": len(queue),
                                  "skipped": len(skipped)}))
