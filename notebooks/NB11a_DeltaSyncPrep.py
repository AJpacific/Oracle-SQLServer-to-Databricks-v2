# Databricks notebook source
# MAGIC %md
# MAGIC # NB11a_DeltaSyncPrep
# MAGIC Builds the Pipeline 2 workload queue for every eligible table after the
# MAGIC initial load. WATERMARK and HYBRID use a bounded temporal extract,
# MAGIC PRIMARY_KEY uses a complete source extract for MERGE, and FULL_LOAD uses
# MAGIC a complete source extract for target refresh. SQL Server datetime2
# MAGIC watermarks use the approved six-fractional-digit AUTO policy: the source
# MAGIC MAX and incremental predicate are normalized to datetime2(6).

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

from pyspark.sql import functions as F, Row
from pyspark.sql.types import ArrayType, StringType, StructField, StructType

run_id = get_run_id()
set_task_value("run_id", run_id)
print("run_id:", run_id)
repo = control_repo()

def ctrl(t):
    return f"{quote_databricks(CATALOG)}.{quote_databricks(CONTROL_SCHEMA)}.{quote_databricks(t)}"

# COMMAND ----------

from datetime import datetime, date, timezone


def coerce_watermark(value, watermark_type, adapter):
    """Return a timezone-aware (UTC) datetime for temporal watermark values.

    Any temporal type supported by this row's source adapter (Oracle DATE/
    TIMESTAMP families, SQL Server DATE/DATETIME/DATETIME2/DATETIMEOFFSET/
    SMALLDATETIME) parses to an aware UTC datetime so ordering is chronological
    and naive/ISO/offset checkpoints compare consistently - never a lexical
    string compare. Non-temporal types (e.g. SQL Server rowversion) are rejected.
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
    """Capture MAX once and return (raw, canonical_utc).

    For SQL Server datetime2, the adapter's query builder normalizes MAX to
    datetime2(6) before JDBC returns the value, matching Databricks microseconds.
    """
    rows = read_source_jdbc(
        adapter, adapter.upper_watermark_query(database, schema, table, wm_col, wm_type),
        source_server=server, source_database=database).collect()
    raw = rows[0]["UPPER_WATERMARK"] if rows else None
    canonical = None if raw is None else sqlb.canonical_watermark_string(raw, strict=True)
    return raw, canonical

# COMMAND ----------

eligible = spark.sql(f"""
    SELECT * FROM {ctrl('source_table_control')}
    WHERE is_active = true
      AND initial_load_completed = true
      AND table_decision = 'AUTO_MIGRATE'
      AND load_strategy IN ('WATERMARK','PRIMARY_KEY','HYBRID','FULL_LOAD')
""").collect()
print("Eligible tables for delta:", len(eligible))

# COMMAND ----------

queue = []
skipped = []
for r in eligible:
    d = r.asDict()
    src_id = d["source_table_id"]
    src_system = d.get("source_system") or "oracle"
    src_server = d.get("source_server")
    src_db = d.get("source_database")
    s_schema, s_table = r["source_schema"], r["source_table"]
    t_catalog = r["target_catalog"] or CATALOG
    t_schema = r["target_schema"] or s_schema.lower()
    t_table = r["target_table"] or s_table.lower()
    stage_table = f"{t_table}_stage"
    strategy = r["load_strategy"]
    pk = list(r["primary_key_columns"]) if r["primary_key_columns"] else []
    wm_col = r["watermark_column"]
    wm_type = r["watermark_data_type"]
    last_wm = r["last_watermark_value"]
    upper_wm = None

    try:
        adapter = get_source_adapter_for_row(r)

        # Use only the latest approved AUTO target columns. For SQL Server
        # DATETIME2 this lets the builder project the watermark itself as
        # datetime2(6), matching the predicate and Delta target representation.
        approved_columns = [
            x["column_name"]
            for x in spark.sql(f"""
                SELECT column_name, ordinal_position
                FROM {ctrl('resolved_column_mappings')}
                WHERE source_table_id = {escape_string_literal(src_id)}
                  AND mapping_status = 'AUTO'
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY column_name ORDER BY captured_ts DESC
                ) = 1
                ORDER BY ordinal_position
            """).collect()
        ]
        if not approved_columns:
            raise ValueError("no approved AUTO columns found for delta extraction")

        if strategy in ("PRIMARY_KEY", "HYBRID") and not pk:
            message = f"{strategy} strategy requires primary_key_columns"
            repo.update_control(src_id, {
                "current_status": "DELTA_CONFIG_ERROR",
                "error_message": message,
            })
            skipped.append((s_schema, s_table, "NO_PRIMARY_KEY"))
            print(f"  SKIP {s_schema}.{s_table}: {message}")
            continue
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
                repo.update_control(src_id, {
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
                repo.update_control(src_id, {
                    "current_status": "DELTA_CONFIG_ERROR",
                    "error_message": message,
                })
                skipped.append((s_schema, s_table, "NON_TEMPORAL_WATERMARK"))
                print(f"  SKIP {s_schema}.{s_table}: {message}")
                continue
            if last_wm is None:
                message = "no committed last_watermark_value after initial load"
                repo.update_control(src_id, {
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
                repo.update_control(src_id, {
                    "current_status": "NO_SOURCE_WATERMARK",
                    "error_message": None,
                })
                print(f"  SKIP {s_schema}.{s_table}: MAX({wm_col}) is null.")
                skipped.append((s_schema, s_table, "NO_SOURCE_WATERMARK"))
                continue
            last_cmp = coerce_watermark(last_wm, wm_type, adapter)
            upper_cmp = coerce_watermark(upper_raw, wm_type, adapter)
            if not (upper_cmp > last_cmp):
                repo.update_control(src_id, {
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
            run_id=run_id, source_table_id=src_id, source_system=src_system,
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
        repo.update_control(src_id, {
            "current_status": queued_status,
            "error_message": None,
        })
        queue.append(queue_row)
    except Exception as e:
        try:
            repo.update_control(src_id, {
                "current_status": "DELTA_PREP_FAILED",
                "error_message": str(e)[:1000],
            })
        except Exception as update_error:
            print(f"  [warn] failed to record prep error: {update_error}")
        skipped.append((s_schema, s_table, "PREP_FAILED"))
        print(f"  FAILED prep [{src_system}] {s_schema}.{s_table}: {e}")

print(f"Prepared {len(queue)} work item(s); skipped {len(skipped)}.")

# COMMAND ----------

if queue:
    queue_schema = StructType([
        StructField("run_id", StringType(), False),
        StructField("source_table_id", StringType(), False),
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
    df.write.format("delta").mode("append").saveAsTable(
        ctrl("delta_sync_queue").replace("`", ""))
    print(f"Queued {len(queue)} tables.")
else:
    print("No tables eligible for delta sync.")

dbutils.notebook.exit(json.dumps({"status": "SUCCEEDED", "run_id": run_id,
                                  "queued": len(queue),
                                  "skipped": len(skipped)}))
