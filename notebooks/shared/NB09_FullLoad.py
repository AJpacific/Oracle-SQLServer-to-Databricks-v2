# Databricks notebook source
# MAGIC %md
# MAGIC # NB09_FullLoad
# MAGIC Reads each AUTO_MIGRATE source table through its registered source adapter
# MAGIC and writes it to the target Delta table (overwrite). Records source/target
# MAGIC counts to table_run_log. Runs for exactly one connection-owned table
# MAGIC work item inside a ForEach task.

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

# Backward-compatible alias for older task parameter mappings.
dbutils.widgets.text("only_source_table_id", "")
only_id = dbutils.widgets.get("only_source_table_id").strip()

# Retry / ForEach parameters. A retry task passes source_table_id (scoping this
# run to one table), plus parent_run_id and attempt_number for lineage.
dbutils.widgets.text("source_table_id", "")
dbutils.widgets.text("parent_run_id", "")
dbutils.widgets.text("attempt_number", "1")
dbutils.widgets.text("recovery_action", "")
_std_id = dbutils.widgets.get("source_table_id").strip()
if _std_id and not only_id:
    only_id = _std_id
parent_run_id = dbutils.widgets.get("parent_run_id").strip() or None
try:
    attempt_number = int(dbutils.widgets.get("attempt_number").strip() or "1")
except ValueError:
    attempt_number = 1

# Full-load write mode policy: overwrite is the default replacement policy.
# Onboarding is an idempotent snapshot replacement.
write_mode = "overwrite"

# Parallelism for the JDBC read (used only when a numeric PK bound is available).
dbutils.widgets.text("num_partitions", "8")
try:
    NUM_PARTITIONS = int(dbutils.widgets.get("num_partitions").strip() or "1")
except ValueError:
    NUM_PARTITIONS = 1

run_id = get_run_id()
connection_id = require_connection_id(CONNECTION_ID, "Full Load work item")
only_id = str(only_id or SOURCE_TABLE_ID or "").strip()
if not only_id:
    raise ValueError("Full Load work item requires source_table_id")
connection = require_valid_connection(connection_id)
print("run_id:", run_id, "| connection_id:", connection_id,
    "| source_table_id:", only_id)
repo = control_repo()

def ctrl(t):
    return f"{quote_databricks(CATALOG)}.{quote_databricks(CONTROL_SCHEMA)}.{quote_databricks(t)}"

# COMMAND ----------

registration = repo.get_source_table(connection_id, only_id)
if registration is None:
    raise ValueError(
        f"connection_id {connection_id!r} and source_table_id {only_id!r} "
        "do not identify a registered table")
assert_table_connection_match(registration, connection_id)
assert_current_source_table_identity(registration, connection)
registration_data = registration.asDict()
if not registration_data.get("is_active"):
    raise ValueError("Full Load registration is not active")
if (registration_data.get("table_decision") or "").upper() != "AUTO_MIGRATE":
    raise ValueError("Full Load registration is not AUTO_MIGRATE")
for target_field in ("target_catalog", "target_schema", "target_table"):
    if not str(registration_data.get(target_field) or "").strip():
        raise ValueError(f"Full Load registration requires {target_field}")
auto = [registration]

# COMMAND ----------

from pyspark.sql import Row

def log_run(ident, target_fqn, s_count, t_count, status, err, started, extra=None):
    fields = {
        "run_id": run_id,
        "source_table_id": ident["source_table_id"],
        "connection_id": ident.get("connection_id"),
        "source_system": ident["source_system"],
        "source_server": ident["source_server"],
        "source_database": ident["source_database"],
        "source_schema": ident["source_schema"], "source_table": ident["source_table"],
        "operation": "FULL_LOAD", "target_full_name": target_fqn,
        "source_row_count": s_count, "target_row_count": t_count,
        "status": status, "error_message": err,
        "attempt_number": attempt_number, "parent_run_id": parent_run_id,
        "started_ts": started, "ended_ts": now_utc(),
    }
    if extra:
        fields.update(extra)
    repo.log_table_run(fields)

# COMMAND ----------

succeeded, failed = 0, 0

for r in auto:
    d = r.asDict()
    conn_id = connection_id
    src_id = d["source_table_id"]
    src_system = require_source_system(
        d.get("source_system"), "source_table_control row")
    connection_data = connection.asDict()
    src_server = connection_data.get("source_server")
    src_db = resolve_effective_source_database(d, connection_data)
    s_schema, s_table = r["source_schema"], r["source_table"]
    ident = {"source_table_id": src_id, "source_system": src_system,
             "connection_id": conn_id,
             "source_server": src_server, "source_database": src_db,
             "source_schema": s_schema, "source_table": s_table}
    t_catalog = r["target_catalog"] or CATALOG
    t_schema = r["target_schema"] or s_schema.lower()
    t_table = r["target_table"] or s_table.lower()
    target_fqn = f"{t_catalog}.{t_schema}.{t_table}"
    started = now_utc()
    src_df = None
    s_count = None
    t_count = None
    current_stage = failcls.METADATA
    try:
        current_stage = failcls.PROVISIONING
        target_owners = spark.sql(f"""
            SELECT connection_id, source_table_id
            FROM {ctrl('source_table_control')}
            WHERE is_active = true
              AND lower(concat_ws('.', coalesce(target_catalog, '{CATALOG}'),
                                  coalesce(target_schema, lower(source_schema)),
                                  coalesce(target_table, lower(source_table)))) =
                  {escape_string_literal(target_fqn.lower())}
        """).collect()
        if (len(target_owners) != 1
                or target_owners[0]["connection_id"] != conn_id
                or target_owners[0]["source_table_id"] != src_id):
            raise ValueError(
                "target FQN collision: the Bronze target is not exclusively "
                "owned by this connection registration")

        current_stage = failcls.CONNECTION
        adapter = get_source_adapter_for_connection(connection)
        current_stage = failcls.METADATA

        # Approved mappings define the target's typed schema (built by NB08).
        latest_mappings = spark.sql(f"""
            SELECT column_name, databricks_delta_type, is_nullable,
                   ordinal_position, mapping_status, include_column
            FROM (
              SELECT column_name, databricks_delta_type, is_nullable,
                     ordinal_position, mapping_status, include_column,
                     ROW_NUMBER() OVER (
                       PARTITION BY column_name
                       ORDER BY captured_ts DESC NULLS LAST, run_id DESC
                     ) AS rn
              FROM {ctrl('resolved_column_mappings')}
              WHERE connection_id = {escape_string_literal(conn_id)}
                AND source_table_id = {escape_string_literal(src_id)}
            )
            WHERE rn = 1
            ORDER BY ordinal_position
        """).collect()
        mrows = [row for row in latest_mappings
                 if row["include_column"] is not False]
        if not mrows:
            raise Exception("no included resolved mappings found")
        unsafe_mappings = [
            row["column_name"] for row in mrows
            if (row["mapping_status"] or "").upper() != "AUTO"
            or not row["databricks_delta_type"]
        ]
        if unsafe_mappings:
            raise ValueError(
                "latest mappings are not safe for Full Load: "
                + ", ".join(unsafe_mappings))

        # Ensure the typed empty table exists even if NB08 wasn't run this session.
        if not spark.catalog.tableExists(target_fqn):
            col_specs = [(m["column_name"], m["databricks_delta_type"], bool(m["is_nullable"]))
                         for m in mrows]
            spark.sql(ddl.build_create_schema(t_catalog, t_schema, "migrated data"))
            spark.sql(ddl.build_create_table(t_catalog, t_schema, t_table, col_specs))

        # Parallelise the read only on a genuinely bounded integral PK. The
        # source adapter decides eligibility (Oracle bounded NUMBER; SQL Server
        # native tinyint/smallint/int/bigint); everything else uses a
        # correctness-safe unpartitioned read.
        pk = list(r["primary_key_columns"]) if r["primary_key_columns"] else []
        part_col = normalized_min = normalized_max = effective_partitions = None
        if NUM_PARTITIONS <= 1:
            part_reason = "num_partitions <= 1"
        elif len(pk) != 1:
            part_reason = "partitioning requires exactly one primary-key column"
        else:
            pk_col = pk[0]
            inv = spark.sql(f"""
                SELECT data_type, numeric_precision, numeric_scale
                FROM {ctrl('source_inventory')}
                                WHERE connection_id = {escape_string_literal(conn_id)}
                  AND source_table_id = {escape_string_literal(src_id)}
                  AND column_name   = {escape_string_literal(pk_col)}
                                ORDER BY captured_ts DESC NULLS LAST, run_id DESC
                LIMIT 1
            """).collect()
            pk_target = next((m["databricks_delta_type"] for m in mrows
                              if m["column_name"] == pk_col), None)
            if not inv:
                part_reason = f"no current-run source metadata for PK {pk_col}"
            else:
                meta = inv[0]
                mm = read_source_jdbc(
                    adapter, adapter.min_max_query(src_db, s_schema, s_table, pk_col),
                    source_server=src_server, source_database=src_db).collect()[0]
                effective_partitions, normalized_min, normalized_max, part_reason = \
                    adapter.resolve_partition_plan(
                        {"data_type": meta["data_type"],
                         "numeric_precision": meta["numeric_precision"],
                         "numeric_scale": meta["numeric_scale"]},
                        pk_target, mm["MIN_VAL"], mm["MAX_VAL"], NUM_PARTITIONS)
                if part_reason is None:
                    part_col = pk_col

        extract_columns = [m["column_name"] for m in mrows]
        extract = adapter.full_extract_query(
            src_db, s_schema, s_table, columns=extract_columns,
            watermark_column=d.get("watermark_column"),
            watermark_type=d.get("watermark_data_type"))
        current_stage = failcls.SOURCE_READ
        if part_col:
            src_df = read_source_jdbc(
                adapter, extract, source_server=src_server, source_database=src_db,
                partition_column=part_col, lower_bound=normalized_min,
                upper_bound=normalized_max, num_partitions=effective_partitions).cache()
        else:
            print(f"  JDBC partitioning disabled for [{src_system}] {s_schema}.{s_table}: "
                  f"{part_reason}; using correctness-safe unpartitioned read.")
            src_df = read_source_jdbc(
                adapter, extract, source_server=src_server,
                source_database=src_db).cache()

        # Count and write the same cached JDBC snapshot.
        s_count = src_df.count()

        # Conform JDBC data to the approved typed schema, then load.
        current_stage = failcls.TARGET_WRITE
        (conform_to_table(src_df, target_fqn)
         .write.format("delta").mode(write_mode).saveAsTable(target_fqn))

        current_stage = failcls.RECONCILIATION
        t_count = spark.table(target_fqn).count()
        counts_match = (t_count == s_count)
        if counts_match:
            repo.update_control_for_connection(conn_id, src_id, {
                "current_status": "FULL_LOADED",
                "error_message": None,
            })
            succeeded += 1
            print(f"  loaded {target_fqn}: {t_count} rows")
            log_run(ident, target_fqn, s_count, t_count, "SUCCEEDED", None, started,
                    extra={"extracted_row_count": s_count,
                           "applied_row_count": t_count})
        else:
            # Detailed operational state stays on the control row; table_run_log
            # status is normalized so the retry selector can filter on FAILED.
            repo.update_control_for_connection(conn_id, src_id, {
                "current_status": "FULL_LOAD_COUNT_MISMATCH",
                "error_message": f"source={s_count} target={t_count}",
            })
            failed += 1
            print(f"  COUNT MISMATCH {target_fqn}: src={s_count} tgt={t_count}")
            log_run(ident, target_fqn, s_count, t_count, "FAILED",
                    f"full-load count mismatch: source={s_count} target={t_count}",
                    started,
                    extra={"failure_stage": failcls.RECONCILIATION,
                           "error_category": failcls.RECONCILIATION_ERROR,
                           "retry_eligible": False,
                           "extracted_row_count": s_count,
                           "applied_row_count": t_count})
    except Exception as e:
        failed += 1
        cls = failcls.classify_failure(e, current_stage, idempotent=True)
        try:
            repo.update_control_for_connection(conn_id, src_id, {
                "current_status": "FULL_LOAD_FAILED",
                "error_message": cls.sanitized_message[:1000],
            })
        except Exception as update_error:
            safe_update_error = failcls.sanitize_message(update_error)
            print(f"  [warn] failed to record control error: {safe_update_error[:300]}")
        try:
            log_run(ident, target_fqn, s_count, t_count,
                    "FAILED", cls.sanitized_message[:1000], started,
                    extra={"failure_stage": cls.stage,
                           "error_category": cls.category,
                           "retry_eligible": cls.retry_eligible})
        except Exception as log_error:
            safe_log_error = failcls.sanitize_message(log_error)
            print(f"  [warn] failed to write table audit: {safe_log_error[:300]}")
        print(f"  FAILED [{src_system}] {s_schema}.{s_table}: "
              f"{cls.sanitized_message[:300]}")
    finally:
        if src_df is not None:
            try:
                src_df.unpersist()
            except Exception as unpersist_error:
                safe_unpersist_error = failcls.sanitize_message(unpersist_error)
                print(f"  [warn] failed to unpersist source data: "
                      f"{safe_unpersist_error[:300]}")

# COMMAND ----------

print(f"Full load complete. succeeded={succeeded} failed={failed}")
if failed > 0:
    raise Exception(f"{failed} table(s) failed full load; see table_run_log.")

dbutils.notebook.exit(json.dumps({"status": "SUCCEEDED", "run_id": run_id,
                                  "connection_id": connection_id,
                                  "source_table_id": only_id,
                                  "loaded": succeeded}))
