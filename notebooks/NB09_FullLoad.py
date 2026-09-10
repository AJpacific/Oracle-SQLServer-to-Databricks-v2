# Databricks notebook source
# MAGIC %md
# MAGIC # NB09_FullLoad
# MAGIC Reads each AUTO_MIGRATE Oracle or SQL Server table through its source
# MAGIC adapter and writes it to the target Delta table (overwrite). Records source/target counts to
# MAGIC table_run_log. Designed to run per-table inside a ForEach Job task, or
# MAGIC loop over all AUTO_MIGRATE tables when run standalone.

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

# Optional single-table parameters (used when driven by a ForEach task).
dbutils.widgets.text("only_source_system", "")
dbutils.widgets.text("only_source_server", "")
dbutils.widgets.text("only_source_database", "")
dbutils.widgets.text("only_source_schema", "")
dbutils.widgets.text("only_source_table", "")
dbutils.widgets.text("only_source_table_id", "")
only_system = dbutils.widgets.get("only_source_system").strip()
only_server = dbutils.widgets.get("only_source_server").strip()
only_database = dbutils.widgets.get("only_source_database").strip()
only_schema = dbutils.widgets.get("only_source_schema").strip()
only_table = dbutils.widgets.get("only_source_table").strip()
only_id = dbutils.widgets.get("only_source_table_id").strip()

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
print("run_id:", run_id, "| single table:",
      only_id or (f"{only_schema}.{only_table}" if only_table else "(all)"))
repo = control_repo()

def ctrl(t):
    return f"{quote_databricks(CATALOG)}.{quote_databricks(CONTROL_SCHEMA)}.{quote_databricks(t)}"

# COMMAND ----------

auto = repo.active_tables(decision="AUTO_MIGRATE").collect()
if only_id:
    auto = [r for r in auto if r["source_table_id"] == only_id]
elif only_schema or only_table or only_system or only_server or only_database:
    if not (only_schema and only_table):
        raise ValueError("Manual filtering requires both only_source_schema and only_source_table")
    def _matches_manual_filter(r):
        d = r.asDict()
        if only_system and normalize_source_system(d.get("source_system") or "oracle") != normalize_source_system(only_system):
            return False
        if only_server and (d.get("source_server") or "").lower() != only_server.lower():
            return False
        if only_database and (d.get("source_database") or "").lower() != only_database.lower():
            return False
        return r["source_schema"] == only_schema and r["source_table"] == only_table
    auto = [r for r in auto if _matches_manual_filter(r)]
    if len(auto) > 1:
        raise ValueError(
            "Ambiguous manual source filter. Use only_source_table_id or add "
            "only_source_system, only_source_server, and only_source_database."
        )
print("Tables to full-load:", len(auto))

# COMMAND ----------

from pyspark.sql import Row

def log_run(ident, target_fqn, s_count, t_count, status, err, started):
    repo.log_table_run({
        "run_id": run_id,
        "source_table_id": ident["source_table_id"],
        "source_system": ident["source_system"],
        "source_server": ident["source_server"],
        "source_database": ident["source_database"],
        "source_schema": ident["source_schema"], "source_table": ident["source_table"],
        "operation": "FULL_LOAD", "target_full_name": target_fqn,
        "source_row_count": s_count, "target_row_count": t_count,
        "status": status, "error_message": err,
        "started_ts": started, "ended_ts": now_utc(),
    })

# COMMAND ----------

succeeded, failed = 0, 0

for r in auto:
    d = r.asDict()
    src_id = d["source_table_id"]
    src_system = d.get("source_system") or "oracle"
    src_server = d.get("source_server")
    src_db = d.get("source_database")
    s_schema, s_table = r["source_schema"], r["source_table"]
    ident = {"source_table_id": src_id, "source_system": src_system,
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
    try:
        adapter = get_source_adapter_for_row(r)

        # Approved mappings define the target's typed schema (built by NB08).
        mrows = spark.sql(f"""
            SELECT column_name, databricks_delta_type, is_nullable, ordinal_position
            FROM {ctrl('resolved_column_mappings')}
            WHERE run_id = {escape_string_literal(run_id)}
              AND source_table_id = {escape_string_literal(src_id)}
            ORDER BY ordinal_position
        """).collect()
        if not mrows:
            raise Exception("no resolved mappings found for this run")

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
                WHERE run_id = {escape_string_literal(run_id)}
                  AND source_table_id = {escape_string_literal(src_id)}
                  AND column_name   = {escape_string_literal(pk_col)}
                ORDER BY captured_ts DESC
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
        (conform_to_table(src_df, target_fqn)
         .write.format("delta").mode(write_mode).saveAsTable(target_fqn))

        t_count = spark.table(target_fqn).count()
        status = "SUCCEEDED" if t_count == s_count else "COUNT_MISMATCH"
        if status == "SUCCEEDED":
            repo.update_control(src_id, {
                "current_status": "FULL_LOADED",
                "error_message": None,
            })
            succeeded += 1
            print(f"  loaded {target_fqn}: {t_count} rows")
        else:
            repo.update_control(src_id, {
                "current_status": "FULL_LOAD_COUNT_MISMATCH",
                "error_message": f"source={s_count} target={t_count}",
            })
            failed += 1
            print(f"  COUNT MISMATCH {target_fqn}: src={s_count} tgt={t_count}")
        log_run(ident, target_fqn, s_count, t_count, status, None, started)
    except Exception as e:
        failed += 1
        try:
            repo.update_control(src_id, {
                "current_status": "FULL_LOAD_FAILED",
                "error_message": str(e)[:1000],
            })
        except Exception as update_error:
            print(f"  [warn] failed to record control error: {update_error}")
        try:
            log_run(ident, target_fqn, s_count, t_count,
                    "FAILED", str(e)[:1000], started)
        except Exception as log_error:
            print(f"  [warn] failed to write table audit: {log_error}")
        print(f"  FAILED [{src_system}] {s_schema}.{s_table}: {e}")
    finally:
        if src_df is not None:
            try:
                src_df.unpersist()
            except Exception as unpersist_error:
                print(f"  [warn] failed to unpersist source data: {unpersist_error}")

# COMMAND ----------

print(f"Full load complete. succeeded={succeeded} failed={failed}")
if failed > 0:
    raise Exception(f"{failed} table(s) failed full load; see table_run_log.")

dbutils.notebook.exit(json.dumps({"status": "SUCCEEDED", "run_id": run_id,
                                  "loaded": succeeded}))
