# Databricks notebook source
# MAGIC %md
# MAGIC # NB11b_DeltaSyncApply
# MAGIC Applies queued Pipeline 2 work according to strategy:
# MAGIC WATERMARK replaces and appends the bounded interval,
# MAGIC PRIMARY_KEY and HYBRID MERGE by primary key, and
# MAGIC FULL_LOAD completely overwrites the target table.
# MAGIC Watermarks are committed only after successful processing.

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

run_id = get_run_id()
print("run_id:", run_id)
repo = control_repo()

def ctrl(t):
    return f"{quote_databricks(CATALOG)}.{quote_databricks(CONTROL_SCHEMA)}.{quote_databricks(t)}"

# COMMAND ----------

queue = spark.sql(f"""
    SELECT * FROM {ctrl('delta_sync_queue')}
    WHERE run_id = {escape_string_literal(run_id)} AND status = 'QUEUED'
""").collect()
print("Queued items:", len(queue))

# COMMAND ----------

try:
    from src.strategy import normalize_watermark_type, is_supported_watermark_type
except ModuleNotFoundError:
    from strategy import normalize_watermark_type, is_supported_watermark_type


def _delta_wm_literal(value, family, adapter):
    "Render a watermark bound as a canonical UTC Delta TIMESTAMP literal."
    if not adapter.is_supported_watermark_type(family):
        raise ValueError(f"Unsupported non-temporal watermark type: {family!r}")
    if value is None:
        raise ValueError("watermark bound value is required")
    canonical = sqlb.canonical_watermark_string(value, strict=True)
    escaped = canonical.replace("'", "''")
    return f"CAST('{escaped}' AS TIMESTAMP)"

def log_run(ident, target_fqn, s_count, t_count,
            op, status, err, started):
    repo.log_table_run({
        "run_id": run_id,
        "source_table_id": ident["source_table_id"],
        "source_system": ident["source_system"],
        "source_server": ident["source_server"],
        "source_database": ident["source_database"],
        "source_schema": ident["source_schema"], "source_table": ident["source_table"],
        "operation": op, "target_full_name": target_fqn,
        "source_row_count": s_count, "target_row_count": t_count,
        "status": status, "error_message": err,
        "started_ts": started, "ended_ts": now_utc(),
    })

# COMMAND ----------

succeeded, failed = 0, 0
for q in queue:
    qd = q.asDict()
    src_id = qd["source_table_id"]
    src_system = qd.get("source_system") or "oracle"
    src_server = qd.get("source_server")
    src_db = qd.get("source_database")
    s_schema, s_table = q["source_schema"], q["source_table"]
    ident = {"source_table_id": src_id, "source_system": src_system,
             "source_server": src_server, "source_database": src_db,
             "source_schema": s_schema, "source_table": s_table}
    t_catalog, t_schema, t_table = q["target_catalog"], q["target_schema"], q["target_table"]
    stage_table = q["stage_table"]
    strategy = q["load_strategy"]
    pk = list(q["primary_key_columns"]) if q["primary_key_columns"] else []
    wm_col, wm_type = q["watermark_column"], q["watermark_data_type"]
    last_wm = q["last_watermark_value"]
    upper_wm = q["upper_watermark_value"]
    delete_policy = qd.get("delete_policy")
    plain_target = f"{t_catalog}.{t_schema}.{t_table}"
    plain_stage = f"{t_catalog}.{t_schema}.{stage_table}"
    target_sql = databricks_fqn(t_catalog, t_schema, t_table)
    started = now_utc()
    src_df = None
    s_count = None
    t_count = None
    op = "DELTA_SYNC"
    data_applied = False
    checkpoint_committed = False

    def _mark_queue(status):
        spark.sql(f"""
            UPDATE {ctrl('delta_sync_queue')} SET status = '{status}'
            WHERE run_id = {escape_string_literal(run_id)}
              AND source_table_id = {escape_string_literal(src_id)}
        """)

    try:
        # The adapter is chosen from the queue row's own source_system, so NB11b
        # never uses one global (Oracle) JDBC connection for every row.
        adapter = get_source_adapter_for_row(q)

        # Defense-in-depth: a stale/hand-edited queue row must never bypass the
        # temporal-only invariant enforced upstream in NB11a.
        if strategy in ("WATERMARK", "HYBRID"):
            if not wm_col or not wm_type:
                raise ValueError(
                    "Temporal delta queue item requires "
                    "watermark_column and watermark_data_type")
            if not adapter.is_supported_watermark_type(wm_type):
                raise ValueError(
                    "Unsupported non-temporal watermark type "
                    f"{wm_type!r} for {wm_col!r}")
        # Read this table's incremental (or full-for-PK) slice from its source,
        # using the queue row's source_server/source_database. Cache so the count
        # and write use the exact same source slice.
        src_df = read_source_jdbc(
            adapter, q["source_query"],
            source_server=src_server, source_database=src_db).cache()
        s_count = src_df.count()

        if strategy == "FULL_LOAD":
            # Atomic complete refresh: overwrite the whole target. Never append,
            # never MERGE. Retry simply overwrites again.
            (conform_to_table(src_df, plain_target)
             .write.format("delta").mode("overwrite").saveAsTable(plain_target))
            op = "DELTA_FULL_REFRESH"
        elif strategy == "WATERMARK":
            if not wm_col or last_wm is None or upper_wm is None:
                raise ValueError(
                    "WATERMARK queue item requires column, lower bound, and upper bound")
            lower_lit = _delta_wm_literal(last_wm, wm_type, adapter)
            upper_lit = _delta_wm_literal(upper_wm, wm_type, adapter)
            wm_sql = quote_databricks(wm_col)
            # Retry-safe replacement of exactly the frozen source interval.
            spark.sql(
                f"DELETE FROM {target_sql} "
                f"WHERE {wm_sql} > {lower_lit} "
                f"AND {wm_sql} <= {upper_lit}"
            )
            (conform_to_table(src_df, plain_target)
             .write.format("delta").mode("append").saveAsTable(plain_target))
            op = "DELTA_APPEND"
        else:
            # PRIMARY_KEY / HYBRID: stage then MERGE by PK (upsert)
            if not pk:
                raise Exception("MERGE strategy requires primary_key_columns")
            if strategy == "HYBRID" and upper_wm is None:
                raise ValueError("HYBRID queue item requires an upper watermark")
            (conform_to_table(src_df, plain_target)
             .write.format("delta").mode("overwrite")
             .option("overwriteSchema", "true").saveAsTable(plain_stage))
            # Propagate source deletes only from a COMPLETE snapshot (PRIMARY_KEY);
            # a HYBRID watermark slice can't tell a delete from an unchanged row.
            hard_delete = (strategy == "PRIMARY_KEY" and s_count > 0
                           and (delete_policy or "").upper() == "HARD_DELETE")
            spark.sql(ddl.build_merge_sql(t_catalog, t_schema, t_table,
                                          stage_table, pk,
                                          delete_unmatched=hard_delete))
            spark.sql(ddl.build_drop_table(t_catalog, t_schema, stage_table))
            op = "DELTA_MERGE"

        data_applied = True
        t_count = spark.table(plain_target).count()
        if strategy == "FULL_LOAD" and t_count != s_count:
            raise ValueError(
                f"FULL_LOAD refresh count mismatch: source={s_count} target={t_count}")
        log_run(ident, plain_target, s_count, t_count,
                op, "SUCCEEDED", None, started)

        # --- Stage: commit the control checkpoint (own failure state) ---------
        # PRIMARY_KEY writes no temporal watermark; only WATERMARK/HYBRID do.
        control_fields = {
            "last_successful_run_id": run_id,
            "last_successful_run_ts": now_utc().strftime("%Y-%m-%d %H:%M:%S.%f"),
            "current_status": ("DELTA_FULL_REFRESH_SUCCEEDED"
                               if strategy == "FULL_LOAD" else "DELTA_SYNCED"),
            "error_message": None,
        }
        try:
            if strategy in ("WATERMARK", "HYBRID"):
                control_fields["last_watermark_value"] = sqlb.canonical_watermark_string(
                    upper_wm, strict=True)
            repo.update_control(src_id, control_fields)
            checkpoint_committed = True
        except Exception as checkpoint_error:
            failed += 1
            try:
                _mark_queue("FAILED")
            except Exception as queue_error:
                print(f"  [warn] failed to mark queue row FAILED: {queue_error}")
            try:
                repo.update_control(src_id, {
                    "current_status": "CHECKPOINT_COMMIT_FAILED",
                    "error_message": ("Data application succeeded, but checkpoint "
                                      f"commit failed: {checkpoint_error}")[:1000],
                })
            except Exception as update_error:
                print(f"  [warn] failed to record checkpoint error: {update_error}")
            try:
                log_run(ident, plain_target, s_count, t_count,
                        "CHECKPOINT_COMMIT", "FAILED", str(checkpoint_error)[:1000], started)
            except Exception as log_error:
                print(f"  [warn] failed to write checkpoint audit: {log_error}")
            print(f"  CHECKPOINT COMMIT FAILED {s_schema}.{s_table}: {checkpoint_error}")
            continue

        # --- Stage: finalize the queue row (own failure state) ----------------
        # The checkpoint is already committed; a failure here must NOT be treated
        # as a data failure and must NOT clear the committed watermark.
        try:
            _mark_queue("SUCCEEDED")
        except Exception as queue_finalize_error:
            failed += 1
            try:
                repo.update_control(src_id, {
                    "current_status": "QUEUE_FINALIZATION_FAILED",
                    "error_message": ("Data application and checkpoint commit "
                                      "succeeded, but delta_sync_queue finalization "
                                      f"failed: {queue_finalize_error}")[:1000],
                })
            except Exception as update_error:
                print(f"  [warn] failed to record finalization error: {update_error}")
            try:
                _mark_queue("FAILED_FINALIZATION")
            except Exception as queue_error:
                print(f"  [warn] failed to mark queue FAILED_FINALIZATION: {queue_error}")
            try:
                log_run(ident, plain_target, s_count, t_count,
                        "QUEUE_FINALIZATION", "FAILED",
                        str(queue_finalize_error)[:1000], started)
            except Exception as log_error:
                print(f"  [warn] failed to write finalization audit: {log_error}")
            print(f"  Data and checkpoint committed successfully, but queue "
                  f"finalization failed {s_schema}.{s_table}: {queue_finalize_error}")
            continue

        succeeded += 1
        print(f"  [{src_system}] {op} {plain_target}: {s_count} src rows, upper_wm={upper_wm}")
    except Exception as apply_error:
        # Failure before the checkpoint was committed: normal data-failure path.
        failed += 1
        try:
            _mark_queue("FAILED")
        except Exception as queue_error:
            print(f"  [warn] failed to mark queue row FAILED: {queue_error}")
        try:
            repo.update_control(src_id, {
                "current_status": "DELTA_FAILED",
                "error_message": str(apply_error)[:1000],
            })
        except Exception as update_error:
            print(f"  [warn] failed to record control error: {update_error}")
        try:
            log_run(ident, plain_target, s_count, t_count,
                    op, "FAILED", str(apply_error)[:1000], started)
        except Exception as log_error:
            print(f"  [warn] failed to write table audit: {log_error}")
        print(f"  FAILED [{src_system}] {s_schema}.{s_table}: {apply_error}")
    finally:
        if src_df is not None:
            try:
                src_df.unpersist()
            except Exception as unpersist_error:
                print(f"  [warn] failed to unpersist source data: {unpersist_error}")

# COMMAND ----------

print(f"Delta apply complete. succeeded={succeeded} failed={failed}")
if failed > 0:
    raise Exception(
        f"{failed} table(s) had delta sync or finalization failures; "
        "inspect source_table_control, delta_sync_queue, and "
        "table_run_log for the exact failure stage."
    )

dbutils.notebook.exit(json.dumps({"status": "SUCCEEDED", "run_id": run_id,
                                  "applied": succeeded}))