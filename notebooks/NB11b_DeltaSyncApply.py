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
            op, status, err, started, extra=None):
    fields = {
        "run_id": run_id,
        "source_table_id": ident["source_table_id"],
        "connection_id": ident.get("connection_id"),
        "source_system": ident["source_system"],
        "source_server": ident["source_server"],
        "source_database": ident["source_database"],
        "source_schema": ident["source_schema"], "source_table": ident["source_table"],
        "operation": op, "target_full_name": target_fqn,
        "source_row_count": s_count, "target_row_count": t_count,
        "status": status, "error_message": err,
        "started_ts": started, "ended_ts": now_utc(),
    }
    if extra:
        fields.update(extra)
    repo.log_table_run(fields)


def write_recon(ident, recon_result):
    "Append each named reconciliation check for a work unit."
    from pyspark.sql import functions as F
    rows = [(run_id, ident["source_table_id"], ident.get("connection_id"),
             ident["source_system"], ident["source_schema"], ident["source_table"],
             c["check_type"], c["source_value"], c["target_value"],
             c["status"], c["message"]) for c in recon_result.checks]
    if not rows:
        return
    cols = ["run_id", "source_table_id", "connection_id", "source_system",
            "source_schema", "source_table", "check_type", "source_value",
            "target_value", "status", "message"]
    (spark.createDataFrame(rows, cols)
     .withColumn("captured_ts", F.current_timestamp())
     .write.format("delta").mode("append").option("mergeSchema", "true")
     .saveAsTable(ctrl("reconciliation_results").replace("`", "")))


def pk_join(pk_cols):
    return " AND ".join(
        f"t.{quote_databricks(c)} = s.{quote_databricks(c)}" for c in pk_cols)


def pk_list(pk_cols):
    return ", ".join(quote_databricks(c) for c in pk_cols)

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
             "connection_id": qd.get("connection_id"),
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
    metrics = {}

    def _update_queue(status, extra=None):
        assignments = [f"status = {escape_string_literal(status)}"]
        for k, v in (extra or {}).items():
            assignments.append(f"{quote_databricks(k)} = {repo._render_value(v)}")
        spark.sql(f"""
            UPDATE {ctrl('delta_sync_queue')} SET {', '.join(assignments)}
            WHERE run_id = {escape_string_literal(run_id)}
              AND source_table_id = {escape_string_literal(src_id)}
        """)

    try:
        # The adapter is chosen from the queue row's own connection/source_system,
        # so NB11b never uses one global (Oracle) JDBC connection for every row.
        adapter = get_source_adapter_routed(q)

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

        # --- Stage 1: EXTRACT the exact frozen slice ---------------------------
        src_df = read_source_jdbc(
            adapter, q["source_query"],
            source_server=src_server, source_database=src_db).cache()
        s_count = src_df.count()   # extracted_row_count

        # --- Stage 2: APPLY to Bronze + Stage 3: RECONCILE the work unit -------
        # Reconciliation always happens here, BEFORE any checkpoint is committed.
        recon_result = None
        if strategy == "FULL_LOAD":
            (conform_to_table(src_df, plain_target)
             .write.format("delta").mode("overwrite").saveAsTable(plain_target))
            op = "DELTA_FULL_REFRESH"
            t_count = spark.table(plain_target).count()
            recon_result = recon.reconcile_full_load(s_count, t_count)
            metrics = {"extracted_row_count": s_count, "applied_row_count": t_count}
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
                f"WHERE {wm_sql} > {lower_lit} AND {wm_sql} <= {upper_lit}")
            (conform_to_table(src_df, plain_target)
             .write.format("delta").mode("append").saveAsTable(plain_target))
            op = "DELTA_APPEND"
            applied_interval = spark.sql(
                f"SELECT COUNT(*) AS c FROM {target_sql} "
                f"WHERE {wm_sql} > {lower_lit} AND {wm_sql} <= {upper_lit}"
            ).collect()[0]["c"]
            t_count = spark.table(plain_target).count()
            recon_result = recon.reconcile_watermark(s_count, applied_interval)
            metrics = {"extracted_row_count": s_count,
                       "applied_row_count": applied_interval}
        else:
            # PRIMARY_KEY / HYBRID: stage then MERGE by PK (upsert)
            if not pk:
                raise Exception("MERGE strategy requires primary_key_columns")
            if strategy == "HYBRID" and upper_wm is None:
                raise ValueError("HYBRID queue item requires an upper watermark")
            (conform_to_table(src_df, plain_target)
             .write.format("delta").mode("overwrite")
             .option("overwriteSchema", "true").saveAsTable(plain_stage))
            staged_count = spark.table(plain_stage).count()
            dup_count = spark.sql(
                f"SELECT COUNT(*) AS c FROM (SELECT {pk_list(pk)} FROM {plain_stage} "
                f"GROUP BY {pk_list(pk)} HAVING COUNT(*) > 1)"
            ).collect()[0]["c"]
            # Propagate source deletes only from a COMPLETE snapshot (PRIMARY_KEY);
            # a HYBRID watermark slice can't tell a delete from an unchanged row.
            hard_delete = (strategy == "PRIMARY_KEY" and s_count > 0
                           and (delete_policy or "").upper() == "HARD_DELETE")
            spark.sql(ddl.build_merge_sql(t_catalog, t_schema, t_table,
                                          stage_table, pk,
                                          delete_unmatched=hard_delete))
            # Validate every staged key exists in Bronze BEFORE dropping the stage.
            missing_count = spark.sql(
                f"SELECT COUNT(*) AS c FROM "
                f"(SELECT DISTINCT {pk_list(pk)} FROM {plain_stage}) s "
                f"LEFT ANTI JOIN {target_sql} t ON {pk_join(pk)}"
            ).collect()[0]["c"]
            t_count = spark.table(plain_target).count()
            spark.sql(ddl.build_drop_table(t_catalog, t_schema, stage_table))
            op = "DELTA_MERGE"
            if strategy == "HYBRID":
                recon_result = recon.reconcile_hybrid(
                    s_count, staged_count, dup_count, missing_count)
            else:
                recon_result = recon.reconcile_primary_key(
                    s_count, staged_count, dup_count, missing_count,
                    delete_policy, t_count)
            metrics = {"extracted_row_count": s_count, "staged_row_count": staged_count,
                       "applied_row_count": t_count, "duplicate_key_count": dup_count}

        _update_queue("DATA_APPLIED", {"data_applied_ts": now_utc(), **metrics})
        write_recon(ident, recon_result)

        # --- Reconciliation gate: a FAIL leaves the checkpoint uncommitted -----
        if not recon_result.passed:
            failed += 1
            _update_queue("RECONCILIATION_FAILED",
                          {"reconciliation_status": recon_result.status,
                           "reconciled_ts": now_utc()})
            repo.update_control(src_id, {
                "current_status": "DELTA_RECONCILIATION_FAILED",
                "error_message": ("delta reconciliation failed before checkpoint; "
                                  "ingest watermark left unchanged")[:1000],
            })
            log_run(ident, plain_target, s_count, t_count, op, "FAILED",
                    "reconciliation failed before checkpoint", started,
                    extra={"failure_stage": "RECONCILIATION",
                           "error_category": "RECONCILIATION_ERROR"})
            print(f"  RECONCILIATION FAILED [{src_system}] {s_schema}.{s_table}: "
                  f"{recon_result.status}")
            continue

        _update_queue("RECONCILED", {"reconciliation_status": recon_result.status,
                                     "reconciled_ts": now_utc()})
        log_run(ident, plain_target, s_count, t_count, op, "SUCCEEDED", None, started,
                extra={"applied_row_count": metrics.get("applied_row_count"),
                       "extracted_row_count": metrics.get("extracted_row_count")})

        # --- Stage 4: commit the control checkpoint ---------------------------
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
            _update_queue("CHECKPOINT_COMMITTED", {"checkpoint_committed_ts": now_utc()})
        except Exception as checkpoint_error:
            failed += 1
            try:
                _update_queue("CHECKPOINT_COMMIT_FAILED")
            except Exception as queue_error:
                print(f"  [warn] failed to mark queue CHECKPOINT_COMMIT_FAILED: {queue_error}")
            try:
                repo.update_control(src_id, {
                    "current_status": "CHECKPOINT_COMMIT_FAILED",
                    "error_message": ("Data applied and reconciled, but checkpoint "
                                      f"commit failed: {checkpoint_error}")[:1000],
                })
            except Exception as update_error:
                print(f"  [warn] failed to record checkpoint error: {update_error}")
            try:
                log_run(ident, plain_target, s_count, t_count,
                        "CHECKPOINT_COMMIT", "FAILED", str(checkpoint_error)[:1000],
                        started, extra={"failure_stage": "CHECKPOINT",
                                        "error_category": "CHECKPOINT_ERROR"})
            except Exception as log_error:
                print(f"  [warn] failed to write checkpoint audit: {log_error}")
            print(f"  CHECKPOINT COMMIT FAILED {s_schema}.{s_table}: {checkpoint_error}")
            continue

        # --- Stage 5: finalize the queue row ----------------------------------
        # The checkpoint is already committed; a failure here must NOT reapply
        # data and must NOT clear the committed watermark.
        try:
            _update_queue("SUCCEEDED", {"finalized_ts": now_utc()})
        except Exception as queue_finalize_error:
            failed += 1
            try:
                repo.update_control(src_id, {
                    "current_status": "QUEUE_FINALIZATION_FAILED",
                    "error_message": ("Data applied, reconciled, and checkpoint "
                                      "committed, but delta_sync_queue finalization "
                                      f"failed: {queue_finalize_error}")[:1000],
                })
            except Exception as update_error:
                print(f"  [warn] failed to record finalization error: {update_error}")
            try:
                _update_queue("FAILED_FINALIZATION")
            except Exception as queue_error:
                print(f"  [warn] failed to mark queue FAILED_FINALIZATION: {queue_error}")
            try:
                log_run(ident, plain_target, s_count, t_count,
                        "QUEUE_FINALIZATION", "FAILED",
                        str(queue_finalize_error)[:1000], started,
                        extra={"failure_stage": "QUEUE_FINALIZATION"})
            except Exception as log_error:
                print(f"  [warn] failed to write finalization audit: {log_error}")
            print(f"  Data, reconciliation, and checkpoint committed, but queue "
                  f"finalization failed {s_schema}.{s_table}: {queue_finalize_error}")
            continue

        succeeded += 1
        print(f"  [{src_system}] {op} {plain_target}: {s_count} src rows, "
              f"recon={recon_result.status}, upper_wm={upper_wm}")
    except Exception as apply_error:
        # Failure before the checkpoint was committed: normal data-failure path.
        failed += 1
        try:
            _update_queue("FAILED")
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
                    op, "FAILED", str(apply_error)[:1000], started,
                    extra={"failure_stage": "SOURCE_READ"})
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