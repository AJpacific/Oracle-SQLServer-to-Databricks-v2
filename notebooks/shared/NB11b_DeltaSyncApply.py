# Databricks notebook source
# MAGIC %md
# MAGIC # NB11b_DeltaSyncApply
# MAGIC Applies queued INGEST recurring-synchronization work according to strategy:
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

# Retry / ForEach parameters. A retry task carries parent_run_id + attempt_number
# for lineage; recovery_action selects a no-reapply path when only the checkpoint
# or the queue finalization failed previously.
dbutils.widgets.text("parent_run_id", "")
dbutils.widgets.text("attempt_number", "1")
dbutils.widgets.text("recovery_action", "")
parent_run_id = dbutils.widgets.get("parent_run_id").strip() or None
try:
    attempt_number = int(dbutils.widgets.get("attempt_number").strip() or "1")
except ValueError:
    attempt_number = 1
recovery_action = dbutils.widgets.get("recovery_action").strip().upper()
only_id = SOURCE_TABLE_ID or None

# COMMAND ----------

# Checkpoint-only / finalization-only recovery: operate on the parent run's
# already-applied+reconciled queue rows and commit/finalize WITHOUT re-reading
# the source or reapplying data (WATERMARK/HYBRID reuse the frozen upper bound).
if recovery_action in ("RETRY_CHECKPOINT_ONLY", "RETRY_QUEUE_FINALIZATION_ONLY"):
    src_run = parent_run_id or run_id
    statuses = (("RECONCILED", "CHECKPOINT_COMMIT_FAILED")
                if recovery_action == "RETRY_CHECKPOINT_ONLY"
                else ("CHECKPOINT_COMMITTED", "FAILED_FINALIZATION"))
    recovery_op = ("CHECKPOINT_RECOVERY"
                   if recovery_action == "RETRY_CHECKPOINT_ONLY"
                   else "QUEUE_FINALIZATION_RECOVERY")
    recovery_stage = (failcls.CHECKPOINT
                      if recovery_action == "RETRY_CHECKPOINT_ONLY"
                      else failcls.QUEUE_FINALIZATION)
    in_list = ", ".join(escape_string_literal(s) for s in statuses)
    scope = (f" AND source_table_id = {escape_string_literal(only_id)}"
             if only_id else "")
    rows = spark.sql(f"""
        SELECT * FROM {ctrl('delta_sync_queue')}
        WHERE run_id = {escape_string_literal(src_run)}
          AND status IN ({in_list}){scope}
    """).collect()
    recovered, rec_failed = 0, 0
    for q in rows:
        qd = q.asDict()
        src_id = q["source_table_id"]
        strategy = q["load_strategy"]
        upper_wm = q["upper_watermark_value"]
        rec_started = now_utc()

        def _log_recovery(status, error=None):
            """Child audit row for a state-only recovery (no data reapplied)."""
            repo.log_table_run({
                "run_id": run_id, "parent_run_id": src_run,
                "source_table_id": src_id, "connection_id": qd.get("connection_id"),
                "source_system": qd.get("source_system"),
                "source_server": qd.get("source_server"),
                "source_database": qd.get("source_database"),
                "source_schema": qd.get("source_schema"),
                "source_table": qd.get("source_table"),
                "operation": recovery_op, "status": status,
                "error_message": error, "attempt_number": attempt_number,
                "failure_stage": None if status == "SUCCEEDED" else recovery_stage,
                "error_category": None if status == "SUCCEEDED" else failcls.CHECKPOINT_ERROR,
                "retry_eligible": None if status == "SUCCEEDED" else False,
                "lower_watermark": qd.get("last_watermark_value"),
                "upper_watermark": upper_wm,
                "started_ts": rec_started, "ended_ts": now_utc(),
            })

        try:
            if recovery_action == "RETRY_CHECKPOINT_ONLY":
                fields = {
                    "last_successful_run_id": src_run,
                    "last_successful_run_ts": now_utc().strftime("%Y-%m-%d %H:%M:%S.%f"),
                    "current_status": ("DELTA_FULL_REFRESH_SUCCEEDED"
                                       if strategy == "FULL_LOAD" else "DELTA_SYNCED"),
                    "error_message": None,
                }
                if strategy in ("WATERMARK", "HYBRID"):
                    fields["last_watermark_value"] = wm.canonical_watermark_string(
                        upper_wm, strict=True)
                repo.update_control(src_id, fields)
                spark.sql(f"""
                    UPDATE {ctrl('delta_sync_queue')}
                    SET status = 'CHECKPOINT_COMMITTED',
                        checkpoint_committed_ts = current_timestamp()
                    WHERE run_id = {escape_string_literal(src_run)}
                      AND source_table_id = {escape_string_literal(src_id)}
                """)
            spark.sql(f"""
                UPDATE {ctrl('delta_sync_queue')}
                SET status = 'SUCCEEDED', finalized_ts = current_timestamp()
                WHERE run_id = {escape_string_literal(src_run)}
                  AND source_table_id = {escape_string_literal(src_id)}
            """)
            recovered += 1
            _log_recovery("SUCCEEDED")
        except Exception as e:
            rec_failed += 1
            safe = failcls.sanitize_message(e)
            try:
                _log_recovery("FAILED", safe[:1000])
            except Exception as log_error:
                safe_log_error = failcls.sanitize_message(log_error)
                print(f"  [warn] recovery audit failed: {safe_log_error[:300]}")
            print(f"  recovery failed for {src_id}: {safe[:300]}")
    print(f"{recovery_action}: recovered={recovered} failed={rec_failed} "
          "(no source read, no data reapplied)")
    if rec_failed:
        raise Exception(f"{rec_failed} row(s) failed {recovery_action}.")
    dbutils.notebook.exit(json.dumps({
        "status": "SUCCEEDED", "run_id": run_id, "recovery_action": recovery_action,
        "recovered": recovered}))

# COMMAND ----------

# RETRY_DELTA_APPLY reuses the parent run's FROZEN work unit: the queue row is
# copied to the child run_id unchanged, so the interval is never widened and
# MAX(watermark) is never recaptured. NB11a is not involved.
queue_run_id = run_id
if recovery_action == "RETRY_DELTA_APPLY":
    if not parent_run_id or not only_id:
        raise ValueError(
            "RETRY_DELTA_APPLY requires both parent_run_id and source_table_id")
    parent_rows = spark.sql(f"""
        SELECT * FROM {ctrl('delta_sync_queue')}
        WHERE run_id = {escape_string_literal(parent_run_id)}
          AND source_table_id = {escape_string_literal(only_id)}
    """).collect()
    if len(parent_rows) != 1:
        raise ValueError(
            f"parent run {parent_run_id!r} has {len(parent_rows)} queue row(s) for "
            f"source_table_id {only_id!r}; expected exactly 1")
    parent = parent_rows[0].asDict()
    # Carry only the frozen work-unit definition; no result metrics or timestamps.
    carried = {
        k: parent.get(k) for k in (
            "source_table_id", "connection_id", "source_system", "source_server",
            "source_database", "source_schema", "source_table", "target_catalog",
            "target_schema", "target_table", "stage_table", "load_strategy",
            "delete_policy", "primary_key_columns", "watermark_column",
            "watermark_data_type", "last_watermark_value", "upper_watermark_value",
            "source_query")
    }
    from pyspark.sql import Row as _Row
    from pyspark.sql import functions as _F
    (spark.createDataFrame([_Row(run_id=run_id, status="QUEUED", **carried)])
     .withColumn("captured_ts", _F.current_timestamp())
     .createOrReplaceTempView("_retry_queue_row"))
    # Idempotent: re-running the same child run never duplicates the row, and a
    # child row that already SUCCEEDED is left alone.
    spark.sql(f"""
        MERGE INTO {ctrl('delta_sync_queue')} t
        USING _retry_queue_row s
          ON t.run_id = s.run_id AND t.source_table_id = s.source_table_id
        WHEN MATCHED AND t.status <> 'SUCCEEDED' THEN UPDATE SET
            t.status = 'QUEUED', t.source_query = s.source_query,
            t.last_watermark_value = s.last_watermark_value,
            t.upper_watermark_value = s.upper_watermark_value,
            t.captured_ts = s.captured_ts
        WHEN NOT MATCHED THEN INSERT *
    """)
    print(f"RETRY_DELTA_APPLY: reusing frozen interval from parent run "
          f"{parent_run_id} (lower={carried['last_watermark_value']}, "
          f"upper={carried['upper_watermark_value']}).")

# COMMAND ----------

# Per-table ForEach scoping: one task must process only its own work unit.
queue_scope = (f" AND source_table_id = {escape_string_literal(only_id)}"
               if only_id else "")
queue = spark.sql(f"""
    SELECT * FROM {ctrl('delta_sync_queue')}
    WHERE run_id = {escape_string_literal(queue_run_id)}
      AND status = 'QUEUED'
      {queue_scope}
""").collect()
if only_id and len(queue) != 1:
    raise ValueError(
        f"source_table_id {only_id!r} resolved to {len(queue)} QUEUED rows; "
        "expected exactly 1")
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
    canonical = wm.canonical_watermark_string(value, strict=True)
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
        "attempt_number": attempt_number, "parent_run_id": parent_run_id,
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
    current_stage = failcls.CONNECTION

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
        current_stage = failcls.CONNECTION
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
        current_stage = failcls.SOURCE_READ
        src_df = read_source_jdbc(
            adapter, q["source_query"],
            source_server=src_server, source_database=src_db).cache()
        s_count = src_df.count()   # extracted_row_count

        # --- Stage 2: APPLY to Bronze + Stage 3: RECONCILE the work unit -------
        # Reconciliation always happens here, BEFORE any checkpoint is committed.
        recon_result = None
        current_stage = failcls.TARGET_WRITE
        if strategy == "FULL_LOAD":
            (conform_to_table(src_df, plain_target)
             .write.format("delta").mode("overwrite").saveAsTable(plain_target))
            op = "DELTA_FULL_REFRESH"
            current_stage = failcls.RECONCILIATION
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
            current_stage = failcls.RECONCILIATION
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
            current_stage = failcls.RECONCILIATION
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
                    extra={"failure_stage": failcls.RECONCILIATION,
                           "error_category": failcls.RECONCILIATION_ERROR,
                           "retry_eligible": False,
                           "extracted_row_count": metrics.get("extracted_row_count"),
                           "applied_row_count": metrics.get("applied_row_count")})
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
        current_stage = failcls.CHECKPOINT
        control_fields = {
            "last_successful_run_id": run_id,
            "last_successful_run_ts": now_utc().strftime("%Y-%m-%d %H:%M:%S.%f"),
            "current_status": ("DELTA_FULL_REFRESH_SUCCEEDED"
                               if strategy == "FULL_LOAD" else "DELTA_SYNCED"),
            "error_message": None,
        }
        try:
            if strategy in ("WATERMARK", "HYBRID"):
                control_fields["last_watermark_value"] = wm.canonical_watermark_string(
                    upper_wm, strict=True)
            repo.update_control(src_id, control_fields)
            _update_queue("CHECKPOINT_COMMITTED", {"checkpoint_committed_ts": now_utc()})
        except Exception as checkpoint_error:
            failed += 1
            safe_checkpoint_error = failcls.sanitize_message(checkpoint_error)
            try:
                _update_queue("CHECKPOINT_COMMIT_FAILED")
            except Exception as queue_error:
                safe_queue_error = failcls.sanitize_message(queue_error)
                print(f"  [warn] failed to mark queue CHECKPOINT_COMMIT_FAILED: "
                      f"{safe_queue_error[:300]}")
            try:
                repo.update_control(src_id, {
                    "current_status": "CHECKPOINT_COMMIT_FAILED",
                    "error_message": ("Data applied and reconciled, but checkpoint "
                                      f"commit failed: {safe_checkpoint_error}")[:1000],
                })
            except Exception as update_error:
                safe_update_error = failcls.sanitize_message(update_error)
                print(f"  [warn] failed to record checkpoint error: "
                      f"{safe_update_error[:300]}")
            try:
                log_run(ident, plain_target, s_count, t_count,
                        "CHECKPOINT_COMMIT", "FAILED", safe_checkpoint_error[:1000],
                        started, extra={"failure_stage": failcls.CHECKPOINT,
                                        "error_category": failcls.CHECKPOINT_ERROR,
                                        "retry_eligible": False})
            except Exception as log_error:
                safe_log_error = failcls.sanitize_message(log_error)
                print(f"  [warn] failed to write checkpoint audit: "
                      f"{safe_log_error[:300]}")
            print(f"  CHECKPOINT COMMIT FAILED {s_schema}.{s_table}: "
                  f"{safe_checkpoint_error[:300]}")
            continue

        # --- Stage 5: finalize the queue row ----------------------------------
        # The checkpoint is already committed; a failure here must NOT reapply
        # data and must NOT clear the committed watermark.
        current_stage = failcls.QUEUE_FINALIZATION
        try:
            _update_queue("SUCCEEDED", {"finalized_ts": now_utc()})
        except Exception as queue_finalize_error:
            failed += 1
            safe_finalize_error = failcls.sanitize_message(queue_finalize_error)
            try:
                repo.update_control(src_id, {
                    "current_status": "QUEUE_FINALIZATION_FAILED",
                    "error_message": ("Data applied, reconciled, and checkpoint "
                                      "committed, but delta_sync_queue finalization "
                                      f"failed: {safe_finalize_error}")[:1000],
                })
            except Exception as update_error:
                safe_update_error = failcls.sanitize_message(update_error)
                print(f"  [warn] failed to record finalization error: "
                      f"{safe_update_error[:300]}")
            try:
                _update_queue("FAILED_FINALIZATION")
            except Exception as queue_error:
                safe_queue_error = failcls.sanitize_message(queue_error)
                print(f"  [warn] failed to mark queue FAILED_FINALIZATION: "
                      f"{safe_queue_error[:300]}")
            try:
                log_run(ident, plain_target, s_count, t_count,
                        "QUEUE_FINALIZATION", "FAILED",
                        safe_finalize_error[:1000], started,
                        extra={"failure_stage": failcls.QUEUE_FINALIZATION,
                               "error_category": failcls.CHECKPOINT_ERROR,
                               "retry_eligible": False})
            except Exception as log_error:
                safe_log_error = failcls.sanitize_message(log_error)
                print(f"  [warn] failed to write finalization audit: "
                      f"{safe_log_error[:300]}")
            print(f"  Data, reconciliation, and checkpoint committed, but queue "
                  f"finalization failed {s_schema}.{s_table}: "
                  f"{safe_finalize_error[:300]}")
            continue

        succeeded += 1
        print(f"  [{src_system}] {op} {plain_target}: {s_count} src rows, "
              f"recon={recon_result.status}, upper_wm={upper_wm}")
    except Exception as apply_error:
        # Failure before the checkpoint was committed: normal data-failure path.
        failed += 1
        cls = failcls.classify_failure(apply_error, current_stage, idempotent=True)
        try:
            _update_queue("FAILED")
        except Exception as queue_error:
            safe_queue_error = failcls.sanitize_message(queue_error)
            print(f"  [warn] failed to mark queue row FAILED: "
                  f"{safe_queue_error[:300]}")
        try:
            repo.update_control(src_id, {
                "current_status": "DELTA_FAILED",
                "error_message": cls.sanitized_message[:1000],
            })
        except Exception as update_error:
            safe_update_error = failcls.sanitize_message(update_error)
            print(f"  [warn] failed to record control error: "
                  f"{safe_update_error[:300]}")
        try:
            log_run(ident, plain_target, s_count, t_count,
                    op, "FAILED", cls.sanitized_message[:1000], started,
                    extra={"failure_stage": cls.stage, "error_category": cls.category,
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

print(f"Delta apply complete. succeeded={succeeded} failed={failed}")
if failed > 0:
    raise Exception(
        f"{failed} table(s) had delta sync or finalization failures; "
        "inspect source_table_control, delta_sync_queue, and "
        "table_run_log for the exact failure stage."
    )

dbutils.notebook.exit(json.dumps({"status": "SUCCEEDED", "run_id": run_id,
                                  "applied": succeeded}))