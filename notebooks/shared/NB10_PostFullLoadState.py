# Databricks notebook source
# MAGIC %md
# MAGIC # NB10_PostFullLoadState
# MAGIC Commits bootstrap state AFTER a successful, validated full load. Sets
# MAGIC initial_load_completed=true and seeds the initial watermark from the exact
# MAGIC Delta snapshot written by NB09. This prevents rows arriving at the source
# MAGIC after the full-load read from being skipped during the handoff to the
# MAGIC INGEST recurring synchronization workflow.

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

from pyspark.sql import functions as F

run_id = get_run_id()
print("run_id:", run_id)
repo = control_repo()

def ctrl(t):
    return f"{quote_databricks(CATALOG)}.{quote_databricks(CONTROL_SCHEMA)}.{quote_databricks(t)}"

# COMMAND ----------

# Only AUTO_MIGRATE tables with an explicit row-count PASS are eligible.
loaded = spark.sql(f"""
    SELECT DISTINCT c.* FROM {ctrl('source_table_control')} c
    JOIN {ctrl('table_run_log')} l
      ON c.source_table_id = l.source_table_id
    WHERE l.run_id = {escape_string_literal(run_id)}
      AND l.operation = 'FULL_LOAD' AND l.status = 'SUCCEEDED'
      AND c.table_decision = 'AUTO_MIGRATE'
            AND EXISTS (
                SELECT 1 FROM {ctrl('reconciliation_results')} rr
                WHERE rr.run_id = {escape_string_literal(run_id)}
                    AND rr.source_table_id = c.source_table_id
                    AND rr.check_type = 'FULL_SNAPSHOT_COUNT'
                    AND rr.status = 'PASS'
            )
      AND NOT EXISTS (
        SELECT 1 FROM {ctrl('reconciliation_results')} rr
        WHERE rr.run_id = {escape_string_literal(run_id)}
          AND rr.source_table_id = c.source_table_id
          AND rr.status = 'FAIL'
      )
""").collect()
print("Tables eligible for state commit:", len(loaded))

# COMMAND ----------

committed, failed = 0, 0
for r in loaded:
    src_id = r["source_table_id"]
    s_schema, s_table = r["source_schema"], r["source_table"]
    strategy = r["load_strategy"]
    wm_col = r["watermark_column"]
    try:
        new_wm = None
        if strategy in ("WATERMARK", "HYBRID"):
            if not wm_col:
                raise ValueError("watermark strategy has no watermark column")
            # Seed from the target snapshot that NB09 actually loaded. A live
            # source MAX here creates a race: a row inserted after NB09 reads but
            # before NB10 runs could advance the checkpoint without being loaded.
            t_catalog = r["target_catalog"] or CATALOG
            t_schema = r["target_schema"] or s_schema.lower()
            t_table = r["target_table"] or s_table.lower()
            target_fqn = f"{t_catalog}.{t_schema}.{t_table}"
            val = (spark.table(target_fqn)
                   .agg(F.max(F.col(f"`{wm_col}`")).alias("mx"))
                   .collect()[0]["mx"])
            if val is None:
                adapter = get_source_adapter_routed(r)
                new_wm = adapter.initial_watermark_value(r["watermark_data_type"])
            else:
                new_wm = wm.canonical_watermark_string(val, strict=True)

        repo.update_control(src_id, {
            "initial_load_completed": True,
            "last_watermark_value": new_wm,
            "last_successful_run_id": run_id,
            "last_successful_run_ts": now_utc().strftime("%Y-%m-%d %H:%M:%S.%f"),
            "current_status": "LOADED",
            "error_message": None,
        })
        committed += 1
        print(f"  committed {s_schema}.{s_table} strategy={strategy} seed_wm={new_wm}")
    except Exception as e:
        failed += 1
        try:
            repo.update_control(src_id, {
                "current_status": "STATE_COMMIT_FAILED",
                "error_message": str(e)[:1000],
            })
        except Exception as update_error:
            print(f"  [warn] failed to record state error: {update_error}")
        print(f"  FAILED state commit {s_schema}.{s_table}: {e}")

# COMMAND ----------

if failed > 0:
    raise Exception(f"{failed} table(s) failed post-full-load state commit.")

dbutils.notebook.exit(json.dumps({"status": "SUCCEEDED", "run_id": run_id,
                                  "committed": committed}))