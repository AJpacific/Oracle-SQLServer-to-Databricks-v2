# Databricks notebook source
# MAGIC %md
# MAGIC # NB12_ValidationAndReconciliation
# MAGIC Compares source vs target for tables loaded in this run and writes
# MAGIC PASS/FAIL to reconciliation_results. A full-load failure blocks NB10;
# MAGIC delta reconciliation reports the already-applied delta run.

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

dbutils.widgets.dropdown("mode", "full", ["full", "delta"])
mode = dbutils.widgets.get("mode")

run_id = get_run_id()
print("run_id:", run_id, "mode:", mode)
repo = control_repo()

def ctrl(t):
    return f"{quote_databricks(CATALOG)}.{quote_databricks(CONTROL_SCHEMA)}.{quote_databricks(t)}"

# COMMAND ----------

op_filter = (
    "'FULL_LOAD'"
    if mode == "full"
    else "'DELTA_MERGE','DELTA_APPEND','DELTA_FULL_REFRESH'"
)
loaded = spark.sql(f"""
    SELECT DISTINCT c.source_table_id, c.source_system,
           c.source_server, c.source_database,
           c.source_schema, c.source_table,
           c.target_catalog, c.target_schema, c.target_table,
           l.operation, l.source_row_count, l.target_row_count
    FROM {ctrl('source_table_control')} c
    JOIN {ctrl('table_run_log')} l
      ON c.source_table_id = l.source_table_id
    WHERE l.run_id = {escape_string_literal(run_id)}
      AND l.status = 'SUCCEEDED'
      AND l.operation IN ({op_filter})
""").collect()
print("Tables to reconcile:", len(loaded))

# COMMAND ----------

from pyspark.sql import functions as F
results = []
any_fail = False

for r in loaded:
    src_id = r["source_table_id"]
    src_system = r["source_system"]
    src_server = r["source_server"]
    src_db = r["source_database"]
    s_schema, s_table = r["source_schema"], r["source_table"]
    t_catalog = r["target_catalog"] or CATALOG
    t_schema = r["target_schema"] or s_schema.lower()
    t_table = r["target_table"] or s_table.lower()
    target_fqn = f"{t_catalog}.{t_schema}.{t_table}"

    try:
        if mode == "full":
            src_count = r["source_row_count"]
            tgt_count = r["target_row_count"]
            if src_count is None or tgt_count is None:
                raise ValueError("FULL_LOAD log is missing source/target row counts")
            status = "PASS" if src_count == tgt_count else "FAIL"
            if status == "FAIL":
                any_fail = True
        elif r["operation"] == "DELTA_FULL_REFRESH":
            # Full refresh overwrote the target, so the logged source and target
            # counts must match exactly (no watermark reconciliation).
            src_count = r["source_row_count"]
            tgt_count = r["target_row_count"]
            if src_count is None or tgt_count is None or src_count != tgt_count:
                status, any_fail = "FAIL", True
            else:
                status = "PASS"
        else:
            # Count through the correct adapter for this row's source.
            adapter = get_source_adapter_for_row(r)
            src_count = read_source_jdbc(
                adapter, adapter.count_query(src_db, s_schema, s_table),
                source_server=src_server, source_database=src_db
            ).collect()[0]["ROW_COUNT"]
            tgt_count = spark.table(target_fqn).count()
            # Incremental: the target retains every row ever synced, so it should
            # hold at least the current source count (more only if the source had
            # hard deletes). Fewer rows than source means data is missing.
            if tgt_count == 0 and src_count > 0:
                status, any_fail = "FAIL", True
            elif tgt_count >= src_count:
                status = "PASS"
            else:
                # behind source: surfaced for investigation but not fatal, since
                # it can be legitimate mid-catch-up lag between scheduled syncs.
                status = "WARN"
        results.append((run_id, src_id, src_system, s_schema, s_table, "ROW_COUNT",
                        str(src_count), str(tgt_count), status,
                        f"src={src_count} tgt={tgt_count} mode={mode}"))

        # ---- Check 2: target table is queryable / not empty on a full load ----
        if mode == "full" and src_count > 0 and tgt_count == 0:
            any_fail = True
            results.append((run_id, src_id, src_system, s_schema, s_table, "NON_EMPTY",
                            str(src_count), str(tgt_count), "FAIL",
                            "source had rows but target is empty"))
    except Exception as e:
        any_fail = True
        results.append((run_id, src_id, src_system, s_schema, s_table, "RECON_ERROR",
                        None, None, "FAIL", str(e)[:500]))
        print(f"  RECON ERROR [{src_system}] {s_schema}.{s_table}: {e}")

# COMMAND ----------

# Delta boundary reporting: for each WATERMARK/HYBRID table in this delta run
# record the previous committed watermark, the upper watermark captured in NB11a,
# and the final committed watermark. Informational only - this never advances the
# watermark and never fails the run.
if mode == "delta":
    wm_details = spark.sql(f"""
        SELECT q.source_table_id, q.source_system,
               q.source_schema, q.source_table,
               q.last_watermark_value  AS previous_wm,
               q.upper_watermark_value AS captured_upper_wm,
               c.last_watermark_value  AS final_wm
        FROM {ctrl('delta_sync_queue')} q
        JOIN {ctrl('source_table_control')} c
          ON q.source_table_id = c.source_table_id
        WHERE q.run_id = {escape_string_literal(run_id)}
                    AND q.status = 'SUCCEEDED'
          AND q.load_strategy IN ('WATERMARK','HYBRID')
          AND q.upper_watermark_value IS NOT NULL
    """).collect()
    for w in wm_details:
        prev = w["previous_wm"]
        upper = w["captured_upper_wm"]
        final = w["final_wm"]
        advanced = (final == upper)
        results.append((run_id, w["source_table_id"], w["source_system"],
                        w["source_schema"], w["source_table"],
                        "DELTA_WATERMARK", upper, final,
                        "PASS" if advanced else "WARN",
                        f"previous={prev} captured_upper={upper} final={final}"))

# COMMAND ----------

if results:
    cols = ["run_id", "source_table_id", "source_system", "source_schema",
            "source_table", "check_type", "source_value", "target_value",
            "status", "message"]
    df = spark.createDataFrame(results, cols).withColumn("captured_ts", F.current_timestamp())
    df.write.format("delta").mode("append").saveAsTable(
        ctrl("reconciliation_results").replace("`", ""))
    df.groupBy("status").count().show()

# COMMAND ----------

if any_fail:
    raise Exception("Reconciliation FAILED for one or more tables.")

dbutils.notebook.exit(json.dumps({"status": "SUCCEEDED", "run_id": run_id,
                                  "checked": len(loaded)}))