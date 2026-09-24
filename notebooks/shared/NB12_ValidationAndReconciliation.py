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
    SELECT DISTINCT c.source_table_id, c.connection_id, c.source_system,
           c.source_server, c.source_database,
           c.source_schema, c.source_table,
           c.target_catalog, c.target_schema, c.target_table,
           l.operation, l.source_row_count, l.target_row_count
    FROM {ctrl('source_table_control')} c
    JOIN {ctrl('table_run_log')} l
            ON c.connection_id = l.connection_id
         AND c.source_table_id = l.source_table_id
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
    conn_id = r["connection_id"]
    src_system = require_source_system(
        r["source_system"], "reconciliation source row")
    src_server = r["source_server"]
    src_db = r["source_database"]
    s_schema, s_table = r["source_schema"], r["source_table"]
    t_catalog, t_schema, t_table = validate_target_identity(
        r["target_catalog"], r["target_schema"], r["target_table"]
    )
    target_fqn = f"{t_catalog}.{t_schema}.{t_table}"

    try:
        # Canonical full-load check type shared with NB10's state gate and the
        # dashboard views; delta runs summarize NB11b's named checks.
        check_type = "FULL_SNAPSHOT_COUNT"
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
            # Source-to-Bronze delta reconciliation already ran in NB11b BEFORE
            # the checkpoint was committed (extract -> apply -> reconcile ->
            # checkpoint -> finalize). NB12 only summarizes those named checks and
            # NEVER derives a PASS from target_count >= source_count.
            check_type = "DELTA_RECON_SUMMARY"
            rr = spark.sql(f"""
                SELECT status, count(*) AS n
                FROM {ctrl('reconciliation_results')}
                WHERE run_id = {escape_string_literal(run_id)}
                                    AND connection_id = {escape_string_literal(conn_id)}
                  AND source_table_id = {escape_string_literal(src_id)}
                  AND check_type IN ('FULL_SNAPSHOT_COUNT','DELTA_INTERVAL_COUNT',
                                     'STAGE_COUNT','DUPLICATE_PRIMARY_KEY',
                                     'MERGED_KEY_EXISTENCE')
                GROUP BY status
            """).collect()
            counts = {row["status"]: row["n"] for row in rr}
            src_count = "reconciled_in_NB11b"
            tgt_count = str(counts)
            if not counts:
                # No mandatory checks recorded: surface for investigation, but do
                # not pass by assumption and do not fabricate a count comparison.
                status = "WARN"
            elif counts.get("FAIL", 0) > 0:
                status, any_fail = "FAIL", True
            elif counts.get("WARN", 0) > 0:
                status = "WARN"
            else:
                status = "PASS"
        results.append((run_id, src_id, conn_id, src_system, s_schema, s_table,
                        check_type, str(src_count), str(tgt_count), status,
                        f"src={src_count} tgt={tgt_count} mode={mode}"))

        # ---- Check 2: target table is queryable / not empty on a full load ----
        if mode == "full" and src_count is not None and src_count > 0 and tgt_count == 0:
            any_fail = True
            results.append((run_id, src_id, conn_id, src_system, s_schema, s_table,
                            "NON_EMPTY", str(src_count), str(tgt_count), "FAIL",
                            "source had rows but target is empty"))
    except Exception as e:
        any_fail = True
        results.append((run_id, src_id, conn_id, src_system, s_schema, s_table,
                        "RECON_ERROR", None, None, "FAIL",
                        failcls.sanitize_message(e)[:500]))
        print(f"  RECON ERROR [{src_system}] {s_schema}.{s_table}: "
              f"{failcls.sanitize_message(e)[:300]}")

# COMMAND ----------

# Delta boundary reporting: for each WATERMARK/HYBRID table in this delta run
# record the previous committed watermark, the upper watermark captured in NB11a,
# and the final committed watermark. Informational only - this never advances the
# watermark and never fails the run.
if mode == "delta":
    wm_details = spark.sql(f"""
        SELECT q.source_table_id, q.connection_id, q.source_system,
               q.source_schema, q.source_table,
               q.last_watermark_value  AS previous_wm,
               q.upper_watermark_value AS captured_upper_wm,
               c.last_watermark_value  AS final_wm
        FROM {ctrl('delta_sync_queue')} q
        JOIN {ctrl('source_table_control')} c
                    ON q.connection_id = c.connection_id
                 AND q.source_table_id = c.source_table_id
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
        results.append((run_id, w["source_table_id"], w["connection_id"],
                        w["source_system"], w["source_schema"], w["source_table"],
                        "DELTA_WATERMARK", upper, final,
                        "PASS" if advanced else "WARN",
                        f"previous={prev} captured_upper={upper} final={final}"))

# COMMAND ----------

owned_check_types = (
    "'FULL_SNAPSHOT_COUNT','NON_EMPTY','RECON_ERROR'"
    if mode == "full"
    else "'DELTA_RECON_SUMMARY','DELTA_WATERMARK','RECON_ERROR'"
)
spark.sql(f"""
    DELETE FROM {ctrl('reconciliation_results')}
    WHERE run_id = {escape_string_literal(run_id)}
      AND check_type IN ({owned_check_types})
""")

if results:
    cols = ["run_id", "source_table_id", "connection_id", "source_system",
            "source_schema", "source_table", "check_type", "source_value",
            "target_value", "status", "message"]
    df = spark.createDataFrame(results, cols).withColumn("captured_ts", F.current_timestamp())
    df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(
        ctrl("reconciliation_results").replace("`", ""))
    df.groupBy("status").count().show()

# COMMAND ----------

if any_fail:
    raise Exception("Reconciliation FAILED for one or more tables.")

dbutils.notebook.exit(json.dumps({"status": "SUCCEEDED", "run_id": run_id,
                                  "checked": len(loaded)}))