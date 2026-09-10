# Databricks notebook source
# MAGIC %md
# MAGIC # NB15_BronzeToSilverETL
# MAGIC ETL-pipeline task: transform one successfully ingested Bronze table into
# MAGIC Silver. It NEVER connects to Oracle or SQL Server and NEVER reads a source
# MAGIC secret scope. It applies configured cleansing + validation rules, separates
# MAGIC valid and invalid records (quarantining rejects), reconciles the exact
# MAGIC processed Bronze set against Silver + quarantine BEFORE committing the ETL
# MAGIC checkpoint, and tracks ETL watermarks separately from source-ingest ones.

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

from pyspark.sql import functions as F

dbutils.widgets.text("source_table_id", "")
dbutils.widgets.text("run_id", "")
dbutils.widgets.text("parent_run_id", "")
dbutils.widgets.text("attempt_number", "1")
dbutils.widgets.dropdown("etl_mode", "AUTO", ["FULL", "INCREMENTAL", "AUTO"])
dbutils.widgets.dropdown("quarantine_enabled", "true", ["true", "false"])
dbutils.widgets.text("exclude_quarantine_columns", "")

src_id = dbutils.widgets.get("source_table_id").strip() or SOURCE_TABLE_ID
run_id = dbutils.widgets.get("run_id").strip() or get_run_id()
parent_run_id = dbutils.widgets.get("parent_run_id").strip() or None
try:
    attempt_number = int(dbutils.widgets.get("attempt_number").strip() or "1")
except ValueError:
    attempt_number = 1
etl_mode = dbutils.widgets.get("etl_mode").strip()
quarantine_enabled = dbutils.widgets.get("quarantine_enabled") == "true"
exclude_cols = {c.strip() for c in
                dbutils.widgets.get("exclude_quarantine_columns").split(",") if c.strip()}

if not src_id:
    raise ValueError("source_table_id is required")

repo = control_repo()

def ctrl(t):
    return f"{quote_databricks(CATALOG)}.{quote_databricks(CONTROL_SCHEMA)}.{quote_databricks(t)}"

# COMMAND ----------

# ---- eligibility -----------------------------------------------------------
row = repo.get_control_row(src_id)
if row is None:
    raise ValueError(f"source_table_id {src_id!r} not found in source_table_control")
d = row.asDict()

SUCCESS_INGEST_STATUSES = {"LOADED", "DELTA_SYNCED", "DELTA_FULL_REFRESH_SUCCEEDED",
                           "FULL_LOADED", "NO_CHANGES"}

def _fail_ineligible(msg):
    raise ValueError(f"table not ETL-eligible: {msg}")

if not d.get("is_active"):
    _fail_ineligible("is_active is false")
if not d.get("initial_load_completed"):
    _fail_ineligible("initial_load_completed is false")
if not d.get("etl_is_active"):
    _fail_ineligible("etl_is_active is false")
if (d.get("current_status") or "") not in SUCCESS_INGEST_STATUSES:
    _fail_ineligible(f"ingest status {d.get('current_status')!r} is not a success state")

bronze_fqn = (f"{d['target_catalog'] or CATALOG}.{d['target_schema'] or d['source_schema'].lower()}."
              f"{d['target_table'] or d['source_table'].lower()}")
if not spark.catalog.tableExists(bronze_fqn):
    _fail_ineligible(f"Bronze table {bronze_fqn} does not exist")

# Silver names default from Bronze only when not explicitly configured.
silver_catalog = d.get("silver_catalog") or (d["target_catalog"] or CATALOG)
silver_schema = d.get("silver_schema") or f"{(d['target_schema'] or d['source_schema'].lower())}_silver"
silver_table = d.get("silver_table") or (d["target_table"] or d["source_table"].lower())
silver_fqn = f"{silver_catalog}.{silver_schema}.{silver_table}"
pk = list(d["primary_key_columns"]) if d.get("primary_key_columns") else []
etl_wm_col = d.get("etl_watermark_column")
last_etl_wm = d.get("last_etl_watermark_value")
print(f"ETL {src_id}: bronze={bronze_fqn} silver={silver_fqn} pk={pk} mode={etl_mode}")

started = now_utc()

def log_etl(op, status, err, s_count, t_count, extra=None):
    fields = {
        "run_id": run_id, "source_table_id": src_id,
        "connection_id": d.get("connection_id"), "source_system": d.get("source_system"),
        "source_server": d.get("source_server"), "source_database": d.get("source_database"),
        "source_schema": d.get("source_schema"), "source_table": d.get("source_table"),
        "operation": op, "target_full_name": silver_fqn,
        "source_row_count": s_count, "target_row_count": t_count,
        "status": status, "error_message": err,
        "attempt_number": attempt_number, "parent_run_id": parent_run_id,
        "started_ts": started, "ended_ts": now_utc(),
    }
    if extra:
        fields.update(extra)
    repo.log_table_run(fields)

# COMMAND ----------

try:
    # ---- Stage: determine processing mode + freeze the Bronze input slice ----
    bronze_cols = set(spark.table(bronze_fqn).columns)
    wm_supported = bool(etl_wm_col) and etl_wm_col in bronze_cols
    effective_mode = etl_mode
    if etl_mode == "AUTO":
        effective_mode = "INCREMENTAL" if (wm_supported and last_etl_wm is not None) else "FULL"
    if effective_mode == "INCREMENTAL" and not wm_supported:
        raise ValueError("INCREMENTAL ETL requires a valid etl_watermark_column present in Bronze")

    etl_op = "ETL_FULL" if effective_mode == "FULL" else "ETL_INCREMENTAL"
    upper_etl_wm = None
    if effective_mode == "INCREMENTAL":
        wm_sql = quote_databricks(etl_wm_col)
        upper_raw = spark.table(bronze_fqn).agg(F.max(F.col(wm_sql)).alias("m")).collect()[0]["m"]
        if upper_raw is None:
            print("  no Bronze rows for the ETL watermark; nothing to process.")
            upper_etl_wm = last_etl_wm
            bronze_df = spark.table(bronze_fqn).where(F.lit(False))
        else:
            upper_etl_wm = sqlb.canonical_watermark_string(upper_raw, strict=False)
            cond = F.col(wm_sql) <= F.lit(upper_raw)
            if last_etl_wm is not None:
                cond = cond & (F.col(wm_sql) > F.lit(last_etl_wm))
            bronze_df = spark.table(bronze_fqn).where(cond)
    else:
        bronze_df = spark.table(bronze_fqn)

    bronze_df = bronze_df.cache()
    input_count = bronze_df.count()

    # ---- Stage: load active rules (validated; no arbitrary SQL accepted) -----
    rule_rows = spark.sql(f"""
        SELECT rule_id, rule_type, column_name, rule_value, severity
        FROM {ctrl('dq_rule')}
        WHERE source_table_id = {escape_string_literal(src_id)} AND is_active = true
    """).collect()
    transforms, validations = [], []
    for r in rule_rows:
        try:
            vr = dqr.validate_rule(r.asDict())
        except Exception as e:
            print(f"  [warn] skipping invalid rule {r['rule_id']}: {e}")
            continue
        (transforms if dqr.is_transformation(vr["rule_type"]) else validations).append(
            {**vr, "rule_id": r["rule_id"], "severity": r["severity"]})

    # ---- Stage: cleansing transforms BEFORE validation (Bronze is untouched) --
    work = bronze_df
    for t in transforms:
        col = t["column_name"]
        if col not in work.columns:
            continue
        qc = F.col(f"`{col}`")
        if t["rule_type"] == dqr.TRIM_STRING:
            work = work.withColumn(col, F.trim(qc.cast("string")))
        elif t["rule_type"] == dqr.STANDARDIZE_CASE:
            mode = dqr.normalize_case_mode(t["rule_value"])
            work = work.withColumn(col, F.upper(qc) if mode == "UPPER" else F.lower(qc))
        elif t["rule_type"] == dqr.DEFAULT_VALUE:
            work = work.withColumn(
                col, F.when(qc.isNull(), F.lit(t["rule_value"]).cast(
                    work.schema[col].dataType)).otherwise(qc))

    # ---- Stage: validation -> per-row failure-reason array -------------------
    empty_arr = F.array().cast("array<string>")
    failures = empty_arr
    dq_result_rows = []
    for v in validations:
        rt = v["rule_type"]
        col = v.get("column_name")
        qc = F.col(f"`{col}`") if col else None
        if rt == dqr.NOT_NULL:
            cond = qc.isNull()
            checked = input_count
        elif rt == dqr.DATA_TYPE:
            cond = qc.isNotNull() & F.expr(
                f"try_cast(`{col}` AS {v['rule_value']})").isNull()
            checked = work.where(qc.isNotNull()).count()
        elif rt == dqr.ALLOWED_VALUES:
            allowed = dqr.parse_allowed_values(v["rule_value"])
            cond = qc.isNotNull() & (~qc.cast("string").isin([str(a) for a in allowed]))
            checked = work.where(qc.isNotNull()).count()
        elif rt == dqr.DUPLICATE_KEY:
            key_cols = pk if pk else ([col] if col else [])
            if not key_cols:
                print(f"  [warn] DUPLICATE_KEY rule {v['rule_id']} has no key; skipped")
                continue
            from pyspark.sql.window import Window
            w = Window.partitionBy(*[F.col(f"`{c}`") for c in key_cols])
            work = work.withColumn("_dupcount", F.count(F.lit(1)).over(w))
            cond = F.col("_dupcount") > 1
            checked = input_count
        else:
            continue

        rejecting = dqr.is_rejecting_severity(v.get("severity"))
        if rejecting:
            failures = F.concat(
                failures,
                F.when(cond, F.array(F.lit(dqr.reason(rt, col)))).otherwise(empty_arr))
        dq_result_rows.append((v["rule_id"], rt, cond, checked, rejecting))

    work = work.withColumn("_dq_failures", failures)
    work = work.cache()

    valid_df = work.where(F.size("_dq_failures") == 0)
    invalid_df = work.where(F.size("_dq_failures") > 0)
    valid_count = valid_df.count()
    rejected_distinct = invalid_df.count()   # one row = one rejected record

    # ---- per-rule dq_result aggregates ---------------------------------------
    result_out = []
    for rule_id, rt, cond, checked, rejecting in dq_result_rows:
        failed = work.where(cond).count()
        status = "PASS" if failed == 0 else ("FAIL" if rejecting else "WARN")
        result_out.append((run_id, src_id, rule_id, rt, input_count, checked,
                           failed, max(checked - failed, 0), status,
                           f"rejecting={rejecting}"))
    if not rule_rows:
        # No-rule behavior: still record NO_RULES and reconcile Bronze->Silver.
        result_out.append((run_id, src_id, None, "NO_RULES", input_count, input_count,
                           0, input_count, "PASS", "no active DQ/transform rules"))
    if result_out:
        (spark.createDataFrame(result_out, [
            "run_id", "source_table_id", "rule_id", "rule_type", "input_count",
            "checked_count", "failed_count", "passed_count", "status", "message"])
         .withColumn("captured_ts", F.current_timestamp())
         .write.format("delta").mode("append").option("mergeSchema", "true")
         .saveAsTable(ctrl("dq_result").replace("`", "")))

    # ---- quarantine rejected rows (redact excluded/sensitive columns) --------
    if quarantine_enabled and rejected_distinct > 0:
        keep = [c for c in bronze_df.columns if c not in exclude_cols]
        json_struct = F.to_json(F.struct(*[F.col(f"`{c}`") for c in keep]))
        (invalid_df
         .withColumn("record_json", json_struct)
         .withColumn("failure_reason", F.concat_ws(",", F.col("_dq_failures")))
         .select(
             F.lit(run_id).alias("run_id"), F.lit(src_id).alias("source_table_id"),
             F.lit(None).cast("string").alias("rule_id"),
             "record_json", "failure_reason")
         .withColumn("quarantined_ts", F.current_timestamp())
         .write.format("delta").mode("append").option("mergeSchema", "true")
         .saveAsTable(ctrl("dq_quarantine").replace("`", "")))

    # ---- prepare valid output (drop internal technical columns) --------------
    drop_internal = [c for c in ("_dq_failures", "_dupcount") if c in valid_df.columns]
    valid_out = valid_df.drop(*drop_internal)

    # ---- provision Silver if missing -----------------------------------------
    spark.sql(ddl.build_create_schema(silver_catalog, silver_schema, "Silver ETL output"))
    silver_exists = spark.catalog.tableExists(silver_fqn)

    # ---- write Silver + ETL reconciliation (before checkpoint) ---------------
    if effective_mode == "FULL":
        (valid_out.write.format("delta").mode("overwrite")
         .option("overwriteSchema", "true").saveAsTable(silver_fqn))
        silver_count = spark.table(silver_fqn).count()
        recon_result = recon.reconcile_etl_full(
            input_count, valid_count, rejected_distinct, silver_count)
    elif pk:
        # INCREMENTAL MERGE: duplicate valid keys must fail before merge.
        dup_valid = valid_out.groupBy(*[F.col(f"`{c}`") for c in pk]).count() \
            .where(F.col("count") > 1).count()
        if not silver_exists:
            (valid_out.limit(0).write.format("delta").mode("overwrite")
             .option("overwriteSchema", "true").saveAsTable(silver_fqn))
        stage_fqn = f"{silver_catalog}.{silver_schema}.{silver_table}_etl_stage"
        (valid_out.write.format("delta").mode("overwrite")
         .option("overwriteSchema", "true").saveAsTable(stage_fqn))
        spark.sql(ddl.build_merge_sql(silver_catalog, silver_schema, silver_table,
                                      f"{silver_table}_etl_stage", pk))
        missing_valid = spark.sql(
            f"SELECT COUNT(*) c FROM (SELECT DISTINCT "
            f"{', '.join(quote_databricks(c) for c in pk)} FROM {stage_fqn}) s "
            f"LEFT ANTI JOIN {silver_fqn} t ON "
            + " AND ".join(f"t.{quote_databricks(c)} = s.{quote_databricks(c)}" for c in pk)
        ).collect()[0]["c"]
        spark.sql(ddl.build_drop_table(silver_catalog, silver_schema,
                                       f"{silver_table}_etl_stage"))
        recon_result = recon.reconcile_etl_incremental_merge(
            input_count, valid_count, rejected_distinct, dup_valid, missing_valid)
    else:
        # INCREMENTAL, no reliable key: retry-safe interval replacement.
        if not silver_exists:
            (valid_out.limit(0).write.format("delta").mode("overwrite")
             .option("overwriteSchema", "true").saveAsTable(silver_fqn))
        wm_sql = quote_databricks(etl_wm_col)
        lower_pred = (f"{wm_sql} > CAST({escape_string_literal(last_etl_wm)} AS TIMESTAMP) AND "
                      if last_etl_wm is not None else "")
        spark.sql(
            f"DELETE FROM {silver_fqn} WHERE {lower_pred}"
            f"{wm_sql} <= CAST({escape_string_literal(upper_etl_wm)} AS TIMESTAMP)")
        (valid_out.write.format("delta").mode("append").saveAsTable(silver_fqn))
        silver_interval = spark.sql(
            f"SELECT COUNT(*) c FROM {silver_fqn} WHERE {lower_pred}"
            f"{wm_sql} <= CAST({escape_string_literal(upper_etl_wm)} AS TIMESTAMP)"
        ).collect()[0]["c"]
        recon_result = recon.reconcile_etl_interval(
            input_count, valid_count, rejected_distinct, silver_interval)

    # persist recon checks
    if recon_result.checks:
        (spark.createDataFrame(
            [(run_id, src_id, d.get("connection_id"), d.get("source_system"),
              d.get("source_schema"), d.get("source_table"), c["check_type"],
              c["source_value"], c["target_value"], c["status"], c["message"])
             for c in recon_result.checks],
            ["run_id", "source_table_id", "connection_id", "source_system",
             "source_schema", "source_table", "check_type", "source_value",
             "target_value", "status", "message"])
         .withColumn("captured_ts", F.current_timestamp())
         .write.format("delta").mode("append").option("mergeSchema", "true")
         .saveAsTable(ctrl("reconciliation_results").replace("`", "")))

    # ---- ETL checkpoint gate --------------------------------------------------
    if not recon_result.passed:
        repo.update_control(src_id, {
            "etl_current_status": "ETL_RECONCILIATION_FAILED",
            "etl_error_message": f"ETL reconciliation failed: {recon_result.status}",
        })
        log_etl(etl_op, "FAILED", "ETL reconciliation failed", input_count, valid_count,
                extra={"failure_stage": "ETL_RECONCILIATION",
                       "error_category": "RECONCILIATION_ERROR",
                       "extracted_row_count": input_count, "applied_row_count": valid_count,
                       "rejected_row_count": rejected_distinct})
        raise Exception(f"ETL reconciliation failed for {src_id}: {recon_result.status}")

    etl_fields = {
        "last_successful_etl_run_id": run_id,
        "last_successful_etl_run_ts": now_utc().strftime("%Y-%m-%d %H:%M:%S.%f"),
        "etl_current_status": "ETL_SUCCEEDED", "etl_error_message": None,
    }
    if effective_mode == "INCREMENTAL" and upper_etl_wm is not None:
        etl_fields["last_etl_watermark_value"] = upper_etl_wm
    repo.update_control(src_id, etl_fields)
    log_etl(etl_op, "SUCCEEDED", None, input_count, valid_count,
            extra={"extracted_row_count": input_count, "applied_row_count": valid_count,
                   "rejected_row_count": rejected_distinct})
    print(f"  ETL SUCCEEDED: input={input_count} valid={valid_count} "
          f"rejected={rejected_distinct} recon={recon_result.status}")
    result = {"status": "SUCCEEDED", "source_table_id": src_id, "run_id": run_id,
              "mode": effective_mode, "input": input_count, "valid": valid_count,
              "rejected": rejected_distinct}

except Exception as e:
    msg = failcls.sanitize_message(e)
    try:
        cur = (repo.get_control_row(src_id).asDict().get("etl_current_status")
               if repo.get_control_row(src_id) else None)
    except Exception:
        cur = None
    if cur != "ETL_RECONCILIATION_FAILED":
        cls = failcls.classify_failure(e, failcls.SILVER_WRITE, idempotent=True)
        try:
            repo.update_control(src_id, {
                "etl_current_status": "ETL_WRITE_FAILED",
                "etl_error_message": msg[:1000]})
        except Exception:
            pass
        try:
            log_etl("ETL_FULL", "FAILED", msg[:1000], None, None,
                    extra={"failure_stage": cls.stage, "error_category": cls.category,
                           "retry_eligible": cls.retry_eligible})
        except Exception:
            pass
    print(f"  ETL FAILED {src_id}: {e}")
    raise

# COMMAND ----------

dbutils.notebook.exit(json.dumps(result))
