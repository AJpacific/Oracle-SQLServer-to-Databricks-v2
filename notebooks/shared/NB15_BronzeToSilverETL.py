# Databricks notebook source
# MAGIC %md
# MAGIC # NB15_BronzeToSilverETL
# MAGIC ETL-pipeline task: transform one successfully ingested Bronze table into
# MAGIC Silver. It NEVER connects to Oracle or SQL Server and NEVER reads a source
# MAGIC secret scope. It applies configured cleansing + validation rules, separates
# MAGIC valid and invalid records (quarantining rejects), reconciles the exact
# MAGIC processed Bronze set BEFORE committing the ETL checkpoint, and tracks ETL
# MAGIC watermarks separately from source-ingest ones.
# MAGIC
# MAGIC DUPLICATE_KEY policy: every record in a duplicate-key group is rejected.
# MAGIC No survivor is kept, because there is no deterministic survivor ordering.

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

dbutils.widgets.text("source_table_id", "")
dbutils.widgets.text("run_id", "")
dbutils.widgets.text("parent_run_id", "")
dbutils.widgets.text("attempt_number", "1")
dbutils.widgets.dropdown("etl_mode", "AUTO", ["FULL", "INCREMENTAL", "AUTO"])
dbutils.widgets.dropdown("quarantine_enabled", "true", ["true", "false"])
dbutils.widgets.text("exclude_quarantine_columns", "")
# A RETRY_ETL task replays the ORIGINAL frozen interval instead of recomputing a
# wider one from the current Bronze MAX.
dbutils.widgets.text("recovery_action", "")
dbutils.widgets.text("retry_lower_watermark", "")
dbutils.widgets.text("retry_upper_watermark", "")

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
recovery_action = dbutils.widgets.get("recovery_action").strip().upper()
retry_lower_wm = dbutils.widgets.get("retry_lower_watermark").strip() or None
retry_upper_wm = dbutils.widgets.get("retry_upper_watermark").strip() or None
is_etl_retry = recovery_action == "RETRY_ETL"

if not src_id:
    raise ValueError("source_table_id is required")
if is_etl_retry and not parent_run_id:
    raise ValueError("RETRY_ETL requires parent_run_id")

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

# One table operation produces exactly one final table_run_log failure row,
# carrying the stage that actually failed.
current_stage = failcls.ETL_READ
failure_already_logged = False
etl_op = "ETL_FULL"
# Resolved before any data is touched; every audit path reads its boundaries.
work_unit = None

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


def write_dq_results(result_rows):
    if not result_rows:
        return
    (spark.createDataFrame(result_rows, [
        "run_id", "source_table_id", "rule_id", "rule_type", "input_count",
        "checked_count", "failed_count", "passed_count", "status", "message"])
     .withColumn("captured_ts", F.current_timestamp())
     .write.format("delta").mode("append").option("mergeSchema", "true")
     .saveAsTable(ctrl("dq_result").replace("`", "")))


def write_recon(recon_result):
    if not recon_result.checks:
        return
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


def fail_etl(stage, category, message, etl_status, s_count=None, t_count=None,
             extra=None):
    """Record one specialized ETL failure with its true stage, then raise.

    The ETL checkpoint is never advanced, and the outer handler will not log a
    second, mislabelled failure row for the same operation.
    """
    global failure_already_logged
    safe = failcls.sanitize_message(message)
    try:
        repo.update_control(src_id, {
            "etl_current_status": etl_status,
            "etl_error_message": safe[:1000]})
    except Exception as e:
        print(f"  [warn] could not record ETL status: {failcls.sanitize_message(e)[:200]}")
    payload = {"failure_stage": stage, "error_category": category,
               "retry_eligible": False}
    # A failed attempt must hand the SAME frozen interval to the next retry.
    if work_unit is not None:
        payload.update(work_unit.audit_fields(attempt_number))
    elif is_etl_retry:
        payload.update({"lower_watermark": retry_lower_wm,
                        "upper_watermark": retry_upper_wm,
                        "parent_run_id": parent_run_id,
                        "attempt_number": attempt_number})
    if extra:
        payload.update(extra)
    try:
        log_etl(etl_op, "FAILED", safe[:1000], s_count, t_count, extra=payload)
    except Exception as e:
        print(f"  [warn] could not write ETL audit: {failcls.sanitize_message(e)[:200]}")
    failure_already_logged = True
    raise Exception(safe)

# COMMAND ----------

try:
    # ---- Stage: determine processing mode + freeze the Bronze input slice ----
    current_stage = failcls.ETL_READ
    bronze_schema = spark.table(bronze_fqn).schema
    bronze_cols = {f.name for f in bronze_schema.fields}
    wm_supported = bool(etl_wm_col) and etl_wm_col in bronze_cols
    effective_mode = etl_mode
    if etl_mode == "AUTO":
        effective_mode = "INCREMENTAL" if (wm_supported and last_etl_wm is not None) else "FULL"
    etl_op = "ETL_FULL" if effective_mode == "FULL" else "ETL_INCREMENTAL"

    if effective_mode == "INCREMENTAL" and not wm_supported:
        fail_etl(failcls.ETL_READ, failcls.CONFIGURATION_ERROR,
                 "INCREMENTAL ETL requires an etl_watermark_column present in Bronze",
                 "ETL_CONFIG_ERROR")

    # One authoritative execution boundary. Bronze filtering, interval
    # replacement, reconciliation, the success audit, every failure audit, and
    # the checkpoint all read from this single work unit, so the processed and
    # the recorded interval can never disagree.
    lower_bound_col = upper_bound_col = None
    if effective_mode == "FULL":
        work_unit = etlwu.build_full_work_unit()
        bronze_df = spark.table(bronze_fqn)
    else:
        cast_to = etlwu.resolve_cast(
            bronze_schema[etl_wm_col].dataType.simpleString())
        if cast_to is None:
            fail_etl(failcls.ETL_READ, failcls.CONFIGURATION_ERROR,
                     f"ETL watermark column {etl_wm_col!r} has unsupported type "
                     f"{bronze_schema[etl_wm_col].dataType.simpleString()}; "
                     "only DATE and TIMESTAMP are supported", "ETL_CONFIG_ERROR")

        wm_sql = quote_databricks(etl_wm_col)
        if is_etl_retry:
            # Replay the ORIGINAL frozen interval: Bronze MAX is never recomputed,
            # so rows arriving after the failed attempt are not swept in.
            raw_lower, raw_upper = retry_lower_wm, retry_upper_wm
            print(f"  RETRY_ETL: replaying frozen interval "
                  f"({raw_lower}, {raw_upper}]")
        else:
            raw_lower = last_etl_wm
            raw_upper = spark.table(bronze_fqn).agg(
                F.max(F.col(wm_sql)).alias("m")).collect()[0]["m"]

        if raw_upper is None and not is_etl_retry:
            print("  no Bronze rows for the ETL watermark; nothing to process.")
            work_unit = etlwu.EtlWorkUnit(
                mode="INCREMENTAL", lower_watermark=raw_lower,
                upper_watermark=raw_lower, watermark_column=etl_wm_col,
                watermark_cast=cast_to, is_retry=False)
            bronze_df = spark.table(bronze_fqn).where(F.lit(False))
        else:
            try:
                work_unit = etlwu.build_incremental_work_unit(
                    etl_wm_col, cast_to, raw_lower, raw_upper,
                    is_retry=is_etl_retry, parent_run_id=parent_run_id)
            except ValueError as bound_error:
                fail_etl(failcls.ETL_READ, failcls.CONFIGURATION_ERROR,
                         str(bound_error), "ETL_CONFIG_ERROR")
            upper_bound_col = F.lit(work_unit.upper_watermark).cast(cast_to)
            cond = F.col(wm_sql) <= upper_bound_col
            if work_unit.lower_watermark is not None:
                lower_bound_col = F.lit(work_unit.lower_watermark).cast(cast_to)
                cond = cond & (F.col(wm_sql) > lower_bound_col)
            bronze_df = spark.table(bronze_fqn).where(cond)

    bronze_df = bronze_df.cache()
    input_count = bronze_df.count()

    # ---- Stage: load + validate active rules (no arbitrary SQL accepted) -----
    current_stage = failcls.DQ_VALIDATION
    rule_rows = spark.sql(f"""
        SELECT rule_id, rule_type, column_name, rule_value, severity
        FROM {ctrl('dq_rule')}
        WHERE source_table_id = {escape_string_literal(src_id)} AND is_active = true
    """).collect()

    # An invalid ACTIVE rule is never silently skipped: it stops the whole table
    # before any transform, Silver write, or quarantine write.
    transforms, validations, invalid_rules = [], [], []
    seen_rule_ids = set()
    for r in rule_rows:
        rd = r.asDict()
        rid = rd.get("rule_id")
        try:
            if rid in seen_rule_ids:
                raise ValueError(f"duplicate active rule_id {rid!r}")
            seen_rule_ids.add(rid)
            vr = dqr.validate_rule(rd, available_columns=bronze_cols,
                                   primary_key_columns=pk)
        except Exception as rule_error:
            invalid_rules.append((rid, rd.get("rule_type"),
                                  failcls.sanitize_message(rule_error)))
            continue
        entry = {**vr, "rule_id": rid, "severity": rd.get("severity")}
        (transforms if dqr.is_transformation(vr["rule_type"]) else
         validations).append(entry)

    # A DEFAULT_VALUE literal that cannot be represented in the target column
    # type would silently insert nulls, so it is validated against the real
    # Bronze schema before anything is transformed, quarantined, or written.
    if transforms:
        probe = spark.range(1)
        for t in [t for t in transforms if t["rule_type"] == dqr.DEFAULT_VALUE]:
            col = t["column_name"]
            configured = t.get("rule_value")
            target_type = bronze_schema[col].dataType
            try:
                converted = (probe
                             .select(F.lit(configured).cast(target_type).alias("v"))
                             .collect()[0]["v"])
            except Exception as cast_error:
                converted = None
                print(f"  [warn] DEFAULT_VALUE cast probe failed for {col}: "
                      f"{failcls.sanitize_message(cast_error)[:200]}")
            if not dqr.default_value_converts(configured, converted):
                invalid_rules.append((
                    t["rule_id"], dqr.DEFAULT_VALUE,
                    f"DEFAULT_VALUE {configured!r} cannot be represented as "
                    f"{target_type.simpleString()} for column {col!r}"))
        transforms = [t for t in transforms
                      if t["rule_id"] not in {r[0] for r in invalid_rules}]

    if invalid_rules:
        write_dq_results([
            (run_id, src_id, rid, rtype, input_count, None, input_count, 0,
             "FAIL", f"invalid rule configuration: {reason}"[:1000])
            for rid, rtype, reason in invalid_rules])
        fail_etl(failcls.DQ_VALIDATION, failcls.CONFIGURATION_ERROR,
                 f"{len(invalid_rules)} active DQ rule(s) are invalid: "
                 + "; ".join(f"{rid}:{reason}" for rid, _t, reason in invalid_rules[:5]),
                 "DQ_CONFIG_ERROR", s_count=input_count)

    # ---- Stage: cleansing transforms BEFORE validation (Bronze untouched) ----
    work = bronze_df
    for t in transforms:
        col = t["column_name"]
        qc = F.col(f"`{col}`")
        if t["rule_type"] == dqr.TRIM_STRING:
            work = work.withColumn(col, F.trim(qc.cast("string")))
        elif t["rule_type"] == dqr.STANDARDIZE_CASE:
            mode = dqr.normalize_case_mode(t["rule_value"])
            work = work.withColumn(col, F.upper(qc) if mode == "UPPER" else F.lower(qc))
        elif t["rule_type"] == dqr.DEFAULT_VALUE:
            target_type = work.schema[col].dataType
            default_col = F.lit(t["rule_value"]).cast(target_type)
            work = work.withColumn(col, F.when(qc.isNull(), default_col).otherwise(qc))

    # ---- Stage: validation -> per-row failure-reason array -------------------
    empty_arr = F.array().cast("array<string>")
    failures = empty_arr
    rule_conditions = []
    dup_helper_cols = []
    for idx, v in enumerate(validations):
        rt = v["rule_type"]
        col = v.get("column_name")
        qc = F.col(f"`{col}`") if col else None
        if rt == dqr.NOT_NULL:
            cond = qc.isNull()
            checked = input_count
            reason_text = dqr.reason(rt, col)
        elif rt == dqr.DATA_TYPE:
            cast_type = dqr.normalize_cast_type(v["rule_value"])
            cond = qc.isNotNull() & F.expr(
                f"try_cast(`{col}` AS {cast_type})").isNull()
            checked = work.where(qc.isNotNull()).count()
            reason_text = dqr.reason(rt, col)
        elif rt == dqr.ALLOWED_VALUES:
            allowed = dqr.parse_allowed_values(v["rule_value"])
            cond = qc.isNotNull() & (~qc.cast("string").isin([str(a) for a in allowed]))
            checked = work.where(qc.isNotNull()).count()
            reason_text = dqr.reason(rt, col)
        elif rt == dqr.DUPLICATE_KEY:
            key_cols = dqr.duplicate_key_columns(v, pk)
            # A unique helper column per rule so multiple DUPLICATE_KEY rules
            # never overwrite one another's group counts.
            helper = f"_dupcount_{idx}"
            w = Window.partitionBy(*[F.col(f"`{c}`") for c in key_cols])
            work = work.withColumn(helper, F.count(F.lit(1)).over(w))
            dup_helper_cols.append(helper)
            # Every record of a duplicate group is rejected (documented policy).
            cond = F.col(helper) > 1
            checked = input_count
            reason_text = f"{rt}:{'+'.join(key_cols)}"
        else:
            continue

        rejecting = dqr.is_rejecting_severity(v.get("severity"))
        if rejecting:
            failures = F.concat(
                failures,
                F.when(cond, F.array(F.lit(reason_text))).otherwise(empty_arr))
        rule_conditions.append((v["rule_id"], rt, cond, checked, rejecting))

    work = work.withColumn("_dq_failures", failures).cache()

    valid_df = work.where(F.size("_dq_failures") == 0)
    invalid_df = work.where(F.size("_dq_failures") > 0)
    valid_count = valid_df.count()
    rejected_distinct = invalid_df.count()   # one row = one rejected record

    # ---- per-rule dq_result aggregates ---------------------------------------
    result_out = []
    for rule_id, rt, cond, checked, rejecting in rule_conditions:
        failed = work.where(cond).count()
        status = "PASS" if failed == 0 else ("FAIL" if rejecting else "WARN")
        result_out.append((run_id, src_id, rule_id, rt, input_count, checked,
                           failed, max((checked or 0) - failed, 0), status,
                           f"rejecting={rejecting}"))
    if not rule_rows:
        # No-rule behavior: still record NO_RULES and reconcile Bronze->Silver.
        result_out.append((run_id, src_id, None, "NO_RULES", input_count, input_count,
                           0, input_count, "PASS", "no active DQ/transform rules"))
    write_dq_results(result_out)

    # ---- quarantine rejected rows (idempotent per run + table) --------------
    internal_cols = ["_dq_failures"] + dup_helper_cols
    if rejected_distinct > 0 and quarantine_enabled:
        keep = [c for c in bronze_df.columns if c not in exclude_cols]
        # Replace this run/table's quarantine rows so a retry cannot duplicate
        # them. History for every other run/table is untouched.
        spark.sql(f"""
            DELETE FROM {ctrl('dq_quarantine')}
            WHERE run_id = {escape_string_literal(run_id)}
              AND source_table_id = {escape_string_literal(src_id)}
        """)
        (invalid_df
         .withColumn("record_json", F.to_json(F.struct(*[F.col(f"`{c}`") for c in keep])))
         .withColumn("failure_reason", F.concat_ws(",", F.col("_dq_failures")))
         .select(
             F.lit(run_id).alias("run_id"), F.lit(src_id).alias("source_table_id"),
             F.lit(None).cast("string").alias("rule_id"),
             "record_json", "failure_reason")
         .withColumn("quarantined_ts", F.current_timestamp())
         .write.format("delta").mode("append").option("mergeSchema", "true")
         .saveAsTable(ctrl("dq_quarantine").replace("`", "")))
    elif rejected_distinct > 0:
        print(f"  quarantine disabled: {rejected_distinct} rejected record(s) "
              "counted for reconciliation but NOT persisted.")

    # ---- prepare valid output (drop internal technical columns) --------------
    valid_out = valid_df.drop(*[c for c in internal_cols if c in valid_df.columns])

    # ---- write Silver + ETL reconciliation (before checkpoint) ---------------
    current_stage = failcls.SILVER_WRITE
    spark.sql(ddl.build_create_schema(silver_catalog, silver_schema, "Silver ETL output"))
    silver_exists = spark.catalog.tableExists(silver_fqn)

    if effective_mode == "FULL":
        (valid_out.write.format("delta").mode("overwrite")
         .option("overwriteSchema", "true").saveAsTable(silver_fqn))
        current_stage = failcls.ETL_RECONCILIATION
        silver_count = spark.table(silver_fqn).count()
        recon_result = recon.reconcile_etl_full(
            input_count, valid_count, rejected_distinct, silver_count)
    elif pk:
        # Duplicate valid keys are detected BEFORE staging and merging: a MERGE
        # with duplicate source keys is non-deterministic, so it must not run.
        missing_pk = [c for c in pk if c not in valid_out.columns]
        if missing_pk:
            fail_etl(failcls.SILVER_WRITE, failcls.CONFIGURATION_ERROR,
                     f"primary-key column(s) {missing_pk} are not present in the "
                     "ETL output; cannot MERGE into Silver", "ETL_CONFIG_ERROR",
                     s_count=input_count, t_count=valid_count)
        dup_valid = (valid_out.groupBy(*[F.col(f"`{c}`") for c in pk]).count()
                     .where(F.col("count") > 1).count())
        if dup_valid > 0:
            current_stage = failcls.ETL_RECONCILIATION
            write_recon(recon.reconcile_etl_incremental_merge(
                input_count, valid_count, rejected_distinct, dup_valid, 0))
            fail_etl(failcls.ETL_RECONCILIATION, failcls.RECONCILIATION_ERROR,
                     f"{dup_valid} duplicate primary-key group(s) in the valid ETL "
                     "input; MERGE not executed and ETL watermark unchanged",
                     "ETL_RECONCILIATION_FAILED",
                     s_count=input_count, t_count=valid_count,
                     extra={"rejected_row_count": rejected_distinct})

        if not silver_exists:
            (valid_out.limit(0).write.format("delta").mode("overwrite")
             .option("overwriteSchema", "true").saveAsTable(silver_fqn))
        stage_fqn = f"{silver_catalog}.{silver_schema}.{silver_table}_etl_stage"
        (valid_out.write.format("delta").mode("overwrite")
         .option("overwriteSchema", "true").saveAsTable(stage_fqn))
        spark.sql(ddl.build_merge_sql(silver_catalog, silver_schema, silver_table,
                                      f"{silver_table}_etl_stage", pk))
        current_stage = failcls.ETL_RECONCILIATION
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
        # INCREMENTAL, no reliable key: retry-safe interval replacement using the
        # exact same typed bounds that selected the Bronze input.
        if not silver_exists:
            (valid_out.limit(0).write.format("delta").mode("overwrite")
             .option("overwriteSchema", "true").saveAsTable(silver_fqn))
        silver_wm = F.col(f"`{etl_wm_col}`")
        interval_cond = silver_wm <= upper_bound_col
        if lower_bound_col is not None:
            interval_cond = interval_cond & (silver_wm > lower_bound_col)
        from delta.tables import DeltaTable
        DeltaTable.forName(spark, silver_fqn).delete(interval_cond)
        (valid_out.write.format("delta").mode("append").saveAsTable(silver_fqn))
        current_stage = failcls.ETL_RECONCILIATION
        silver_interval = spark.table(silver_fqn).where(interval_cond).count()
        recon_result = recon.reconcile_etl_interval(
            input_count, valid_count, rejected_distinct, silver_interval)

    write_recon(recon_result)

    # ---- ETL checkpoint gate -------------------------------------------------
    if not recon_result.passed:
        fail_etl(failcls.ETL_RECONCILIATION, failcls.RECONCILIATION_ERROR,
                 f"ETL reconciliation failed: {recon_result.status}",
                 "ETL_RECONCILIATION_FAILED", s_count=input_count, t_count=valid_count,
                 extra={"extracted_row_count": input_count,
                        "applied_row_count": valid_count,
                        "rejected_row_count": rejected_distinct})

    current_stage = failcls.CHECKPOINT
    etl_fields = {
        "last_successful_etl_run_id": run_id,
        "last_successful_etl_run_ts": now_utc().strftime("%Y-%m-%d %H:%M:%S.%f"),
        "etl_current_status": "ETL_SUCCEEDED", "etl_error_message": None,
    }
    # Advances only to the frozen upper bound, and only after reconciliation.
    committed_watermark = etlwu.checkpoint_value(work_unit, recon_result.passed)
    if committed_watermark is not None:
        etl_fields["last_etl_watermark_value"] = committed_watermark
    repo.update_control(src_id, etl_fields)
    log_etl(etl_op, "SUCCEEDED", None, input_count, valid_count,
            extra={"extracted_row_count": input_count,
                   "applied_row_count": valid_count,
                   "rejected_row_count": rejected_distinct,
                   **work_unit.audit_fields(attempt_number)})
    print(f"  ETL SUCCEEDED: input={input_count} valid={valid_count} "
          f"rejected={rejected_distinct} recon={recon_result.status} "
          f"interval=({work_unit.lower_watermark}, {work_unit.upper_watermark}]")
    result = {"status": "SUCCEEDED", "source_table_id": src_id, "run_id": run_id,
              "mode": effective_mode, "input": input_count, "valid": valid_count,
              "rejected": rejected_distinct,
              "lower_watermark": work_unit.lower_watermark,
              "upper_watermark": work_unit.upper_watermark}

except Exception as e:
    # A specialized handler already recorded the accurate stage; never relabel it
    # and never write a second failure row for the same operation.
    if not failure_already_logged:
        msg = failcls.sanitize_message(e)
        cls = failcls.classify_failure(e, current_stage, idempotent=True)
        etl_status = {
            failcls.DQ_VALIDATION: "DQ_FAILED",
            failcls.ETL_RECONCILIATION: "ETL_RECONCILIATION_FAILED",
            failcls.CHECKPOINT: "ETL_CHECKPOINT_FAILED",
        }.get(current_stage, "ETL_WRITE_FAILED")
        try:
            repo.update_control(src_id, {
                "etl_current_status": etl_status,
                "etl_error_message": msg[:1000]})
        except Exception as update_error:
            print(f"  [warn] could not record ETL status: {update_error}")
        try:
            log_etl(etl_op, "FAILED", msg[:1000], None, None,
                    extra={"failure_stage": cls.stage, "error_category": cls.category,
                           "retry_eligible": cls.retry_eligible,
                           **(work_unit.audit_fields(attempt_number)
                              if work_unit is not None else {})})
        except Exception as log_error:
            print(f"  [warn] could not write ETL audit: {log_error}")
    print(f"  ETL FAILED {src_id} at {current_stage}: "
          f"{failcls.sanitize_message(e)[:300]}")
    raise

# COMMAND ----------

dbutils.notebook.exit(json.dumps(result))
