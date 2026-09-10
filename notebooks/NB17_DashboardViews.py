# Databricks notebook source
# MAGIC %md
# MAGIC # NB17_DashboardViews
# MAGIC Creates dashboard-ready SQL views over the existing control/audit tables.
# MAGIC These are plain views (no new materialized reporting tables); base tables
# MAGIC keep full history and the views surface the latest record per table using
# MAGIC window functions. Idempotent (CREATE OR REPLACE).

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

def ctrl(t):
    return f"{quote_databricks(CATALOG)}.{quote_databricks(CONTROL_SCHEMA)}.{quote_databricks(t)}"

# COMMAND ----------

# ---- vw_assessment_summary -------------------------------------------------
spark.sql(f"""
CREATE OR REPLACE VIEW {ctrl('vw_assessment_summary')} AS
WITH sa AS (
  SELECT connection_id, source_system, source_database,
    count(DISTINCT source_schema) AS schemas,
    count(CASE WHEN object_type='TABLE' THEN 1 END) AS tables,
    count(CASE WHEN object_type='VIEW' THEN 1 END) AS views,
    count(CASE WHEN object_type='PROCEDURE' THEN 1 END) AS procedures,
    count(CASE WHEN object_type='FUNCTION' THEN 1 END) AS functions,
    count(CASE WHEN object_type IN ('PACKAGE','PACKAGE_BODY') THEN 1 END) AS packages,
    count(*) AS assessed_objects,
    count(CASE WHEN compatibility_status='COMPATIBLE' THEN 1 END) AS compatible,
    count(CASE WHEN compatibility_status='REVIEW' THEN 1 END) AS review,
    count(CASE WHEN compatibility_status='MANUAL' THEN 1 END) AS manual,
    count(CASE WHEN compatibility_status='UNABLE_TO_ASSESS' THEN 1 END) AS unable
  FROM {ctrl('source_assessment')}
  GROUP BY connection_id, source_system, source_database
),
soa AS (
  SELECT connection_id,
    count(CASE WHEN complexity_category='AUTO_CONVERT' THEN 1 END) AS auto_convert,
    count(CASE WHEN complexity_category='CONVERT_WITH_REVIEW' THEN 1 END) AS convert_with_review,
    count(CASE WHEN complexity_category='MANUAL_REDESIGN' THEN 1 END) AS manual_redesign
  FROM {ctrl('sql_object_assessment')}
  GROUP BY connection_id
)
SELECT sa.*, coalesce(soa.auto_convert,0) AS auto_convert,
  coalesce(soa.convert_with_review,0) AS convert_with_review,
  coalesce(soa.manual_redesign,0) AS manual_redesign
FROM sa LEFT JOIN soa ON sa.connection_id = soa.connection_id
""")
print("vw_assessment_summary ready.")

# COMMAND ----------

# ---- vw_ingest_status ------------------------------------------------------
spark.sql(f"""
CREATE OR REPLACE VIEW {ctrl('vw_ingest_status')} AS
WITH latest AS (
  SELECT *, ROW_NUMBER() OVER (
      PARTITION BY source_table_id ORDER BY ended_ts DESC) AS rn
  FROM {ctrl('table_run_log')}
  WHERE operation IN ('FULL_LOAD','DELTA_MERGE','DELTA_APPEND','DELTA_FULL_REFRESH')
)
SELECT c.connection_id, c.source_table_id, c.source_system,
  c.source_schema, c.source_table,
  concat_ws('.', c.target_catalog, c.target_schema, c.target_table) AS bronze_target,
  c.table_decision, c.load_strategy, c.current_status AS ingest_status,
  c.initial_load_completed, l.operation AS latest_operation, l.run_id AS latest_run_id,
  l.source_row_count, l.target_row_count, l.attempt_number, l.failure_stage,
  l.retry_eligible, c.last_watermark_value, c.updated_ts
FROM {ctrl('source_table_control')} c
LEFT JOIN latest l ON c.source_table_id = l.source_table_id AND l.rn = 1
""")
print("vw_ingest_status ready.")

# COMMAND ----------

# ---- vw_etl_status ---------------------------------------------------------
spark.sql(f"""
CREATE OR REPLACE VIEW {ctrl('vw_etl_status')} AS
WITH latest AS (
  SELECT *, ROW_NUMBER() OVER (
      PARTITION BY source_table_id ORDER BY ended_ts DESC) AS rn
  FROM {ctrl('table_run_log')}
  WHERE operation IN ('ETL_FULL','ETL_INCREMENTAL')
),
q AS (
  SELECT run_id, source_table_id, count(*) AS quarantine_count
  FROM {ctrl('dq_quarantine')} GROUP BY run_id, source_table_id
)
SELECT c.source_table_id,
  concat_ws('.', c.target_catalog, c.target_schema, c.target_table) AS bronze_target,
  concat_ws('.', c.silver_catalog, c.silver_schema, c.silver_table) AS silver_target,
  c.etl_load_strategy, c.etl_current_status, l.operation AS latest_etl_operation,
  l.run_id AS latest_etl_run_id, l.source_row_count AS input_count,
  l.target_row_count AS valid_count, l.rejected_row_count AS failed_count,
  coalesce(q.quarantine_count, 0) AS quarantine_count,
  l.attempt_number AS etl_attempt_number, l.failure_stage AS etl_failure_stage,
  c.last_etl_watermark_value, c.updated_ts
FROM {ctrl('source_table_control')} c
LEFT JOIN latest l ON c.source_table_id = l.source_table_id AND l.rn = 1
LEFT JOIN q ON q.run_id = l.run_id AND q.source_table_id = c.source_table_id
""")
print("vw_etl_status ready.")

# COMMAND ----------

# ---- vw_validation_status --------------------------------------------------
spark.sql(f"""
CREATE OR REPLACE VIEW {ctrl('vw_validation_status')} AS
WITH ingest_recon AS (
  SELECT source_table_id, status, captured_ts, ROW_NUMBER() OVER (
      PARTITION BY source_table_id ORDER BY captured_ts DESC) AS rn
  FROM {ctrl('reconciliation_results')}
  WHERE check_type IN ('FULL_SNAPSHOT_COUNT','DELTA_INTERVAL_COUNT','STAGE_COUNT',
    'DUPLICATE_PRIMARY_KEY','MERGED_KEY_EXISTENCE','ROW_COUNT','DELTA_RECON_SUMMARY')
),
etl_recon AS (
  SELECT source_table_id, status, captured_ts, ROW_NUMBER() OVER (
      PARTITION BY source_table_id ORDER BY captured_ts DESC) AS rn
  FROM {ctrl('reconciliation_results')}
  WHERE check_type LIKE 'ETL_%'
),
dq AS (
  SELECT source_table_id, status, captured_ts, ROW_NUMBER() OVER (
      PARTITION BY source_table_id ORDER BY captured_ts DESC) AS rn
  FROM {ctrl('dq_result')}
),
q AS (
  SELECT source_table_id, count(*) AS quarantine_count
  FROM {ctrl('dq_quarantine')} GROUP BY source_table_id
)
SELECT c.source_table_id, c.source_schema, c.source_table,
  ir.status AS latest_ingest_recon_status,
  er.status AS latest_etl_recon_status,
  dq.status AS latest_dq_status,
  coalesce(q.quarantine_count, 0) AS quarantine_count,
  coalesce(c.error_message, c.etl_error_message) AS outstanding_error,
  c.last_successful_run_ts AS last_successful_ingest_ts,
  c.last_successful_etl_run_ts AS last_successful_etl_ts
FROM {ctrl('source_table_control')} c
LEFT JOIN ingest_recon ir ON ir.source_table_id = c.source_table_id AND ir.rn = 1
LEFT JOIN etl_recon er ON er.source_table_id = c.source_table_id AND er.rn = 1
LEFT JOIN dq ON dq.source_table_id = c.source_table_id AND dq.rn = 1
LEFT JOIN q ON q.source_table_id = c.source_table_id
""")
print("vw_validation_status ready.")

# COMMAND ----------

dbutils.notebook.exit(json.dumps({"status": "SUCCEEDED", "views": [
    "vw_assessment_summary", "vw_ingest_status", "vw_etl_status",
    "vw_validation_status"]}))
