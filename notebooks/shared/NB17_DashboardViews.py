# Databricks notebook source
# MAGIC %md
# MAGIC # NB17_DashboardViews
# MAGIC Creates dashboard-ready SQL views over the existing control/audit tables.
# MAGIC These are plain views (no new materialized reporting tables); base tables
# MAGIC keep full history and the views surface the latest record per table using
# MAGIC deterministic window ordering. Idempotent (CREATE OR REPLACE).

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

def ctrl(t):
    return f"{quote_databricks(CATALOG)}.{quote_databricks(CONTROL_SCHEMA)}.{quote_databricks(t)}"

# Deterministic "latest run" ordering shared by every view: a null ended_ts never
# wins, and run_id breaks an exact timestamp tie.
LATEST_RUN_ORDER = ("ended_ts DESC NULLS LAST, started_ts DESC NULLS LAST, "
                    "run_id DESC")

# COMMAND ----------

# ---- vw_assessment_summary -------------------------------------------------
# Only the LATEST assessment per connection is aggregated, so repeated
# assessments are not summed together as if they were one current inventory.
spark.sql(f"""
CREATE OR REPLACE VIEW {ctrl('vw_assessment_summary')} AS
WITH latest_sa AS (
  SELECT connection_id, assessment_id,
         ROW_NUMBER() OVER (PARTITION BY connection_id
             ORDER BY max_captured_ts DESC NULLS LAST, assessment_id DESC) AS rn
  FROM (
    SELECT connection_id, assessment_id, max(captured_ts) AS max_captured_ts
    FROM {ctrl('source_assessment')}
    GROUP BY connection_id, assessment_id
  )
),
sa AS (
  SELECT a.connection_id, a.assessment_id, a.source_system, a.source_database,
    count(DISTINCT a.source_schema) AS schemas,
    count(CASE WHEN a.object_type='TABLE' THEN 1 END) AS tables,
    count(CASE WHEN a.object_type='VIEW' THEN 1 END) AS views,
    count(CASE WHEN a.object_type='PROCEDURE' THEN 1 END) AS procedures,
    count(CASE WHEN a.object_type='FUNCTION' THEN 1 END) AS functions,
    count(CASE WHEN a.object_type IN ('PACKAGE','PACKAGE_BODY') THEN 1 END) AS packages,
    count(*) AS assessed_objects,
    count(CASE WHEN a.compatibility_status='COMPATIBLE' THEN 1 END) AS compatible,
    count(CASE WHEN a.compatibility_status='REVIEW' THEN 1 END) AS review,
    count(CASE WHEN a.compatibility_status='MANUAL' THEN 1 END) AS manual,
    count(CASE WHEN a.compatibility_status='UNABLE_TO_ASSESS' THEN 1 END) AS unable
  FROM {ctrl('source_assessment')} a
  JOIN latest_sa l
    ON a.connection_id = l.connection_id
   AND a.assessment_id = l.assessment_id AND l.rn = 1
  GROUP BY a.connection_id, a.assessment_id, a.source_system, a.source_database
),
latest_soa AS (
  SELECT connection_id, assessment_id,
         ROW_NUMBER() OVER (PARTITION BY connection_id
             ORDER BY max_captured_ts DESC NULLS LAST, assessment_id DESC) AS rn
  FROM (
    SELECT connection_id, assessment_id, max(captured_ts) AS max_captured_ts
    FROM {ctrl('sql_object_assessment')}
    GROUP BY connection_id, assessment_id
  )
),
soa AS (
  SELECT o.connection_id,
    count(1) AS sql_objects_inventoried,
    count(CASE WHEN o.source_definition IS NOT NULL
                AND trim(o.source_definition) <> '' THEN 1 END)
      AS sql_objects_with_definition,
    count(CASE WHEN o.source_definition IS NULL
                 OR trim(o.source_definition) = '' THEN 1 END)
      AS sql_objects_without_definition
  FROM {ctrl('sql_object_assessment')} o
  JOIN latest_soa l
    ON o.connection_id = l.connection_id
   AND o.assessment_id = l.assessment_id AND l.rn = 1
  GROUP BY o.connection_id
)
SELECT sa.*, sc.connection_name, sc.connection_status, sc.is_active,
  sc.last_validated_ts,
  coalesce(soa.sql_objects_inventoried, 0) AS sql_objects_inventoried,
  coalesce(soa.sql_objects_with_definition, 0) AS sql_objects_with_definition,
  coalesce(soa.sql_objects_without_definition, 0) AS sql_objects_without_definition
FROM sa
LEFT JOIN soa ON sa.connection_id = soa.connection_id
LEFT JOIN {ctrl('source_connection')} sc
  ON sa.connection_id = sc.connection_id
""")
print("vw_assessment_summary ready.")

# COMMAND ----------

# ---- vw_ingest_status ------------------------------------------------------
spark.sql(f"""
CREATE OR REPLACE VIEW {ctrl('vw_ingest_status')} AS
WITH latest AS (
  SELECT *, ROW_NUMBER() OVER (
  PARTITION BY connection_id, source_table_id
  ORDER BY {LATEST_RUN_ORDER}) AS rn
  FROM {ctrl('table_run_log')}
  WHERE operation IN ('FULL_LOAD','DELTA_MERGE','DELTA_APPEND','DELTA_FULL_REFRESH')
)
SELECT c.connection_id, sc.connection_name, c.source_table_id, c.source_system,
  sc.connection_status, sc.is_active, sc.last_validated_ts,
  c.source_schema, c.source_table,
  concat_ws('.', c.target_catalog, c.target_schema, c.target_table) AS bronze_target,
  c.table_decision, c.load_strategy, c.current_status AS ingest_status,
  c.initial_load_completed, l.operation AS latest_operation, l.run_id AS latest_run_id,
  l.source_row_count, l.target_row_count, l.attempt_number, l.failure_stage,
  l.error_category, l.retry_eligible, c.last_watermark_value, c.updated_ts
FROM {ctrl('source_table_control')} c
LEFT JOIN {ctrl('source_connection')} sc
  ON c.connection_id = sc.connection_id
LEFT JOIN latest l ON c.connection_id = l.connection_id
  AND c.source_table_id = l.source_table_id AND l.rn = 1
""")
print("vw_ingest_status ready.")

# COMMAND ----------

# ---- vw_etl_status ---------------------------------------------------------
# quarantine_count is scoped to the LATEST ETL run for the table, not lifetime.
spark.sql(f"""
CREATE OR REPLACE VIEW {ctrl('vw_etl_status')} AS
WITH latest AS (
  SELECT *, ROW_NUMBER() OVER (
  PARTITION BY connection_id, source_table_id
  ORDER BY {LATEST_RUN_ORDER}) AS rn
  FROM {ctrl('table_run_log')}
  WHERE operation IN ('ETL_FULL','ETL_INCREMENTAL')
),
q AS (
  SELECT run_id, connection_id, source_table_id, count(*) AS quarantine_count
  FROM {ctrl('dq_quarantine')}
  GROUP BY run_id, connection_id, source_table_id
)
SELECT c.source_table_id, c.connection_id, sc.connection_name, c.source_system,
  sc.connection_status, sc.is_active, sc.last_validated_ts,
  concat_ws('.', c.target_catalog, c.target_schema, c.target_table) AS bronze_target,
  concat_ws('.', c.silver_catalog, c.silver_schema, c.silver_table) AS silver_target,
  c.etl_load_strategy, c.etl_current_status, l.operation AS latest_etl_operation,
  l.run_id AS latest_etl_run_id, l.source_row_count AS input_count,
  l.target_row_count AS valid_count, l.rejected_row_count AS failed_count,
  coalesce(q.quarantine_count, 0) AS latest_run_quarantine_count,
  l.attempt_number AS etl_attempt_number, l.failure_stage AS etl_failure_stage,
  c.last_etl_watermark_value, c.updated_ts
FROM {ctrl('source_table_control')} c
LEFT JOIN {ctrl('source_connection')} sc
  ON c.connection_id = sc.connection_id
LEFT JOIN latest l ON c.connection_id = l.connection_id
  AND c.source_table_id = l.source_table_id AND l.rn = 1
LEFT JOIN q ON q.run_id = l.run_id
  AND q.connection_id = c.connection_id
  AND q.source_table_id = c.source_table_id
""")
print("vw_etl_status ready.")

# COMMAND ----------

# ---- vw_validation_status --------------------------------------------------
# Ingest and ETL errors are exposed separately (never coalesced into one field),
# and the quarantine count is the LATEST ETL run's count.
spark.sql(f"""
CREATE OR REPLACE VIEW {ctrl('vw_validation_status')} AS
WITH ingest_recon AS (
  SELECT connection_id, source_table_id, status, captured_ts, ROW_NUMBER() OVER (
      PARTITION BY connection_id, source_table_id
      ORDER BY captured_ts DESC NULLS LAST, run_id DESC) AS rn
  FROM {ctrl('reconciliation_results')}
  WHERE check_type IN ('FULL_SNAPSHOT_COUNT','DELTA_INTERVAL_COUNT','STAGE_COUNT',
    'DUPLICATE_PRIMARY_KEY','MERGED_KEY_EXISTENCE','DELTA_RECON_SUMMARY')
),
etl_recon AS (
  SELECT connection_id, source_table_id, status, captured_ts, ROW_NUMBER() OVER (
      PARTITION BY connection_id, source_table_id
      ORDER BY captured_ts DESC NULLS LAST, run_id DESC) AS rn
  FROM {ctrl('reconciliation_results')}
  WHERE check_type LIKE 'ETL_%'
),
dq AS (
  SELECT connection_id, source_table_id, status, captured_ts, ROW_NUMBER() OVER (
      PARTITION BY connection_id, source_table_id
      ORDER BY captured_ts DESC NULLS LAST, run_id DESC) AS rn
  FROM {ctrl('dq_result')}
),
latest_etl_run AS (
  SELECT connection_id, source_table_id, run_id, ROW_NUMBER() OVER (
      PARTITION BY connection_id, source_table_id
      ORDER BY {LATEST_RUN_ORDER}) AS rn
  FROM {ctrl('table_run_log')}
  WHERE operation IN ('ETL_FULL','ETL_INCREMENTAL')
),
q AS (
  SELECT run_id, connection_id, source_table_id, count(*) AS quarantine_count
  FROM {ctrl('dq_quarantine')}
  GROUP BY run_id, connection_id, source_table_id
)
SELECT c.source_table_id, c.connection_id, sc.connection_name, c.source_system,
  sc.connection_status, sc.is_active, sc.last_validated_ts,
  c.source_schema, c.source_table,
  ir.status AS latest_ingest_recon_status,
  er.status AS latest_etl_recon_status,
  dq.status AS latest_dq_status,
  coalesce(q.quarantine_count, 0) AS latest_run_quarantine_count,
  c.error_message AS ingest_error,
  c.etl_error_message AS etl_error,
  c.last_successful_run_ts AS last_successful_ingest_ts,
  c.last_successful_etl_run_ts AS last_successful_etl_ts
FROM {ctrl('source_table_control')} c
LEFT JOIN {ctrl('source_connection')} sc
  ON c.connection_id = sc.connection_id
LEFT JOIN ingest_recon ir ON ir.connection_id = c.connection_id
  AND ir.source_table_id = c.source_table_id AND ir.rn = 1
LEFT JOIN etl_recon er ON er.connection_id = c.connection_id
  AND er.source_table_id = c.source_table_id AND er.rn = 1
LEFT JOIN dq ON dq.connection_id = c.connection_id
  AND dq.source_table_id = c.source_table_id AND dq.rn = 1
LEFT JOIN latest_etl_run ler ON ler.connection_id = c.connection_id
  AND ler.source_table_id = c.source_table_id AND ler.rn = 1
LEFT JOIN q ON q.run_id = ler.run_id
  AND q.connection_id = c.connection_id
  AND q.source_table_id = c.source_table_id
""")
print("vw_validation_status ready.")

# COMMAND ----------

dbutils.notebook.exit(json.dumps({"status": "SUCCEEDED", "views": [
    "vw_assessment_summary", "vw_ingest_status", "vw_etl_status",
    "vw_validation_status"]}))
