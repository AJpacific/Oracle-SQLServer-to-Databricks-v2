# Databricks notebook source
# MAGIC %md
# MAGIC # NB00_ControlTableInit
# MAGIC Creates the control schema and every control/audit Delta table inside the
# MAGIC existing Unity Catalog catalog. Idempotent: safe to re-run. Never drops tables.

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

dbutils.widgets.dropdown("seed_poc_rows", "false", ["true", "false"])
seed_poc = dbutils.widgets.get("seed_poc_rows") == "true"

run_id = new_run_id("init")
set_task_value("run_id", run_id)
print("run_id:", run_id)

# COMMAND ----------

# MAGIC %md ### 1. Control schema (catalog already exists)

# COMMAND ----------

# The Unity Catalog catalog is expected to already exist; only the control schema
# is created here, so no CREATE CATALOG privilege is required.
spark.sql(ddl.build_create_schema(
  CATALOG, CONTROL_SCHEMA, "migration accelerator control & audit"))
print("Control schema ready.")

# COMMAND ----------

# MAGIC %md ### 2. Control & audit tables

# COMMAND ----------

def ctrl(t):
    return f"{quote_databricks(CATALOG)}.{quote_databricks(CONTROL_SCHEMA)}.{quote_databricks(t)}"

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {ctrl('source_connection')} (
  connection_id            STRING,
  connection_name          STRING,
  source_system            STRING,
  source_server            STRING,
  source_database          STRING,
  secret_scope             STRING,
  trust_server_certificate BOOLEAN,
  connection_status        STRING,
  is_active                BOOLEAN,
  error_message            STRING,
  last_validated_ts        TIMESTAMP,
  created_ts               TIMESTAMP,
  updated_ts               TIMESTAMP
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {ctrl('source_table_control')} (
  source_table_id          STRING,
  connection_id            STRING,
  source_identity_version  INT,
  legacy_source_table_id   STRING,
  source_system            STRING,
  source_server            STRING,
  source_database          STRING,
  source_schema            STRING,
  source_table             STRING,
  target_catalog           STRING,
  target_schema            STRING,
  target_table             STRING,
  is_active                BOOLEAN,
  mapping_status           STRING,
  table_decision           STRING,
  load_strategy            STRING,
  delete_policy            STRING,
  primary_key_columns      ARRAY<STRING>,
  watermark_column         STRING,
  watermark_data_type      STRING,
  last_watermark_value     STRING,
  initial_load_completed   BOOLEAN,
  last_successful_run_id   STRING,
  last_successful_run_ts   TIMESTAMP,
  current_status           STRING,
  error_message            STRING,
  created_ts               TIMESTAMP,
  updated_ts               TIMESTAMP
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {ctrl('source_inventory')} (
  run_id STRING, connection_id STRING, source_table_id STRING, source_system STRING,
  source_server STRING, source_database STRING,
  source_schema STRING, source_table STRING, column_name STRING,
  ordinal_position INT, is_nullable STRING, data_type STRING,
  character_maximum_length INT, numeric_precision INT, numeric_scale INT,
  datetime_precision INT, is_identity BOOLEAN, is_computed BOOLEAN,
  is_hidden BOOLEAN, is_rowversion BOOLEAN, source_type_schema STRING,
  captured_ts TIMESTAMP
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {ctrl('normalized_source_inventory')} (
  run_id STRING, connection_id STRING, source_table_id STRING, source_system STRING,
  source_server STRING, source_database STRING,
  source_schema STRING, source_table STRING, column_name STRING,
  ordinal_position INT, raw_type STRING, normalized_type STRING,
  precision INT, scale INT, length INT, is_nullable BOOLEAN,
  is_identity BOOLEAN, is_computed BOOLEAN, is_hidden BOOLEAN,
  is_rowversion BOOLEAN, source_type_schema STRING,
  schema_hash STRING, captured_ts TIMESTAMP
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {ctrl('resolved_column_mappings')} (
  run_id STRING, connection_id STRING, source_table_id STRING, source_system STRING,
  source_server STRING, source_database STRING,
  source_schema STRING, source_table STRING, column_name STRING,
  ordinal_position INT, source_type STRING, databricks_delta_type STRING,
  mapping_status STRING, fidelity STRING, notes STRING, is_nullable BOOLEAN,
  is_identity BOOLEAN, is_computed BOOLEAN, is_hidden BOOLEAN,
  is_rowversion BOOLEAN, source_type_schema STRING,
  captured_ts TIMESTAMP
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {ctrl('mapping_validation_results')} (
  run_id STRING, connection_id STRING, source_table_id STRING, source_system STRING,
  source_server STRING, source_database STRING,
  source_schema STRING, source_table STRING, column_name STRING,
  severity STRING, rule STRING, message STRING, captured_ts TIMESTAMP
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {ctrl('table_load_decisions')} (
  run_id STRING, connection_id STRING, source_table_id STRING, source_system STRING,
  source_server STRING, source_database STRING,
  source_schema STRING, source_table STRING,
  decision STRING, reason STRING, blocked_columns INT, review_columns INT,
  total_columns INT, captured_ts TIMESTAMP
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {ctrl('review_queue')} (
  connection_id STRING, source_table_id STRING, source_system STRING,
  source_server STRING, source_database STRING,
  source_schema STRING, source_table STRING, decision STRING, reason STRING,
  blocked_columns INT, review_columns INT, total_columns INT,
  review_status STRING, run_id STRING, captured_ts TIMESTAMP
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {ctrl('job_run_log')} (
  run_id STRING, job_name STRING, status STRING, started_ts TIMESTAMP,
  ended_ts TIMESTAMP, message STRING
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {ctrl('table_run_log')} (
  run_id STRING, connection_id STRING, source_table_id STRING, source_system STRING,
  source_server STRING, source_database STRING,
  source_schema STRING, source_table STRING, operation STRING,
  target_full_name STRING, source_row_count BIGINT, target_row_count BIGINT,
  status STRING, error_message STRING, started_ts TIMESTAMP, ended_ts TIMESTAMP
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {ctrl('delta_sync_queue')} (
  run_id STRING, connection_id STRING, source_table_id STRING, source_system STRING,
  source_server STRING, source_database STRING,
  source_schema STRING, source_table STRING,
  target_catalog STRING, target_schema STRING, target_table STRING,
  stage_table STRING, load_strategy STRING, delete_policy STRING,
  primary_key_columns ARRAY<STRING>,
  watermark_column STRING, watermark_data_type STRING, last_watermark_value STRING,
  upper_watermark_value STRING,
  source_query STRING, status STRING, captured_ts TIMESTAMP
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {ctrl('reconciliation_results')} (
  run_id STRING, connection_id STRING, source_table_id STRING, source_system STRING,
  source_schema STRING, source_table STRING, check_type STRING,
  source_value STRING, target_value STRING, status STRING, message STRING,
  captured_ts TIMESTAMP
) USING DELTA
""")

# --- INGEST: source assessment & SQL-object assessment ---------------------
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {ctrl('source_assessment')} (
  assessment_id STRING, run_id STRING, connection_id STRING,
  source_system STRING, source_server STRING, source_database STRING,
  source_schema STRING, object_name STRING, object_type STRING,
  row_count BIGINT, row_count_method STRING, size_mb DECIMAL(18,2),
  column_count INT, compatibility_status STRING,
  assessment_message STRING, is_selected BOOLEAN,
  selection_status STRING, selected_ts TIMESTAMP, selected_by STRING,
  onboarding_run_id STRING, onboarding_attempt_id STRING,
  onboarding_started_ts TIMESTAMP, registration_completed_ts TIMESTAMP,
  onboarding_completed_ts TIMESTAMP, onboarding_failed_stage STRING,
  onboarding_error_message STRING,
  captured_ts TIMESTAMP
) USING DELTA
""")

# --- ROUTING: target configuration -----------------------------------------
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {ctrl('accelerator_target_config')} (
  config_id STRING,
  source_system STRING,
  connection_id STRING,
  target_catalog STRING,
  target_schema_mode STRING,
  target_schema STRING,
  is_default BOOLEAN,
  is_active BOOLEAN,
  created_ts TIMESTAMP,
  updated_ts TIMESTAMP
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {ctrl('sql_object_assessment')} (
  assessment_id STRING, run_id STRING, connection_id STRING,
  source_system STRING, source_database STRING, source_schema STRING,
  object_name STRING, object_type STRING, source_definition STRING,
  error_message STRING,
  captured_ts TIMESTAMP, updated_ts TIMESTAMP
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {ctrl('sql_object_artifact_manifest')} (
  connection_id             STRING,
  source_system             STRING,
  source_database           STRING,
  source_schema             STRING,
  object_type               STRING,
  object_name               STRING,
  target_catalog            STRING,
  target_schema             STRING,
  target_volume             STRING,
  artifact_path             STRING,
  source_definition_hash    STRING,
  source_assessment_id      STRING,
  source_captured_ts        TIMESTAMP,
  last_materialized_run_id  STRING,
  materialization_status    STRING,
  error_message             STRING,
  created_ts                TIMESTAMP,
  updated_ts                TIMESTAMP
) USING DELTA
""")

# --- ETL: data-quality rules, results, and quarantine ----------------------
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {ctrl('dq_rule')} (
  rule_id STRING, connection_id STRING, source_table_id STRING, rule_type STRING,
  column_name STRING, rule_value STRING, severity STRING,
  is_active BOOLEAN, created_ts TIMESTAMP, updated_ts TIMESTAMP
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {ctrl('dq_result')} (
  run_id STRING, connection_id STRING, source_table_id STRING,
  rule_id STRING, rule_type STRING,
  input_count BIGINT, checked_count BIGINT, failed_count BIGINT,
  passed_count BIGINT, status STRING, message STRING, captured_ts TIMESTAMP
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {ctrl('dq_quarantine')} (
  run_id STRING, connection_id STRING, source_table_id STRING, rule_id STRING,
  record_json STRING, failure_reason STRING, quarantined_ts TIMESTAMP
) USING DELTA
""")

# Idempotent upgrade: add newer columns to control tables created by an older run.
def _ensure_columns(table_key, cols):
    plain = ctrl(table_key).replace("`", "")
    existing = {
        field.name
        for field in spark.table(plain).schema.fields
    }

    for name, typ in cols:
        if name not in existing:
            spark.sql(
                f"ALTER TABLE {ctrl(table_key)} "
                f"ADD COLUMNS "
                f"({quote_databricks(name)} {typ})"
            )
            print(f"Added column {name} to {table_key}")

_ensure_columns(
    "source_table_control",
    [
        ("source_table_id", "STRING"),
        ("delete_policy", "STRING"),
        ("connection_id", "STRING"),
        ("source_identity_version", "INT"),
        ("legacy_source_table_id", "STRING"),
        # Bronze-to-Silver ETL fields (owned by the ETL pipeline). Source-ingest
        # watermarks and ETL watermarks are kept strictly separate.
        ("silver_catalog", "STRING"),
        ("silver_schema", "STRING"),
        ("silver_table", "STRING"),
        ("etl_is_active", "BOOLEAN"),
        ("etl_load_strategy", "STRING"),
        ("etl_watermark_column", "STRING"),
        ("last_etl_watermark_value", "STRING"),
        ("last_successful_etl_run_id", "STRING"),
        ("last_successful_etl_run_ts", "TIMESTAMP"),
        ("etl_current_status", "STRING"),
        ("etl_error_message", "STRING"),
    ]
)

# Source-qualified identity is propagated through every operational table so no
# join or update ever relies on source_schema + source_table alone.
_SOURCE_ID_ONLY = [("source_table_id", "STRING"), ("source_system", "STRING")]
_SOURCE_ID_FULL = [
    ("source_table_id", "STRING"), ("source_system", "STRING"),
    ("source_server", "STRING"), ("source_database", "STRING"),
]
_ensure_columns("source_inventory", _SOURCE_ID_FULL + [
    ("is_identity", "BOOLEAN"), ("is_computed", "BOOLEAN"),
    ("is_hidden", "BOOLEAN"), ("is_rowversion", "BOOLEAN"),
    ("source_type_schema", "STRING"),
])
_ensure_columns("normalized_source_inventory", _SOURCE_ID_FULL + [
    ("is_identity", "BOOLEAN"), ("is_computed", "BOOLEAN"),
    ("is_hidden", "BOOLEAN"), ("is_rowversion", "BOOLEAN"),
    ("source_type_schema", "STRING"),
])
_ensure_columns("resolved_column_mappings", _SOURCE_ID_FULL + [
    ("is_identity", "BOOLEAN"), ("is_computed", "BOOLEAN"),
    ("is_hidden", "BOOLEAN"), ("is_rowversion", "BOOLEAN"),
    ("source_type_schema", "STRING"),
    # Canonical, source-neutral column-policy outcome decided by the adapter.
    ("include_column", "BOOLEAN"), ("is_writable", "BOOLEAN"),
    ("requires_review", "BOOLEAN"), ("policy_code", "STRING"),
])
_ensure_columns("mapping_validation_results", _SOURCE_ID_FULL)
_ensure_columns("table_load_decisions", _SOURCE_ID_FULL)
_ensure_columns("review_queue", _SOURCE_ID_FULL)
_ensure_columns("table_run_log", _SOURCE_ID_FULL)
_ensure_columns("reconciliation_results", _SOURCE_ID_ONLY)
_ensure_columns(
    "delta_sync_queue",
    [
        ("source_table_id", "STRING"),
        ("source_system", "STRING"),
        ("source_server", "STRING"),
        ("source_database", "STRING"),
        ("delete_policy", "STRING"),
        ("upper_watermark_value", "STRING"),
    ]
)

# connection_id is propagated idempotently onto every operational table so the
# INGEST pipeline can scope work per connection without recomputing identity.
_CONNECTION_ID = [("connection_id", "STRING")]
for _t in ("source_inventory", "normalized_source_inventory",
           "resolved_column_mappings", "mapping_validation_results",
           "table_load_decisions", "review_queue", "table_run_log",
           "delta_sync_queue", "reconciliation_results", "dq_rule",
           "dq_result", "dq_quarantine"):
    _ensure_columns(_t, _CONNECTION_ID)

# Retry / failure-classification lineage on the shared audit table (used by both
# the INGEST and ETL pipelines and by the retry selector).
_ensure_columns("table_run_log", [
    ("attempt_number", "INT"), ("failure_stage", "STRING"),
    ("error_category", "STRING"), ("retry_eligible", "BOOLEAN"),
    ("retry_status", "STRING"), ("parent_run_id", "STRING"),
    ("lower_watermark", "STRING"), ("upper_watermark", "STRING"),
    ("extracted_row_count", "BIGINT"), ("staged_row_count", "BIGINT"),
    ("applied_row_count", "BIGINT"), ("rejected_row_count", "BIGINT"),
])

# Per-work-unit reconciliation columns so source-to-Bronze delta is reconciled
# against the exact frozen interval BEFORE the checkpoint is committed.
_ensure_columns("delta_sync_queue", [
    ("extracted_row_count", "BIGINT"), ("staged_row_count", "BIGINT"),
    ("applied_row_count", "BIGINT"), ("rejected_row_count", "BIGINT"),
    ("duplicate_key_count", "BIGINT"), ("reconciliation_status", "STRING"),
    ("data_applied_ts", "TIMESTAMP"), ("reconciled_ts", "TIMESTAMP"),
    ("checkpoint_committed_ts", "TIMESTAMP"), ("finalized_ts", "TIMESTAMP"),
])

# Source assessment selection lifecycle columns (additive upgrade)
_ensure_columns("source_assessment", [
    ("selection_status", "STRING"),
    ("selected_ts", "TIMESTAMP"),
    ("selected_by", "STRING"),
    ("onboarding_run_id", "STRING"),
    ("onboarding_attempt_id", "STRING"),
    ("onboarding_started_ts", "TIMESTAMP"),
    ("registration_completed_ts", "TIMESTAMP"),
    ("onboarding_completed_ts", "TIMESTAMP"),
    ("onboarding_failed_stage", "STRING"),
    ("onboarding_error_message", "STRING"),
])

# Accelerator target configuration columns (additive upgrade)
_ensure_columns("accelerator_target_config", [
    ("config_id", "STRING"),
    ("source_system", "STRING"),
    ("connection_id", "STRING"),
    ("target_catalog", "STRING"),
    ("target_schema_mode", "STRING"),
    ("target_schema", "STRING"),
    ("is_default", "BOOLEAN"),
    ("is_active", "BOOLEAN"),
    ("created_ts", "TIMESTAMP"),
    ("updated_ts", "TIMESTAMP"),
])

# Identity upgrades are never performed during ordinary initialization. Legacy
# rows remain untouched until the dry-run-first deployment migration is run.
_legacy_identity_count = spark.sql(f"""
  SELECT count(*) AS c
  FROM {ctrl('source_table_control')}
  WHERE source_identity_version IS NULL OR source_identity_version <> 2
""").collect()[0]["c"]
if _legacy_identity_count:
  print(f"Found {_legacy_identity_count} legacy source identity row(s); run "
      "deployment/NB_MigrateSourceTableIdentityV2 manually.")

# Validate ownership metadata after all additive schema upgrades. Counts and
# identifiers are safe to report; secret_scope values are never selected.
_control_validation_errors = []
_validation_checks = (
  ("BLANK_CONNECTION_ID", f"""
    SELECT count(*) AS c FROM {ctrl('source_connection')}
    WHERE connection_id IS NULL OR trim(connection_id) = ''
  """),
  ("DUPLICATE_CONNECTION_ID", f"""
    SELECT count(*) AS c FROM (
      SELECT connection_id FROM {ctrl('source_connection')}
      GROUP BY connection_id HAVING count(*) > 1
    )
  """),
  ("BLANK_CONNECTION_SOURCE_SYSTEM", f"""
    SELECT count(*) AS c FROM {ctrl('source_connection')}
    WHERE source_system IS NULL OR trim(source_system) = ''
  """),
  ("BLANK_SECRET_SCOPE", f"""
    SELECT count(*) AS c FROM {ctrl('source_connection')}
    WHERE secret_scope IS NULL OR trim(secret_scope) = ''
  """),
  ("ACTIVE_CONNECTION_INVALID_STATUS", f"""
    -- Active connection has a missing or unsupported connection_status.
    -- Expected REGISTERED, VALID, or FAILED.
    SELECT count(*) AS c FROM {ctrl('source_connection')}
    WHERE coalesce(is_active, false) = true
      AND upper(trim(coalesce(connection_status, ''))) NOT IN (
          'REGISTERED',
          'VALID',
          'FAILED'
      )
  """),
  ("BLANK_TABLE_CONNECTION_ID", f"""
    SELECT count(*) AS c FROM {ctrl('source_table_control')}
    WHERE connection_id IS NULL OR trim(connection_id) = ''
  """),
  ("BLANK_SOURCE_TABLE_ID", f"""
    SELECT count(*) AS c FROM {ctrl('source_table_control')}
    WHERE source_table_id IS NULL OR trim(source_table_id) = ''
  """),
  ("DUPLICATE_TABLE_OWNERSHIP", f"""
    SELECT count(*) AS c FROM (
      SELECT connection_id, source_table_id
      FROM {ctrl('source_table_control')}
      GROUP BY connection_id, source_table_id HAVING count(*) > 1
    )
  """),
  ("ORPHAN_TABLE_CONNECTION", f"""
    SELECT count(*) AS c
    FROM {ctrl('source_table_control')} c
    LEFT ANTI JOIN {ctrl('source_connection')} sc
      ON c.connection_id = sc.connection_id
  """),
  ("TABLE_CONNECTION_SOURCE_MISMATCH", f"""
    SELECT count(*) AS c
    FROM {ctrl('source_table_control')} c
    JOIN {ctrl('source_connection')} sc
      ON c.connection_id = sc.connection_id
        WHERE c.source_system IS NULL OR trim(c.source_system) = ''
          OR lower(trim(c.source_system)) <> lower(trim(sc.source_system))
  """),
    ("TABLE_CONNECTION_ENDPOINT_MISMATCH", f"""
        SELECT count(*) AS c
        FROM {ctrl('source_table_control')} c
        JOIN {ctrl('source_connection')} sc
          ON c.connection_id = sc.connection_id
        WHERE (c.source_server IS NOT NULL AND trim(c.source_server) <> ''
               AND sc.source_server IS NOT NULL AND trim(sc.source_server) <> ''
               AND lower(trim(c.source_server)) <>
                   lower(trim(sc.source_server)))
           OR (sc.source_database IS NOT NULL AND trim(sc.source_database) <> ''
               AND (c.source_database IS NULL OR trim(c.source_database) = ''
                    OR lower(trim(c.source_database)) <>
                        lower(trim(sc.source_database))))
    """),
    ("MISSING_SQLSERVER_SOURCE_DATABASE", f"""
        SELECT count(*) AS c
        FROM {ctrl('source_table_control')} c
        JOIN {ctrl('source_connection')} sc
          ON c.connection_id = sc.connection_id
        WHERE lower(trim(coalesce(c.source_system, sc.source_system, ''))) = 'sqlserver'
          AND (c.source_database IS NULL OR trim(c.source_database) = '')
    """),
  ("ACTIVE_TABLE_INVALID_CONNECTION", f"""
    SELECT count(*) AS c
    FROM {ctrl('source_table_control')} c
    JOIN {ctrl('source_connection')} sc
      ON c.connection_id = sc.connection_id
    WHERE c.is_active = true AND (
          coalesce(sc.is_active, false) <> true
          OR sc.connection_status IS NULL OR sc.connection_status <> 'VALID'
      OR sc.secret_scope IS NULL OR trim(sc.secret_scope) = '')
  """),
  ("ACTIVE_TABLE_INCOMPLETE_TARGET", f"""
    SELECT count(*) AS c FROM {ctrl('source_table_control')}
    WHERE is_active = true AND (
      target_catalog IS NULL OR trim(target_catalog) = ''
      OR target_schema IS NULL OR trim(target_schema) = ''
      OR target_table IS NULL OR trim(target_table) = '')
  """),
    ("DUPLICATE_SOURCE_ASSESSMENT_KEY", f"""
        SELECT count(*) AS c FROM (
          SELECT connection_id, assessment_id, source_database, source_schema,
                 object_type, object_name
          FROM {ctrl('source_assessment')}
          GROUP BY connection_id, assessment_id, source_database, source_schema,
                   object_type, object_name
          HAVING count(*) > 1
        )
    """),
    ("DUPLICATE_SOURCE_INVENTORY_KEY", f"""
        SELECT count(*) AS c FROM (
          SELECT run_id, connection_id, source_table_id, column_name
          FROM {ctrl('source_inventory')}
          GROUP BY run_id, connection_id, source_table_id, column_name
          HAVING count(*) > 1
        )
    """),
    ("DUPLICATE_NORMALIZED_INVENTORY_KEY", f"""
        SELECT count(*) AS c FROM (
          SELECT run_id, connection_id, source_table_id, column_name
          FROM {ctrl('normalized_source_inventory')}
          GROUP BY run_id, connection_id, source_table_id, column_name
          HAVING count(*) > 1
        )
    """),
    ("DUPLICATE_RESOLVED_MAPPING_KEY", f"""
        SELECT count(*) AS c FROM (
          SELECT run_id, connection_id, source_table_id, column_name
          FROM {ctrl('resolved_column_mappings')}
          GROUP BY run_id, connection_id, source_table_id, column_name
          HAVING count(*) > 1
        )
    """),
    ("DUPLICATE_MAPPING_VALIDATION_KEY", f"""
        SELECT count(*) AS c FROM (
          SELECT run_id, connection_id, source_table_id, column_name, rule
          FROM {ctrl('mapping_validation_results')}
          GROUP BY run_id, connection_id, source_table_id, column_name, rule
          HAVING count(*) > 1
        )
    """),
    ("DUPLICATE_TABLE_DECISION_KEY", f"""
        SELECT count(*) AS c FROM (
          SELECT run_id, connection_id, source_table_id
          FROM {ctrl('table_load_decisions')}
          GROUP BY run_id, connection_id, source_table_id
          HAVING count(*) > 1
        )
    """),
    ("DUPLICATE_REVIEW_QUEUE_KEY", f"""
        SELECT count(*) AS c FROM (
          SELECT run_id, connection_id, source_table_id
          FROM {ctrl('review_queue')}
          GROUP BY run_id, connection_id, source_table_id
          HAVING count(*) > 1
        )
    """),
    ("DUPLICATE_TABLE_RUN_KEY", f"""
        SELECT count(*) AS c FROM (
          SELECT run_id, connection_id, source_table_id, operation,
                 coalesce(attempt_number, 1) AS attempt_number
          FROM {ctrl('table_run_log')}
          GROUP BY run_id, connection_id, source_table_id, operation,
                   coalesce(attempt_number, 1)
          HAVING count(*) > 1
        )
    """),
    ("DUPLICATE_DELTA_QUEUE_KEY", f"""
        SELECT count(*) AS c FROM (
          SELECT run_id, connection_id, source_table_id
          FROM {ctrl('delta_sync_queue')}
          GROUP BY run_id, connection_id, source_table_id
          HAVING count(*) > 1
        )
    """),
    ("DUPLICATE_RECONCILIATION_KEY", f"""
        SELECT count(*) AS c FROM (
          SELECT run_id, connection_id, source_table_id, check_type
          FROM {ctrl('reconciliation_results')}
          GROUP BY run_id, connection_id, source_table_id, check_type
          HAVING count(*) > 1
        )
    """),
    ("DUPLICATE_DQ_RULE_KEY", f"""
        SELECT count(*) AS c FROM (
          SELECT connection_id, source_table_id, rule_id
          FROM {ctrl('dq_rule')}
          GROUP BY connection_id, source_table_id, rule_id
          HAVING count(*) > 1
        )
    """),
    ("DUPLICATE_DQ_RESULT_KEY", f"""
        SELECT count(*) AS c FROM (
          SELECT run_id, connection_id, source_table_id,
                 coalesce(rule_id, rule_type) AS rule_identity
          FROM {ctrl('dq_result')}
          GROUP BY run_id, connection_id, source_table_id,
                   coalesce(rule_id, rule_type)
          HAVING count(*) > 1
        )
    """),
    ("BLANK_TARGET_CONFIG_ID", f"""
        SELECT count(*) AS c FROM {ctrl('accelerator_target_config')}
        WHERE config_id IS NULL OR trim(config_id) = ''
    """),
    ("DUPLICATE_TARGET_CONFIG_ID", f"""
        SELECT count(*) AS c FROM (
          SELECT config_id
          FROM {ctrl('accelerator_target_config')}
          GROUP BY config_id
          HAVING count(*) > 1
        )
    """),
    ("INVALID_TARGET_SCHEMA_MODE", f"""
        SELECT count(*) AS c FROM {ctrl('accelerator_target_config')}
        WHERE is_active = true AND is_default = true
          AND (target_schema_mode IS NULL
               OR upper(trim(target_schema_mode)) NOT IN (
                  'SOURCE_SCHEMA', 'PREFIX_WITH_DATABASE', 'EXPLICIT'))
    """),
    ("BLANK_ACTIVE_DEFAULT_TARGET_CATALOG", f"""
        SELECT count(*) AS c FROM {ctrl('accelerator_target_config')}
        WHERE is_active = true AND is_default = true
          AND (target_catalog IS NULL OR trim(target_catalog) = '')
    """),
    ("EXPLICIT_MODE_MISSING_TARGET_SCHEMA", f"""
        SELECT count(*) AS c FROM {ctrl('accelerator_target_config')}
        WHERE is_active = true AND is_default = true
          AND upper(trim(target_schema_mode)) = 'EXPLICIT'
          AND (target_schema IS NULL OR trim(target_schema) = '')
    """),
    ("DUPLICATE_CONNECTION_ACTIVE_DEFAULT_TARGET_CONFIG", f"""
        SELECT count(*) AS c FROM (
          SELECT connection_id
          FROM {ctrl('accelerator_target_config')}
          WHERE is_active = true AND is_default = true
            AND connection_id IS NOT NULL AND trim(connection_id) <> ''
          GROUP BY connection_id
          HAVING count(*) > 1
        )
    """),
    ("DUPLICATE_SOURCE_ACTIVE_DEFAULT_TARGET_CONFIG", f"""
        SELECT count(*) AS c FROM (
          SELECT lower(trim(source_system)) AS src_sys
          FROM {ctrl('accelerator_target_config')}
          WHERE is_active = true AND is_default = true
            AND (connection_id IS NULL OR trim(connection_id) = '')
            AND source_system IS NOT NULL AND trim(source_system) <> ''
          GROUP BY lower(trim(source_system))
          HAVING count(*) > 1
        )
    """),
    ("DUPLICATE_GLOBAL_ACTIVE_DEFAULT_TARGET_CONFIG", f"""
        SELECT count(*) AS c FROM (
          SELECT 1 AS g
          FROM {ctrl('accelerator_target_config')}
          WHERE is_active = true AND is_default = true
            AND (connection_id IS NULL OR trim(connection_id) = '')
            AND (source_system IS NULL OR trim(source_system) = '')
          HAVING count(*) > 1
        )
    """),
    ("ORPHAN_TARGET_CONFIG_CONNECTION", f"""
        SELECT count(*) AS c
        FROM {ctrl('accelerator_target_config')} tc
        LEFT ANTI JOIN {ctrl('source_connection')} sc
          ON tc.connection_id = sc.connection_id
        WHERE tc.is_active = true AND tc.is_default = true
          AND tc.connection_id IS NOT NULL AND trim(tc.connection_id) <> ''
    """),
    ("TARGET_CONFIG_CONNECTION_SOURCE_MISMATCH", f"""
        SELECT count(*) AS c
        FROM {ctrl('accelerator_target_config')} tc
        JOIN {ctrl('source_connection')} sc
          ON tc.connection_id = sc.connection_id
        WHERE tc.is_active = true AND tc.is_default = true
          AND tc.source_system IS NOT NULL AND trim(tc.source_system) <> ''
          AND {canonical_source_system_sql('tc.source_system')} <> {canonical_source_system_sql('sc.source_system')}
    """),
    ("INVALID_ASSESSMENT_SELECTION_STATUS", f"""
        SELECT count(*) AS c
        FROM {ctrl('source_assessment')}
        WHERE selection_status IS NOT NULL AND trim(selection_status) <> ''
          AND upper(trim(selection_status)) NOT IN (
              'NOT_SELECTED', 'SELECTED', 'ONBOARDING', 'REGISTERED', 'ONBOARDED', 'FAILED',
              'REVIEW_REQUIRED', 'BLOCKED')
    """),
    ("ONBOARDING_WITHOUT_RUN_ID", f"""
        SELECT count(*) AS c
        FROM {ctrl('source_assessment')}
        WHERE upper(trim(coalesce(selection_status, ''))) = 'ONBOARDING'
          AND (onboarding_run_id IS NULL OR trim(onboarding_run_id) = '')
    """),
    ("ONBOARDING_WITHOUT_ATTEMPT_ID", f"""
        SELECT count(*) AS c
        FROM {ctrl('source_assessment')}
        WHERE upper(trim(coalesce(selection_status, ''))) = 'ONBOARDING'
          AND (onboarding_attempt_id IS NULL OR trim(onboarding_attempt_id) = '')
    """),
    ("REGISTERED_WITHOUT_RUN_ID", f"""
        SELECT count(*) AS c
        FROM {ctrl('source_assessment')}
        WHERE upper(trim(coalesce(selection_status, ''))) = 'REGISTERED'
          AND (onboarding_run_id IS NULL OR trim(onboarding_run_id) = '')
    """),
    ("REGISTERED_WITHOUT_ATTEMPT_ID", f"""
        SELECT count(*) AS c
        FROM {ctrl('source_assessment')}
        WHERE upper(trim(coalesce(selection_status, ''))) = 'REGISTERED'
          AND (onboarding_attempt_id IS NULL OR trim(onboarding_attempt_id) = '')
    """),
    ("INVALID_ONBOARDING_FAILED_STAGE", f"""
        SELECT count(*) AS c
        FROM {ctrl('source_assessment')}
        WHERE onboarding_failed_stage IS NOT NULL AND trim(onboarding_failed_stage) <> ''
          AND upper(trim(onboarding_failed_stage)) NOT IN (
              'REGISTRATION', 'INVENTORY', 'TYPE_NORMALIZATION',
              'MAPPING_GENERATION', 'MAPPING_VALIDATION', 'TABLE_DECISION',
              'TARGET_PROVISIONING', 'FINALIZATION')
    """),
    ("DUPLICATE_SQL_OBJECT_ARTIFACT_OWNER", f"""
        SELECT count(*) AS c FROM (
          SELECT connection_id, source_database, source_schema, object_type, object_name
          FROM {ctrl('sql_object_artifact_manifest')}
          GROUP BY connection_id, source_database, source_schema, object_type, object_name
          HAVING count(*) > 1
        )
    """),
)
for _code, _query in _validation_checks:
  _count = spark.sql(_query).collect()[0]["c"]
  if _count:
    _control_validation_errors.append(
      {"code": _code, "count": int(_count)})

_connection_metadata_rows = spark.sql(f"""
  SELECT connection_id, source_system, source_server, source_database,
       secret_scope, trust_server_certificate
  FROM {ctrl('source_connection')}
  WHERE source_system IS NOT NULL AND trim(source_system) <> ''
""").collect()
_unsupported_connections = 0
_invalid_connection_metadata = 0
for _connection_row in _connection_metadata_rows:
  try:
    _connection_system = require_source_system(
      _connection_row["source_system"], "registered connection")
    _connection_adapter = get_source_adapter(_connection_system)
    _connection_adapter.validate_connection_metadata(
      _connection_row.asDict())
  except ValueError as _connection_error:
    _safe_connection_error = failcls.sanitize_message(_connection_error)
    if "Unsupported source_system" in _safe_connection_error:
      _unsupported_connections += 1
    else:
      _invalid_connection_metadata += 1
if _unsupported_connections:
  _control_validation_errors.append({
    "code": "UNSUPPORTED_CONNECTION_SOURCE_SYSTEM",
    "count": _unsupported_connections,
  })
if _invalid_connection_metadata:
  _control_validation_errors.append({
    "code": "INVALID_CONNECTION_METADATA",
    "count": _invalid_connection_metadata,
  })

# Legacy v1 identities are reported as a business status (MIGRATION_REQUIRED)
# rather than structural corruption, so additive installation completes safely.
if _legacy_identity_count:
  print(f"Notice: {_legacy_identity_count} legacy source identity row(s) require migration; "
        "NB00 will report business_status='MIGRATION_REQUIRED'.")

_v2_rows = spark.sql(f"""
  SELECT c.connection_id, c.source_table_id,
       coalesce(sc.source_system, c.source_system) AS source_system,
       coalesce(sc.source_server, c.source_server) AS source_server,
       c.source_database AS source_database,
       c.source_schema, c.source_table
  FROM {ctrl('source_table_control')} c
  JOIN {ctrl('source_connection')} sc
    ON c.connection_id = sc.connection_id
  WHERE c.source_identity_version = {SOURCE_IDENTITY_VERSION}
""").collect()
_inconsistent_v2 = 0
for _row in _v2_rows:
  try:
    _expected_id = compute_source_table_id(
      _row["connection_id"], _row["source_system"],
      _row["source_server"], _row["source_database"],
      _row["source_schema"], _row["source_table"])
    if _row["source_table_id"] != _expected_id:
      _inconsistent_v2 += 1
  except Exception:
    _inconsistent_v2 += 1
if _inconsistent_v2:
  _control_validation_errors.append({
    "code": "INCONSISTENT_SOURCE_IDENTITY_V2",
    "count": _inconsistent_v2,
  })

if _control_validation_errors:
  print("Control-table structural validation failed:",
      json.dumps(_control_validation_errors))
  raise RuntimeError(
    "Control-table ownership validation failed; correct registry metadata "
    "or repair structural corruption")

print("All control & audit tables created.")

# COMMAND ----------

# MAGIC %md ### 3. Optionally seed POC rows
# MAGIC Oracle object names are stored UPPER CASE in the data dictionary, so we
# MAGIC register them upper case to match ALL_TAB_COLUMNS lookups in NB01. SQL
# MAGIC Server names preserve their stored casing. Every seed row carries the full
# MAGIC source identity and a deterministic source_table_id so two sources that
# MAGIC share a schema.table never collide.

# COMMAND ----------

dbutils.widgets.dropdown("seed_sqlserver_examples", "false", ["true", "false"])
dbutils.widgets.text("seed_oracle_connection_id", "")
dbutils.widgets.text("seed_sqlserver_connection_id", "")
seed_sqlserver = dbutils.widgets.get("seed_sqlserver_examples") == "true"

if seed_poc:
    from pyspark.sql import Row
    now = now_utc()

    def _seed_row(connection_id, source_system, source_server, source_database,
                  source_schema, source_table, target_schema, target_table):
        sid = compute_source_table_id(
            connection_id, source_system, source_server, source_database,
            source_schema, source_table)
        return Row(
            source_table_id=sid, connection_id=connection_id,
            source_identity_version=SOURCE_IDENTITY_VERSION,
            legacy_source_table_id=None, source_system=source_system,
            source_server=source_server, source_database=source_database,
            source_schema=source_schema, source_table=source_table,
            target_catalog=CATALOG, target_schema=target_schema,
            target_table=target_table,
            is_active=True, mapping_status=None, table_decision=None,
            load_strategy=None, delete_policy=None, primary_key_columns=None,
            watermark_column=None, watermark_data_type=None,
            last_watermark_value=None, initial_load_completed=False,
            last_successful_run_id=None, last_successful_run_ts=None,
            current_status="REGISTERED", error_message=None,
            created_ts=now, updated_ts=now)

    oracle_connection_id = require_connection_id(
        dbutils.widgets.get("seed_oracle_connection_id"), "POC Oracle seed")
    oracle_connection = require_valid_connection(oracle_connection_id, "oracle")
    oracle_data = oracle_connection.asDict()
    seed = [
        _seed_row(
            oracle_connection_id, "oracle", oracle_data.get("source_server"),
            oracle_data.get("source_database"), "HR", "EMPLOYEES",
            "hr", "employees"),
        _seed_row(
            oracle_connection_id, "oracle", oracle_data.get("source_server"),
            oracle_data.get("source_database"), "SALES", "CUSTOMERS",
            "sales", "customers"),
    ]
    # Optional, clearly-labeled SQL Server example registrations. These require
    # a populated source_database on the selected registered connection.
    if seed_sqlserver:
        sqlserver_connection_id = require_connection_id(
            dbutils.widgets.get("seed_sqlserver_connection_id"),
            "POC SQL Server seed")
        sqlserver_connection = require_valid_connection(
            sqlserver_connection_id, "sqlserver")
        sqlserver_data = sqlserver_connection.asDict()
        seed += [
            _seed_row(
                sqlserver_connection_id, "sqlserver",
                sqlserver_data.get("source_server"),
                sqlserver_data.get("source_database"), "dbo", "Employees",
                "adventureworks_dbo", "employees"),
            _seed_row(
                sqlserver_connection_id, "sqlserver",
                sqlserver_data.get("source_server"),
                sqlserver_data.get("source_database"), "Sales", "Customer",
                "adventureworks_sales", "customer"),
        ]

    df_new = spark.createDataFrame(seed)
    df_new.createOrReplaceTempView("seed_rows")
    # MERGE on the source-qualified id so re-runs don't duplicate registrations
    # and different sources with the same schema.table stay distinct.
    spark.sql(f"""
        MERGE INTO {ctrl('source_table_control')} t
        USING seed_rows s
        ON t.connection_id = s.connection_id
       AND t.source_table_id = s.source_table_id
        WHEN NOT MATCHED THEN INSERT (
          source_table_id, connection_id, source_identity_version,
          legacy_source_table_id, source_system, source_server,
          source_database, source_schema, source_table, target_catalog,
          target_schema, target_table, is_active, mapping_status,
          table_decision, load_strategy, delete_policy, primary_key_columns,
          watermark_column, watermark_data_type, last_watermark_value,
          initial_load_completed, last_successful_run_id,
          last_successful_run_ts, current_status, error_message,
          created_ts, updated_ts
        ) VALUES (
          s.source_table_id, s.connection_id, s.source_identity_version,
          s.legacy_source_table_id, s.source_system, s.source_server,
          s.source_database, s.source_schema, s.source_table, s.target_catalog,
          s.target_schema, s.target_table, s.is_active, s.mapping_status,
          s.table_decision, s.load_strategy, s.delete_policy,
          s.primary_key_columns, s.watermark_column, s.watermark_data_type,
          s.last_watermark_value, s.initial_load_completed,
          s.last_successful_run_id, s.last_successful_run_ts,
          s.current_status, s.error_message, s.created_ts, s.updated_ts
        )
    """)
    print("Seeded POC control rows (composite ownership key, no duplicates).")
else:
    print("Skipped POC seeding.")

# COMMAND ----------

_noncanonical_source_systems_count = spark.sql(f"""
    SELECT count(*) AS c
    FROM (
        SELECT source_system FROM {ctrl('source_connection')}
        WHERE source_system IS NOT NULL AND trim(source_system) <> ''
          AND lower(trim(source_system)) NOT IN ('oracle', 'sqlserver')
        UNION ALL
        SELECT source_system FROM {ctrl('source_assessment')}
        WHERE source_system IS NOT NULL AND trim(source_system) <> ''
          AND lower(trim(source_system)) NOT IN ('oracle', 'sqlserver')
        UNION ALL
        SELECT source_system FROM {ctrl('source_table_control')}
        WHERE source_system IS NOT NULL AND trim(source_system) <> ''
          AND lower(trim(source_system)) NOT IN ('oracle', 'sqlserver')
    )
""").collect()[0]["c"]

if _noncanonical_source_systems_count:
    print(f"Notice: {_noncanonical_source_systems_count} row(s) use non-canonical source_system aliases; "
          "NB00 reports business_status='SOURCE_SYSTEM_CANONICALIZATION_REQUIRED'.")
    print("Dry-run administrative repair queries:")
    print(f"  UPDATE {ctrl('source_connection')} SET source_system = 'sqlserver' WHERE lower(trim(source_system)) IN ('sql_server', 'sql-server', 'mssql', 'sql server', 'microsoft sql server', 'microsoft_sql_server');")
    print(f"  UPDATE {ctrl('source_assessment')} SET source_system = 'sqlserver' WHERE lower(trim(source_system)) IN ('sql_server', 'sql-server', 'mssql', 'sql server', 'microsoft sql server', 'microsoft_sql_server');")
    print(f"  UPDATE {ctrl('source_table_control')} SET source_system = 'sqlserver' WHERE lower(trim(source_system)) IN ('sql_server', 'sql-server', 'mssql', 'sql server', 'microsoft sql server', 'microsoft_sql_server');")

_downstream_missing_db_checks = (
    ("MISSING_SQLSERVER_NORMALIZED_SOURCE_DATABASE", "normalized_source_inventory"),
    ("MISSING_SQLSERVER_RESOLVED_MAPPING_SOURCE_DATABASE", "resolved_column_mappings"),
    ("MISSING_SQLSERVER_MAPPING_VALIDATION_SOURCE_DATABASE", "mapping_validation_results"),
    ("MISSING_SQLSERVER_TABLE_DECISION_SOURCE_DATABASE", "table_load_decisions"),
    ("MISSING_SQLSERVER_REVIEW_QUEUE_SOURCE_DATABASE", "review_queue"),
)
_missing_sqlserver_database_downstream_count = 0
for _code, _tbl in _downstream_missing_db_checks:
    _cnt = spark.sql(f"""
        SELECT count(*) AS c
        FROM {ctrl(_tbl)}
        WHERE lower(trim(coalesce(source_system, ''))) = 'sqlserver'
          AND (source_database IS NULL OR trim(source_database) = '')
    """).collect()[0]["c"]
    if _cnt:
        _missing_sqlserver_database_downstream_count += int(_cnt)
        print(f"Notice: {_cnt} row(s) in {_tbl} missing SQL Server source_database [{_code}].")

business_status = "MIGRATION_REQUIRED" if _legacy_identity_count else "READY"
if business_status == "READY" and _noncanonical_source_systems_count:
    business_status = "SOURCE_SYSTEM_CANONICALIZATION_REQUIRED"
if business_status == "READY" and _missing_sqlserver_database_downstream_count:
    business_status = "REPAIR_REQUIRED"

set_task_value("status", "SUCCEEDED")
set_task_value("business_status", business_status)
set_task_value("legacy_identity_count", int(_legacy_identity_count or 0))
set_task_value("noncanonical_source_systems_count", int(_noncanonical_source_systems_count or 0))
set_task_value("missing_sqlserver_database_downstream_count", int(_missing_sqlserver_database_downstream_count or 0))
set_task_value("run_id", run_id)

spark.sql(f"""
INSERT INTO {ctrl('job_run_log')}
VALUES ({escape_string_literal(run_id)}, 'NB00_ControlTableInit', 'SUCCEEDED',
        current_timestamp(), current_timestamp(), 'control tables ready')
""")
print(f"NB00 complete: status=SUCCEEDED, business_status={business_status}, "
      f"legacy_identity_count={int(_legacy_identity_count or 0)}, "
      f"noncanonical_source_systems_count={int(_noncanonical_source_systems_count or 0)}, "
      f"missing_sqlserver_database_downstream_count={int(_missing_sqlserver_database_downstream_count or 0)}")
dbutils.notebook.exit(json.dumps({
    "status": "SUCCEEDED",
    "business_status": business_status,
    "legacy_identity_count": int(_legacy_identity_count or 0),
    "noncanonical_source_systems_count": int(_noncanonical_source_systems_count or 0),
    "missing_sqlserver_database_downstream_count": int(_missing_sqlserver_database_downstream_count or 0),
    "fatal_validation_error_count": 0,
    "run_id": run_id,
}))