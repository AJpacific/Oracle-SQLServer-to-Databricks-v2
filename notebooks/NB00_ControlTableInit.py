# Databricks notebook source
# MAGIC %md
# MAGIC # NB00_ControlTableInit
# MAGIC Creates the control schema and every control/audit Delta table inside the
# MAGIC existing Unity Catalog catalog. Idempotent: safe to re-run. Never drops tables.

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

display(spark.sql("SHOW GRANTS ON SCHEMA da_accelerators.control"))

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
spark.sql(ddl.build_create_schema(CATALOG, CONTROL_SCHEMA, "Oracle and SQL Server accelerator control & audit"))
print("Control schema ready.")

# COMMAND ----------

# MAGIC %md ### 2. Control & audit tables

# COMMAND ----------

def ctrl(t):
    return f"{quote_databricks(CATALOG)}.{quote_databricks(CONTROL_SCHEMA)}.{quote_databricks(t)}"

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {ctrl('source_table_control')} (
  source_table_id          STRING,
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
  run_id STRING, source_table_id STRING, source_system STRING,
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
  run_id STRING, source_table_id STRING, source_system STRING,
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
  run_id STRING, source_table_id STRING, source_system STRING,
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
  run_id STRING, source_table_id STRING, source_system STRING,
  source_schema STRING, source_table STRING, column_name STRING,
  severity STRING, rule STRING, message STRING, captured_ts TIMESTAMP
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {ctrl('table_load_decisions')} (
  run_id STRING, source_table_id STRING, source_system STRING,
  source_schema STRING, source_table STRING,
  decision STRING, reason STRING, blocked_columns INT, review_columns INT,
  total_columns INT, captured_ts TIMESTAMP
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {ctrl('review_queue')} (
  source_table_id STRING, source_system STRING,
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
  run_id STRING, source_table_id STRING, source_system STRING,
  source_server STRING, source_database STRING,
  source_schema STRING, source_table STRING, operation STRING,
  target_full_name STRING, source_row_count BIGINT, target_row_count BIGINT,
  status STRING, error_message STRING, started_ts TIMESTAMP, ended_ts TIMESTAMP
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {ctrl('delta_sync_queue')} (
  run_id STRING, source_table_id STRING, source_system STRING,
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
  run_id STRING, source_table_id STRING, source_system STRING,
  source_schema STRING, source_table STRING, check_type STRING,
  source_value STRING, target_value STRING, status STRING, message STRING,
  captured_ts TIMESTAMP
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
_ensure_columns("normalized_source_inventory", _SOURCE_ID_ONLY + [
    ("is_identity", "BOOLEAN"), ("is_computed", "BOOLEAN"),
    ("is_hidden", "BOOLEAN"), ("is_rowversion", "BOOLEAN"),
    ("source_type_schema", "STRING"),
])
_ensure_columns("resolved_column_mappings", _SOURCE_ID_ONLY + [
    ("is_identity", "BOOLEAN"), ("is_computed", "BOOLEAN"),
    ("is_hidden", "BOOLEAN"), ("is_rowversion", "BOOLEAN"),
    ("source_type_schema", "STRING"),
])
_ensure_columns("mapping_validation_results", _SOURCE_ID_ONLY)
_ensure_columns("table_load_decisions", _SOURCE_ID_ONLY)
_ensure_columns("review_queue", _SOURCE_ID_ONLY)
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

# Backfill source_table_id for existing control rows that predate this upgrade
# and have the source fields needed to compute a deterministic id. Rows missing
# source_schema/source_table cannot be uniquely backfilled; they are preserved
# but stay unusable by new runs (which require a source_table_id).
_existing = spark.sql(f"""
    SELECT source_system, source_server, source_database,
           source_schema, source_table
    FROM {ctrl('source_table_control')}
    WHERE (source_table_id IS NULL OR source_table_id = '')
      AND source_schema IS NOT NULL AND source_table IS NOT NULL
""").collect()
_backfilled = 0
for _r in _existing:
    try:
        _sid = compute_source_table_id(
            _r["source_system"] or "oracle", _r["source_server"],
            _r["source_database"], _r["source_schema"], _r["source_table"])
    except Exception as _e:
        print(f"  [warn] cannot backfill id for "
              f"{_r['source_schema']}.{_r['source_table']}: {_e}")
        continue
    # Match the historical row on the full identity (null-safe) to set its id.
    spark.sql(f"""
        UPDATE {ctrl('source_table_control')}
        SET source_table_id = {escape_string_literal(_sid)},
            source_system = {escape_string_literal(_r['source_system'] or 'oracle')},
            updated_ts = current_timestamp()
        WHERE (source_table_id IS NULL OR source_table_id = '')
          AND source_schema = {escape_string_literal(_r['source_schema'])}
          AND source_table = {escape_string_literal(_r['source_table'])}
          AND ((source_server IS NULL AND {escape_string_literal(_r['source_server'])} IS NULL)
               OR source_server = {escape_string_literal(_r['source_server'])})
          AND ((source_database IS NULL AND {escape_string_literal(_r['source_database'])} IS NULL)
               OR source_database = {escape_string_literal(_r['source_database'])})
    """)
    _backfilled += 1
if _backfilled:
    print(f"Backfilled source_table_id for {_backfilled} existing control row(s).")

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
seed_sqlserver = dbutils.widgets.get("seed_sqlserver_examples") == "true"

if seed_poc:
    from pyspark.sql import Row
    now = now_utc()

    def _seed_row(source_system, source_server, source_database,
                  source_schema, source_table, target_schema, target_table):
        sid = compute_source_table_id(source_system, source_server,
                                      source_database, source_schema, source_table)
        return Row(
            source_table_id=sid, source_system=source_system,
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

    seed = [
        _seed_row("oracle", None, None, "HR", "EMPLOYEES", "hr", "employees"),
        _seed_row("oracle", None, None, "SALES", "CUSTOMERS", "sales", "customers"),
    ]
    # Optional, clearly-labeled SQL Server example registrations. These require a
    # populated source_database and the sqlserver-migration secret scope.
    if seed_sqlserver:
        seed += [
            _seed_row("sqlserver", "sql-server-name", "AdventureWorks",
                      "dbo", "Employees", "adventureworks_dbo", "employees"),
            _seed_row("sqlserver", "sql-server-name", "AdventureWorks",
                      "Sales", "Customer", "adventureworks_sales", "customer"),
        ]

    df_new = spark.createDataFrame(seed)
    df_new.createOrReplaceTempView("seed_rows")
    # MERGE on the source-qualified id so re-runs don't duplicate registrations
    # and different sources with the same schema.table stay distinct.
    spark.sql(f"""
        MERGE INTO {ctrl('source_table_control')} t
        USING seed_rows s
        ON t.source_table_id = s.source_table_id
        WHEN NOT MATCHED THEN INSERT *
    """)
    print("Seeded POC control rows (merge on source_table_id, no duplicates).")
else:
    print("Skipped POC seeding.")

# COMMAND ----------

spark.sql(f"""
INSERT INTO {ctrl('job_run_log')}
VALUES ({escape_string_literal(run_id)}, 'NB00_ControlTableInit', 'SUCCEEDED',
        current_timestamp(), current_timestamp(), 'control tables ready')
""")
print("NB00 complete.")
dbutils.notebook.exit(json.dumps({"status": "SUCCEEDED", "run_id": run_id}))