# Databricks notebook source
# MAGIC %md
# MAGIC # NB21_SQLArtifactInit
# MAGIC Authoritative initializer and additive legacy upgrader for SQL Artifact Migration
# MAGIC control and execution log tables inside Unity Catalog. Completely independent of
# MAGIC table onboarding or Full Load.
# MAGIC Idempotent: safe to re-run. Never drops, alters, or truncates existing tables.

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

import json

try:
    from src.identifiers import quote_databricks, escape_string_literal
    from src.sql_artifact_control_common import (
        SQL_ARTIFACT_CONTROL_TABLE,
        SQL_ARTIFACT_EXECUTION_LOG_TABLE,
        SQL_ARTIFACT_CONTROL_COLUMNS,
        SQL_ARTIFACT_EXECUTION_LOG_COLUMNS,
        build_create_artifact_control_ddl,
        build_create_artifact_execution_log_ddl,
        build_upgrade_artifact_control_ddl,
        build_upgrade_artifact_execution_log_ddl,
        build_upgrade_sql_object_assessment_ddl,
        build_upgrade_sql_object_artifact_manifest_ddl,
        sanitize_error,
        plan_identity_migration,
        verify_schema,
    )
except ModuleNotFoundError:
    from identifiers import quote_databricks, escape_string_literal
    from sql_artifact_control_common import (
        SQL_ARTIFACT_CONTROL_TABLE,
        SQL_ARTIFACT_EXECUTION_LOG_TABLE,
        SQL_ARTIFACT_CONTROL_COLUMNS,
        SQL_ARTIFACT_EXECUTION_LOG_COLUMNS,
        build_create_artifact_control_ddl,
        build_create_artifact_execution_log_ddl,
        build_upgrade_artifact_control_ddl,
        build_upgrade_artifact_execution_log_ddl,
        build_upgrade_sql_object_assessment_ddl,
        build_upgrade_sql_object_artifact_manifest_ddl,
        sanitize_error,
        plan_identity_migration,
        verify_schema,
    )

dbutils.widgets.text("run_id", "")
dbutils.widgets.text("catalog", "da_accelerators")
dbutils.widgets.text("control_schema", "control")

run_id = dbutils.widgets.get("run_id").strip() or get_run_id()
catalog = dbutils.widgets.get("catalog").strip() or CATALOG
control_schema = dbutils.widgets.get("control_schema").strip() or CONTROL_SCHEMA

print(f"Initializing and upgrading SQL artifact control tables in {catalog}.{control_schema}")

ctrl_fqn = f"{quote_databricks(catalog)}.{quote_databricks(control_schema)}.{quote_databricks(SQL_ARTIFACT_CONTROL_TABLE)}"
log_fqn = f"{quote_databricks(catalog)}.{quote_databricks(control_schema)}.{quote_databricks(SQL_ARTIFACT_EXECUTION_LOG_TABLE)}"
assessment_fqn = f"{quote_databricks(catalog)}.{quote_databricks(control_schema)}.{quote_databricks('sql_object_assessment')}"
manifest_fqn = f"{quote_databricks(catalog)}.{quote_databricks(control_schema)}.{quote_databricks('sql_object_artifact_manifest')}"

def _table_exists(fqn: str) -> bool:
    try:
        spark.table(fqn)
        return True
    except Exception:
        return False

# COMMAND ----------

# 1. Create SQL Artifact Control Table (Authoritative Owner)
try:
    control_ddl = build_create_artifact_control_ddl(catalog, control_schema)
    spark.sql(control_ddl)
    existing_cols = spark.table(ctrl_fqn).columns
    for stmt in build_upgrade_artifact_control_ddl(catalog, control_schema, existing_cols):
        spark.sql(stmt)
except Exception as e:
    safe_err = sanitize_error(e)
    raise RuntimeError(f"NB21 failed initializing/upgrading {SQL_ARTIFACT_CONTROL_TABLE}: {safe_err}") from e

print(f"SQL artifact control table ready: {catalog}.{control_schema}.{SQL_ARTIFACT_CONTROL_TABLE}")

# 2. Create Append-Only Execution Log Table (Authoritative Owner)
try:
    log_ddl = build_create_artifact_execution_log_ddl(catalog, control_schema)
    spark.sql(log_ddl)
    existing_cols = spark.table(log_fqn).columns
    for stmt in build_upgrade_artifact_execution_log_ddl(catalog, control_schema, existing_cols):
        spark.sql(stmt)
except Exception as e:
    safe_err = sanitize_error(e)
    raise RuntimeError(f"NB21 failed initializing/upgrading {SQL_ARTIFACT_EXECUTION_LOG_TABLE}: {safe_err}") from e

print(f"SQL artifact execution log table ready: {catalog}.{control_schema}.{SQL_ARTIFACT_EXECUTION_LOG_TABLE}")

# 3. Additive Legacy Upgrades for sql_object_assessment
if _table_exists(assessment_fqn):
    try:
        existing_cols = spark.table(assessment_fqn).columns
        for stmt in build_upgrade_sql_object_assessment_ddl(catalog, control_schema, existing_cols):
            spark.sql(stmt)
        print(f"Legacy upgrade check completed for {assessment_fqn}")
    except Exception as e:
        safe_err = sanitize_error(e)
        raise RuntimeError(f"NB21 failed upgrading sql_object_assessment: {safe_err}") from e

# 4. Additive Legacy Upgrades for sql_object_artifact_manifest
if _table_exists(manifest_fqn):
    try:
        existing_cols = spark.table(manifest_fqn).columns
        for stmt in build_upgrade_sql_object_artifact_manifest_ddl(catalog, control_schema, existing_cols):
            spark.sql(stmt)
        print(f"Legacy upgrade check completed for {manifest_fqn}")
    except Exception as e:
        safe_err = sanitize_error(e)
        raise RuntimeError(f"NB21 failed upgrading sql_object_artifact_manifest: {safe_err}") from e

# COMMAND ----------

def _row_to_dict(r):
    return r.asDict() if hasattr(r, "asDict") else dict(r)

# 5. v1 -> v2 Identity Migration (Defect 2)
try:
    ctrl_rows = [_row_to_dict(r) for r in spark.table(ctrl_fqn).collect()]
    updates, conflicts = plan_identity_migration(ctrl_rows)
    if conflicts:
        conflict_sample = [f"old={c.get('old_artifact_id')} -> v2={c.get('conflicting_v2_id')}" for c in conflicts[:10]]
        raise RuntimeError(f"NB21 identity migration detected {len(conflicts)} conflict(s): {conflict_sample}")

    for upd in updates:
        old_id = escape_string_literal(upd["old_artifact_id"])
        new_id = escape_string_literal(upd["artifact_id"])
        leg_id = escape_string_literal(upd["legacy_artifact_id"])
        spark.sql(f"""
            UPDATE {ctrl_fqn}
            SET artifact_id = {new_id},
                legacy_artifact_id = {leg_id},
                identity_version = 2
            WHERE artifact_id = {old_id}
        """)
except Exception as e:
    safe_err = sanitize_error(e)
    raise RuntimeError(f"NB21 failed during v1->v2 identity migration: {safe_err}") from e

# COMMAND ----------

# 6. Authoritative Schema Verification (Defect 6)
problems_ctrl = verify_schema(SQL_ARTIFACT_CONTROL_COLUMNS, spark.table(ctrl_fqn).schema)
if problems_ctrl:
    raise RuntimeError(f"NB21 verification failed for {SQL_ARTIFACT_CONTROL_TABLE}: {problems_ctrl}")

problems_log = verify_schema(SQL_ARTIFACT_EXECUTION_LOG_COLUMNS, spark.table(log_fqn).schema)
if problems_log:
    raise RuntimeError(f"NB21 verification failed for {SQL_ARTIFACT_EXECUTION_LOG_TABLE}: {problems_log}")

if _table_exists(assessment_fqn):
    assess_cols_lower = {c.lower() for c in spark.table(assessment_fqn).columns}
    for req_col in ("source_database", "error_message"):
        if req_col not in assess_cols_lower:
            raise RuntimeError(f"NB21 verification failed: sql_object_assessment missing required column '{req_col}'")

if _table_exists(manifest_fqn):
    manifest_cols_lower = {c.lower() for c in spark.table(manifest_fqn).columns}
    for req_col in ("source_database", "error_message"):
        if req_col not in manifest_cols_lower:
            raise RuntimeError(f"NB21 verification failed: sql_object_artifact_manifest missing required column '{req_col}'")

# COMMAND ----------

result = {
    "status": "SUCCEEDED",
    "run_id": run_id,
    "catalog": catalog,
    "control_schema": control_schema,
    "tables_initialized": [
        f"{catalog}.{control_schema}.{SQL_ARTIFACT_CONTROL_TABLE}",
        f"{catalog}.{control_schema}.{SQL_ARTIFACT_EXECUTION_LOG_TABLE}",
    ],
}

dbutils.notebook.exit(json.dumps(result))
