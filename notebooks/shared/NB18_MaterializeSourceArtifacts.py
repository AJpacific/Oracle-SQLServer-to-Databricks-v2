# Databricks notebook source
# MAGIC %md
# MAGIC # NB18_MaterializeSourceArtifacts
# MAGIC Original source definitions are read from `sql_object_assessment` and
# MAGIC materialized unchanged as `.sql` artifacts inside Unity Catalog Volumes.
# MAGIC It never converts, classifies, reviews, executes, or deploys source SQL.

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

import os
import uuid
import json
from datetime import datetime, timezone
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType
)

try:
    from src import sql_object_artifact_common as sqlobj_art
except ModuleNotFoundError:
    import sql_object_artifact_common as sqlobj_art

INACCESSIBLE_DEFINITION_REASON = (
    "Source definition is unavailable, blank, encrypted, "
    "or not visible to the registered source principal."
)

ACTIVE_MATERIALIZATION_STATUS = "IN_PROGRESS"
STALE_MATERIALIZATION_MINUTES = 120

MANIFEST_SCHEMA = StructType([
    StructField("connection_id", StringType(), False),
    StructField("source_system", StringType(), True),
    StructField("source_database", StringType(), True),
    StructField("source_schema", StringType(), False),
    StructField("object_type", StringType(), False),
    StructField("object_name", StringType(), False),

    StructField("target_catalog", StringType(), True),
    StructField("target_schema", StringType(), True),
    StructField("target_volume", StringType(), True),
    StructField("artifact_path", StringType(), True),

    StructField("source_definition_hash", StringType(), True),
    StructField("source_assessment_id", StringType(), True),
    StructField("source_captured_ts", TimestampType(), True),

    StructField("last_materialized_run_id", StringType(), True),
    StructField("materialization_status", StringType(), False),
    StructField("error_message", StringType(), True),

    StructField("created_ts", TimestampType(), True),
    StructField("updated_ts", TimestampType(), True),
])

def _row_to_dict(row):
    if row is None:
        return {}
    if hasattr(row, "asDict"):
        try:
            return row.asDict(recursive=True)
        except TypeError:
            return row.asDict()
    return dict(row)

def _is_recent_in_progress(updated_ts, threshold_minutes=120):
    if not updated_ts:
        return False
    if isinstance(updated_ts, str):
        try:
            updated_ts = datetime.fromisoformat(updated_ts)
        except Exception:
            return False
    now = now_utc()
    if hasattr(updated_ts, "tzinfo") and updated_ts.tzinfo is None:
        updated_ts = updated_ts.replace(tzinfo=timezone.utc)
    diff = (now - updated_ts).total_seconds() / 60.0
    return 0 <= diff <= threshold_minutes

def _ensure_widget(name, default):
    try:
        dbutils.widgets.text(name, default)
    except Exception:
        pass

_ensure_widget("run_id", "")
_ensure_widget("connection_id", "")
_ensure_widget("catalog", "da_accelerators")
_ensure_widget("control_schema", "control")
_ensure_widget("only_source_system", "")
_ensure_widget("only_assessment_id", "")
_ensure_widget("volume_name", "_source_artifacts")

run_id = dbutils.widgets.get("run_id").strip() or get_run_id()
connection_id = dbutils.widgets.get("connection_id").strip()
catalog = dbutils.widgets.get("catalog").strip() or CATALOG
control_schema = dbutils.widgets.get("control_schema").strip() or CONTROL_SCHEMA
only_source_system = dbutils.widgets.get("only_source_system").strip()
only_assessment_id = dbutils.widgets.get("only_assessment_id").strip()
volume_name = dbutils.widgets.get("volume_name").strip() or sqlobj_art.DEFAULT_VOLUME_NAME

repo = control_repo()

assessment_fqn = f"{quote_databricks(catalog)}.{quote_databricks(control_schema)}.{quote_databricks('sql_object_assessment')}"
manifest_fqn = f"{quote_databricks(catalog)}.{quote_databricks(control_schema)}.{quote_databricks('sql_object_artifact_manifest')}"

where_clauses = [
    "upper(trim(replace(object_type, ' ', '_'))) IN ('VIEW', 'PROCEDURE', 'FUNCTION', 'PACKAGE', 'PACKAGE_BODY')"
]

if only_assessment_id:
    where_clauses.append(f"assessment_id = {escape_string_literal(only_assessment_id)}")

if connection_id:
    where_clauses.append(f"connection_id = {escape_string_literal(connection_id)}")

if only_source_system:
    canonical_src = normalize_source_system(only_source_system)
    where_clauses.append(f"lower(trim(source_system)) = {escape_string_literal(canonical_src)}")

where_str = " AND ".join(where_clauses)
if where_str:
    where_str = "WHERE " + where_str

candidate_query = f"""
WITH ranked AS (
    SELECT
        assessment_id, run_id, connection_id, source_system, source_database,
        source_schema, object_name, object_type, source_definition,
        captured_ts, updated_ts,
        ROW_NUMBER() OVER (
            PARTITION BY connection_id, source_database, source_schema, object_type, object_name
            ORDER BY captured_ts DESC NULLS LAST, updated_ts DESC NULLS LAST,
                     assessment_id DESC, run_id DESC
        ) AS rn
    FROM {assessment_fqn}
    {where_str}
)
SELECT assessment_id, run_id, connection_id, source_system, source_database,
       source_schema, object_name, object_type, source_definition,
       captured_ts, updated_ts
FROM ranked
WHERE rn = 1
"""

raw_candidates = spark.sql(candidate_query).collect()
candidates = [_row_to_dict(r) for r in raw_candidates]

raw_manifest = spark.sql(f"SELECT * FROM {manifest_fqn}").collect()
existing_manifest_rows = [_row_to_dict(r) for r in raw_manifest]

existing_manifest_by_owner = {}
existing_paths_to_owner = {}
duplicate_manifest_owners = set()

for r in existing_manifest_rows:
    owner = sqlobj_art.artifact_owner_key(r)
    if owner in existing_manifest_by_owner:
        duplicate_manifest_owners.add(owner)
    existing_manifest_by_owner[owner] = r
    path = r.get("artifact_path")
    if path:
        existing_paths_to_owner.setdefault(path, set()).add(owner)

discovered_count = 0
materialized_count = 0
updated_count = 0
unchanged_count = 0
skipped_count = 0
failed_count = 0
errors = []
manifest_updates = []

def _merge_manifest_rows(rows):
    global failed_count
    if not rows:
        return
    m_df = spark.createDataFrame(rows, schema=MANIFEST_SCHEMA)
    view_name = f"_manifest_updates_{uuid.uuid4().hex}"
    m_df.createOrReplaceTempView(view_name)
    spark.sql(f"""
        MERGE INTO {manifest_fqn} t
        USING {view_name} s
           ON t.connection_id = s.connection_id
          AND coalesce(t.source_database, '') = coalesce(s.source_database, '')
          AND t.source_schema = s.source_schema
          AND t.object_type = s.object_type
          AND t.object_name = s.object_name
        WHEN MATCHED AND (
            t.materialization_status <> '{ACTIVE_MATERIALIZATION_STATUS}'
            OR t.last_materialized_run_id IS NULL
            OR trim(t.last_materialized_run_id) = ''
            OR t.last_materialized_run_id = s.last_materialized_run_id
            OR t.updated_ts IS NULL
            OR t.updated_ts < current_timestamp() - INTERVAL {STALE_MATERIALIZATION_MINUTES} MINUTES
        ) THEN UPDATE SET
          t.source_system = s.source_system,
          t.source_database = s.source_database,
          t.target_catalog = s.target_catalog,
          t.target_schema = s.target_schema,
          t.target_volume = s.target_volume,
          t.artifact_path = s.artifact_path,
          t.source_definition_hash = s.source_definition_hash,
          t.source_assessment_id = s.source_assessment_id,
          t.source_captured_ts = s.source_captured_ts,
          t.last_materialized_run_id = s.last_materialized_run_id,
          t.materialization_status = s.materialization_status,
          t.error_message = s.error_message,
          t.updated_ts = s.updated_ts
        WHEN NOT MATCHED THEN INSERT (
          connection_id, source_system, source_database, source_schema,
          object_type, object_name, target_catalog, target_schema,
          target_volume, artifact_path, source_definition_hash,
          source_assessment_id, source_captured_ts, last_materialized_run_id,
          materialization_status, error_message, created_ts, updated_ts
        ) VALUES (
          s.connection_id, s.source_system, s.source_database, s.source_schema,
          s.object_type, s.object_name, s.target_catalog, s.target_schema,
          s.target_volume, s.artifact_path, s.source_definition_hash,
          s.source_assessment_id, s.source_captured_ts, s.last_materialized_run_id,
          s.materialization_status, s.error_message, s.created_ts, s.updated_ts
        )
    """)
    for r in rows:
        owner_rows = spark.sql(f"""
            SELECT * FROM {manifest_fqn}
            WHERE connection_id = {escape_string_literal(r.get("connection_id"))}
              AND coalesce(source_database, '') = {escape_string_literal(r.get("source_database") or "")}
              AND source_schema = {escape_string_literal(r.get("source_schema"))}
              AND object_type = {escape_string_literal(r.get("object_type"))}
              AND object_name = {escape_string_literal(r.get("object_name"))}
        """).collect()
        if len(owner_rows) == 0:
            errors.append(f"MANIFEST_PERSISTENCE_FAILED: owner ({r.get('source_schema')}.{r.get('object_name')}) was not inserted")
            if r.get("materialization_status") == "UNABLE_TO_MATERIALIZE":
                failed_count += 1
        elif len(owner_rows) > 1:
            errors.append(f"DUPLICATE_MANIFEST_OWNERS: duplicate manifest entries found for owner ({r.get('source_schema')}.{r.get('object_name')})")
            if r.get("materialization_status") == "UNABLE_TO_MATERIALIZE":
                failed_count += 1
        else:
            rec = _row_to_dict(owner_rows[0])
            if (
                rec.get("materialization_status") == ACTIVE_MATERIALIZATION_STATUS
                and rec.get("last_materialized_run_id")
                and rec.get("last_materialized_run_id") != run_id
                and _is_recent_in_progress(rec.get("updated_ts"), STALE_MATERIALIZATION_MINUTES)
            ):
                conflict_msg = f"CONCURRENT_MATERIALIZATION: manifest persistence skipped; owner '{r.get('source_schema')}.{r.get('object_name')}' is actively held by another run"
                errors.append(failcls.sanitize_message(conflict_msg)[:500])

def _claim_manifest_owner(
    row,
    object_type,
    target_catalog,
    target_schema,
    target_volume,
    artifact_path,
    source_definition_hash,
    created_ts=None,
):
    now = now_utc()
    claim_row = {
        "connection_id": row.get("connection_id"),
        "source_system": row.get("source_system"),
        "source_database": row.get("source_database"),
        "source_schema": row.get("source_schema"),
        "object_type": object_type,
        "object_name": row.get("object_name"),
        "target_catalog": target_catalog,
        "target_schema": target_schema,
        "target_volume": target_volume,
        "artifact_path": artifact_path,
        "source_definition_hash": source_definition_hash,
        "source_assessment_id": row.get("assessment_id"),
        "source_captured_ts": row.get("captured_ts"),
        "last_materialized_run_id": run_id,
        "materialization_status": ACTIVE_MATERIALIZATION_STATUS,
        "error_message": None,
        "created_ts": created_ts or now,
        "updated_ts": now,
    }
    c_df = spark.createDataFrame([claim_row], schema=MANIFEST_SCHEMA)
    view_name = f"_claim_owner_{uuid.uuid4().hex}"
    c_df.createOrReplaceTempView(view_name)
    spark.sql(f"""
        MERGE INTO {manifest_fqn} t
        USING {view_name} s
           ON t.connection_id = s.connection_id
          AND coalesce(t.source_database, '') = coalesce(s.source_database, '')
          AND t.source_schema = s.source_schema
          AND t.object_type = s.object_type
          AND t.object_name = s.object_name
        WHEN MATCHED AND (
            t.materialization_status <> '{ACTIVE_MATERIALIZATION_STATUS}'
            OR t.last_materialized_run_id = s.last_materialized_run_id
            OR t.updated_ts IS NULL
            OR t.updated_ts < current_timestamp() - INTERVAL {STALE_MATERIALIZATION_MINUTES} MINUTES
        ) THEN UPDATE SET
          t.source_system = s.source_system,
          t.source_database = s.source_database,
          t.target_catalog = s.target_catalog,
          t.target_schema = s.target_schema,
          t.target_volume = s.target_volume,
          t.artifact_path = s.artifact_path,
          t.source_definition_hash = s.source_definition_hash,
          t.source_assessment_id = s.source_assessment_id,
          t.source_captured_ts = s.source_captured_ts,
          t.last_materialized_run_id = s.last_materialized_run_id,
          t.materialization_status = s.materialization_status,
          t.error_message = NULL,
          t.updated_ts = s.updated_ts
        WHEN NOT MATCHED THEN INSERT (
          connection_id, source_system, source_database, source_schema,
          object_type, object_name, target_catalog, target_schema,
          target_volume, artifact_path, source_definition_hash,
          source_assessment_id, source_captured_ts, last_materialized_run_id,
          materialization_status, error_message, created_ts, updated_ts
        ) VALUES (
          s.connection_id, s.source_system, s.source_database, s.source_schema,
          s.object_type, s.object_name, s.target_catalog, s.target_schema,
          s.target_volume, s.artifact_path, s.source_definition_hash,
          s.source_assessment_id, s.source_captured_ts, s.last_materialized_run_id,
          s.materialization_status, NULL, s.created_ts, s.updated_ts
        )
    """)
    owner_rows = spark.sql(f"""
        SELECT * FROM {manifest_fqn}
        WHERE connection_id = {escape_string_literal(row.get("connection_id"))}
          AND coalesce(source_database, '') = {escape_string_literal(row.get("source_database") or "")}
          AND source_schema = {escape_string_literal(row.get("source_schema"))}
          AND object_type = {escape_string_literal(object_type)}
          AND object_name = {escape_string_literal(row.get("object_name"))}
    """).collect()
    if len(owner_rows) == 0:
        return False, "CONCURRENT_MATERIALIZATION: artifact owner claim could not be acquired", None
    if len(owner_rows) > 1:
        return False, f"DUPLICATE_MANIFEST_OWNERS: duplicate manifest entries found for owner ({row.get('connection_id')}, {row.get('source_schema')}, {object_type}, {row.get('object_name')})", None
    owner_rec = _row_to_dict(owner_rows[0])
    if (
        owner_rec.get("materialization_status") != ACTIVE_MATERIALIZATION_STATUS
        or owner_rec.get("last_materialized_run_id") != run_id
    ):
        return False, "CONCURRENT_MATERIALIZATION: artifact owner is actively held by another run", owner_rec
    return True, None, owner_rec

def _finalize_claimed_owner(result_row):
    f_df = spark.createDataFrame([result_row], schema=MANIFEST_SCHEMA)
    view_name = f"_finalize_owner_{uuid.uuid4().hex}"
    f_df.createOrReplaceTempView(view_name)
    spark.sql(f"""
        MERGE INTO {manifest_fqn} t
        USING {view_name} s
           ON t.connection_id = s.connection_id
          AND coalesce(t.source_database, '') = coalesce(s.source_database, '')
          AND t.source_schema = s.source_schema
          AND t.object_type = s.object_type
          AND t.object_name = s.object_name
        WHEN MATCHED AND t.last_materialized_run_id = s.last_materialized_run_id
                     AND t.materialization_status = '{ACTIVE_MATERIALIZATION_STATUS}'
        THEN UPDATE SET
          t.source_system = s.source_system,
          t.source_database = s.source_database,
          t.target_catalog = s.target_catalog,
          t.target_schema = s.target_schema,
          t.target_volume = s.target_volume,
          t.artifact_path = s.artifact_path,
          t.source_definition_hash = s.source_definition_hash,
          t.source_assessment_id = s.source_assessment_id,
          t.source_captured_ts = s.source_captured_ts,
          t.last_materialized_run_id = s.last_materialized_run_id,
          t.materialization_status = s.materialization_status,
          t.error_message = s.error_message,
          t.updated_ts = s.updated_ts
    """)
    owner_rows = spark.sql(f"""
        SELECT * FROM {manifest_fqn}
        WHERE connection_id = {escape_string_literal(result_row["connection_id"])}
          AND coalesce(source_database, '') = {escape_string_literal(result_row.get("source_database") or "")}
          AND source_schema = {escape_string_literal(result_row["source_schema"])}
          AND object_type = {escape_string_literal(result_row["object_type"])}
          AND object_name = {escape_string_literal(result_row["object_name"])}
    """).collect()
    if len(owner_rows) != 1:
        return False
    rec = _row_to_dict(owner_rows[0])
    return (
        rec.get("materialization_status") == result_row["materialization_status"]
        and rec.get("last_materialized_run_id") == run_id
    )

def _append_manifest_result(
    row,
    object_type,
    materialization_status,
    error_message=None,
    target_catalog=None,
    target_schema=None,
    target_volume=None,
    artifact_path=None,
    source_definition_hash=None,
    created_ts=None,
):
    now = now_utc()
    bounded_err = (failcls.sanitize_message(error_message)[:1000]) if error_message else None
    manifest_updates.append({
        "connection_id": row.get("connection_id"),
        "source_system": row.get("source_system"),
        "source_database": row.get("source_database"),
        "source_schema": row.get("source_schema"),
        "object_type": object_type,
        "object_name": row.get("object_name"),
        "target_catalog": target_catalog,
        "target_schema": target_schema,
        "target_volume": target_volume,
        "artifact_path": artifact_path,
        "source_definition_hash": source_definition_hash,
        "source_assessment_id": row.get("assessment_id"),
        "source_captured_ts": row.get("captured_ts"),
        "last_materialized_run_id": run_id,
        "materialization_status": materialization_status,
        "error_message": bounded_err,
        "created_ts": created_ts or now,
        "updated_ts": now,
    })

def _write_artifact_file(vol_path, content):
    dirname = os.path.dirname(vol_path)
    os.makedirs(dirname, exist_ok=True)
    tmp_path = f"{vol_path}.tmp.{uuid.uuid4().hex}"
    try:
        with open(tmp_path, "w", encoding="utf-8", newline="") as f:
            f.write(content)
        os.replace(tmp_path, vol_path)
    except Exception as exc:
        if os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except Exception:
                pass
        raise exc

for row in candidates:
    discovered_count += 1

    try:
        norm_type = sqlobj_art.normalize_object_type(row.get("object_type"))
    except Exception as e:
        err_msg = failcls.sanitize_message(e)
        errors.append(err_msg[:500])
        failed_count += 1
        continue

    conn_id = row.get("connection_id")
    source_sys = row.get("source_system")
    source_db = row.get("source_database")
    source_sch = row.get("source_schema")
    obj_name = row.get("object_name")
    assess_id = row.get("assessment_id")
    capt_ts = row.get("captured_ts")

    owner = sqlobj_art.artifact_owner_key(row)
    legacy_owner = (conn_id, source_sch, norm_type, obj_name)
    existing_rec = existing_manifest_by_owner.get(owner) or existing_manifest_by_owner.get(legacy_owner)
    prev_created_ts = existing_rec.get("created_ts") if existing_rec else None

    # Fail fast if another active run is currently materializing this owner:
    # Do NOT mutate the owner manifest row; preserve active run's lock intact
    if existing_rec and existing_rec.get("materialization_status") == ACTIVE_MATERIALIZATION_STATUS:
        last_run = existing_rec.get("last_materialized_run_id")
        upd_ts = existing_rec.get("updated_ts")
        if last_run and last_run != run_id and _is_recent_in_progress(upd_ts, STALE_MATERIALIZATION_MINUTES):
            err_msg = "CONCURRENT_MATERIALIZATION: artifact owner is actively held by another run"
            errors.append(failcls.sanitize_message(err_msg)[:500])
            failed_count += 1
            continue

    source_def = row.get("source_definition")
    if source_def is None or not str(source_def).strip():
        skipped_count += 1
        _append_manifest_result(
            row=row,
            object_type=norm_type,
            materialization_status="UNABLE_TO_MATERIALIZE",
            error_message=INACCESSIBLE_DEFINITION_REASON,
            target_volume=volume_name,
            created_ts=prev_created_ts,
        )
        continue

    if owner in duplicate_manifest_owners:
        err_msg = f"DUPLICATE_MANIFEST_OWNERS: Duplicate manifest entry found for owner {owner}"
        errors.append(failcls.sanitize_message(err_msg)[:500])
        failed_count += 1
        _append_manifest_result(
            row=row,
            object_type=norm_type,
            materialization_status="FAILED",
            error_message=err_msg,
            target_volume=volume_name,
            created_ts=prev_created_ts,
        )
        continue

    try:
        target_cfg = repo.resolve_target_config(conn_id)
    except Exception as e:
        err_msg = f"TARGET_CONFIG_ERROR: {failcls.sanitize_message(e)}"
        errors.append(err_msg[:500])
        failed_count += 1
        _append_manifest_result(
            row=row,
            object_type=norm_type,
            materialization_status="TARGET_CONFIG_ERROR",
            error_message=err_msg,
            target_volume=volume_name,
            created_ts=prev_created_ts,
        )
        continue

    target_cat = target_cfg.get("target_catalog")
    target_mode = str(target_cfg.get("target_schema_mode") or "").upper().strip()
    explicit_sch = target_cfg.get("target_schema")

    if target_mode == "EXPLICIT":
        if not explicit_sch or not explicit_sch.strip():
            err_msg = "TARGET_CONFIG_ERROR: EXPLICIT target_schema_mode requires target_schema"
            errors.append(err_msg[:500])
            failed_count += 1
            _append_manifest_result(
                row=row,
                object_type=norm_type,
                materialization_status="TARGET_CONFIG_ERROR",
                error_message=err_msg,
                target_catalog=target_cat,
                target_volume=volume_name,
                created_ts=prev_created_ts,
            )
            continue
        target_sch = explicit_sch.strip()
    elif target_mode == "PREFIX_WITH_DATABASE":
        target_sch = f"{(source_db or '')}_{source_sch}".lower().strip("_")
    else:
        target_sch = source_sch.lower()

    try:
        rel_path = sqlobj_art.build_artifact_relative_path(
            conn_id, source_sch, norm_type, obj_name, source_database=source_db
        )
        vol_path = sqlobj_art.build_artifact_volume_path(target_cat, target_sch, volume_name, rel_path)
    except Exception as e:
        err_msg = failcls.sanitize_message(e)
        errors.append(err_msg[:500])
        failed_count += 1
        _append_manifest_result(
            row=row,
            object_type=norm_type,
            materialization_status="TARGET_CONFIG_ERROR",
            error_message=err_msg,
            target_catalog=target_cat,
            target_schema=target_sch,
            target_volume=volume_name,
            created_ts=prev_created_ts,
        )
        continue

    owners_with_path = existing_paths_to_owner.get(vol_path, set())
    if any(o != owner and o != legacy_owner for o in owners_with_path):
        err_msg = f"ARTIFACT_PATH_COLLISION: path {vol_path} is already claimed by another owner"
        errors.append(failcls.sanitize_message(err_msg)[:500])
        failed_count += 1
        _append_manifest_result(
            row=row,
            object_type=norm_type,
            materialization_status="ARTIFACT_PATH_COLLISION",
            error_message=err_msg,
            target_catalog=target_cat,
            target_schema=target_sch,
            target_volume=volume_name,
            artifact_path=vol_path,
            created_ts=prev_created_ts,
        )
        continue

    def_hash = sqlobj_art.definition_sha256(source_def)

    if existing_rec:
        old_path = existing_rec.get("artifact_path")
        old_hash = existing_rec.get("source_definition_hash")
        old_status = existing_rec.get("materialization_status")

        if old_path and old_path != vol_path:
            err_msg = f"TARGET_CONFIG_CHANGED: artifact path changed from {old_path} to {vol_path}"
            errors.append(failcls.sanitize_message(err_msg)[:500])
            failed_count += 1
            _append_manifest_result(
                row=row,
                object_type=norm_type,
                materialization_status="TARGET_CONFIG_CHANGED",
                error_message=err_msg,
                target_catalog=target_cat,
                target_schema=target_sch,
                target_volume=volume_name,
                artifact_path=old_path,
                source_definition_hash=def_hash,
                created_ts=prev_created_ts,
            )
            continue

        if old_hash == def_hash and old_status == "SUCCEEDED":
            unchanged_count += 1
            continue

    # Attempt conditional IN_PROGRESS claim and verify ownership before mutating filesystem
    claimed, claim_err, reread_rec = _claim_manifest_owner(
        row=row,
        object_type=norm_type,
        target_catalog=target_cat,
        target_schema=target_sch,
        target_volume=volume_name,
        artifact_path=vol_path,
        source_definition_hash=def_hash,
        created_ts=prev_created_ts,
    )
    if not claimed:
        errors.append(failcls.sanitize_message(claim_err)[:500])
        failed_count += 1
        continue

    existing_manifest_by_owner[owner] = reread_rec
    existing_manifest_by_owner[legacy_owner] = reread_rec

    try:
        spark.sql(ddl.build_create_schema(target_cat, target_sch))
        spark.sql(f"CREATE VOLUME IF NOT EXISTS {quote_databricks(target_cat)}.{quote_databricks(target_sch)}.{quote_databricks(volume_name)}")
    except Exception as e:
        err_msg = f"VOLUME_CREATION_FAILED: {failcls.sanitize_message(e)}"
        errors.append(err_msg[:500])
        failed_count += 1
        final_res = {
            "connection_id": conn_id,
            "source_system": source_sys,
            "source_database": source_db,
            "source_schema": source_sch,
            "object_type": norm_type,
            "object_name": obj_name,
            "target_catalog": target_cat,
            "target_schema": target_sch,
            "target_volume": volume_name,
            "artifact_path": vol_path,
            "source_definition_hash": def_hash,
            "source_assessment_id": assess_id,
            "source_captured_ts": capt_ts,
            "last_materialized_run_id": run_id,
            "materialization_status": "VOLUME_CREATION_FAILED",
            "error_message": failcls.sanitize_message(err_msg)[:1000],
            "created_ts": prev_created_ts or now_utc(),
            "updated_ts": now_utc(),
        }
        if not _finalize_claimed_owner(final_res):
            errors.append("VOLUME_CREATION_FAILED: finalization verification failed")
        continue

    try:
        _write_artifact_file(vol_path, source_def)
    except Exception as e:
        err_msg = f"WRITE_FAILED: {failcls.sanitize_message(e)}"
        errors.append(err_msg[:500])
        failed_count += 1
        final_res = {
            "connection_id": conn_id,
            "source_system": source_sys,
            "source_database": source_db,
            "source_schema": source_sch,
            "object_type": norm_type,
            "object_name": obj_name,
            "target_catalog": target_cat,
            "target_schema": target_sch,
            "target_volume": volume_name,
            "artifact_path": vol_path,
            "source_definition_hash": def_hash,
            "source_assessment_id": assess_id,
            "source_captured_ts": capt_ts,
            "last_materialized_run_id": run_id,
            "materialization_status": "WRITE_FAILED",
            "error_message": failcls.sanitize_message(err_msg)[:1000],
            "created_ts": prev_created_ts or now_utc(),
            "updated_ts": now_utc(),
        }
        if not _finalize_claimed_owner(final_res):
            errors.append("WRITE_FAILED: finalization verification failed")
        continue

    success_res = {
        "connection_id": conn_id,
        "source_system": source_sys,
        "source_database": source_db,
        "source_schema": source_sch,
        "object_type": norm_type,
        "object_name": obj_name,
        "target_catalog": target_cat,
        "target_schema": target_sch,
        "target_volume": volume_name,
        "artifact_path": vol_path,
        "source_definition_hash": def_hash,
        "source_assessment_id": assess_id,
        "source_captured_ts": capt_ts,
        "last_materialized_run_id": run_id,
        "materialization_status": "SUCCEEDED",
        "error_message": None,
        "created_ts": prev_created_ts or now_utc(),
        "updated_ts": now_utc(),
    }
    if _finalize_claimed_owner(success_res):
        if existing_rec:
            updated_count += 1
        else:
            materialized_count += 1
    else:
        failed_count += 1
        errors.append("FINALIZATION_FAILED: artifact owner terminal status could not be verified")

_merge_manifest_rows(manifest_updates)

if discovered_count == 0:
    business_status = "NO_OBJECTS"
elif failed_count > 0 and (materialized_count > 0 or updated_count > 0 or unchanged_count > 0):
    business_status = "PARTIAL"
elif failed_count > 0:
    business_status = "FAILED"
else:
    business_status = "COMPLETE"

status = "FAILED" if failed_count > 0 else "SUCCEEDED"

set_task_value("run_id", run_id)
set_task_value("status", status)
set_task_value("business_status", business_status)
set_task_value("discovered_count", int(discovered_count))
set_task_value("materialized_count", int(materialized_count))
set_task_value("updated_count", int(updated_count))
set_task_value("unchanged_count", int(unchanged_count))
set_task_value("skipped_count", int(skipped_count))
set_task_value("failed_count", int(failed_count))

result_payload = {
    "status": status,
    "business_status": business_status,
    "run_id": run_id,
    "discovered_count": int(discovered_count),
    "materialized_count": int(materialized_count),
    "updated_count": int(updated_count),
    "unchanged_count": int(unchanged_count),
    "skipped_count": int(skipped_count),
    "failed_count": int(failed_count),
    "errors": errors[:20],
}

print(f"NB18 complete: status={status}, business_status={business_status}, "
      f"discovered={discovered_count}, materialized={materialized_count}, "
      f"updated={updated_count}, unchanged={unchanged_count}, "
      f"skipped={skipped_count}, failed={failed_count}")

if failed_count > 0:
    raise RuntimeError(f"NB18 materialization failed with {failed_count} error(s)")

dbutils.notebook.exit(json.dumps(result_payload))
