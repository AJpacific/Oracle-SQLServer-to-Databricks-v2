# Databricks notebook source
# MAGIC %md
# MAGIC # NB_MigrateSourceTableIdentityV2
# MAGIC Manual, dry-run-first migration from physical-source IDs to
# MAGIC connection-owned source identity v2. This notebook is never part of an
# MAGIC onboarding, Full Load, Delta, retry, or ETL workflow.

# COMMAND ----------

# MAGIC %run ../shared/_common

# COMMAND ----------

dbutils.widgets.dropdown("dry_run", "true", ["true", "false"])
dbutils.widgets.text("catalog", "da_accelerators")
dbutils.widgets.text("control_schema", "control")
dbutils.widgets.text("only_connection_ids", "")
dbutils.widgets.text("batch_size", "0")

dry_run = dbutils.widgets.get("dry_run").strip().lower() == "true"
migration_catalog = dbutils.widgets.get("catalog").strip()
migration_schema = dbutils.widgets.get("control_schema").strip()
only_connection_ids = {
    value.strip()
    for value in dbutils.widgets.get("only_connection_ids").split(",")
    if value.strip()
}
try:
    batch_size = int(dbutils.widgets.get("batch_size").strip() or "0")
except ValueError as exc:
    raise ValueError("batch_size must be a non-negative integer") from exc
if not migration_catalog or not migration_schema:
    raise ValueError("catalog and control_schema are required")
if batch_size < 0:
    raise ValueError("batch_size must be a non-negative integer")

migration_id = new_run_id("source_identity_v2")
repo = ControlRepository(spark, migration_catalog, migration_schema)


def ctrl(table_name):
    return (
        f"{quote_databricks(migration_catalog)}."
        f"{quote_databricks(migration_schema)}."
        f"{quote_databricks(table_name)}"
    )


def _safe_error(stage, error, connection_id=None, old_source_table_id=None):
    return {
        "stage": stage,
        "connection_id": connection_id,
        "old_source_table_id": old_source_table_id,
        "exception_type": type(error).__name__,
        "message": failcls.sanitize_message(error)[:400],
    }


CHILD_TABLE_CANDIDATES = (
    "source_inventory",
    "normalized_source_inventory",
    "resolved_column_mappings",
    "mapping_validation_results",
    "table_load_decisions",
    "review_queue",
    "table_run_log",
    "delta_sync_queue",
    "reconciliation_results",
    "dq_rule",
    "dq_result",
    "dq_quarantine",
)
ERROR_LIMIT = 20
errors = []

# COMMAND ----------

from pyspark.sql import functions as F

base_control = spark.table(ctrl("source_table_control").replace("`", ""))
if only_connection_ids:
    base_control = base_control.filter(
        F.col("connection_id").isin(list(only_connection_ids))
    )

unmigrated_candidates = (
    base_control.filter(
        (F.col("source_identity_version").isNull())
        | (F.col("source_identity_version") != F.lit(SOURCE_IDENTITY_VERSION))
    )
    .orderBy("connection_id", "source_schema", "source_table", "source_table_id")
)

total_unmigrated_count = unmigrated_candidates.count()

if total_unmigrated_count > 0:
    batch_query = unmigrated_candidates
    if batch_size > 0:
        batch_query = batch_query.limit(batch_size)
else:
    batch_query = base_control.orderBy(
        "connection_id", "source_schema", "source_table", "source_table_id"
    )
    if batch_size > 0:
        batch_query = batch_query.limit(batch_size)

control_rows = batch_query.collect()

connection_ids = sorted({
    str(row["connection_id"] or "").strip()
    for row in control_rows if str(row["connection_id"] or "").strip()
})
connections = {}
for connection_id in connection_ids:
    try:
        connection = repo.get_connection(connection_id)
        if connection is None:
            raise ValueError(
                f"connection_id {connection_id!r} not found in source_connection")
        connections[connection_id] = connection
    except Exception as exc:
        errors.append(_safe_error("connection_validation", exc, connection_id))

mappings = []
for row in control_rows:
    data = row.asDict(recursive=True)
    old_source_table_id = str(data.get("source_table_id") or "").strip()
    connection_id = str(data.get("connection_id") or "").strip()
    status = "READY"
    message = None
    new_source_table_id = None
    mapped_source_system = data.get("source_system")
    mapped_server = data.get("source_server")
    mapped_database = data.get("source_database")
    try:
        connection_id = require_connection_id(
            connection_id, "source identity migration row")
        if not old_source_table_id:
            raise ValueError(
                "migration row requires an existing source_table_id")
        for field in ("source_system", "source_schema", "source_table"):
            if not str(data.get(field) or "").strip():
                raise ValueError(f"migration row requires {field}")
        connection = connections.get(connection_id)
        if connection is None:
            raise ValueError(
                f"connection_id {connection_id!r} is unavailable for migration")
        assert_source_identity_match(data, connection)
        connection_data = connection.asDict()
        source_system = require_source_system(
            data.get("source_system"), "source identity migration row")
        mapped_source_system = source_system
        mapped_server = connection_data.get("source_server")
        mapped_database = resolve_effective_source_database(data, connection)
        new_source_table_id = compute_source_table_id(
            connection_id, source_system, mapped_server, mapped_database,
            data["source_schema"], data["source_table"])
        if (data.get("source_identity_version") == SOURCE_IDENTITY_VERSION
                and old_source_table_id == new_source_table_id):
            status = "ALREADY_MIGRATED"
            message = "registration already uses source identity v2"
    except Exception as exc:
        status = "BLOCKED"
        message = failcls.sanitize_message(exc)[:400]
        errors.append(_safe_error(
            "mapping_validation", exc, connection_id or None,
            old_source_table_id or None))
    mappings.append({
        "migration_id": migration_id,
        "connection_id": connection_id or None,
        "old_source_table_id": old_source_table_id or None,
        "new_source_table_id": new_source_table_id,
        "source_system": mapped_source_system,
        "source_server": mapped_server,
        "source_database": mapped_database,
        "source_schema": data.get("source_schema"),
        "source_table": data.get("source_table"),
        "source_identity_version": SOURCE_IDENTITY_VERSION,
        "migration_status": status,
        "migration_message": message,
    })

# Validate deterministic ownership before selecting a batch.
owner_counts = {}
for mapping in mappings:
    owner = (mapping.get("connection_id"), mapping.get("old_source_table_id"))
    owner_counts[owner] = owner_counts.get(owner, 0) + 1
for owner, count in owner_counts.items():
    if count <= 1:
        continue
    error = ValueError(
        f"registration ownership key {owner!r} occurs {count} times")
    errors.append(_safe_error(
        "duplicate_registration", error, owner[0], owner[1]))
    for mapping in mappings:
        if (mapping.get("connection_id"),
                mapping.get("old_source_table_id")) == owner:
            mapping["migration_status"] = "BLOCKED"
            mapping["migration_message"] = failcls.sanitize_message(error)

new_id_owners = {}
for mapping in mappings:
    new_id = mapping.get("new_source_table_id")
    if not new_id:
        continue
    owner = (mapping.get("connection_id"), mapping.get("old_source_table_id"))
    prior = new_id_owners.setdefault(new_id, owner)
    if prior != owner:
        error = ValueError(
            f"new source_table_id {new_id!r} has multiple registration owners")
        errors.append(_safe_error("identity_collision", error))
        mapping["migration_status"] = "BLOCKED"
        mapping["migration_message"] = failcls.sanitize_message(error)

new_ids = {
    mapping.get("new_source_table_id")
    for mapping in mappings
    if mapping.get("new_source_table_id")
}
existing_id_owners = {}
if new_ids:
    in_new_ids = ", ".join(escape_string_literal(nid) for nid in sorted(new_ids))
    conflicting_ctrl_rows = spark.sql(f"""
        SELECT connection_id, source_table_id
        FROM {ctrl('source_table_control')}
        WHERE source_table_id IN ({in_new_ids})
    """).collect()
    for row in conflicting_ctrl_rows:
        source_table_id = str(row["source_table_id"] or "").strip()
        owner = (str(row["connection_id"] or "").strip(), source_table_id)
        existing_id_owners.setdefault(source_table_id, set()).add(owner)

for mapping in mappings:
    new_key = (mapping.get("connection_id"), mapping.get("new_source_table_id"))
    old_key = (mapping.get("connection_id"), mapping.get("old_source_table_id"))
    conflicting_owners = existing_id_owners.get(
        mapping.get("new_source_table_id"), set()) - {old_key, new_key}
    if mapping["migration_status"] == "READY" and conflicting_owners:
        error = ValueError(
            f"new identity {new_key!r} conflicts with an existing registration")
        errors.append(_safe_error(
            "identity_collision", error, mapping.get("connection_id"),
            mapping.get("old_source_table_id")))
        mapping["migration_status"] = "BLOCKED"
        mapping["migration_message"] = failcls.sanitize_message(error)

child_tables = []
child_table_columns = {}
for table_name in CHILD_TABLE_CANDIDATES:
    plain_name = ctrl(table_name).replace("`", "")
    if not spark.catalog.tableExists(plain_name):
        continue
    columns = set(spark.table(plain_name).columns)
    if "source_table_id" in columns:
        if "connection_id" not in columns:
            error = ValueError(
                f"{table_name} has source_table_id but no connection_id")
            errors.append(_safe_error("child_schema_validation", error))
        else:
            child_tables.append(table_name)
            child_table_columns[table_name] = columns

candidate_pairs = sorted({
    (mapping["connection_id"], mapping["new_source_table_id"])
    for mapping in mappings
    if mapping.get("connection_id") and mapping.get("new_source_table_id")
})
candidate_df = (
    spark.createDataFrame(
        candidate_pairs, ["connection_id", "source_table_id"])
    if candidate_pairs else
    spark.createDataFrame(
        [], "connection_id STRING, source_table_id STRING")
)
candidate_df.createOrReplaceTempView("_identity_v2_candidates")

for table_name in child_tables:
    incomplete = spark.sql(f"""
        SELECT count(*) AS c
        FROM {ctrl(table_name)}
        WHERE source_table_id IS NULL OR trim(source_table_id) = ''
           OR connection_id IS NULL OR trim(connection_id) = ''
    """).collect()[0]["c"]
    if incomplete:
        errors.append(_safe_error(
            "child_ownership_validation",
            ValueError(
                f"{table_name} has {incomplete} incomplete identity row(s)")))

    connection_scope = (
        " AND child.connection_id IN (" + ", ".join(
            escape_string_literal(value)
            for value in sorted(only_connection_ids)
        ) + ")" if only_connection_ids else ""
    )
    orphaned = spark.sql(f"""
        SELECT count(*) AS c
        FROM {ctrl(table_name)} child
        LEFT ANTI JOIN {ctrl('source_table_control')} owner
          ON child.connection_id = owner.connection_id
         AND child.source_table_id = owner.source_table_id
                LEFT ANTI JOIN _identity_v2_candidates candidate
                    ON child.connection_id = candidate.connection_id
                 AND child.source_table_id = candidate.source_table_id
        WHERE child.connection_id IS NOT NULL
          AND trim(child.connection_id) <> ''
          AND child.source_table_id IS NOT NULL
          AND trim(child.source_table_id) <> ''
          {connection_scope}
    """).collect()[0]["c"]
    if orphaned:
        errors.append(_safe_error(
            "child_ownership_validation",
            ValueError(
                f"{table_name} has {orphaned} orphan identity row(s)")))

ready_mappings = [
    mapping for mapping in mappings if mapping["migration_status"] == "READY"
]
for mapping in ready_mappings:
    for table_name in child_tables:
        ambiguous = spark.sql(f"""
            SELECT count(*) AS c
            FROM {ctrl(table_name)}
            WHERE source_table_id =
                  {escape_string_literal(mapping['old_source_table_id'])}
              AND (connection_id IS NULL OR trim(connection_id) = '')
        """).collect()[0]["c"]
        if ambiguous:
            error = ValueError(
                f"{table_name} has {ambiguous} child row(s) without connection_id "
                f"for legacy source_table_id {mapping['old_source_table_id']!r}")
            errors.append(_safe_error(
                "child_ownership_validation", error,
                mapping["connection_id"], mapping["old_source_table_id"]))
            mapping["migration_status"] = "BLOCKED"
            mapping["migration_message"] = failcls.sanitize_message(error)
            break

batch_candidate_count = len(mappings)
rows_examined = len(mappings)
rows_already_migrated = sum(
    mapping["migration_status"] == "ALREADY_MIGRATED" for mapping in mappings)
rows_blocked = sum(
    mapping["migration_status"] == "BLOCKED" for mapping in mappings)
rows_ready = sum(
    mapping["migration_status"] == "READY" for mapping in mappings
)
ready_batch = [
    mapping for mapping in mappings
    if mapping["migration_status"] == "READY"
]

from pyspark.sql.types import IntegerType, StringType, StructField, StructType

mapping_schema = StructType([
    StructField("migration_id", StringType(), False),
    StructField("connection_id", StringType(), True),
    StructField("old_source_table_id", StringType(), True),
    StructField("new_source_table_id", StringType(), True),
    StructField("source_system", StringType(), True),
    StructField("source_server", StringType(), True),
    StructField("source_database", StringType(), True),
    StructField("source_schema", StringType(), True),
    StructField("source_table", StringType(), True),
    StructField("source_identity_version", IntegerType(), False),
    StructField("migration_status", StringType(), False),
    StructField("migration_message", StringType(), True),
])
error_schema = StructType([
    StructField("stage", StringType(), False),
    StructField("connection_id", StringType(), True),
    StructField("old_source_table_id", StringType(), True),
    StructField("exception_type", StringType(), False),
    StructField("message", StringType(), False),
])
mapping_df = spark.createDataFrame(mappings, mapping_schema) if mappings else None
print(f"Identity v2 migration dry_run={dry_run}: examined={rows_examined}, "
      f"ready={rows_ready}, already_migrated={rows_already_migrated}, "
      f"blocked={rows_blocked}")
if mapping_df is not None:
    display(mapping_df.orderBy("connection_id", "source_schema", "source_table"))
if errors:
    display(spark.createDataFrame(errors[:ERROR_LIMIT], error_schema))

# COMMAND ----------

rows_migrated = 0
child_rows_updated = 0
if not dry_run:
    if rows_blocked or errors:
        result = {
            "status": "FAILED",
            "business_status": "BLOCKED" if rows_blocked else "FAILED",
            "dry_run": False,
            "migration_id": migration_id,
            "total_unmigrated_count": total_unmigrated_count,
            "batch_candidate_count": batch_candidate_count,
            "rows_examined": rows_examined,
            "rows_ready": rows_ready,
            "rows_blocked": rows_blocked,
            "rows_already_migrated": rows_already_migrated,
            "rows_migrated": 0,
            "remaining_unmigrated_count": total_unmigrated_count,
            "child_rows_updated": 0,
            "error_count": len(errors),
            "errors": errors[:ERROR_LIMIT],
        }
        print(json.dumps(result))
        raise RuntimeError(
            "Identity v2 migration blocked by pre-validation errors")

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {ctrl('source_table_identity_migration')} (
          migration_id STRING,
          connection_id STRING,
          old_source_table_id STRING,
          new_source_table_id STRING,
          source_identity_version INT,
          migration_status STRING,
          migration_message STRING,
          created_ts TIMESTAMP,
          updated_ts TIMESTAMP
        ) USING DELTA
    """)

    for mapping in ready_batch:
        connection_id = mapping["connection_id"]
        old_id = mapping["old_source_table_id"]
        new_id = mapping["new_source_table_id"]
        source_system = mapping["source_system"]
        try:
            spark.sql(f"""
                MERGE INTO {ctrl('source_table_identity_migration')} t
                USING (SELECT
                    {escape_string_literal(migration_id)} AS migration_id,
                    {escape_string_literal(connection_id)} AS connection_id,
                    {escape_string_literal(old_id)} AS old_source_table_id,
                    {escape_string_literal(new_id)} AS new_source_table_id
                ) s
                  ON t.connection_id = s.connection_id
                 AND t.old_source_table_id = s.old_source_table_id
                 AND t.new_source_table_id = s.new_source_table_id
                WHEN MATCHED THEN UPDATE SET
                  t.migration_id = s.migration_id,
                  t.source_identity_version = {SOURCE_IDENTITY_VERSION},
                  t.migration_status = 'IN_PROGRESS',
                  t.migration_message = NULL,
                  t.updated_ts = current_timestamp()
                WHEN NOT MATCHED THEN INSERT (
                  migration_id, connection_id, old_source_table_id,
                  new_source_table_id, source_identity_version,
                  migration_status, migration_message, created_ts, updated_ts
                ) VALUES (
                  s.migration_id, s.connection_id, s.old_source_table_id,
                  s.new_source_table_id, {SOURCE_IDENTITY_VERSION},
                  'IN_PROGRESS', NULL, current_timestamp(), current_timestamp()
                )
            """)
            for table_name in child_tables:
                row_count = spark.sql(f"""
                    SELECT count(*) AS c FROM {ctrl(table_name)}
                    WHERE connection_id = {escape_string_literal(connection_id)}
                      AND source_table_id = {escape_string_literal(old_id)}
                """).collect()[0]["c"]
                if row_count:
                    source_system_update = (
                        f", source_system = "
                        f"{escape_string_literal(source_system)}"
                        if "source_system" in child_table_columns[table_name]
                        else "")
                    spark.sql(f"""
                        UPDATE {ctrl(table_name)}
                        SET source_table_id = {escape_string_literal(new_id)}
                            {source_system_update}
                        WHERE connection_id =
                              {escape_string_literal(connection_id)}
                          AND source_table_id =
                              {escape_string_literal(old_id)}
                    """)
                    child_rows_updated += row_count

            # Update the owner last, after every child table has succeeded.
            spark.sql(f"""
                UPDATE {ctrl('source_table_control')}
                SET legacy_source_table_id =
                        coalesce(legacy_source_table_id,
                                 {escape_string_literal(old_id)}),
                    source_table_id = {escape_string_literal(new_id)},
                    source_system = {escape_string_literal(source_system)},
                    source_server =
                        {escape_string_literal(mapping['source_server'])},
                    source_database =
                        {escape_string_literal(mapping['source_database'])},
                    source_identity_version = {SOURCE_IDENTITY_VERSION},
                    updated_ts = current_timestamp()
                WHERE connection_id = {escape_string_literal(connection_id)}
                  AND source_table_id = {escape_string_literal(old_id)}
            """)
            spark.sql(f"""
                UPDATE {ctrl('source_table_identity_migration')}
                SET migration_status = 'MIGRATED', migration_message = NULL,
                    updated_ts = current_timestamp()
                WHERE connection_id = {escape_string_literal(connection_id)}
                  AND old_source_table_id = {escape_string_literal(old_id)}
                  AND new_source_table_id = {escape_string_literal(new_id)}
            """)
            rows_migrated += 1
        except Exception as exc:
            safe_message = failcls.sanitize_message(exc)[:400]
            errors.append(_safe_error(
                "migration_execution", exc, connection_id, old_id))
            try:
                spark.sql(f"""
                    UPDATE {ctrl('source_table_identity_migration')}
                    SET migration_status = 'PARTIAL',
                        migration_message =
                            {escape_string_literal(safe_message)},
                        updated_ts = current_timestamp()
                    WHERE connection_id =
                          {escape_string_literal(connection_id)}
                      AND old_source_table_id =
                          {escape_string_literal(old_id)}
                      AND new_source_table_id =
                          {escape_string_literal(new_id)}
                """)
            except Exception as status_exc:
                errors.append(_safe_error(
                    "migration_status_update", status_exc,
                    connection_id, old_id))
            break

    if not errors:
        duplicate_owners = spark.sql(f"""
            SELECT count(*) AS c FROM (
              SELECT connection_id, source_table_id
              FROM {ctrl('source_table_control')}
              GROUP BY connection_id, source_table_id HAVING count(*) > 1
            )
        """).collect()[0]["c"]
        if duplicate_owners:
            errors.append(_safe_error(
                "final_verification",
                ValueError(
                    f"{duplicate_owners} duplicate table ownership key(s) remain")))
        for mapping in ready_batch:
            connection_id = mapping["connection_id"]
            old_id = mapping["old_source_table_id"]
            new_id = mapping["new_source_table_id"]
            owner_count = spark.sql(f"""
                SELECT count(*) AS c
                FROM {ctrl('source_table_control')}
                WHERE connection_id = {escape_string_literal(connection_id)}
                  AND source_table_id = {escape_string_literal(new_id)}
                  AND source_identity_version = {SOURCE_IDENTITY_VERSION}
            """).collect()[0]["c"]
            if owner_count != 1:
                errors.append(_safe_error(
                    "final_verification",
                    ValueError(
                        f"expected one migrated owner row; found {owner_count}"),
                    connection_id, old_id))
                continue
            for table_name in child_tables:
                remaining = spark.sql(f"""
                    SELECT count(*) AS c FROM {ctrl(table_name)}
                    WHERE connection_id = {escape_string_literal(connection_id)}
                      AND source_table_id = {escape_string_literal(old_id)}
                """).collect()[0]["c"]
                if remaining:
                    errors.append(_safe_error(
                        "final_verification",
                        ValueError(
                            f"{table_name} retains {remaining} legacy child row(s)"),
                        connection_id, old_id))

    if errors:
        result = {
            "status": "PARTIAL" if rows_migrated > 0 else "FAILED",
            "business_status": "PARTIAL" if rows_migrated > 0 else "FAILED",
            "dry_run": False,
            "migration_id": migration_id,
            "total_unmigrated_count": total_unmigrated_count,
            "batch_candidate_count": batch_candidate_count,
            "rows_examined": rows_examined,
            "rows_ready": rows_ready,
            "rows_blocked": rows_blocked,
            "rows_already_migrated": rows_already_migrated,
            "rows_migrated": rows_migrated,
            "remaining_unmigrated_count": max(0, total_unmigrated_count - rows_migrated),
            "child_rows_updated": child_rows_updated,
            "error_count": len(errors),
            "errors": errors[:ERROR_LIMIT],
        }
        print(json.dumps(result))
        raise RuntimeError(
            "Identity v2 migration stopped after a partial table update")

remaining_ready = rows_ready - rows_migrated if not dry_run else rows_ready
remaining_unmigrated_count = (
    max(0, total_unmigrated_count - rows_migrated) if not dry_run
    else max(0, total_unmigrated_count - rows_ready)
)

if dry_run:
    if rows_blocked:
        business_status = "BLOCKED"
        status = "FAILED"
    elif total_unmigrated_count == 0:
        business_status = "COMPLETE"
        status = "SUCCEEDED"
    elif remaining_unmigrated_count > 0:
        business_status = "MORE_WORK_REMAINS"
        status = "SUCCEEDED"
    else:
        business_status = "DRY_RUN_COMPLETE"
        status = "SUCCEEDED"
else:
    if errors:
        business_status = "PARTIAL" if rows_migrated > 0 else "FAILED"
        status = "PARTIAL" if rows_migrated > 0 else "FAILED"
    elif rows_blocked:
        business_status = "BLOCKED"
        status = "FAILED"
    elif remaining_unmigrated_count > 0:
        business_status = "MORE_WORK_REMAINS"
        status = "PARTIAL" if not dry_run and remaining_ready > 0 else "SUCCEEDED"
    else:
        business_status = "COMPLETE"
        status = "SUCCEEDED"

result = {
    "status": status,
    "business_status": business_status,
    "dry_run": dry_run,
    "migration_id": migration_id,
    "total_unmigrated_count": total_unmigrated_count,
    "batch_candidate_count": batch_candidate_count,
    "rows_examined": rows_examined,
    "rows_ready": rows_ready,
    "rows_blocked": rows_blocked,
    "rows_already_migrated": rows_already_migrated,
    "rows_migrated": rows_migrated,
    "remaining_unmigrated_count": remaining_unmigrated_count,
    "child_rows_updated": child_rows_updated,
    "error_count": len(errors),
    "errors": errors[:ERROR_LIMIT],
}
dbutils.notebook.exit(json.dumps(result))