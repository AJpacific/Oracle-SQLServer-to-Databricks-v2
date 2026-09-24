# Databricks notebook source
# MAGIC %md
# MAGIC # NB01B_RegisterSelectedTables
# MAGIC INGEST-pipeline task that registers explicitly selected assessed tables
# MAGIC into `source_table_control`. Only COMPATIBLE and REVIEW tables are
# MAGIC registered; every new row is inactive (`is_active=false`,
# MAGIC `current_status=REGISTERED`) and must be reviewed/activated separately.
# MAGIC Supports both ASSESSMENT_FLAGS (control-table driven) and legacy WIDGETS modes.
# MAGIC It preserves existing watermark, initial-load, successful-run, and manually
# MAGIC assigned target names, computes the deterministic connection-owned
# MAGIC `source_table_id`, and blocks target-name collisions before the MERGE.

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

dbutils.widgets.dropdown("selection_mode", "ASSESSMENT_FLAGS",
                         ["ASSESSMENT_FLAGS", "WIDGETS"])
dbutils.widgets.text("assessment_id", "")
dbutils.widgets.text("connection_id", "")
dbutils.widgets.text("run_id", "")
dbutils.widgets.dropdown("include_failed_retries", "false", ["true", "false"])
dbutils.widgets.text("selected_schemas", "")
dbutils.widgets.text("selected_tables", "")
dbutils.widgets.text("target_catalog", "")
dbutils.widgets.dropdown("target_schema_mode", "SOURCE_SCHEMA",
                         ["SOURCE_SCHEMA", "PREFIX_WITH_DATABASE", "EXPLICIT"])
dbutils.widgets.text("target_schema", "")

selection_mode = dbutils.widgets.get("selection_mode").strip().upper() or "ASSESSMENT_FLAGS"
assessment_id = dbutils.widgets.get("assessment_id").strip()
connection_id = dbutils.widgets.get("connection_id").strip()
run_id = dbutils.widgets.get("run_id").strip() or get_run_id()
batch_attempt_id = new_run_id("batch_attempt")
include_failed_retries = dbutils.widgets.get("include_failed_retries").strip().lower() in ("true", "1", "yes")

if not assessment_id or not connection_id:
    raise ValueError("assessment_id and connection_id are required")

repo = control_repo()
connection = require_valid_connection(connection_id)
connection_data = connection.asDict()

if selection_mode == "ASSESSMENT_FLAGS":
    target_config = repo.resolve_target_config(connection_id)
    target_config_id = target_config["config_id"]
    target_catalog = target_config["target_catalog"]
    target_schema_mode = target_config["target_schema_mode"]
    explicit_target_schema = target_config["target_schema"]
else:
    target_config_id = None
    target_catalog = dbutils.widgets.get("target_catalog").strip() or CATALOG
    target_schema_mode = dbutils.widgets.get("target_schema_mode").strip()
    explicit_target_schema = dbutils.widgets.get("target_schema").strip()
    if target_schema_mode == "EXPLICIT" and not explicit_target_schema:
        raise ValueError("EXPLICIT target_schema_mode requires target_schema")

def ctrl(t):
    return f"{quote_databricks(CATALOG)}.{quote_databricks(CONTROL_SCHEMA)}.{quote_databricks(t)}"

# COMMAND ----------

# The assessment ID must have one connection owner. Filtering only the table
# rows would otherwise turn a wrong assessment/connection pair into a silent
# no-op.
assessment_owners = spark.sql(f"""
    SELECT DISTINCT connection_id, source_system, source_server, source_database
    FROM {ctrl('source_assessment')}
    WHERE assessment_id = {escape_string_literal(assessment_id)}
      AND connection_id = {escape_string_literal(connection_id)}
""").collect()
if len(assessment_owners) != 1:
    raise ValueError(
        f"assessment_id {assessment_id!r} resolves to "
        f"{len(assessment_owners)} metadata owner(s) for connection_id "
        f"{connection_id!r}; expected exactly one")
assessment_owner = assessment_owners[0]
assert_table_connection_match(assessment_owner, connection_id)
assert_source_identity_match(assessment_owner, connection)

# Read TABLE objects for the exact assessment + connection; register only those
# explicitly selected and only when COMPATIBLE or REVIEW.
assessed = spark.sql(f"""
    SELECT connection_id, assessment_id, source_schema, object_name, source_system, source_server,
           source_database, compatibility_status, is_selected, selection_status
    FROM {ctrl('source_assessment')}
    WHERE assessment_id = {escape_string_literal(assessment_id)}
      AND connection_id = {escape_string_literal(connection_id)}
      AND object_type = 'TABLE'
""").collect()
print(f"Assessed tables in scope: {len(assessed)}")

# COMMAND ----------

selected_schemas = {s.strip() for s in dbutils.widgets.get("selected_schemas").split(",") if s.strip()}
selected_tables = {s.strip() for s in dbutils.widgets.get("selected_tables").split(",") if s.strip()}

def _selected(schema, obj, is_sel, sel_status):
    if selection_mode == "ASSESSMENT_FLAGS":
        return is_assessment_selection_candidate(
            is_selected=is_sel,
            selection_status=sel_status,
            include_failed_retries=include_failed_retries,
        )
    else:
        if selected_tables:
            return f"{schema}.{obj}" in selected_tables or obj in selected_tables
        if selected_schemas:
            return schema in selected_schemas
        return False

def _target_schema(schema, database):
    if target_schema_mode == "EXPLICIT":
        return validate_identifier(explicit_target_schema)
    if target_schema_mode == "PREFIX_WITH_DATABASE":
        norm_db = normalize_target_identifier(database, identifier_type="schema")
        norm_sch = normalize_target_identifier(schema, identifier_type="schema")
        return f"{norm_db}_{norm_sch}"
    return normalize_target_identifier(schema, identifier_type="schema")

# Check overlapping assessment selections across assessment IDs in ASSESSMENT_FLAGS mode
if selection_mode == "ASSESSMENT_FLAGS":
    overlaps = repo.check_overlapping_selected_assessments(
        source_system=connection_data.get("source_system"),
        only_connection_ids=[connection_id],
        include_failed_retries=include_failed_retries,
    )
    if overlaps:
        sample = overlaps[0]
        db_part = f"{sample['source_database']}." if sample.get("source_database") else ""
        conflict_msg = (
            f"AMBIGUOUS_SELECTED_ASSESSMENT: Table {db_part}{sample['source_schema']}.{sample['object_name']} "
            f"in connection {connection_id!r} is selected in {sample['conflicting_assessment_count']} "
            f"assessment IDs. Operator must deselect obsolete rows before onboarding."
        )
        raise ValueError(conflict_msg)

candidates = []
skipped = []
for r in assessed:
    assert_source_identity_match(r, connection)

    row_dict = r.asDict(recursive=True)
    schema = row_dict["source_schema"]
    obj = row_dict["object_name"]

    is_sel = row_dict.get("is_selected")
    sel_st = row_dict.get("selection_status")
    if not _selected(schema, obj, is_sel, sel_st):
        continue
    comp = r["compatibility_status"]
    if comp not in ("COMPATIBLE", "REVIEW"):
        skipped.append((schema, obj, f"not registerable ({comp})"))
        continue
    src_system = require_source_system(
        connection_data.get("source_system"), "registered connection")
    src_server = connection_data.get("source_server")
    src_db = resolve_effective_source_database(row_dict, connection_data)
    sid = compute_source_table_id(
        connection_id, src_system, src_server,
        src_db, schema, obj)
    t_schema = _target_schema(schema, src_db)
    t_table = normalize_target_identifier(obj, identifier_type="table")
    candidates.append({
        "source_table_id": sid, "connection_id": connection_id,
        "source_system": src_system, "source_server": src_server,
        "source_database": src_db, "source_schema": schema, "source_table": obj,
        "target_catalog": target_catalog, "target_schema": t_schema,
        "target_table": t_table, "mapping_status": comp,
        "target_fqn": f"{normalize_target_component(target_catalog)}.{normalize_target_component(t_schema)}.{normalize_target_component(t_table)}",
    })

candidate_databases = {c["source_database"] for c in candidates if c.get("source_database")}
conn_db = str(connection_data.get("source_database") or "").strip()
if not conn_db and len(candidate_databases) > 1 and target_schema_mode == "SOURCE_SCHEMA":
    raise ValueError(
        "MULTI_DATABASE_TARGET_MODE_REQUIRED: "
        "PREFIX_WITH_DATABASE or an explicit collision-free target strategy is "
        "required when one connection assesses multiple databases."
    )

print(f"Selected {len(candidates)} table(s); skipped {len(skipped)}.")

# COMMAND ----------

# ---- collision detection BEFORE any MERGE ----------------------------------
# Intentionally retained batch collection: finding target owners in a single query
# avoids executing N separate Spark queries for N candidate tables.
# 1) Two selected tables resolving to the same target FQN.
from collections import Counter
fqn_counts = Counter(c["target_fqn"] for c in candidates)
internal_collisions = {f for f, n in fqn_counts.items() if n > 1}

# 2) An existing non-retired registration using the same target FQN,
#    plus the existing connection_id so a conflicting one can be blocked.
existing = spark.sql(f"""
    SELECT source_table_id, connection_id,
           lower(concat_ws('.', target_catalog, target_schema, target_table)) AS target_fqn,
           is_active, current_status
    FROM {ctrl('source_table_control')}
    WHERE target_catalog IS NOT NULL AND trim(target_catalog) <> ''
      AND target_schema IS NOT NULL AND trim(target_schema) <> ''
      AND target_table IS NOT NULL AND trim(target_table) <> ''
      AND coalesce(current_status, '') NOT IN ('RETIRED', 'DECOMMISSIONED')
""").collect()
existing_fqn = {}
for e in existing:
    existing_fqn.setdefault(e["target_fqn"], set()).add(
        (e["connection_id"], e["source_table_id"]))

# 3) Existing registrations for this exact connection to protect stored targets (immutability)
existing_for_conn = spark.sql(f"""
    SELECT source_table_id, connection_id, target_catalog, target_schema, target_table,
           is_active, current_status
    FROM {ctrl('source_table_control')}
    WHERE connection_id = {escape_string_literal(connection_id)}
""").collect()
existing_conn_map = {
    r["source_table_id"]: r.asDict(recursive=True)
    for r in existing_for_conn
}

valid = []
already_registered = []
conflicts = []
errors = []
target_conflict_count = 0
target_config_changed_count = 0

for c in candidates:
    schema = c["source_schema"]
    table = c["source_table"]
    sid = c["source_table_id"]

    if c["target_fqn"] in internal_collisions:
        msg = f"target collision within selection: {c['target_fqn']}"
        skipped.append((schema, table, msg))
        conflicts.append(c["target_fqn"])
        errors.append(f"{schema}.{table}: {msg}")
        target_conflict_count += 1
        if selection_mode == "ASSESSMENT_FLAGS":
            succ = repo.mark_assessment_preclaim_failed(
                connection_id=connection_id,
                assessment_id=assessment_id,
                source_schema=schema,
                object_name=table,
                error=msg,
                allow_existing_failed=include_failed_retries,
                source_database=c["source_database"],
            )
            if not succ:
                print(f"[info] Pre-claim failure state not recorded for {schema}.{table} (already claimed, completed, terminal, or missing)")
        continue

    owners = existing_fqn.get(c["target_fqn"], set())
    if owners - {(connection_id, c["source_table_id"])}:
        msg = f"target collision with existing row: {c['target_fqn']}"
        skipped.append((schema, table, msg))
        conflicts.append(c["target_fqn"])
        errors.append(f"{schema}.{table}: {msg}")
        target_conflict_count += 1
        if selection_mode == "ASSESSMENT_FLAGS":
            succ = repo.mark_assessment_preclaim_failed(
                connection_id=connection_id,
                assessment_id=assessment_id,
                source_schema=schema,
                object_name=table,
                error=msg,
                allow_existing_failed=include_failed_retries,
                source_database=c["source_database"],
            )
            if not succ:
                print(f"[info] Pre-claim failure state not recorded for {schema}.{table} (already claimed, completed, terminal, or missing)")
        continue

    # Protect stored target for existing registrations
    existing_reg = existing_conn_map.get(sid)
    if existing_reg is not None:
        reg_cat = normalize_target_component(existing_reg.get("target_catalog"))
        reg_sch = normalize_target_component(existing_reg.get("target_schema"))
        reg_tbl = normalize_target_component(existing_reg.get("target_table"))
        prop_cat = normalize_target_component(c["target_catalog"])
        prop_sch = normalize_target_component(c["target_schema"])
        prop_tbl = normalize_target_component(c["target_table"])

        if (reg_cat, reg_sch, reg_tbl) == (prop_cat, prop_sch, prop_tbl):
            already_registered.append(c)
        else:
            msg = (
                f"TARGET_CONFIG_CHANGED: Existing registration uses "
                f"{reg_cat}.{reg_sch}.{reg_tbl}, but current config proposes "
                f"{prop_cat}.{prop_sch}.{prop_tbl}. Target remains immutable."
            )
            skipped.append((schema, table, msg))
            conflicts.append(f"{reg_cat}.{reg_sch}.{reg_tbl} != {prop_cat}.{prop_sch}.{prop_tbl}")
            errors.append(f"{schema}.{table}: {msg}")
            target_config_changed_count += 1
            if selection_mode == "ASSESSMENT_FLAGS":
                succ = repo.mark_assessment_preclaim_failed(
                    connection_id=connection_id,
                    assessment_id=assessment_id,
                    source_schema=schema,
                    object_name=table,
                    error=msg,
                    allow_existing_failed=include_failed_retries,
                    source_database=c["source_database"],
                )
                if not succ:
                    print(f"[info] Pre-claim failure state not recorded for {schema}.{table} (already claimed, completed, terminal, or missing)")
            continue
    else:
        valid.append(c)

print(f"Registerable after collision check: {len(valid)}; already registered: {len(already_registered)}; "
      f"connection conflicts: {len(conflicts)}")

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    BooleanType,
    IntegerType,
)
registration_schema = StructType([
    StructField("source_table_id", StringType(), False),
    StructField("connection_id", StringType(), False),
    StructField("source_system", StringType(), False),
    StructField("source_server", StringType(), True),
    StructField("source_database", StringType(), True),
    StructField("source_schema", StringType(), False),
    StructField("source_table", StringType(), False),
    StructField("target_catalog", StringType(), False),
    StructField("target_schema", StringType(), False),
    StructField("target_table", StringType(), False),
    StructField("mapping_status", StringType(), True),
    StructField("is_active", BooleanType(), False),
    StructField("current_status", StringType(), False),
    StructField("initial_load_completed", BooleanType(), False),
    StructField("delete_policy", StringType(), False),
    StructField("source_identity_version", IntegerType(), False),
    StructField("legacy_source_table_id", StringType(), True),
])

validated_count = len(valid) + len(already_registered)
claim_acquired_count = 0
claim_conflict_count = 0
registered_count = 0
already_registered_count = 0
registration_failed_count = 0
worklist = []

def _validate_registration_dict(candidate, reg):
    cid = candidate["connection_id"]
    sid = candidate["source_table_id"]
    if reg.get("connection_id") != cid or reg.get("source_table_id") != sid:
        raise RuntimeError(f"Registration identity mismatch for {cid}.{sid}")

    if int(reg.get("source_identity_version") or 0) != SOURCE_IDENTITY_VERSION:
        raise RuntimeError(
            f"Registration identity version mismatch for {cid}.{sid}: "
            f"expected {SOURCE_IDENTITY_VERSION}, got {reg.get('source_identity_version')}"
        )

    exp_db = str(candidate.get("source_database") or "").strip()
    act_db = str(reg.get("source_database") or "").strip()
    if exp_db.casefold() != act_db.casefold():
        raise RuntimeError(
            f"Registration source_database mismatch for {cid}.{sid}: "
            f"expected {exp_db!r}, got {act_db!r}"
        )

    exp_cat = normalize_target_component(candidate.get("target_catalog"))
    exp_sch = normalize_target_component(candidate.get("target_schema"))
    exp_tbl = normalize_target_component(candidate.get("target_table"))

    act_cat = normalize_target_component(reg.get("target_catalog"))
    act_sch = normalize_target_component(reg.get("target_schema"))
    act_tbl = normalize_target_component(reg.get("target_table"))

    if (act_cat, act_sch, act_tbl) != (exp_cat, exp_sch, exp_tbl):
        raise RuntimeError(
            f"TARGET_CONFIG_CHANGED_AFTER_CLAIM: expected target ({exp_cat}.{exp_sch}.{exp_tbl}) "
            f"does not match registered target ({act_cat}.{act_sch}.{act_tbl})"
        )

def _verify_exact_registration(candidate):
    cid = candidate["connection_id"]
    sid = candidate["source_table_id"]
    v_rows = spark.sql(f"""
        SELECT connection_id, source_table_id, source_identity_version,
               source_database,
               target_catalog, target_schema, target_table
        FROM {ctrl('source_table_control')}
        WHERE connection_id = {escape_string_literal(cid)}
          AND source_table_id = {escape_string_literal(sid)}
    """).collect()
    if not v_rows:
        raise RuntimeError(f"Registration verification failed: no registration found for {cid}.{sid}")
    if len(v_rows) > 1:
        raise RuntimeError(f"Registration verification failed: duplicate registrations found for {cid}.{sid}")

    reg = v_rows[0].asDict() if hasattr(v_rows[0], "asDict") else dict(v_rows[0])
    _validate_registration_dict(candidate, reg)

if selection_mode == "ASSESSMENT_FLAGS":
    # 1. Acquire atomic claims per table to preserve strict attempt ownership
    claimed_valid = []
    for c in valid:
        schema = c["source_schema"]
        table = c["source_table"]
        sid = c["source_table_id"]
        attempt_id = new_run_id("attempt")
        try:
            claim_res = repo.claim_assessment_selection_row(
                connection_id, assessment_id, schema, table, run_id, attempt_id,
                allow_failed_retry=include_failed_retries,
                source_database=c["source_database"]
            )
            if not claim_res.acquired:
                claim_conflict_count += 1
                msg = f"claim conflict: {claim_res.reason or 'could not acquire claim'}"
                skipped.append((schema, table, msg))
                errors.append(f"{schema}.{table}: {msg}")
                continue

            claim_acquired_count += 1
            claimed_valid.append((c, attempt_id))
        except Exception as exc:
            registration_failed_count += 1
            safe_err = failcls.sanitize_message(exc)
            errors.append(f"{schema}.{table}: {safe_err}")

    claimed_already_registered = []
    for c in already_registered:
        schema = c["source_schema"]
        table = c["source_table"]
        sid = c["source_table_id"]
        attempt_id = new_run_id("attempt")
        try:
            claim_res = repo.claim_assessment_selection_row(
                connection_id, assessment_id, schema, table, run_id, attempt_id,
                allow_failed_retry=include_failed_retries,
                source_database=c["source_database"]
            )
            if not claim_res.acquired:
                claim_conflict_count += 1
                msg = f"claim conflict: {claim_res.reason or 'could not acquire claim'}"
                skipped.append((schema, table, msg))
                errors.append(f"{schema}.{table}: {msg}")
                continue

            claim_acquired_count += 1
            claimed_already_registered.append((c, attempt_id))
        except Exception as exc:
            registration_failed_count += 1
            safe_err = failcls.sanitize_message(exc)
            errors.append(f"{schema}.{table}: {safe_err}")

    # 2. Batch insert all claimed valid registrations into source_table_control via single MERGE
    if claimed_valid:
        reg_rows = [Row(
            source_table_id=c["source_table_id"], connection_id=c["connection_id"],
            source_system=c["source_system"], source_server=c["source_server"],
            source_database=c["source_database"], source_schema=c["source_schema"],
            source_table=c["source_table"], target_catalog=c["target_catalog"],
            target_schema=c["target_schema"], target_table=c["target_table"],
            mapping_status=c["mapping_status"],
            is_active=False,
            current_status="REGISTERED",
            initial_load_completed=False,
            delete_policy="IGNORE_DELETES",
            source_identity_version=SOURCE_IDENTITY_VERSION,
            legacy_source_table_id=None
        ) for c, _ in claimed_valid]
        src_df = (
            spark.createDataFrame(
                reg_rows,
                schema=registration_schema,
            )
            .withColumn("created_ts", F.current_timestamp())
            .withColumn("updated_ts", F.current_timestamp())
        )
        src_df.createOrReplaceTempView("_batch_register_rows")
        spark.sql(f"""
            MERGE INTO {ctrl('source_table_control')} t
            USING _batch_register_rows s
            ON t.connection_id = s.connection_id
           AND t.source_table_id = s.source_table_id
            WHEN NOT MATCHED THEN INSERT (
                source_table_id, connection_id, source_system, source_server,
                source_database, source_schema, source_table, target_catalog,
                target_schema, target_table, mapping_status, is_active,
                current_status, initial_load_completed, delete_policy,
                source_identity_version, legacy_source_table_id,
                created_ts, updated_ts
            ) VALUES (
                s.source_table_id, s.connection_id, s.source_system, s.source_server,
                s.source_database, s.source_schema, s.source_table, s.target_catalog,
                s.target_schema, s.target_table, s.mapping_status, s.is_active,
                s.current_status, s.initial_load_completed, s.delete_policy,
                s.source_identity_version, s.legacy_source_table_id,
                s.created_ts, s.updated_ts
            )
        """)

    # 3. Batch verify exact registrations in source_table_control
    all_claimed = claimed_valid + claimed_already_registered
    v_rows_map = {}
    if all_claimed:
        claimed_sids = [c["source_table_id"] for c, _ in all_claimed]
        sids_in = ", ".join(escape_string_literal(sid) for sid in claimed_sids)
        v_rows = spark.sql(f"""
            SELECT connection_id, source_table_id, source_identity_version,
                   source_database, target_catalog, target_schema, target_table
            FROM {ctrl('source_table_control')}
            WHERE connection_id = {escape_string_literal(connection_id)}
              AND source_table_id IN ({sids_in})
        """).collect()
        for vr in v_rows:
            vr_dict = vr.asDict() if hasattr(vr, "asDict") else dict(vr)
            v_rows_map[vr_dict["source_table_id"]] = vr_dict

    for c, attempt_id in claimed_valid:
        schema = c["source_schema"]
        table = c["source_table"]
        sid = c["source_table_id"]
        try:
            reg = v_rows_map.get(sid)
            if not reg:
                _verify_exact_registration(c)
            else:
                _validate_registration_dict(c, reg)

            succ = repo.mark_assessment_registration_succeeded(
                connection_id, assessment_id, schema, table, run_id, attempt_id,
                source_database=c["source_database"]
            )
            if not succ:
                raise RuntimeError(f"Failed to mark assessment row REGISTERED for {schema}.{table}")

            registered_count += 1
            worklist.append({"connection_id": connection_id, "source_table_id": sid})
        except Exception as exc:
            registration_failed_count += 1
            safe_err = failcls.sanitize_message(exc)
            errors.append(f"{schema}.{table}: {safe_err}")
            try:
                repo.mark_assessment_onboarding_failed(
                    connection_id, assessment_id, schema, table,
                    run_id, attempt_id, failed_stage="REGISTRATION", error=safe_err,
                    source_database=c["source_database"]
                )
            except Exception as state_exc:
                print(f"[warn] Failed to set FAILED state on {schema}.{table}: {failcls.sanitize_message(state_exc)}")

    for c, attempt_id in claimed_already_registered:
        schema = c["source_schema"]
        table = c["source_table"]
        sid = c["source_table_id"]
        try:
            reg = v_rows_map.get(sid)
            if not reg:
                _verify_exact_registration(c)
            else:
                _validate_registration_dict(c, reg)

            succ = repo.mark_assessment_registration_succeeded(
                connection_id, assessment_id, schema, table, run_id, attempt_id,
                source_database=c["source_database"]
            )
            if not succ:
                raise RuntimeError(f"Failed to mark assessment row REGISTERED for {schema}.{table}")

            already_registered_count += 1
            worklist.append({"connection_id": connection_id, "source_table_id": sid})
        except Exception as exc:
            registration_failed_count += 1
            safe_err = failcls.sanitize_message(exc)
            errors.append(f"{schema}.{table}: {safe_err}")
            try:
                repo.mark_assessment_onboarding_failed(
                    connection_id, assessment_id, schema, table,
                    run_id, attempt_id, failed_stage="REGISTRATION", error=safe_err,
                    source_database=c["source_database"]
                )
            except Exception as state_exc:
                print(f"[warn] Failed to set FAILED state on {schema}.{table}: {failcls.sanitize_message(state_exc)}")

else:
    # Legacy WIDGETS mode: preserves existing batch registration and selection marking
    if valid:
        reg_rows = [Row(
            source_table_id=c["source_table_id"],
            connection_id=c["connection_id"],
            source_system=c["source_system"],
            source_server=c["source_server"],
            source_database=c["source_database"],
            source_schema=c["source_schema"],
            source_table=c["source_table"],
            target_catalog=c["target_catalog"],
            target_schema=c["target_schema"],
            target_table=c["target_table"],
            mapping_status=c["mapping_status"],
            is_active=False,
            current_status="REGISTERED",
            initial_load_completed=False,
            delete_policy="IGNORE_DELETES",
            source_identity_version=SOURCE_IDENTITY_VERSION,
            legacy_source_table_id=None,
        ) for c in valid]

        src_df = (
            spark.createDataFrame(
                reg_rows,
                schema=registration_schema,
            )
            .withColumn("created_ts", F.current_timestamp())
            .withColumn("updated_ts", F.current_timestamp())
        )
        src_df.createOrReplaceTempView("_register_rows")
        spark.sql(f"""
            MERGE INTO {ctrl('source_table_control')} t
            USING _register_rows s
            ON t.connection_id = s.connection_id
           AND t.source_table_id = s.source_table_id
            WHEN NOT MATCHED THEN INSERT (
                source_table_id, connection_id, source_system, source_server,
                source_database, source_schema, source_table, target_catalog,
                target_schema, target_table, mapping_status, is_active,
                current_status, initial_load_completed, delete_policy,
                source_identity_version, legacy_source_table_id,
                created_ts, updated_ts
            ) VALUES (
                s.source_table_id, s.connection_id, s.source_system, s.source_server,
                s.source_database, s.source_schema, s.source_table, s.target_catalog,
                s.target_schema, s.target_table, s.mapping_status, s.is_active,
                s.current_status, s.initial_load_completed, s.delete_policy,
                s.source_identity_version, s.legacy_source_table_id,
                s.created_ts, s.updated_ts
            )
        """)
        registered_count = len(valid)

    already_registered_count = len(already_registered)
    all_registered = valid + already_registered
    if all_registered:
        sel_rows = [Row(assessment_id=assessment_id, connection_id=connection_id,
                        source_database=c["source_database"],
                        source_schema=c["source_schema"], object_name=c["source_table"])
                    for c in all_registered]
        spark.createDataFrame(sel_rows).createOrReplaceTempView("_selected_objects")
        spark.sql(f"""
            MERGE INTO {ctrl('source_assessment')} t
            USING _selected_objects s
              ON t.assessment_id = s.assessment_id
             AND t.connection_id = s.connection_id
             AND t.source_database = s.source_database
             AND t.source_schema = s.source_schema
             AND t.object_name  = s.object_name
             AND t.object_type  = 'TABLE'
            WHEN MATCHED THEN UPDATE SET t.is_selected = true
        """)
    for c in all_registered:
        worklist.append({"connection_id": connection_id,
                         "source_table_id": c["source_table_id"]})

# COMMAND ----------

selected_count = len(candidates)
skipped_count = len(skipped)
conflict_count = len(conflicts)
failed_count = len(errors)
processed_count = registered_count + already_registered_count
remaining_selected_count = max(0, selected_count - processed_count)
incomplete_count = remaining_selected_count

if selected_count == 0:
    business_status = "NO_SELECTION"
elif incomplete_count == 0:
    business_status = "COMPLETE"
elif processed_count == 0:
    business_status = "BLOCKED"
else:
    business_status = "PARTIAL"

execution_status = "SUCCEEDED" if incomplete_count == 0 else "FAILED"

for s in skipped:
    print("  SKIP", s)

validate_task_value_payload(worklist, key="worklist")

try:
    dbutils.jobs.taskValues.set(key="run_id", value=run_id)
    dbutils.jobs.taskValues.set(key="connection_id", value=connection_id)
    dbutils.jobs.taskValues.set(key="assessment_id", value=assessment_id)
    dbutils.jobs.taskValues.set(key="status", value=execution_status)
    dbutils.jobs.taskValues.set(key="business_status", value=business_status)
    dbutils.jobs.taskValues.set(key="selected_count", value=selected_count)
    dbutils.jobs.taskValues.set(key="validated_count", value=validated_count)
    dbutils.jobs.taskValues.set(key="claimed_count", value=claim_acquired_count)
    dbutils.jobs.taskValues.set(key="registered_count", value=registered_count)
    dbutils.jobs.taskValues.set(key="already_registered_count", value=already_registered_count)
    dbutils.jobs.taskValues.set(key="failed_count", value=failed_count)
    dbutils.jobs.taskValues.set(key="skipped_count", value=skipped_count)
    dbutils.jobs.taskValues.set(key="conflict_count", value=conflict_count)
except Exception:
    pass

bounded_errors = [
    str(error)[:500]
    for error in errors[:20]
]

exit_payload = {
    "status": execution_status,
    "business_status": business_status,
    "run_id": run_id,
    "batch_attempt_id": batch_attempt_id,
    "assessment_id": assessment_id,
    "connection_id": connection_id,
    "selection_mode": selection_mode,
    "target_config_id": target_config_id,
    "selected_count": selected_count,
    "processed_count": processed_count,
    "incomplete_count": incomplete_count,
    "validated_count": validated_count,
    "claim_acquired_count": claim_acquired_count,
    "claim_conflict_count": claim_conflict_count,
    "registered_count": registered_count,
    "already_registered_count": already_registered_count,
    "registration_failed_count": registration_failed_count,
    "target_conflict_count": target_conflict_count,
    "target_config_changed_count": target_config_changed_count,
    "remaining_selected_count": remaining_selected_count,
    "skipped_count": skipped_count,
    "conflict_count": conflict_count,
    "failed_count": failed_count,
    "errors": bounded_errors,
    "registered": registered_count,
    "skipped": skipped_count,
    "conflicts": conflicts,
    "worklist": worklist,
}

payload_str = json.dumps(exit_payload)
print(f"Registration outcome: {payload_str}")

if incomplete_count > 0:
    raise RuntimeError(
        f"Registration incomplete: {incomplete_count} table(s) could not be registered "
        f"(business_status={business_status}, failed={registration_failed_count}, "
        f"claim_conflicts={claim_conflict_count}, target_conflicts={target_conflict_count}, "
        f"config_changed={target_config_changed_count})"
    )

dbutils.notebook.exit(payload_str)