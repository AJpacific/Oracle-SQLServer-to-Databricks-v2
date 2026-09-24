# Databricks notebook source
# MAGIC %md
# MAGIC # NB_RepairInvalidTargetIdentifiers
# MAGIC Administrative deployment notebook for repairing invalid or pre-normalized
# MAGIC target identifiers in `source_table_control`.
# MAGIC 
# MAGIC Invariant protections:
# MAGIC - Defaults to dry_run=true (performs zero mutations).
# MAGIC - Never modifies PROVISIONED rows or tables with initial_load_completed = true.
# MAGIC - Never modifies RETIRED or DECOMMISSIONED rows.
# MAGIC - Never modifies source identity fields, source_table_id, or target_catalog.
# MAGIC - Preserves explicit configured target schemas in EXPLICIT mode.
# MAGIC - Detects target collisions against other batch candidates, existing registrations,
# MAGIC   and Unity Catalog tables.
# MAGIC - Successful repair updates target_schema and target_table, clears error_message,
# MAGIC   and sets current_status to 'READY_FOR_PROVISIONING'.

# COMMAND ----------

# MAGIC %run ../shared/_common

# COMMAND ----------

import json
from collections import Counter
from datetime import datetime, timezone

dbutils.widgets.dropdown("dry_run", "true", ["true", "false"])
dbutils.widgets.text("catalog", "da_accelerators")
dbutils.widgets.text("control_schema", "control")
dbutils.widgets.text("only_connection_ids", "")
dbutils.widgets.text("only_source_table_ids", "")
dbutils.widgets.text("batch_size", "0")

dry_run = dbutils.widgets.get("dry_run").strip().lower() in ("true", "1", "yes")
catalog = dbutils.widgets.get("catalog").strip() or CATALOG
control_schema = dbutils.widgets.get("control_schema").strip() or CONTROL_SCHEMA
only_connection_ids_raw = dbutils.widgets.get("only_connection_ids").strip()
only_source_table_ids_raw = dbutils.widgets.get("only_source_table_ids").strip()

try:
    batch_size = int(dbutils.widgets.get("batch_size").strip() or "0")
except ValueError:
    batch_size = 0

only_connection_ids = [
    c.strip() for c in only_connection_ids_raw.split(",") if c.strip()
] if only_connection_ids_raw else []

only_source_table_ids = [
    s.strip() for s in only_source_table_ids_raw.split(",") if s.strip()
] if only_source_table_ids_raw else []

repair_run_id = f"repair_{now_utc().strftime('%Y%m%d_%H%M%S')}"

repo = ControlRepository(spark, catalog=catalog, control_schema=control_schema)

def ctrl(t):
    return f"{quote_databricks(catalog)}.{quote_databricks(control_schema)}.{quote_databricks(t)}"

print(
    f"Starting target identifier repair: dry_run={dry_run}, "
    f"catalog={catalog}, control_schema={control_schema}, "
    f"batch_size={batch_size}, run_id={repair_run_id}"
)

# COMMAND ----------

# Query candidates scoped strictly to pre-provision failures
allowed_failure_statuses = ("PROVISION_FAILED", "PROVISION_CONFIG_ERROR")
status_filter = ", ".join(escape_string_literal(s) for s in allowed_failure_statuses)

conn_filter = ""
if only_connection_ids:
    in_conns = ", ".join(escape_string_literal(c) for c in only_connection_ids)
    conn_filter = f"AND connection_id IN ({in_conns})"

table_filter = ""
if only_source_table_ids:
    in_tables = ", ".join(escape_string_literal(t) for t in only_source_table_ids)
    table_filter = f"AND source_table_id IN ({in_tables})"

candidate_sql = f"""
    SELECT connection_id, source_table_id, source_system,
           source_server, source_database, source_schema, source_table,
           target_catalog, target_schema, target_table,
           target_strategy, current_status, initial_load_completed,
           is_active, error_message, updated_ts
    FROM {ctrl('source_table_control')}
    WHERE coalesce(initial_load_completed, false) = false
      AND coalesce(current_status, '') IN ({status_filter})
      AND coalesce(current_status, '') <> 'PROVISIONED'
      AND coalesce(current_status, '') NOT IN ('RETIRED', 'DECOMMISSIONED')
      {conn_filter}
      {table_filter}
    ORDER BY connection_id, source_table_id
"""

raw_candidates = spark.sql(candidate_sql).collect()
print(f"Found {len(raw_candidates)} candidate row(s) in eligible failure statuses.")

# Query all existing active/non-retired registrations for collision checking
existing_registrations_sql = f"""
    SELECT connection_id, source_table_id,
           target_catalog, target_schema, target_table,
           current_status, is_active
    FROM {ctrl('source_table_control')}
    WHERE coalesce(current_status, '') NOT IN ('RETIRED', 'DECOMMISSIONED')
"""
existing_registrations = spark.sql(existing_registrations_sql).collect()

# Map lower complete target FQN to owners: fqn -> set of (conn_id, src_id)
existing_fqn_owners = {}
for er in existing_registrations:
    cat = (er["target_catalog"] or catalog).strip()
    sch = (er["target_schema"] or "").strip()
    tbl = (er["target_table"] or "").strip()
    if cat and sch and tbl:
        fqn = f"{cat}.{sch}.{tbl}".lower()
        existing_fqn_owners.setdefault(fqn, set()).add(
            (er["connection_id"], er["source_table_id"])
        )

# COMMAND ----------

# Process candidate proposals and evaluate readiness
candidates_evaluated = []
seen_proposed_fqns_in_batch = {}

connection_cache = {}

for r in raw_candidates:
    d = r.asDict() if hasattr(r, "asDict") else dict(r)
    conn_id = d["connection_id"]
    src_id = d["source_table_id"]
    src_sys = d.get("source_system")
    src_db = d.get("source_database")
    src_sch = d.get("source_schema")
    src_tbl = d.get("source_table")
    old_cat = d.get("target_catalog") or catalog
    old_sch = d.get("target_schema")
    old_tbl = d.get("target_table")
    cur_status = d.get("current_status")
    init_done = d.get("initial_load_completed")

    item = {
        "connection_id": conn_id,
        "source_table_id": src_id,
        "source_database": src_db,
        "source_schema": src_sch,
        "source_table": src_tbl,
        "old_target_catalog": old_cat,
        "old_target_schema": old_sch,
        "old_target_table": old_tbl,
        "current_status": cur_status,
        "proposed_target_catalog": old_cat,
        "proposed_target_schema": None,
        "proposed_target_table": None,
        "repair_status": "BLOCKED",
        "repair_message": "",
    }

    # Invariant: Never touch PROVISIONED or loaded tables
    if cur_status == "PROVISIONED" or init_done is True:
        item["repair_status"] = "BLOCKED"
        item["repair_message"] = "Table is PROVISIONED or initial_load_completed is true"
        candidates_evaluated.append(item)
        continue

    # Verify parent connection
    if conn_id not in connection_cache:
        connection_cache[conn_id] = repo.get_connection(conn_id)
    conn = connection_cache[conn_id]
    if conn is None:
        item["repair_status"] = "BLOCKED"
        item["repair_message"] = f"Parent connection {conn_id!r} does not exist"
        candidates_evaluated.append(item)
        continue

    # Verify complete source identity
    if not src_sys or not src_sch or not src_tbl:
        item["repair_status"] = "BLOCKED"
        item["repair_message"] = "Incomplete source identity metadata"
        candidates_evaluated.append(item)
        continue

    # Determine target schema strategy
    strat = (d.get("target_strategy") or "PREFIX_WITH_DATABASE").strip().upper()
    try:
        if strat == "PREFIX_WITH_DATABASE":
            if not src_db:
                norm_sch = normalize_target_identifier(src_sch, identifier_type="schema")
                prop_sch = norm_sch
            else:
                norm_db = normalize_target_identifier(src_db, identifier_type="schema")
                norm_sch = normalize_target_identifier(src_sch, identifier_type="schema")
                prop_sch = f"{norm_db}_{norm_sch}"
        elif strat == "SOURCE_SCHEMA":
            prop_sch = normalize_target_identifier(src_sch, identifier_type="schema")
        elif strat == "EXPLICIT":
            # In EXPLICIT mode, preserve the authoritative configured schema; do not normalize
            explicit_sch = str(old_sch or "").strip()
            if not explicit_sch:
                raise ValueError("EXPLICIT target_strategy requires nonblank target_schema")
            prop_sch = validate_identifier(explicit_sch)
        else:
            raise ValueError(f"Unknown target_strategy: {strat!r}")

        prop_tbl = normalize_target_identifier(src_tbl, identifier_type="table")
        validate_identifier(old_cat)
    except Exception as derive_err:
        item["repair_status"] = "ERROR"
        item["repair_message"] = f"Identifier derivation failed: {failcls.sanitize_message(derive_err)[:200]}"
        candidates_evaluated.append(item)
        continue

    item["proposed_target_schema"] = prop_sch
    item["proposed_target_table"] = prop_tbl

    # Check whether existing stored values already match deterministic proposal and pass validation
    is_schema_valid = False
    try:
        if old_sch and validate_identifier(str(old_sch).strip()) == prop_sch:
            is_schema_valid = True
    except Exception:
        is_schema_valid = False

    is_table_valid = False
    try:
        if old_tbl and validate_identifier(str(old_tbl).strip()) == prop_tbl:
            is_table_valid = True
    except Exception:
        is_table_valid = False

    if is_schema_valid and is_table_valid:
        item["repair_status"] = "UNCHANGED"
        item["repair_message"] = "Target identifiers already match deterministic normalized values"
        candidates_evaluated.append(item)
        continue

    # Collision and ownership safety checks
    proposed_fqn = f"{old_cat}.{prop_sch}.{prop_tbl}".lower()

    # Check 1: Intra-batch collision
    if proposed_fqn in seen_proposed_fqns_in_batch:
        first_conn, first_src = seen_proposed_fqns_in_batch[proposed_fqn]
        item["repair_status"] = "BLOCKED"
        item["repair_message"] = (
            f"TARGET_FQN_COLLISION: proposed target FQN {proposed_fqn} collides with another "
            f"candidate in this batch ({first_conn} / {first_src})"
        )
        candidates_evaluated.append(item)
        continue
    seen_proposed_fqns_in_batch[proposed_fqn] = (conn_id, src_id)

    # Check 2: Collision against existing non-retired registrations
    existing_owners = existing_fqn_owners.get(proposed_fqn, set())
    other_owners = {
        (c, s) for (c, s) in existing_owners
        if not (c == conn_id and s == src_id)
    }
    if other_owners:
        owner_desc = ", ".join(f"{c}/{s}" for c, s in sorted(other_owners)[:3])
        item["repair_status"] = "BLOCKED"
        item["repair_message"] = (
            f"TARGET_FQN_COLLISION: proposed target FQN {proposed_fqn} is already registered "
            f"to other table(s): {owner_desc}"
        )
        candidates_evaluated.append(item)
        continue

    # Check 3: Collision against live Unity Catalog tables (fail-closed)
    proposed_target_fqn = f"{old_cat}.{prop_sch}.{prop_tbl}"
    try:
        target_exists = spark.catalog.tableExists(proposed_target_fqn)
    except Exception as exc:
        safe_error = failcls.sanitize_message(exc)[:200]
        item["repair_status"] = "BLOCKED"
        item["repair_message"] = (
            "TARGET_EXISTENCE_CHECK_FAILED: "
            + safe_error
        )
        candidates_evaluated.append(item)
        continue

    if target_exists:
        item["repair_status"] = "BLOCKED"
        item["repair_message"] = (
            "TARGET_FQN_COLLISION: proposed target table "
            f"{proposed_target_fqn} already exists in Unity Catalog "
            "and this registration is not PROVISIONED"
        )
        candidates_evaluated.append(item)
        continue

    item["repair_status"] = "READY"
    item["repair_message"] = "Ready for repair to READY_FOR_PROVISIONING"
    candidates_evaluated.append(item)

# Apply batch_size limit to READY candidates if configured
ready_candidates = [c for c in candidates_evaluated if c["repair_status"] == "READY"]
more_work_remains = False
if batch_size > 0 and len(ready_candidates) > batch_size:
    more_work_remains = True
    candidates_to_execute = ready_candidates[:batch_size]
    # Mark remaining ready candidates as defer
    for deferred in ready_candidates[batch_size:]:
        deferred["repair_status"] = "DEFERRED_BATCH_LIMIT"
        deferred["repair_message"] = f"Deferred by batch_size limit ({batch_size})"
else:
    candidates_to_execute = ready_candidates

# COMMAND ----------

# Display sanitized results
display_cols = [
    "connection_id", "source_table_id", "source_database",
    "source_schema", "source_table", "old_target_catalog",
    "old_target_schema", "old_target_table",
    "proposed_target_catalog", "proposed_target_schema",
    "proposed_target_table", "repair_status", "repair_message"
]

display_rows = []
for c in candidates_evaluated:
    display_rows.append({k: c.get(k) for k in display_cols})

if display_rows:
    display_df = spark.createDataFrame(display_rows)
    display_df.show(len(display_rows), truncate=False)
else:
    print("No candidates examined.")

# COMMAND ----------

# Execution: mutate ONLY when dry_run is false
updated_count = 0
unchanged_count = len([c for c in candidates_evaluated if c["repair_status"] == "UNCHANGED"])
blocked_count = len([c for c in candidates_evaluated if c["repair_status"] in ("BLOCKED", "DEFERRED_BATCH_LIMIT")])
error_count = len([c for c in candidates_evaluated if c["repair_status"] == "ERROR"])
ready_count = len(ready_candidates)
bounded_errors = []

if not dry_run:
    print(f"Executing repair on {len(candidates_to_execute)} READY candidate(s)...")
    now_ts = now_utc().strftime("%Y-%m-%d %H:%M:%S.%f")

    for item in candidates_to_execute:
        conn_id = item["connection_id"]
        src_id = item["source_table_id"]
        prop_sch = item["proposed_target_schema"]
        prop_tbl = item["proposed_target_table"]
        expected_cur_status = item["current_status"]
        old_sch = item["old_target_schema"]
        old_tbl = item["old_target_table"]

        # Optimistic concurrency verification query
        verification_sql = f"""
            SELECT target_schema, target_table, current_status, initial_load_completed
            FROM {ctrl('source_table_control')}
            WHERE connection_id = {escape_string_literal(conn_id)}
              AND source_table_id = {escape_string_literal(src_id)}
        """
        curr = spark.sql(verification_sql).collect()
        if not curr:
            msg = f"Row {conn_id}/{src_id} disappeared concurrently"
            bounded_errors.append(msg)
            error_count += 1
            continue

        c_row = curr[0]
        if (c_row["current_status"] != expected_cur_status
                or c_row["target_schema"] != old_sch
                or c_row["target_table"] != old_tbl
                or c_row["initial_load_completed"] is True):
            msg = f"Row {conn_id}/{src_id} changed concurrently; update aborted"
            bounded_errors.append(msg)
            error_count += 1
            continue

        # Atomic scoped update
        update_sql = f"""
            UPDATE {ctrl('source_table_control')}
            SET target_schema = {escape_string_literal(prop_sch)},
                target_table = {escape_string_literal(prop_tbl)},
                current_status = 'READY_FOR_PROVISIONING',
                error_message = NULL,
                updated_ts = {escape_string_literal(now_ts)}
            WHERE connection_id = {escape_string_literal(conn_id)}
              AND source_table_id = {escape_string_literal(src_id)}
              AND current_status = {escape_string_literal(expected_cur_status)}
              AND coalesce(initial_load_completed, false) = false
        """
        spark.sql(update_sql)

        # Optimistic post-update verification
        verify_after = spark.sql(f"""
            SELECT target_schema, target_table, current_status, error_message
            FROM {ctrl('source_table_control')}
            WHERE connection_id = {escape_string_literal(conn_id)}
              AND source_table_id = {escape_string_literal(src_id)}
        """).collect()

        if (verify_after
                and verify_after[0]["target_schema"] == prop_sch
                and verify_after[0]["target_table"] == prop_tbl
                and verify_after[0]["current_status"] == "READY_FOR_PROVISIONING"
                and verify_after[0]["error_message"] is None):
            updated_count += 1
            print(f"  REPAIRED {conn_id} / {src_id} -> {prop_sch}.{prop_tbl} (READY_FOR_PROVISIONING)")
        else:
            msg = f"Post-update verification failed for {conn_id}/{src_id}"
            bounded_errors.append(msg)
            error_count += 1
else:
    print(f"DRY RUN completed. Zero mutations performed. {ready_count} row(s) ready for repair.")

# COMMAND ----------

summary = {
    "repair_run_id": repair_run_id,
    "dry_run": dry_run,
    "candidates_examined": len(candidates_evaluated),
    "ready_count": ready_count,
    "blocked_count": blocked_count,
    "updated_count": updated_count,
    "unchanged_count": unchanged_count,
    "error_count": error_count,
    "more_work_remains": more_work_remains,
    "bounded_errors": bounded_errors[:50],
}

print("Repair execution summary:")
print(json.dumps(summary, indent=2))

dbutils.notebook.exit(json.dumps(summary))
