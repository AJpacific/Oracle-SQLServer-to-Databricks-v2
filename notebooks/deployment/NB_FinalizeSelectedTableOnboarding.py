# Databricks notebook source
# MAGIC %md
# MAGIC # NB_FinalizeSelectedTableOnboarding
# MAGIC Finalizes selected assessment rows as ONBOARDED after target provisioning completes.
# MAGIC Verifies that each exact REGISTERED candidate has a valid, active, provisioned
# MAGIC registration in source_table_control with collision-free target ownership.
# MAGIC Performs metadata checks only: no source JDBC or credential access.

# COMMAND ----------

# MAGIC %run ../shared/_common

# COMMAND ----------

import json

dbutils.widgets.text("run_id", "")
dbutils.widgets.text("connection_id", "")
dbutils.widgets.text("assessment_id", "")
dbutils.widgets.text("attempt_id", "")
dbutils.widgets.text("catalog", "da_accelerators")
dbutils.widgets.text("control_schema", "control")

run_id = dbutils.widgets.get("run_id").strip()
if not run_id:
    raise ValueError(
        "run_id is required and must be passed from the original Job run context"
    )

connection_id = dbutils.widgets.get("connection_id").strip()
if not connection_id:
    raise ValueError("connection_id is required")

assessment_id = dbutils.widgets.get("assessment_id").strip()
if not assessment_id:
    raise ValueError("assessment_id is required")

attempt_id = dbutils.widgets.get("attempt_id").strip()
catalog = dbutils.widgets.get("catalog").strip() or CATALOG
control_schema = dbutils.widgets.get("control_schema").strip() or CONTROL_SCHEMA

repo = ControlRepository(
    spark,
    catalog=catalog,
    control_schema=control_schema,
)

connection = repo.get_connection(connection_id)
if not connection:
    raise ValueError(f"Connection not found: {connection_id}")

conn_dict = (
    connection.asDict(recursive=True)
    if hasattr(connection, "asDict")
    else dict(connection)
)

if not conn_dict.get("is_active"):
    raise ValueError(f"Connection is inactive: {connection_id}")
if str(conn_dict.get("connection_status") or "").strip().upper() != "VALID":
    raise ValueError(f"Connection status is not VALID: {connection_id} ({conn_dict.get('connection_status')})")
scope_key = "secret" + "_scope"
if not str(conn_dict.get(scope_key) or "").strip():
    raise ValueError(f"Connection {scope_key} is missing: {connection_id}")

def ctrl(t):
    return f"{quote_databricks(catalog)}.{quote_databricks(control_schema)}.{quote_databricks(t)}"

# COMMAND ----------

# Read registered candidate assessment rows owned by the current run
where_clauses = [
    f"sa.connection_id = {escape_string_literal(connection_id)}",
    f"sa.assessment_id = {escape_string_literal(assessment_id)}",
    "sa.object_type = 'TABLE'",
    "sa.is_selected = true",
    "upper(trim(coalesce(sa.selection_status, ''))) = 'REGISTERED'",
    f"sa.onboarding_run_id = {escape_string_literal(run_id)}",
]
if attempt_id:
    where_clauses.append(f"sa.onboarding_attempt_id = {escape_string_literal(attempt_id)}")

candidate_query = f"""
    SELECT sa.source_database, sa.source_schema, sa.object_name, sa.source_system,
           sa.onboarding_run_id, sa.onboarding_attempt_id
    FROM {ctrl('source_assessment')} sa
    WHERE {' AND '.join(where_clauses)}
    ORDER BY sa.source_schema, sa.object_name
"""
candidate_rows = spark.sql(candidate_query).collect()
candidate_count = len(candidate_rows)
print(f"Candidate REGISTERED rows to finalize: {candidate_count}")

# COMMAND ----------

onboarded_count = 0
review_required_count = 0
blocked_count = 0
failed_count = 0
errors = []

for row in candidate_rows:
    row_dict = row.asDict(recursive=True)

    schema = row_dict["source_schema"]
    table = row_dict["object_name"]
    source_sys = (
        row_dict.get("source_system")
        or conn_dict.get("source_system")
    )
    effective_attempt_id = (
        attempt_id
        or row_dict.get("onboarding_attempt_id")
        or None
    )
    effective_database = resolve_effective_source_database(row_dict, conn_dict)

    try:
        # 1. Recompute deterministic source_table_id for identity v2
        expected_sid = compute_source_table_id(
            connection_id=connection_id,
            source_system=source_sys,
            source_server=conn_dict.get("source_server"),
            source_database=effective_database,
            source_schema=schema,
            source_table=table,
        )

        # 2. Resolve source_table_control row
        reg_query = f"""
            SELECT source_table_id, connection_id, source_database, source_identity_version,
                   table_decision, is_active, current_status,
                   target_catalog, target_schema, target_table
            FROM {ctrl('source_table_control')}
            WHERE connection_id = {escape_string_literal(connection_id)}
              AND source_table_id = {escape_string_literal(expected_sid)}
        """
        reg_records = spark.sql(reg_query).collect()
        if not reg_records:
            raise ValueError(f"No source_table_control registration found for {schema}.{table} (id: {expected_sid})")
        if len(reg_records) > 1:
            raise ValueError(f"Multiple source_table_control registrations found for {schema}.{table} (id: {expected_sid})")

        reg = reg_records[0].asDict() if hasattr(reg_records[0], "asDict") else dict(reg_records[0])

        # 3. Verify connection ownership, source_table_id, and identity version
        if reg.get("connection_id") != connection_id:
            raise ValueError(f"Registration connection mismatch for {schema}.{table}")
        if reg.get("source_table_id") != expected_sid:
            raise ValueError(f"Registration source_table_id mismatch for {schema}.{table}")
        if int(reg.get("source_identity_version") or 0) != SOURCE_IDENTITY_VERSION:
            raise ValueError(f"Registration for {schema}.{table} has invalid identity version: {reg.get('source_identity_version')}")
        if effective_database and reg.get("source_database"):
            if reg.get("source_database").strip().lower() != effective_database.strip().lower():
                raise ValueError(
                    f"Registration source_database mismatch for {schema}.{table}: "
                    f"control has {reg.get('source_database')!r}, expected {effective_database!r}"
                )

        decision = str(reg.get("table_decision") or "").upper().strip()

        # Handle non-AUTO_MIGRATE tables: transition to terminal review/blocked state
        if decision != "AUTO_MIGRATE":
            if decision in ("MANUAL_REVIEW", "REVIEW"):
                terminal_status = "REVIEW_REQUIRED"
            else:
                terminal_status = "BLOCKED"

            terminal_message = (
                f"Table decision {decision or 'UNSET'} prevents automated onboarding completion"
            )

            transitioned = repo.mark_assessment_onboarding_terminal(
                connection_id=connection_id,
                assessment_id=assessment_id,
                source_schema=schema,
                object_name=table,
                run_id=run_id,
                attempt_id=effective_attempt_id or None,
                terminal_status=terminal_status,
                message=terminal_message,
                source_database=effective_database,
            )

            if not transitioned:
                raise RuntimeError(
                    f"Failed to transition assessment row {schema}.{table} to {terminal_status}"
                )

            if terminal_status == "REVIEW_REQUIRED":
                review_required_count += 1
            else:
                blocked_count += 1

            continue

        # 4. Require active and PROVISIONED
        if not reg.get("is_active"):
            raise ValueError(f"Registration for {schema}.{table} is not active (is_active=False)")
        if str(reg.get("current_status") or "").upper().strip() != "PROVISIONED":
            raise ValueError(f"Registration for {schema}.{table} is not PROVISIONED (current_status={reg.get('current_status')!r})")

        # 5. Require complete target FQN
        t_cat = normalize_target_component(reg.get("target_catalog"))
        t_sch = normalize_target_component(reg.get("target_schema"))
        t_tbl = normalize_target_component(reg.get("target_table"))
        if not t_cat or not t_sch or not t_tbl:
            raise ValueError(f"Registration for {schema}.{table} has incomplete target FQN ({t_cat}.{t_sch}.{t_tbl})")

        # 6. Verify target ownership remains collision-free
        conflicts = repo.find_target_owners(
            target_catalog=t_cat,
            target_schema=t_sch,
            target_table=t_tbl,
            exclude_owner=(connection_id, expected_sid),
            include_reserved=True,
        )
        if conflicts:
            raise ValueError(f"Target FQN {t_cat}.{t_sch}.{t_tbl} is reserved by another registration")

        # 7. Finalize exact assessment row as ONBOARDED
        succ = repo.mark_assessment_onboarding_completed(
            connection_id=connection_id,
            assessment_id=assessment_id,
            source_schema=schema,
            object_name=table,
            run_id=run_id,
            attempt_id=effective_attempt_id,
            source_database=effective_database,
        )
        if not succ:
            raise RuntimeError(f"Failed to transition assessment row {schema}.{table} to ONBOARDED")

        onboarded_count += 1

    except Exception as exc:
        failed_count += 1
        safe_msg = failcls.sanitize_message(exc)
        errors.append(f"{schema}.{table}: {safe_msg}")
        try:
            repo.mark_assessment_onboarding_failed(
                connection_id=connection_id,
                assessment_id=assessment_id,
                source_schema=schema,
                object_name=table,
                run_id=run_id,
                attempt_id=effective_attempt_id,
                failed_stage="FINALIZATION",
                error=safe_msg,
                source_database=effective_database,
            )
        except Exception as state_exc:
            print(f"[warn] Failed to mark assessment row FAILED: {failcls.sanitize_message(state_exc)}")

# COMMAND ----------

terminal_count = (
    onboarded_count
    + review_required_count
    + blocked_count
)

if candidate_count == 0:
    business_status = "NO_CANDIDATES"
elif failed_count > 0:
    business_status = "FAILED" if terminal_count == 0 else "PARTIAL"
elif onboarded_count == candidate_count:
    business_status = "COMPLETE"
elif terminal_count == candidate_count and (review_required_count + blocked_count) > 0:
    business_status = "TERMINAL_REVIEW"
else:
    business_status = "PARTIAL"

execution_status = "FAILED" if failed_count > 0 else "SUCCEEDED"

bounded_errors = [
    str(error)[:500]
    for error in errors[:20]
]

exit_payload = {
    "status": execution_status,
    "business_status": business_status,
    "run_id": run_id,
    "attempt_id": attempt_id,
    "connection_id": connection_id,
    "assessment_id": assessment_id,
    "candidate_count": candidate_count,
    "onboarded_count": onboarded_count,
    "review_required_count": review_required_count,
    "blocked_count": blocked_count,
    "failed_count": failed_count,
    "errors": bounded_errors,
}

set_task_value("run_id", run_id)
set_task_value("connection_id", connection_id)
set_task_value("assessment_id", assessment_id)
set_task_value("onboarded_count", onboarded_count)
set_task_value("business_status", business_status)
set_task_value("status", execution_status)

payload_str = json.dumps(exit_payload)
print(f"Finalization outcome: {payload_str}")

if failed_count > 0:
    raise RuntimeError(
        f"Finalization failed for {failed_count} table(s): {'; '.join(bounded_errors[:5])}"
    )

dbutils.notebook.exit(payload_str)