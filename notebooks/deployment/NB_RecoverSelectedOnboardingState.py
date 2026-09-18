# Databricks notebook source
# MAGIC %md
# MAGIC # NB_RecoverSelectedOnboardingState
# MAGIC Administrative evidence-based recovery notebook for stale or interrupted
# MAGIC assessment onboarding claims.
# MAGIC Does NOT automatically reset rows solely based on elapsed time.
# MAGIC Requires explicit operator action, verified owning run_id, and defaults to dry-run mode.
# MAGIC Performs metadata operations only: no source JDBC or secret access.

# COMMAND ----------

# MAGIC %run ../shared/_common

# COMMAND ----------

import json

dbutils.widgets.text("connection_id", "")
dbutils.widgets.text("assessment_id", "")
dbutils.widgets.text("expected_run_id", "")
dbutils.widgets.text("expected_attempt_id", "")
dbutils.widgets.dropdown("recovery_action", "MARK_FAILED",
                         ["MARK_FAILED", "RESET_TO_SELECTED", "RESUME_REGISTERED", "FINALIZE_ONBOARDED"])
dbutils.widgets.dropdown("dry_run", "true", ["true", "false"])
dbutils.widgets.text("catalog", "da_accelerators")
dbutils.widgets.text("control_schema", "control")

connection_id = dbutils.widgets.get("connection_id").strip()
assessment_id = dbutils.widgets.get("assessment_id").strip()
expected_run_id = dbutils.widgets.get("expected_run_id").strip()
expected_attempt_id = dbutils.widgets.get("expected_attempt_id").strip()
recovery_action = dbutils.widgets.get("recovery_action").strip().upper()
dry_run = dbutils.widgets.get("dry_run").strip().lower() in ("true", "1", "yes")
catalog = dbutils.widgets.get("catalog").strip() or CATALOG
control_schema = dbutils.widgets.get("control_schema").strip() or CONTROL_SCHEMA

if not connection_id:
    raise ValueError("connection_id is required")
if not assessment_id:
    raise ValueError("assessment_id is required")
if not expected_run_id:
    raise ValueError("expected_run_id is required for ownership-safe recovery")

ALLOWED_RECOVERY_ACTIONS = frozenset({
    "MARK_FAILED",
    "RESET_TO_SELECTED",
    "RESUME_REGISTERED",
    "FINALIZE_ONBOARDED",
})

if recovery_action not in ALLOWED_RECOVERY_ACTIONS:
    raise ValueError(
        f"Invalid recovery_action {recovery_action!r}; expected one of: "
        f"{', '.join(sorted(ALLOWED_RECOVERY_ACTIONS))}"
    )

repo = ControlRepository(
    spark,
    catalog=catalog,
    control_schema=control_schema,
)

connection = repo.get_connection(connection_id)
if connection is None:
    raise ValueError(
        f"Unknown connection_id {connection_id!r}"
    )

connection_data = (
    connection.asDict(recursive=True)
    if hasattr(connection, "asDict")
    else dict(connection)
)

source_system = require_source_system(
    connection_data.get("source_system"),
    "registered connection",
)

def ctrl(t):
    return f"{quote_databricks(catalog)}.{quote_databricks(control_schema)}.{quote_databricks(t)}"

# COMMAND ----------

def add_detail(
    source_schema,
    source_table,
    status,
    action=None,
    reason=None,
):
    item = {
        "schema": str(source_schema or "")[:256],
        "table": str(source_table or "")[:256],
        "status": str(status or "")[:64],
    }
    if action:
        item["action"] = str(action)[:128]
    if reason:
        item["reason"] = sanitize_error_message(
            reason
        )[:500]
    details.append(item)

# COMMAND ----------

# Inspect current candidate rows matching exact connection, assessment, and owning run
where_clauses = [
    f"sa.connection_id = {escape_string_literal(connection_id)}",
    f"sa.assessment_id = {escape_string_literal(assessment_id)}",
    "sa.object_type = 'TABLE'",
    f"sa.onboarding_run_id = {escape_string_literal(expected_run_id)}",
]

inspect_query = f"""
    SELECT sa.source_schema, sa.object_name, sa.selection_status, sa.is_selected,
           sa.onboarding_run_id, sa.onboarding_attempt_id,
           sa.onboarding_started_ts, sa.registration_completed_ts
    FROM {ctrl('source_assessment')} sa
    WHERE {' AND '.join(where_clauses)}
    ORDER BY sa.source_schema, sa.object_name
"""
candidates = spark.sql(inspect_query).collect()
print(f"Discovered {len(candidates)} row(s) owned by run={expected_run_id} (dry_run={dry_run})")

# COMMAND ----------

candidate_count = len(candidates)
recovered_count = 0
would_recover_count = 0
skipped_count = 0
transition_failure_count = 0
errors = []
details = []

for r in candidates:
    sch = r["source_schema"]
    tbl = r["object_name"]
    cur_status = str(r.get("selection_status") or "").upper().strip()

    row_attempt_id = str(
        r.get("onboarding_attempt_id") or ""
    ).strip()

    requested_attempt_id = str(
        expected_attempt_id or ""
    ).strip()

    if not row_attempt_id:
        skipped_count += 1
        add_detail(
            sch,
            tbl,
            status="SKIPPED",
            reason=(
                "Assessment row has no recorded onboarding "
                "attempt ID; ownership-safe recovery cannot "
                "adopt a missing attempt"
            ),
        )
        continue

    if (
        requested_attempt_id
        and requested_attempt_id != row_attempt_id
    ):
        skipped_count += 1
        add_detail(
            sch,
            tbl,
            status="SKIPPED",
            reason=(
                "Expected onboarding attempt ID does not "
                "match the row owner"
            ),
        )
        continue

    effective_attempt_id = row_attempt_id

    expected_source_table_id = compute_source_table_id(
        connection_id=connection_id,
        source_system=source_system,
        source_server=connection_data.get("source_server"),
        source_database=connection_data.get("source_database"),
        source_schema=sch,
        source_table=tbl,
    )

    # Execute approved recovery action
    if recovery_action == "MARK_FAILED":
        if cur_status in ("ONBOARDED", "REVIEW_REQUIRED", "BLOCKED"):
            skipped_count += 1
            add_detail(sch, tbl, status="SKIPPED", reason=f"Row is already in terminal state {cur_status}")
            continue
        if cur_status not in ("ONBOARDING", "REGISTERED"):
            skipped_count += 1
            add_detail(sch, tbl, status="SKIPPED", reason=f"Row status {cur_status} is not eligible for MARK_FAILED (requires ONBOARDING or REGISTERED)")
            continue

        if dry_run:
            would_recover_count += 1
            add_detail(
                sch,
                tbl,
                status="WOULD_CHANGE",
                action="WOULD_MARK_FAILED",
            )
            continue

        try:
            changed = repo.mark_assessment_onboarding_failed(
                connection_id=connection_id,
                assessment_id=assessment_id,
                source_schema=sch,
                object_name=tbl,
                run_id=expected_run_id,
                attempt_id=effective_attempt_id,
                failed_stage="REGISTRATION",
                error="Administratively marked FAILED after interrupted onboarding execution",
            )
            if not changed:
                transition_failure_count += 1
                failure_message = (
                    f"{sch}.{tbl}: Ownership-safe FAILED "
                    "transition was not verified"
                )
                safe_failure_message = sanitize_error_message(failure_message)[:500]
                errors.append(safe_failure_message)
                add_detail(
                    sch,
                    tbl,
                    status="FAILED",
                    action="MARK_FAILED",
                    reason=safe_failure_message,
                )
                continue

            recovered_count += 1
            add_detail(
                sch,
                tbl,
                status="RECOVERED",
                action="MARKED_FAILED",
            )
        except Exception as exc:
            transition_failure_count += 1
            safe_error = sanitize_error_message(exc)[:500]
            errors.append(f"{sch}.{tbl}: {safe_error}")
            add_detail(
                sch,
                tbl,
                status="FAILED",
                action="MARK_FAILED",
                reason=safe_error,
            )
            continue

    elif recovery_action == "RESET_TO_SELECTED":
        if cur_status in ("ONBOARDED", "REGISTERED", "REVIEW_REQUIRED", "BLOCKED"):
            skipped_count += 1
            add_detail(sch, tbl, status="SKIPPED", reason=f"Row status {cur_status} is ineligible for RESET_TO_SELECTED (must be ONBOARDING or FAILED)")
            continue
        if cur_status not in ("ONBOARDING", "FAILED"):
            skipped_count += 1
            add_detail(sch, tbl, status="SKIPPED", reason=f"Row status {cur_status} is ineligible for RESET_TO_SELECTED (must be ONBOARDING or FAILED)")
            continue

        if r.get("is_selected") is False:
            skipped_count += 1
            add_detail(sch, tbl, status="SKIPPED", reason="Row is not currently selected")
            continue

        exact_registration_rows = spark.sql(f"""
            SELECT source_table_id
            FROM {ctrl('source_table_control')}
            WHERE connection_id =
                  {escape_string_literal(connection_id)}
              AND source_table_id =
                  {escape_string_literal(expected_source_table_id)}
        """).collect()

        schema_table_rows = spark.sql(f"""
            SELECT source_table_id
            FROM {ctrl('source_table_control')}
            WHERE connection_id =
                  {escape_string_literal(connection_id)}
              AND source_schema =
                  {escape_string_literal(sch)}
              AND source_table =
                  {escape_string_literal(tbl)}
        """).collect()

        if exact_registration_rows or schema_table_rows:
            skipped_count += 1
            add_detail(
                sch,
                tbl,
                status="SKIPPED",
                reason=(
                    "A table registration already exists; "
                    "cannot reset to SELECTED"
                ),
            )
            continue

        if dry_run:
            would_recover_count += 1
            add_detail(
                sch,
                tbl,
                status="WOULD_CHANGE",
                action="WOULD_RESET_TO_SELECTED",
            )
            continue

        try:
            spark.sql(f"""
                UPDATE {ctrl('source_assessment')}
                SET is_selected = true,
                    selection_status = 'SELECTED',
                    onboarding_run_id = NULL,
                    onboarding_attempt_id = NULL,
                    onboarding_started_ts = NULL,
                    registration_completed_ts = NULL,
                    onboarding_completed_ts = NULL,
                    onboarding_failed_stage = NULL,
                    onboarding_error_message = NULL
                WHERE connection_id = {escape_string_literal(connection_id)}
                  AND assessment_id = {escape_string_literal(assessment_id)}
                  AND source_schema = {escape_string_literal(sch)}
                  AND object_type = 'TABLE'
                  AND object_name = {escape_string_literal(tbl)}
                  AND onboarding_run_id = {escape_string_literal(expected_run_id)}
                  AND onboarding_attempt_id = {escape_string_literal(effective_attempt_id)}
                  AND upper(trim(coalesce(selection_status, ''))) IN ('ONBOARDING', 'FAILED')
            """)

            updated_row = repo._get_assessment_selection_row(
                connection_id=connection_id,
                assessment_id=assessment_id,
                source_schema=sch,
                object_name=tbl,
            )

            reset_succeeded = (
                updated_row is not None
                and str(
                    updated_row.get("selection_status") or ""
                ).strip().upper() == "SELECTED"
                and updated_row.get("is_selected") is True
                and not str(
                    updated_row.get("onboarding_run_id") or ""
                ).strip()
                and not str(
                    updated_row.get("onboarding_attempt_id") or ""
                ).strip()
            )

            if not reset_succeeded:
                transition_failure_count += 1
                failure_message = (
                    f"{sch}.{tbl}: Conditional RESET_TO_SELECTED "
                    "could not be verified"
                )
                safe_failure_message = sanitize_error_message(failure_message)[:500]
                errors.append(safe_failure_message)
                add_detail(
                    sch,
                    tbl,
                    status="FAILED",
                    action="RESET_TO_SELECTED",
                    reason=safe_failure_message,
                )
                continue

            recovered_count += 1
            add_detail(
                sch,
                tbl,
                status="RECOVERED",
                action="RESET_TO_SELECTED",
            )
        except Exception as exc:
            transition_failure_count += 1
            safe_error = sanitize_error_message(exc)[:500]
            errors.append(f"{sch}.{tbl}: {safe_error}")
            add_detail(
                sch,
                tbl,
                status="FAILED",
                action="RESET_TO_SELECTED",
                reason=safe_error,
            )
            continue

    elif recovery_action == "RESUME_REGISTERED":
        if cur_status in ("ONBOARDED", "REVIEW_REQUIRED", "BLOCKED"):
            skipped_count += 1
            add_detail(sch, tbl, status="SKIPPED", reason=f"Row is already in terminal state {cur_status}")
            continue
        if cur_status != "ONBOARDING":
            skipped_count += 1
            add_detail(sch, tbl, status="SKIPPED", reason=f"Row status {cur_status} is not eligible for RESUME_REGISTERED (requires ONBOARDING)")
            continue

        reg_check = spark.sql(f"""
            SELECT
                source_table_id,
                connection_id,
                source_identity_version,
                target_catalog,
                target_schema,
                target_table
            FROM {ctrl('source_table_control')}
            WHERE connection_id =
                  {escape_string_literal(connection_id)}
              AND source_table_id =
                  {escape_string_literal(expected_source_table_id)}
        """).collect()

        if not reg_check:
            skipped_count += 1
            add_detail(
                sch,
                tbl,
                status="SKIPPED",
                reason=(
                    "No exact deterministic source-table "
                    "registration was found"
                ),
            )
            continue

        if len(reg_check) > 1:
            skipped_count += 1
            add_detail(
                sch,
                tbl,
                status="SKIPPED",
                reason=(
                    "Multiple exact deterministic source-table "
                    "registrations were found"
                ),
            )
            continue

        registration = (
            reg_check[0].asDict(recursive=True)
            if hasattr(reg_check[0], "asDict")
            else dict(reg_check[0])
        )

        actual_connection_id = str(
            registration.get("connection_id") or ""
        ).strip()

        actual_source_table_id = str(
            registration.get("source_table_id") or ""
        ).strip()

        if actual_connection_id != connection_id:
            skipped_count += 1
            add_detail(
                sch,
                tbl,
                status="SKIPPED",
                reason="Registration connection owner mismatch",
            )
            continue

        if actual_source_table_id != expected_source_table_id:
            skipped_count += 1
            add_detail(
                sch,
                tbl,
                status="SKIPPED",
                reason=(
                    "Registration deterministic source "
                    "identity mismatch"
                ),
            )
            continue

        actual_identity_version = int(
            registration.get("source_identity_version") or 0
        )

        if actual_identity_version != SOURCE_IDENTITY_VERSION:
            skipped_count += 1
            add_detail(
                sch,
                tbl,
                status="SKIPPED",
                reason=(
                    f"Identity version {actual_identity_version} "
                    f"!= {SOURCE_IDENTITY_VERSION}"
                ),
            )
            continue

        t_cat = normalize_target_component(registration.get("target_catalog"))
        t_sch = normalize_target_component(registration.get("target_schema"))
        t_tbl = normalize_target_component(registration.get("target_table"))
        if not t_cat or not t_sch or not t_tbl:
            skipped_count += 1
            add_detail(sch, tbl, status="SKIPPED", reason="Target FQN incomplete")
            continue

        if dry_run:
            would_recover_count += 1
            add_detail(
                sch,
                tbl,
                status="WOULD_CHANGE",
                action="WOULD_RESUME_REGISTERED",
            )
            continue

        try:
            changed = repo.mark_assessment_registration_succeeded(
                connection_id=connection_id,
                assessment_id=assessment_id,
                source_schema=sch,
                object_name=tbl,
                run_id=expected_run_id,
                attempt_id=effective_attempt_id,
            )
            if not changed:
                transition_failure_count += 1
                failure_message = (
                    f"{sch}.{tbl}: Ownership-safe REGISTERED "
                    "transition was not verified"
                )
                safe_failure_message = sanitize_error_message(failure_message)[:500]
                errors.append(safe_failure_message)
                add_detail(
                    sch,
                    tbl,
                    status="FAILED",
                    action="RESUME_REGISTERED",
                    reason=safe_failure_message,
                )
                continue

            recovered_count += 1
            add_detail(
                sch,
                tbl,
                status="RECOVERED",
                action="RESUMED_REGISTERED",
            )
        except Exception as exc:
            transition_failure_count += 1
            safe_error = sanitize_error_message(exc)[:500]
            errors.append(f"{sch}.{tbl}: {safe_error}")
            add_detail(
                sch,
                tbl,
                status="FAILED",
                action="RESUME_REGISTERED",
                reason=safe_error,
            )
            continue

    elif recovery_action == "FINALIZE_ONBOARDED":
        if cur_status in ("ONBOARDED", "REVIEW_REQUIRED", "BLOCKED"):
            skipped_count += 1
            add_detail(sch, tbl, status="SKIPPED", reason=f"Row is already in terminal state {cur_status}")
            continue
        if cur_status != "REGISTERED":
            skipped_count += 1
            add_detail(sch, tbl, status="SKIPPED", reason=f"Row status {cur_status} is not eligible for FINALIZE_ONBOARDED (requires REGISTERED)")
            continue

        prov_check = spark.sql(f"""
            SELECT
                source_table_id,
                connection_id,
                source_identity_version,
                table_decision,
                current_status,
                is_active,
                target_catalog,
                target_schema,
                target_table
            FROM {ctrl('source_table_control')}
            WHERE connection_id = {escape_string_literal(connection_id)}
              AND source_table_id = {escape_string_literal(expected_source_table_id)}
        """).collect()
        if not prov_check:
            skipped_count += 1
            add_detail(
                sch,
                tbl,
                status="SKIPPED",
                reason=(
                    "No exact deterministic source-table "
                    "registration was found"
                ),
            )
            continue
        if len(prov_check) > 1:
            skipped_count += 1
            add_detail(
                sch,
                tbl,
                status="SKIPPED",
                reason=(
                    "Multiple exact deterministic source-table "
                    "registrations were found"
                ),
            )
            continue

        registration = (
            prov_check[0].asDict(recursive=True)
            if hasattr(prov_check[0], "asDict")
            else dict(prov_check[0])
        )
        actual_connection_id = str(
            registration.get("connection_id") or ""
        ).strip()
        actual_source_table_id = str(
            registration.get("source_table_id") or ""
        ).strip()
        actual_identity_version = int(
            registration.get("source_identity_version") or 0
        )
        table_decision = str(
            registration.get("table_decision") or ""
        ).strip().upper()
        current_status = str(
            registration.get("current_status") or ""
        ).strip().upper()
        is_active = bool(
            registration.get("is_active")
        )

        if actual_connection_id != connection_id:
            skipped_count += 1
            add_detail(
                sch,
                tbl,
                status="SKIPPED",
                reason="Registration connection owner mismatch",
            )
            continue

        if actual_source_table_id != expected_source_table_id:
            skipped_count += 1
            add_detail(
                sch,
                tbl,
                status="SKIPPED",
                reason=(
                    "Registration deterministic source "
                    "identity mismatch"
                ),
            )
            continue

        if actual_identity_version != SOURCE_IDENTITY_VERSION:
            skipped_count += 1
            add_detail(
                sch,
                tbl,
                status="SKIPPED",
                reason=(
                    f"Identity version {actual_identity_version} "
                    f"!= {SOURCE_IDENTITY_VERSION}"
                ),
            )
            continue

        if table_decision != "AUTO_MIGRATE":
            skipped_count += 1
            add_detail(
                sch,
                tbl,
                status="SKIPPED",
                reason=f"Table decision {table_decision} != AUTO_MIGRATE",
            )
            continue

        if not is_active or current_status != "PROVISIONED":
            skipped_count += 1
            add_detail(
                sch,
                tbl,
                status="SKIPPED",
                reason="Registration is not active and PROVISIONED in source_table_control",
            )
            continue

        t_cat = normalize_target_component(registration.get("target_catalog"))
        t_sch = normalize_target_component(registration.get("target_schema"))
        t_tbl = normalize_target_component(registration.get("target_table"))

        if not t_cat or not t_sch or not t_tbl:
            skipped_count += 1
            add_detail(sch, tbl, status="SKIPPED", reason="Target FQN incomplete")
            continue

        conflicts = repo.find_target_owners(
            target_catalog=t_cat,
            target_schema=t_sch,
            target_table=t_tbl,
            exclude_owner=(connection_id, expected_source_table_id),
            include_reserved=True,
        )
        if conflicts:
            skipped_count += 1
            add_detail(sch, tbl, status="SKIPPED", reason="Target FQN collision detected")
            continue

        if dry_run:
            would_recover_count += 1
            add_detail(
                sch,
                tbl,
                status="WOULD_CHANGE",
                action="WOULD_FINALIZE_ONBOARDED",
            )
            continue

        try:
            changed = repo.mark_assessment_onboarding_completed(
                connection_id=connection_id,
                assessment_id=assessment_id,
                source_schema=sch,
                object_name=tbl,
                run_id=expected_run_id,
                attempt_id=effective_attempt_id,
            )
            if not changed:
                transition_failure_count += 1
                failure_message = (
                    f"{sch}.{tbl}: Ownership-safe ONBOARDED "
                    "transition was not verified"
                )
                safe_failure_message = sanitize_error_message(failure_message)[:500]
                errors.append(safe_failure_message)
                add_detail(
                    sch,
                    tbl,
                    status="FAILED",
                    action="FINALIZE_ONBOARDED",
                    reason=safe_failure_message,
                )
                continue

            recovered_count += 1
            add_detail(
                sch,
                tbl,
                status="RECOVERED",
                action="FINALIZED_ONBOARDED",
            )
        except Exception as exc:
            transition_failure_count += 1
            safe_error = sanitize_error_message(exc)[:500]
            errors.append(f"{sch}.{tbl}: {safe_error}")
            add_detail(
                sch,
                tbl,
                status="FAILED",
                action="FINALIZE_ONBOARDED",
                reason=safe_error,
            )
            continue

# COMMAND ----------

if dry_run:
    execution_status = "SUCCEEDED"
    business_status = "DRY_RUN_COMPLETE"
elif transition_failure_count > 0:
    execution_status = "FAILED"
    business_status = (
        "PARTIAL"
        if recovered_count > 0
        else "FAILED"
    )
elif recovered_count > 0:
    execution_status = "SUCCEEDED"
    business_status = "RECOVERED"
else:
    execution_status = "SUCCEEDED"
    business_status = "NO_CHANGES"

bounded_errors = [
    str(error)[:500]
    for error in errors[:20]
]

bounded_details = []
for detail in details[:100]:
    bounded_detail = {
        "schema": str(detail.get("schema") or "")[:256],
        "table": str(detail.get("table") or "")[:256],
        "status": str(detail.get("status") or "")[:64],
    }
    if detail.get("action"):
        bounded_detail["action"] = str(detail["action"])[:128]
    if detail.get("reason"):
        bounded_detail["reason"] = sanitize_error_message(
            detail["reason"]
        )[:500]
    bounded_details.append(bounded_detail)

result = {
    "status": execution_status,
    "execution_status": execution_status,
    "business_status": business_status,
    "dry_run": dry_run,
    "recovery_action": recovery_action,
    "connection_id": connection_id,
    "assessment_id": assessment_id,
    "expected_run_id": expected_run_id,
    "expected_attempt_id": expected_attempt_id or None,
    "candidate_count": candidate_count,
    "recovered_count": recovered_count,
    "would_recover_count": would_recover_count,
    "skipped_count": skipped_count,
    "transition_failure_count": transition_failure_count,
    "errors": bounded_errors,
    "details": bounded_details,
}

set_task_value("status", execution_status)
set_task_value("business_status", business_status)
set_task_value("connection_id", connection_id)
set_task_value("assessment_id", assessment_id)
set_task_value("expected_run_id", expected_run_id)
set_task_value("recovery_action", recovery_action)
set_task_value("dry_run", dry_run)
set_task_value("candidate_count", candidate_count)
set_task_value("recovered_count", recovered_count)
set_task_value("would_recover_count", would_recover_count)
set_task_value("skipped_count", skipped_count)
set_task_value("transition_failure_count", transition_failure_count)

print(json.dumps(result))

if transition_failure_count > 0:
    raise RuntimeError(
        "Administrative onboarding recovery did not complete all requested mutations: "
        f"action={recovery_action}, "
        f"recovered_count={recovered_count}, "
        f"transition_failure_count={transition_failure_count}"
    )

dbutils.notebook.exit(json.dumps(result))
