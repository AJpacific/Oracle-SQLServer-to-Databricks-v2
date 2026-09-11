"""
Static source checks for notebook-level behavior that cannot be unit tested
without Spark: connection routing, connection_id propagation and scoping, run_id
priority, ETL ordering guarantees, retry frozen-interval reuse, state-only
recovery auditing, and production cleanliness.

These read notebook source text; they never execute a notebook.
"""

import os
import re
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
for p in (HERE, os.path.dirname(HERE)):
    if p not in sys.path:
        sys.path.insert(0, p)

from _nbsource import (  # noqa: E402
    SHARED_NOTEBOOKS, SOURCE_TOKENS, shared_nb, source_nb,
    all_shared_notebooks, all_source_notebooks,
)


class TestConnectionRouting(unittest.TestCase):
    ROUTED = ("NB09_FullLoad.py", "NB10_PostFullLoadState.py",
              "NB11a_DeltaSyncPrep.py", "NB11b_DeltaSyncApply.py")

    def test_shared_source_notebooks_use_routed_adapter(self):
        for name in self.ROUTED:
            self.assertIn("get_source_adapter_routed(", shared_nb(name), name)

    def test_source_inventory_uses_routed_adapter(self):
        for token in SOURCE_TOKENS:
            self.assertIn("get_source_adapter_routed(",
                          source_nb(token, "NB01_SourceInventory.py"), token)

    def test_legacy_row_adapter_only_in_common_fallback(self):
        for name in SHARED_NOTEBOOKS:
            self.assertNotIn("get_source_adapter_for_row(", shared_nb(name), name)
        for token, name in all_source_notebooks():
            self.assertNotIn("get_source_adapter_for_row(",
                             source_nb(token, name), f"{token}/{name}")
        common = shared_nb("_common.py")
        self.assertIn("def get_source_adapter_for_row(", common)
        self.assertIn("def get_source_adapter_routed(", common)

    def test_routing_guards_connection_state(self):
        common = shared_nb("_common.py")
        self.assertIn("assert_source_system_match", common)
        self.assertIn("not found in source_connection", common)
        self.assertIn("is not VALID", common)

    def test_no_legacy_oracle_read_jdbc_outside_common(self):
        for name in SHARED_NOTEBOOKS:
            self.assertIsNone(
                re.search(r"(?<![._\w])read_jdbc\(", shared_nb(name)), name)
        for token, name in all_source_notebooks():
            self.assertIsNone(
                re.search(r"(?<![._\w])read_jdbc\(", source_nb(token, name)),
                f"{token}/{name}")


class TestExplicitSourceSystem(unittest.TestCase):
    def test_shared_executable_code_has_no_oracle_default(self):
        patterns = (r"\bor\s+[\"']oracle[\"']",
                    r"\belse\s+[\"']oracle[\"']")
        for name in all_shared_notebooks():
            code = shared_nb(name)
            for pattern in patterns:
                self.assertIsNone(re.search(pattern, code), f"{name}: {pattern}")

    def test_common_exports_required_source_guard(self):
        common = shared_nb("_common.py")
        self.assertIn("require_source_system", common)
        self.assertIn("source_table_id_for_row", common)

    def test_backfill_skips_and_counts_missing_sources(self):
        source = shared_nb("NB00_ControlTableInit.py")
        self.assertIn("_skipped_missing_source", source)
        self.assertIn("classify this legacy row manually", source)
        self.assertIn("Skipped {_skipped_missing_source} legacy row(s)", source)
        missing_guard = source.index("if _r[\"source_system\"] is None")
        compute = source.index("_sid = compute_source_table_id", missing_guard)
        self.assertLess(missing_guard, compute)

    def test_runtime_rows_use_required_source_guard(self):
        for name in (
                "NB02_TypeNormalization.py", "NB03_MappingRulesGeneration.py",
                "NB04_MappingValidation.py", "NB07_TableDecisionGeneration.py",
                "NB08_TargetProvisioning.py", "NB09_FullLoad.py",
                "NB10_PostFullLoadState.py", "NB11a_DeltaSyncPrep.py",
                "NB11b_DeltaSyncApply.py",
                "NB12_ValidationAndReconciliation.py",
                "NB15_BronzeToSilverETL.py"):
            self.assertIn("require_source_system(", shared_nb(name), name)


class TestConnectionIdPropagationAndScoping(unittest.TestCase):
    def test_inventory_writes_connection_id(self):
        for token in SOURCE_TOKENS:
            code = source_nb(token, "NB01_SourceInventory.py")
            self.assertIn('"connection_id": conn_id', code, token)

    def test_bulk_notebooks_scope_by_connection(self):
        for name in ("NB02_TypeNormalization.py", "NB03_MappingRulesGeneration.py",
                     "NB04_MappingValidation.py", "NB07_TableDecisionGeneration.py"):
            code = shared_nb(name)
            self.assertIn("CONNECTION_ID", code, name)
            self.assertIn("connection_id = {escape_string_literal(CONNECTION_ID)}",
                          code, name)

    def test_provisioning_scopes_active_tables(self):
        self.assertIn("repo.active_tables(connection_id=(CONNECTION_ID or None)",
                      shared_nb("NB08_TargetProvisioning.py"))

    def test_full_load_scopes_active_tables(self):
        self.assertIn("connection_id=(CONNECTION_ID or None)",
                      shared_nb("NB09_FullLoad.py"))

    def test_delta_queue_carries_connection_id(self):
        code = shared_nb("NB11a_DeltaSyncPrep.py")
        self.assertIn('connection_id=d.get("connection_id")', code)
        self.assertIn('StructField("connection_id"', code)

    def test_reconciliation_results_carry_connection_id(self):
        self.assertIn("c.connection_id",
                      shared_nb("NB12_ValidationAndReconciliation.py"))


class TestRunId(unittest.TestCase):
    def test_widget_has_priority(self):
        body = (shared_nb("_common.py").split("def get_run_id():")[1]
                .split("def set_task_value")[0])
        self.assertLess(body.index('dbutils.widgets.get("run_id")'),
                        body.index("dbutils.jobs.taskValues.get"))
        self.assertLess(body.index("dbutils.jobs.taskValues.get"),
                        body.index("return new_run_id()"))


class TestDeltaApplyScoping(unittest.TestCase):
    SRC = None

    @classmethod
    def setUpClass(cls):
        cls.SRC = shared_nb("NB11b_DeltaSyncApply.py")

    def test_normal_queue_query_is_scoped(self):
        self.assertIn("queue_scope", self.SRC)
        self.assertIn("AND source_table_id = {escape_string_literal(only_id)}",
                      self.SRC)

    def test_scoped_run_requires_exactly_one_queued_row(self):
        self.assertIn("resolved to {len(queue)} QUEUED rows", self.SRC)
        self.assertIn("expected exactly 1", self.SRC)

    def test_retry_delta_apply_requires_parent_run(self):
        self.assertIn("RETRY_DELTA_APPLY requires both parent_run_id and "
                      "source_table_id", self.SRC)

    def test_retry_delta_apply_reads_parent_queue_row(self):
        block = self.SRC.split('if recovery_action == "RETRY_DELTA_APPLY":')[1]
        self.assertIn("run_id = {escape_string_literal(parent_run_id)}", block)

    def test_retry_delta_apply_preserves_frozen_bounds(self):
        block = self.SRC.split('if recovery_action == "RETRY_DELTA_APPLY":')[1]
        self.assertIn('"last_watermark_value"', block)
        self.assertIn('"upper_watermark_value"', block)
        self.assertIn('"source_query"', block)

    def test_retry_delta_apply_does_not_recapture_watermark(self):
        block = self.SRC.split('if recovery_action == "RETRY_DELTA_APPLY":')[1]
        block = block.split("# COMMAND ----------")[0]
        self.assertNotIn("upper_watermark_query", block)
        self.assertNotIn("capture_upper_watermark", block)

    def test_retry_child_queue_row_is_idempotent(self):
        block = self.SRC.split('if recovery_action == "RETRY_DELTA_APPLY":')[1]
        self.assertIn("MERGE INTO", block)
        self.assertIn("t.run_id = s.run_id AND t.source_table_id = s.source_table_id",
                      block)
        self.assertIn("WHEN MATCHED AND t.status <> 'SUCCEEDED'", block)

    def test_retry_does_not_copy_result_metrics(self):
        block = self.SRC.split('if recovery_action == "RETRY_DELTA_APPLY":')[1]
        carried = block.split("carried = {")[1].split("}")[0]
        for excluded in ("applied_row_count", "reconciled_ts",
                         "checkpoint_committed_ts", "finalized_ts",
                         "reconciliation_status"):
            self.assertNotIn(excluded, carried)

    def test_child_audit_carries_parent_and_attempt(self):
        self.assertIn('"parent_run_id": parent_run_id', self.SRC)
        self.assertIn('"attempt_number": attempt_number', self.SRC)


class TestStateOnlyRecoveryAudit(unittest.TestCase):
    SRC = None

    @classmethod
    def setUpClass(cls):
        cls.SRC = shared_nb("NB11b_DeltaSyncApply.py")

    def test_recovery_operations_are_named(self):
        self.assertIn('"CHECKPOINT_RECOVERY"', self.SRC)
        self.assertIn('"QUEUE_FINALIZATION_RECOVERY"', self.SRC)

    def test_recovery_writes_child_audit_rows(self):
        block = self.SRC.split("RETRY_CHECKPOINT_ONLY\", \"RETRY_QUEUE_FINALIZATION_ONLY\"")[1]
        self.assertIn("def _log_recovery(", block)
        self.assertIn('"parent_run_id": src_run', block)
        self.assertIn('_log_recovery("SUCCEEDED")', block)
        self.assertIn('_log_recovery("FAILED"', block)

    def test_recovery_never_reads_source_or_reapplies(self):
        block = self.SRC.split(
            "RETRY_CHECKPOINT_ONLY\", \"RETRY_QUEUE_FINALIZATION_ONLY\"")[1]
        block = block.split("# RETRY_DELTA_APPLY reuses")[0]
        for forbidden in ("read_source_jdbc", "build_merge_sql", "DELETE FROM",
                          "saveAsTable"):
            self.assertNotIn(forbidden, block)
        self.assertIn("no source read, no data reapplied", self.SRC)


class TestSpecializedDeltaFailureFields(unittest.TestCase):
    SRC = None

    @classmethod
    def setUpClass(cls):
        cls.SRC = shared_nb("NB11b_DeltaSyncApply.py")

    def test_reconciliation_failure_fields(self):
        self.assertIn('"error_category": failcls.RECONCILIATION_ERROR', self.SRC)

    def test_checkpoint_failure_fields(self):
        self.assertIn('"failure_stage": failcls.CHECKPOINT', self.SRC)
        self.assertIn('"error_category": failcls.CHECKPOINT_ERROR', self.SRC)

    def test_finalization_failure_fields(self):
        self.assertIn('"failure_stage": failcls.QUEUE_FINALIZATION', self.SRC)

    def test_specialized_failures_are_not_retry_eligible(self):
        # Each of the three specialized handlers sets retry_eligible False.
        self.assertGreaterEqual(self.SRC.count('"retry_eligible": False'), 3)


class TestEtlBehavior(unittest.TestCase):
    SRC = None

    @classmethod
    def setUpClass(cls):
        cls.SRC = shared_nb("NB15_BronzeToSilverETL.py")

    def test_duplicate_keys_checked_before_merge(self):
        dup = self.SRC.index("dup_valid = (valid_out.groupBy")
        merge = self.SRC.index("ddl.build_merge_sql(silver_catalog")
        self.assertLess(dup, merge)
        self.assertIn("MERGE not executed and ETL watermark unchanged", self.SRC)

    def test_invalid_rules_fail_before_silver_write(self):
        self.assertLess(self.SRC.index("if invalid_rules:"),
                        self.SRC.index("current_stage = failcls.SILVER_WRITE"))
        self.assertIn("DQ_CONFIG_ERROR", self.SRC)

    def test_default_value_validated_before_transformation(self):
        validate = self.SRC.index("DEFAULT_VALUE cast probe failed")
        transform = self.SRC.index("# ---- Stage: cleansing transforms")
        self.assertLess(validate, transform)
        self.assertIn("dqr.default_value_converts(", self.SRC)
        self.assertIn("cannot be represented as", self.SRC)

    def test_invalid_default_value_blocks_silver(self):
        # The DEFAULT_VALUE probe feeds invalid_rules, which raises via fail_etl
        # before the Silver write stage is reached.
        self.assertLess(self.SRC.index("invalid_rules.append((\n                    t[\"rule_id\"]"),
                        self.SRC.index("current_stage = failcls.SILVER_WRITE"))

    def test_single_failure_log_guard(self):
        self.assertIn("failure_already_logged", self.SRC)
        self.assertIn("if not failure_already_logged:", self.SRC)

    def test_typed_watermark_comparison(self):
        # Typed bounds are resolved once by the work unit, never lexically.
        self.assertIn("etlwu.resolve_cast(", self.SRC)
        self.assertIn(".cast(cast_to)", self.SRC)
        self.assertIn("only DATE and TIMESTAMP are supported", self.SRC)

    def test_retry_reuses_frozen_bounds(self):
        self.assertIn("is_etl_retry", self.SRC)
        self.assertIn("retry_upper_watermark", self.SRC)
        self.assertIn("retry_lower_watermark", self.SRC)
        self.assertIn("RETRY_ETL requires parent_run_id", self.SRC)

    def test_retry_does_not_recompute_bronze_max(self):
        # The retry branch of boundary resolution must never call Bronze MAX.
        block = (self.SRC.split("        if is_etl_retry:")[1]
                 .split("        else:")[0])
        self.assertNotIn("F.max(", block)
        self.assertIn("raw_lower, raw_upper = retry_lower_wm, retry_upper_wm", block)

    def test_incremental_retry_without_bounds_is_configuration_error(self):
        # Boundary validation lives in the pure work-unit builder and surfaces
        # as a configuration failure before any data is touched.
        self.assertIn("etlwu.build_incremental_work_unit(", self.SRC)
        self.assertIn("ETL_CONFIG_ERROR", self.SRC)
        self.assertLess(self.SRC.index("etlwu.build_incremental_work_unit("),
                        self.SRC.index("current_stage = failcls.SILVER_WRITE"))

    def test_all_paths_share_one_work_unit(self):
        self.assertIn("work_unit.audit_fields(attempt_number)", self.SRC)
        self.assertIn("etlwu.checkpoint_value(work_unit", self.SRC)
        # The success audit must not fall back to the control-row watermark.
        self.assertNotIn('"lower_watermark": last_etl_wm', self.SRC)

    def test_quarantine_idempotent_and_counted_when_disabled(self):
        self.assertIn("DELETE FROM", self.SRC)
        self.assertIn("counted for reconciliation but NOT persisted", self.SRC)

    def test_reconciliation_precedes_checkpoint(self):
        self.assertLess(self.SRC.index("if not recon_result.passed:"),
                        self.SRC.index("current_stage = failcls.CHECKPOINT"))

    def test_unique_duplicate_helper_columns(self):
        self.assertIn('f"_dupcount_{idx}"', self.SRC)

    def test_etl_never_touches_a_source(self):
        self.assertNotIn("get_source_adapter", self.SRC)
        self.assertNotIn("dbutils.secrets", self.SRC)
        self.assertNotIn("read_source_jdbc", self.SRC)


class TestIngestFailureStages(unittest.TestCase):
    def test_full_load_tracks_stage(self):
        code = shared_nb("NB09_FullLoad.py")
        self.assertIn("current_stage = failcls.TARGET_WRITE", code)
        self.assertIn("current_stage = failcls.RECONCILIATION", code)
        self.assertIn("classify_failure(e, current_stage", code)
        self.assertIn("expected exactly 1", code)

    def test_delta_apply_tracks_stage(self):
        code = shared_nb("NB11b_DeltaSyncApply.py")
        for stage in ("SOURCE_READ", "TARGET_WRITE", "RECONCILIATION",
                      "CHECKPOINT", "QUEUE_FINALIZATION"):
            self.assertIn(f"current_stage = failcls.{stage}", code)
        self.assertIn("classify_failure(apply_error, current_stage", code)

    def test_audit_status_normalized(self):
        code = shared_nb("NB09_FullLoad.py")
        self.assertNotIn('"COUNT_MISMATCH", None, started', code)
        self.assertIn('log_run(ident, target_fqn, s_count, t_count, "FAILED"', code)


class TestRetrySelector(unittest.TestCase):
    SRC = None

    @classmethod
    def setUpClass(cls):
        cls.SRC = shared_nb("NB14_RetryFailedTables.py")

    def test_returns_frozen_bounds(self):
        self.assertIn("lower_watermark, upper_watermark", self.SRC)
        self.assertIn('"retry_lower_watermark": r["lower_watermark"]', self.SRC)
        self.assertIn('"retry_upper_watermark": r["upper_watermark"]', self.SRC)

    def test_additional_retry_semantics(self):
        self.assertIn("retry_count = prev_attempt - 1", self.SRC)
        self.assertIn("retry_count >= max_retries", self.SRC)

    def test_safe_recovery_actions_remain_selectable(self):
        self.assertIn("SAFE_RECOVERY_ACTIONS", self.SRC)
        self.assertIn("RETRY_CHECKPOINT_ONLY", self.SRC)
        self.assertIn("RETRY_QUEUE_FINALIZATION_ONLY", self.SRC)

    def test_deterministic_latest_attempt_ordering(self):
        self.assertIn("ended_ts DESC NULLS LAST", self.SRC)


class TestCanonicalFullLoadCheck(unittest.TestCase):
    def test_nb12_writes_canonical_check_type(self):
        self.assertIn('check_type = "FULL_SNAPSHOT_COUNT"',
                      shared_nb("NB12_ValidationAndReconciliation.py"))

    def test_nb10_gates_on_canonical_check_type(self):
        code = shared_nb("NB10_PostFullLoadState.py")
        self.assertIn("rr.check_type = 'FULL_SNAPSHOT_COUNT'", code)
        self.assertNotIn("rr.check_type = 'ROW_COUNT'", code)

    def test_dashboard_uses_canonical_check_type(self):
        self.assertIn("FULL_SNAPSHOT_COUNT", shared_nb("NB17_DashboardViews.py"))

    def test_no_count_comparison_shortcut(self):
        self.assertNotIn("tgt_count >= src_count",
                         shared_nb("NB12_ValidationAndReconciliation.py"))


class TestDashboardViews(unittest.TestCase):
    SRC = None

    @classmethod
    def setUpClass(cls):
        cls.SRC = shared_nb("NB17_DashboardViews.py")

    def test_deterministic_latest_ordering(self):
        self.assertIn("ended_ts DESC NULLS LAST", self.SRC)
        self.assertIn("started_ts DESC NULLS LAST", self.SRC)
        self.assertIn("run_id DESC", self.SRC)

    def test_latest_assessment_only(self):
        self.assertIn("latest_sa", self.SRC)
        self.assertIn("latest_soa", self.SRC)

    def test_connection_id_in_etl_view(self):
        etl = self.SRC.split("CREATE OR REPLACE VIEW {ctrl('vw_etl_status')}")[1]
        self.assertIn("c.connection_id", etl)

    def test_errors_exposed_separately(self):
        self.assertIn("AS ingest_error", self.SRC)
        self.assertIn("AS etl_error", self.SRC)
        self.assertNotIn("coalesce(c.error_message, c.etl_error_message)", self.SRC)

    def test_quarantine_count_scoped_to_latest_run(self):
        self.assertIn("latest_run_quarantine_count", self.SRC)
        self.assertIn("latest_etl_run", self.SRC)


class TestNotification(unittest.TestCase):
    def test_distinct_failed_tables_reported(self):
        code = shared_nb("NB16_NotifyFailures.py")
        for token in ("distinct_failed_tables", "table_run_failures=",
                      "reconciliation_failures=", "dq_rule_failures="):
            self.assertIn(token, code)

    def test_webhook_never_printed(self):
        code = shared_nb("NB16_NotifyFailures.py")
        self.assertNotIn("print(webhook", code)
        self.assertIn("never printed", code)


class TestDiagnostics(unittest.TestCase):
    def test_counters_initialized(self):
        for token in SOURCE_TOKENS:
            code = source_nb(token, "TEST_CONNECTION.py")
            self.assertIn("sample_count = 0", code)
            self.assertIn("meta_count = 0", code)
            self.assertIn("pk_count = 0", code)

    def test_registered_connection_is_authority(self):
        for token in SOURCE_TOKENS:
            code = source_nb(token, "TEST_CONNECTION.py")
            self.assertIn("if CONNECTION_ID:", code)
            self.assertIn("get_source_adapter_for_connection(", code)
            self.assertIn('test_server = cd.get("source_server")', code)
            self.assertIn('test_database = cd.get("source_database")', code)

    def test_sqlserver_registered_mode_needs_no_database_widget(self):
        code = source_nb("sqlserver", "TEST_CONNECTION.py")
        registered = code.split("if CONNECTION_ID:")[1].split("else:")[0]
        # The widget is never read in the registered branch.
        self.assertNotIn('dbutils.widgets.get("test_database")', registered)
        self.assertIn("legacy mode requires the test_database widget", code)

    def test_source_system_mismatch_fails(self):
        self.assertIn("requires an Oracle connection",
                      source_nb("oracle", "TEST_CONNECTION.py"))
        self.assertIn("requires a SQL Server connection",
                      source_nb("sqlserver", "TEST_CONNECTION.py"))

    def test_legacy_mode_is_announced(self):
        for token in SOURCE_TOKENS:
            self.assertIn("legacy fallback", source_nb(token, "TEST_CONNECTION.py"))

    def test_probe_comes_from_adapter(self):
        for token in SOURCE_TOKENS:
            self.assertIn("probe_connection(adapter",
                          source_nb(token, "TEST_CONNECTION.py"))


class TestConnectionNotebookSanitization(unittest.TestCase):
    def test_failures_are_sanitized_without_an_adapter(self):
        for token in SOURCE_TOKENS:
            code = source_nb(token, "NB00A_UpsertAndValidateConnection.py")
            self.assertIn("failcls.sanitize_message(e)", code, token)
            # No dependency on a constructed adapter for redaction.
            self.assertNotIn("adapter.redact_jdbc_url(str(e))", code, token)

    def test_no_secrets_returned(self):
        for token in SOURCE_TOKENS:
            code = source_nb(token, "NB00A_UpsertAndValidateConnection.py")
            tail = code.split("dbutils.notebook.exit(")[-1]
            for banned in ("password", "secret_scope", "jdbc"):
                self.assertNotIn(banned, tail.lower(), token)


class TestProductionCleanliness(unittest.TestCase):
    def _every_notebook(self):
        for name in all_shared_notebooks():
            yield f"shared/{name}", shared_nb(name)
        for token, name in all_source_notebooks():
            yield f"sources/{token}/{name}", source_nb(token, name)

    def test_no_personal_workspace_path(self):
        for label, code in self._every_notebook():
            self.assertNotIn("/Workspace/Users/", code, label)

    def test_no_hardcoded_control_schema(self):
        self.assertNotIn("da_accelerators.control",
                         shared_nb("NB00_ControlTableInit.py"))

    def test_seeding_disabled_by_default(self):
        code = shared_nb("NB00_ControlTableInit.py")
        self.assertIn('dropdown("seed_poc_rows", "false"', code)
        self.assertIn('dropdown("seed_sqlserver_examples", "false"', code)

    def test_no_ipv4_widget_defaults(self):
        ipv4 = re.compile(r"\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b")
        for label, code in self._every_notebook():
            for line in code.splitlines():
                if "widgets.text" in line or "widgets.dropdown" in line:
                    self.assertIsNone(ipv4.search(line), f"{label}: {line}")

    def test_legacy_pipeline_terminology_removed(self):
        for label, code in self._every_notebook():
            self.assertNotIn("Pipeline 1", code, label)
            self.assertNotIn("Pipeline 2", code, label)

    def test_assessment_wording_does_not_claim_exact(self):
        for label, code in self._every_notebook():
            lowered = code.lower()
            self.assertNotIn("exact catalog row counts", lowered, label)
            self.assertNotIn("exact for sql server", lowered, label)


if __name__ == "__main__":
    unittest.main()
