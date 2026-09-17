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
        self.assertIn("get_source_adapter_routed(",
                      shared_nb("NB11b_DeltaSyncApply.py"))
        full_load = shared_nb("NB09_FullLoad.py")
        self.assertIn("get_source_adapter_for_connection(connection)", full_load)
        post_load = shared_nb("NB10_PostFullLoadState.py")
        self.assertIn("require_valid_connection(", post_load)
        self.assertIn("get_source_adapter_for_connection(", post_load)
        delta_prep = shared_nb("NB11a_DeltaSyncPrep.py")
        self.assertIn("connection_cache = {}", delta_prep)
        self.assertIn("adapter_cache = {}", delta_prep)
        self.assertIn("get_source_adapter_for_connection(connection)", delta_prep)

    def test_source_inventory_uses_routed_adapter(self):
        for token in SOURCE_TOKENS:
            self.assertIn("get_source_adapter_routed(",
                          source_nb(token, "NB01_SourceInventory.py"), token)

    def test_no_shared_legacy_row_adapter_fallback(self):
        for name in SHARED_NOTEBOOKS:
            self.assertNotIn("get_source_adapter_for_row(", shared_nb(name), name)
        for token, name in all_source_notebooks():
            self.assertNotIn("get_source_adapter_for_row(",
                             source_nb(token, name), f"{token}/{name}")
        common = shared_nb("_common.py")
        self.assertNotIn("def get_source_adapter_for_row(", common)
        self.assertNotIn("def _legacy_scope_for(", common)
        self.assertIn("def get_source_adapter_routed(", common)

    def test_routing_guards_connection_state(self):
        common = shared_nb("_common.py")
        self.assertIn("assert_source_system_match", common)
        self.assertIn("not found in source_connection", common)
        self.assertIn("is not VALID", common)
        self.assertIn("has a blank", common)
        self.assertIn('"secret_scope"', common)

    def test_routing_requires_registered_table_binding(self):
        common = shared_nb("_common.py")
        routed = common.split("def get_source_adapter_routed(", 1)[1]
        routed = routed.split("def read_source_jdbc(", 1)[0]
        self.assertIn("require_connection_id(", routed)
        self.assertIn("control_repo().get_source_table(conn_id, src_id)", routed)
        self.assertIn("assert_current_source_table_identity(control_row, connection)",
                  routed)
        self.assertNotIn("get_source_adapter_for_row(", routed)

    def test_registered_connection_metadata_is_authoritative(self):
        common = shared_nb("_common.py")
        routed = common.split("def get_source_adapter_routed(", 1)[1]
        routed = routed.split("def read_source_jdbc(", 1)[0]
        self.assertIn("assert_source_identity_match(d, connection)", routed)
        self.assertNotIn("source_database=d.get", routed)
        builder = common.split("def get_source_adapter_for_connection(", 1)[1]
        builder = builder.split("def get_source_adapter_routed(", 1)[0]
        self.assertIn("source_database override does not match", builder)
        self.assertIn("database = registered_database", builder)

    def test_shared_operational_notebooks_have_no_source_branches(self):
        for name in self.ROUTED:
            code = shared_nb(name).lower()
            self.assertNotRegex(
                code,
                r"if\s+[^\n]*(source_system|src_system)\s*==\s*['\"]",
                name)

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

    def test_identity_backfill_is_explicit_only(self):
        source = shared_nb("NB00_ControlTableInit.py")
        self.assertIn("Identity upgrades are never performed", source)
        self.assertIn("NB_MigrateSourceTableIdentityV2", source)
        self.assertNotIn("_sid = compute_source_table_id", source)

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
            self.assertIn("require_connection_id(CONNECTION_ID", code, name)
            self.assertIn("connection_id = {escape_string_literal(connection_id)}",
                          code, name)
            self.assertNotIn("if CONNECTION_ID else", code, name)

    def test_provisioning_scopes_active_tables(self):
        code = shared_nb("NB08_TargetProvisioning.py")
        self.assertIn("repo.active_tables_for_connection(", code)
        self.assertIn("require_connection_id(CONNECTION_ID", code)

    def test_onboarding_retries_replace_only_the_connection_slice(self):
        for name, table in (
                ("NB02_TypeNormalization.py", "normalized_source_inventory"),
                ("NB03_MappingRulesGeneration.py", "resolved_column_mappings"),
                ("NB04_MappingValidation.py", "mapping_validation_results"),
                ("NB07_TableDecisionGeneration.py", "table_load_decisions")):
            code = shared_nb(name)
            delete = code.split(f"DELETE FROM {{ctrl('{table}')}}", 1)[1]
            delete = delete.split('"""', 1)[0]
            self.assertIn("WHERE run_id =", delete, name)
            self.assertIn("AND connection_id =", delete, name)

    def test_full_load_scopes_active_tables(self):
        code = shared_nb("NB09_FullLoad.py")
        self.assertIn("repo.get_source_table(connection_id, only_id)", code)
        self.assertIn("require_valid_connection(connection_id)", code)
        self.assertNotIn("repo.active_tables(", code)

    def test_global_full_load_uses_latest_owned_onboarding_metadata(self):
        code = shared_nb("NB09_FullLoad.py")
        mappings = code.split("latest_mappings = spark.sql", 1)[1]
        mappings = mappings.split('""").collect()', 1)[0]
        self.assertIn("connection_id =", mappings)
        self.assertIn("source_table_id =", mappings)
        self.assertIn("ROW_NUMBER() OVER", mappings)
        self.assertNotIn("WHERE run_id =", mappings)

    def test_full_load_blocks_target_collision_before_adapter_creation(self):
        code = shared_nb("NB09_FullLoad.py")
        collision = code.index("target_owners = spark.sql")
        adapter = code.index("get_source_adapter_for_connection(connection)")
        self.assertLess(collision, adapter)
        self.assertIn("target FQN collision", code)

    def test_delta_queue_carries_connection_id(self):
        code = shared_nb("NB11a_DeltaSyncPrep.py")
        self.assertIn("connection_id=conn_id", code)
        self.assertIn('StructField("connection_id"', code)
        self.assertIn("JOIN {ctrl('source_connection')} sc", code)
        self.assertIn("sc.connection_status = 'VALID'", code)
        self.assertNotIn("if CONNECTION_ID else", code)

    def test_delta_prep_uses_latest_safe_owned_mappings(self):
        code = shared_nb("NB11a_DeltaSyncPrep.py")
        block = code.split("latest_mapping_rows = spark.sql", 1)[1]
        block = block.split('""").collect()', 1)[0]
        self.assertIn("ROW_NUMBER() OVER", block)
        self.assertIn("connection_id =", block)
        self.assertIn("source_table_id =", block)
        self.assertNotIn("AND mapping_status = 'AUTO'", block)
        self.assertIn("missing_projected_pk", code)

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
        queue_query = self.SRC.split("# Per-table ForEach scoping", 1)[1]
        queue_query = queue_query.split('""").collect()', 1)[0]
        self.assertIn("run_id =", queue_query)
        self.assertIn("AND connection_id =", queue_query)
        self.assertIn("AND source_table_id = {escape_string_literal(only_id)}",
                      queue_query)

    def test_scoped_run_requires_exactly_one_queued_row(self):
        self.assertIn("resolved to {len(queue)} QUEUED rows", self.SRC)
        self.assertIn("expected exactly 1", self.SRC)

    def test_retry_delta_apply_requires_parent_run(self):
        self.assertIn("RETRY_DELTA_APPLY requires parent_run_id", self.SRC)
        self.assertIn("Delta work item requires source_table_id", self.SRC)
        self.assertIn("require_connection_id(CONNECTION_ID", self.SRC)

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
        self.assertIn("t.run_id = s.run_id", block)
        self.assertIn("t.connection_id = s.connection_id", block)
        self.assertIn("t.source_table_id = s.source_table_id", block)
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
        self.assertIn("assert_current_source_table_identity", block)
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
        self.assertIn("get_source_table(connection_id, only_id)", code)

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
        self.assertIn("failcls.build_retry_collections(", self.SRC)

    def test_additional_retry_semantics(self):
        import inspect
        from src import failure_classifier as failcls
        helper = inspect.getsource(failcls.build_retry_item)
        self.assertIn("retry_count = previous_attempt - 1", helper)
        self.assertIn("retry_count >= int(max_retries)", helper)

    def test_safe_recovery_actions_remain_selectable(self):
        import inspect
        from src import failure_classifier as failcls
        helper = inspect.getsource(failcls.build_retry_item)
        self.assertIn("SAFE_STATE_RECOVERY_ACTIONS", helper)
        self.assertIn("RETRY_CHECKPOINT_ONLY", helper)
        self.assertIn("RETRY_QUEUE_FINALIZATION_ONLY", helper)

    def test_deterministic_latest_attempt_ordering(self):
        self.assertIn(
            "PARTITION BY connection_id, source_table_id, operation", self.SRC)
        self.assertIn("COALESCE(attempt_number, 1) DESC", self.SRC)
        self.assertIn("ended_ts DESC NULLS LAST", self.SRC)
        self.assertIn("started_ts DESC NULLS LAST", self.SRC)
        self.assertIn("run_id DESC", self.SRC)

    def test_pipeline_and_exact_operation_filters(self):
        self.assertIn("operations_for_pipeline(pipeline_name)", self.SRC)
        self.assertIn("operation IN ({owned_sql})", self.SRC)
        self.assertIn("operation = {escape_string_literal(operation_filter)}",
                      self.SRC)

    def test_manual_review_is_a_separate_task_value(self):
        self.assertIn('"manual_review_items": manual_review_items', self.SRC)
        self.assertIn('_set_json_task_value_if_fits("manual_review_items"',
                      self.SRC)

    def test_no_raw_error_message_in_output(self):
        result_block = self.SRC.split("result = {", 1)[1]
        self.assertNotIn('"error_message"', result_block)


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

    def test_reconciliation_summary_replacement_handles_empty_rerun(self):
        code = shared_nb("NB12_ValidationAndReconciliation.py")
        delete = code.index("DELETE FROM {ctrl('reconciliation_results')}")
        write_guard = code.index("if results:")
        self.assertLess(delete, write_guard)


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

    def test_operational_history_is_partitioned_and_joined_by_connection(self):
        self.assertNotIn("PARTITION BY source_table_id", self.SRC)
        self.assertIn("PARTITION BY connection_id, source_table_id", self.SRC)
        for predicate in (
                "c.connection_id = l.connection_id",
                "ir.connection_id = c.connection_id",
                "er.connection_id = c.connection_id",
                "dq.connection_id = c.connection_id"):
            self.assertIn(predicate, self.SRC)

    def test_safe_connection_metadata_is_exposed_without_scope(self):
        for field in ("connection_name", "connection_status", "is_active",
                      "last_validated_ts"):
            self.assertIn(field, self.SRC)
        self.assertNotIn("secret_scope", self.SRC)


class TestNotification(unittest.TestCase):
    def test_distinct_failed_tables_reported(self):
        code = shared_nb("NB16_NotifyFailures.py")
        for token in ("distinct_failed_tables", "table_run_failures=",
                      "reconciliation_failures=", "dq_rule_failures="):
            self.assertIn(token, code)
        self.assertIn('(r["connection_id"], r["source_table_id"])', code)
        self.assertIn("only_connection_ids", code)

    def test_webhook_never_printed(self):
        code = shared_nb("NB16_NotifyFailures.py")
        self.assertNotIn("print(webhook", code)
        self.assertIn("never printed", code)


class TestDiagnostics(unittest.TestCase):
    def test_counters_initialized(self):
        for token in SOURCE_TOKENS:
            code = source_nb(token, "TEST_CONNECTION.py")
            self.assertIn("sample_count = 0", code)
            self.assertIn("sample_columns = []", code)
            self.assertIn("sample_query_succeeded = False", code)
            self.assertIn("meta_count = 0", code)
            self.assertIn("pk_count = 0", code)

    def test_sample_values_default_to_hidden(self):
        for token in SOURCE_TOKENS:
            code = source_nb(token, "TEST_CONNECTION.py")
            self.assertIn(
                'widgets.dropdown("show_sample_values", "false"', code,
                token)
            display_block = code.split("if show_sample_values:")[-1]
            self.assertIn("sample_df.show(", display_block, token)
            before_guard = code.split("if show_sample_values:")[-2]
            self.assertNotIn("sample_df.show(", before_guard, token)

    def test_default_sample_output_contains_no_row_values(self):
        for token in SOURCE_TOKENS:
            code = source_nb(token, "TEST_CONNECTION.py")
            self.assertIn('print("Sample query succeeded.")', code, token)
            self.assertIn('print("Sample rows returned:", sample_count)',
                          code, token)
            self.assertIn('print("Sample column names:", sample_columns)',
                          code, token)
            self.assertIn("approved non-sensitive test data", code, token)

    def test_registered_connection_is_authority(self):
        for token in SOURCE_TOKENS:
            code = source_nb(token, "TEST_CONNECTION.py")
            self.assertIn("if CONNECTION_ID:", code)
            self.assertIn("get_source_adapter_for_connection(", code)
            self.assertIn('test_server = cd.get("source_server")', code)
            self.assertIn('test_database = cd.get("source_database")', code)

    def test_sqlserver_registered_mode_needs_no_database_widget(self):
        code = source_nb("sqlserver", "TEST_CONNECTION.py")
        registered = code.split("if CONNECTION_ID:")[1].split(
            "elif allow_legacy_mode:")[0]
        # The widget is never read in the registered branch.
        self.assertNotIn('dbutils.widgets.get("test_database")', registered)
        self.assertIn("legacy mode requires the test_database widget", code)

    def test_legacy_diagnostic_mode_is_explicit_opt_in(self):
        for token in SOURCE_TOKENS:
            code = source_nb(token, "TEST_CONNECTION.py")
            self.assertIn(
                'widgets.dropdown("allow_legacy_mode", "false"', code, token)
            self.assertIn("elif allow_legacy_mode:", code, token)
            self.assertIn("connection_id is required", code, token)

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
    def test_connection_starts_inactive_until_probe_succeeds(self):
        for token in SOURCE_TOKENS:
            code = source_nb(token, "NB00A_UpsertAndValidateConnection.py")
            self.assertIn('"connection_status": "REGISTERED"', code, token)
            self.assertIn('"is_active": False', code, token)
            self.assertIn(
                'update_connection_status(connection_id, "VALID"', code,
                token)
            self.assertIn(
                'update_connection_status(connection_id, "FAILED"', code,
                token)

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
