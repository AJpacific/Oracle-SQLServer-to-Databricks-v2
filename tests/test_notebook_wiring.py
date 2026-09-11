"""
Static source checks for notebook-level defects that cannot be unit tested
without Spark: connection-id routing, connection_id propagation, run_id
priority, targeted is_selected, ETL ordering guarantees, terminology, and the
absence of hard-coded environment values.

These read the notebook source text; they do not execute any notebook.
"""

import os
import re
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
NB = os.path.join(ROOT, "notebooks")


def read_nb(name):
    with open(os.path.join(NB, name), "r", encoding="utf-8") as fh:
        return fh.read()


def all_notebooks():
    return [f for f in os.listdir(NB) if f.endswith(".py")]


class TestConnectionRouting(unittest.TestCase):
    PRODUCTION_SOURCE_NOTEBOOKS = [
        "NB01_SourceInventory.py",
        "NB09_FullLoad.py",
        "NB10_PostFullLoadState.py",
        "NB11a_DeltaSyncPrep.py",
        "NB11b_DeltaSyncApply.py",
    ]

    def test_production_notebooks_use_routed_adapter(self):
        for name in self.PRODUCTION_SOURCE_NOTEBOOKS:
            src = read_nb(name)
            self.assertIn("get_source_adapter_routed(", src, name)

    def test_no_legacy_row_adapter_in_production_notebooks(self):
        # The legacy helper may only remain as the internal fallback in _common.
        for name in all_notebooks():
            if name == "_common.py":
                continue
            src = read_nb(name)
            self.assertNotIn("get_source_adapter_for_row(", src, name)

    def test_common_keeps_documented_fallback(self):
        src = read_nb("_common.py")
        self.assertIn("def get_source_adapter_for_row(", src)
        self.assertIn("def get_source_adapter_routed(", src)

    def test_routing_requires_valid_and_guards_conflict(self):
        src = read_nb("_common.py")
        self.assertIn("assert_source_system_match", src)
        self.assertIn("not found in source_connection", src)
        self.assertIn("is not VALID", src)

    def test_no_legacy_oracle_read_jdbc_in_production(self):
        for name in all_notebooks():
            if name in ("_common.py", "00_TEST_ORACLE_CONNECTION.py"):
                continue
            src = read_nb(name)
            self.assertIsNone(re.search(r"(?<![._\w])read_jdbc\(", src), name)


class TestConnectionIdPropagation(unittest.TestCase):
    def test_inventory_writes_connection_id(self):
        src = read_nb("NB01_SourceInventory.py")
        self.assertIn('StructField("connection_id"', src)
        self.assertIn("conn_id", src)

    def test_normalized_inventory_writes_connection_id(self):
        self.assertIn('StructField("connection_id"',
                      read_nb("NB02_TypeNormalization.py"))

    def test_mappings_write_connection_id(self):
        self.assertIn('StructField("connection_id"',
                      read_nb("NB03_MappingRulesGeneration.py"))

    def test_validation_results_write_connection_id(self):
        self.assertIn('"connection_id"', read_nb("NB04_MappingValidation.py"))

    def test_decisions_and_review_queue_write_connection_id(self):
        src = read_nb("NB07_TableDecisionGeneration.py")
        self.assertIn('StructField("connection_id"', src)
        self.assertIn('"connection_id",', src)

    def test_delta_queue_carries_connection_id(self):
        src = read_nb("NB11a_DeltaSyncPrep.py")
        self.assertIn("connection_id=d.get(\"connection_id\")", src)
        self.assertIn('StructField("connection_id"', src)

    def test_reconciliation_results_carry_connection_id(self):
        src = read_nb("NB12_ValidationAndReconciliation.py")
        self.assertIn("c.connection_id", src)
        self.assertIn('"connection_id"', src)

    def test_bulk_notebooks_support_connection_scoping(self):
        self.assertIn("connection_id=(CONNECTION_ID or None)",
                      read_nb("NB01_SourceInventory.py"))
        self.assertIn("connection_id=(CONNECTION_ID or None)",
                      read_nb("NB09_FullLoad.py"))


class TestRegistration(unittest.TestCase):
    def test_backfills_null_connection_id_only(self):
        src = read_nb("NB01B_RegisterSelectedTables.py")
        self.assertIn("WHEN MATCHED", src)
        self.assertIn("t.connection_id IS NULL OR trim(t.connection_id) = ''", src)
        self.assertIn("t.connection_id = s.connection_id", src)

    def test_conflicting_connection_id_blocked(self):
        src = read_nb("NB01B_RegisterSelectedTables.py")
        self.assertIn("connection conflict", src)
        self.assertIn("conflicts", src)

    def test_operational_state_not_overwritten(self):
        src = read_nb("NB01B_RegisterSelectedTables.py")
        matched = src.split("WHEN MATCHED")[1].split("WHEN NOT MATCHED")[0]
        for protected in ("last_watermark_value", "initial_load_completed",
                          "target_table", "target_schema", "current_status",
                          "load_strategy", "last_successful_run_id"):
            self.assertNotIn(protected, matched)

    def test_is_selected_is_targeted_not_blanket(self):
        src = read_nb("NB01B_RegisterSelectedTables.py")
        self.assertNotIn("SET is_selected = true\n        WHERE", src)
        self.assertIn("_selected_objects", src)
        self.assertIn("t.object_name  = s.object_name", src)


class TestRunId(unittest.TestCase):
    def test_widget_has_priority(self):
        src = read_nb("_common.py")
        body = src.split("def get_run_id():")[1].split("def set_task_value")[0]
        widget_pos = body.index('dbutils.widgets.get("run_id")')
        task_pos = body.index("dbutils.jobs.taskValues.get")
        new_pos = body.index("return new_run_id()")
        self.assertLess(widget_pos, task_pos)
        self.assertLess(task_pos, new_pos)

    def test_retry_child_run_id_is_passed_explicitly(self):
        self.assertIn('dbutils.widgets.get("run_id")',
                      read_nb("NB15_BronzeToSilverETL.py"))


class TestAssessmentIdempotency(unittest.TestCase):
    def test_source_assessment_uses_merge(self):
        src = read_nb("NB01A_SourceAssessment.py")
        self.assertIn("MERGE INTO", src)
        self.assertIn("_assessed_objects", src)
        self.assertNotIn('mode("append")', src)

    def test_sql_object_assessment_uses_merge_and_keeps_review(self):
        src = read_nb("NB13_SQLObjectAssessmentAndConversion.py")
        self.assertIn("MERGE INTO", src)
        self.assertIn("APPROVED", src)
        self.assertIn("REJECTED", src)

    def test_unknown_sqlserver_type_not_defaulted_to_procedure(self):
        src = read_nb("NB13_SQLObjectAssessmentAndConversion.py")
        self.assertNotIn('_SS_TYPE.get((m["OBJECT_TYPE"] or "").strip(), "PROCEDURE")',
                         src)
        self.assertIn("skipped_types", src)

    def test_missing_definition_is_unable_to_assess(self):
        src = read_nb("NB13_SQLObjectAssessmentAndConversion.py")
        self.assertIn('complexity = "UNABLE_TO_ASSESS"', src)

    def test_package_and_package_body_distinct(self):
        src = read_nb("NB13_SQLObjectAssessmentAndConversion.py")
        self.assertIn('"PACKAGE_BODY"', src)
        self.assertIn("_ORACLE_TYPES", src)


class TestEtlOrdering(unittest.TestCase):
    SRC = None

    @classmethod
    def setUpClass(cls):
        cls.SRC = read_nb("NB15_BronzeToSilverETL.py")

    def test_duplicate_keys_checked_before_merge(self):
        dup_pos = self.SRC.index("dup_valid = (valid_out.groupBy")
        stage_pos = self.SRC.index("_etl_stage\"")
        merge_pos = self.SRC.index("ddl.build_merge_sql(silver_catalog")
        self.assertLess(dup_pos, stage_pos)
        self.assertLess(dup_pos, merge_pos)

    def test_duplicate_keys_fail_before_merge(self):
        self.assertIn("MERGE not executed and ETL watermark unchanged", self.SRC)

    def test_invalid_rules_fail_before_silver_write(self):
        invalid_pos = self.SRC.index("if invalid_rules:")
        write_pos = self.SRC.index("current_stage = failcls.SILVER_WRITE")
        self.assertLess(invalid_pos, write_pos)
        self.assertIn("DQ_CONFIG_ERROR", self.SRC)

    def test_single_failure_log_guard(self):
        self.assertIn("failure_already_logged", self.SRC)
        self.assertIn("if not failure_already_logged:", self.SRC)

    def test_typed_watermark_comparison(self):
        self.assertIn("TimestampType", self.SRC)
        self.assertIn("DateType", self.SRC)
        self.assertIn('.cast(cast_to)', self.SRC)
        self.assertIn("only DATE and TIMESTAMP are supported", self.SRC)

    def test_quarantine_is_idempotent_per_run_and_table(self):
        self.assertIn("DELETE FROM", self.SRC)
        self.assertIn("dq_quarantine", self.SRC)

    def test_quarantine_disabled_still_counts_rejects(self):
        self.assertIn("counted for reconciliation but NOT persisted", self.SRC)

    def test_reconciliation_precedes_checkpoint(self):
        recon_pos = self.SRC.index("if not recon_result.passed:")
        ckpt_pos = self.SRC.index("current_stage = failcls.CHECKPOINT")
        self.assertLess(recon_pos, ckpt_pos)

    def test_unique_duplicate_helper_columns(self):
        self.assertIn('f"_dupcount_{idx}"', self.SRC)

    def test_etl_never_builds_a_source_adapter(self):
        self.assertNotIn("get_source_adapter", self.SRC)
        self.assertNotIn("dbutils.secrets", self.SRC)


class TestIngestFailureStages(unittest.TestCase):
    def test_full_load_tracks_stage(self):
        src = read_nb("NB09_FullLoad.py")
        self.assertIn("current_stage = failcls.TARGET_WRITE", src)
        self.assertIn("current_stage = failcls.RECONCILIATION", src)
        self.assertIn("classify_failure(e, current_stage", src)

    def test_delta_apply_tracks_stage(self):
        src = read_nb("NB11b_DeltaSyncApply.py")
        for stage in ("SOURCE_READ", "TARGET_WRITE", "RECONCILIATION",
                      "CHECKPOINT", "QUEUE_FINALIZATION"):
            self.assertIn(f"current_stage = failcls.{stage}", src)
        self.assertIn("classify_failure(apply_error, current_stage", src)

    def test_table_run_log_status_normalized(self):
        # Detailed operational states live on the control row; the audit status
        # stays SUCCEEDED/FAILED so the retry selector can filter on FAILED.
        src = read_nb("NB09_FullLoad.py")
        self.assertNotIn('"COUNT_MISMATCH", None, started', src)
        self.assertIn('log_run(ident, target_fqn, s_count, t_count, "FAILED"', src)

    def test_single_table_scope_must_resolve_exactly_one(self):
        src = read_nb("NB09_FullLoad.py")
        self.assertIn("expected exactly 1", src)


class TestCanonicalFullLoadCheck(unittest.TestCase):
    def test_nb12_writes_canonical_check_type(self):
        self.assertIn('check_type = "FULL_SNAPSHOT_COUNT"',
                      read_nb("NB12_ValidationAndReconciliation.py"))

    def test_nb10_gates_on_canonical_check_type(self):
        src = read_nb("NB10_PostFullLoadState.py")
        self.assertIn("rr.check_type = 'FULL_SNAPSHOT_COUNT'", src)
        self.assertNotIn("rr.check_type = 'ROW_COUNT'", src)

    def test_dashboard_uses_canonical_check_type(self):
        self.assertIn("FULL_SNAPSHOT_COUNT", read_nb("NB17_DashboardViews.py"))

    def test_nb12_does_not_use_count_comparison_shortcut(self):
        src = read_nb("NB12_ValidationAndReconciliation.py")
        self.assertNotIn("tgt_count >= src_count", src)


class TestDashboardViews(unittest.TestCase):
    SRC = None

    @classmethod
    def setUpClass(cls):
        cls.SRC = read_nb("NB17_DashboardViews.py")

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
        src = read_nb("NB16_NotifyFailures.py")
        self.assertIn("distinct_failed_tables", src)
        self.assertIn("table_run_failures=", src)
        self.assertIn("reconciliation_failures=", src)
        self.assertIn("dq_rule_failures=", src)

    def test_webhook_never_printed(self):
        src = read_nb("NB16_NotifyFailures.py")
        self.assertNotIn("print(webhook", src)
        self.assertIn("never printed", src)


class TestProductionCleanliness(unittest.TestCase):
    def test_no_personal_workspace_path(self):
        for name in all_notebooks():
            self.assertNotIn("/Workspace/Users/", read_nb(name), name)

    def test_no_hardcoded_control_schema(self):
        self.assertNotIn("da_accelerators.control",
                         read_nb("NB00_ControlTableInit.py"))

    def test_seeding_disabled_by_default(self):
        src = read_nb("NB00_ControlTableInit.py")
        self.assertIn('dropdown("seed_poc_rows", "false"', src)
        self.assertIn('dropdown("seed_sqlserver_examples", "false"', src)

    def test_no_ipv4_defaults(self):
        ipv4 = re.compile(r"\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b")
        for name in all_notebooks():
            src = read_nb(name)
            for line in src.splitlines():
                if "widgets.text" in line or "widgets.dropdown" in line:
                    self.assertIsNone(ipv4.search(line), f"{name}: {line}")

    def test_diagnostic_counters_initialized(self):
        for name in ("00_TEST_ORACLE_CONNECTION.py", "00_TEST_SQLSERVER_CONNECTION.py"):
            src = read_nb(name)
            self.assertIn("sample_count = 0", src)
            self.assertIn("meta_count = 0", src)
            self.assertIn("pk_count = 0", src)

    def test_diagnostics_prefer_connection_id(self):
        for name in ("00_TEST_ORACLE_CONNECTION.py", "00_TEST_SQLSERVER_CONNECTION.py"):
            src = read_nb(name)
            self.assertIn("if CONNECTION_ID:", src)
            self.assertIn("get_source_adapter_for_connection(", src)
            self.assertIn("legacy fallback", src)

    def test_diagnostics_reject_mismatched_source_system(self):
        self.assertIn("requires an Oracle connection",
                      read_nb("00_TEST_ORACLE_CONNECTION.py"))
        self.assertIn("requires a ", read_nb("00_TEST_SQLSERVER_CONNECTION.py"))

    def test_legacy_pipeline_terminology_removed(self):
        for name in all_notebooks():
            src = read_nb(name)
            self.assertNotIn("Pipeline 1", src, name)
            self.assertNotIn("Pipeline 2", src, name)


if __name__ == "__main__":
    unittest.main()
