"""
Unit tests for failure classification and retry recovery mapping (Commit 4).
"""

import os
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.join(os.path.dirname(HERE), "src")
for p in (SRC, os.path.dirname(HERE)):
    if p not in sys.path:
        sys.path.insert(0, p)

import failure_classifier as fc  # noqa: E402


def _failed_row(operation, source_table_id="table_001", attempt_number=1,
                failure_stage=fc.SOURCE_READ, retry_eligible=True,
                run_id="run_1", ended_ts="2026-01-01T00:00:02Z",
                started_ts="2026-01-01T00:00:01Z",
                lower_watermark=None, upper_watermark=None,
                connection_id="connection_1"):
    return {
        "run_id": run_id,
        "source_table_id": source_table_id,
        "connection_id": connection_id,
        "operation": operation,
        "failure_stage": failure_stage,
        "error_category": fc.TRANSIENT_CONNECTION,
        "retry_eligible": retry_eligible,
        "attempt_number": attempt_number,
        "lower_watermark": lower_watermark,
        "upper_watermark": upper_watermark,
        "started_ts": started_ts,
        "ended_ts": ended_ts,
        "status": "FAILED",
    }


class TestClassifyFailure(unittest.TestCase):
    def test_timeout_is_retryable(self):
        c = fc.classify_failure(Exception("Read timed out"), fc.SOURCE_READ)
        self.assertEqual(c.category, fc.TIMEOUT)
        self.assertTrue(c.retry_eligible)

    def test_transient_connection_retryable(self):
        c = fc.classify_failure(Exception("Connection refused: connect"), fc.CONNECTION)
        self.assertEqual(c.category, fc.TRANSIENT_CONNECTION)
        self.assertTrue(c.retry_eligible)

    def test_transient_compute_retryable(self):
        c = fc.classify_failure(Exception("java.lang.OutOfMemoryError"), fc.TARGET_WRITE)
        self.assertEqual(c.category, fc.TRANSIENT_COMPUTE)
        self.assertTrue(c.retry_eligible)

    def test_source_permission_not_retryable(self):
        c = fc.classify_failure(Exception("ORA-01031: insufficient privileges"),
                                fc.SOURCE_READ)
        self.assertEqual(c.category, fc.SOURCE_PERMISSION)
        self.assertFalse(c.retry_eligible)

    def test_object_missing_not_retryable(self):
        c = fc.classify_failure(Exception("ORA-00942: table or view does not exist"),
                                fc.METADATA)
        self.assertEqual(c.category, fc.SOURCE_OBJECT_MISSING)
        self.assertFalse(c.retry_eligible)

    def test_mapping_error_not_retryable(self):
        c = fc.classify_failure(Exception("blocked datatype requires an explicit mapping"),
                                fc.MAPPING)
        self.assertEqual(c.category, fc.MAPPING_ERROR)
        self.assertFalse(c.retry_eligible)

    def test_configuration_error_not_retryable(self):
        c = fc.classify_failure(Exception("MERGE strategy requires primary_key_columns"),
                                fc.PROVISIONING)
        self.assertEqual(c.category, fc.CONFIGURATION_ERROR)
        self.assertFalse(c.retry_eligible)

    def test_reconciliation_error_not_retryable(self):
        c = fc.classify_failure(Exception("some generic failure"), fc.RECONCILIATION)
        self.assertEqual(c.category, fc.RECONCILIATION_ERROR)
        self.assertFalse(c.retry_eligible)

    def test_checkpoint_error_not_retryable(self):
        c = fc.classify_failure(Exception("generic"), fc.CHECKPOINT)
        self.assertEqual(c.category, fc.CHECKPOINT_ERROR)
        self.assertFalse(c.retry_eligible)

    def test_target_write_retryable_only_when_idempotent(self):
        idem = fc.classify_failure(Exception("write failed"), fc.TARGET_WRITE,
                                   idempotent=True)
        self.assertEqual(idem.category, fc.TARGET_WRITE_ERROR)
        self.assertTrue(idem.retry_eligible)
        non = fc.classify_failure(Exception("write failed"), fc.TARGET_WRITE,
                                  idempotent=False)
        self.assertFalse(non.retry_eligible)

    def test_message_is_sanitized(self):
        c = fc.classify_failure(
            Exception("login failed for user=sa;password=Secret123!"), fc.CONNECTION)
        self.assertNotIn("Secret123", c.sanitized_message)
        self.assertIn("***", c.sanitized_message)

    def test_unknown_stage_normalized(self):
        c = fc.classify_failure(Exception("weird"), "NOT_A_STAGE")
        self.assertEqual(c.stage, fc.UNKNOWN)


class TestRecoveryAction(unittest.TestCase):
    def test_checkpoint_only_does_not_reapply(self):
        self.assertEqual(fc.recovery_action("DELTA_MERGE", fc.CHECKPOINT),
                         "RETRY_CHECKPOINT_ONLY")

    def test_finalization_only_does_not_reapply(self):
        self.assertEqual(fc.recovery_action("DELTA_APPEND", fc.QUEUE_FINALIZATION),
                         "RETRY_QUEUE_FINALIZATION_ONLY")

    def test_full_load_retries_full(self):
        self.assertEqual(fc.recovery_action("FULL_LOAD", fc.SOURCE_READ),
                         "RETRY_FULL_LOAD")

    def test_delta_retries_delta(self):
        self.assertEqual(fc.recovery_action("DELTA_MERGE", fc.SOURCE_READ),
                         "RETRY_DELTA_APPLY")

    def test_etl_retries_etl(self):
        self.assertEqual(fc.recovery_action("ETL_INCREMENTAL", fc.DQ_VALIDATION),
                         "RETRY_ETL")

    def test_generic_etl_retries_etl(self):
        self.assertEqual(fc.recovery_action("ETL", fc.ETL_READ), "RETRY_ETL")

    def test_unknown_is_manual(self):
        self.assertEqual(fc.recovery_action("SOMETHING", fc.METADATA), "MANUAL_REVIEW")


class TestRetrySelectionPolicy(unittest.TestCase):
    def test_same_table_operations_remain_independent_before_filtering(self):
        selected = fc.latest_failed_attempts([
            _failed_row("FULL_LOAD"),
            _failed_row("ETL_INCREMENTAL", failure_stage=fc.ETL_READ),
        ])
        self.assertEqual(
            {fc.retry_work_identity(row) for row in selected},
            {("connection_1", "table_001", "FULL_LOAD"),
             ("connection_1", "table_001", "ETL_INCREMENTAL")})

    def test_same_table_operation_on_two_connections_remains_independent(self):
        selected = fc.latest_failed_attempts([
            _failed_row("FULL_LOAD", connection_id="ORA_FIN_READ"),
            _failed_row("FULL_LOAD", connection_id="ORA_FIN_MIGRATION"),
        ])
        self.assertEqual(len(selected), 2)
        self.assertEqual(
            {row["connection_id"] for row in selected},
            {"ORA_FIN_READ", "ORA_FIN_MIGRATION"})

    def test_same_table_ingest_operations_are_both_selected(self):
        selected = fc.select_failed_attempts([
            _failed_row("FULL_LOAD"),
            _failed_row("CHECKPOINT_RECOVERY", failure_stage=fc.CHECKPOINT,
                        retry_eligible=False),
        ], "INGEST")
        self.assertEqual(
            {row["operation"] for row in selected},
            {"FULL_LOAD", "CHECKPOINT_RECOVERY"})

    def test_latest_attempt_is_selected_per_operation(self):
        selected = fc.latest_failed_attempts([
            _failed_row("FULL_LOAD", attempt_number=1),
            _failed_row("FULL_LOAD", attempt_number=2),
            _failed_row("ETL_INCREMENTAL", attempt_number=1,
                        failure_stage=fc.ETL_READ),
        ])
        attempts = {row["operation"]: row["attempt_number"] for row in selected}
        self.assertEqual(attempts, {"FULL_LOAD": 2, "ETL_INCREMENTAL": 1})

    def test_attempts_do_not_cross_operations(self):
        rows = [
            _failed_row("FULL_LOAD", attempt_number=3),
            _failed_row("ETL_INCREMENTAL", attempt_number=1,
                        failure_stage=fc.ETL_READ),
        ]
        etl_rows = fc.select_failed_attempts(rows, "ETL")
        worklist, manual_items, _duplicates = fc.build_retry_collections(
            etl_rows, "child", "parent", "ETL", max_retries=3)
        self.assertEqual(manual_items, [])
        self.assertEqual(worklist[0]["previous_attempt_number"], 1)
        self.assertEqual(worklist[0]["attempt_number"], 2)

    def test_blank_operation_returns_all_owned_operations(self):
        rows = [
            _failed_row("FULL_LOAD"),
            _failed_row("DELTA_MERGE", source_table_id="table_002"),
            _failed_row("ETL_INCREMENTAL", failure_stage=fc.ETL_READ),
        ]
        selected = fc.select_failed_attempts(rows, "INGEST", "")
        self.assertEqual(
            {row["operation"] for row in selected},
            {"FULL_LOAD", "DELTA_MERGE"})

    def test_exact_operation_filter(self):
        rows = [
            _failed_row("FULL_LOAD"),
            _failed_row("DELTA_FULL_REFRESH", source_table_id="table_002"),
        ]
        selected = fc.select_failed_attempts(rows, "INGEST", "full_load")
        self.assertEqual([row["operation"] for row in selected], ["FULL_LOAD"])

    def test_pipeline_filtering_is_bidirectional(self):
        rows = [
            _failed_row("FULL_LOAD"),
            _failed_row("ETL_INCREMENTAL", failure_stage=fc.ETL_READ),
        ]
        self.assertEqual(
            [row["operation"] for row in
             fc.select_failed_attempts(rows, "INGEST")],
            ["FULL_LOAD"])
        self.assertEqual(
            [row["operation"] for row in fc.select_failed_attempts(rows, "ETL")],
            ["ETL_INCREMENTAL"])

    def test_pipeline_operation_mismatch_fails(self):
        with self.assertRaisesRegex(ValueError, "does not belong"):
            fc.select_failed_attempts([], "ETL", "FULL_LOAD")

    def test_unknown_pipeline_fails(self):
        with self.assertRaisesRegex(ValueError, "Unsupported pipeline_name"):
            fc.select_failed_attempts([], "UNKNOWN")

    def test_attempt_number_precedes_timestamps(self):
        selected = fc.latest_failed_attempts([
            _failed_row("FULL_LOAD", attempt_number=1,
                        ended_ts="2026-01-03T00:00:00Z", run_id="run_z"),
            _failed_row("FULL_LOAD", attempt_number=2,
                        ended_ts="2026-01-01T00:00:00Z", run_id="run_a"),
        ])
        self.assertEqual(selected[0]["attempt_number"], 2)

    def test_connection_is_part_of_retry_identity(self):
        selected = fc.latest_failed_attempts([
            _failed_row("FULL_LOAD", run_id="run_a", connection_id="old"),
            _failed_row("FULL_LOAD", run_id="run_b", connection_id="new"),
        ])
        self.assertEqual(len(selected), 2)

    def test_retry_limit_is_operation_specific(self):
        full_work, full_manual, _ = fc.build_retry_collections(
            [_failed_row("FULL_LOAD", attempt_number=4)],
            "child", "parent", "INGEST", max_retries=3)
        etl_work, etl_manual, _ = fc.build_retry_collections(
            [_failed_row("ETL_INCREMENTAL", attempt_number=1,
                         failure_stage=fc.ETL_READ)],
            "child", "parent", "ETL", max_retries=3)
        self.assertEqual(full_work, [])
        self.assertEqual(full_manual[0]["reason"], fc.MAX_RETRIES_REACHED)
        self.assertEqual(etl_manual, [])
        self.assertEqual(etl_work[0]["attempt_number"], 2)

    def test_manual_review_is_not_executable(self):
        worklist, manual_items, _ = fc.build_retry_collections(
            [_failed_row("DELTA_MERGE", failure_stage=fc.RECONCILIATION,
                         retry_eligible=False)],
            "child", "parent", "INGEST", max_retries=3)
        self.assertEqual(worklist, [])
        self.assertEqual(manual_items[0]["reason"], fc.NOT_RETRY_ELIGIBLE)
        self.assertEqual(set(manual_items[0]), set(fc.MANUAL_REVIEW_FIELDS))

    def test_state_only_recovery_remains_selectable(self):
        worklist, manual_items, _ = fc.build_retry_collections(
            [_failed_row("CHECKPOINT_RECOVERY", failure_stage=fc.CHECKPOINT,
                         retry_eligible=False)],
            "child", "parent", "INGEST", max_retries=3)
        self.assertEqual(manual_items, [])
        self.assertEqual(worklist[0]["recovery_action"],
                         fc.RETRY_CHECKPOINT_ONLY)

    def test_worklist_uniqueness(self):
        duplicate = _failed_row("FULL_LOAD")
        worklist, manual_items, duplicates = fc.build_retry_collections(
            [duplicate, duplicate], "child", "parent", "INGEST", max_retries=3)
        self.assertEqual(manual_items, [])
        self.assertEqual(len(worklist), 1)
        self.assertEqual(
            duplicates,
            [("connection_1", "table_001", "FULL_LOAD",
              fc.RETRY_FULL_LOAD)])

    def test_retry_row_requires_connection_id(self):
        with self.assertRaisesRegex(ValueError, "connection_id"):
            fc.retry_work_identity(_failed_row("FULL_LOAD", connection_id=""))

    def test_executable_output_contract(self):
        work_item, manual_item = fc.build_retry_item(
            _failed_row("FULL_LOAD"), "child", "parent", "INGEST", 3)
        self.assertIsNone(manual_item)
        self.assertEqual(tuple(work_item), fc.WORKLIST_FIELDS)
        self.assertEqual(work_item["run_id"], "child")
        self.assertEqual(work_item["parent_run_id"], "parent")
        self.assertEqual(work_item["pipeline_name"], "INGEST")

    def test_frozen_bounds_are_retained(self):
        delta_item, _ = fc.build_retry_item(
            _failed_row("DELTA_MERGE", lower_watermark="lower-delta",
                        upper_watermark="upper-delta"),
            "child", "parent", "INGEST", 3)
        etl_item, _ = fc.build_retry_item(
            _failed_row("ETL_INCREMENTAL", failure_stage=fc.ETL_READ,
                        lower_watermark="lower-etl", upper_watermark="upper-etl"),
            "child", "parent", "ETL", 3)
        self.assertEqual(
            (delta_item["retry_lower_watermark"],
             delta_item["retry_upper_watermark"]),
            ("lower-delta", "upper-delta"))
        self.assertEqual(
            (etl_item["retry_lower_watermark"],
             etl_item["retry_upper_watermark"]),
            ("lower-etl", "upper-etl"))


if __name__ == "__main__":
    unittest.main()
