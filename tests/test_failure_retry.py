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

    def test_unknown_is_manual(self):
        self.assertEqual(fc.recovery_action("SOMETHING", fc.METADATA), "MANUAL_REVIEW")


if __name__ == "__main__":
    unittest.main()
