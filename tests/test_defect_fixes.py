"""
Unit tests for the defect-fix pass.

Covered here: full table_run_log persistence (Fix 1), strengthened sanitization
(Fix 27), retry/max-retries semantics (Fix 23), and the DQ rule-validation
hardening used by the ETL fixes (Fix 10/11/21).
"""

import os
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.join(os.path.dirname(HERE), "src")
for p in (SRC, os.path.dirname(HERE)):
    if p not in sys.path:
        sys.path.insert(0, p)

import control_repository as cr  # noqa: E402
import failure_classifier as fc  # noqa: E402
import dq_rules as dq  # noqa: E402


class _CapturingSpark:
    """Minimal stand-in; the audit row itself is built by a pure function."""

    def sql(self, statement):
        return self


class TestTableRunLogSchema(unittest.TestCase):
    def _log(self, fields):
        names = [name for name, _t in cr.TABLE_RUN_LOG_COLUMNS]
        types = {name: t for name, t in cr.TABLE_RUN_LOG_COLUMNS}
        values = dict(zip(names, cr.build_table_run_row(fields)))
        return names, values, types

    def test_all_extended_fields_present(self):
        names, _v, _s = self._log({"run_id": "r1"})
        for expected in (
            "run_id", "source_table_id", "connection_id", "source_system",
            "source_server", "source_database", "source_schema", "source_table",
            "operation", "target_full_name", "source_row_count",
            "target_row_count", "status", "error_message", "attempt_number",
            "failure_stage", "error_category", "retry_eligible", "retry_status",
            "parent_run_id", "lower_watermark", "upper_watermark",
            "extracted_row_count", "staged_row_count", "applied_row_count",
            "rejected_row_count", "started_ts", "ended_ts",
        ):
            self.assertIn(expected, names)

    def test_extended_values_persist(self):
        _n, v, _s = self._log({
            "run_id": "r1", "connection_id": "sqlserver_a",
            "attempt_number": 3, "failure_stage": "TARGET_WRITE",
            "error_category": "TARGET_WRITE_ERROR", "retry_eligible": True,
            "parent_run_id": "parent_1", "extracted_row_count": 10,
            "staged_row_count": 10, "applied_row_count": 9,
            "rejected_row_count": 1, "lower_watermark": "a", "upper_watermark": "b",
        })
        self.assertEqual(v["connection_id"], "sqlserver_a")
        self.assertEqual(v["attempt_number"], 3)
        self.assertEqual(v["failure_stage"], "TARGET_WRITE")
        self.assertIs(v["retry_eligible"], True)
        self.assertEqual(v["parent_run_id"], "parent_1")
        self.assertEqual(v["applied_row_count"], 9)
        self.assertEqual(v["rejected_row_count"], 1)

    def test_legacy_call_still_works(self):
        _n, v, _s = self._log({
            "run_id": "r1", "source_table_id": "sid", "source_system": "oracle",
            "source_schema": "HR", "source_table": "EMP", "operation": "FULL_LOAD",
            "source_row_count": 5, "target_row_count": 5, "status": "SUCCEEDED",
        })
        self.assertEqual(v["operation"], "FULL_LOAD")
        self.assertEqual(v["source_row_count"], 5)
        self.assertIsNone(v["attempt_number"])
        self.assertIsNone(v["retry_eligible"])

    def test_numeric_coercion(self):
        _n, v, _s = self._log({"source_row_count": "12", "attempt_number": "2"})
        self.assertEqual(v["source_row_count"], 12)
        self.assertEqual(v["attempt_number"], 2)

    def test_column_types(self):
        _n, _v, by_name = self._log({"run_id": "r1"})
        self.assertEqual(by_name["attempt_number"], "int")
        self.assertEqual(by_name["retry_eligible"], "boolean")
        for c in ("extracted_row_count", "staged_row_count",
                  "applied_row_count", "rejected_row_count"):
            self.assertEqual(by_name[c], "bigint")

    def test_secret_like_fields_never_written(self):
        names, _v, _s = self._log({
            "run_id": "r1", "password": "tiger", "token": "abc",
            "jdbc_url": "jdbc:oracle:thin:@//h:1521/s", "webhook_url": "https://x",
        })
        for banned in ("password", "token", "jdbc_url", "webhook_url",
                       "user", "username", "secret"):
            self.assertNotIn(banned, names)

    def test_error_message_sanitized_in_repository(self):
        _n, v, _s = self._log({
            "error_message": "login failed for user=sa;password=Secret123!"})
        self.assertNotIn("Secret123", v["error_message"])
        self.assertIn("***", v["error_message"])

    def test_bool_coercion_preserves_null(self):
        self.assertIsNone(cr._to_bool(None))
        self.assertIs(cr._to_bool("true"), True)
        self.assertIs(cr._to_bool("false"), False)


class TestSanitization(unittest.TestCase):
    def test_password_property(self):
        self.assertNotIn("hunter2",
                         fc.sanitize_message("jdbc:x;password=hunter2;"))

    def test_user_property(self):
        self.assertNotIn("scott", fc.sanitize_message("url;user=scott;"))

    def test_bearer_token(self):
        out = fc.sanitize_message("Authorization: Bearer eyJhbGciOiJIUzI1NiJ9")
        self.assertNotIn("eyJhbGciOiJIUzI1NiJ9", out)

    def test_basic_credentials(self):
        out = fc.sanitize_message("Authorization: Basic dXNlcjpwYXNzd29yZA==")
        self.assertNotIn("dXNlcjpwYXNzd29yZA==", out)

    def test_url_userinfo(self):
        out = fc.sanitize_message("https://admin:s3cret@host/path")
        self.assertNotIn("s3cret", out)

    def test_client_secret_and_sas(self):
        out = fc.sanitize_message("client_secret=abc123 sas=xyz789")
        self.assertNotIn("abc123", out)
        self.assertNotIn("xyz789", out)

    def test_webhook_token_redacted(self):
        out = fc.redact_url(
            "https://outlook.office.com/webhook/abc-def/IncomingWebhook/tok123")
        self.assertNotIn("tok123", out)

    def test_non_sensitive_text_readable(self):
        out = fc.sanitize_message(
            "ORA-00942: table or view HR.EMPLOYEES does not exist")
        self.assertIn("HR.EMPLOYEES", out)
        self.assertIn("ORA-00942", out)


class TestMaxRetriesSemantics(unittest.TestCase):
    """max_retries = additional attempts allowed after the first."""

    @staticmethod
    def _allowed(prev_attempt, max_retries):
        retry_count = prev_attempt - 1
        return retry_count < max_retries

    def test_initial_failure_may_retry(self):
        self.assertTrue(self._allowed(1, 3))     # -> attempt 2

    def test_second_and_third_may_retry(self):
        self.assertTrue(self._allowed(2, 3))     # -> attempt 3
        self.assertTrue(self._allowed(3, 3))     # -> attempt 4

    def test_fourth_attempt_is_exhausted(self):
        self.assertFalse(self._allowed(4, 3))    # three retries consumed

    def test_zero_retries_blocks_immediately(self):
        self.assertFalse(self._allowed(1, 0))


class TestSafeRecoverySelectable(unittest.TestCase):
    def test_checkpoint_only_is_safe_recovery(self):
        self.assertEqual(
            fc.recovery_action("CHECKPOINT_COMMIT", fc.CHECKPOINT),
            "RETRY_CHECKPOINT_ONLY")

    def test_finalization_only_is_safe_recovery(self):
        self.assertEqual(
            fc.recovery_action("QUEUE_FINALIZATION", fc.QUEUE_FINALIZATION),
            "RETRY_QUEUE_FINALIZATION_ONLY")

    def test_target_write_failure_maps_to_data_retry(self):
        self.assertEqual(
            fc.recovery_action("FULL_LOAD", fc.TARGET_WRITE), "RETRY_FULL_LOAD")


class TestRuleValidationHardening(unittest.TestCase):
    COLS = {"id", "name", "status"}

    def test_missing_column_fails(self):
        with self.assertRaises(ValueError):
            dq.validate_rule({"rule_type": "NOT_NULL", "column_name": "absent"},
                             available_columns=self.COLS)

    def test_existing_column_ok(self):
        dq.validate_rule({"rule_type": "NOT_NULL", "column_name": "name"},
                         available_columns=self.COLS)

    def test_invalid_data_type_target_fails(self):
        with self.assertRaises(ValueError):
            dq.validate_rule({"rule_type": "DATA_TYPE", "column_name": "id",
                              "rule_value": "SELECT 1"},
                             available_columns=self.COLS)

    def test_valid_data_type_targets(self):
        for t in ("INT", "timestamp", "DECIMAL(10,2)", "string"):
            dq.validate_rule({"rule_type": "DATA_TYPE", "column_name": "id",
                              "rule_value": t}, available_columns=self.COLS)

    def test_duplicate_key_without_keys_fails(self):
        with self.assertRaises(ValueError):
            dq.validate_rule({"rule_type": "DUPLICATE_KEY"},
                             available_columns=self.COLS, primary_key_columns=[])

    def test_duplicate_key_uses_primary_key(self):
        r = dq.validate_rule({"rule_type": "DUPLICATE_KEY"},
                             available_columns=self.COLS,
                             primary_key_columns=["id"])
        self.assertEqual(dq.duplicate_key_columns(r, ["id"]), ["id"])

    def test_duplicate_key_composite_from_rule(self):
        keys = dq.duplicate_key_columns({"column_name": "id, name"}, ["other"])
        self.assertEqual(keys, ["id", "name"])

    def test_duplicate_key_missing_configured_column_fails(self):
        with self.assertRaises(ValueError):
            dq.validate_rule({"rule_type": "DUPLICATE_KEY",
                              "column_name": "id,absent"},
                             available_columns=self.COLS)

    def test_rule_validation_without_columns_is_backward_compatible(self):
        dq.validate_rule({"rule_type": "NOT_NULL", "column_name": "anything"})


if __name__ == "__main__":
    unittest.main()
