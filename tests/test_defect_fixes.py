"""
Unit tests for the defect-fix pass.

Covered here: full table_run_log persistence (Fix 1), strengthened sanitization
(Fix 27), retry/max-retries semantics (Fix 23), and the DQ rule-validation
hardening used by the ETL fixes (Fix 10/11/21).
"""

import ast
import os
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
SRC = os.path.join(ROOT, "src")
for p in (SRC, ROOT):
    if p not in sys.path:
        sys.path.insert(0, p)

import control_repository as cr  # noqa: E402
import failure_classifier as fc  # noqa: E402
import dq_rules as dq  # noqa: E402
import _modscan as modscan  # noqa: E402
import _nbvalidate as nbvalidate  # noqa: E402
from _nbsource import shared_nb  # noqa: E402


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

    def test_repository_requires_complete_audit_identity(self):
        import inspect
        method = inspect.getsource(cr.ControlRepository.log_table_run)
        for required in ("run_id", "connection_id", "source_table_id",
                         "operation", "attempt_number"):
            self.assertIn(f'"{required}"', method)

    def test_log_table_run_uses_sanitized_row_builder(self):
        with open(cr.__file__, encoding="utf-8") as stream:
            source = stream.read()
        method = source.split("def log_table_run(self, fields: dict):", 1)[1]
        method = method.split("def log_job_run", 1)[0]
        self.assertIn("build_table_run_row(fields)", method)
        self.assertIn("DELETE FROM", method)
        for key in ("run_id", "connection_id", "source_table_id", "operation",
                "attempt_number"):
            self.assertIn(key, method)
        self.assertLess(method.index("DELETE FROM"),
                method.index('.mode("append")'))

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
        for bad in ("SELECT 1", "TIME(7)", "TIME(8)"):
            with self.subTest(bad=bad):
                with self.assertRaises(ValueError):
                    dq.validate_rule({"rule_type": "DATA_TYPE", "column_name": "id",
                                      "rule_value": bad},
                                     available_columns=self.COLS)

    def test_valid_data_type_targets(self):
        for t in ("INT", "timestamp", "DECIMAL(10,2)", "string", "TIME", "TIME(0)", "TIME(3)", "TIME(6)"):
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


class TestSanitizationCoverage(unittest.TestCase):
    """Every sensitive form the persistence and notification paths may see."""

    CASES = {
        "password": "jdbc:sqlserver://h;password=hunter2;",
        "pwd": "conn;pwd=hunter2;",
        "user": "conn;user=scott;",
        "username": "conn;username=scott;",
        "client_secret": "client_secret=abc123secret",
        "secret": "secret=abc123secret",
        "token": "token=abc123secret",
        "access_token": "access_token=abc123secret",
        "refresh_token": "refresh_token=abc123secret",
        "bearer": "Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9",
        "basic": "Authorization: Basic dXNlcjpwYXNzd29yZDEyMw==",
        "jdbc_userinfo": "jdbc:oracle:thin:@//scott:hunter2@host:1521/svc",
        "url_userinfo": "https://admin:hunter2@host/path",
        "sas_sig": "https://acct.blob.core.windows.net/c?sig=abc123secret&se=x",
    }
    LEAKS = ("hunter2", "scott", "abc123secret",
             "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9",
             "dXNlcjpwYXNzd29yZDEyMw==")

    def test_every_sensitive_form_is_redacted(self):
        for label, raw in self.CASES.items():
            out = fc.sanitize_message(raw)
            for leak in self.LEAKS:
                self.assertNotIn(leak, out, f"{label}: {out}")

    def test_webhook_token_redacted(self):
        raw = ("https://outlook.office.com/webhook/abc-def@ghi/IncomingWebhook/"
               "tok123secret/xyz")
        for out in (fc.sanitize_message(raw), fc.redact_url(raw)):
            self.assertNotIn("tok123secret", out)

    def test_databricks_token_redacted(self):
        out = fc.sanitize_message("Authorization: Bearer dapi1234567890abcdef")
        self.assertNotIn("dapi1234567890abcdef", out)

    def test_sanitization_is_idempotent(self):
        for raw in self.CASES.values():
            once = fc.sanitize_message(raw)
            self.assertEqual(once, fc.sanitize_message(once))

    def test_safe_diagnostic_context_is_retained(self):
        out = fc.sanitize_message(
            "ORA-00942: table or view HR.EMPLOYEES does not exist")
        self.assertIn("ORA-00942", out)
        self.assertIn("HR.EMPLOYEES", out)

    def test_safe_identifiers_not_rewritten(self):
        out = fc.sanitize_message(
            "target dbo.user_tokens in database AppDb schema sales")
        self.assertIn("dbo.user_tokens", out)
        self.assertIn("AppDb", out)
        self.assertIn("sales", out)

    def test_classification_preserved_through_sanitization(self):
        classification = fc.classify_failure(
            Exception("Connection refused; password=hunter2"), fc.CONNECTION)
        self.assertEqual(classification.category, fc.TRANSIENT_CONNECTION)
        self.assertNotIn("hunter2", classification.sanitized_message)

    def test_adapter_redaction_uses_the_shared_sanitizer(self):
        from source_adapters.factory import get_source_adapter
        adapter = get_source_adapter("oracle")
        out = adapter.redact_jdbc_url(
            "jdbc:oracle:thin:@//scott:hunter2@host:1521/svc?token=abc123secret")
        self.assertNotIn("hunter2", out)
        self.assertNotIn("abc123secret", out)

    def test_none_and_empty_are_safe(self):
        self.assertEqual(fc.sanitize_message(None), "")
        self.assertEqual(fc.redact_url(None), "")


class TestNotebookExceptionOutput(unittest.TestCase):
    def test_no_caught_exception_is_printed_directly(self):
        class UnsafeExceptionUse(ast.NodeVisitor):
            def __init__(self, exception_name):
                self.exception_name = exception_name
                self.found = False

            def visit_Call(self, node):
                function_name = (getattr(node.func, "id", None)
                                 or getattr(node.func, "attr", None))
                if function_name in ("sanitize_message", "type"):
                    return
                self.generic_visit(node)

            def visit_Name(self, node):
                if node.id == self.exception_name:
                    self.found = True

        findings = []
        for path in nbvalidate.all_notebook_paths():
            tree = modscan.parse_notebook(path)
            for handler in (node for node in ast.walk(tree)
                            if isinstance(node, ast.ExceptHandler) and node.name):
                for call in (node for node in ast.walk(handler)
                             if isinstance(node, ast.Call)
                             and isinstance(node.func, ast.Name)
                             and node.func.id == "print"):
                    visitor = UnsafeExceptionUse(handler.name)
                    visitor.visit(call)
                    if visitor.found:
                        findings.append(
                            f"{os.path.relpath(path, ROOT)}:{call.lineno}:"
                            f" print({handler.name})")
        self.assertEqual(findings, [])

    def test_no_raw_exception_conversion_in_notebooks(self):
        forbidden = ("str(e)", "str(exc)", "repr(e)", "repr(exc)",
                     "print(e)", "print(exc)", "traceback.print_exc",
                     "traceback.format_exc")
        for path in nbvalidate.all_notebook_paths():
            source = nbvalidate.read_notebook(path)
            for token in forbidden:
                self.assertNotIn(token, source, f"{path}: {token}")

    def test_nb03_reuses_one_sanitized_mapping_error(self):
        source = shared_nb("NB03_MappingRulesGeneration.py")
        self.assertEqual(
            source.count("safe_error = failcls.sanitize_message(exc)"), 1)
        self.assertNotIn("str(exc)", source)
        self.assertIn("type(exc).__name__, safe_error[:500]", source)
        self.assertIn('"BLOCKED", "UNKNOWN"', source)
        self.assertIn("r['source_schema']", source)
        self.assertIn("r['source_table']", source)
        self.assertIn("r['column_name']", source)

    def test_nb08_sanitizes_before_persisting_and_printing(self):
        source = shared_nb("NB08_TargetProvisioning.py")
        handler = source.split("except Exception as e:", 1)[1]
        self.assertIn("safe_error = failcls.sanitize_message(e)", handler)
        self.assertIn('"current_status": "PROVISION_FAILED"', handler)
        self.assertIn('"error_message": safe_error[:1000]', handler)
        self.assertIn("type(e).__name__", handler)
        self.assertIn("safe_error[:300]", handler)
        self.assertNotIn("str(e)", handler)
        self.assertIn("if failed > 0:", source)
        self.assertIn("raise Exception", source)


class TestDefaultValueConversion(unittest.TestCase):
    """A configured DEFAULT_VALUE that cannot be represented must fail."""

    def test_null_default_is_always_acceptable(self):
        self.assertTrue(dq.default_value_converts(None, None))

    def test_convertible_value_accepted(self):
        self.assertTrue(dq.default_value_converts("42", 42))
        self.assertTrue(dq.default_value_converts("", ""))

    def test_value_that_casts_to_null_rejected(self):
        # e.g. DEFAULT_VALUE 'abc' on an INT column -> try_cast yields null.
        self.assertFalse(dq.default_value_converts("abc", None))

    def test_empty_string_on_numeric_rejected(self):
        self.assertFalse(dq.default_value_converts("", None))


class TestControlRepositorySanitization(unittest.TestCase):
    """Only message fields are sanitized; identifiers are left intact."""

    class _RecordingSpark:
        def __init__(self):
            self.executed = []

        def sql(self, statement):
            self.executed.append(statement)
            return _NoRows()

    def _repo(self):
        spark = self._RecordingSpark()
        return cr.ControlRepository(spark, "cat", "control"), spark

    def test_update_control_sanitizes_error_message(self):
        repo, spark = self._repo()
        repo.update_control("sid", {
            "error_message": "login failed for user=sa;password=Secret123!"})
        sql = spark.executed[-1]
        self.assertNotIn("Secret123", sql)
        self.assertIn("***", sql)

    def test_update_control_sanitizes_etl_error_message(self):
        repo, spark = self._repo()
        repo.update_control("sid", {"etl_error_message": "token=abc123xyz"})
        self.assertNotIn("abc123xyz", spark.executed[-1])

    def test_update_control_does_not_rewrite_identifiers(self):
        repo, spark = self._repo()
        repo.update_control("sid", {
            "target_schema": "user_password_data",
            "current_status": "LOADED",
            "target_table": "tokens"})
        sql = spark.executed[-1]
        self.assertIn("user_password_data", sql)
        self.assertIn("LOADED", sql)
        self.assertIn("tokens", sql)

    def test_update_connection_status_sanitizes(self):
        repo, spark = self._repo()
        repo.update_connection_status("c1", "FAILED",
                                      "jdbc:sqlserver://h;password=hunter2")
        sql = spark.executed[-1]
        self.assertNotIn("hunter2", sql)
        self.assertIn("FAILED", sql)

    def test_upsert_connection_sanitizes_error_message(self):
        repo, spark = self._repo()
        repo.upsert_connection({
            "connection_id": "c1", "connection_name": "Connection",
            "source_system": "oracle", "secret_scope": "scope",
            "error_message": "login failed; password=hunter2",
        })
        self.assertNotIn("hunter2", " ".join(spark.executed))

    def test_identity_update_sanitizes_error_message(self):
        repo, spark = self._repo()
        repo.update_control_by_identity(
            "oracle", None, None, "HR", "EMPLOYEES",
            {"error_message": "Authorization: Bearer abcdef123456"},
            connection_id="c1")
        self.assertNotIn("abcdef123456", spark.executed[-1])
        self.assertIn("connection_id = 'c1'", spark.executed[-1])

    def test_log_job_run_sanitizes_message(self):
        repo, spark = self._repo()
        repo.log_job_run("r1", "job", "FAILED", "Authorization: Bearer abcdef123456")
        self.assertNotIn("abcdef123456", spark.executed[-1])

    def test_log_job_run_keeps_plain_message(self):
        repo, spark = self._repo()
        repo.log_job_run("r1", "job", "SUCCEEDED", "control tables ready")
        self.assertIn("control tables ready", spark.executed[-1])


class _NoRows:
    def collect(self):
        return []


if __name__ == "__main__":
    unittest.main()
