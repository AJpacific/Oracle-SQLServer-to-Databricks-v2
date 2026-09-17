"""
Unit tests for SQL-object classification and deterministic conversion (Commit 5).
Nothing here executes or deploys SQL; only classification/text transforms.
"""

import os
import sys
import unittest
from unittest import mock

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.join(os.path.dirname(HERE), "src")
for p in (SRC, HERE, os.path.dirname(HERE)):
    if p not in sys.path:
        sys.path.insert(0, p)

import sql_object_assessment_common as common  # noqa: E402
import sql_object_converter as sc  # noqa: E402
from _nbsource import source_nb  # noqa: E402


class TestClassify(unittest.TestCase):
    def test_simple_view_auto(self):
        cat, reason = sc.classify_sql_object(
            "oracle", "VIEW", "CREATE VIEW v AS SELECT id, name FROM t")
        self.assertEqual(cat, sc.AUTO_CONVERT)
        self.assertTrue(reason)

    def test_view_with_source_syntax_review(self):
        cat, _ = sc.classify_sql_object(
            "oracle", "VIEW", "CREATE VIEW v AS SELECT NVL(a,0) FROM t CONNECT BY x")
        self.assertEqual(cat, sc.CONVERT_WITH_REVIEW)

    def test_cursor_forces_manual(self):
        cat, reason = sc.classify_sql_object(
            "oracle", "PROCEDURE", "BEGIN OPEN CURSOR c FOR SELECT 1 FROM dual; END;")
        self.assertEqual(cat, sc.MANUAL_REDESIGN)
        self.assertIn("CURSOR", reason.upper())

    def test_dynamic_sql_forces_manual(self):
        cat, _ = sc.classify_sql_object(
            "sqlserver", "PROCEDURE", "EXEC sp_executesql @sql")
        self.assertEqual(cat, sc.MANUAL_REDESIGN)

    def test_temp_table_forces_manual(self):
        cat, _ = sc.classify_sql_object(
            "sqlserver", "PROCEDURE", "SELECT * INTO #tmp FROM t")
        self.assertEqual(cat, sc.MANUAL_REDESIGN)

    def test_oracle_package_manual(self):
        cat, _ = sc.classify_sql_object("oracle", "PACKAGE_BODY", "PACKAGE BODY pkg AS ...")
        self.assertEqual(cat, sc.MANUAL_REDESIGN)

    def test_missing_text_unable(self):
        cat, _ = sc.classify_sql_object("oracle", "VIEW", "")
        self.assertEqual(cat, sc.UNABLE_TO_ASSESS)

    def test_keyword_inside_string_not_manual(self):
        # 'WHILE' appears only inside a string literal -> must NOT trigger manual.
        cat, _ = sc.classify_sql_object(
            "sqlserver", "VIEW", "CREATE VIEW v AS SELECT 'the WHILE loop text' AS c")
        self.assertNotEqual(cat, sc.MANUAL_REDESIGN)


class TestConvert(unittest.TestCase):
    def test_oracle_nvl_and_sysdate(self):
        out, lang, status = sc.convert_sql_object_deterministic(
            "oracle", "VIEW", "CREATE OR REPLACE FORCE VIEW v AS "
            "SELECT NVL(a,0) x, SYSDATE d FROM t")
        self.assertEqual(status, sc.GENERATED)
        self.assertEqual(lang, "DATABRICKS_SQL")
        self.assertIn("COALESCE(", out)
        self.assertIn("current_timestamp()", out)
        self.assertNotIn("FORCE VIEW", out)

    def test_sqlserver_isnull_getdate_len_brackets(self):
        out, lang, status = sc.convert_sql_object_deterministic(
            "sqlserver", "VIEW",
            "CREATE VIEW v AS SELECT ISNULL([col],0) a, LEN([n]) b, GETDATE() c FROM [dbo].[t]")
        self.assertEqual(status, sc.GENERATED)
        self.assertIn("COALESCE(", out)
        self.assertIn("length(", out)
        self.assertIn("current_timestamp()", out)
        self.assertIn("`col`", out)
        self.assertIn("`dbo`", out)

    def test_replacements_skip_string_literals(self):
        out, _, _ = sc.convert_sql_object_deterministic(
            "sqlserver", "VIEW", "CREATE VIEW v AS SELECT 'ISNULL stays' AS lit, ISNULL(x,0) FROM t")
        self.assertIn("'ISNULL stays'", out)      # literal untouched
        self.assertIn("COALESCE(x,0)", out)        # code converted

    def test_simple_top_becomes_limit(self):
        out, _, status = sc.convert_sql_object_deterministic(
            "sqlserver", "VIEW", "CREATE VIEW v AS SELECT TOP (5) a FROM t")
        self.assertEqual(status, sc.GENERATED)
        self.assertIn("LIMIT 5", out)
        self.assertNotIn("TOP", out.upper())

    def test_procedure_not_supported_guidance(self):
        out, lang, status = sc.convert_sql_object_deterministic(
            "oracle", "PROCEDURE", "BEGIN WHILE TRUE LOOP NULL; END LOOP; END;")
        self.assertEqual(status, sc.NOT_SUPPORTED)
        self.assertEqual(lang, "MANUAL_REDESIGN_GUIDANCE")
        self.assertIn("MANUAL REDESIGN", out)

    def test_empty_definition_not_started(self):
        out, lang, status = sc.convert_sql_object_deterministic("oracle", "VIEW", "")
        self.assertEqual(status, sc.NOT_STARTED)

    def test_unknown_source_never_uses_sqlserver_rewrites(self):
        source = "CREATE VIEW v AS SELECT ISNULL([value], 0) FROM [dbo].[t]"
        out, language, status = sc.convert_sql_object_deterministic(
            "futuredb", "VIEW", source)
        self.assertEqual(status, sc.NOT_SUPPORTED)
        self.assertEqual(language, "MANUAL_REDESIGN_GUIDANCE")
        self.assertNotIn("COALESCE", out)

    def test_missing_source_fails(self):
        with self.assertRaises(ValueError):
            sc.convert_sql_object_deterministic(None, "VIEW", "SELECT 1")


class TestSqlObjectCoverage(unittest.TestCase):
    def test_complete_discovery_query_failure_is_failed(self):
        self.assertEqual(common.discovery_business_status(
            schema_discovery_failed=False,
            object_discovery_attempts=1,
            object_discovery_successes=0,
            discovery_failures=1), "FAILED")

    def test_inaccessible_definition_is_partial_not_discovery_failure(self):
        self.assertEqual(common.discovery_business_status(
            schema_discovery_failed=False,
            object_discovery_attempts=1,
            object_discovery_successes=1,
            inaccessible_definitions=1), "PARTIAL")
        record = common.build_sql_object_record(
            assessment_id="a1", run_id="r1", connection_id="c1",
            source_system="oracle", source_database=None, source_schema="S",
            object_name="V", object_type="VIEW", source_definition=None)
        self.assertEqual(record["complexity_category"], sc.UNABLE_TO_ASSESS)

    def test_notebooks_return_coverage_counters_and_sanitized_errors(self):
        for source in ("oracle", "sqlserver"):
            code = source_nb(source, "NB13_SQLObjectAssessmentAndConversion.py")
            for key in ("execution_status", "business_status",
                        "discovered_objects", "persisted_objects",
                        "inaccessible_definitions", "unsupported_object_types",
                        "discovery_failures", "errors"):
                self.assertIn(f'"{key}"', code, source)
            capture = code.split("def _capture_discovery_error", 1)[1]
            capture = capture.split("# COMMAND ----------", 1)[0]
            self.assertLess(capture.index("failcls.sanitize_message(error)"),
                            capture.index("discovery_errors.append(detail)"),
                            source)
            self.assertNotIn("str(error)", capture, source)


class TestAiSafety(unittest.TestCase):
    def _record(self, use_ai):
        return common.build_sql_object_record(
            assessment_id="a1", run_id="r1", connection_id="c1",
            source_system="oracle", source_database=None, source_schema="S",
            object_name="P", object_type="PROCEDURE",
            source_definition="BEGIN NULL; END;", mode="CONVERT",
            use_ai=use_ai)

    def test_use_ai_false_cannot_invoke_an_ai_endpoint(self):
        with mock.patch.object(
                common.converter, "convert_sql_object_ai", create=True,
                side_effect=AssertionError("AI endpoint invoked")) as ai_endpoint:
            record = self._record(use_ai=False)
        ai_endpoint.assert_not_called()
        self.assertEqual(record["conversion_status"], sc.NOT_SUPPORTED)

    def test_unconfigured_ai_preserves_deterministic_output(self):
        baseline = self._record(use_ai=False)
        requested = self._record(use_ai=True)
        self.assertEqual(requested["conversion_status"], sc.NOT_CONFIGURED)
        self.assertEqual(requested["converted_definition"],
                         baseline["converted_definition"])
        self.assertEqual(requested["complexity_category"],
                         baseline["complexity_category"])

    def test_generated_output_remains_pending_review(self):
        record = common.build_sql_object_record(
            assessment_id="a1", run_id="r1", connection_id="c1",
            source_system="sqlserver", source_database="db", source_schema="S",
            object_name="V", object_type="VIEW",
            source_definition="CREATE VIEW v AS SELECT a FROM t",
            mode="CONVERT", use_ai=True)
        self.assertEqual(record["conversion_status"], sc.GENERATED)
        self.assertEqual(record["review_status"], common.PENDING_REVIEW)


if __name__ == "__main__":
    unittest.main()
