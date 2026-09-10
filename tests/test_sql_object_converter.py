"""
Unit tests for SQL-object classification and deterministic conversion (Commit 5).
Nothing here executes or deploys SQL; only classification/text transforms.
"""

import os
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.join(os.path.dirname(HERE), "src")
for p in (SRC, os.path.dirname(HERE)):
    if p not in sys.path:
        sys.path.insert(0, p)

import sql_object_converter as sc  # noqa: E402


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


if __name__ == "__main__":
    unittest.main()
