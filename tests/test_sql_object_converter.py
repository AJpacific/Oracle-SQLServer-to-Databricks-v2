"""
Unit tests for source SQL-object inventory and discovery coverage.
Nothing here executes, converts, or deploys SQL; definitions are only inventoried.
"""

import os
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.join(os.path.dirname(HERE), "src")
for p in (SRC, HERE, os.path.dirname(HERE)):
    if p not in sys.path:
        sys.path.insert(0, p)

import sql_object_assessment_common as common  # noqa: E402
from _nbsource import source_nb  # noqa: E402

ORACLE_VIEW = (
    "CREATE OR REPLACE FORCE VIEW v AS\n"
    "    SELECT NVL(a, 0) AS x,   SYSDATE AS d\n"
    "      FROM t\n"
)
SQLSERVER_VIEW = (
    "CREATE VIEW [dbo].[v] AS\r\n"
    "    SELECT ISNULL([col], 0) AS a, GETDATE() AS c\r\n"
    "      FROM [dbo].[t];\r\n"
)


def _record(source_system, object_type, definition, **kwargs):
    return common.build_sql_object_record(
        assessment_id=kwargs.get("assessment_id", "a1"),
        run_id=kwargs.get("run_id", "r1"),
        connection_id=kwargs.get("connection_id", "c1"),
        source_system=source_system, source_database="db",
        source_schema="S", object_name=kwargs.get("object_name", "V"),
        object_type=object_type, source_definition=definition)


class TestSourceDefinitionPreserved(unittest.TestCase):
    def test_oracle_definition_is_preserved_exactly(self):
        record = _record("oracle", "VIEW", ORACLE_VIEW)
        self.assertEqual(record["source_definition"], ORACLE_VIEW)
        self.assertIsNone(record["error_message"])

    def test_sqlserver_definition_is_preserved_exactly(self):
        record = _record("sqlserver", "VIEW", SQLSERVER_VIEW)
        self.assertEqual(record["source_definition"], SQLSERVER_VIEW)
        self.assertIsNone(record["error_message"])

    def test_source_sql_text_is_never_rewritten(self):
        for system, definition in (("oracle", ORACLE_VIEW),
                                   ("sqlserver", SQLSERVER_VIEW)):
            out = _record(system, "VIEW", definition)["source_definition"]
            # Dialect-specific tokens and whitespace survive untouched.
            self.assertNotIn("COALESCE", out.upper().replace("NVL", ""))
            self.assertEqual(out.count("\n"), definition.count("\n"))
            self.assertEqual(out.strip() == out, definition.strip() == definition)

    def test_record_matches_the_field_contract(self):
        record = _record("oracle", "PROCEDURE", "BEGIN NULL; END;",
                         object_name="P")
        self.assertEqual(sorted(record), sorted(common.SQL_OBJECT_FIELDS))

    def test_no_conversion_fields_exist_on_the_contract(self):
        for removed in ("converted_definition", "conversion_status",
                        "conversion_language", "complexity_category",
                        "classification_reason", "review_status"):
            self.assertNotIn(removed, common.SQL_OBJECT_FIELDS)
            self.assertNotIn(removed, common.SQL_OBJECT_UPDATE_FIELDS)


class TestInaccessibleDefinition(unittest.TestCase):
    def test_blank_encrypted_or_missing_definition_is_reported(self):
        for definition in (None, "", "   \n\t "):
            record = _record("sqlserver", "PROCEDURE", definition,
                             object_name="P")
            self.assertEqual(record["error_message"],
                             common.INACCESSIBLE_REASON)
            self.assertEqual(record["source_definition"], definition)

    def test_error_message_never_exposes_credentials(self):
        record = _record("oracle", "VIEW", None)
        for secret in ("password", "secret", "token", "jdbc:", "@"):
            self.assertNotIn(secret, record["error_message"].lower())

    def test_unsupported_object_type_is_rejected(self):
        with self.assertRaises(ValueError):
            _record("sqlserver", "TRIGGER", "...")


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
        self.assertEqual(record["error_message"], common.INACCESSIBLE_REASON)

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


class TestNoConversionBehaviourRemains(unittest.TestCase):
    CONVERSION_TOKENS = (
        "sql_object_converter", "classify_sql_object",
        "convert_sql_object_deterministic", "converted_definition",
        "conversion_status", "conversion_language", "complexity_category",
        "classification_reason", "review_status", "summarize_complexity",
    )

    def test_nb13_notebooks_only_inventory_definitions(self):
        for source in ("oracle", "sqlserver"):
            code = source_nb(source, "NB13_SQLObjectAssessmentAndConversion.py")
            for token in self.CONVERSION_TOKENS:
                self.assertNotIn(token, code, f"{source}: {token}")
            self.assertIn("persist_sql_object_records(", code)
            self.assertIn("build_sql_object_record(", code)

    def test_nb13_no_longer_exposes_mode_or_use_ai_widgets(self):
        for source in ("oracle", "sqlserver"):
            code = source_nb(source, "NB13_SQLObjectAssessmentAndConversion.py")
            self.assertNotIn('dbutils.widgets.dropdown("mode"', code)
            self.assertNotIn('widgets.get("mode")', code)
            self.assertNotIn("use_ai", code)

    def test_nb13_never_executes_or_deploys_source_sql(self):
        for source in ("oracle", "sqlserver"):
            code = source_nb(source, "NB13_SQLObjectAssessmentAndConversion.py")
            for forbidden in ("spark.sql(source_definition", "CREATE OR REPLACE VIEW",
                              "executeUpdate", "DROP TABLE"):
                self.assertNotIn(forbidden, code, source)


if __name__ == "__main__":
    unittest.main()
