"""
Unit tests for source assessment (Commit 2): discovery query generation and
table compatibility classification. All pure string / policy checks (no Spark).
"""

import os
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.join(os.path.dirname(HERE), "src")
for p in (SRC, HERE, os.path.dirname(HERE)):
    if p not in sys.path:
        sys.path.insert(0, p)

import assessment_common as assess  # noqa: E402
import sql_builder as ora  # noqa: E402
import sqlserver_sql_builder as ss  # noqa: E402
from _nbsource import source_nb  # noqa: E402
from crosssourcetypemapper import classify_table_compatibility  # noqa: E402
from source_adapters.factory import get_source_adapter  # noqa: E402


class TestOracleDiscovery(unittest.TestCase):
    def test_list_schemas_excludes_system_owners(self):
        q = ora.list_schemas_query()
        self.assertIn("all_objects", q)
        self.assertIn("'SYS'", q)
        self.assertIn("NOT IN", q)

    def test_list_tables_uses_all_tables_and_estimated_rows(self):
        q = ora.list_tables_query()
        self.assertIn("all_tables", q)
        self.assertIn("num_rows AS ROW_COUNT", q)

    def test_list_tables_scoped_to_owner(self):
        q = ora.list_tables_query("HR")
        self.assertIn("owner = 'HR'", q)

    def test_table_statistics_labelled_estimated(self):
        query = ora.table_statistics_query()
        self.assertIn("'ESTIMATED' AS ROW_COUNT_METHOD", query)
        self.assertIn("'ESTIMATED_8K_BLOCKS' AS SIZE_MB_METHOD", query)
        self.assertIn("blocks,0) * 8192", query)
        self.assertNotIn("'CATALOG'", query)

    def test_list_routines_covers_package(self):
        q = ora.list_routines_query()
        for kind in ("PROCEDURE", "FUNCTION", "PACKAGE", "PACKAGE BODY"):
            self.assertIn(kind, q)

    def test_object_source_ordered_by_line(self):
        q = ora.object_source_query("HR", "PKG1", "PACKAGE BODY")
        self.assertIn("all_source", q)
        self.assertIn("ORDER BY line", q)


class TestSqlServerDiscovery(unittest.TestCase):
    def test_list_schemas_uses_sys_schemas(self):
        q = ss.list_schemas_query("SourceDb")
        self.assertIn("sys.schemas", q)
        self.assertIn("[SourceDb].sys.", q)

    def test_list_tables_uses_partitions_for_counts(self):
        q = ss.list_tables_query("SourceDb")
        self.assertIn("sys.partitions", q)
        self.assertIn("SUM(pr.rows) AS ROW_COUNT", q)

    def test_table_statistics_not_labelled_exact(self):
        # Catalog metadata is not a COUNT_BIG(*), so it must not claim EXACT.
        q = ss.table_statistics_query("SourceDb")
        self.assertIn("'CATALOG' AS ROW_COUNT_METHOD", q)
        self.assertNotIn("'EXACT'", q)
        self.assertIn("allocation_units", q)

    def test_row_count_not_multiplied_by_allocation_join(self):
        # Row count and size are aggregated in independent subqueries and joined by
        # object_id, so the allocation join cannot duplicate row counts.
        q = ss.table_statistics_query("SourceDb")
        self.assertIn("row_counts", q)
        self.assertIn("allocation_pages", q)
        self.assertIn("size_pages", q)
        self.assertIn("index_id IN (0,1)", q)
        row_subquery = q.split(") row_counts")[0]
        self.assertNotIn("allocation_units", row_subquery)

    def test_allocation_units_use_correct_containers(self):
        q = ss.table_statistics_query("SourceDb")
        # IN_ROW_DATA (1) and ROW_OVERFLOW_DATA (3) hang off hobt_id.
        self.assertIn("au.container_id = pr.hobt_id AND au.type IN (1,3)", q)
        # LOB_DATA (2) hangs off partition_id.
        self.assertIn("au.container_id = pr.partition_id AND au.type = 2", q)

    def test_allocation_branches_use_union_all_not_or_join(self):
        q = ss.table_statistics_query("SourceDb")
        self.assertIn("UNION ALL", q)
        allocation = q.split("UNION ALL")[0]
        self.assertNotIn(" OR ", allocation)

    def test_sizes_aggregated_once_before_join(self):
        q = ss.table_statistics_query("SourceDb")
        self.assertIn("SUM(allocation_pages.total_pages) AS total_pages", q)
        self.assertIn("GROUP BY allocation_pages.object_id", q)

    def test_zero_allocation_tables_are_not_null(self):
        q = ss.table_statistics_query("SourceDb")
        self.assertIn("COALESCE(size_pages.total_pages, 0)", q)
        self.assertIn("COALESCE(row_counts.ROW_COUNT, 0)", q)

    def test_no_table_wide_count_in_broad_assessment(self):
        for q in (ss.table_statistics_query("SourceDb"),
                  ss.list_tables_query("SourceDb")):
            self.assertNotIn("COUNT_BIG", q.upper())
            self.assertNotIn("COUNT(*)", q.upper())

    def test_system_schemas_excluded(self):
        q = ss.table_statistics_query("SourceDb")
        self.assertIn("'sys'", q)
        self.assertIn("NOT IN", q)

    def test_module_definition_uses_sql_modules(self):
        q = ss.module_definition_query("SourceDb", "dbo", "usp_Get")
        self.assertIn("sys.sql_modules", q)
        self.assertIn("'dbo'", q)
        self.assertIn("'usp_Get'", q)


class TestCompatibilityClassification(unittest.TestCase):
    def test_all_auto_is_compatible(self):
        self.assertEqual(classify_table_compatibility(["AUTO", "AUTO"]), "COMPATIBLE")

    def test_review_present_no_blocked_is_review(self):
        self.assertEqual(classify_table_compatibility(["AUTO", "REVIEW"]), "REVIEW")

    def test_blocked_present_is_manual(self):
        self.assertEqual(
            classify_table_compatibility(["AUTO", "REVIEW", "BLOCKED"]), "MANUAL")

    def test_empty_is_unable(self):
        self.assertEqual(classify_table_compatibility([]), "UNABLE_TO_ASSESS")


class TestAssessmentDiscoveryStatus(unittest.TestCase):
    def test_mandatory_discovery_failure_is_failed(self):
        for stage in assess.MANDATORY_DISCOVERY_STAGES:
            self.assertEqual(
                assess.assessment_business_status([{"stage": stage}]),
                "FAILED")

    def test_optional_discovery_failure_is_partial(self):
        self.assertEqual(
            assess.assessment_business_status([{"stage": "view_discovery"}]),
            "PARTIAL")

    def test_no_discovery_errors_is_complete(self):
        self.assertEqual(assess.assessment_business_status([]), "COMPLETE")

    def test_source_notebooks_collect_only_sanitized_errors(self):
        for source in ("oracle", "sqlserver"):
            code = source_nb(source, "NB01A_SourceAssessment.py")
            capture = code.split("def _capture_assessment_error", 1)[1]
            capture = capture.split("# COMMAND ----------", 1)[0]
            sanitize = capture.index("failcls.sanitize_message(error)")
            append = capture.index("assessment_errors.append(detail)")
            logged = capture.index("print(")
            self.assertLess(sanitize, append, source)
            self.assertLess(sanitize, logged, source)
            self.assertNotIn("str(error)", capture, source)

    def test_source_notebooks_fail_mandatory_coverage(self):
        for source in ("oracle", "sqlserver"):
            code = source_nb(source, "NB01A_SourceAssessment.py")
            self.assertIn(
                '_capture_assessment_error("schema_discovery", e)', code,
                source)
            self.assertIn(
                '_capture_assessment_error("table_discovery", e, schema)',
                code, source)
            discovery = code.index("discovered_tables = _q(")
            include_guard = code.index('if "TABLE" in include_types:', discovery)
            self.assertLess(discovery, include_guard, source)
            result = code.split("business_status =", 1)[1]
            self.assertIn('if business_status == "FAILED":', result, source)
            self.assertIn("raise RuntimeError(", result, source)

    def test_source_notebooks_return_bounded_error_details(self):
        for source in ("oracle", "sqlserver"):
            code = source_nb(source, "NB01A_SourceAssessment.py")
            for key in ("execution_status", "business_status",
                        "objects_assessed", "error_count", "errors",
                        "compatibility_summary"):
                self.assertIn(f'"{key}"', code, source)
            self.assertIn(
                "assessment_errors[:assess_common.ASSESSMENT_ERROR_LIMIT]",
                code, source)


class TestAdapterDiscoveryRouting(unittest.TestCase):
    def test_oracle_adapter_returns_oracle_discovery(self):
        a = get_source_adapter("oracle")
        self.assertIn("all_tables", a.list_tables_query())

    def test_sqlserver_adapter_returns_sqlserver_discovery(self):
        a = get_source_adapter("sqlserver", source_database="Db")
        self.assertIn("sys.tables", a.list_tables_query("Db"))


if __name__ == "__main__":
    unittest.main()
