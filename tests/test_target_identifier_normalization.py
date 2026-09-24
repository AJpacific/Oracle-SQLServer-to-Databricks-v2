"""
Comprehensive tests for target identifier normalization across the accelerator.

Covers:
A. Identifier normalization rules (all 12 rules)
B. Table routing and schema normalization
C. Control table metadata changes and NB03 StructType/tuple consistency
D. Target column collision detection and table decision blocking
E. Target provisioning pre-DDL guards and target name usage
F. Full load source extraction vs target projection
G. Delta sync PK and watermark translation
H. Regression and integration consistency
"""

import builtins
import os
import re
import sys
import unittest
from unittest.mock import MagicMock

# Mock Databricks notebook environment before importing notebook modules
if not hasattr(builtins, "dbutils"):
    mock_dbutils = MagicMock()
    mock_dbutils.widgets.get.return_value = "dummy"
    builtins.dbutils = mock_dbutils
if not hasattr(builtins, "spark"):
    builtins.spark = MagicMock()

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.join(os.path.dirname(HERE), "src")
ROOT = os.path.dirname(HERE)
for p in (SRC, HERE, ROOT):
    if p not in sys.path:
        sys.path.insert(0, p)

from _nbsource import shared_nb, deployment_nb
from identifiers import (
    normalize_target_identifier,
    validate_identifier,
    validate_sqlserver_identifier,
    quote_oracle,
    quote_oracle_column,
    quote_sqlserver,
    quote_sqlserver_column,
    IdentifierError,
)
from notebooks.shared._common import (
    resolve_target_column_name,
    validate_target_identity,
    get_complete_mapping_snapshot,
    project_and_validate_dataframe,
    safe_source_column,
)
import sql_builder
import sqlserver_sql_builder
import failure_classifier as failcls
from _fakes import FakeSpark, FakeRow, FakeDataFrame


class TestIdentifierNormalization(unittest.TestCase):
    """A. Test all 12 rules of normalize_target_identifier."""

    def test_project_code(self):
        self.assertEqual(normalize_target_identifier("Project Code"), "project_code")

    def test_start_time(self):
        self.assertEqual(normalize_target_identifier("Start Time"), "start_time")

    def test_day_of_ctm(self):
        self.assertEqual(
            normalize_target_identifier("Day of CTM/Non-CTM"),
            "day_of_ctm_non_ctm",
        )

    def test_agenttype_hierarchy(self):
        self.assertEqual(
            normalize_target_identifier("AgentType Hierarchy - AgentType"),
            "agenttype_hierarchy_agenttype",
        )

    def test_aa_qa_data_dump(self):
        self.assertEqual(
            normalize_target_identifier("AA QA Data Dump", identifier_type="table"),
            "aa_qa_data_dump",
        )

    def test_sales_reporting(self):
        self.assertEqual(
            normalize_target_identifier("Sales.Reporting"),
            "sales_reporting",
        )

    def test_amount_percent(self):
        self.assertEqual(normalize_target_identifier("Amount(%)"), "amount")

    def test_digit_prefix_column(self):
        self.assertEqual(
            normalize_target_identifier("2026 Value", identifier_type="column"),
            "column_2026_value",
        )

    def test_digit_prefix_table(self):
        self.assertEqual(
            normalize_target_identifier("2026 Sales", identifier_type="table"),
            "table_2026_sales",
        )

    def test_digit_prefix_schema(self):
        self.assertEqual(
            normalize_target_identifier("2026 Schema", identifier_type="schema"),
            "schema_2026_schema",
        )

    def test_digit_prefix_stage(self):
        self.assertEqual(
            normalize_target_identifier("2026 Stage", identifier_type="stage"),
            "stage_2026_stage",
        )

    def test_repeated_special_characters_collapse(self):
        self.assertEqual(
            normalize_target_identifier("Foo$$$Bar---Baz///Qux"),
            "foo_bar_baz_qux",
        )

    def test_leading_and_trailing_underscores_stripped(self):
        self.assertEqual(normalize_target_identifier("___foo_bar___"), "foo_bar")
        self.assertEqual(normalize_target_identifier("  --foo_bar--  "), "foo_bar")

    def test_long_identifiers_deterministic(self):
        long_name = "A" * 200
        res1 = normalize_target_identifier(long_name)
        res2 = normalize_target_identifier(long_name)
        self.assertEqual(res1, res2)
        self.assertLessEqual(len(res1), 128)
        self.assertEqual(validate_identifier(res1), res1)

    def test_blank_and_none_fail(self):
        with self.assertRaises(IdentifierError):
            normalize_target_identifier(None)
        with self.assertRaises(IdentifierError):
            normalize_target_identifier("")
        with self.assertRaises(IdentifierError):
            normalize_target_identifier("   ")
        with self.assertRaises(IdentifierError):
            normalize_target_identifier("---///%%%")

    def test_deterministic_repeatability(self):
        inputs = [
            "Customer ID",
            "Order Date/Time",
            "Billing.Address#1",
            "2026_Quarterly_Results",
        ]
        for inp in inputs:
            first = normalize_target_identifier(inp)
            for _ in range(20):
                self.assertEqual(normalize_target_identifier(inp), first)

    def test_existing_valid_names_unchanged(self):
        self.assertEqual(normalize_target_identifier("employee_id"), "employee_id")
        self.assertEqual(normalize_target_identifier("first_name"), "first_name")
        self.assertEqual(
            normalize_target_identifier("orders", identifier_type="table"),
            "orders",
        )


class TestTableRouting(unittest.TestCase):
    """B. Table routing and schema normalization."""

    def test_prefix_with_database_convention(self):
        norm_db = normalize_target_identifier("BI_Centene", identifier_type="schema")
        norm_sch = normalize_target_identifier("dbo", identifier_type="schema")
        target_schema = f"{norm_db}_{norm_sch}"
        self.assertEqual(target_schema, "bi_centene_dbo")
        self.assertNotIn("__", target_schema)

    def test_source_schema_convention(self):
        norm_sch = normalize_target_identifier("dbo", identifier_type="schema")
        self.assertEqual(norm_sch, "dbo")

    def test_explicit_target_schema_validated(self):
        self.assertEqual(validate_identifier("custom_analytics"), "custom_analytics")
        with self.assertRaises(IdentifierError):
            validate_identifier("custom analytics")

    def test_nb01b_wiring_uses_normalize_target_identifier(self):
        code = shared_nb("NB01B_RegisterSelectedTables.py")
        self.assertIn('normalize_target_identifier(obj, identifier_type="table")', code)
        self.assertIn("normalize_target_identifier(database, identifier_type=\"schema\")", code)
        self.assertIn("normalize_target_identifier(schema, identifier_type=\"schema\")", code)


class TestMetadataAndNB03(unittest.TestCase):
    """C. Metadata schema change and NB03 StructType/tuple consistency."""

    def setUp(self):
        self.nb00 = shared_nb("NB00_ControlTableInit.py")
        self.nb03 = shared_nb("NB03_MappingRulesGeneration.py")

    def test_nb00_ddl_contains_target_column_name(self):
        self.assertIn("target_column_name STRING,", self.nb00)
        # target_column_name logically between column_name and ordinal_position within resolved_column_mappings
        tbl_idx = self.nb00.index("resolved_column_mappings")
        pos_col = self.nb00.index("column_name STRING,", tbl_idx)
        pos_target = self.nb00.index("target_column_name STRING,", tbl_idx)
        pos_ord = self.nb00.index("ordinal_position INT,", tbl_idx)
        self.assertLess(pos_col, pos_target)
        self.assertLess(pos_target, pos_ord)

    def test_nb00_ensure_columns_contains_target_column_name(self):
        self.assertIn('("target_column_name", "STRING")', self.nb00)

    def test_nb00_business_status_repair_required_on_missing_target_col(self):
        self.assertIn("missing_target_column_name_count", self.nb00)
        self.assertIn("MISSING_TARGET_COLUMN_NAME_RESOLVED_MAPPING", self.nb00)
        self.assertIn("REPAIR_REQUIRED", self.nb00)

    def test_nb03_struct_type_matches_tuple_field_count(self):
        # Extract StructFields from resolved_mapping_schema
        schema_idx = self.nb03.index("resolved_mapping_schema = StructType([")
        schema_block = self.nb03[schema_idx:self.nb03.index("])", schema_idx) + 2]
        fields = re.findall(r'StructField\("([^"]+)"', schema_block)
        self.assertIn("column_name", fields)
        self.assertIn("target_column_name", fields)
        self.assertEqual(fields[fields.index("column_name") + 1], "target_column_name")
        self.assertEqual(fields[fields.index("target_column_name") + 1], "ordinal_position")
        self.assertEqual(len(fields), 26)

    def test_nb03_generates_target_column_name_and_retains_source_column(self):
        self.assertIn('target_col = normalize_target_identifier(r["column_name"], identifier_type="column")', self.nb03)
        self.assertIn('"INVALID_TARGET_COLUMN_NAME"', self.nb03)


class TestCollisionHandling(unittest.TestCase):
    """D. Target column collision detection and table blocking."""

    def test_collision_detection_in_memory(self):
        # Simulate rows in NB03: "Agent Name", "Agent-Name", "Agent_Name"
        test_rows = [
            ("run1", "tbl1", "conn1", "sqlserver", "srv", "db", "dbo", "agent_info",
             "Agent Name", "agent_name", 1, "varchar", "STRING", "AUTO", "EXACT", "",
             True, False, False, False, False, None, True, True, False, None),
            ("run1", "tbl1", "conn1", "sqlserver", "srv", "db", "dbo", "agent_info",
             "Agent-Name", "agent_name", 2, "varchar", "STRING", "AUTO", "EXACT", "",
             True, False, False, False, False, None, True, True, False, None),
            ("run1", "tbl1", "conn1", "sqlserver", "srv", "db", "dbo", "agent_info",
             "Agent_Name", "agent_name", 3, "varchar", "STRING", "AUTO", "EXACT", "",
             True, False, False, False, False, None, True, True, False, None),
        ]

        from collections import defaultdict
        col_groups = defaultdict(list)
        for idx, m in enumerate(test_rows):
            t_col = m[9]
            if t_col:
                key = (m[0], m[2], m[1], t_col)
                col_groups[key].append(idx)

        mapped = list(test_rows)
        for key, indices in col_groups.items():
            if len(indices) > 1:
                colliding_names = [mapped[i][8] for i in indices]
                colliding_names_str = ", ".join(repr(c) for c in sorted(colliding_names))
                for idx in indices:
                    row = list(mapped[idx])
                    row[13] = "BLOCKED"
                    row[15] = f"Target column name collision on '{row[9]}': colliding source columns [{colliding_names_str}]"
                    row[22] = False
                    row[23] = False
                    row[24] = True
                    row[25] = "TARGET_COLUMN_NAME_COLLISION"
                    mapped[idx] = tuple(row)

        for m in mapped:
            self.assertEqual(m[13], "BLOCKED")
            self.assertEqual(m[22], False)
            self.assertEqual(m[24], True)
            self.assertEqual(m[25], "TARGET_COLUMN_NAME_COLLISION")
            self.assertIn("Agent Name", m[15])
            self.assertIn("Agent-Name", m[15])
            self.assertIn("Agent_Name", m[15])

    def test_nb07_blocks_table_on_collision_or_invalid_target_col(self):
        code = shared_nb("NB07_TableDecisionGeneration.py")
        self.assertIn("TARGET_COLUMN_NAME_COLLISION", code)
        self.assertIn("INVALID_TARGET_COLUMN_NAME", code)
        self.assertIn("BLANK_TARGET_COLUMN_NAME", code)
        self.assertIn('agg[key]["blocked"] += 1', code)


class TestNB04MappingValidation(unittest.TestCase):
    """E. NB04 separate validation of target_column_name."""

    def setUp(self):
        self.nb04 = shared_nb("NB04_MappingValidation.py")

    def test_nb04_has_blank_invalid_and_duplicate_rules(self):
        self.assertIn('"BLANK_TARGET_COLUMN_NAME"', self.nb04)
        self.assertIn('"INVALID_TARGET_COLUMN_NAME"', self.nb04)
        self.assertIn('"DUPLICATE_TARGET_COLUMN_NAME"', self.nb04)

    def test_nb04_preserves_spaces_in_source_column_name(self):
        # Verify that NB04 does not call validate_identifier on source column_name
        self.assertIn('col = r["source_schema"], r["source_table"], r["column_name"]', self.nb04)
        self.assertIn("validate_identifier(norm_t_col)", self.nb04)
        self.assertNotIn("validate_identifier(col)", self.nb04)


class TestTargetProvisioning(unittest.TestCase):
    """F. NB08 uses target_column_name without falling back to column_name."""

    def setUp(self):
        self.nb08 = shared_nb("NB08_TargetProvisioning.py")

    def test_nb08_selects_target_column_name(self):
        self.assertIn("target_column_name", self.nb08)
        self.assertIn('col_specs = [(c["target_column_name"], c["databricks_delta_type"], bool(c["is_nullable"]))', self.nb08)

    def test_nb08_no_fallback_to_column_name(self):
        # Ensure col_specs does not do c.get("target_column_name") or c["column_name"]
        self.assertNotIn('c["target_column_name"] or c["column_name"]', self.nb08)
        self.assertNotIn("coalesce(target_column_name, column_name)", self.nb08.lower())

    def test_nb08_pre_ddl_guards(self):
        self.assertIn("get_complete_mapping_snapshot(conn_id, src_id)", self.nb08)
        common = shared_nb("_common.py")
        self.assertIn("seen_target_names = set()", common)
        self.assertIn("Duplicate target_column_name", common)
        self.assertIn("blank target_column_name", common)
        self.assertIn("validate_identifier(t_col_str)", common)


class TestFullLoadTargetProjection(unittest.TestCase):
    """G. NB09 source extraction uses source name and projects to target name."""

    def setUp(self):
        self.nb09 = shared_nb("NB09_FullLoad.py")

    def test_nb09_extract_uses_source_column_names(self):
        self.assertIn('extract_columns = [m["column_name"] for m in mrows]', self.nb09)

    def test_nb09_projects_and_aliases_to_target_column_name(self):
        self.assertIn("project_and_validate_dataframe(src_df, mrows)", self.nb09)
        common = shared_nb("_common.py")
        self.assertIn("safe_source_column", common)

    def test_nb09_missing_table_creation_uses_target_name(self):
        self.assertIn('col_specs = [(m["target_column_name"], m["databricks_delta_type"], bool(m["is_nullable"]))', self.nb09)


class TestDeltaSyncTranslation(unittest.TestCase):
    """H. NB10 and NB11 translation of PKs and watermarks."""

    def test_resolve_target_column_name_exact(self):
        mappings = [
            {"column_name": "Project Code", "target_column_name": "project_code"},
            {"column_name": "Employee ID", "target_column_name": "employee_id"},
        ]
        self.assertEqual(resolve_target_column_name("Project Code", mappings), "project_code")
        self.assertEqual(resolve_target_column_name("Employee ID", mappings), "employee_id")

    def test_resolve_target_column_name_case_insensitive(self):
        mappings = [
            {"column_name": "Project Code", "target_column_name": "project_code"},
        ]
        self.assertEqual(resolve_target_column_name("project code", mappings), "project_code")

    def test_resolve_target_column_name_missing_fails(self):
        mappings = [
            {"column_name": "Project Code", "target_column_name": "project_code"},
        ]
        with self.assertRaises(ValueError) as ctx:
            resolve_target_column_name("Unknown Col", mappings)
        self.assertIn("No included approved target mapping found", str(ctx.exception))

    def test_resolve_target_column_name_ambiguous_fails(self):
        mappings = [
            {"column_name": "ColA", "target_column_name": "col_a"},
            {"column_name": "cola", "target_column_name": "cola_2"},
        ]
        with self.assertRaises(ValueError) as ctx:
            resolve_target_column_name("COLA", mappings)
        self.assertIn("Ambiguous mapping", str(ctx.exception))

    def test_nb10_resolves_watermark_to_target_column_name(self):
        code = shared_nb("NB10_PostFullLoadState.py")
        self.assertIn("target_wm_col = resolve_target_column_name(wm_col, mappings)", code)
        self.assertIn('F.max(F.col(f"`{target_wm_col}`"))', code)

    def test_nb11a_normalizes_stage_table(self):
        code = shared_nb("NB11a_DeltaSyncPrep.py")
        self.assertIn('stage_table = normalize_target_identifier(f"{t_table}_stage", identifier_type="stage")', code)

    def test_nb11b_projects_source_and_translates_pk_watermark(self):
        code = shared_nb("NB11b_DeltaSyncApply.py")
        self.assertIn("get_complete_mapping_snapshot(conn_id, src_id)", code)
        self.assertIn("project_and_validate_dataframe(src_df, approved_mappings)", code)
        self.assertIn("target_wm_col = resolve_target_column_name(wm_col, approved_mappings)", code)
        self.assertIn("target_pk = [resolve_target_column_name(c, approved_mappings) for c in pk]", code)
        self.assertIn("build_merge_sql(t_catalog, t_schema, t_table,\n                                          stage_table, target_pk,", code)


class TestCompleteMappingSnapshotSelection(unittest.TestCase):
    """A. Test one shared complete-mapping-snapshot resolver."""

    def test_one_whole_mapping_run_selected(self):
        sp = FakeSpark([
            [FakeRow({"run_id": "run_2"})],
            [
                FakeRow({"column_name": "col_a", "target_column_name": "col_a", "databricks_delta_type": "string", "mapping_status": "AUTO", "include_column": True, "ordinal_position": 1}),
                FakeRow({"column_name": "col_b", "target_column_name": "col_b", "databricks_delta_type": "int", "mapping_status": "AUTO", "include_column": True, "ordinal_position": 2}),
            ]
        ])
        sel_run, rows = get_complete_mapping_snapshot("c1", "t1", spark_session=sp)
        self.assertEqual(sel_run, "run_2")
        self.assertEqual(len(rows), 2)
        self.assertEqual([r["column_name"] for r in rows], ["col_a", "col_b"])

    def test_removed_column_from_latest_run_does_not_return_from_older_run(self):
        sp = FakeSpark([
            [FakeRow({"run_id": "run_latest"})],
            [
                FakeRow({"column_name": "col_a", "target_column_name": "col_a", "databricks_delta_type": "string", "mapping_status": "AUTO", "include_column": True, "ordinal_position": 1}),
            ]
        ])
        sel_run, rows = get_complete_mapping_snapshot("c1", "t1", spark_session=sp)
        cols = [r["column_name"] for r in rows]
        self.assertIn("col_a", cols)
        self.assertNotIn("col_c", cols)

    def test_deterministic_tie_breaking_query(self):
        sp = FakeSpark([
            [FakeRow({"run_id": "run_1"})],
            [FakeRow({"column_name": "c", "target_column_name": "c", "databricks_delta_type": "string", "mapping_status": "AUTO", "include_column": True, "ordinal_position": 1})]
        ])
        get_complete_mapping_snapshot("c1", "t1", spark_session=sp)
        run_query = sp.executed[0]
        self.assertIn("ORDER BY max(captured_ts) DESC NULLS LAST, run_id DESC", run_query)
        self.assertIn("GROUP BY run_id", run_query)

    def test_empty_selected_run_fails_clearly(self):
        sp = FakeSpark([[]])
        with self.assertRaises(ValueError) as ctx:
            get_complete_mapping_snapshot("c1", "t1", spark_session=sp)
        self.assertIn("No mapping run found in resolved_column_mappings", str(ctx.exception))

    def test_blank_or_duplicate_or_non_auto_target_column_fails(self):
        # Blank target column
        sp = FakeSpark([
            [FakeRow({"run_id": "r1"})],
            [FakeRow({"column_name": "c1", "target_column_name": "  ", "databricks_delta_type": "string", "mapping_status": "AUTO", "include_column": True, "ordinal_position": 1})]
        ])
        with self.assertRaises(ValueError) as ctx:
            get_complete_mapping_snapshot("c1", "t1", spark_session=sp)
        self.assertIn("blank target_column_name", str(ctx.exception))

        # Duplicate target column
        sp = FakeSpark([
            [FakeRow({"run_id": "r1"})],
            [
                FakeRow({"column_name": "c1", "target_column_name": "target_col", "databricks_delta_type": "string", "mapping_status": "AUTO", "include_column": True, "ordinal_position": 1}),
                FakeRow({"column_name": "c2", "target_column_name": "TARGET_COL", "databricks_delta_type": "string", "mapping_status": "AUTO", "include_column": True, "ordinal_position": 2}),
            ]
        ])
        with self.assertRaises(ValueError) as ctx:
            get_complete_mapping_snapshot("c1", "t1", spark_session=sp)
        self.assertIn("Duplicate target_column_name", str(ctx.exception))

        # Non-AUTO
        sp = FakeSpark([
            [FakeRow({"run_id": "r1"})],
            [FakeRow({"column_name": "c1", "target_column_name": "c1", "databricks_delta_type": "string", "mapping_status": "MANUAL", "include_column": True, "ordinal_position": 1})]
        ])
        with self.assertRaises(ValueError) as ctx:
            get_complete_mapping_snapshot("c1", "t1", spark_session=sp)
        self.assertIn("non-AUTO mapping_status", str(ctx.exception))

        # Empty databricks_delta_type
        sp = FakeSpark([
            [FakeRow({"run_id": "r1"})],
            [FakeRow({"column_name": "c1", "target_column_name": "c1", "databricks_delta_type": None, "mapping_status": "AUTO", "include_column": True, "ordinal_position": 1})]
        ])
        with self.assertRaises(ValueError) as ctx:
            get_complete_mapping_snapshot("c1", "t1", spark_session=sp)
        self.assertIn("empty databricks_delta_type", str(ctx.exception))


class TestStrictSourceDataFrameProjection(unittest.TestCase):
    """B. Test strict source DataFrame column contract and outer-whitespace resolution."""

    def test_missing_approved_source_column_fails(self):
        class MockDF:
            columns = ["col_a"]
            def select(self, *cols): return self

        mappings = [
            {"column_name": "col_a", "target_column_name": "col_a"},
            {"column_name": "col_b", "target_column_name": "col_b"},
        ]
        with self.assertRaises(ValueError) as ctx:
            project_and_validate_dataframe(MockDF(), mappings)
        self.assertIn("Extracted source DataFrame is missing approved columns: 'col_b'", str(ctx.exception))

    def test_exact_source_column_match_wins_over_trimmed(self):
        # Case 1: Exact match wins even when another column has the same trimmed form
        captured_projections = []
        class MockDF:
            columns = ["Site", "Site "]
            def select(self, *cols):
                captured_projections.extend(cols)
                class MockResult:
                    columns = ["site"]
                return MockResult()

        mappings = [
            {"column_name": "Site ", "target_column_name": "site"},
        ]
        res = project_and_validate_dataframe(MockDF(), mappings)
        self.assertEqual(res.columns, ["site"])
        self.assertEqual(len(captured_projections), 1)
        self.assertEqual(captured_projections[0].src, "`Site `")
        self.assertEqual(captured_projections[0].tgt, "site")

    def test_unique_trailing_whitespace_fallback(self):
        # Case 2: Unique trailing-whitespace fallback (e.g. "Site  " in mapping -> "Site" in DataFrame)
        captured_projections = []
        class MockDF:
            columns = ["Site", "Other"]
            def select(self, *cols):
                captured_projections.extend(cols)
                class MockResult:
                    columns = ["site"]
                return MockResult()

        mappings = [
            {"column_name": "Site  ", "target_column_name": "site"},
        ]
        res = project_and_validate_dataframe(MockDF(), mappings)
        self.assertEqual(res.columns, ["site"])
        self.assertEqual(captured_projections[0].src, "`Site`")
        self.assertEqual(captured_projections[0].tgt, "site")

    def test_unique_leading_whitespace_fallback(self):
        # Unique leading whitespace fallback
        captured_projections = []
        class MockDF:
            columns = ["Site", "Other"]
            def select(self, *cols):
                captured_projections.extend(cols)
                class MockResult:
                    columns = ["site"]
                return MockResult()

        mappings = [
            {"column_name": "   Site", "target_column_name": "site"},
        ]
        res = project_and_validate_dataframe(MockDF(), mappings)
        self.assertEqual(res.columns, ["site"])
        self.assertEqual(captured_projections[0].src, "`Site`")
        self.assertEqual(captured_projections[0].tgt, "site")

    def test_unique_leading_and_trailing_whitespace_fallback(self):
        # Unique leading and trailing whitespace fallback
        captured_projections = []
        class MockDF:
            columns = ["Site", "Other"]
            def select(self, *cols):
                captured_projections.extend(cols)
                class MockResult:
                    columns = ["site"]
                return MockResult()

        mappings = [
            {"column_name": "  Site  ", "target_column_name": "site"},
        ]
        res = project_and_validate_dataframe(MockDF(), mappings)
        self.assertEqual(res.columns, ["site"])
        self.assertEqual(captured_projections[0].src, "`Site`")
        self.assertEqual(captured_projections[0].tgt, "site")

    def test_ambiguous_trimmed_fallback_fails(self):
        # Case 3: Ambiguous fallback when multiple actual columns match trimmed name
        class MockDF:
            columns = ["Site", "Site "]
            def select(self, *cols): return self

        mappings = [
            {"column_name": "Site  ", "target_column_name": "site"},
        ]
        with self.assertRaises(ValueError) as ctx:
            project_and_validate_dataframe(MockDF(), mappings)
        err = str(ctx.exception)
        self.assertIn("Ambiguous source column mapping", err)
        self.assertIn("'Site  '", err)
        self.assertIn("'Site'", err)
        self.assertIn("'Site '", err)

    def test_internal_whitespace_not_normalized(self):
        # Case 4: Internal whitespace must not be removed
        class MockDF:
            columns = ["SiteCode"]
            def select(self, *cols): return self

        mappings = [
            {"column_name": "Site Code", "target_column_name": "site_code"},
        ]
        with self.assertRaises(ValueError) as ctx:
            project_and_validate_dataframe(MockDF(), mappings)
        self.assertIn("Extracted source DataFrame is missing approved columns: 'Site Code'", str(ctx.exception))

    def test_case_difference_fails_without_silent_match(self):
        # Case 5: Case difference must fail closed (no case-insensitive matching)
        class MockDF:
            columns = ["site"]
            def select(self, *cols): return self

        mappings = [
            {"column_name": "Site", "target_column_name": "site"},
        ]
        with self.assertRaises(ValueError) as ctx:
            project_and_validate_dataframe(MockDF(), mappings)
        self.assertIn("Extracted source DataFrame is missing approved columns: 'Site'", str(ctx.exception))

    def test_blank_approved_source_column_fails(self):
        # Case 7: Blank or invalid input
        class MockDF:
            columns = ["Site"]
            def select(self, *cols): return self

        for bad in ["", "   ", None]:
            mappings = [
                {"column_name": bad, "target_column_name": "site"},
            ]
            with self.assertRaises(ValueError) as ctx:
                project_and_validate_dataframe(MockDF(), mappings)
            self.assertIn("blank or invalid", str(ctx.exception))

    def test_duplicate_source_mappings_fail(self):
        class MockDF:
            columns = ["col_a"]
            def select(self, *cols): return self

        mappings = [
            {"column_name": "col_a", "target_column_name": "col_a_1"},
            {"column_name": "col_a", "target_column_name": "col_a_2"},
        ]
        with self.assertRaises(ValueError) as ctx:
            project_and_validate_dataframe(MockDF(), mappings)
        self.assertIn("Duplicate mappings found for source columns", str(ctx.exception))

    def test_duplicate_target_mappings_fail(self):
        class MockDF:
            columns = ["col_a", "col_b"]
            def select(self, *cols): return self

        mappings = [
            {"column_name": "col_a", "target_column_name": "same_target"},
            {"column_name": "col_b", "target_column_name": "same_target"},
        ]
        with self.assertRaises(ValueError) as ctx:
            project_and_validate_dataframe(MockDF(), mappings)
        self.assertIn("Duplicate target column name", str(ctx.exception))

    def test_target_columns_appear_in_ordinal_order(self):
        class MockProjectedDF:
            columns = ["target_1", "target_2"]
        class MockDF:
            columns = ["source_2", "source_1"]
            def select(self, *cols):
                return MockProjectedDF()

        mappings = [
            {"column_name": "source_1", "target_column_name": "target_1", "ordinal_position": 1},
            {"column_name": "source_2", "target_column_name": "target_2", "ordinal_position": 2},
        ]
        res = project_and_validate_dataframe(MockDF(), mappings)
        self.assertEqual(res.columns, ["target_1", "target_2"])

    def test_period_in_source_column_treated_as_literal_name(self):
        col = safe_source_column("Billing.Address")
        self.assertIn("`Billing.Address`", repr(col))

    def test_backtick_handling_is_safe(self):
        col = safe_source_column("col`name")
        self.assertIn("`col``name`", repr(col))


class TestTargetMetadataValidation(unittest.TestCase):
    """C. Test authoritative target metadata validation without fallbacks."""

    def test_blank_target_catalog_fails(self):
        with self.assertRaises(ValueError) as ctx:
            validate_target_identity("", "schema_a", "table_a")
        self.assertIn("target_catalog is missing; repair registration metadata", str(ctx.exception))

    def test_blank_target_schema_fails(self):
        with self.assertRaises(ValueError) as ctx:
            validate_target_identity("cat", "   ", "table_a")
        self.assertIn("target_schema is missing; repair registration metadata", str(ctx.exception))

    def test_blank_target_table_fails(self):
        with self.assertRaises(ValueError) as ctx:
            validate_target_identity("cat", "schema_a", None)
        self.assertIn("target_table is missing; repair registration metadata", str(ctx.exception))

    def test_stored_invalid_target_identifiers_fail(self):
        with self.assertRaises(ValueError):
            validate_target_identity("cat", "schema with spaces", "table_a")
        with self.assertRaises(ValueError):
            validate_target_identity("cat", "schema_a", "table-hyphen")

    def test_valid_target_identity(self):
        cat, sch, tbl = validate_target_identity(" my_cat ", " my_sch ", " my_tbl ")
        self.assertEqual((cat, sch, tbl), ("my_cat", "my_sch", "my_tbl"))

    def test_operational_notebooks_do_not_use_source_schema_lower_fallback(self):
        for nb_name in (
            "NB08_TargetProvisioning.py",
            "NB09_FullLoad.py",
            "NB10_PostFullLoadState.py",
            "NB11a_DeltaSyncPrep.py",
            "NB11b_DeltaSyncApply.py",
            "NB15_BronzeToSilverETL.py",
        ):
            code = shared_nb(nb_name)
            self.assertNotIn("source_schema.lower()", code, nb_name)
            self.assertNotIn("source_table.lower()", code, nb_name)
            self.assertNotIn("s_schema.lower()", code, nb_name)
            self.assertNotIn("s_table.lower()", code, nb_name)


class TestJdbcColumnLabelBehavior(unittest.TestCase):
    """D. Test JDBC column label behavior across Oracle and SQL Server extract query builders."""

    def test_dialect_specific_column_quoting(self):
        test_cols = [
            "Project Code",
            "Billing.Address",
            "Day of CTM/Non-CTM",
            "Agent Name",
        ]
        # Oracle quoting
        self.assertEqual(quote_oracle_column("Project Code"), '"Project Code"')
        self.assertEqual(quote_oracle_column("Billing.Address"), '"Billing.Address"')
        self.assertEqual(quote_oracle_column("Day of CTM/Non-CTM"), '"Day of CTM/Non-CTM"')
        self.assertEqual(quote_oracle_column("Agent Name"), '"Agent Name"')

        ora_sql = sql_builder.build_full_extract_query("HR", "EMPLOYEES", columns=test_cols)
        self.assertIn('"Project Code"', ora_sql)
        self.assertIn('"Billing.Address"', ora_sql)
        self.assertIn('"Day of CTM/Non-CTM"', ora_sql)
        self.assertIn('"Agent Name"', ora_sql)

        # SQL Server quoting
        self.assertEqual(quote_sqlserver_column("Project Code"), '[Project Code]')
        self.assertEqual(quote_sqlserver_column("Billing.Address"), '[Billing.Address]')
        self.assertEqual(quote_sqlserver_column("Day of CTM/Non-CTM"), '[Day of CTM/Non-CTM]')
        self.assertEqual(quote_sqlserver_column("Agent Name"), '[Agent Name]')

        mssql_sql = sqlserver_sql_builder.build_full_extract_query("DB", "dbo", "EMPLOYEES", columns=test_cols)
        self.assertIn('[Project Code]', mssql_sql)
        self.assertIn('[Billing.Address]', mssql_sql)
        self.assertIn('[Day of CTM/Non-CTM]', mssql_sql)
        self.assertIn('[Agent Name]', mssql_sql)

    def test_sqlserver_datetime2_alias_preserves_exact_source_name(self):
        sql = sqlserver_sql_builder.build_full_extract_query(
            "DB", "dbo", "EMP",
            columns=["Day of CTM/Non-CTM"],
            watermark_column="Day of CTM/Non-CTM",
            watermark_type="DATETIME2",
        )
        self.assertIn("CAST([Day of CTM/Non-CTM] AS datetime2(6)) AS [Day of CTM/Non-CTM]", sql)

    def test_jdbc_column_labels_project_to_target_names(self):
        source_cols = ["Project Code", "Billing.Address", "Day of CTM/Non-CTM", "Agent Name"]
        class MockDF:
            columns = list(source_cols)
            def select(self, *cols):
                mock = MockDF()
                mock.columns = [
                    "project_code", "billing_address", "day_of_ctm_non_ctm", "agent_name"
                ]
                return mock

        mappings = [
            {"column_name": c, "target_column_name": normalize_target_identifier(c), "ordinal_position": i}
            for i, c in enumerate(source_cols, 1)
        ]
        projected = project_and_validate_dataframe(MockDF(), mappings)
        self.assertEqual(
            projected.columns,
            ["project_code", "billing_address", "day_of_ctm_non_ctm", "agent_name"]
        )


class TestRepairNotebook(unittest.TestCase):
    """E. Test administrative target identifier repair notebook behavior and invariants."""

    def setUp(self):
        self.code = deployment_nb("NB_RepairInvalidTargetIdentifiers.py")

    def test_dry_run_defaults_true(self):
        self.assertIn('dbutils.widgets.dropdown("dry_run", "true", ["true", "false"])', self.code)
        self.assertIn('dry_run = dbutils.widgets.get("dry_run").strip().lower() in ("true", "1", "yes")', self.code)

    def test_dry_run_performs_zero_mutations(self):
        self.assertIn("if not dry_run:", self.code)
        self.assertIn("DRY RUN completed. Zero mutations performed.", self.code)

    def test_candidate_scope_and_exclusions(self):
        self.assertIn("coalesce(initial_load_completed, false) = false", self.code)
        self.assertIn("coalesce(current_status, '') <> 'PROVISIONED'", self.code)
        self.assertIn("coalesce(current_status, '') NOT IN ('RETIRED', 'DECOMMISSIONED')", self.code)
        self.assertIn('allowed_failure_statuses = ("PROVISION_FAILED", "PROVISION_CONFIG_ERROR")', self.code)

    def test_target_schema_and_table_derivations(self):
        self.assertIn('strat == "PREFIX_WITH_DATABASE"', self.code)
        self.assertIn('prop_sch = f"{norm_db}_{norm_sch}"', self.code)
        self.assertIn('strat == "SOURCE_SCHEMA"', self.code)
        self.assertIn('strat == "EXPLICIT"', self.code)
        self.assertIn('validate_identifier(explicit_sch)', self.code)
        self.assertIn('normalize_target_identifier(src_tbl, identifier_type="table")', self.code)

    def test_collision_detection_and_blocking(self):
        self.assertIn("TARGET_FQN_COLLISION", self.code)
        self.assertIn("existing_fqn_owners.get(proposed_fqn", self.code)
        self.assertIn("spark.catalog.tableExists", self.code)

    def test_repair_status_transition_to_ready_for_provisioning(self):
        self.assertIn("current_status = 'READY_FOR_PROVISIONING'", self.code)
        self.assertIn("error_message = NULL", self.code)

    def test_source_identity_and_target_catalog_unchanged(self):
        self.assertNotIn("SET source_schema =", self.code)
        self.assertNotIn("SET source_table =", self.code)
        self.assertNotIn("SET source_database =", self.code)
        self.assertNotIn("SET target_catalog =", self.code)

    def test_optimistic_concurrency_detection(self):
        self.assertIn("verification_sql =", self.code)
        self.assertIn("changed concurrently; update aborted", self.code)

    def test_sanitized_display_columns_only(self):
        self.assertIn("display_cols = [", self.code)
        for forbidden in ("secret", "password", "jdbc_url", "credential"):
            self.assertNotIn(f'"{forbidden}"', self.code.lower())

    def _run_candidate_evaluation(self, raw_candidates, table_exists_side_effect_or_return):
        mock_spark = MagicMock()
        if isinstance(table_exists_side_effect_or_return, Exception) or (
            isinstance(table_exists_side_effect_or_return, type) and issubclass(table_exists_side_effect_or_return, Exception)
        ):
            mock_spark.catalog.tableExists.side_effect = table_exists_side_effect_or_return
        elif callable(table_exists_side_effect_or_return):
            mock_spark.catalog.tableExists.side_effect = table_exists_side_effect_or_return
        else:
            mock_spark.catalog.tableExists.return_value = table_exists_side_effect_or_return

        mock_repo = MagicMock()
        mock_repo.get_connection.return_value = {"connection_id": "conn_1"}

        code_segment = self.code.split("candidates_evaluated = []", 1)[1].split("ready_candidates = [", 1)[0]
        exec_scope = {
            "raw_candidates": raw_candidates,
            "catalog": "da_accelerators",
            "existing_fqn_owners": {},
            "repo": mock_repo,
            "spark": mock_spark,
            "failcls": failcls,
            "normalize_target_identifier": normalize_target_identifier,
            "validate_identifier": validate_identifier,
            "candidates_evaluated": [],
            "seen_proposed_fqns_in_batch": {},
            "connection_cache": {},
        }
        exec("candidates_evaluated = []\n" + code_segment, exec_scope)
        return exec_scope["candidates_evaluated"], mock_spark

    def test_unsafe_pattern_removed(self):
        self.assertNotIn("except Exception as uc_err:", self.code)
        self.assertNotIn("pass\n\n    item[\"repair_status\"] = \"READY\"", self.code)

    def test_source_contains_failcls_sanitize(self):
        self.assertIn("failcls.sanitize_message(exc)", self.code)
        self.assertIn("TARGET_EXISTENCE_CHECK_FAILED: ", self.code)
        self.assertIn("TARGET_FQN_COLLISION: ", self.code)

    def test_table_exists_false_permits_ready(self):
        candidate = {
            "connection_id": "conn_1",
            "source_table_id": "src_1",
            "source_system": "sqlserver",
            "source_database": "sales_db",
            "source_schema": "dbo",
            "source_table": "Customer List",
            "target_catalog": "da_accelerators",
            "target_schema": "sales_db_dbo",
            "target_table": "customer list",
            "current_status": "PROVISION_FAILED",
            "initial_load_completed": False,
            "target_strategy": "PREFIX_WITH_DATABASE",
        }
        evaluated, mock_spark = self._run_candidate_evaluation([candidate], False)
        self.assertEqual(len(evaluated), 1)
        self.assertEqual(evaluated[0]["repair_status"], "READY")
        self.assertNotIn("TARGET_EXISTENCE_CHECK_FAILED", evaluated[0]["repair_message"])
        self.assertIn("Ready for repair", evaluated[0]["repair_message"])
        mock_spark.catalog.tableExists.assert_called_once_with("da_accelerators.sales_db_dbo.customer_list")

    def test_table_exists_true_blocks_with_collision(self):
        candidate = {
            "connection_id": "conn_1",
            "source_table_id": "src_1",
            "source_system": "sqlserver",
            "source_database": "sales_db",
            "source_schema": "dbo",
            "source_table": "Customer List",
            "target_catalog": "da_accelerators",
            "target_schema": "sales_db_dbo",
            "target_table": "customer list",
            "current_status": "PROVISION_FAILED",
            "initial_load_completed": False,
            "target_strategy": "PREFIX_WITH_DATABASE",
        }
        evaluated, mock_spark = self._run_candidate_evaluation([candidate], True)
        self.assertEqual(len(evaluated), 1)
        self.assertEqual(evaluated[0]["repair_status"], "BLOCKED")
        self.assertTrue(evaluated[0]["repair_message"].startswith("TARGET_FQN_COLLISION: "))
        self.assertIn("already exists in Unity Catalog", evaluated[0]["repair_message"])

    def test_table_exists_exception_blocks_with_sanitized_bounded_diagnostic(self):
        long_err = "Catalog metadata lookup failed: " + ("A" * 300)
        candidate = {
            "connection_id": "conn_1",
            "source_table_id": "src_1",
            "source_system": "sqlserver",
            "source_database": "sales_db",
            "source_schema": "dbo",
            "source_table": "Customer List",
            "target_catalog": "da_accelerators",
            "target_schema": "sales_db_dbo",
            "target_table": "customer list",
            "current_status": "PROVISION_FAILED",
            "initial_load_completed": False,
            "target_strategy": "PREFIX_WITH_DATABASE",
        }
        evaluated, mock_spark = self._run_candidate_evaluation([candidate], RuntimeError(long_err))
        self.assertEqual(len(evaluated), 1)
        self.assertEqual(evaluated[0]["repair_status"], "BLOCKED")
        prefix = "TARGET_EXISTENCE_CHECK_FAILED: "
        self.assertTrue(evaluated[0]["repair_message"].startswith(prefix))
        error_detail = evaluated[0]["repair_message"][len(prefix):]
        self.assertLessEqual(len(error_detail), 200)

    def test_table_exists_exception_continues_safely_for_next_candidates(self):
        c1 = {
            "connection_id": "conn_1",
            "source_table_id": "src_1",
            "source_system": "sqlserver",
            "source_database": "sales_db",
            "source_schema": "dbo",
            "source_table": "Table 1",
            "target_catalog": "da_accelerators",
            "target_schema": "sales_db_dbo",
            "target_table": "table 1",
            "current_status": "PROVISION_FAILED",
            "initial_load_completed": False,
            "target_strategy": "PREFIX_WITH_DATABASE",
        }
        c2 = {
            "connection_id": "conn_1",
            "source_table_id": "src_2",
            "source_system": "sqlserver",
            "source_database": "sales_db",
            "source_schema": "dbo",
            "source_table": "Table 2",
            "target_catalog": "da_accelerators",
            "target_schema": "sales_db_dbo",
            "target_table": "table 2",
            "current_status": "PROVISION_FAILED",
            "initial_load_completed": False,
            "target_strategy": "PREFIX_WITH_DATABASE",
        }
        def fake_table_exists(fqn):
            if "table_1" in fqn:
                raise RuntimeError("Catalog timeout for table_1")
            return False

        evaluated, mock_spark = self._run_candidate_evaluation([c1, c2], fake_table_exists)
        self.assertEqual(len(evaluated), 2)
        self.assertEqual(evaluated[0]["repair_status"], "BLOCKED")
        self.assertTrue(evaluated[0]["repair_message"].startswith("TARGET_EXISTENCE_CHECK_FAILED: "))
        self.assertEqual(evaluated[1]["repair_status"], "READY")
        self.assertIn("Ready for repair", evaluated[1]["repair_message"])

    def test_provisioned_and_completed_tables_never_call_table_exists(self):
        provisioned_cand = {
            "connection_id": "conn_1",
            "source_table_id": "src_p",
            "source_system": "sqlserver",
            "source_database": "sales_db",
            "source_schema": "dbo",
            "source_table": "Prov Table",
            "target_catalog": "da_accelerators",
            "target_schema": "sales_db_dbo",
            "target_table": "prov table",
            "current_status": "PROVISIONED",
            "initial_load_completed": False,
            "target_strategy": "PREFIX_WITH_DATABASE",
        }
        completed_cand = {
            "connection_id": "conn_1",
            "source_table_id": "src_c",
            "source_system": "sqlserver",
            "source_database": "sales_db",
            "source_schema": "dbo",
            "source_table": "Done Table",
            "target_catalog": "da_accelerators",
            "target_schema": "sales_db_dbo",
            "target_table": "done table",
            "current_status": "PROVISION_FAILED",
            "initial_load_completed": True,
            "target_strategy": "PREFIX_WITH_DATABASE",
        }
        evaluated, mock_spark = self._run_candidate_evaluation([provisioned_cand, completed_cand], False)
        self.assertEqual(len(evaluated), 2)
        self.assertEqual(evaluated[0]["repair_status"], "BLOCKED")
        self.assertEqual(evaluated[1]["repair_status"], "BLOCKED")
        mock_spark.catalog.tableExists.assert_not_called()



class TestDQTargetContract(unittest.TestCase):
    """F. Test Bronze-to-Silver DQ target column resolution and ambiguity detection."""

    def test_ambiguous_dq_column_name_fails(self):
        code = shared_nb("NB15_BronzeToSilverETL.py")
        self.assertIn("Ambiguous DQ column_name", code)
        self.assertIn("matches Bronze column", code)
        self.assertIn("source column mapping to", code)

    def test_dq_rule_validation_against_target_columns(self):
        code = shared_nb("NB15_BronzeToSilverETL.py")
        self.assertIn("dqr.validate_rule(rd, available_columns=bronze_cols", code)


class TestOriginal49NamingPatterns(unittest.TestCase):
    """G. Test all 49 realistic source identifier naming patterns normalize to valid Databricks identifiers."""

    PATTERNS = [
        "Project Code",
        "Billing.Address",
        "Day of CTM/Non-CTM",
        "Agent Name",
        "Agent-Name",
        "Agent_Name",
        "AgentType Hierarchy - AgentType",
        "Sales.Reporting",
        "AA QA Data Dump",
        "Amount(%)",
        "Total Net ($)",
        "2026 Value",
        "2026 Sales",
        "2026 Schema",
        "100_percent_done",
        "First Name",
        "Last Name",
        "Address Line 1",
        "Address Line 2",
        "Postal/Zip Code",
        "Phone #",
        "Tax ID / SSN",
        "Order - Status",
        "Unit Price ($/ea)",
        "Discount %",
        "Gross Profit [USD]",
        "Margin (bps)",
        "Q1-2026-Revenue",
        "Year.Quarter.Month",
        "User & Role Mapping",
        "Dept/Division",
        "Account:SubAccount",
        "Item # Code",
        "Batch # ID",
        "Ship-To/Bill-To",
        "Rate (per hr)",
        "Weight [kg/lbs]",
        "Is_Active?",
        "Flag (Y/N)",
        "Source.Table.Column",
        "Data+Time+Stamp",
        "Employee:Manager",
        "Ref # / Order #",
        "Code @ Dept",
        "Region / Area / Zone",
        "Field with   Multiple    Spaces",
        "__leading_and_trailing__",
        "Mix-Of_all.Characters/Here(100%)",
        "Final Check #49",
    ]

    def test_all_49_patterns_normalize_to_valid_target_column(self):
        for pattern in self.PATTERNS:
            normalized = normalize_target_identifier(pattern, identifier_type="column")
            self.assertEqual(validate_identifier(normalized), normalized)
            self.assertRegex(normalized, r"^[a-z_#$][a-z0-9_#$]*$", f"Failed on pattern: {pattern}")
            self.assertNotIn(" ", normalized)
            self.assertNotIn(".", normalized)
            self.assertNotIn("/", normalized)
            self.assertNotIn("-", normalized)

    def test_all_49_patterns_normalize_to_valid_target_table(self):
        for pattern in self.PATTERNS:
            normalized = normalize_target_identifier(pattern, identifier_type="table")
            self.assertEqual(validate_identifier(normalized), normalized)
            self.assertRegex(normalized, r"^[a-z_#$][a-z0-9_#$]*$", f"Failed on pattern: {pattern}")


if __name__ == "__main__":
    unittest.main()
