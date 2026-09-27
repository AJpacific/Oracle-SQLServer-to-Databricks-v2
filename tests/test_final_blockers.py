"""Tests for final 8 defects.

Written prior to implementation to verify test failures, then run to confirm passes.
"""

from decimal import Decimal
import os
import re
import unittest
import yaml

from src import sql_artifact_control_common as art_ctrl
from src.sql_artifact_converter import SQLArtifactConverter

# Try importing parse_integral_metadata
try:
    from src.type_mappers.base import parse_integral_metadata
except ImportError:
    parse_integral_metadata = None

from src.type_mappers.oracle import OracleTypeMapper
from src.type_mappers.sqlserver import SqlServerTypeMapper

# These functions will be imported from src.sql_artifact_scope once created:
try:
    from src.sql_artifact_scope import (
        build_candidate_where_clauses,
        build_object_map,
    )
except ImportError:
    build_candidate_where_clauses = None
    build_object_map = None

try:
    from src.sql_artifact_control_common import (
        plan_identity_migration,
        build_target_ownership,
        check_target_ownership,
        decide_target_deployment_action,
        is_connection_usable,
        verify_schema,
        decide_summary,
    )
except ImportError:
    plan_identity_migration = None
    build_target_ownership = None
    check_target_ownership = None
    decide_target_deployment_action = None
    is_connection_usable = None
    verify_schema = None
    decide_summary = None

from tests.test_sql_object_artifact_materialization import (
    FakeRow,
    run_nb18_harness,
)


class TestDefect1CandidateWhereClauses(unittest.TestCase):
    """DEFECT 1: ASSESSMENT_FLAGS generates invalid SQL."""

    def test_alias_soa_no_soa_dot_func_syntax(self):
        self.assertIsNotNone(build_candidate_where_clauses, "build_candidate_where_clauses must be defined")
        clauses = build_candidate_where_clauses(
            alias="soa",
            run_id="run_1",
            connection_id="c1",
            only_assessment_id="a1",
            only_source_system="oracle",
            object_types=["VIEW"],
            allow_global_scope=False,
        )
        for clause in clauses:
            self.assertIsNone(
                re.search(r"\bsoa\.[a-z_]+\s*\(", clause),
                f"Clause has invalid function qualification: {clause}",
            )

    def test_alias_soa_qualifies_all_columns(self):
        self.assertIsNotNone(build_candidate_where_clauses, "build_candidate_where_clauses must be defined")
        clauses = build_candidate_where_clauses(
            alias="soa",
            run_id="run_1",
            connection_id="c1",
            only_assessment_id="a1",
            only_source_system="oracle",
            object_types=["VIEW"],
            allow_global_scope=False,
        )
        combined = " AND ".join(clauses)
        # Every column referenced must be qualified with soa.
        self.assertIn("soa.object_type", combined)
        self.assertIn("soa.run_id", combined)
        self.assertIn("soa.connection_id", combined)
        self.assertIn("soa.assessment_id", combined)
        self.assertIn("soa.source_system", combined)

    def test_alias_empty_has_no_soa_dot(self):
        self.assertIsNotNone(build_candidate_where_clauses, "build_candidate_where_clauses must be defined")
        clauses = build_candidate_where_clauses(
            alias="",
            run_id="run_1",
            connection_id="c1",
            only_assessment_id="a1",
            only_source_system="oracle",
            object_types=["VIEW"],
            allow_global_scope=False,
        )
        for clause in clauses:
            self.assertNotIn("soa.", clause)

    def test_run_id_always_present_when_not_global(self):
        self.assertIsNotNone(build_candidate_where_clauses, "build_candidate_where_clauses must be defined")
        clauses = build_candidate_where_clauses(
            alias="soa",
            run_id="run_123",
            connection_id="",
            only_assessment_id="",
            only_source_system="",
            object_types=[],
            allow_global_scope=False,
        )
        combined = " AND ".join(clauses)
        self.assertIn("soa.run_id = 'run_123'", combined)

    def test_blank_run_id_raises_value_error(self):
        self.assertIsNotNone(build_candidate_where_clauses, "build_candidate_where_clauses must be defined")
        with self.assertRaises(ValueError):
            build_candidate_where_clauses(
                alias="soa",
                run_id="",
                connection_id="",
                only_assessment_id="",
                only_source_system="",
                object_types=[],
                allow_global_scope=False,
            )

    def test_value_with_single_quote_escaped(self):
        self.assertIsNotNone(build_candidate_where_clauses, "build_candidate_where_clauses must be defined")
        clauses = build_candidate_where_clauses(
            alias="soa",
            run_id="run_1",
            connection_id="c'1",
            only_assessment_id="",
            only_source_system="",
            object_types=[],
            allow_global_scope=False,
        )
        combined = " AND ".join(clauses)
        self.assertIn("c''1", combined)


class TestDefect2ArtifactIdentity(unittest.TestCase):
    """DEFECT 2: Artifact identity v2 and migration planning."""

    def test_missing_source_system_raises(self):
        with self.assertRaises(ValueError):
            art_ctrl.compute_artifact_id("c1", "", "db", "sch", "VIEW", "obj")
        with self.assertRaises(ValueError):
            art_ctrl.compute_artifact_id("c1", None, "db", "sch", "VIEW", "obj")

    def test_casing_produces_different_ids(self):
        id_db_upper = art_ctrl.compute_artifact_id("c1", "sqlserver", "DB", "sch", "VIEW", "obj")
        id_db_lower = art_ctrl.compute_artifact_id("c1", "sqlserver", "db", "sch", "VIEW", "obj")
        self.assertNotEqual(id_db_upper, id_db_lower)

        id_hr_upper = art_ctrl.compute_artifact_id("c1", "oracle", "", "HR", "VIEW", "obj")
        id_hr_lower = art_ctrl.compute_artifact_id("c1", "oracle", "", "hr", "VIEW", "obj")
        self.assertNotEqual(id_hr_upper, id_hr_lower)

        id_foo_upper = art_ctrl.compute_artifact_id("c1", "oracle", "", "sch", "VIEW", "FOO")
        id_foo_mixed = art_ctrl.compute_artifact_id("c1", "oracle", "", "sch", "VIEW", "Foo")
        self.assertNotEqual(id_foo_upper, id_foo_mixed)

    def test_same_inputs_twice_same_id(self):
        id1 = art_ctrl.compute_artifact_id("c1", "oracle", "", "sch", "VIEW", "obj")
        id2 = art_ctrl.compute_artifact_id("c1", "oracle", "", "sch", "VIEW", "obj")
        self.assertEqual(id1, id2)

    def test_oracle_source_database_none_and_empty_same_id(self):
        id_none = art_ctrl.compute_artifact_id("c1", "oracle", None, "sch", "VIEW", "obj")
        id_empty = art_ctrl.compute_artifact_id("c1", "oracle", "", "sch", "VIEW", "obj")
        self.assertEqual(id_none, id_empty)

    def test_plan_identity_migration_v1_row(self):
        self.assertIsNotNone(plan_identity_migration, "plan_identity_migration must be defined")
        old_row = {
            "artifact_id": "old_v1_id",
            "connection_id": "c1",
            "source_system": "oracle",
            "source_database": "",
            "source_schema": "HR",
            "object_type": "VIEW",
            "object_name": "EMP_V",
            "identity_version": 1,
            "legacy_artifact_id": None,
        }
        updates, conflicts = plan_identity_migration([old_row])
        self.assertEqual(len(conflicts), 0)
        self.assertEqual(len(updates), 1)
        upd = updates[0]
        self.assertEqual(upd["legacy_artifact_id"], "old_v1_id")
        self.assertEqual(upd["identity_version"], 2)
        v2_expected = art_ctrl.compute_artifact_id("c1", "oracle", "", "HR", "VIEW", "EMP_V")
        self.assertEqual(upd["artifact_id"], v2_expected)

    def test_plan_identity_migration_already_v2_row(self):
        self.assertIsNotNone(plan_identity_migration, "plan_identity_migration must be defined")
        v2_id = art_ctrl.compute_artifact_id("c1", "oracle", "", "HR", "VIEW", "EMP_V")
        v2_row = {
            "artifact_id": v2_id,
            "connection_id": "c1",
            "source_system": "oracle",
            "source_database": "",
            "source_schema": "HR",
            "object_type": "VIEW",
            "object_name": "EMP_V",
            "identity_version": 2,
            "legacy_artifact_id": "old_id",
        }
        updates, conflicts = plan_identity_migration([v2_row])
        self.assertEqual(len(updates), 0)
        self.assertEqual(len(conflicts), 0)

    def test_plan_identity_migration_conflict_detected(self):
        self.assertIsNotNone(plan_identity_migration, "plan_identity_migration must be defined")
        v2_id = art_ctrl.compute_artifact_id("c1", "oracle", "", "HR", "VIEW", "EMP_V")
        existing_v2_row = {
            "artifact_id": v2_id,
            "connection_id": "c1",
            "source_system": "oracle",
            "source_database": "",
            "source_schema": "HR",
            "object_type": "VIEW",
            "object_name": "EMP_V",
            "identity_version": 2,
            "legacy_artifact_id": None,
        }
        v1_row = {
            "artifact_id": "old_hash_conflict",
            "connection_id": "c1",
            "source_system": "oracle",
            "source_database": "",
            "source_schema": "HR",
            "object_type": "VIEW",
            "object_name": "EMP_V",
            "identity_version": None,
            "legacy_artifact_id": None,
        }
        updates, conflicts = plan_identity_migration([existing_v2_row, v1_row])
        self.assertEqual(len(conflicts), 1)
        self.assertEqual(len(updates), 0)

    def test_plan_identity_migration_idempotent_on_output(self):
        self.assertIsNotNone(plan_identity_migration, "plan_identity_migration must be defined")
        old_row = {
            "artifact_id": "old_v1_id",
            "connection_id": "c1",
            "source_system": "oracle",
            "source_database": "",
            "source_schema": "HR",
            "object_type": "VIEW",
            "object_name": "EMP_V",
            "identity_version": 1,
            "legacy_artifact_id": None,
        }
        updates, _ = plan_identity_migration([old_row])
        # Apply updates to row
        updated_row = dict(old_row)
        updated_row.update(updates[0])
        # Run plan on updated row
        second_updates, second_conflicts = plan_identity_migration([updated_row])
        self.assertEqual(len(second_updates), 0)
        self.assertEqual(len(second_conflicts), 0)

    def test_docs_describe_v2_identity(self):
        doc_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "docs", "databricks_job_task_mapping.md",
        )
        with open(doc_path, "r", encoding="utf-8") as f:
            content = f.read()
        self.assertIn("v2", content)
        self.assertIn("source_system", content)


class TestDefect3AutoConversionTokenAwareAndObjectMap(unittest.TestCase):
    """DEFECT 3: Token-aware rewrite and object map wiring."""

    def test_literal_not_rewritten_from_rewritten(self):
        conv = SQLArtifactConverter()
        sql = "CREATE VIEW v_orders AS SELECT 'dbo.orders' AS label FROM dbo.orders"
        obj_map = {"dbo.orders": "`cat`.`sch`.`orders`"}
        res = conv.convert_artifact("sqlserver", "VIEW", "v_orders", sql, "cat", "sch", object_map=obj_map)
        self.assertEqual(res.classification, "AUTO")
        self.assertIn("'dbo.orders'", res.converted_definition)
        self.assertIn("FROM `cat`.`sch`.`orders`", res.converted_definition)

    def test_comments_not_rewritten(self):
        conv = SQLArtifactConverter()
        sql = "CREATE VIEW v_orders AS SELECT 1 FROM dbo.orders -- comment with dbo.orders\n /* block with dbo.orders */"
        obj_map = {"dbo.orders": "`cat`.`sch`.`orders`"}
        res = conv.convert_artifact("sqlserver", "VIEW", "v_orders", sql, "cat", "sch", object_map=obj_map)
        self.assertEqual(res.classification, "AUTO")
        self.assertIn("-- comment with dbo.orders", res.converted_definition)
        self.assertIn("/* block with dbo.orders */", res.converted_definition)

    def test_unmapped_from_becomes_manual_review(self):
        conv = SQLArtifactConverter()
        sql = "CREATE VIEW v_orders AS SELECT id FROM dbo.orders"
        res = conv.convert_artifact("sqlserver", "VIEW", "v_orders", sql, "cat", "sch", object_map={})
        self.assertEqual(res.classification, "MANUAL_REVIEW")
        self.assertIn("Unresolved source-qualified reference", res.manual_review_reason)

    def test_join_reference_rewritten_when_mapped(self):
        conv = SQLArtifactConverter()
        sql = "CREATE VIEW v_orders AS SELECT a.id FROM `cat`.`sch`.`tbl1` a JOIN dbo.customers c ON a.cid = c.id"
        obj_map = {"dbo.customers": "`cat`.`sch`.`customers`"}
        res = conv.convert_artifact("sqlserver", "VIEW", "v_orders", sql, "cat", "sch", object_map=obj_map)
        self.assertEqual(res.classification, "AUTO")
        self.assertIn("JOIN `cat`.`sch`.`customers`", res.converted_definition)

    def test_build_object_map_excludes_inactive_and_not_loaded(self):
        self.assertIsNotNone(build_object_map, "build_object_map must be defined")
        control_rows = [
            # Active and loaded -> included
            {"connection_id": "c1", "source_database": "db1", "source_schema": "dbo", "source_table": "t1",
             "target_catalog": "cat", "target_schema": "sch", "target_table": "t1",
             "is_active": True, "initial_load_completed": True},
            # Inactive -> excluded
            {"connection_id": "c1", "source_database": "db1", "source_schema": "dbo", "source_table": "t2",
             "target_catalog": "cat", "target_schema": "sch", "target_table": "t2",
             "is_active": False, "initial_load_completed": True},
            # Initial load not completed -> excluded
            {"connection_id": "c1", "source_database": "db1", "source_schema": "dbo", "source_table": "t3",
             "target_catalog": "cat", "target_schema": "sch", "target_table": "t3",
             "is_active": True, "initial_load_completed": False},
        ]
        obj_map = build_object_map(control_rows, connection_id="c1", source_database="db1")
        self.assertIn("dbo.t1", obj_map)
        self.assertIn("db1.dbo.t1", obj_map)
        self.assertNotIn("dbo.t2", obj_map)
        self.assertNotIn("dbo.t3", obj_map)

    def test_build_object_map_excludes_other_connections(self):
        self.assertIsNotNone(build_object_map, "build_object_map must be defined")
        control_rows = [
            {"connection_id": "c1", "source_database": "db1", "source_schema": "dbo", "source_table": "t1",
             "target_catalog": "cat", "target_schema": "sch", "target_table": "t1",
             "is_active": True, "initial_load_completed": True},
            {"connection_id": "c2", "source_database": "db1", "source_schema": "dbo", "source_table": "t1",
             "target_catalog": "cat", "target_schema": "sch", "target_table": "t1",
             "is_active": True, "initial_load_completed": True},
        ]
        obj_map = build_object_map(control_rows, connection_id="c1", source_database="db1")
        self.assertEqual(obj_map["dbo.t1"], "`cat`.`sch`.`t1`")


class TestDefect4TargetOwnership(unittest.TestCase):
    """DEFECT 4: Target ownership build and check."""

    def test_two_historical_owners_is_ambiguous(self):
        self.assertIsNotNone(build_target_ownership, "build_target_ownership must be defined")
        self.assertIsNotNone(check_target_ownership, "check_target_ownership must be defined")
        control_rows = [
            {"target_catalog": "cat", "target_schema": "sch", "target_object_name": "v1", "artifact_id": "art_1"},
            {"target_catalog": "cat", "target_schema": "sch", "target_object_name": "v1", "artifact_id": "art_2"},
        ]
        ownership = build_target_ownership(control_rows)
        status = check_target_ownership(ownership, "cat.sch.v1", "art_1")
        self.assertEqual(status, "AMBIGUOUS")

    def test_ownership_states(self):
        self.assertIsNotNone(build_target_ownership, "build_target_ownership must be defined")
        self.assertIsNotNone(check_target_ownership, "check_target_ownership must be defined")
        control_rows = [
            {"target_catalog": "cat", "target_schema": "sch", "target_object_name": "v1", "artifact_id": "art_1"},
        ]
        ownership = build_target_ownership(control_rows)
        # Same owner
        self.assertEqual(check_target_ownership(ownership, "cat.sch.v1", "art_1"), "OWNED")
        # Other owner
        self.assertEqual(check_target_ownership(ownership, "cat.sch.v1", "art_2"), "CONFLICT")
        # Free
        self.assertEqual(check_target_ownership(ownership, "cat.sch.v2", "art_1"), "FREE")

    def test_in_run_deployment_conflict(self):
        self.assertIsNotNone(build_target_ownership, "build_target_ownership must be defined")
        self.assertIsNotNone(check_target_ownership, "check_target_ownership must be defined")
        ownership = build_target_ownership([])
        # First check is FREE
        self.assertEqual(check_target_ownership(ownership, "cat.sch.v1", "art_A"), "FREE")
        # Deploy art_A and register in map
        ownership.setdefault("cat.sch.v1", set()).add("art_A")
        # Art_B targeting same FQN in-run is CONFLICT
        self.assertEqual(check_target_ownership(ownership, "cat.sch.v1", "art_B"), "CONFLICT")

    def test_fqn_comparison_case_insensitive(self):
        self.assertIsNotNone(build_target_ownership, "build_target_ownership must be defined")
        self.assertIsNotNone(check_target_ownership, "check_target_ownership must be defined")
        control_rows = [
            {"target_catalog": "CAT", "target_schema": "SCH", "target_object_name": "V1", "artifact_id": "art_1"},
        ]
        ownership = build_target_ownership(control_rows)
        self.assertEqual(check_target_ownership(ownership, "cat.sch.v1", "art_1"), "OWNED")
        self.assertEqual(check_target_ownership(ownership, "cat.sch.v1", "art_2"), "CONFLICT")


class TestDefect5ConnectionUsable(unittest.TestCase):
    """DEFECT 5: Connection usable verification."""

    def test_is_connection_usable_parametrized(self):
        self.assertIsNotNone(is_connection_usable, "is_connection_usable must be defined")
        # Valid cases
        valid_cases = [
            {"is_active": True, "connection_status": "VALID", "secret_scope": "scope1"},
            {"is_active": "true", "connection_status": "VALID", "secret_scope": "scope1"},
            {"is_active": " TRUE ", "connection_status": " valid ", "secret_scope": " scope1 "},
        ]
        for row in valid_cases:
            usable, reason = is_connection_usable(row)
            self.assertTrue(usable, f"Expected usable for {row}, got reason: {reason}")

        # Invalid cases
        invalid_cases = [
            {"is_active": False, "connection_status": "VALID", "secret_scope": "scope1"},
            {"is_active": "false", "connection_status": "VALID", "secret_scope": "scope1"},
            {"is_active": None, "connection_status": "VALID", "secret_scope": "scope1"},
            {"connection_status": "VALID", "secret_scope": "scope1"},  # missing is_active
            {"is_active": 0, "connection_status": "VALID", "secret_scope": "scope1"},
            {"is_active": 1, "connection_status": "VALID", "secret_scope": "scope1"},
            {"is_active": "yes", "connection_status": "VALID", "secret_scope": "scope1"},
            {"is_active": True, "connection_status": "INVALID", "secret_scope": "scope1"},
            {"is_active": True, "connection_status": "", "secret_scope": "scope1"},
            {"is_active": True, "connection_status": "VALID", "secret_scope": ""},
            {"is_active": True, "connection_status": "VALID", "secret_scope": None},
        ]
        for row in invalid_cases:
            usable, _ = is_connection_usable(row)
            self.assertFalse(usable, f"Expected NOT usable for {row}")

    def test_nb18_is_active_none_not_materialized(self):
        cand = FakeRow(
            run_id="run_100",
            connection_id="c_none",
            source_system="oracle",
            source_schema="s1",
            object_name="v1",
            object_type="VIEW",
            source_definition="SELECT 1",
        )
        res = run_nb18_harness(
            candidates=[cand],
            widget_values={"run_id": "run_100", "connection_id": "", "only_assessment_id": "", "allow_global_scope": "false"},
            connections={"c_none": {"connection_id": "c_none", "is_active": None, "connection_status": "VALID", "secret_scope": "scope1"}},
        )
        self.assertEqual(res["task_values"].get("materialized_count", 0), 0)
        self.assertEqual(res["fs"].write_count, 0)


class TestDefect6SchemaInitAndUpgradeGaps(unittest.TestCase):
    """DEFECT 6: Schema initialization, NOT NULL, verification, and DAG ordering."""

    def test_create_builder_contains_not_null(self):
        ddl_ctrl = art_ctrl.build_create_artifact_control_ddl("cat", "sch")
        self.assertTrue("`artifact_id` STRING NOT NULL" in ddl_ctrl or "artifact_id STRING NOT NULL" in ddl_ctrl)
        self.assertTrue("`connection_id` STRING NOT NULL" in ddl_ctrl or "connection_id STRING NOT NULL" in ddl_ctrl)

        ddl_log = art_ctrl.build_create_artifact_execution_log_ddl("cat", "sch")
        self.assertTrue("`log_id` STRING NOT NULL" in ddl_log or "log_id STRING NOT NULL" in ddl_log)

    def test_verify_schema(self):
        self.assertIsNotNone(verify_schema, "verify_schema must be defined")
        expected = [
            ("col1", "STRING"),
            ("col2", "INT"),
            ("col3", "TIMESTAMP"),
        ]
        actual_good = [
            {"name": "col1", "type": "string"},
            {"name": "col2", "type": "integer"},
            {"name": "col3", "type": "timestamp"},
        ]
        problems = verify_schema(expected, actual_good)
        self.assertEqual(problems, [])

        # Missing column
        actual_missing = [
            {"name": "col1", "type": "string"},
        ]
        problems_missing = verify_schema(expected, actual_missing)
        self.assertTrue(any("Missing column col2" in p for p in problems_missing))

        # Wrong type
        actual_wrong_type = [
            {"name": "col1", "type": "string"},
            {"name": "col2", "type": "string"},
            {"name": "col3", "type": "timestamp"},
        ]
        problems_wrong = verify_schema(expected, actual_wrong_type)
        self.assertTrue(any("Type mismatch on col2" in p for p in problems_wrong))

    def test_yaml_capture_tasks_depend_on_init(self):
        job_file = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "jobs", "ACCELERATOR_SQL_ARTIFACT_MIGRATION.yaml",
        )
        with open(job_file, "r", encoding="utf-8") as f:
            job = yaml.safe_load(f)

        tasks = {t["task_key"]: t for t in job["tasks"]}
        self.assertNotIn("T01a_Get_Oracle_Connection_Worklist", tasks)
        self.assertNotIn("T01b_Get_SQLServer_Database_Worklist", tasks)
        self.assertNotIn("T02a_ForEach_Oracle_SQL_Object_Capture", tasks)
        self.assertNotIn("T02b_ForEach_SQLServer_SQL_Object_Capture", tasks)

        expected_tasks = [
            "T00_Create_Run_Context",
            "T03_Init_SQL_Artifact_Control",
            "T23_Fetch_Selected_SQL_Artifacts",
            "T24_Lakebridge_Analyze_And_Transpile",
            "T06_SQL_Artifact_Summary",
        ]
        self.assertEqual(list(tasks.keys()), expected_tasks)
        t03_deps = [d["task_key"] for d in tasks["T03_Init_SQL_Artifact_Control"].get("depends_on", [])]
        t23_deps = [d["task_key"] for d in tasks["T23_Fetch_Selected_SQL_Artifacts"].get("depends_on", [])]
        t24_deps = [d["task_key"] for d in tasks["T24_Lakebridge_Analyze_And_Transpile"].get("depends_on", [])]
        t06_deps = [d["task_key"] for d in tasks["T06_SQL_Artifact_Summary"].get("depends_on", [])]

        self.assertEqual(t03_deps, ["T00_Create_Run_Context"])
        self.assertEqual(t23_deps, ["T03_Init_SQL_Artifact_Control"])
        self.assertEqual(t24_deps, ["T23_Fetch_Selected_SQL_Artifacts"])
        self.assertEqual(t06_deps, ["T24_Lakebridge_Analyze_And_Transpile"])

        self.assertEqual(tasks["T23_Fetch_Selected_SQL_Artifacts"].get("run_if"), "ALL_SUCCESS")
        self.assertEqual(tasks["T24_Lakebridge_Analyze_And_Transpile"].get("run_if"), "ALL_SUCCESS")
        self.assertEqual(tasks["T06_SQL_Artifact_Summary"].get("run_if"), "ALL_DONE")

    def test_yaml_graph_acyclic(self):
        job_file = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "jobs", "ACCELERATOR_SQL_ARTIFACT_MIGRATION.yaml",
        )
        with open(job_file, "r", encoding="utf-8") as f:
            job = yaml.safe_load(f)

        adj = {t["task_key"]: [d["task_key"] for d in t.get("depends_on", [])] for t in job["tasks"]}
        visited = {}  # 0=visiting, 1=visited
        def dfs(node):
            visited[node] = 0
            for neighbor in adj.get(node, []):
                if visited.get(neighbor) == 0:
                    return True
                if neighbor not in visited and dfs(neighbor):
                    return True
            visited[node] = 1
            return False

        has_cycle = any(dfs(node) for node in adj if node not in visited)
        self.assertFalse(has_cycle, "Workflow task graph must be acyclic")


class TestDefect7SummaryDecisions(unittest.TestCase):
    """DEFECT 7: Summary business status and job failure propagation."""

    def test_summary_failed_log_stages_returns_failed(self):
        self.assertIsNotNone(decide_summary, "decide_summary must be defined")
        ctrl_metrics = {"deployed_count": 5, "failed_count": 0, "total_candidates": 5}
        status, bus_status = decide_summary(ctrl_metrics, failed_log_stages=2)
        self.assertEqual(status, "FAILED")

    def test_summary_ctrl_failed_returns_failed(self):
        self.assertIsNotNone(decide_summary, "decide_summary must be defined")
        ctrl_metrics = {"deployed_count": 4, "failed_count": 1, "total_candidates": 5}
        status, bus_status = decide_summary(ctrl_metrics, failed_log_stages=0)
        self.assertEqual(status, "FAILED")

    def test_summary_manual_review_only_succeeded(self):
        self.assertIsNotNone(decide_summary, "decide_summary must be defined")
        ctrl_metrics = {"manual_review_count": 3, "failed_count": 0, "total_candidates": 3}
        status, bus_status = decide_summary(ctrl_metrics, failed_log_stages=0)
        self.assertEqual(status, "SUCCEEDED")
        self.assertEqual(bus_status, "MANUAL_REVIEW_REQUIRED")

    def test_summary_no_objects_succeeded(self):
        self.assertIsNotNone(decide_summary, "decide_summary must be defined")
        ctrl_metrics = {"total_candidates": 0, "failed_count": 0}
        status, bus_status = decide_summary(ctrl_metrics, failed_log_stages=0)
        self.assertEqual(status, "SUCCEEDED")
        self.assertEqual(bus_status, "NO_OBJECTS")

    def test_summary_all_deployed_succeeded(self):
        self.assertIsNotNone(decide_summary, "decide_summary must be defined")
        ctrl_metrics = {"deployed_count": 5, "total_candidates": 5, "failed_count": 0}
        status, bus_status = decide_summary(ctrl_metrics, failed_log_stages=0)
        self.assertEqual(status, "SUCCEEDED")
        self.assertEqual(bus_status, "COMPLETE")


class TestDefect8IntegralMetadataParsing(unittest.TestCase):
    """DEFECT 8: Non-integral precision and scale rejection."""

    def test_parse_integral_metadata_behavior(self):
        self.assertIsNotNone(parse_integral_metadata, "parse_integral_metadata must be defined")
        # Accepted
        self.assertEqual(parse_integral_metadata(18), 18)
        self.assertEqual(parse_integral_metadata(18.0), 18)
        self.assertEqual(parse_integral_metadata(Decimal("18")), 18)
        self.assertEqual(parse_integral_metadata(Decimal("18.0")), 18)
        self.assertEqual(parse_integral_metadata("18"), 18)
        self.assertEqual(parse_integral_metadata("  18  "), 18)

        # Rejected
        self.assertIsNone(parse_integral_metadata(18.9))
        self.assertIsNone(parse_integral_metadata("18.5"))
        self.assertIsNone(parse_integral_metadata(True))
        self.assertIsNone(parse_integral_metadata(False))
        self.assertIsNone(parse_integral_metadata(float("nan")))
        self.assertIsNone(parse_integral_metadata(float("inf")))
        self.assertIsNone(parse_integral_metadata("abc"))
        self.assertIsNone(parse_integral_metadata(None))

    def test_oracle_fractional_precision_scale_fails_closed(self):
        mapper = OracleTypeMapper()
        res_p = mapper.map_column("NUMBER", precision=18.9, scale=0)
        self.assertEqual(res_p.status, "BLOCKED")
        self.assertEqual(res_p.fidelity, "UNKNOWN")

        res_s = mapper.map_column("NUMBER", precision=18, scale=2.7)
        self.assertEqual(res_s.status, "BLOCKED")
        self.assertEqual(res_s.fidelity, "UNKNOWN")

    def test_sqlserver_fractional_precision_scale_fails_closed(self):
        mapper = SqlServerTypeMapper()
        res_p = mapper.map_column("DECIMAL", precision=18.9, scale=2)
        self.assertEqual(res_p.status, "BLOCKED")
        self.assertEqual(res_p.fidelity, "UNKNOWN")

        res_s = mapper.map_column("DECIMAL", precision=18, scale=2.7)
        self.assertEqual(res_s.status, "BLOCKED")
        self.assertEqual(res_s.fidelity, "UNKNOWN")


class TestItems123DirectExecutions(unittest.TestCase):
    """Direct execution tests for:
    Item 1: NB22 target ownership decision
    Item 2: converter convert_view (QUOTED_IDENT & undotted bare name) and validate_converted_sql
    Item 3: NB22 passing object_map to convert_artifact
    """

    def test_item1_nb22_ownership_decision_direct(self):
        """Execute NB22's ownership decision directly for OWNED, FREE, CONFLICT, and AMBIGUOUS."""
        self.assertIsNotNone(build_target_ownership, "build_target_ownership must be defined")
        self.assertIsNotNone(check_target_ownership, "check_target_ownership must be defined")

        historical_control_rows = [
            {"target_catalog": "cat", "target_schema": "sch", "target_object_name": "v_owned", "artifact_id": "art_1"},
            {"target_catalog": "cat", "target_schema": "sch", "target_object_name": "v_conflict", "artifact_id": "art_other"},
            {"target_catalog": "cat", "target_schema": "sch", "target_object_name": "v_ambig", "artifact_id": "art_a"},
            {"target_catalog": "cat", "target_schema": "sch", "target_object_name": "v_ambig", "artifact_id": "art_b"},
        ]
        ownership = build_target_ownership(historical_control_rows)

        # 1. FREE target
        target_free = "cat.sch.v_free"
        status_free = check_target_ownership(ownership, target_free, "art_1")
        self.assertEqual(status_free, "FREE")
        can_deploy_free, _, _ = decide_target_deployment_action(ownership, target_free, "art_1")
        self.assertTrue(can_deploy_free)

        # Simulate successful deploy updating ownership
        if target_free not in ownership:
            ownership[target_free] = set()
        ownership[target_free].add("art_1")
        self.assertEqual(check_target_ownership(ownership, target_free, "art_1"), "OWNED")

        # 2. OWNED target by same artifact_id
        target_owned = "cat.sch.v_owned"
        status_owned = check_target_ownership(ownership, target_owned, "art_1")
        self.assertEqual(status_owned, "OWNED")
        can_deploy_owned, _, _ = decide_target_deployment_action(ownership, target_owned, "art_1")
        self.assertTrue(can_deploy_owned)

        # 3. CONFLICT target owned by art_other
        target_conflict = "cat.sch.v_conflict"
        status_conflict = check_target_ownership(ownership, target_conflict, "art_1")
        self.assertEqual(status_conflict, "CONFLICT")
        can_deploy_conflict, _, err_conflict = decide_target_deployment_action(ownership, target_conflict, "art_1")
        self.assertFalse(can_deploy_conflict)
        self.assertIn("TARGET_OWNERSHIP_CONFLICT", err_conflict)

        # 4. AMBIGUOUS target owned by multiple artifacts
        target_ambig = "cat.sch.v_ambig"
        status_ambig = check_target_ownership(ownership, target_ambig, "art_1")
        self.assertEqual(status_ambig, "AMBIGUOUS")
        can_deploy_ambig, _, err_ambig = decide_target_deployment_action(ownership, target_ambig, "art_1")
        self.assertFalse(can_deploy_ambig)
        self.assertIn("TARGET_OWNERSHIP_CONFLICT", err_ambig)

    def test_item2_converter_quoted_ident_and_bare_name_and_validate_catalog_direct(self):
        """Execute converter directly for QUOTED_IDENT, undotted bare names, CTEs, and catalog validation."""
        conv = SQLArtifactConverter()

        # A) Next token after FROM is QUOTED_IDENT -> MANUAL_REVIEW
        res_double_quoted = conv.convert_artifact(
            "oracle", "VIEW", "v_quoted",
            'CREATE VIEW v_quoted AS SELECT * FROM "raw_table"',
            "cat", "sch"
        )
        self.assertEqual(res_double_quoted.classification, "MANUAL_REVIEW")
        self.assertIn("Quoted identifier", res_double_quoted.manual_review_reason)

        res_backticked = conv.convert_artifact(
            "sqlserver", "VIEW", "v_backticked",
            'CREATE VIEW v_backticked AS SELECT * FROM `raw_table`',
            "cat", "sch"
        )
        self.assertEqual(res_backticked.classification, "MANUAL_REVIEW")
        self.assertIn("Quoted identifier", res_backticked.manual_review_reason)

        res_bracketed = conv.convert_artifact(
            "oracle", "VIEW", "v_bracketed",
            'CREATE VIEW v_bracketed AS SELECT * FROM [raw_table]',
            "cat", "sch"
        )
        self.assertEqual(res_bracketed.classification, "MANUAL_REVIEW")
        self.assertIn("Quoted identifier", res_bracketed.manual_review_reason)

        # B) Next token after FROM is undotted bare name not in object_map and not CTE -> MANUAL_REVIEW
        res_bare = conv.convert_artifact(
            "sqlserver", "VIEW", "v_bare",
            "CREATE VIEW v_bare AS SELECT id FROM raw_orders",
            "cat", "sch"
        )
        self.assertEqual(res_bare.classification, "MANUAL_REVIEW")
        self.assertIn("Unresolved bare table reference", res_bare.manual_review_reason)

        # C) Undotted bare name that IS a CTE name -> AUTO
        res_cte = conv.convert_artifact(
            "sqlserver", "VIEW", "v_cte",
            "CREATE VIEW v_cte AS WITH raw_orders AS (SELECT 1 AS id) SELECT id FROM raw_orders",
            "cat", "sch"
        )
        self.assertEqual(res_cte.classification, "AUTO")

        # D) Undotted bare name in object_map -> AUTO
        res_bare_mapped = conv.convert_artifact(
            "sqlserver", "VIEW", "v_bare_mapped",
            "CREATE VIEW v_bare_mapped AS SELECT id FROM raw_orders",
            "cat", "sch",
            object_map={"raw_orders": "`cat`.`sch`.`raw_orders`"}
        )
        self.assertEqual(res_bare_mapped.classification, "AUTO")

        # E) validate_converted_sql rejects any dotted reference whose first part is not target catalog
        val_bad_oracle, err_bad_oracle = conv.validate_converted_sql(
            "CREATE OR REPLACE VIEW `cat`.`sch`.`v` AS SELECT * FROM oracle_schema.view1",
            "cat", "sch", "v"
        )
        self.assertFalse(val_bad_oracle)
        self.assertIn("oracle_schema.view1", err_bad_oracle)

        val_bad_cat, err_bad_cat = conv.validate_converted_sql(
            "CREATE OR REPLACE VIEW `cat`.`sch`.`v` AS SELECT * FROM other_catalog.sch.tbl",
            "cat", "sch", "v"
        )
        self.assertFalse(val_bad_cat)
        self.assertIn("other_catalog.sch.tbl", err_bad_cat)

        val_bad_dbo, err_bad_dbo = conv.validate_converted_sql(
            "CREATE OR REPLACE VIEW `cat`.`sch`.`v` AS SELECT * FROM dbo.tbl",
            "cat", "sch", "v"
        )
        self.assertFalse(val_bad_dbo)
        self.assertIn("dbo.tbl", err_bad_dbo)

        # Valid target catalog and alias
        val_ok, err_ok = conv.validate_converted_sql(
            "CREATE OR REPLACE VIEW `cat`.`sch`.`v` AS SELECT a.id FROM `cat`.`sch`.`tbl` a",
            "cat", "sch", "v"
        )
        self.assertTrue(val_ok)
        self.assertIsNone(err_ok)

    def test_item3_nb22_passes_object_map_into_convert_artifact(self):
        """Confirm NB22 passes object_map=build_object_map(...) into convert_artifact."""
        with open("notebooks/shared/NB22_SQLArtifactMigrate.py", "r", encoding="utf-8") as f:
            nb22_code = f.read()

        # Verify NB22 builds obj_map and passes it to convert_artifact
        self.assertIn("obj_map = build_object_map(stc_rows, conn_id, src_db)", nb22_code)
        self.assertIn("object_map=obj_map", nb22_code)

        # Directly execute build_object_map + convert_artifact integration
        stc_rows = [
            {
                "connection_id": "conn1",
                "source_database": "db1",
                "source_schema": "dbo",
                "source_table": "customers",
                "target_catalog": "cat",
                "target_schema": "sch",
                "target_table": "customers",
                "is_active": True,
                "initial_load_completed": True,
            }
        ]
        obj_map = build_object_map(stc_rows, "conn1", "db1")
        conv = SQLArtifactConverter()
        sql = "CREATE VIEW v_cust AS SELECT id FROM dbo.customers"
        res = conv.convert_artifact(
            "sqlserver", "VIEW", "v_cust", sql, "cat", "sch", object_map=obj_map
        )
        self.assertEqual(res.classification, "AUTO")
        self.assertIn("FROM `cat`.`sch`.`customers`", res.converted_definition)


# ---------------------------------------------------------------------------
# Harness and Helpers for NB22 and Items A through E
# ---------------------------------------------------------------------------

from tests.test_sql_object_artifact_materialization import (
    FakeWidgets,
    FakeJobTaskValues,
    FakeNotebookExit,
    FakeFilesystem,
    FakeFileIO,
    FakeDbutils,
    DummyType,
    StructType,
    StructField,
    StringType,
    TimestampType,
)
from src.sql_artifact_scope import (
    build_candidate_query,
    validate_max_artifacts,
)
from src.sql_artifact_converter import (
    wrap_oracle_view_definition_if_needed,
)
from src.sql_artifact_control_common import (
    resolve_target_catalog_and_schema,
    compute_artifact_id,
)


class IntegerType(DummyType):
    pass


class BooleanType(DummyType):
    pass


class FakeDataFrameWriter:
    def __init__(self, df, appends=None):
        self.df = df
        self.appends = appends if appends is not None else []

    def format(self, fmt):
        return self

    def mode(self, m):
        return self

    def saveAsTable(self, table_name):
        self.appends.append((table_name, self.df.rows))


class FakeDataFrameNB22:
    def __init__(self, rows, schema=None, spark=None):
        self.rows = rows
        self.schema = schema
        self.spark = spark

    def collect(self):
        return self.rows

    def createOrReplaceTempView(self, name):
        if self.spark:
            self.spark.temp_views[name] = self.rows

    @property
    def write(self):
        return FakeDataFrameWriter(self, getattr(self.spark, "table_appends", None))


class FakeSparkSessionNB22:
    def __init__(
        self,
        candidates=None,
        historical_control_rows=None,
        stc_rows=None,
        fail_stc_read=False,
        fail_historical_control_read=False,
    ):
        self.candidates = candidates or []
        self.historical_control_rows = historical_control_rows or []
        self.stc_rows = stc_rows or []
        self.fail_stc_read = fail_stc_read
        self.fail_historical_control_read = fail_historical_control_read
        self.executed_queries = []
        self.temp_views = {}
        self.table_appends = []

    def sql(self, query):
        self.executed_queries.append(query)
        q = query.strip()
        if "source_table_control" in q:
            if self.fail_stc_read:
                raise RuntimeError("Failed to read source_table_control: mock error")
            return FakeDataFrameNB22([FakeRow(**r) for r in self.stc_rows], spark=self)
        if "sql_artifact_control" in q and q.upper().startswith("SELECT"):
            if self.fail_historical_control_read:
                raise RuntimeError("Failed to read historical control rows: mock error")
            return FakeDataFrameNB22([FakeRow(**r) for r in self.historical_control_rows], spark=self)
        if "sql_artifact_control" in q and "MERGE INTO" in q:
            return FakeDataFrameNB22([], spark=self)
        if "source_assessment" in q and "sql_object_assessment" in q:
            return FakeDataFrameNB22([FakeRow(**r) for r in self.candidates], spark=self)
        return FakeDataFrameNB22([], spark=self)

    def createDataFrame(self, rows, schema=None):
        return FakeDataFrameNB22(rows, schema=schema, spark=self)


class FakeControlRepoNB22:
    def __init__(self, target_configs=None, connections=None):
        self.target_configs = target_configs or {}
        self.connections = connections or {}

    def resolve_target_config(self, conn_id):
        if conn_id in self.target_configs:
            cfg = self.target_configs[conn_id]
            if isinstance(cfg, Exception):
                raise cfg
            return cfg
        return {
            "target_catalog": "da_accelerators",
            "target_schema_mode": "SAME_AS_SOURCE",
            "target_schema": None,
        }

    def get_connection(self, conn_id):
        if conn_id in self.connections:
            c = self.connections[conn_id]
            if isinstance(c, Exception):
                raise c
            return c
        return {
            "connection_id": conn_id,
            "is_active": True,
            "connection_status": "VALID",
            "secret_scope": "test_scope",
        }


def run_nb22_harness(
    candidates=None,
    historical_control_rows=None,
    stc_rows=None,
    target_configs=None,
    connections=None,
    widget_values=None,
    fail_stc_read=False,
    fail_historical_control_read=False,
    fail_file_write=False,
    fs=None,
):
    import builtins
    if fs is None:
        fs = FakeFilesystem()

    orig_os_makedirs = os.makedirs
    orig_os_replace = os.replace
    orig_os_remove = os.remove
    orig_os_path_exists = os.path.exists
    orig_builtins_open = builtins.open

    def fake_makedirs(path, exist_ok=True):
        fs.makedirs(path, exist_ok=exist_ok)

    def fake_replace(src, dst):
        if fail_file_write:
            raise OSError("Simulated volume disk write I/O error")
        fs.replace(src, dst)

    def fake_remove(path):
        fs.remove(path)

    def fake_exists(path):
        return fs.exists(path)

    def fake_open(path, mode="r", **kwargs):
        if "w" in mode and isinstance(path, str) and "/Volumes/" in path:
            return FakeFileIO(fs, path)
        if "r" in mode and isinstance(path, str) and "/Volumes/" in path:
            if fs.exists(path):
                import io
                return io.StringIO(fs.files[path])
        return orig_builtins_open(path, mode, **kwargs)

    os.makedirs = fake_makedirs
    os.replace = fake_replace
    os.remove = fake_remove
    os.path.exists = fake_exists
    builtins.open = fake_open

    spark = FakeSparkSessionNB22(
        candidates=candidates,
        historical_control_rows=historical_control_rows,
        stc_rows=stc_rows,
        fail_stc_read=fail_stc_read,
        fail_historical_control_read=fail_historical_control_read,
    )
    repo = FakeControlRepoNB22(target_configs=target_configs, connections=connections)
    w_vals = dict(widget_values or {})
    w_vals.setdefault("run_id", "run-test-22")
    dbutils = FakeDbutils(widget_values=w_vals)
    task_values = {}

    def set_task_value(k, v):
        task_values[k] = v

    import sys
    pyspark_types = type("pyspark_types", (), {
        "StructType": StructType,
        "StructField": StructField,
        "StringType": StringType,
        "IntegerType": IntegerType,
        "BooleanType": BooleanType,
        "TimestampType": TimestampType,
    })
    pyspark_sql = type("pyspark_sql", (), {"types": pyspark_types, "functions": type("F", (), {})()})
    pyspark_mod = type("pyspark", (), {"sql": pyspark_sql})

    sys.modules["pyspark"] = pyspark_mod
    sys.modules["pyspark.sql"] = pyspark_sql
    sys.modules["pyspark.sql.types"] = pyspark_types

    nb22_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "notebooks", "shared", "NB22_SQLArtifactMigrate.py"
    )
    with open(nb22_path, "r", encoding="utf-8") as f:
        code_lines = [l for l in f if not l.strip().startswith("%run") and not l.strip().startswith("# MAGIC %run")]
    code = "".join(code_lines)

    env = {
        "__file__": nb22_path,
        "__name__": "__main__",
        "spark": spark,
        "dbutils": dbutils,
        "control_repo": lambda: repo,
        "get_run_id": lambda: w_vals.get("run_id", "run-test-22"),
        "CATALOG": "da_accelerators",
        "CONTROL_SCHEMA": "control",
        "quote_databricks": lambda s: f"`{s}`",
        "escape_string_literal": lambda s: f"'{s}'",
        "set_task_value": set_task_value,
        "StructType": StructType,
        "StructField": StructField,
        "StringType": StringType,
        "IntegerType": IntegerType,
        "BooleanType": BooleanType,
        "TimestampType": TimestampType,
        "os": os,
        "uuid": __import__("uuid"),
        "json": __import__("json"),
        "datetime": __import__("datetime").datetime,
        "timezone": __import__("datetime").timezone,
    }

    error_raised = None
    exit_payload = None

    try:
        exec(code, env)
    except FakeNotebookExit as fne:
        import json
        exit_payload = json.loads(fne.payload)
    except Exception as exc:
        error_raised = exc
    finally:
        os.makedirs = orig_os_makedirs
        os.replace = orig_os_replace
        os.remove = orig_os_remove
        os.path.exists = orig_os_path_exists
        builtins.open = orig_builtins_open

    return {
        "exit_payload": exit_payload,
        "task_values": task_values,
        "spark": spark,
        "fs": fs,
        "error_raised": error_raised,
    }


class TestConvertAndStorePipelineRequirements(unittest.TestCase):
    """Executable tests for each item in Sections A, B, C, D, and E (Item 20)."""

    def setUp(self):
        from datetime import datetime, timezone
        self.now = datetime(2026, 9, 21, 10, 0, 0, tzinfo=timezone.utc)
        self.standard_stc = [
            {
                "connection_id": "conn1",
                "source_database": None,
                "source_schema": "HR",
                "source_table": "employees",
                "target_catalog": "da_accelerators",
                "target_schema": "hr",
                "target_table": "employees",
                "is_active": True,
                "initial_load_completed": True,
            }
        ]

    # --- SECTION A: Remove deployment ---

    def test_item01_nb22_never_executes_converted_sql_and_only_creates_schema_for_volume(self):
        """Item 1: NB22 removes every spark.sql call executing converted SQL; CREATE SCHEMA is used only for volume."""
        candidate = {
            "connection_id": "conn1",
            "source_system": "oracle",
            "source_database": None,
            "source_schema": "HR",
            "object_name": "V_EMP",
            "object_type": "VIEW",
            "source_definition": "SELECT employee_id FROM hr.employees",
            "captured_ts": self.now,
            "run_id": "run-test-22",
            "is_selected": True,
        }
        res = run_nb22_harness(candidates=[candidate], stc_rows=self.standard_stc)
        self.assertIsNone(res["error_raised"])
        self.assertEqual(res["exit_payload"]["status"], "SUCCEEDED")

        # Verify spark.sql executed queries: no converted DDL and only CREATE SCHEMA IF NOT EXISTS / CREATE VOLUME IF NOT EXISTS
        schema_created = False
        volume_created = False
        for query in res["spark"].executed_queries:
            q_upper = query.upper().strip()
            self.assertFalse(
                q_upper.startswith("CREATE OR REPLACE VIEW") or q_upper.startswith("CREATE VIEW"),
                f"Converted DDL must never be executed via spark.sql: {query}",
            )
            if "CREATE SCHEMA" in q_upper:
                self.assertTrue(
                    q_upper.startswith("CREATE SCHEMA IF NOT EXISTS"),
                    f"Only CREATE SCHEMA IF NOT EXISTS is allowed to hold the volume: {query}",
                )
                schema_created = True
            elif "CREATE VOLUME" in q_upper:
                self.assertTrue(
                    schema_created,
                    "CREATE SCHEMA IF NOT EXISTS must precede CREATE VOLUME IF NOT EXISTS",
                )
                volume_created = True
            elif q_upper.startswith("CREATE TABLE") and ("SQL_ARTIFACT_CONTROL" in q_upper or "SQL_ARTIFACT_EXECUTION_LOG" in q_upper):
                continue
            elif q_upper.startswith("CREATE "):
                self.fail(f"No other CREATE statement allowed in NB22: {query}")

        self.assertTrue(schema_created, "CREATE SCHEMA IF NOT EXISTS should be executed for volume")
        self.assertTrue(volume_created, "CREATE VOLUME IF NOT EXISTS should be executed")

    def test_item02_nb22_converts_stores_volume_atomic_not_deployed(self):
        """Item 2: NB22 converts, writes .sql atomic file to volume, deployment_status=NOT_DEPLOYED."""
        cands = [
            # AUTO candidate
            {
                "connection_id": "conn1",
                "source_system": "oracle",
                "source_database": None,
                "source_schema": "HR",
                "object_name": "V_AUTO",
                "object_type": "VIEW",
                "source_definition": "SELECT employee_id FROM hr.employees",
                "captured_ts": self.now,
                "run_id": "run-test-22",
                "is_selected": True,
            },
            # MANUAL_REVIEW candidate (bare table)
            {
                "connection_id": "conn1",
                "source_system": "oracle",
                "source_database": None,
                "source_schema": "HR",
                "object_name": "V_MANUAL",
                "object_type": "VIEW",
                "source_definition": "SELECT id FROM unmapped_bare_table",
                "captured_ts": self.now,
                "run_id": "run-test-22",
                "is_selected": True,
            },
        ]
        res = run_nb22_harness(candidates=cands, stc_rows=self.standard_stc)
        self.assertIsNone(res["error_raised"])
        self.assertEqual(res["exit_payload"]["status"], "SUCCEEDED")

        # Converted SQL written as .sql files under /Volumes/da_accelerators/ConvertedArtifacts/converted_artifacts/
        files = res["fs"].files
        self.assertIn("/Volumes/da_accelerators/ConvertedArtifacts/converted_artifacts/_no_database/HR/views/V_AUTO.sql", files)
        self.assertIn("/Volumes/da_accelerators/ConvertedArtifacts/converted_artifacts/_no_database/HR/views/V_MANUAL.sql", files)
        self.assertGreaterEqual(res["fs"].replace_count, 2, "Atomic replace must be used for file writes")

        # CREATE SCHEMA IF NOT EXISTS and CREATE VOLUME are executed before writing
        create_schema_queries = [q for q in res["spark"].executed_queries if "CREATE SCHEMA" in q.upper()]
        create_volume_queries = [q for q in res["spark"].executed_queries if "CREATE VOLUME" in q.upper()]
        self.assertGreaterEqual(len(create_schema_queries), 1)
        self.assertGreaterEqual(len(create_volume_queries), 1)

        # Inspect control records
        all_merged_rows = []
        for rows in res["spark"].temp_views.values():
            all_merged_rows.extend(rows)

        self.assertEqual(len(all_merged_rows), 2)
        for r in all_merged_rows:
            self.assertEqual(r["deployment_status"], "NOT_DEPLOYED")
            self.assertIsNotNone(r["converted_artifact_path"])
            self.assertIsNotNone(r["converted_definition"])
            self.assertIsNotNone(r["converted_definition_hash"])
            self.assertIn(r["conversion_classification"], ["AUTO", "MANUAL_REVIEW"])
            self.assertIsNotNone(r["source_definition"], "Raw source definition must always be preserved")

    def test_item02_nb22_write_failure_records_failed_and_partial_business_status(self):
        """NB22: If writing converted .sql file fails, count row as FAILED (error_code ARTIFACT_WRITE_FAILED), keep converted text, business_status PARTIAL."""
        candidate = {
            "connection_id": "conn1",
            "source_system": "oracle",
            "source_database": None,
            "source_schema": "HR",
            "object_name": "V_WRITE_FAIL",
            "object_type": "VIEW",
            "source_definition": "SELECT employee_id FROM hr.employees",
            "captured_ts": self.now,
            "run_id": "run-test-22",
            "is_selected": True,
        }
        res = run_nb22_harness(
            candidates=[candidate],
            stc_rows=self.standard_stc,
            fail_file_write=True,
        )
        self.assertIsNone(res["error_raised"])
        payload = res["exit_payload"]
        self.assertEqual(payload["failed_count"], 1)
        self.assertEqual(payload["artifact_write_failed_count"], 1)
        self.assertEqual(payload["business_status"], "PARTIAL")
        self.assertEqual(payload["status"], "SUCCEEDED")

        # Verify control updates keep converted text and record ARTIFACT_WRITE_FAILED
        all_merged_rows = []
        for rows in res["spark"].temp_views.values():
            all_merged_rows.extend(rows)
        self.assertEqual(len(all_merged_rows), 1)
        row = all_merged_rows[0]
        self.assertEqual(row["conversion_status"], "FAILED")
        self.assertEqual(row["error_code"], "ARTIFACT_WRITE_FAILED")
        self.assertIn("ARTIFACT_WRITE_FAILED", row["error_message"])
        self.assertTrue(row["manual_review_required"])
        # Converted definition MUST be kept in sql_artifact_control
        self.assertIsNotNone(row["converted_definition"])
        self.assertIn("SELECT employee_id FROM `da_accelerators`.`hr`.`employees`", row["converted_definition"])
        self.assertIsNotNone(row["converted_definition_hash"])
        self.assertEqual(row["deployment_status"], "NOT_DEPLOYED")

        # Verify execution log
        log_appends = res["spark"].table_appends
        self.assertEqual(len(log_appends), 1)
        log_row = log_appends[0][1][0]
        self.assertEqual(log_row["status"], "FAILED")
        self.assertEqual(log_row["error_code"], "ARTIFACT_WRITE_FAILED")
        self.assertIn("ARTIFACT_WRITE_FAILED", log_row["error_message"])

    def test_item03_nb22_removed_ownership_and_deploy_retains_dedup(self):
        """Item 3: NB22 removed ownership and deploy logic, retains compute_artifact_id and same-run dedup."""
        nb22_path = os.path.join("notebooks", "shared", "NB22_SQLArtifactMigrate.py")
        with open(nb22_path, "r", encoding="utf-8") as f:
            nb22_code = f.read()

        # Unused ownership/deploy code paths must be absent
        self.assertNotIn("build_target_ownership", nb22_code)
        self.assertNotIn("check_target_ownership", nb22_code)
        self.assertNotIn("deployed_target_to_owner", nb22_code)
        self.assertIn("compute_artifact_id", nb22_code)

        # Same-run duplicate candidate deduplication
        from datetime import datetime, timezone
        cands = [
            {
                "connection_id": "conn1",
                "source_system": "oracle",
                "source_database": None,
                "source_schema": "HR",
                "object_name": "V_DUP",
                "object_type": "VIEW",
                "source_definition": "SELECT 1 FROM dual",
                "captured_ts": datetime(2026, 9, 21, 10, 0, 0, tzinfo=timezone.utc),
                "run_id": "run-test-22",
                "is_selected": True,
            },
            {
                "connection_id": "conn1",
                "source_system": "oracle",
                "source_database": None,
                "source_schema": "HR",
                "object_name": "V_DUP",
                "object_type": "VIEW",
                "source_definition": "SELECT 2 FROM dual",
                "captured_ts": datetime(2026, 9, 21, 11, 0, 0, tzinfo=timezone.utc),
                "run_id": "run-test-22",
                "is_selected": True,
            },
        ]
        res = run_nb22_harness(candidates=cands, stc_rows=self.standard_stc)
        self.assertIsNone(res["error_raised"])
        self.assertEqual(res["exit_payload"]["skipped_duplicates_count"], 1)

    def test_item04_docs_reflect_convert_and_store_only(self):
        """Item 4: Docs: say 'converts, classifies, and stores; does not deploy and does not connect to source databases'."""
        doc_files = [
            os.path.join("docs", "supported_features_and_limitations.md"),
            os.path.join("docs", "databricks_job_task_mapping.md"),
            "README.md",
        ]
        phrase = "converts, classifies, and stores; does not deploy and does not connect to source databases"
        for df in doc_files:
            with open(df, "r", encoding="utf-8") as f:
                content = f.read()
            self.assertIn(phrase, content.lower(), f"Doc {df} must contain the exact scope phrase")
            self.assertNotIn("deploys AUTO artifacts", content)
            self.assertNotIn("auto-deploys", content)

    # --- SECTION B: Candidate selection ---

    def test_item05_candidate_selection_driven_and_validations(self):
        """Item 5: Selection-driven candidates, join on run_id, validations, definition missing, unusable connection."""
        query = build_candidate_query(
            source_assessment_fqn="cat.ctrl.source_assessment",
            sql_object_assessment_fqn="cat.ctrl.sql_object_assessment",
            connection_id="conn1",
            source_database="db1",
        )
        # Selection-driven checks
        self.assertIn("sa.is_selected = true", query)
        self.assertIn("<> 'TABLE'", query)
        self.assertIn("ds.assessment_run_id = soa.run_id", query, "Must join on run_id from Assessment Job 1A")

        # Invalid max_artifacts raises ValueError
        with self.assertRaises(ValueError):
            validate_max_artifacts(-1)
        with self.assertRaises(ValueError):
            validate_max_artifacts("invalid_number")

        # Missing or blank definition -> FAILED with DEFINITION_MISSING, business_status PARTIAL
        cand_missing_def = {
            "connection_id": "conn1",
            "source_system": "oracle",
            "source_database": None,
            "source_schema": "HR",
            "object_name": "V_NO_DEF",
            "object_type": "VIEW",
            "source_definition": "   ",  # blank definition
            "captured_ts": self.now,
            "run_id": "run-test-22",
            "is_selected": True,
        }
        res_missing = run_nb22_harness(candidates=[cand_missing_def], stc_rows=self.standard_stc)
        self.assertIsNone(res_missing["error_raised"])
        self.assertEqual(res_missing["exit_payload"]["definition_missing_count"], 1)
        self.assertEqual(res_missing["exit_payload"]["business_status"], "PARTIAL")

        # Unusable connection skipped without opening JDBC
        connections = {
            "conn_broken": {
                "connection_id": "conn_broken",
                "is_active": False,
                "connection_status": "DISABLED",
            }
        }
        cand_broken = {
            "connection_id": "conn_broken",
            "source_system": "oracle",
            "source_database": None,
            "source_schema": "HR",
            "object_name": "V_BROKEN",
            "object_type": "VIEW",
            "source_definition": "SELECT 1 FROM dual",
            "captured_ts": self.now,
            "run_id": "run-test-22",
            "is_selected": True,
        }
        res_broken = run_nb22_harness(
            candidates=[cand_broken],
            connections=connections,
            stc_rows=self.standard_stc,
        )
        self.assertIsNone(res_broken["error_raised"])
        self.assertEqual(res_broken["exit_payload"]["skipped_connections_count"], 1)

    def test_item06_nb18_and_nb22_shared_candidate_query(self):
        """Item 6: NB18 and NB22 use the same candidate query through shared helper in src/."""
        nb18_path = os.path.join("notebooks", "shared", "NB18_MaterializeSourceArtifacts.py")
        nb22_path = os.path.join("notebooks", "shared", "NB22_SQLArtifactMigrate.py")
        with open(nb18_path, "r", encoding="utf-8") as f:
            nb18_code = f.read()
        with open(nb22_path, "r", encoding="utf-8") as f:
            nb22_code = f.read()

        self.assertIn("build_candidate_query", nb18_code)
        self.assertIn("candidate_query = build_candidate_query(", nb18_code)
        self.assertIn("build_candidate_query", nb22_code)
        self.assertIn("candidate_query = build_candidate_query(", nb22_code)

    # --- SECTION C: Runtime and correctness ---

    def test_item07_nb22_typing_imports(self):
        """Item 7: NB22 has 'from typing import Any, Dict, List, Optional, Tuple' fixing NameError."""
        nb22_path = os.path.join("notebooks", "shared", "NB22_SQLArtifactMigrate.py")
        with open(nb22_path, "r", encoding="utf-8") as f:
            code = f.read()
        self.assertIn("from typing import Any, Dict, List, Optional, Tuple", code)

    def test_item08_oracle_view_wrapping(self):
        """Item 8: Oracle views: wrap with CREATE OR REPLACE VIEW if not starting with CREATE."""
        # Unwrapped Oracle view
        wrapped = wrap_oracle_view_definition_if_needed(
            "oracle", "VIEW", "HR", "V_TEST", "SELECT 1 AS col FROM dual"
        )
        self.assertEqual(wrapped, 'CREATE OR REPLACE VIEW "HR"."V_TEST" AS SELECT 1 AS col FROM dual')

        # Already wrapped Oracle view
        already = 'CREATE OR REPLACE VIEW "HR"."V_TEST" AS SELECT 1 FROM dual'
        self.assertEqual(wrap_oracle_view_definition_if_needed("oracle", "VIEW", "HR", "V_TEST", already), already)

        # SQL Server view
        sql_server_def = "SELECT 1 AS col"
        self.assertEqual(
            wrap_oracle_view_definition_if_needed("sqlserver", "VIEW", "dbo", "v_test", sql_server_def),
            sql_server_def,
        )

        # Non-view object
        proc_def = "BEGIN NULL; END;"
        self.assertEqual(
            wrap_oracle_view_definition_if_needed("oracle", "PROCEDURE", "HR", "P_TEST", proc_def),
            proc_def,
        )

    def test_item09_converter_rules_code_spans_comment_mask_cte_comma_joins(self):
        """Item 9: Converter rules on CODE spans only, comment masking, CTE from masked code, comma-joins."""
        conv = SQLArtifactConverter()

        # A) Rules to CODE spans only: NVL inside string literal is preserved
        sql_string = "CREATE VIEW v1 AS SELECT 'NVL(a, b)' AS col FROM hr.employees"
        res_str = conv.convert_artifact(
            "oracle", "VIEW", "v1", sql_string, "cat", "sch",
            object_map={"hr.employees": "`cat`.`sch`.`employees`"}
        )
        self.assertEqual(res_str.classification, "AUTO")
        self.assertIn("'NVL(a, b)'", res_str.converted_definition)

        # B) LINE_COMMENT and BLOCK_COMMENT masking
        sql_comment = "CREATE VIEW v2 AS SELECT id /* NVL(1, 2) */ FROM hr.employees -- NVL(3, 4)"
        res_comment = conv.convert_artifact(
            "oracle", "VIEW", "v2", sql_comment, "cat", "sch",
            object_map={"hr.employees": "`cat`.`sch`.`employees`"}
        )
        self.assertEqual(res_comment.classification, "AUTO")

        # C) Comma-joins -> MANUAL_REVIEW
        sql_comma = "CREATE VIEW v3 AS SELECT * FROM hr.employees e, hr.departments d WHERE e.dept_id = d.id"
        res_comma = conv.convert_artifact(
            "oracle", "VIEW", "v3", sql_comma, "cat", "sch",
            object_map={
                "hr.employees": "`cat`.`sch`.`employees`",
                "hr.departments": "`cat`.`sch`.`departments`",
            }
        )
        self.assertEqual(res_comma.classification, "MANUAL_REVIEW")
        self.assertIn("comma-join", res_comma.manual_review_reason.lower())

        # D) Unresolved bare table in FROM list -> MANUAL_REVIEW
        sql_bare = "CREATE VIEW v4 AS SELECT id FROM raw_unmapped"
        res_bare = conv.convert_artifact("sqlserver", "VIEW", "v4", sql_bare, "cat", "sch")
        self.assertEqual(res_bare.classification, "MANUAL_REVIEW")
        self.assertIn("Unresolved bare table reference", res_bare.manual_review_reason)

    def test_item10_control_schema_and_merge_identity_version_2(self):
        """Item 10: NB22 control writes include identity_version = 2 and legacy_artifact_id."""
        nb22_path = os.path.join("notebooks", "shared", "NB22_SQLArtifactMigrate.py")
        with open(nb22_path, "r", encoding="utf-8") as f:
            code = f.read()

        self.assertIn('StructField("identity_version", IntegerType(), True)', code)
        self.assertIn('StructField("legacy_artifact_id", StringType(), True)', code)
        self.assertIn("t.identity_version = s.identity_version", code)
        self.assertIn("t.legacy_artifact_id = s.legacy_artifact_id", code)
        self.assertIn("s.identity_version", code)
        self.assertIn("s.legacy_artifact_id", code)

    def test_item11_plan_identity_migration_duplicate_old_aid_fails_closed(self):
        """Item 11: plan_identity_migration: duplicate old artifact_id is a conflict; NB21 fails closed."""
        old_rows = [
            {"artifact_id": "old_1", "connection_id": "c1", "source_schema": "s1", "object_name": "v1", "object_type": "VIEW"},
            {"artifact_id": "old_1", "connection_id": "c2", "source_schema": "s2", "object_name": "v2", "object_type": "VIEW"},
        ]
        updates, conflicts = plan_identity_migration(old_rows)
        self.assertEqual(updates, [])
        self.assertGreater(len(conflicts), 0)
        self.assertIn("appears 2 times", conflicts[0]["reason"])

    def test_item12_nb22_duplicate_historical_aid_raises(self):
        """Item 12: NB22 raises if historical control rows contain duplicate artifact_id."""
        historical_duplicates = [
            {"artifact_id": "aid_conflict", "connection_id": "c1"},
            {"artifact_id": "aid_conflict", "connection_id": "c2"},
        ]
        candidate = {
            "connection_id": "conn1",
            "source_system": "oracle",
            "source_database": None,
            "source_schema": "HR",
            "object_name": "V_EMP",
            "object_type": "VIEW",
            "source_definition": "SELECT 1 FROM dual",
            "captured_ts": self.now,
            "run_id": "run-test-22",
            "is_selected": True,
        }
        res = run_nb22_harness(
            candidates=[candidate],
            historical_control_rows=historical_duplicates,
            stc_rows=self.standard_stc,
        )
        self.assertIsNotNone(res["error_raised"])
        self.assertIn("Duplicate artifact_id found in historical control rows", str(res["error_raised"]))

    def test_item13_build_object_map_source_db_and_conflict_dropping_and_stc_failure(self):
        """Item 13: build_object_map excludes blank-database rows when db given; drops conflicting 2-part keys; NB22 raises on STC failure."""
        stc = [
            # Blank database row
            {
                "connection_id": "c1",
                "source_database": "",
                "source_schema": "dbo",
                "source_table": "orders",
                "target_catalog": "cat",
                "target_schema": "sch1",
                "target_table": "orders",
                "is_active": True,
                "initial_load_completed": True,
            },
            # Specific database row
            {
                "connection_id": "c1",
                "source_database": "sales_db",
                "source_schema": "dbo",
                "source_table": "orders",
                "target_catalog": "cat",
                "target_schema": "sch2",
                "target_table": "orders",
                "is_active": True,
                "initial_load_completed": True,
            },
            # Conflicting 2-part key: dbo.customers maps to two different targets
            {
                "connection_id": "c1",
                "source_database": "sales_db",
                "source_schema": "dbo",
                "source_table": "customers",
                "target_catalog": "cat",
                "target_schema": "sch_a",
                "target_table": "customers",
                "is_active": True,
                "initial_load_completed": True,
            },
            {
                "connection_id": "c1",
                "source_database": "finance_db",
                "source_schema": "dbo",
                "source_table": "customers",
                "target_catalog": "cat",
                "target_schema": "sch_b",
                "target_table": "customers",
                "is_active": True,
                "initial_load_completed": True,
            },
        ]
        # When source_database is given, blank-database rows are excluded
        om = build_object_map(stc, "c1", source_database="sales_db")
        self.assertIn("dbo.orders", om)
        self.assertEqual(om["dbo.orders"], "`cat`.`sch2`.`orders`")

        # Conflict dropping: dbo.customers mapped to 2 different targets across c1
        om_all = build_object_map(stc, "c1")
        self.assertNotIn("dbo.customers", om_all, "Conflicting 2-part key must be dropped")

        # NB22 raises if reading source_table_control fails
        res_fail = run_nb22_harness(
            candidates=[],
            fail_stc_read=True,
        )
        self.assertIsNotNone(res_fail["error_raised"])
        self.assertIn("Failed to read source_table_control", str(res_fail["error_raised"]))

    def test_item14_shared_target_path_derivation(self):
        """Item 14: NB18 and NB22 derive target catalog/schema with shared helper using normalize_target_identifier."""
        nb18_path = os.path.join("notebooks", "shared", "NB18_MaterializeSourceArtifacts.py")
        nb22_path = os.path.join("notebooks", "shared", "NB22_SQLArtifactMigrate.py")
        with open(nb18_path, "r", encoding="utf-8") as f:
            nb18_code = f.read()
        with open(nb22_path, "r", encoding="utf-8") as f:
            nb22_code = f.read()

        self.assertIn("resolve_target_catalog_and_schema", nb18_code)
        self.assertIn("resolve_target_catalog_and_schema", nb22_code)

        # Direct execution of resolve_target_catalog_and_schema
        cat, sch = resolve_target_catalog_and_schema(
            {"target_catalog": "MY_CATALOG", "target_schema_mode": "SAME_AS_SOURCE", "target_schema": None},
            source_database="sales",
            source_schema="HR_SCHEMA",
        )
        self.assertEqual(cat, "MY_CATALOG")
        self.assertEqual(sch, "hr_schema")

    # --- SECTION D: Job and failure propagation ---

    def test_item15_nb13_untouched(self):
        """Item 15: Do not change NB13 (Oracle or SQL Server)."""
        oracle_nb13 = os.path.join("notebooks", "sources", "oracle", "NB13_SQLObjectAssessmentAndConversion.py")
        sqlserver_nb13 = os.path.join("notebooks", "sources", "sqlserver", "NB13_SQLObjectAssessmentAndConversion.py")
        self.assertTrue(os.path.exists(oracle_nb13), "Oracle NB13 must exist")
        self.assertTrue(os.path.exists(sqlserver_nb13), "SQL Server NB13 must exist")
        import subprocess
        res = subprocess.run(
            ["git", "status", "--porcelain", oracle_nb13, sqlserver_nb13],
            capture_output=True,
            text=True,
        )
        self.assertEqual(res.stdout.strip(), "", "NB13 files must remain untouched in git status")

    def test_item16_yaml_task_graph(self):
        """Item 16: YAML: exactly T00 -> T03 -> T04 -> T05 -> T06 with required dependencies and run_if."""
        yaml_path = os.path.join("jobs", "ACCELERATOR_SQL_ARTIFACT_MIGRATION.yaml")
        with open(yaml_path, "r", encoding="utf-8") as f:
            job_spec = yaml.safe_load(f)

        tasks = {t["task_key"]: t for t in job_spec["tasks"]}
        expected_task_keys = {
            "T00_Create_Run_Context",
            "T03_Init_SQL_Artifact_Control",
            "T23_Fetch_Selected_SQL_Artifacts",
            "T24_Lakebridge_Analyze_And_Transpile",
            "T06_SQL_Artifact_Summary",
        }
        self.assertEqual(set(tasks.keys()), expected_task_keys)

        # Removed tasks:
        self.assertNotIn("T01a_Capture_Oracle_Definitions", tasks)
        self.assertNotIn("T01b_Capture_SQLServer_Definitions", tasks)
        self.assertNotIn("T02a_Get_Oracle_Delta_Worklist", tasks)
        self.assertNotIn("T02b_Get_SQLServer_Delta_Worklist", tasks)
        self.assertNotIn("T04_Materialize_Source_Artifacts", tasks)
        self.assertNotIn("T05_Migrate_SQL_Artifacts", tasks)

        # Dependencies
        self.assertEqual([d["task_key"] for d in tasks["T03_Init_SQL_Artifact_Control"].get("depends_on", [])], ["T00_Create_Run_Context"])
        self.assertEqual([d["task_key"] for d in tasks["T23_Fetch_Selected_SQL_Artifacts"].get("depends_on", [])], ["T03_Init_SQL_Artifact_Control"])
        self.assertEqual([d["task_key"] for d in tasks["T24_Lakebridge_Analyze_And_Transpile"].get("depends_on", [])], ["T23_Fetch_Selected_SQL_Artifacts"])
        self.assertEqual([d["task_key"] for d in tasks["T06_SQL_Artifact_Summary"].get("depends_on", [])], ["T24_Lakebridge_Analyze_And_Transpile"])

        # run_if
        self.assertEqual(tasks["T23_Fetch_Selected_SQL_Artifacts"].get("run_if"), "ALL_SUCCESS")
        self.assertEqual(tasks["T24_Lakebridge_Analyze_And_Transpile"].get("run_if"), "ALL_SUCCESS")
        self.assertEqual(tasks["T06_SQL_Artifact_Summary"].get("run_if"), "ALL_DONE")

        # T06 receives T24 status
        t06_params = tasks["T06_SQL_Artifact_Summary"]["notebook_task"]["base_parameters"]
        self.assertEqual(t06_params.get("t24_status"), "{{tasks.T24_Lakebridge_Analyze_And_Transpile.values.status}}")

    def test_item17_yaml_parameters(self):
        """Item 17: YAML parameters: catalog, control_schema, connection_id, source_database, max_artifacts."""
        yaml_path = os.path.join("jobs", "ACCELERATOR_SQL_ARTIFACT_MIGRATION.yaml")
        with open(yaml_path, "r", encoding="utf-8") as f:
            job_spec = yaml.safe_load(f)

        job_params = job_spec.get("parameters", [])
        param_names = [p["name"] for p in job_params]
        expected_params = ["catalog", "control_schema", "connection_id", "source_database", "max_artifacts"]
        self.assertEqual(param_names, expected_params)

        for disallowed in ["only_source_system", "only_assessment_id", "allow_global_scope", "selection_mode"]:
            self.assertNotIn(disallowed, param_names)

        # Pass catalog and control_schema to every task
        for t in job_spec["tasks"]:
            bp = t["notebook_task"]["base_parameters"]
            self.assertIn("catalog", bp, f"Task {t['task_key']} must have catalog parameter")
            self.assertIn("control_schema", bp, f"Task {t['task_key']} must have control_schema parameter")

    def test_item18_nb18_missing_definition_partial(self):
        """Item 18: NB18: if any selected object was skipped for missing or blank definition, business_status = PARTIAL."""
        candidate = {
            "connection_id": "conn1",
            "source_system": "oracle",
            "source_database": None,
            "source_schema": "HR",
            "object_name": "V_EMPTY",
            "object_type": "VIEW",
            "source_definition": None,  # Missing definition
            "captured_ts": self.now,
            "run_id": "run-test-18",
            "is_selected": True,
        }
        res = run_nb18_harness(candidates=[candidate])
        self.assertIsNone(res["error"])
        self.assertEqual(res["exit_payload"]["business_status"], "PARTIAL")
        self.assertEqual(res["exit_payload"]["skipped_count"], 1)

    def test_item19_summary_notebook_t05_status_and_counts(self):
        """Item 19: Summary: if T05's status is not SUCCEEDED, report FAILED and raise; report required counts."""
        # When t05_status is not SUCCEEDED -> status FAILED
        status_failed, _ = decide_summary(
            ctrl_metrics={},
            t05_status="FAILED",
        )
        self.assertEqual(status_failed, "FAILED")

        # When t05_status is SUCCEEDED with missing definition -> PARTIAL
        status_partial, b_status_partial = decide_summary(
            ctrl_metrics={"total_artifacts": 1, "definition_missing": 1},
            t05_status="SUCCEEDED",
        )
        self.assertEqual(status_partial, "SUCCEEDED")
        self.assertEqual(b_status_partial, "PARTIAL")

        # Verify summary notebook source contains raise check on t05_status
        summary_path = os.path.join("notebooks", "deployment", "NB_SQLArtifactSummary.py")
        with open(summary_path, "r", encoding="utf-8") as f:
            summary_code = f.read()
        self.assertIn('if t05_status and str(t05_status).strip().upper() != "SUCCEEDED":', summary_code)
        self.assertIn("raise RuntimeError(", summary_code)

    def test_item20_no_jdbc_or_source_adapter_in_artifact_job(self):
        """Item 20 Bullet: no JDBC or source adapter is created anywhere in the artifact job."""
        artifact_job_files = [
            os.path.join("notebooks", "shared", "NB18_MaterializeSourceArtifacts.py"),
            os.path.join("notebooks", "shared", "NB21_SQLArtifactInit.py"),
            os.path.join("notebooks", "shared", "NB22_SQLArtifactMigrate.py"),
            os.path.join("notebooks", "deployment", "NB_SQLArtifactSummary.py"),
        ]
        forbidden_patterns = [
            r"DriverManager\.getConnection",
            r"jdbc:[a-zA-Z0-9]+://",
            r"\bcreate_source_adapter\b",
            r"\bOracleAdapter\b",
            r"\bSqlServerAdapter\b",
            r"\.get_table_reader\b",
        ]
        for file_path in artifact_job_files:
            with open(file_path, "r", encoding="utf-8") as f:
                content = f.read()
            for pattern in forbidden_patterns:
                matches = re.findall(pattern, content)
                self.assertEqual(matches, [], f"Found forbidden JDBC/adapter usage '{pattern}' in {file_path}")


if __name__ == "__main__":
    unittest.main()

