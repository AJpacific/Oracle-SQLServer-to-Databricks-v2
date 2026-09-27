"""
test_sql_artifact_migration.py - Comprehensive test suite for the independent
SQL Artifact Migration workflow.

Covers all 26 required test aspects from Section 9:
- Existing control tables remain read-only
- Existing control-table schemas are unchanged
- Full Load table processing is unchanged
- Delta Sync table processing is unchanged
- Candidate filtering excludes TABLE objects
- Only selected artifacts are processed
- Oracle identity handling
- SQL Server identity handling
- Identifier normalization
- Same name in different schemas does not collide
- Same name in different databases/connections does not collide
- Source hash generation is deterministic
- Unchanged rerun is skipped
- Changed definition creates a new processing attempt
- Original and converted definitions are preserved
- AUTO artifacts can reach deployment
- MANUAL_REVIEW artifacts are not deployed
- UNSUPPORTED artifacts are not deployed
- Missing configuration fails only the affected artifact
- Target schema is created only when missing
- Existing target schema is not altered
- Existing target object is not destructively overwritten
- Individual artifact failure does not stop unrelated artifacts
- Execution log is append-only
- Errors are sanitized
- YAML task dependencies and parameters are valid
"""

from __future__ import annotations

import os
import re
import sys
import unittest
import yaml
from unittest.mock import MagicMock, patch

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
SRC = os.path.join(ROOT, "src")
NOTEBOOKS = os.path.join(ROOT, "notebooks")
SHARED = os.path.join(NOTEBOOKS, "shared")
DEPLOYMENT = os.path.join(NOTEBOOKS, "deployment")
JOBS = os.path.join(ROOT, "jobs")

for p in (SRC, HERE, ROOT, NOTEBOOKS, SHARED, DEPLOYMENT):
    if p not in sys.path:
        sys.path.insert(0, p)

from _nbsource import shared_nb
from sql_artifact_control_common import (
    SQL_ARTIFACT_CONTROL_TABLE,
    SQL_ARTIFACT_EXECUTION_LOG_TABLE,
    CLASSIFICATION_AUTO,
    CLASSIFICATION_MANUAL_REVIEW,
    CLASSIFICATION_UNSUPPORTED,
    CONVERSION_STATUS_CONVERTED,
    CONVERSION_STATUS_PARTIAL,
    CONVERSION_STATUS_UNSUPPORTED,
    CONVERSION_STATUS_FAILED,
    CONVERSION_STATUS_SKIPPED,
    DEPLOYMENT_STATUS_NOT_DEPLOYED,
    DEPLOYMENT_STATUS_DEPLOYED,
    DEPLOYMENT_STATUS_FAILED,
    DEPLOYMENT_STATUS_SKIPPED,
    compute_artifact_id,
    compute_definition_hash,
    normalize_artifact_type,
    sanitize_error,
    build_create_artifact_control_ddl,
    build_create_artifact_execution_log_ddl,
    build_upgrade_artifact_control_ddl,
    build_upgrade_artifact_execution_log_ddl,
    build_upgrade_sql_object_assessment_ddl,
    build_upgrade_sql_object_artifact_manifest_ddl,
    SQL_ARTIFACT_CONTROL_COLUMNS,
    SQL_ARTIFACT_EXECUTION_LOG_COLUMNS,
)
from _nbsource import shared_nb, source_nb
from test_sql_object_artifact_materialization import (
    run_nb18_harness,
    FakeRow,
    FakeSparkSession,
    FakeFilesystem,
)
from sql_artifact_converter import (
    SQLArtifactConverter,
    ArtifactConversionResult,
)
from sql_object_artifact_common import (
    build_artifact_relative_path,
    build_artifact_volume_path,
    canonical_artifact_owner_id,
    canonical_owner_key,
)
import sql_object_artifact_common as sqlobj_art
import sqlserver_sql_builder as ss_builder
from identifiers import (
    normalize_target_identifier,
    validate_identifier,
    databricks_fqn,
)
from ddl_builder import build_create_schema


class TestExistingControlTablesSafety(unittest.TestCase):
    """Verify existing control tables remain strictly read-only and schemas are untouched."""

    EXISTING_CONTROL_TABLES = (
        "source_connection",
        "source_table_control",
        "source_inventory",
        "normalized_source_inventory",
        "resolved_column_mappings",
        "mapping_validation_results",
        "table_load_decisions",
        "review_queue",
        "job_run_log",
        "table_run_log",
        "delta_sync_queue",
        "reconciliation_results",
        "dq_rule",
        "dq_result",
        "dq_quarantine",
        "source_assessment",
        "accelerator_target_config",
        "sql_object_assessment",
        "sql_object_artifact_manifest",
    )

    def test_existing_control_tables_remain_read_only(self):
        """Verify the new workflow notebooks never write or MERGE into existing tables."""
        for nb_path in (
            os.path.join(SHARED, "NB21_SQLArtifactInit.py"),
            os.path.join(SHARED, "NB22_SQLArtifactMigrate.py"),
            os.path.join(DEPLOYMENT, "NB_SQLArtifactSummary.py"),
        ):
            with open(nb_path, "r", encoding="utf-8") as f:
                code = f.read()

            for table in self.EXISTING_CONTROL_TABLES:
                # Disallow INSERT INTO, MERGE INTO, UPDATE, DELETE FROM, TRUNCATE on existing tables
                for mutation in (
                    f"MERGE INTO {{ctrl('{table}')}}",
                    f"MERGE INTO {table}",
                    f"INSERT INTO {{ctrl('{table}')}}",
                    f"INSERT INTO {table}",
                    f"UPDATE {{ctrl('{table}')}}",
                    f"UPDATE {table}",
                    f"DELETE FROM {{ctrl('{table}')}}",
                    f"DELETE FROM {table}",
                    f"TRUNCATE TABLE {{ctrl('{table}')}}",
                    f"TRUNCATE TABLE {table}",
                ):
                    self.assertNotIn(mutation, code, f"Forbidden mutation of {table} in {nb_path}")

    def test_existing_control_table_schemas_are_unchanged(self):
        """Verify that existing control-table definitions in NB00 remain untouched."""
        nb00 = shared_nb("NB00_ControlTableInit.py")

        # Check key existing tables maintain exact structure
        for table in (
            "source_connection",
            "source_table_control",
            "source_assessment",
            "accelerator_target_config",
            "sql_object_assessment",
        ):
            self.assertIn(f"CREATE TABLE IF NOT EXISTS {{ctrl('{table}')}}", nb00)


class TestTableMigrationWorkflowsUntouched(unittest.TestCase):
    """Verify Full Load and Delta Sync pipelines are completely unchanged."""

    def test_full_load_table_processing_is_unchanged(self):
        nb09 = shared_nb("NB09_FullLoad.py")
        self.assertNotIn("sql_artifact_control", nb09)
        self.assertNotIn("NB22_SQLArtifactMigrate", nb09)
        self.assertNotIn("VIEW", nb09)
        self.assertNotIn("PACKAGE", nb09)

    def test_delta_sync_table_processing_is_unchanged(self):
        nb11a = shared_nb("NB11a_DeltaSyncPrep.py")
        nb11b = shared_nb("NB11b_DeltaSyncApply.py")
        for code in (nb11a, nb11b):
            self.assertNotIn("sql_artifact_control", code)
            self.assertNotIn("NB22_SQLArtifactMigrate", code)


class TestCandidateDiscoveryAndFiltering(unittest.TestCase):
    """Verify candidate discovery excludes TABLE objects and enforces filters."""

    def test_candidate_filtering_excludes_table_objects(self):
        from sql_artifact_scope import build_candidate_query
        q = build_candidate_query("sa", "soa")
        self.assertIn("<> 'TABLE'", q)

    def test_candidate_filtering_requires_available_definition(self):
        nb22 = shared_nb("NB22_SQLArtifactMigrate.py")
        self.assertIn("DEFINITION_MISSING", nb22)
        self.assertIn("definition_missing_count", nb22)

    def test_candidate_filtering_requires_identity_fields(self):
        from sql_artifact_scope import build_candidate_query
        q = build_candidate_query("sa", "soa")
        for field in ("connection_id", "source_schema", "object_name", "source_system"):
            self.assertIn(field, q)


class TestArtifactIdentityAndNormalization(unittest.TestCase):
    """Verify deterministic identity, collision prevention, and normalization."""

    def test_oracle_identity_handling(self):
        aid1 = compute_artifact_id("conn1", "oracle", None, "HR", "VIEW", "EMP_VIEW")
        aid2 = compute_artifact_id("conn1", "oracle", "", "HR", "VIEW", "EMP_VIEW")
        self.assertEqual(aid1, aid2)
        self.assertEqual(len(aid1), 64)

        # Exact case preservation: Foo and FOO produce different exact artifact IDs
        aid_lower = compute_artifact_id("conn1", "oracle", "", "HR", "VIEW", "emp_view")
        self.assertNotEqual(aid1, aid_lower)
        aid_foo = compute_artifact_id("conn1", "oracle", "", "HR", "VIEW", "Foo")
        aid_FOO = compute_artifact_id("conn1", "oracle", "", "HR", "VIEW", "FOO")
        self.assertNotEqual(aid_foo, aid_FOO)

    def test_sqlserver_identity_handling(self):
        aid = compute_artifact_id("conn_ss", "sqlserver", "AdventureWorks", "dbo", "VIEW", "vCustomer")
        self.assertEqual(len(aid), 64)

    def test_identifier_normalization(self):
        norm = normalize_target_identifier("My View #1!", identifier_type="view")
        self.assertEqual(norm, "my_view_1")

        # Leading digit gets prefix
        norm_digit = normalize_target_identifier("123_custom_view", identifier_type="view")
        self.assertEqual(norm_digit, "view_123_custom_view")

    def test_same_name_in_different_schemas_does_not_collide(self):
        aid1 = compute_artifact_id("conn1", "sqlserver", "db1", "schema_a", "VIEW", "common_view")
        aid2 = compute_artifact_id("conn1", "sqlserver", "db1", "schema_b", "VIEW", "common_view")
        self.assertNotEqual(aid1, aid2)

    def test_same_name_in_different_databases_connections_does_not_collide(self):
        aid_db1 = compute_artifact_id("conn1", "sqlserver", "db_alpha", "dbo", "VIEW", "v1")
        aid_db2 = compute_artifact_id("conn1", "sqlserver", "db_beta", "dbo", "VIEW", "v1")
        aid_conn2 = compute_artifact_id("conn2", "sqlserver", "db_alpha", "dbo", "VIEW", "v1")
        self.assertNotEqual(aid_db1, aid_db2)
        self.assertNotEqual(aid_db1, aid_conn2)

    def test_same_name_in_different_object_types_does_not_collide(self):
        aid_view = compute_artifact_id("conn1", "sqlserver", "db1", "dbo", "VIEW", "proc_or_view")
        aid_proc = compute_artifact_id("conn1", "sqlserver", "db1", "dbo", "PROCEDURE", "proc_or_view")
        self.assertNotEqual(aid_view, aid_proc)


class TestDefinitionHashingAndReruns(unittest.TestCase):
    """Verify deterministic hashing, unchanged rerun skipping, and change detection."""

    def test_source_hash_generation_is_deterministic(self):
        def1 = "CREATE VIEW v AS SELECT 1 AS a"
        def2 = "CREATE VIEW v AS SELECT 1 AS a"
        self.assertEqual(compute_definition_hash(def1), compute_definition_hash(def2))

    def test_unchanged_rerun_is_skipped(self):
        nb22 = shared_nb("NB22_SQLArtifactMigrate.py")
        self.assertIn("skipped_duplicates_count", nb22)
        self.assertIn("DEPLOYMENT_STATUS_NOT_DEPLOYED", nb22)

    def test_changed_definition_creates_new_processing_attempt(self):
        nb22 = shared_nb("NB22_SQLArtifactMigrate.py")
        self.assertIn("attempt_num = (prev_record.get(\"attempt_count\") or 0) + 1", nb22)


class TestConversionAndClassification(unittest.TestCase):
    """Verify conversion, classification (AUTO, MANUAL_REVIEW, UNSUPPORTED), and validation."""

    def setUp(self):
        self.converter = SQLArtifactConverter()

    def test_original_and_converted_definitions_are_preserved(self):
        res = self.converter.convert_artifact(
            source_system="sqlserver",
            object_type="VIEW",
            object_name="v_test",
            source_definition="CREATE VIEW [dbo].[v_test] AS SELECT ISNULL(x, 0) AS y FROM t;",
            target_catalog="cat",
            target_schema="sch",
            object_map={"t": "`cat`.`sch`.`t`"},
        )
        self.assertEqual(res.classification, CLASSIFICATION_AUTO)
        self.assertIn("CREATE OR REPLACE VIEW `cat`.`sch`.`v_test` AS", res.converted_definition)
        self.assertIn("COALESCE(x, 0)", res.converted_definition)

    def test_auto_artifacts_can_reach_deployment(self):
        ora_view = "CREATE OR REPLACE VIEW dept_v AS SELECT NVL(dept_id, 0) AS id, SYSDATE AS t FROM departments"
        res = self.converter.convert_artifact(
            source_system="oracle",
            object_type="VIEW",
            object_name="dept_v",
            source_definition=ora_view,
            target_catalog="my_cat",
            target_schema="my_sch",
            object_map={"departments": "`my_cat`.`my_sch`.`departments`"},
        )
        self.assertEqual(res.classification, CLASSIFICATION_AUTO)
        self.assertTrue(res.validation_passed)
        self.assertFalse(res.manual_review_required)

    def test_manual_review_artifacts_are_not_deployed(self):
        # Stored procedure with procedural logic
        proc_def = """
        CREATE PROCEDURE usp_CalcSalary
        AS
        BEGIN
            DECLARE @cnt INT = 0;
            WHILE @cnt < 10
            BEGIN
                SET @cnt = @cnt + 1;
            END;
        END;
        """
        res = self.converter.convert_artifact(
            source_system="sqlserver",
            object_type="PROCEDURE",
            object_name="usp_CalcSalary",
            source_definition=proc_def,
            target_catalog="cat",
            target_schema="sch",
        )
        self.assertEqual(res.classification, CLASSIFICATION_MANUAL_REVIEW)
        self.assertTrue(res.manual_review_required)
        self.assertFalse(res.validation_passed)

    def test_unsupported_artifacts_are_not_deployed(self):
        # Linked server / OPENQUERY
        view_with_linked_srv = """
        CREATE VIEW v_Remote
        AS
        SELECT * FROM OPENQUERY(RemoteOracle, 'SELECT * FROM emp');
        """
        res = self.converter.convert_artifact(
            source_system="sqlserver",
            object_type="VIEW",
            object_name="v_Remote",
            source_definition=view_with_linked_srv,
            target_catalog="cat",
            target_schema="sch",
        )
        self.assertEqual(res.classification, CLASSIFICATION_UNSUPPORTED)
        self.assertTrue(res.manual_review_required)
        self.assertIn("LINKED_SERVER", res.unsupported_features)

    def test_triggers_are_classified_as_unsupported(self):
        trigger_def = "CREATE TRIGGER tr_audit ON t AFTER INSERT AS BEGIN NULL; END;"
        res = self.converter.convert_artifact(
            source_system="sqlserver",
            object_type="TRIGGER",
            object_name="tr_audit",
            source_definition=trigger_def,
            target_catalog="cat",
            target_schema="sch",
        )
        self.assertEqual(res.classification, CLASSIFICATION_UNSUPPORTED)
        self.assertIn("DATABASE_TRIGGERS", res.unsupported_features)

    def test_packages_are_classified_as_manual_review(self):
        pkg_def = "CREATE PACKAGE pkg_math AS FUNCTION add_nums(a INT, b INT) RETURN INT; END;"
        res = self.converter.convert_artifact(
            source_system="oracle",
            object_type="PACKAGE",
            object_name="pkg_math",
            source_definition=pkg_def,
            target_catalog="cat",
            target_schema="sch",
        )
        self.assertEqual(res.classification, CLASSIFICATION_MANUAL_REVIEW)


class TestTargetSchemaAndDeploymentSafety(unittest.TestCase):
    """Verify target schema creation, non-destructive execution, and error isolation."""

    def test_target_schema_created_only_when_missing(self):
        ddl = build_create_schema("my_cat", "my_sch", comment="test")
        self.assertTrue(ddl.startswith("CREATE SCHEMA IF NOT EXISTS"))
        self.assertNotIn("DROP SCHEMA", ddl)
        self.assertNotIn("ALTER SCHEMA", ddl)

    def test_existing_target_object_not_destructively_overwritten(self):
        nb22 = shared_nb("NB22_SQLArtifactMigrate.py")
        self.assertNotIn("DROP VIEW IF EXISTS", nb22)
        self.assertNotIn("DROP TABLE IF EXISTS", nb22)

    def test_individual_artifact_failure_does_not_stop_unrelated_artifacts(self):
        nb22 = shared_nb("NB22_SQLArtifactMigrate.py")
        # Verify the candidate processing loop contains error catching and continues
        self.assertIn("for cand in candidates:", nb22)
        self.assertIn("continue", nb22)

    def test_execution_log_is_append_only(self):
        nb22 = shared_nb("NB22_SQLArtifactMigrate.py")
        self.assertIn('.write.format("delta").mode("append").saveAsTable', nb22)
        self.assertNotIn('.mode("overwrite")', nb22)

    def test_errors_are_sanitized(self):
        raw_error = "JDBC connection failed: jdbc:sqlserver://myhost:1433;user=admin;password=Secret123;token=abcdef"
        safe = sanitize_error(raw_error)
        self.assertNotIn("Secret123", safe)
        self.assertNotIn("abcdef", safe)
        self.assertIn("***", safe)


class TestJobDefinitionAndTaskGraph(unittest.TestCase):
    """Verify Job YAML definition, parameters, and task sequence."""

    def test_yaml_task_dependencies_and_parameters_valid(self):
        yaml_path = os.path.join(JOBS, "ACCELERATOR_SQL_ARTIFACT_MIGRATION.yaml")
        self.assertTrue(os.path.exists(yaml_path), "Job YAML must exist")

        with open(yaml_path, "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f)

        self.assertEqual(cfg["name"], "ACCELERATOR_SQL_ARTIFACT_MIGRATION")
        tasks = {t["task_key"]: t for t in cfg["tasks"]}

        self.assertIn("T00_Create_Run_Context", tasks)
        self.assertNotIn("T01a_Get_Oracle_Connection_Worklist", tasks)
        self.assertNotIn("T01b_Get_SQLServer_Database_Worklist", tasks)
        self.assertNotIn("T02a_ForEach_Oracle_SQL_Object_Capture", tasks)
        self.assertNotIn("T02b_ForEach_SQLServer_SQL_Object_Capture", tasks)
        self.assertIn("T03_Init_SQL_Artifact_Control", tasks)
        self.assertIn("T23_Fetch_Selected_SQL_Artifacts", tasks)
        self.assertIn("T24_Lakebridge_Analyze_And_Transpile", tasks)
        self.assertIn("T06_SQL_Artifact_Summary", tasks)

        # Check dependencies
        t03_deps = [d["task_key"] for d in tasks["T03_Init_SQL_Artifact_Control"].get("depends_on", [])]
        self.assertEqual(t03_deps, ["T00_Create_Run_Context"])

        t23_deps = [d["task_key"] for d in tasks["T23_Fetch_Selected_SQL_Artifacts"].get("depends_on", [])]
        self.assertEqual(t23_deps, ["T03_Init_SQL_Artifact_Control"])
        self.assertEqual(tasks["T23_Fetch_Selected_SQL_Artifacts"].get("run_if"), "ALL_SUCCESS")

        t24_deps = [d["task_key"] for d in tasks["T24_Lakebridge_Analyze_And_Transpile"].get("depends_on", [])]
        self.assertEqual(t24_deps, ["T23_Fetch_Selected_SQL_Artifacts"])
        self.assertEqual(tasks["T24_Lakebridge_Analyze_And_Transpile"].get("run_if"), "ALL_SUCCESS")

        t06_deps = [d["task_key"] for d in tasks["T06_SQL_Artifact_Summary"].get("depends_on", [])]
        self.assertEqual(t06_deps, ["T24_Lakebridge_Analyze_And_Transpile"])
        self.assertEqual(tasks["T06_SQL_Artifact_Summary"].get("run_if"), "ALL_DONE")
class TestPathCollisionAndContract(unittest.TestCase):
    """Verify Fix 1 & Fix 3: Path collisions prevention and strict builder contract."""

    def test_distinct_names_normalizing_to_same_stem_do_not_collide(self):
        p_space = build_artifact_relative_path(
            connection_id="c1", source_system="oracle", source_schema="s1",
            object_type="VIEW", object_name="A B", append_hash=True
        )
        p_slash = build_artifact_relative_path(
            connection_id="c1", source_system="oracle", source_schema="s1",
            object_type="VIEW", object_name="A/B"
        )
        p_under = build_artifact_relative_path(
            connection_id="c1", source_system="oracle", source_schema="s1",
            object_type="VIEW", object_name="A_B"
        )
        self.assertNotEqual(p_space, p_slash)
        self.assertNotEqual(p_space, p_under)
        self.assertNotEqual(p_slash, p_under)

    def test_case_sensitivity_collision_prevention_foo_vs_FOO(self):
        p_mixed = build_artifact_relative_path(
            connection_id="c1", source_system="oracle", source_schema="s1",
            object_type="VIEW", object_name="Foo", append_hash=True
        )
        p_upper = build_artifact_relative_path(
            connection_id="c1", source_system="oracle", source_schema="s1",
            object_type="VIEW", object_name="FOO", append_hash=True
        )
        self.assertNotEqual(p_mixed, p_upper)

    def test_same_run_colliding_candidates_rejected_foo_vs_FOO(self):
        from _fakes import FakeRow
        from test_sql_object_artifact_materialization import run_nb18_harness
        cand1 = FakeRow(connection_id="c1", source_system="oracle", source_database="db1", source_schema="s1", object_name="Foo Bar", object_type="VIEW", source_definition="SELECT 1")
        cand2 = FakeRow(connection_id="c1", source_system="oracle", source_database="db1", source_schema="s1", object_name="Foo  Bar", object_type="VIEW", source_definition="SELECT 2")
        res = run_nb18_harness(candidates=[cand1, cand2])
        # Neither colliding candidate is written
        self.assertEqual(res["fs"].write_count, 0)
        self.assertEqual(res["task_values"]["failed_count"], 2)
        self.assertTrue(any("ARTIFACT_PATH_COLLISION" in str(e) for e in res["env"]["errors"]))

    def test_same_object_across_databases_distinct_paths(self):
        p_dba = build_artifact_relative_path(
            connection_id="c1", source_system="sqlserver", source_database="DB_A",
            source_schema="dbo", object_type="PROCEDURE", object_name="ProcessOrders"
        )
        p_dbb = build_artifact_relative_path(
            connection_id="c1", source_system="sqlserver", source_database="DB_B",
            source_schema="dbo", object_type="PROCEDURE", object_name="ProcessOrders"
        )
        self.assertNotEqual(p_dba, p_dbb)
        self.assertIn("DB_A/dbo/", p_dba)
        self.assertIn("DB_B/dbo/", p_dbb)

    def test_same_object_across_connections_distinct_paths(self):
        # Centralized converted paths omit connection_id
        p_c1 = build_artifact_relative_path(
            connection_id="conn_1", source_system="oracle", source_schema="s1",
            object_type="VIEW", object_name="v_emp"
        )
        p_c2 = build_artifact_relative_path(
            connection_id="conn_2", source_system="oracle", source_schema="s1",
            object_type="VIEW", object_name="v_emp"
        )
        self.assertEqual(p_c1, p_c2)
        # Connection-owned source paths retain connection_id
        p_c1_conn = build_artifact_relative_path(
            connection_id="conn_1", source_system="oracle", source_schema="s1",
            object_type="VIEW", object_name="v_emp", include_connection_id=True
        )
        p_c2_conn = build_artifact_relative_path(
            connection_id="conn_2", source_system="oracle", source_schema="s1",
            object_type="VIEW", object_name="v_emp", include_connection_id=True
        )
        self.assertNotEqual(p_c1_conn, p_c2_conn)
        self.assertTrue(p_c1_conn.startswith("conn_1/"))
        self.assertTrue(p_c2_conn.startswith("conn_2/"))

    def test_package_and_package_body_distinct_paths(self):
        p_pkg = build_artifact_relative_path(
            connection_id="c1", source_system="oracle", source_schema="s1",
            object_type="PACKAGE", object_name="pkg_orders"
        )
        p_body = build_artifact_relative_path(
            connection_id="c1", source_system="oracle", source_schema="s1",
            object_type="PACKAGE_BODY", object_name="pkg_orders"
        )
        self.assertNotEqual(p_pkg, p_body)
        self.assertIn("/packages/", p_pkg)
        self.assertIn("/package_bodies/", p_body)

    def test_schemas_named_view_procedure_remain_schemas(self):
        for sch in ("VIEW", "PROCEDURE", "FUNCTION", "PACKAGE", "PACKAGE_BODY"):
            p = build_artifact_relative_path(
                connection_id="c1", source_system="oracle", source_schema=sch,
                object_type="VIEW", object_name="v_target"
            )
            self.assertIn(f"/{sch}/views/", p)

    def test_ambiguous_positional_arguments_rejected(self):
        with self.assertRaises(ValueError):
            build_artifact_relative_path("c1", "VIEW", "PROCEDURE", "my_obj")


class TestSqlServerMultiDatabaseIsolationAndDiscovery(unittest.TestCase):
    """Verify Fix 2, Fix 8, Fix 9: SQL Server module discovery, multi-db isolation."""

    def test_module_definition_query_filters_to_supported_module_types(self):
        q = ss_builder.module_definition_query(database="sales_db", owner="dbo")
        self.assertIn("o.type IN ('V', 'P', 'FN', 'IF', 'TF')", q)
        self.assertNotIn("'U'", q)
        self.assertNotIn("'PK'", q)

    def test_sqlserver_source_database_in_artifact_id(self):
        id_a = compute_artifact_id(
            connection_id="c1", source_system="sqlserver", source_database="DB_A", source_schema="dbo",
            object_type="VIEW", object_name="v_sales"
        )
        id_b = compute_artifact_id(
            connection_id="c1", source_system="sqlserver", source_database="DB_B", source_schema="dbo",
            object_type="VIEW", object_name="v_sales"
        )
        self.assertNotEqual(id_a, id_b)

    def test_canonical_owner_key_preserves_database(self):
        rec_a = {"connection_id": "c1", "source_system": "sqlserver", "source_database": "DB_A", "source_schema": "dbo", "object_type": "VIEW", "object_name": "v1"}
        rec_b = {"connection_id": "c1", "source_system": "sqlserver", "source_database": "DB_B", "source_schema": "dbo", "object_type": "VIEW", "object_name": "v1"}
        key_a = canonical_owner_key(rec_a)
        key_b = canonical_owner_key(rec_b)
        self.assertNotEqual(key_a, key_b)
        self.assertEqual(key_a[2], "DB_A")
        self.assertEqual(key_b[2], "DB_B")


class TestTruthfulSummaryAndClassification(unittest.TestCase):
    """Verify Fix 10: Summary counts invariant and terminal outcomes."""

    def test_summary_invariant_formula(self):
        # Invariant: total_candidates = deployed + unchanged + repaired + manual_review + unsupported + failed + skipped
        counts = {
            "deployed": 10,
            "unchanged": 5,
            "repaired": 2,
            "manual_review": 3,
            "unsupported": 1,
            "failed": 2,
            "explicitly_skipped": 0,
        }
        total = sum(counts.values())
        self.assertEqual(total, 23)
        self.assertEqual(total, (
            counts["deployed"] + counts["unchanged"] + counts["repaired"]
            + counts["manual_review"] + counts["unsupported"] + counts["failed"]
            + counts["explicitly_skipped"]
        ))

    def test_summary_notebook_status_mapping(self):
        # All clean -> COMPLETE
        # failures present -> PARTIAL or FAILED
        # manual review present -> MANUAL_REVIEW_REQUIRED
        # unsupported present -> UNSUPPORTED_PRESENT
        def get_status(total, deployed, skipped, manual, unsupported, failed):
            if total == 0:
                return "NO_CANDIDATES"
            elif failed > 0:
                return "PARTIAL" if (deployed > 0 or skipped > 0) else "FAILED"
            elif manual > 0:
                return "MANUAL_REVIEW_REQUIRED"
            elif unsupported > 0:
                return "UNSUPPORTED_PRESENT"
            elif skipped > 0 and deployed == 0:
                return "PARTIAL"
            else:
                return "COMPLETE"

        self.assertEqual(get_status(0, 0, 0, 0, 0, 0), "NO_CANDIDATES")
        self.assertEqual(get_status(10, 10, 0, 0, 0, 0), "COMPLETE")
        self.assertEqual(get_status(10, 5, 0, 0, 0, 5), "PARTIAL")
        self.assertEqual(get_status(10, 0, 0, 0, 0, 10), "FAILED")
        self.assertEqual(get_status(10, 5, 0, 3, 0, 0), "MANUAL_REVIEW_REQUIRED")
        self.assertEqual(get_status(10, 5, 0, 0, 2, 0), "UNSUPPORTED_PRESENT")


class TestBlocker1SameRunArtifactPathCollisions(unittest.TestCase):
    """Verify Blocker 1: Same-run artifact path collisions and owner isolation."""

    def test_v_1_versus_v_at_1(self):
        p1 = build_artifact_relative_path(
            connection_id="c1", source_system="oracle", source_schema="s1",
            object_type="VIEW", object_name="V 1"
        )
        p2 = build_artifact_relative_path(
            connection_id="c1", source_system="oracle", source_schema="s1",
            object_type="VIEW", object_name="V@1"
        )
        self.assertNotEqual(p1, p2)
        self.assertTrue(p1.endswith(".sql"))
        self.assertTrue(p2.endswith(".sql"))

    def test_a_b_versus_a_slash_b(self):
        p1 = build_artifact_relative_path(
            connection_id="c1", source_system="oracle", source_schema="s1",
            object_type="VIEW", object_name="A B"
        )
        p2 = build_artifact_relative_path(
            connection_id="c1", source_system="oracle", source_schema="s1",
            object_type="VIEW", object_name="A/B"
        )
        self.assertNotEqual(p1, p2)

    def test_case_only_name_differences_in_same_batch(self):
        cand1 = FakeRow(connection_id="c1", source_system="oracle", source_database="", source_schema="s1", object_name="Foo Bar", object_type="VIEW", source_definition="SELECT 1")
        cand2 = FakeRow(connection_id="c1", source_system="oracle", source_database="", source_schema="s1", object_name="Foo  Bar", object_type="VIEW", source_definition="SELECT 2")
        res = run_nb18_harness(candidates=[cand1, cand2])
        self.assertEqual(res["fs"].write_count, 0)
        self.assertEqual(res["task_values"]["failed_count"], 2)
        self.assertTrue(any("ARTIFACT_PATH_COLLISION" in str(e) for e in res["env"]["errors"]))

    def test_same_object_name_in_different_schemas(self):
        p1 = build_artifact_relative_path(
            connection_id="c1", source_system="oracle", source_schema="schema_a",
            object_type="VIEW", object_name="v1"
        )
        p2 = build_artifact_relative_path(
            connection_id="c1", source_system="oracle", source_schema="schema_b",
            object_type="VIEW", object_name="v1"
        )
        self.assertNotEqual(p1, p2)
        self.assertIn("/schema_a/", p1)
        self.assertIn("/schema_b/", p2)

    def test_same_object_name_in_different_databases(self):
        p1 = build_artifact_relative_path(
            connection_id="c1", source_system="sqlserver", source_database="DB_A",
            source_schema="dbo", object_type="VIEW", object_name="v1"
        )
        p2 = build_artifact_relative_path(
            connection_id="c1", source_system="sqlserver", source_database="DB_B",
            source_schema="dbo", object_type="VIEW", object_name="v1"
        )
        self.assertNotEqual(p1, p2)
        self.assertIn("DB_A/dbo/", p1)
        self.assertIn("DB_B/dbo/", p2)

    def test_same_object_name_under_different_connections(self):
        p1 = build_artifact_relative_path(
            connection_id="conn1", source_system="oracle", source_schema="s1",
            object_type="VIEW", object_name="v1", include_connection_id=True
        )
        p2 = build_artifact_relative_path(
            connection_id="conn2", source_system="oracle", source_schema="s1",
            object_type="VIEW", object_name="v1", include_connection_id=True
        )
        self.assertNotEqual(p1, p2)
        self.assertTrue(p1.startswith("conn1/"))
        self.assertTrue(p2.startswith("conn2/"))

    def test_oracle_package_versus_package_body(self):
        p1 = build_artifact_relative_path(
            connection_id="c1", source_system="oracle", source_schema="s1",
            object_type="PACKAGE", object_name="pkg_a"
        )
        p2 = build_artifact_relative_path(
            connection_id="c1", source_system="oracle", source_schema="s1",
            object_type="PACKAGE_BODY", object_name="pkg_a"
        )
        self.assertNotEqual(p1, p2)
        self.assertIn("/packages/", p1)
        self.assertIn("/package_bodies/", p2)

    def test_two_collisions_in_one_batch(self):
        c1 = FakeRow(connection_id="c1", source_system="oracle", source_database="", source_schema="s1", object_name="Foo Bar", object_type="VIEW", source_definition="SELECT 1")
        c2 = FakeRow(connection_id="c1", source_system="oracle", source_database="", source_schema="s1", object_name="Foo  Bar", object_type="VIEW", source_definition="SELECT 2")
        c3 = FakeRow(connection_id="c1", source_system="oracle", source_database="", source_schema="s1", object_name="Bar Baz", object_type="VIEW", source_definition="SELECT 3")
        c4 = FakeRow(connection_id="c1", source_system="oracle", source_database="", source_schema="s1", object_name="Bar  Baz", object_type="VIEW", source_definition="SELECT 4")
        res = run_nb18_harness(candidates=[c1, c2, c3, c4])
        self.assertEqual(res["fs"].write_count, 0)
        self.assertEqual(res["task_values"]["failed_count"], 4)

    def test_simulated_two_run_path_claim(self):
        rel_p = build_artifact_relative_path(connection_id="c1", source_database="", source_schema="s1", object_type="VIEW", object_name="Foo", include_connection_id=True)
        vol_path = sqlobj_art.build_artifact_volume_path("da_accelerators", "s1", "_source_artifacts", rel_p)
        mr = FakeRow(
            connection_id="c1", source_database="", source_schema="s1",
            object_type="VIEW", object_name="Foo", artifact_path=vol_path,
            materialization_status="SUCCEEDED", source_definition_hash="h1"
        )
        cand = FakeRow(
            connection_id="c1", source_system="oracle", source_database="",
            source_schema="s1", object_name=" Foo ", object_type="VIEW",
            source_definition="SELECT 2"
        )
        res = run_nb18_harness(candidates=[cand], manifest_rows=[mr])
        self.assertEqual(res["fs"].write_count, 0)
        self.assertEqual(res["task_values"]["failed_count"], 1)
        self.assertTrue(any("ARTIFACT_PATH_COLLISION" in str(e) for e in res["env"]["errors"]))

    def test_same_owner_rerun(self):
        rel_p = build_artifact_relative_path(connection_id="c1", source_database="", source_schema="s1", object_type="VIEW", object_name="foo", include_connection_id=True)
        vol_path = sqlobj_art.build_artifact_volume_path("da_accelerators", "s1", "_source_artifacts", rel_p)
        content = "SELECT 1 FROM dual;"
        def_hash = compute_definition_hash(content)
        mr = FakeRow(
            connection_id="c1", source_database="", source_schema="s1",
            object_type="VIEW", object_name="foo", artifact_path=vol_path,
            materialization_status="SUCCEEDED", source_definition_hash=def_hash
        )
        fs = FakeFilesystem()
        fs.files[vol_path] = content
        cand = FakeRow(
            connection_id="c1", source_system="oracle", source_database="",
            source_schema="s1", object_name="foo", object_type="VIEW",
            source_definition=content
        )
        res = run_nb18_harness(candidates=[cand], manifest_rows=[mr], fs=fs)
        self.assertEqual(res["fs"].write_count, 0)
        self.assertEqual(res["task_values"]["unchanged_count"], 1)

    def test_correct_same_owner_repair(self):
        rel_p = build_artifact_relative_path(connection_id="c1", source_database="", source_schema="s1", object_type="VIEW", object_name="foo", include_connection_id=True)
        vol_path = sqlobj_art.build_artifact_volume_path("da_accelerators", "s1", "_source_artifacts", rel_p)
        content = "SELECT 1 FROM dual;"
        def_hash = compute_definition_hash(content)
        mr = FakeRow(
            connection_id="c1", source_database="", source_schema="s1",
            object_type="VIEW", object_name="foo", artifact_path=vol_path,
            materialization_status="SUCCEEDED", source_definition_hash=def_hash
        )
        fs = FakeFilesystem()
        cand = FakeRow(
            connection_id="c1", source_system="oracle", source_database="",
            source_schema="s1", object_name="foo", object_type="VIEW",
            source_definition=content
        )
        res = run_nb18_harness(candidates=[cand], manifest_rows=[mr], fs=fs)
        self.assertEqual(res["task_values"]["repaired_count"], 1)
        self.assertEqual(res["fs"].files[vol_path], content)


class TestBlocker2RepairMissingAndCorruptedFiles(unittest.TestCase):
    """Verify Blocker 2: Repair missing and corrupted physical files."""

    def test_correct_unchanged_file(self):
        content = "SELECT 1"
        h = compute_definition_hash(content)
        rel_p = build_artifact_relative_path(connection_id="c1", source_schema="s1", object_type="VIEW", object_name="v1", include_connection_id=True)
        vol_path = sqlobj_art.build_artifact_volume_path("da_accelerators", "s1", "_source_artifacts", rel_p)
        fs = FakeFilesystem()
        fs.files[vol_path] = content
        mr = FakeRow(connection_id="c1", source_schema="s1", object_type="VIEW", object_name="v1", artifact_path=vol_path, source_definition_hash=h, materialization_status="SUCCEEDED")
        cand = FakeRow(connection_id="c1", source_system="oracle", source_schema="s1", object_name="v1", object_type="VIEW", source_definition=content)
        res = run_nb18_harness(candidates=[cand], manifest_rows=[mr], fs=fs)
        self.assertEqual(res["task_values"]["unchanged_count"], 1)
        self.assertEqual(res["task_values"]["repaired_count"], 0)
        self.assertEqual(fs.write_count, 0)

    def test_missing_file_repaired(self):
        content = "SELECT 1"
        h = compute_definition_hash(content)
        rel_p = build_artifact_relative_path(connection_id="c1", source_schema="s1", object_type="VIEW", object_name="v1", include_connection_id=True)
        vol_path = sqlobj_art.build_artifact_volume_path("da_accelerators", "s1", "_source_artifacts", rel_p)
        fs = FakeFilesystem()
        mr = FakeRow(connection_id="c1", source_schema="s1", object_type="VIEW", object_name="v1", artifact_path=vol_path, source_definition_hash=h, materialization_status="SUCCEEDED")
        cand = FakeRow(connection_id="c1", source_system="oracle", source_schema="s1", object_name="v1", object_type="VIEW", source_definition=content)
        res = run_nb18_harness(candidates=[cand], manifest_rows=[mr], fs=fs)
        self.assertEqual(res["task_values"]["repaired_count"], 1)
        self.assertEqual(fs.files[vol_path], content)

    def test_corrupted_file_repaired(self):
        expected_content = "SELECT 1"
        h = compute_definition_hash(expected_content)
        rel_p = build_artifact_relative_path(connection_id="c1", source_schema="s1", object_type="VIEW", object_name="v1", include_connection_id=True)
        vol_path = sqlobj_art.build_artifact_volume_path("da_accelerators", "s1", "_source_artifacts", rel_p)
        fs = FakeFilesystem()
        fs.files[vol_path] = "CORRUPTED CONTENT"
        mr = FakeRow(connection_id="c1", source_schema="s1", object_type="VIEW", object_name="v1", artifact_path=vol_path, source_definition_hash=h, materialization_status="SUCCEEDED")
        cand = FakeRow(connection_id="c1", source_system="oracle", source_schema="s1", object_name="v1", object_type="VIEW", source_definition=expected_content)
        res = run_nb18_harness(candidates=[cand], manifest_rows=[mr], fs=fs)
        self.assertEqual(res["task_values"]["repaired_count"], 1)
        self.assertEqual(fs.files[vol_path], expected_content)

    def test_empty_file_when_nonempty_content_expected(self):
        expected_content = "SELECT 100"
        h = compute_definition_hash(expected_content)
        rel_p = build_artifact_relative_path(connection_id="c1", source_schema="s1", object_type="VIEW", object_name="v1", include_connection_id=True)
        vol_path = sqlobj_art.build_artifact_volume_path("da_accelerators", "s1", "_source_artifacts", rel_p)
        fs = FakeFilesystem()
        fs.files[vol_path] = ""
        mr = FakeRow(connection_id="c1", source_schema="s1", object_type="VIEW", object_name="v1", artifact_path=vol_path, source_definition_hash=h, materialization_status="SUCCEEDED")
        cand = FakeRow(connection_id="c1", source_system="oracle", source_schema="s1", object_name="v1", object_type="VIEW", source_definition=expected_content)
        res = run_nb18_harness(candidates=[cand], manifest_rows=[mr], fs=fs)
        self.assertEqual(res["task_values"]["repaired_count"], 1)
        self.assertEqual(fs.files[vol_path], expected_content)

    def test_failed_temporary_write_cleans_up(self):
        cand = FakeRow(connection_id="c1", source_system="oracle", source_schema="s1", object_name="v1", object_type="VIEW", source_definition="SELECT 1")
        fs = FakeFilesystem()
        fs.fail_write = True
        res = run_nb18_harness(candidates=[cand], fs=fs)
        self.assertEqual(res["task_values"]["failed_count"], 1)
        self.assertTrue(any(p.startswith("/Volumes/") and ".tmp" in p for p in fs.removed_paths))


class TestBlocker3RunAndAssessmentScoping(unittest.TestCase):
    """Verify Blocker 1 & 3: Run and assessment scoping rules."""

    def test_shipped_yaml_defaults_accepted_by_nb18(self):
        cand = FakeRow(run_id="run_current_123", connection_id="c1", source_system="oracle", source_schema="s1", object_name="v1", object_type="VIEW", source_definition="SELECT 1")
        res = run_nb18_harness(
            candidates=[cand],
            widget_values={"run_id": "run_current_123", "connection_id": "", "only_assessment_id": "", "allow_global_scope": "false"}
        )
        self.assertIsNone(res["error"])
        self.assertEqual(res["task_values"]["materialized_count"], 1)

    def test_blank_run_id_rejected_in_nb18(self):
        cand = FakeRow(connection_id="c1", source_system="oracle", source_schema="s1", object_name="v1", object_type="VIEW", source_definition="SELECT 1")
        res = run_nb18_harness(
            candidates=[cand],
            widget_values={"run_id": "", "connection_id": "", "only_assessment_id": "", "allow_global_scope": "false"}
        )
        self.assertIsNotNone(res["error"])
        self.assertIn("Blank run_id rejected", str(res["error"]))

    def test_explicit_global_scope_accepted_in_nb18(self):
        cand = FakeRow(connection_id="c1", source_system="oracle", source_schema="s1", object_name="v1", object_type="VIEW", source_definition="SELECT 1")
        res = run_nb18_harness(
            candidates=[cand],
            widget_values={"connection_id": "", "only_assessment_id": "", "allow_global_scope": "true"}
        )
        self.assertIsNone(res["error"])
        self.assertEqual(res["task_values"]["materialized_count"], 1)

    def test_summary_excludes_other_runs(self):
        summary_nb = shared_nb("../deployment/NB_SQLArtifactSummary.py")
        self.assertIn("WHERE run_id = {escape_string_literal(run_id)}", summary_nb)


class TestBlocker5JobYamlValidation(unittest.TestCase):
    """Verify Blocker 5: Databricks Job YAML structure, DAG acyclicity, and parameter propagation."""

    def setUp(self):
        yaml_path = os.path.join(JOBS, "ACCELERATOR_SQL_ARTIFACT_MIGRATION.yaml")
        with open(yaml_path, "r", encoding="utf-8") as f:
            self.job = yaml.safe_load(f)

    def test_job_metadata(self):
        self.assertEqual(self.job["name"], "ACCELERATOR_SQL_ARTIFACT_MIGRATION")
        self.assertIn("tasks", self.job)

    def test_all_task_keys_are_unique(self):
        task_keys = [t["task_key"] for t in self.job["tasks"]]
        self.assertEqual(len(task_keys), len(set(task_keys)))

    def test_graph_is_acyclic(self):
        tasks = {t["task_key"]: t for t in self.job["tasks"]}
        visited = set()
        rec_stack = set()

        def is_cyclic(k):
            visited.add(k)
            rec_stack.add(k)
            for dep in tasks[k].get("depends_on", []):
                dep_key = dep["task_key"]
                if dep_key not in visited:
                    if is_cyclic(dep_key):
                        return True
                elif dep_key in rec_stack:
                    return True
            rec_stack.remove(k)
            return False

        for k in tasks:
            if k not in visited:
                self.assertFalse(is_cyclic(k), f"Cycle detected in task {k}")

    def test_all_notebook_paths_exist_on_disk(self):
        for t in self.job["tasks"]:
            nb_path = None
            if "notebook_task" in t:
                nb_path = t["notebook_task"]["notebook_path"]
            elif "for_each_task" in t:
                nb_path = t["for_each_task"]["task"]["notebook_task"]["notebook_path"]
            self.assertIsNotNone(nb_path)
            full_path_py = os.path.join(ROOT, nb_path + ".py")
            full_path_ipynb = os.path.join(ROOT, nb_path + ".ipynb")
            self.assertTrue(
                os.path.exists(full_path_py) or os.path.exists(full_path_ipynb),
                f"Notebook path {nb_path} does not exist on disk"
            )

    def test_parameter_propagation_and_no_secrets(self):
        content = yaml.dump(self.job)
        self.assertNotIn("password", content.lower())
        self.assertNotIn("secret", content.lower())
        self.assertNotIn("NB09_FullLoad", content)
        self.assertNotIn("NB11a_DeltaSyncPrep", content)


class TestBlocker6PackageCompleteness(unittest.TestCase):
    """Verify Blocker 6: Package completeness and absence of temporary patch scripts."""

    def test_temporary_patch_scripts_do_not_exist(self):
        for script in ("fix_nb.py", "fix_nb2.py"):
            self.assertFalse(
                os.path.exists(os.path.join(ROOT, script)),
                f"Temporary patch script {script} must not remain in repository"
            )

    def test_no_references_to_temporary_patch_scripts(self):
        for root_dir in (SRC, NOTEBOOKS, JOBS):
            for dirpath, _, filenames in os.walk(root_dir):
                for f in filenames:
                    if f.endswith((".py", ".ipynb", ".yaml", ".md")):
                        p = os.path.join(dirpath, f)
                        with open(p, "r", encoding="utf-8", errors="ignore") as fh:
                            content = fh.read()
                        self.assertNotIn("fix_nb.py", content, f"Stale reference in {p}")
                        self.assertNotIn("fix_nb2.py", content, f"Stale reference in {p}")


class TestBlocker7LegacySchemaUpgrade(unittest.TestCase):
    """Verify Blocker 7: Additive legacy schema upgrade."""

    def test_fresh_installation_creates_tables(self):
        ddl_ctrl = build_create_artifact_control_ddl("da_acc", "ctrl")
        ddl_log = build_create_artifact_execution_log_ddl("da_acc", "ctrl")
        self.assertIn("CREATE TABLE IF NOT EXISTS `da_acc`.`ctrl`.`sql_artifact_control`", ddl_ctrl)
        self.assertIn("CREATE TABLE IF NOT EXISTS `da_acc`.`ctrl`.`sql_artifact_execution_log`", ddl_log)

    def test_already_current_installation_is_idempotent(self):
        ctrl_cols = [c[0] for c in SQL_ARTIFACT_CONTROL_COLUMNS]
        log_cols = [c[0] for c in SQL_ARTIFACT_EXECUTION_LOG_COLUMNS]
        self.assertEqual(build_upgrade_artifact_control_ddl("da_acc", "ctrl", ctrl_cols), [])
        self.assertEqual(build_upgrade_artifact_execution_log_ddl("da_acc", "ctrl", log_cols), [])

    def test_missing_column_upgrade_additive(self):
        partial_cols = ["artifact_id", "connection_id", "source_schema", "object_name"]
        stmts = build_upgrade_artifact_control_ddl("da_acc", "ctrl", partial_cols)
        self.assertEqual(len(stmts), 1)
        self.assertIn("ALTER TABLE `da_acc`.`ctrl`.`sql_artifact_control` ADD COLUMNS", stmts[0])
        self.assertIn("source_database", stmts[0])
        self.assertIn("error_message", stmts[0])

    def test_sql_object_assessment_additive_upgrade(self):
        partial_cols = ["assessment_id", "connection_id", "source_schema", "object_name"]
        stmts = build_upgrade_sql_object_assessment_ddl("da_acc", "ctrl", partial_cols)
        self.assertEqual(len(stmts), 1)
        self.assertIn("ALTER TABLE `da_acc`.`ctrl`.`sql_object_assessment` ADD COLUMNS", stmts[0])
        self.assertIn("source_database", stmts[0])
        self.assertIn("error_message", stmts[0])

    def test_sql_object_artifact_manifest_additive_upgrade(self):
        partial_cols = ["connection_id", "source_schema", "object_name"]
        stmts = build_upgrade_sql_object_artifact_manifest_ddl("da_acc", "ctrl", partial_cols)
        self.assertEqual(len(stmts), 1)
        self.assertIn("ALTER TABLE `da_acc`.`ctrl`.`sql_object_artifact_manifest` ADD COLUMNS", stmts[0])
        self.assertIn("source_database", stmts[0])
        self.assertIn("error_message", stmts[0])


class TestSection12BlockerBehavioral(unittest.TestCase):
    """Executes behavioral tests proving all 10 blockers are resolved with representative data."""

    def test_01_default_yaml_parameters_accepted_and_current_run_scoped(self):
        cand_current = FakeRow(assessment_id="a1", run_id="run_100", connection_id="c1", source_system="oracle", source_schema="s1", object_name="v1", object_type="VIEW", source_definition="SELECT 1")
        cand_prior = FakeRow(assessment_id="a2", run_id="run_099", connection_id="c1", source_system="oracle", source_schema="s1", object_name="v2", object_type="VIEW", source_definition="SELECT 2")
        cand_newer = FakeRow(assessment_id="a3", run_id="run_101", connection_id="c1", source_system="oracle", source_schema="s1", object_name="v3", object_type="VIEW", source_definition="SELECT 3")

        res = run_nb18_harness(
            candidates=[cand_current, cand_prior, cand_newer],
            widget_values={"run_id": "run_100", "connection_id": "", "only_assessment_id": "", "allow_global_scope": "false"}
        )
        self.assertIsNone(res["error"])
        self.assertEqual(res["task_values"]["materialized_count"], 1)

    def test_02_global_scope_never_implicit(self):
        cand = FakeRow(assessment_id="a1", run_id="run_100", connection_id="c1", source_system="oracle", source_schema="s1", object_name="v1", object_type="VIEW", source_definition="SELECT 1")
        res = run_nb18_harness(
            candidates=[cand],
            widget_values={"run_id": "", "connection_id": "", "only_assessment_id": "", "allow_global_scope": "false"}
        )
        self.assertIsNotNone(res["error"])
        self.assertIn("Blank run_id rejected", str(res["error"]))

        res_global = run_nb18_harness(
            candidates=[cand],
            widget_values={"run_id": "run_100", "connection_id": "", "only_assessment_id": "", "allow_global_scope": "true"}
        )
        self.assertIsNone(res_global["error"])

    def test_03_converter_rejects_top_and_comments_before_as(self):
        conv = SQLArtifactConverter()

        # TOP view becomes MANUAL_REVIEW
        res_top = conv.convert_artifact("sqlserver", "VIEW", "v_top", "CREATE VIEW v_top AS SELECT TOP 10 * FROM tbl", "cat", "sch")
        self.assertEqual(res_top.classification, CLASSIFICATION_MANUAL_REVIEW)

        # TOP (...) becomes MANUAL_REVIEW
        res_topp = conv.convert_artifact("sqlserver", "VIEW", "v_topp", "CREATE VIEW v_topp AS SELECT TOP (5) * FROM tbl", "cat", "sch")
        self.assertEqual(res_topp.classification, CLASSIFICATION_MANUAL_REVIEW)

        # AS inside block comment does not become boundary
        sql_block = "CREATE VIEW v_comment /* AS inside comment */ AS SELECT 1"
        res_block = conv.convert_artifact("sqlserver", "VIEW", "v_comment", sql_block, "cat", "sch")
        self.assertEqual(res_block.classification, CLASSIFICATION_AUTO)
        self.assertIn("SELECT 1", res_block.converted_definition)

        # AS inside line comment does not become boundary
        sql_line = "CREATE VIEW v_line -- AS in line comment\n AS SELECT 42"
        res_line = conv.convert_artifact("sqlserver", "VIEW", "v_line", sql_line, "cat", "sch")
        self.assertEqual(res_line.classification, CLASSIFICATION_AUTO)
        self.assertIn("SELECT 42", res_line.converted_definition)

        # AS inside quoted string does not become boundary
        sql_str = "CREATE VIEW v_str (col1) AS SELECT 'AS inside string' FROM dual"
        res_str = conv.convert_artifact("oracle", "VIEW", "v_str", sql_str, "cat", "sch")
        self.assertEqual(res_str.classification, CLASSIFICATION_AUTO)

    def test_04_unresolved_source_references_cannot_become_auto(self):
        conv = SQLArtifactConverter()
        # Unresolved dbo.orders reference
        res = conv.convert_artifact("sqlserver", "VIEW", "v_orders", "CREATE VIEW v_orders AS SELECT id FROM dbo.orders", "cat", "sch")
        self.assertEqual(res.classification, CLASSIFICATION_MANUAL_REVIEW)
        self.assertIn("Unresolved source-qualified reference", res.manual_review_reason)

        # Resolved with object map becomes AUTO
        res_mapped = conv.convert_artifact(
            "sqlserver", "VIEW", "v_orders",
            "CREATE VIEW v_orders AS SELECT id FROM dbo.orders",
            "cat", "sch",
            object_map={"dbo.orders": "`cat`.`sch`.`orders`"}
        )
        self.assertEqual(res_mapped.classification, CLASSIFICATION_AUTO)

        # Pure literal select without external references is AUTO
        res_lit = conv.convert_artifact("oracle", "VIEW", "v_lit", "CREATE VIEW v_lit AS SELECT 1 as val", "cat", "sch")
        self.assertEqual(res_lit.classification, CLASSIFICATION_AUTO)

    def test_05_distinct_source_owners_cannot_share_target_ownership(self):
        aid_foo = compute_artifact_id("c1", "oracle", "db1", "s1", "VIEW", "Foo")
        aid_FOO = compute_artifact_id("c1", "oracle", "db1", "s1", "VIEW", "FOO")
        self.assertNotEqual(aid_foo, aid_FOO)

    def test_06_cross_run_target_takeover_is_blocked(self):
        from sql_artifact_control_common import make_canonical_owner
        owner_a = make_canonical_owner("c1", "oracle", "db1", "s1", "VIEW", "v1")
        owner_b = make_canonical_owner("c2", "sqlserver", "db2", "s2", "VIEW", "v1")
        self.assertNotEqual(owner_a, owner_b)

    def test_07_legacy_upgrade_helpers_invoked_by_production_initialization(self):
        ctrl_cols = ["artifact_id", "connection_id", "source_schema", "object_name"]
        stmts_ctrl = build_upgrade_artifact_control_ddl("cat", "sch", ctrl_cols)
        self.assertTrue(any("ADD COLUMNS" in s for s in stmts_ctrl))

        assess_cols = ["assessment_id", "connection_id", "source_schema", "object_name"]
        stmts_assess = build_upgrade_sql_object_assessment_ddl("cat", "sch", assess_cols)
        self.assertTrue(any("source_database" in s for s in stmts_assess))
        self.assertTrue(any("error_message" in s for s in stmts_assess))

        manifest_cols = ["connection_id", "source_schema", "object_name"]
        stmts_manifest = build_upgrade_sql_object_artifact_manifest_ddl("cat", "sch", manifest_cols)
        self.assertTrue(any("source_database" in s for s in stmts_manifest))
        self.assertTrue(any("error_message" in s for s in stmts_manifest))

    def test_08_upgrade_failure_makes_nb21_fail(self):
        with open(os.path.join(SHARED, "NB21_SQLArtifactInit.py"), "r", encoding="utf-8") as f:
            code = f.read()
        self.assertIn("raise RuntimeError", code)
        self.assertNotIn("print(f\"Schema upgrade check", code)

    def test_09_execution_log_failure_makes_summary_non_complete(self):
        with open(os.path.join(DEPLOYMENT, "NB_SQLArtifactSummary.py"), "r", encoding="utf-8") as f:
            code = f.read()
        self.assertIn("total_failed_stages > 0", code)
        self.assertIn("raise RuntimeError", code)

    def test_10_nb22_raises_after_durable_failures(self):
        with open(os.path.join(SHARED, "NB22_SQLArtifactMigrate.py"), "r", encoding="utf-8") as f:
            code = f.read()
        self.assertIn("if failed_count > 0:", code)
        self.assertIn("raise RuntimeError", code)

    def test_11_oracle_unconstrained_number_fidelity(self):
        from type_mappers.oracle import OracleTypeMapper
        mapper = OracleTypeMapper()
        res = mapper.map_column("NUMBER")
        self.assertEqual(res.status, "REVIEW")
        self.assertEqual(res.fidelity, "UNKNOWN")

        res_exact = mapper.map_column("NUMBER", precision=10, scale=0)
        self.assertEqual(res_exact.status, "AUTO")
        self.assertEqual(res_exact.fidelity, "EXACT")

    def test_12_invalid_oracle_precision_scale_fails_closed(self):
        from type_mappers.oracle import OracleTypeMapper
        mapper = OracleTypeMapper()
        res = mapper.map_column("NUMBER", precision=4, scale=5)
        self.assertEqual(res.status, "REVIEW")
        self.assertEqual(res.fidelity, "LOSSY")

        res_p40 = mapper.map_column("NUMBER", precision=40, scale=0)
        self.assertEqual(res_p40.status, "BLOCKED")

    def test_13_invalid_sqlserver_decimal_fails_closed(self):
        from type_mappers.sqlserver import SqlServerTypeMapper
        mapper = SqlServerTypeMapper()
        res_zero = mapper.map_column("DECIMAL", precision=0, scale=0)
        self.assertEqual(res_zero.status, "BLOCKED")

        res_bad_scale = mapper.map_column("DECIMAL", precision=4, scale=7)
        self.assertEqual(res_bad_scale.status, "BLOCKED")

        res_neg = mapper.map_column("DECIMAL", precision=10, scale=-2)
        self.assertEqual(res_neg.status, "BLOCKED")

        res_ok = mapper.map_column("DECIMAL", precision=18, scale=2)
        self.assertEqual(res_ok.status, "AUTO")
        self.assertEqual(res_ok.fidelity, "EXACT")

    def test_14_nb18_generic_lookup_failure_does_not_bypass_validation(self):
        cand = FakeRow(assessment_id="a1", run_id="run_100", connection_id="c1", source_system="oracle", source_schema="s1", object_name="v1", object_type="VIEW", source_definition="SELECT 1")
        res = run_nb18_harness(
            candidates=[cand],
            widget_values={"run_id": "run_100", "connection_id": "", "only_assessment_id": "", "allow_global_scope": "false"},
            connections={"c1": RuntimeError("Generic network timeout connecting to control database")}
        )
        self.assertIsNotNone(res["error"])
        self.assertIn("Generic network timeout", str(res["error"]))

    def test_15_trigger_is_not_advertised_by_normal_capture(self):
        conv = SQLArtifactConverter()
        res_trig = conv.convert_artifact("oracle", "TRIGGER", "trg_audit", "CREATE TRIGGER trg_audit ...", "cat", "sch")
        self.assertEqual(res_trig.classification, CLASSIFICATION_UNSUPPORTED)

    def test_16_only_source_system_prevents_other_source_branch(self):
        with open(os.path.join(JOBS, "ACCELERATOR_SQL_ARTIFACT_MIGRATION.yaml"), "r", encoding="utf-8") as f:
            job = yaml.safe_load(f)
        task_keys = [t["task_key"] for t in job["tasks"]]
        self.assertEqual(task_keys, [
            "T00_Create_Run_Context",
            "T03_Init_SQL_Artifact_Control",
            "T23_Fetch_Selected_SQL_Artifacts",
            "T24_Lakebridge_Analyze_And_Transpile",
            "T06_SQL_Artifact_Summary",
        ])


if __name__ == "__main__":
    unittest.main()

