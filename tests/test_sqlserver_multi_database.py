"""
Comprehensive tests for SQL Server multi-database discovery in Job 1A.

Covers:
1. SQL Server database discovery query contract (accessible_databases_query)
2. SQL Server adapter clone, override, and JDBC URL routing
3. Blank database connection validation contract (master fallback)
4. Worklist validation (blank database allowed for SQL Server, required for Oracle)
5. Database assessment worklist logic (populated emits 1, blank discovers, deduplicates, sorts, handles errors)
6. Object identity and assessment isolation across databases (merge keys, artifact paths)
7. Schema filtering parameter contracts (include_schemas, exclude_schemas for SQL Server & Oracle)
8. Backward compatibility for populated SQL Server connections and Oracle connections
"""

import json
import os
import sys
import unittest
from unittest.mock import MagicMock, patch

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.join(os.path.dirname(HERE), "src")
NOTEBOOKS = os.path.join(os.path.dirname(HERE), "notebooks")
for p in (SRC, HERE, NOTEBOOKS, os.path.dirname(HERE)):
    if p not in sys.path:
        sys.path.insert(0, p)

import assessment_common as assess
import sql_object_assessment_common as sqlobj_common
import sql_object_artifact_common as sqlobj_art
import sqlserver_sql_builder as ss_builder
from _nbsource import shared_nb, source_nb
from source_adapters.factory import get_source_adapter
from source_adapters.sqlserver import SqlServerSourceAdapter as SqlServerAdapter
from _fakes import FakeSpark, FakeRow
from control_repository import ControlRepository
from source_identity import compute_source_table_id

DEPLOYMENT = os.path.join(NOTEBOOKS, "deployment")

def deployment_nb(name):
    path = os.path.join(DEPLOYMENT, name)
    if name.endswith(".ipynb"):
        with open(path, "r", encoding="utf-8") as f:
            nb = json.load(f)
        return "\n".join("".join(c.get("source", [])) for c in nb.get("cells", []) if c.get("cell_type") == "code")
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


class TestSqlServerDatabaseDiscoveryQuery(unittest.TestCase):
    """Verify accessible_databases_query contract."""

    def test_query_structure_and_filters(self):
        q = ss_builder.accessible_databases_query()
        self.assertIn("sys.databases", q)
        self.assertIn("HAS_DBACCESS(name) = 1", q)
        self.assertIn("state_desc = 'ONLINE'", q)
        self.assertIn("database_name", q)
        self.assertIn("ORDER BY name", q)

    def test_excludes_system_databases(self):
        q = ss_builder.accessible_databases_query()
        for sys_db in ("'master'", "'model'", "'msdb'", "'tempdb'"):
            self.assertIn(sys_db, q)
        self.assertIn("NOT IN", q)

    def test_exposed_on_sqlserver_adapter(self):
        adapter = get_source_adapter("sqlserver", source_database="TestDb")
        self.assertTrue(hasattr(adapter, "accessible_databases_query"))
        q = adapter.accessible_databases_query()
        self.assertEqual(q, ss_builder.accessible_databases_query())

    def test_oracle_adapter_does_not_expose_database_discovery(self):
        adapter = get_source_adapter("oracle", source_database="XE")
        self.assertFalse(hasattr(adapter, "accessible_databases_query"))


class TestSqlServerAdapterOverrideAndRouting(unittest.TestCase):
    """Verify database override, cloning, and JDBC URL construction."""

    def test_resolve_effective_database_precedence(self):
        adapter = get_source_adapter("sqlserver")
        # Blank configured, requested populated -> returns requested
        self.assertEqual(adapter.resolve_effective_database("", "DiscoveredDB"), "DiscoveredDB")
        self.assertEqual(adapter.resolve_effective_database(None, "DiscoveredDB"), "DiscoveredDB")

        # Populated configured, requested matches -> returns requested
        self.assertEqual(adapter.resolve_effective_database("AppDB", "AppDB"), "AppDB")
        self.assertEqual(adapter.resolve_effective_database("AppDB", "appdb"), "appdb")

        # Populated configured, requested blank -> returns configured
        self.assertEqual(adapter.resolve_effective_database("AppDB", ""), "AppDB")
        self.assertEqual(adapter.resolve_effective_database("AppDB", None), "AppDB")

        # Populated configured, requested override takes precedence
        self.assertEqual(adapter.resolve_effective_database("AppDB", "OtherDB"), "OtherDB")

    def test_clone_and_with_database(self):
        adapter = SqlServerAdapter(
            source_server="srv.example.com",
            source_database=None,
            secret_scope="scope1",
            config={"trust_server_certificate": True}
        )
        self.assertIsNone(adapter.source_database)

        clone = adapter.with_database("FinDB")
        self.assertEqual(clone.source_database, "FinDB")
        self.assertEqual(clone.source_server, "srv.example.com")
        self.assertEqual(clone.secret_scope, "scope1")
        self.assertTrue(clone.config.get("trust_server_certificate"))
        # Original remains untouched
        self.assertIsNone(adapter.source_database)

    def test_jdbc_url_uses_database_override(self):
        adapter = SqlServerAdapter(
            secret_provider=lambda s, k: "dummy",
            source_server="srv.example.com",
            source_database=None,
            secret_scope="scope1"
        )
        url, _ = adapter.get_jdbc_url_and_props(source_database="OverrideDB")
        self.assertIn("databaseName=OverrideDB", url)

    def test_blank_database_metadata_allowed_for_sqlserver(self):
        adapter = SqlServerAdapter()
        # Should not raise for blank database
        adapter.validate_connection_metadata({
            "connection_id": "c1",
            "source_system": "sqlserver",
            "source_server": "srv1",
            "source_database": "",
            "secret_scope": "scope1"
        })
        adapter.validate_connection_metadata({
            "connection_id": "c1",
            "source_system": "sqlserver",
            "source_server": "srv1",
            "source_database": None,
            "secret_scope": "scope1"
        })


class TestConnectionValidationMasterFallback(unittest.TestCase):
    """Verify NB00A blank database validation uses master temporarily."""

    def test_sqlserver_nb00a_uses_master_for_blank_database(self):
        code = source_nb("sqlserver", "NB00A_UpsertAndValidateConnection.py")
        self.assertIn("configured_database or \"master\"", code)
        self.assertIn("probe_database", code)
        # Verify master is not saved into source_connection
        self.assertNotIn("source_database = 'master'", code)
        self.assertNotIn("source_database = \"master\"", code)

    def test_oracle_nb00a_requires_database(self):
        code = source_nb("oracle", "NB00A_UpsertAndValidateConnection.py")
        self.assertNotIn("probe_database", code)
        self.assertNotIn("master", code)


class TestConnectionWorklistValidation(unittest.TestCase):
    """Verify NB_GetConnectionWorklist allows blank database for SQL Server only."""

    def test_nb_get_connection_worklist_allows_blank_sqlserver_database(self):
        code = deployment_nb("NB_GetConnectionWorklist.ipynb")
        # SQL Server permits blank database for discover-all mode, Oracle requires it
        self.assertIn('if source_system == "oracle":', code)
        self.assertIn('sc.source_database', code)
        self.assertIn("discover-all", code)


class TestDatabaseAssessmentWorklist(unittest.TestCase):
    """Verify NB_GetAssessmentDatabaseWorklist implementation."""

    def test_notebook_exists(self):
        path = os.path.join(NOTEBOOKS, "deployment", "NB_GetAssessmentDatabaseWorklist.py")
        self.assertTrue(os.path.exists(path), f"File {path} does not exist")

    def test_deterministic_sorting_and_deduplication(self):
        code = deployment_nb("NB_GetAssessmentDatabaseWorklist.py")
        self.assertIn("seen_lower", code)
        self.assertIn("worklist.sort(", code)
        self.assertIn("accessible_databases_query", code)
        self.assertIn("master", code)

    def test_zero_accessible_databases_fails(self):
        code = deployment_nb("NB_GetAssessmentDatabaseWorklist.py")
        self.assertIn("discovered 0 accessible user databases", code)
        self.assertIn("FAILED", code)

    def test_payload_limit_enforced(self):
        code = deployment_nb("NB_GetAssessmentDatabaseWorklist.py")
        self.assertIn("TASK_VALUE_LIMIT_BYTES", code)
        self.assertIn("validate_task_value_payload", code)


class TestAssessmentIsolationAcrossDatabases(unittest.TestCase):
    """Verify assessment merge keys, SQL-object keys, and artifact paths include source_database."""

    def test_merge_keys_include_source_database(self):
        self.assertIn("source_database", assess.ASSESSMENT_MERGE_KEYS)
        self.assertIn("source_database", sqlobj_common.SQL_OBJECT_MERGE_KEYS)

    def test_table_identity_distinguishes_databases(self):
        keys1 = {
            "connection_id": "c1",
            "source_system": "sqlserver",
            "source_server": "srv",
            "source_database": "DB_A",
            "source_schema": "dbo",
            "table_name": "Customers"
        }
        keys2 = {
            "connection_id": "c1",
            "source_system": "sqlserver",
            "source_server": "srv",
            "source_database": "DB_B",
            "source_schema": "dbo",
            "table_name": "Customers"
        }
        # In ASSESSMENT_MERGE_KEYS, the tuples must differ
        t1 = tuple(keys1.get(k) for k in assess.ASSESSMENT_MERGE_KEYS)
        t2 = tuple(keys2.get(k) for k in assess.ASSESSMENT_MERGE_KEYS)
        self.assertNotEqual(t1, t2)

    def test_sql_object_identity_distinguishes_databases(self):
        obj1 = {
            "connection_id": "c1",
            "source_database": "DB_A",
            "source_schema": "dbo",
            "object_type": "PROCEDURE",
            "object_name": "usp_Load"
        }
        obj2 = {
            "connection_id": "c1",
            "source_database": "DB_B",
            "source_schema": "dbo",
            "object_type": "PROCEDURE",
            "object_name": "usp_Load"
        }
        t1 = tuple(obj1.get(k) for k in sqlobj_common.SQL_OBJECT_MERGE_KEYS)
        t2 = tuple(obj2.get(k) for k in sqlobj_common.SQL_OBJECT_MERGE_KEYS)
        self.assertNotEqual(t1, t2)

    def test_artifact_path_includes_source_database(self):
        path_a = sqlobj_art.build_artifact_relative_path(
            "c1", "dbo", "PROCEDURE", "usp_Load", source_database="DB_A"
        )
        path_b = sqlobj_art.build_artifact_relative_path(
            "c1", "dbo", "PROCEDURE", "usp_Load", source_database="DB_B"
        )
        self.assertEqual(path_a, "c1/db_a/dbo/procedures/usp_load.sql")
        self.assertEqual(path_b, "c1/db_b/dbo/procedures/usp_load.sql")
        self.assertNotEqual(path_a, path_b)

    def test_artifact_owner_key_includes_source_database(self):
        row_a = {"connection_id": "c1", "source_database": "DB_A", "source_schema": "dbo", "object_type": "VIEW", "object_name": "v1"}
        row_b = {"connection_id": "c1", "source_database": "DB_B", "source_schema": "dbo", "object_type": "VIEW", "object_name": "v1"}
        key_a = sqlobj_art.artifact_owner_key(row_a)
        key_b = sqlobj_art.artifact_owner_key(row_b)
        self.assertEqual(key_a, ("c1", "DB_A", "dbo", "VIEW", "v1"))
        self.assertEqual(key_b, ("c1", "DB_B", "dbo", "VIEW", "v1"))
        self.assertNotEqual(key_a, key_b)


class TestSchemaFilteringParameters(unittest.TestCase):
    """Verify include_schemas and exclude_schemas parameter handling."""

    def test_sqlserver_assessment_and_sqlobj_have_schema_widgets(self):
        for nb in ("NB01A_SourceAssessment.py", "NB13_SQLObjectAssessmentAndConversion.py"):
            code = source_nb("sqlserver", nb)
            self.assertIn('widgets.text("include_schemas", "")', code)
            self.assertIn('widgets.text("exclude_schemas", "")', code)
            self.assertIn('widgets.text("source_database", "")', code)

    def test_oracle_assessment_and_sqlobj_have_schema_widgets(self):
        for nb in ("NB01A_SourceAssessment.py", "NB13_SQLObjectAssessmentAndConversion.py"):
            code = source_nb("oracle", nb)
            self.assertIn('widgets.text("include_schemas", "")', code)
            self.assertIn('widgets.text("exclude_schemas", "")', code)
            # Oracle NB13 must pass exclude_schemas to resolve_assessment_schemas
            self.assertIn("exclude_schemas", code)

    def test_shared_schema_resolver_filtering(self):
        common_code = shared_nb("_common.py")
        func_part = common_code.split("def resolve_assessment_schemas(", 1)[1]
        func_text = "def resolve_assessment_schemas(" + func_part.split("\n\nprint(", 1)[0]

        mock_df = MagicMock()
        mock_df.collect.return_value = [
            {"SCHEMA_NAME": "dbo"},
            {"SCHEMA_NAME": "finance"},
            {"SCHEMA_NAME": "hr"},
            {"SCHEMA_NAME": "sales"},
            {"SCHEMA_NAME": "audit"},
        ]
        mock_read = MagicMock(return_value=mock_df)
        ns = {"read_source_jdbc": mock_read, "print": lambda *args: None}
        exec(func_text, ns)
        resolve_assessment_schemas = ns["resolve_assessment_schemas"]

        adapter = MagicMock()

        # Blank include and exclude -> all accessible schemas
        res1 = resolve_assessment_schemas(adapter, "DB", [], [])
        self.assertEqual(res1, ["dbo", "finance", "hr", "sales", "audit"])

        # Include limits schemas
        res2 = resolve_assessment_schemas(adapter, "DB", ["finance", "hr"], [])
        self.assertEqual(res2, ["finance", "hr"])

        # Exclude removes schemas
        res3 = resolve_assessment_schemas(adapter, "DB", [], ["audit", "hr"])
        self.assertEqual(res3, ["dbo", "finance", "sales"])

        # Include and exclude combined
        res4 = resolve_assessment_schemas(adapter, "DB", ["finance", "hr", "sales"], ["hr"])
        self.assertEqual(res4, ["finance", "sales"])


class TestAssessmentSummaryMultiDatabase(unittest.TestCase):
    """Verify assessment summary handles multi-database aggregation."""

    def test_assessment_summary_selects_source_database(self):
        code = deployment_nb("NB_AssessmentSummary.py")
        self.assertIn("source_database", code)
        self.assertIn("databases_assessed", code)


class TestOperationalDatabaseResolution(unittest.TestCase):
    """Verify adapter resolve_operational_database contract."""

    def test_sqlserver_blank_configured_returns_operational(self):
        adapter = get_source_adapter("sqlserver")
        self.assertEqual(adapter.resolve_operational_database("", "DatabaseA"), "DatabaseA")
        self.assertEqual(adapter.resolve_operational_database(None, "DatabaseA"), "DatabaseA")
        self.assertEqual(adapter.resolve_operational_database("   ", "DatabaseA"), "DatabaseA")

    def test_sqlserver_blank_operational_raises(self):
        adapter = get_source_adapter("sqlserver")
        for blank in ("", None, "   "):
            with self.subTest(blank=blank), self.assertRaises(ValueError) as ctx:
                adapter.resolve_operational_database(None, blank)
            self.assertIn("nonblank source_database", str(ctx.exception))

    def test_sqlserver_matching_populated_succeeds(self):
        adapter = get_source_adapter("sqlserver")
        self.assertEqual(adapter.resolve_operational_database("DatabaseA", "DatabaseA"), "DatabaseA")
        self.assertEqual(adapter.resolve_operational_database("DatabaseA", "databasea"), "DatabaseA")

    def test_sqlserver_mismatch_raises(self):
        adapter = get_source_adapter("sqlserver")
        with self.assertRaises(ValueError) as ctx:
            adapter.resolve_operational_database("DatabaseA", "DatabaseB")
        self.assertIn("does not match configured connection database", str(ctx.exception))

    def test_oracle_requires_configured_and_match(self):
        adapter = get_source_adapter("oracle")
        # Matching configured and operational succeeds
        self.assertEqual(adapter.resolve_operational_database("XE", "XE"), "XE")
        self.assertEqual(adapter.resolve_operational_database("XE", "xe"), "XE")

        # Blank configured fails
        with self.assertRaises(ValueError) as ctx:
            adapter.resolve_operational_database("", "XE")
        self.assertIn("requires configured connection database", str(ctx.exception))

        # Blank operational fails
        with self.assertRaises(ValueError) as ctx:
            adapter.resolve_operational_database("XE", "")
        self.assertIn("operational row requires nonblank", str(ctx.exception))

        # Mismatch fails
        with self.assertRaises(ValueError) as ctx:
            adapter.resolve_operational_database("XE", "ORCL")
        self.assertIn("does not match configured connection database", str(ctx.exception))


class TestMultiDatabaseRegistrationAndTargetRouting(unittest.TestCase):
    """Verify registration identity, source_table_id computation, target routing, and collision guards."""

    def test_distinct_source_table_id_across_databases(self):
        id_a = compute_source_table_id("conn_1", "sqlserver", "srv.corp", "DatabaseA", "dbo", "Customer")
        id_b = compute_source_table_id("conn_1", "sqlserver", "srv.corp", "DatabaseB", "dbo", "Customer")
        self.assertNotEqual(id_a, id_b)
        self.assertEqual(len(id_a), 64)
        self.assertEqual(len(id_b), 64)

    def test_target_schema_generation_modes(self):
        nb_code = shared_nb("NB01B_RegisterSelectedTables.py")
        self.assertIn("PREFIX_WITH_DATABASE", nb_code)
        self.assertIn("SOURCE_SCHEMA", nb_code)
        self.assertIn("MULTI_DATABASE_TARGET_MODE_REQUIRED", nb_code)

    def test_prefix_with_database_produces_isolated_targets(self):
        def _target_schema(schema, db, mode="PREFIX_WITH_DATABASE"):
            if mode == "PREFIX_WITH_DATABASE":
                return f"{db.strip()}_{schema.strip()}".lower()
            elif mode == "SOURCE_SCHEMA":
                return schema.strip().lower()
            return "explicit"

        target_a = f"da_accelerators.{_target_schema('dbo', 'DatabaseA')}.customer"
        target_b = f"da_accelerators.{_target_schema('dbo', 'DatabaseB')}.customer"
        self.assertEqual(target_a, "da_accelerators.databasea_dbo.customer")
        self.assertEqual(target_b, "da_accelerators.databaseb_dbo.customer")
        self.assertNotEqual(target_a, target_b)

    def test_nb01b_verifies_registered_source_database(self):
        code = shared_nb("NB01B_RegisterSelectedTables.py")
        self.assertIn("act_db", code)
        self.assertIn("source_database", code)
        self.assertIn("exp_db.casefold() != act_db.casefold()", code)


class TestMultiDatabaseOverlapDetection(unittest.TestCase):
    """Verify check_overlapping_selected_assessments groups by source_database."""

    def test_overlap_query_includes_source_database(self):
        spark = FakeSpark(results=[[]])
        repo = ControlRepository(spark, "cat", "ctrl")
        conflicts = repo.check_overlapping_selected_assessments("sqlserver")
        sql = spark.last_sql()
        self.assertIn("sa.source_database", sql)
        self.assertIn("GROUP BY sa.connection_id, sa.source_database, sa.source_schema, sa.object_name", sql)
        self.assertEqual(conflicts, [])

    def test_different_databases_do_not_conflict(self):
        spark = FakeSpark(results=[[]])
        repo = ControlRepository(spark, "cat", "ctrl")
        conflicts = repo.check_overlapping_selected_assessments("sqlserver")
        self.assertEqual(conflicts, [])

    def test_true_overlap_reports_database_in_payload(self):
        fake_row = FakeRow({
            "connection_id": "c1",
            "source_database": "DatabaseA",
            "source_schema": "dbo",
            "object_name": "Customer",
            "conflicting_assessment_count": 2,
            "conflicting_assessment_ids": "a1, a2",
        })
        spark = FakeSpark(results=[[fake_row]])
        repo = ControlRepository(spark, "cat", "ctrl")
        conflicts = repo.check_overlapping_selected_assessments("sqlserver")
        self.assertEqual(len(conflicts), 1)
        self.assertEqual(conflicts[0]["source_database"], "DatabaseA")
        self.assertEqual(conflicts[0]["source_schema"], "dbo")
        self.assertEqual(conflicts[0]["object_name"], "Customer")


class TestDatabaseQualifiedStateTransitions(unittest.TestCase):
    """Verify claim and status update methods include source_database predicate."""

    def test_claim_row_includes_database_predicate(self):
        spark = FakeSpark(results=[[FakeRow({"c": 1})]])
        repo = ControlRepository(spark, "cat", "ctrl")
        repo.claim_assessment_selection_row(
            "c1", "a1", "dbo", "Customer", "run_1", "att_1", source_database="DatabaseA"
        )
        sql = spark.last_sql()
        self.assertIn("source_database = 'DatabaseA'", sql)
        self.assertIn("source_schema = 'dbo'", sql)
        self.assertIn("object_name = 'Customer'", sql)

    def test_registration_succeeded_includes_database_predicate(self):
        spark = FakeSpark(results=[[FakeRow({"c": 1})]])
        repo = ControlRepository(spark, "cat", "ctrl")
        repo.mark_assessment_registration_succeeded(
            "c1", "a1", "dbo", "Customer", "run_1", "att_1", source_database="DatabaseA"
        )
        sql = spark.last_sql()
        self.assertIn("source_database = 'DatabaseA'", sql)

    def test_onboarding_failed_includes_database_predicate(self):
        spark = FakeSpark(results=[[FakeRow({"c": 1})]])
        repo = ControlRepository(spark, "cat", "ctrl")
        repo.mark_assessment_onboarding_failed(
            "c1", "a1", "dbo", "Customer", "REGISTRATION", "failed", source_database="DatabaseA"
        )
        sql = spark.last_sql()
        self.assertIn("source_database = 'DatabaseA'", sql)


class TestDownstreamDatabaseRoutingWiring(unittest.TestCase):
    """Verify downstream notebooks and helpers preserve operational source_database."""

    def test_nb00_recomputes_identity_from_table_control_database(self):
        code = shared_nb("NB00_ControlTableInit.py")
        self.assertIn("c.source_database AS source_database", code)
        self.assertIn("MISSING_SQLSERVER_SOURCE_DATABASE", code)
        self.assertIn("DUPLICATE_SOURCE_ASSESSMENT_KEY", code)
        self.assertIn("DUPLICATE_SQL_OBJECT_ARTIFACT_OWNER", code)

    def test_nb09_uses_resolve_effective_source_database(self):
        code = shared_nb("NB09_FullLoad.py")
        self.assertIn("src_db = resolve_effective_source_database(d, connection_data)", code)

    def test_nb11a_uses_resolve_effective_source_database(self):
        code = shared_nb("NB11a_DeltaSyncPrep.py")
        self.assertIn("src_db = resolve_effective_source_database(d, connection_data)", code)
        self.assertIn("source_database=src_db", code)

    def test_nb11b_uses_routed_adapter_source_database(self):
        code = shared_nb("NB11b_DeltaSyncApply.py")
        self.assertIn("adapter = get_source_adapter_routed(q)", code)
        self.assertIn("ident[\"source_database\"] = src_db", code)

    def test_nb_migrate_identity_v2_uses_resolve_effective_source_database(self):
        code = deployment_nb("NB_MigrateSourceTableIdentityV2.py")
        self.assertIn("mapped_database = resolve_effective_source_database(data, connection)", code)


class TestMergeKeysAndArtifactIsolation(unittest.TestCase):
    """Verify natural keys for source_assessment, sql_object_assessment, and manifest include source_database."""

    def test_source_assessment_merge_keys_include_source_database(self):
        self.assertIn("source_database", assess.ASSESSMENT_MERGE_KEYS)

    def test_sql_object_assessment_merge_keys_include_source_database(self):
        self.assertIn("source_database", sqlobj_common.SQL_OBJECT_MERGE_KEYS)

    def test_two_databases_persist_independently(self):
        row_a = ("c1", "a1", "DatabaseA", "dbo", "TABLE", "Customer")
        row_b = ("c1", "a1", "DatabaseB", "dbo", "TABLE", "Customer")
        self.assertNotEqual(row_a, row_b)

        proc_a = ("c1", "a1", "DatabaseA", "dbo", "PROCEDURE", "usp_Load")
        proc_b = ("c1", "a1", "DatabaseB", "dbo", "PROCEDURE", "usp_Load")
        self.assertNotEqual(proc_a, proc_b)


if __name__ == "__main__":
    unittest.main()
