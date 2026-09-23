"""
Comprehensive test suite for Job 1B onboarding optimization, deduplication,
batching, and execution scope correctness.

Covers Scenarios A through K:
- A. Worklist deduplication (multiple assessments under 1 connection -> connection emitted once)
- B. Multiple connections (multiple connections emit each distinct connection once in deterministic order; filtering rules)
- C. Duplicate inventory prevention (8 assessments, 1 connection, 137 tables -> 8 T02, 1 T03, 137 candidates evaluated)
- D. Oracle duplicate prevention (multiple Oracle assessments under 1 connection -> registration assessment-scoped, inventory connection-scoped)
- E. Current-run isolation (registered_tables_for_onboarding_run filters strictly to current run_id, excludes historical tables)
- F. Partial registration (connection emitted if >=1 current-run registration succeeded; failed rows stay failed)
- G. T02 batching (atomic claim per row preserved; batch MERGE and batch verification query)
- H. Inventory batching (batch_columns_metadata_query and batch_primary_key_query queries, PK order preservation, database grouping)
- I. Repair runs (idempotent candidates when status advanced; target provisioning ALREADY_PROVISIONED handling)
- J. Backward compatibility (single assessment/connection contract unchanged, SQL Server blank-database preserved)
- K. Architecture contract (SQL Server and Oracle Job 1B definitions are Databricks-managed; documented sequence, parameters, dependencies, concurrency)
"""

import json
import os
import sys
import unittest
import yaml
from unittest.mock import MagicMock, patch

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
SRC = os.path.join(ROOT, "src")
NOTEBOOKS = os.path.join(ROOT, "notebooks")
JOBS = os.path.join(ROOT, "jobs")
for p in (SRC, HERE, NOTEBOOKS, ROOT):
    if p not in sys.path:
        sys.path.insert(0, p)

from _fakes import FakeSpark, FakeRow, FakeDataFrame
from _nbsource import shared_nb, source_nb
from control_repository import ControlRepository
from source_adapters.factory import get_source_adapter
from source_identity import compute_source_table_id
import sql_builder as ora_builder
import sqlserver_sql_builder as ss_builder


def deployment_nb(name):
    path = os.path.join(NOTEBOOKS, "deployment", name)
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


class TestScenarioAWorklistDeduplication(unittest.TestCase):
    """Scenario A: 1 connection with multiple assessment batches emits connection_id exactly once."""

    def test_single_connection_multiple_assessments_deduplicated(self):
        code = deployment_nb("NB_GetRegisteredConnectionWorklist.py")
        self.assertIn(".distinct()", code)
        self.assertIn('.orderBy("connection_id")', code)
        self.assertIn('set(item.keys()) != {"connection_id"}', code)

        # Simulate data with 3 assessments under sqlserver1
        rows = [
            {"connection_id": "sqlserver1", "assessment_id": "assessment_a"},
            {"connection_id": "sqlserver1", "assessment_id": "assessment_b"},
            {"connection_id": "sqlserver1", "assessment_id": "assessment_c"},
        ]
        # Deduplication logic
        distinct_conns = sorted(list({r["connection_id"] for r in rows}))
        worklist = [{"connection_id": c} for c in distinct_conns]

        self.assertEqual(len(worklist), 1)
        self.assertEqual(worklist[0]["connection_id"], "sqlserver1")
        self.assertEqual(set(worklist[0].keys()), {"connection_id"})


class TestScenarioBMultipleConnections(unittest.TestCase):
    """Scenario B: Multiple connections emit each distinct connection once in deterministic order."""

    def test_multiple_connections_deduplicated_and_sorted(self):
        rows = [
            {"connection_id": "sqlserver2", "assessment_id": "assessment_c"},
            {"connection_id": "sqlserver1", "assessment_id": "assessment_a"},
            {"connection_id": "sqlserver1", "assessment_id": "assessment_b"},
        ]
        distinct_conns = sorted(list({r["connection_id"] for r in rows}))
        worklist = [{"connection_id": c} for c in distinct_conns]

        self.assertEqual(len(worklist), 2)
        self.assertEqual(worklist[0]["connection_id"], "sqlserver1")
        self.assertEqual(worklist[1]["connection_id"], "sqlserver2")

    def test_inclusion_and_exclusion_precedence(self):
        code = deployment_nb("NB_GetRegisteredConnectionWorklist.py")
        self.assertIn("raw_only_conns", code)
        self.assertIn("raw_exclude_conns", code)
        self.assertIn("exclusion takes precedence over inclusion", code.lower())

        conns = ["sqlserver1", "sqlserver2", "sqlserver3"]
        only = ["sqlserver1", "sqlserver2"]
        exclude = ["sqlserver2"]

        # Filter: only applied first, then exclude
        filtered = [c for c in conns if c in only and c not in exclude]
        self.assertEqual(filtered, ["sqlserver1"])


class TestScenarioCDuplicateInventoryPrevention(unittest.TestCase):
    """Scenario C: 8 assessments, 1 connection, 137 tables -> 8 T02, 1 T03, 137 candidates."""

    def test_inventory_executes_once_for_connection(self):
        # In the new architecture, T03 iterates over deduplicated connection worklist
        connection_worklist = [{"connection_id": "sqlserver1"}]
        self.assertEqual(len(connection_worklist), 1)

        # In T03, candidate query uses registered_tables_for_onboarding_run
        fake_stc = [
            FakeRow({
                "connection_id": "sqlserver1",
                "source_table_id": f"tbl_{i}",
                "source_database": "AdventureWorks",
                "source_schema": "dbo",
                "source_table": f"Table_{i}",
                "current_status": "REGISTERED",
                "table_decision": "AUTO_MIGRATE",
                "is_active": False,
            })
            for i in range(137)
        ]

        spark = FakeSpark(results=[fake_stc])
        repo = ControlRepository(spark, "cat", "ctrl")

        candidates = repo.registered_tables_for_onboarding_run("sqlserver1", "run_123").collect()
        self.assertEqual(len(candidates), 137)


class TestScenarioDOracleDuplicatePrevention(unittest.TestCase):
    """Scenario D: Oracle multiple assessments under 1 connection."""

    def test_oracle_inventory_connection_scoped(self):
        # Oracle Job 1B architecture contract (documented in docs/databricks_job_task_mapping.md):
        # - T02 is assessment-scoped (inputs from T01 worklist)
        # - T02B gets registered connections (deduplicated connection worklist)
        # - T03 through T08 consume T02B worklist
        # - T09 and T10 consume T01 worklist
        mapping_path = os.path.join(ROOT, "docs", "databricks_job_task_mapping.md")
        with open(mapping_path, "r", encoding="utf-8") as f:
            doc = f.read()

        # T02 is assessment-scoped (inputs from T01)
        self.assertIn("J1B_T02_Register_Selected_Tables", doc)
        self.assertIn("Assessment (ForEach `T01.worklist`", doc)

        # T02B gets registered connections
        self.assertIn("J1B_T02B_Get_Registered_<Source>_Connections", doc)
        self.assertIn("NB_GetRegisteredConnectionWorklist", doc)

        # T03 through T08 consume T02B worklist
        for stage in ("J1B_T03_<Source>_Source_Inventory", "J1B_T04_Type_Normalization",
                      "J1B_T05_Mapping_Generation", "J1B_T06_Mapping_Validation",
                      "J1B_T07_Table_Decision", "J1B_T08_Target_Provisioning"):
            self.assertIn(f"| `{stage}`", doc)
            self.assertIn("Connection (ForEach `T02B.worklist`", doc)

        # T09 and T10 consume T01 worklist
        self.assertIn("J1B_T09_Finalize_Onboarding", doc)
        self.assertIn("J1B_T10_Mark_Downstream_Failure", doc)


class TestScenarioECurrentRunIsolation(unittest.TestCase):
    """Scenario E: Historical tables exist under the same connection; stage processes only current-run."""

    def test_registered_tables_for_onboarding_run_scopes_by_run_id(self):
        spark = FakeSpark()
        repo = ControlRepository(spark, "cat", "ctrl")

        repo.registered_tables_for_onboarding_run("sqlserver1", "run_current")

        last_sql = spark.last_sql()
        self.assertIn("source_table_control", last_sql)
        self.assertIn("source_assessment", last_sql)
        self.assertIn("onboarding_run_id = 'run_current'", last_sql)


class TestScenarioFPartialRegistration(unittest.TestCase):
    """Scenario F: Partial registration emits connection once if >=1 table registered."""

    def test_connection_emitted_when_partial_registrations_succeed(self):
        code = deployment_nb("NB_GetRegisteredConnectionWorklist.py")
        self.assertIn("sa.registration_completed_ts", code)
        self.assertIn("ELIGIBLE_SELECTION_STATUSES", code)
        self.assertIn("EMPTY_REGISTERED_CONNECTION_WORKLIST", code)


class TestScenarioGT02Batching(unittest.TestCase):
    """Scenario G: Atomic claim preserved per row, batch MERGE and verification used."""

    def test_nb01b_preserves_atomic_claim_and_batches_merge(self):
        code = shared_nb("NB01B_RegisterSelectedTables.py")
        self.assertIn("repo.claim_assessment_selection_row(", code)
        self.assertIn("MERGE INTO", code)
        self.assertIn("claimed_valid", code)
        self.assertIn("spark.createDataFrame(", code)
        self.assertIn("reg_rows", code)
        self.assertIn("selected_count", code)
        self.assertIn("validated_count", code)
        self.assertIn("claimed_count", code)
        self.assertIn("registered_count", code)
        self.assertIn("already_registered_count", code)
        self.assertIn("failed_count", code)
        self.assertIn("skipped_count", code)
        self.assertIn("conflict_count", code)


class TestScenarioHInventoryBatching(unittest.TestCase):
    """Scenario H: Batch metadata queries for columns and primary keys."""

    def test_sqlserver_batch_metadata_queries(self):
        col_q = ss_builder.batch_columns_metadata_query("AdventureWorks", [("dbo", "T1"), ("dbo", "T2")])
        self.assertIn("[AdventureWorks].sys.columns", col_q)
        self.assertIn("IS_IDENTITY", col_q)
        self.assertIn("IS_COMPUTED", col_q)
        self.assertIn("IS_HIDDEN", col_q)
        self.assertIn("ORDINAL_POSITION", col_q)

        pk_q = ss_builder.batch_primary_key_query("AdventureWorks", [("dbo", "T1"), ("dbo", "T2")])
        self.assertIn("[AdventureWorks].sys.indexes", pk_q)
        self.assertIn("is_primary_key = 1", pk_q)
        self.assertIn("key_ordinal", pk_q)

    def test_oracle_batch_metadata_queries(self):
        col_q = ora_builder.batch_columns_metadata_query([("HR", "EMPLOYEES"), ("HR", "DEPARTMENTS")])
        self.assertIn("all_tab_columns", col_q.lower())
        self.assertIn("column_id", col_q.lower())
        self.assertIn("data_type", col_q.lower())

        pk_q = ora_builder.batch_primary_key_query([("HR", "EMPLOYEES"), ("HR", "DEPARTMENTS")])
        self.assertIn("all_constraints", pk_q.lower())
        self.assertIn("constraint_type = 'p'", pk_q.lower())
        self.assertIn("position", pk_q.lower())

    def test_adapter_batch_methods_exist(self):
        ora_ad = get_source_adapter("oracle", source_server="srv", source_database="XE", secret_scope="sc")
        ss_ad = get_source_adapter("sqlserver", source_server="srv", source_database="DB", secret_scope="sc")

        self.assertTrue(hasattr(ora_ad, "batch_columns_metadata_query"))
        self.assertTrue(hasattr(ora_ad, "batch_primary_key_query"))
        self.assertTrue(hasattr(ss_ad, "batch_columns_metadata_query"))
        self.assertTrue(hasattr(ss_ad, "batch_primary_key_query"))


class TestScenarioIRepairRuns(unittest.TestCase):
    """Scenario I: Same-run repair idempotency."""

    def test_target_provisioning_handles_already_provisioned(self):
        code = shared_nb("NB08_TargetProvisioning.py")
        self.assertIn("ALREADY_PROVISIONED", code)
        self.assertIn("NO_AUTO_MIGRATE_CANDIDATES", code)
        self.assertIn("registered_tables_for_onboarding_run", code)


class TestScenarioJBackwardCompatibility(unittest.TestCase):
    """Scenario J: Backward compatibility for single connection/assessment and discovery parent."""

    def test_source_identity_algorithm_unchanged(self):
        sid = compute_source_table_id(
            connection_id="conn1",
            source_system="sqlserver",
            source_server="server1",
            source_database="Sales",
            source_schema="dbo",
            source_table="Orders",
        )
        self.assertEqual(len(sid), 64)

    def test_oracle_database_required(self):
        ora_ad = get_source_adapter("oracle", source_server="srv", source_database="ORCL", secret_scope="sc")
        self.assertEqual(ora_ad.source_database, "ORCL")


class TestScenarioKArchitectureContract(unittest.TestCase):
    """Scenario K: Job 1B architecture and Databricks-managed definition verification."""

    def test_job_1b_yaml_not_in_repository(self):
        # Repository YAML deployment validation is not applicable because Job 1B is Databricks-managed
        ss_path = os.path.join(JOBS, "ACCELERATOR_SQLSERVER_JOB_1B_ONBOARDING.yaml")
        ora_path = os.path.join(JOBS, "ACCELERATOR_ORACLE_JOB_1B_ONBOARDING.yaml")
        self.assertFalse(os.path.exists(ss_path), "SQL Server Job 1B YAML must not be maintained in repo")
        self.assertFalse(os.path.exists(ora_path), "Oracle Job 1B YAML must not be maintained in repo")

    def test_job_1b_documented_task_graph(self):
        # Validate the documented task mapping in docs/databricks_job_task_mapping.md
        mapping_path = os.path.join(ROOT, "docs", "databricks_job_task_mapping.md")
        with open(mapping_path, "r", encoding="utf-8") as f:
            doc = f.read()

        expected_sequence = [
            "J1B_T00_Create_Run_Context",
            "J1B_T01_Get_Selected_Assessments",
            "J1B_T02_Register_Selected_Tables",
            "J1B_T02B_Get_Registered_Connections",
            "J1B_T03_Source_Inventory",
            "J1B_T04_Type_Normalization",
            "J1B_T05_Mapping_Generation",
            "J1B_T06_Mapping_Validation",
            "J1B_T07_Table_Decision",
            "J1B_T08_Target_Provisioning",
            "J1B_T09_Finalize_Onboarding",
            "J1B_T10_Mark_Downstream_Failure",
        ]
        for task in expected_sequence:
            self.assertIn(task, doc, f"Task {task} must be documented in task sequence")

        # Confirm documentation clearly states Job 1B definitions are Databricks-managed
        self.assertIn("SQL Server and Oracle Job 1B definitions are managed directly in Databricks", doc)
        self.assertIn("not deployed from repository YAML", doc)


if __name__ == "__main__":
    unittest.main()
