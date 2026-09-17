"""Static and pure contracts for connection-owned operational orchestration."""

import ast
import json
import os
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
DEPLOYMENT = os.path.join(ROOT, "notebooks", "deployment")
SRC = os.path.join(ROOT, "src")
for path in (SRC, HERE, ROOT):
    if path not in sys.path:
        sys.path.insert(0, path)

import inventory_common as inventory  # noqa: E402
from source_adapters.factory import get_source_adapter  # noqa: E402
from source_identity import compute_source_table_id  # noqa: E402
from _nbsource import (  # noqa: E402
    all_shared_notebooks, shared_nb, source_nb,
)


def deployment_code(name):
    with open(os.path.join(DEPLOYMENT, name), encoding="utf-8") as stream:
        notebook = json.load(stream)
    return "\n".join(
        "\n".join(cell.get("source", []))
        for cell in notebook.get("cells", [])
        if cell.get("cell_type") == "code"
    )


class TestDeploymentNotebookStructure(unittest.TestCase):
    def test_deployment_code_cells_compile_and_have_metadata(self):
        for name in ("NB_CreateRunContext.ipynb",
                     "NB_GetFullLoadWorklist.ipynb",
                     "NB_GetDeltaWorklist.ipynb"):
            with self.subTest(name=name):
                with open(os.path.join(DEPLOYMENT, name), encoding="utf-8") as stream:
                    notebook = json.load(stream)
                for cell in notebook["cells"]:
                    self.assertEqual(cell.get("cell_type"), "code")
                    self.assertIsInstance(cell.get("metadata"), dict)
                    source = "\n".join(cell.get("source", []))
                    ordinary = "\n".join(
                        line for line in source.splitlines()
                        if not line.lstrip().startswith(("%", "# MAGIC")))
                    ast.parse(ordinary)

    def test_run_context_publishes_only_run_id(self):
        code = deployment_code("NB_CreateRunContext.ipynb")
        self.assertIn('taskValues.set(key="run_id"', code)
        self.assertNotIn('widgets.text("connection_id"', code)
        self.assertNotIn('taskValues.set(key="connection_id"', code)
        for forbidden in ("source_connection", "get_source_adapter", "secrets.get",
                          "read_source_jdbc"):
            self.assertNotIn(forbidden, code)

    def test_worklists_emit_only_safe_identifiers(self):
        for name in ("NB_GetFullLoadWorklist.ipynb",
                     "NB_GetDeltaWorklist.ipynb"):
            code = deployment_code(name)
            item = code.split("worklist = [", 1)[1].split("for row in rows", 1)[0]
            for required in ("run_id", "connection_id", "source_table_id"):
                self.assertIn(f'"{required}"', item, name)
            for forbidden in ("source_server", "source_database", "source_schema",
                              "source_table", "source_system", "secret_scope",
                              "password", "jdbc"):
                self.assertNotIn(f'"{forbidden}"', item.lower(), name)

    def test_worklists_are_global_metadata_only(self):
        for name in ("NB_GetFullLoadWorklist.ipynb",
                     "NB_GetDeltaWorklist.ipynb"):
            code = deployment_code(name)
            self.assertIn("only_connection_ids", code)
            self.assertIn("connection_count", code)
            self.assertIn("source_connection", code)
            self.assertIn("connection_status", code)
            self.assertNotIn("CONNECTION_ID", code)
            for forbidden in ("get_source_adapter", "read_source_jdbc",
                              "dbutils.secrets", "probe_connection"):
                self.assertNotIn(forbidden, code, name)

    def test_full_load_worklist_enforces_connection_eligibility(self):
        code = deployment_code("NB_GetFullLoadWorklist.ipynb")
        for required in (
                "source_table_control", "source_connection",
                'F.col("c.connection_id") == F.col("sc.connection_id")',
                'F.col("c.is_active") == F.lit(True)',
                'F.col("sc.is_active") == F.lit(True)',
                'F.upper(F.col("sc.connection_status")) == F.lit("VALID")',
                "source_identity_version", "target_catalog", "target_schema",
                "target_table", "secret_scope"):
            self.assertIn(required, code)
        self.assertIn(
            '(item["run_id"], item["connection_id"], item["source_table_id"])',
            code)

    def test_delta_worklist_joins_queue_control_and_connection_by_owner(self):
        code = deployment_code("NB_GetDeltaWorklist.ipynb")
        for required in (
                "delta_sync_queue", "source_table_control", "source_connection",
                'F.col("q.connection_id") == F.col("c.connection_id")',
                'F.col("q.source_table_id") == F.col("c.source_table_id")',
                'F.col("c.connection_id") == F.col("sc.connection_id")',
                'F.upper(F.col("q.status")) == F.lit("QUEUED")',
                'F.upper(F.col("sc.connection_status")) == F.lit("VALID")'):
            self.assertIn(required, code)
        self.assertIn(
            '(item["run_id"], item["connection_id"], item["source_table_id"])',
            code)

    def test_empty_worklist_is_successful(self):
        for name in ("NB_GetFullLoadWorklist.ipynb",
                     "NB_GetDeltaWorklist.ipynb"):
            code = deployment_code(name)
            self.assertIn('"status": "SUCCEEDED"', code)
            self.assertNotIn("No eligible", code)


class TestIdentityMigrationNotebook(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        path = os.path.join(DEPLOYMENT, "NB_MigrateSourceTableIdentityV2.py")
        with open(path, encoding="utf-8") as stream:
            cls.code = stream.read()

    def test_dry_run_defaults_true_and_precedes_mutation(self):
        self.assertIn('widgets.dropdown("dry_run", "true"', self.code)
        dry_run_guard = self.code.index("if not dry_run:")
        create_mapping = self.code.index("CREATE TABLE IF NOT EXISTS")
        first_update = self.code.index("UPDATE {ctrl(table_name)}")
        self.assertLess(dry_run_guard, create_mapping)
        self.assertLess(dry_run_guard, first_update)

    def test_child_updates_use_connection_and_old_id(self):
        update = self.code.split("UPDATE {ctrl(table_name)}", 1)[1]
        update = update.split('"""', 1)[0]
        self.assertIn("connection_id =", update)
        self.assertIn("source_table_id =", update)

    def test_control_owner_updates_last_and_records_lineage(self):
        child_update = self.code.index("UPDATE {ctrl(table_name)}")
        control_update = self.code.index("UPDATE {ctrl('source_table_control')}")
        self.assertLess(child_update, control_update)
        self.assertIn("legacy_source_table_id", self.code)
        self.assertIn("source_identity_version", self.code)

    def test_partial_rerun_accepts_already_moved_candidate_children(self):
        self.assertIn("_identity_v2_candidates", self.code)
        self.assertIn(
            "child.source_table_id = candidate.source_table_id", self.code)

    def test_batched_execution_reports_partial_until_all_ready_rows_move(self):
        self.assertIn("remaining_ready = rows_ready - rows_migrated", self.code)
        self.assertIn('"PARTIAL" if not dry_run and remaining_ready > 0',
                      self.code)

    def test_migration_records_do_not_contain_secrets(self):
        mapping = self.code.split("mappings.append({", 1)[1].split("})", 1)[0]
        for forbidden in ("secret_scope", "password", "token", "jdbc_url"):
            self.assertNotIn(forbidden, mapping.lower())

    def test_all_known_child_tables_are_covered(self):
        for table_name in (
                "source_inventory", "normalized_source_inventory",
                "resolved_column_mappings", "mapping_validation_results",
                "table_load_decisions", "review_queue", "table_run_log",
                "delta_sync_queue", "reconciliation_results", "dq_rule",
                "dq_result", "dq_quarantine"):
            self.assertIn(f'"{table_name}"', self.code)


class TestConnectionOwnedSchemas(unittest.TestCase):
    def test_base_owned_tables_declare_connection_id(self):
        ddl = shared_nb("NB00_ControlTableInit.py")
        for table_name in (
                "source_table_control", "source_inventory",
                "normalized_source_inventory", "resolved_column_mappings",
                "mapping_validation_results", "table_load_decisions",
                "review_queue", "table_run_log", "delta_sync_queue",
                "reconciliation_results", "dq_rule", "dq_result",
                "dq_quarantine"):
            block = ddl.split(
                f"CREATE TABLE IF NOT EXISTS {{ctrl('{table_name}')}} (", 1)[1]
            block = block.split(") USING DELTA", 1)[0]
            self.assertIn("connection_id", block, table_name)

    def test_source_table_control_declares_v2_lineage(self):
        ddl = shared_nb("NB00_ControlTableInit.py")
        block = ddl.split(
            "CREATE TABLE IF NOT EXISTS {ctrl('source_table_control')} (", 1)[1]
        block = block.split(") USING DELTA", 1)[0]
        self.assertIn("source_identity_version", block)
        self.assertIn("legacy_source_table_id", block)

    def test_control_init_validates_connection_owned_logical_keys(self):
        ddl = shared_nb("NB00_ControlTableInit.py")
        for code in (
                "DUPLICATE_CONNECTION_ID", "DUPLICATE_TABLE_OWNERSHIP",
                "DUPLICATE_SOURCE_ASSESSMENT_KEY",
                "DUPLICATE_SOURCE_INVENTORY_KEY",
                "DUPLICATE_NORMALIZED_INVENTORY_KEY",
                "DUPLICATE_RESOLVED_MAPPING_KEY",
                "DUPLICATE_MAPPING_VALIDATION_KEY",
                "DUPLICATE_TABLE_DECISION_KEY",
                "DUPLICATE_REVIEW_QUEUE_KEY", "DUPLICATE_TABLE_RUN_KEY",
                "DUPLICATE_DELTA_QUEUE_KEY",
                "DUPLICATE_RECONCILIATION_KEY",
                "DUPLICATE_DQ_RULE_KEY", "DUPLICATE_DQ_RESULT_KEY"):
            self.assertIn(code, ddl)

    def test_operational_notebooks_do_not_use_single_key_repository_methods(self):
        for name in all_shared_notebooks():
            if name in ("_common.py", "NB00_ControlTableInit.py"):
                continue
            code = shared_nb(name)
            self.assertNotIn("repo.get_control_row(", code, name)
            self.assertNotIn("repo.update_control(", code, name)


class TestConnectionOwnedInventory(unittest.TestCase):
    @staticmethod
    def _row(connection_id):
        source_table_id = compute_source_table_id(
            connection_id, "oracle", "finance-host", "FINPDB",
            "FINANCE", "INVOICE")
        values = {
            "run_id": "run-1", "connection_id": connection_id,
            "source_table_id": source_table_id, "source_system": "oracle",
            "source_server": "finance-host", "source_database": "FINPDB",
            "source_schema": "FINANCE", "source_table": "INVOICE",
            "column_name": "INVOICE_ID", "ordinal_position": 1,
            "is_nullable": "NO", "data_type": "NUMBER",
            "character_maximum_length": None, "numeric_precision": 10,
            "numeric_scale": 0, "datetime_precision": None,
            "is_identity": False, "is_computed": False,
            "is_hidden": False, "is_rowversion": False,
            "source_type_schema": None,
        }
        return tuple(values[field] for field in inventory.INVENTORY_FIELDS)

    def test_same_physical_table_has_separate_inventory_keys(self):
        read_row = self._row("ORA_FIN_READ")
        migration_row = self._row("ORA_FIN_MIGRATION")
        read_record = inventory.inventory_record_dict(read_row)
        migration_record = inventory.inventory_record_dict(migration_row)
        self.assertNotEqual(read_record["source_table_id"],
                            migration_record["source_table_id"])
        self.assertNotEqual(
            tuple(read_record[field] for field in inventory.INVENTORY_MERGE_KEYS),
            tuple(migration_record[field]
                  for field in inventory.INVENTORY_MERGE_KEYS))

    def test_one_persistence_batch_cannot_mix_connections(self):
        with self.assertRaisesRegex(ValueError, "exactly one run_id, connection_id"):
            inventory.validate_inventory_batch([
                self._row("ORA_FIN_READ"),
                self._row("ORA_FIN_MIGRATION"),
            ])


class TestMixedSourceOwnership(unittest.TestCase):
    def test_connections_resolve_independent_adapters_and_scopes(self):
        read_adapter = get_source_adapter(
            "oracle", source_server="finance-host", source_database="FINPDB",
            secret_scope="ora-fin-read")
        migration_adapter = get_source_adapter(
            "oracle", source_server="finance-host", source_database="FINPDB",
            secret_scope="ora-fin-migration")
        sales_adapter = get_source_adapter(
            "sqlserver", source_server="sales-host", source_database="Sales",
            secret_scope="mssql-sales")
        self.assertEqual(read_adapter.source_system, "oracle")
        self.assertEqual(migration_adapter.source_system, "oracle")
        self.assertEqual(sales_adapter.source_system, "sqlserver")
        self.assertEqual(
            {read_adapter.secret_scope, migration_adapter.secret_scope},
            {"ora-fin-read", "ora-fin-migration"})
        self.assertEqual(sales_adapter.secret_scope, "mssql-sales")

    def test_registration_and_provisioning_guard_target_collisions(self):
        registration = shared_nb("NB01B_RegisterSelectedTables.py")
        provisioning = shared_nb("NB08_TargetProvisioning.py")
        self.assertIn("owners - {(connection_id, c[\"source_table_id\"])}",
                      registration)
        self.assertIn("all_active_auto", provisioning)
        self.assertIn('(r["connection_id"], r["source_table_id"])',
                      provisioning)
        self.assertIn("target FQN collision", provisioning)

    def test_registration_scopes_assessment_ownership_by_connection(self):
        registration = shared_nb("NB01B_RegisterSelectedTables.py")
        owners = registration.split("assessment_owners =", 1)[1]
        owners = owners.split('""").collect()', 1)[0]
        self.assertIn("assessment_id =", owners)
        self.assertIn("AND connection_id =", owners)

    def test_assessments_publish_connection_owned_assessment_id(self):
        for source in ("oracle", "sqlserver"):
            code = source_nb(source, "NB01A_SourceAssessment.py")
            self.assertIn('set_task_value("assessment_id", assessment_id)',
                          code, source)
            self.assertIn("require_valid_connection(connection_id, SOURCE_SYSTEM)",
                          code, source)

    def test_etl_retains_connection_lineage_without_source_access(self):
        etl = shared_nb("NB15_BronzeToSilverETL.py")
        self.assertIn("repo.get_source_table(connection_id, src_id)", etl)
        self.assertIn('F.lit(connection_id).alias("connection_id")', etl)
        for forbidden in ("get_source_adapter", "read_source_jdbc",
                          "dbutils.secrets"):
            self.assertNotIn(forbidden, etl)


if __name__ == "__main__":
    unittest.main()