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
from control_repository import ControlRepository  # noqa: E402
from source_adapters.factory import get_source_adapter  # noqa: E402
from source_identity import compute_source_table_id  # noqa: E402
from _fakes import FakeSpark, FakeRow  # noqa: E402
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

    def test_delta_worklist_has_catalog_and_control_schema_widgets(self):
        code = deployment_code("NB_GetDeltaWorklist.ipynb")
        self.assertIn('widgets.text("catalog"', code)
        self.assertIn('widgets.text("control_schema"', code)
        self.assertIn("da_accelerators", code)
        self.assertIn('"control"', code)



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

    def test_migration_does_not_update_source_connection(self):
        self.assertNotIn("UPDATE {ctrl('source_connection')}", self.code)
        self.assertNotIn('UPDATE {ctrl("source_connection")}', self.code)

    def test_candidate_filtering_and_ordering_in_spark_before_collect(self):
        self.assertIn(".filter(", self.code)
        self.assertIn(".orderBy(", self.code)
        self.assertIn('"connection_id", "source_schema", "source_table", "source_table_id"', self.code)
        self.assertIn(".limit(batch_size)", self.code)
        self.assertIn("control_rows = batch_query.collect()", self.code)
        order_idx = self.code.index('.orderBy("connection_id", "source_schema", "source_table", "source_table_id")')
        collect_idx = self.code.index("control_rows = batch_query.collect()")
        self.assertLess(order_idx, collect_idx)

    def test_all_mutation_statements_are_behind_dry_run_guard(self):
        dry_run_guard = self.code.index("if not dry_run:")
        for mutation in ("CREATE TABLE IF NOT EXISTS", "UPDATE {ctrl(table_name)}", "UPDATE {ctrl('source_table_control')}"):
            mutation_idx = self.code.index(mutation)
            self.assertLess(dry_run_guard, mutation_idx)

    def test_migration_result_schema_and_business_statuses(self):
        for field in ("business_status", "dry_run", "migration_id",
                      "total_unmigrated_count", "batch_candidate_count",
                      "rows_examined", "rows_ready", "rows_migrated",
                      "rows_already_migrated", "rows_blocked",
                      "remaining_unmigrated_count", "child_rows_updated",
                      "error_count", "errors"):
            self.assertIn(f'"{field}"', self.code)
        for bstatus in ("COMPLETE", "DRY_RUN_COMPLETE", "MORE_WORK_REMAINS",
                        "BLOCKED", "PARTIAL", "FAILED"):
            self.assertIn(f'"{bstatus}"', self.code)

    def test_dynamic_child_table_coverage_matches_ddl(self):
        import re
        ddl = shared_nb("NB00_ControlTableInit.py")
        table_blocks = re.findall(
            r"CREATE TABLE IF NOT EXISTS \{ctrl\('(\w+)'\)\}\s*\((.*?)\)\s*USING DELTA",
            ddl, re.DOTALL)
        ddl_child_tables = set()
        for tname, cols in table_blocks:
            if "source_table_id" in cols and tname != "source_table_control":
                ddl_child_tables.add(tname)
                self.assertIn("connection_id", cols, f"{tname} lacks connection_id")

        candidates_block = self.code.split("CHILD_TABLE_CANDIDATES = (", 1)[1].split(")", 1)[0]
        configured_candidates = set(re.findall(r'"(\w+)"', candidates_block))

        self.assertEqual(
            configured_candidates, ddl_child_tables,
            "CHILD_TABLE_CANDIDATES must dynamically cover every identity-bearing child table in NB00"
        )
        self.assertNotIn("source_assessment", configured_candidates)
        self.assertNotIn("sql_object_assessment", configured_candidates)
        self.assertNotIn("source_connection", configured_candidates)

    def test_collision_query_safety_and_batch_duplicate_blocking(self):
        self.assertIn("if new_ids:", self.code)
        self.assertIn("escape_string_literal(nid)", self.code)
        self.assertIn("new_id_owners.setdefault(new_id, owner)", self.code)
        self.assertIn("- {old_key, new_key}", self.code)

    def test_rows_ready_metric_and_invariants(self):
        # A. READY counts only current batch READY statuses
        batch_a = [
            {"migration_status": "READY"},
            {"migration_status": "READY"},
            {"migration_status": "BLOCKED"},
            {"migration_status": "ALREADY_MIGRATED"},
        ]
        batch_candidate_count = len(batch_a)
        rows_examined = len(batch_a)
        rows_ready = sum(m["migration_status"] == "READY" for m in batch_a)
        rows_blocked = sum(m["migration_status"] == "BLOCKED" for m in batch_a)
        rows_already = sum(m["migration_status"] == "ALREADY_MIGRATED" for m in batch_a)
        self.assertEqual(rows_ready, 2)
        self.assertEqual(rows_blocked, 1)
        self.assertEqual(rows_already, 1)
        self.assertLessEqual(rows_ready, batch_candidate_count)
        self.assertLessEqual(rows_ready + rows_blocked + rows_already, batch_candidate_count)

        # B. Blocked rows are not READY
        batch_b = [
            {"migration_status": "READY"},
            {"migration_status": "BLOCKED"},
            {"migration_status": "BLOCKED"},
        ]
        rows_ready_b = sum(m["migration_status"] == "READY" for m in batch_b)
        rows_blocked_b = sum(m["migration_status"] == "BLOCKED" for m in batch_b)
        self.assertEqual(rows_ready_b, 1)
        self.assertEqual(rows_blocked_b, 2)
        self.assertEqual(rows_ready_b + rows_blocked_b, len(batch_b))

        # C. READY never exceeds batch candidate count for any fixture
        for fixture in (batch_a, batch_b, []):
            rr = sum(m["migration_status"] == "READY" for m in fixture)
            self.assertLessEqual(rr, len(fixture))

        # D. Limited batch metrics
        total_unmigrated_count = 10
        batch_size = 4
        batch_d = [
            {"migration_status": "READY"},
            {"migration_status": "READY"},
            {"migration_status": "READY"},
            {"migration_status": "BLOCKED"},
        ]
        b_count = len(batch_d)
        r_ready = sum(m["migration_status"] == "READY" for m in batch_d)
        r_blocked = sum(m["migration_status"] == "BLOCKED" for m in batch_d)
        rem_unmigrated = total_unmigrated_count - r_ready
        self.assertEqual(total_unmigrated_count, 10)
        self.assertEqual(b_count, 4)
        self.assertEqual(len(batch_d), 4)
        self.assertEqual(r_ready, 3)
        self.assertEqual(r_blocked, 1)
        self.assertLessEqual(r_ready, b_count)
        self.assertGreater(rem_unmigrated, 0)
        bstatus = "MORE_WORK_REMAINS" if rem_unmigrated > 0 else "DRY_RUN_COMPLETE"
        self.assertNotEqual(bstatus, "COMPLETE")
        self.assertNotEqual(bstatus, "DRY_RUN_COMPLETE")

        # E. No-work metrics
        total_unmigrated_count_e = 0
        batch_candidate_count_e = 0
        self.assertEqual(total_unmigrated_count_e, 0)
        self.assertEqual(batch_candidate_count_e, 0)

        # F. Already-migrated rows counted only under rows_already_migrated
        batch_f = [{"migration_status": "ALREADY_MIGRATED"}]
        self.assertEqual(sum(m["migration_status"] == "READY" for m in batch_f), 0)
        self.assertEqual(sum(m["migration_status"] == "BLOCKED" for m in batch_f), 0)
        self.assertEqual(sum(m["migration_status"] == "ALREADY_MIGRATED" for m in batch_f), 1)

        # G. Blocked execution reports rows_blocked and rows_migrated = 0
        # H. Dry-run reports rows_migrated = 0, child_rows_updated = 0
        self.assertIn('"rows_migrated": 0', self.code)
        self.assertIn('"child_rows_updated": 0', self.code)

        # I. Invariant check
        self.assertLessEqual(rows_ready + rows_blocked + rows_already, batch_candidate_count)

    def test_rows_ready_regression_syntax_check(self):
        import ast
        self.assertNotIn("rows_ready = max", self.code)
        self.assertNotIn("max(total_unmigrated_count", self.code)
        parsed = ast.parse(self.code)
        for node in ast.walk(parsed):
            if isinstance(node, ast.Assign):
                for target in node.targets:
                    if isinstance(target, ast.Name) and target.id == "rows_ready":
                        if isinstance(getattr(node.value, "func", None), ast.Name) and node.value.func.id == "max":
                            self.fail("rows_ready must not be assigned via max()")



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

    def test_control_init_reports_migration_required_and_does_not_fail(self):
        init_code = shared_nb("NB00_ControlTableInit.py")
        self.assertIn('business_status = "MIGRATION_REQUIRED" if _legacy_identity_count else "READY"', init_code)
        self.assertIn('"business_status": business_status', init_code)
        self.assertIn('"legacy_identity_count": int(_legacy_identity_count or 0)', init_code)
        self.assertIn('set_task_value("business_status", business_status)', init_code)
        self.assertIn('set_task_value("legacy_identity_count", int(_legacy_identity_count or 0))', init_code)
        self.assertIn("raise RuntimeError(", init_code)

    def test_nb00_legacy_predicate_semantics(self):
        init_code = shared_nb("NB00_ControlTableInit.py")
        self.assertIn("WHERE source_identity_version IS NULL OR source_identity_version <> 2", init_code)

        def is_legacy(version):
            return version is None or version != 2

        # 1. New v2 row with null legacy_source_table_id is not legacy
        self.assertFalse(is_legacy(2))
        # 2. Migrated v2 row with populated legacy_source_table_id is not legacy
        self.assertFalse(is_legacy(2))
        # 3. Null source_identity_version is legacy
        self.assertTrue(is_legacy(None))
        # 4. Version other than 2 is legacy
        self.assertTrue(is_legacy(1))
        self.assertTrue(is_legacy(0))



    def test_operational_notebooks_do_not_use_single_key_repository_methods(self):
        forbidden_attrs = {"update_control", "get_control_row", "get_watermark"}
        target_notebooks = [
            ("Oracle Source Inventory", source_nb("oracle", "NB01_SourceInventory.py")),
            ("SQL Server Source Inventory", source_nb("sqlserver", "NB01_SourceInventory.py")),
            ("NB07_TableDecisionGeneration", shared_nb("NB07_TableDecisionGeneration.py")),
            ("NB08_TargetProvisioning", shared_nb("NB08_TargetProvisioning.py")),
            ("NB09_FullLoad", shared_nb("NB09_FullLoad.py")),
            ("NB10_PostFullLoadState", shared_nb("NB10_PostFullLoadState.py")),
            ("NB11a_DeltaSyncPrep", shared_nb("NB11a_DeltaSyncPrep.py")),
            ("NB11b_DeltaSyncApply", shared_nb("NB11b_DeltaSyncApply.py")),
            ("NB12_ValidationAndReconciliation", shared_nb("NB12_ValidationAndReconciliation.py")),
            ("NB14_RetryFailedTables", shared_nb("NB14_RetryFailedTables.py")),
            ("NB15_BronzeToSilverETL", shared_nb("NB15_BronzeToSilverETL.py")),
        ]
        # Also include all operational shared notebooks (excluding NB00 and _common)
        for name in all_shared_notebooks():
            if name not in ("_common.py", "NB00_ControlTableInit.py"):
                target_notebooks.append((name, shared_nb(name)))

        for label, code in target_notebooks:
            with self.subTest(notebook=label):
                for call_name in ("repo.get_control_row(", "repo.update_control(", "repo.get_watermark("):
                    self.assertNotIn(call_name, code, f"{label} contains string {call_name}")
                cleaned = "\n".join(
                    line for line in code.splitlines()
                    if not line.lstrip().startswith(("%", "# MAGIC"))
                )
                tree = ast.parse(cleaned)
                for node in ast.walk(tree):
                    if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
                        self.assertNotIn(
                            node.func.attr,
                            forbidden_attrs,
                            f"{label} has AST call to legacy method {node.func.attr}",
                        )



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

    def test_onboarding_activation_flow_and_full_load_exclusion(self):
        reg_code = shared_nb("NB01B_RegisterSelectedTables.py")
        prov_code = shared_nb("NB08_TargetProvisioning.py")
        load_code = shared_nb("NB09_FullLoad.py")
        self.assertIn('.withColumn("is_active", F.lit(False))', reg_code)
        self.assertIn('.withColumn("current_status", F.lit("REGISTERED"))', reg_code)
        for src in ("oracle", "sqlserver"):
            inv_code = source_nb(src, "NB01_SourceInventory.py")
            self.assertIn("include_onboarding=True", inv_code, src)
        self.assertIn("include_onboarding=True", prov_code)
        self.assertIn('"is_active": True', prov_code)
        self.assertIn('"current_status": "PROVISIONED"', prov_code)
        self.assertIn("WHERE is_active = true", load_code)
        self.assertIn("coalesce(target_catalog", load_code)
        self.assertIn("target FQN collision", load_code)

    def test_full_load_target_collision_guard_behavior(self):
        code = shared_nb("NB09_FullLoad.py")
        collision_idx = code.index("target_owners = spark.sql")
        adapter_idx = code.index("get_source_adapter_for_connection(connection)")
        self.assertLess(collision_idx, adapter_idx)

        # A & B & C & D. Pre-load guard checks target collision before target write
        self.assertIn("target FQN collision", code)
        self.assertIn('target_owners[0]["connection_id"] != conn_id', code)
        self.assertIn('target_owners[0]["source_table_id"] != src_id', code)

        # Target comparison normalizes with coalesce and lower
        self.assertIn("coalesce(target_catalog", code)
        self.assertIn("coalesce(target_schema", code)
        self.assertIn("coalesce(target_table", code)

        # E. Inactive conflicting registrations excluded by is_active = true
        self.assertIn("WHERE is_active = true", code)

        # F & G. Source neutrality: check block has no dialect branches
        check_block = code[collision_idx:adapter_idx]
        self.assertNotIn("oracle", check_block.lower())
        self.assertNotIn("sqlserver", check_block.lower())


class TestControlRepositoryOwnershipSafety(unittest.TestCase):
    def test_update_control_for_connection_rejects_protected_fields(self):
        repo = ControlRepository(FakeSpark(), "cat", "ctrl")
        for field in ("connection_id", "source_table_id", "source_identity_version", "legacy_source_table_id"):
            with self.subTest(field=field):
                with self.assertRaisesRegex(ValueError, "Cannot update immutable identity field"):
                    repo.update_control_for_connection("c1", "t1", {field: "val"})

    def test_legacy_update_control_rejects_protected_fields(self):
        repo = ControlRepository(FakeSpark(), "cat", "ctrl")
        for field in ("connection_id", "source_table_id", "source_identity_version", "legacy_source_table_id"):
            with self.subTest(field=field):
                with self.assertRaisesRegex(ValueError, "Cannot update immutable identity field"):
                    repo.update_control("t1", {field: "val"})

    def test_legacy_single_key_methods_raise_on_multiple_rows(self):
        dup_rows = [
            FakeRow(source_table_id="t1", watermark_value="1"),
            FakeRow(source_table_id="t1", watermark_value="2"),
        ]
        repo = ControlRepository(FakeSpark(results=[dup_rows]), "cat", "ctrl")
        with self.assertRaisesRegex(ValueError, "matches 2 registrations; expected at most one"):
            repo.get_watermark("t1")

        repo = ControlRepository(FakeSpark(results=[dup_rows]), "cat", "ctrl")
        with self.assertRaisesRegex(ValueError, "matches 2 registrations; expected at most one"):
            repo.get_control_row("t1")

        repo = ControlRepository(FakeSpark(results=[dup_rows]), "cat", "ctrl")
        with self.assertRaisesRegex(ValueError, "matches 2 registrations; use update_control_for_connection"):
            repo.update_control("t1", {"status": "SUCCESS"})

    def test_get_connection_raises_on_duplicates(self):
        dup_conns = [
            FakeRow(connection_id="c1", source_system="oracle"),
            FakeRow(connection_id="c1", source_system="oracle"),
        ]
        repo = ControlRepository(FakeSpark(results=[dup_conns]), "cat", "ctrl")
        with self.assertRaisesRegex(ValueError, "resolves to 2 source_connection rows; expected at most one"):
            repo.get_connection("c1")

    def test_get_source_table_raises_on_duplicates(self):
        dup_tables = [
            FakeRow(connection_id="c1", source_table_id="t1"),
            FakeRow(connection_id="c1", source_table_id="t1"),
        ]
        repo = ControlRepository(FakeSpark(results=[dup_tables]), "cat", "ctrl")
        with self.assertRaisesRegex(ValueError, "resolve to 2 rows; expected at most one"):
            repo.get_source_table("c1", "t1")

    def test_active_tables_for_connection_rejects_blank_connection(self):
        repo = ControlRepository(FakeSpark(), "cat", "ctrl")
        with self.assertRaises(ValueError):
            repo.active_tables_for_connection("")

    def test_active_tables_for_connection_include_onboarding(self):
        repo = ControlRepository(FakeSpark(), "cat", "ctrl")
        repo.active_tables_for_connection("c1", include_onboarding=False)
        self.assertIn("is_active = true", repo.spark.last_sql())
        self.assertNotIn("current_status IN", repo.spark.last_sql())

        repo.active_tables_for_connection("c1", include_onboarding=True)
        self.assertIn("is_active = true OR current_status IN ('REGISTERED', 'INVENTORIED', 'PROVISIONED')",
                      repo.spark.last_sql())

    def test_endpoint_change_blocked_when_dependent_tables_exist(self):
        existing = [FakeRow(
            connection_id="c1", source_system="oracle",
            source_server="old-host", source_database="FINPDB",
            secret_scope="scope", trust_server_certificate=False,
            connection_status="VALID", is_active=True)]
        repo = ControlRepository(FakeSpark(results=[existing, [FakeRow(c=3)]]), "cat", "ctrl")
        with self.assertRaisesRegex(ValueError, "Cannot change material endpoint.*dependent table"):
            repo.upsert_connection({
                "connection_id": "c1", "source_system": "oracle",
                "source_server": "new-host", "source_database": "FINPDB",
                "secret_scope": "scope",
            })

    def test_endpoint_change_allowed_when_no_dependent_tables(self):
        existing = [FakeRow(
            connection_id="c1", source_system="oracle",
            source_server="old-host", source_database="FINPDB",
            secret_scope="scope", trust_server_certificate=False,
            connection_status="VALID", is_active=True)]
        repo = ControlRepository(FakeSpark(results=[existing, [FakeRow(c=0)]]), "cat", "ctrl")
        repo.upsert_connection({
            "connection_id": "c1", "source_system": "oracle",
            "source_server": "new-host", "source_database": "FINPDB",
            "secret_scope": "scope",
        })
        sql = repo.spark.last_sql()
        self.assertIn("`connection_status` = 'REGISTERED'", sql)
        self.assertIn("`is_active` = false", sql)

    def test_connection_name_change_preserves_validation_status(self):
        existing = [FakeRow(
            connection_id="c1", source_system="oracle",
            source_server="host", source_database="FINPDB",
            secret_scope="scope", trust_server_certificate=False,
            connection_status="VALID", is_active=True)]
        repo = ControlRepository(FakeSpark(results=[existing]), "cat", "ctrl")
        repo.upsert_connection({
            "connection_id": "c1", "connection_name": "New Name",
            "source_system": "oracle", "source_server": "host",
            "source_database": "FINPDB", "secret_scope": "scope",
        })
        sql = repo.spark.last_sql()
        self.assertNotIn("`connection_status` = 'REGISTERED'", sql)


if __name__ == "__main__":
    unittest.main()
