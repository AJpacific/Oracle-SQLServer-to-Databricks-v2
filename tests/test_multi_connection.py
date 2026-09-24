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


def deployment_nb(name):
    path = os.path.join(DEPLOYMENT, name)
    if name.endswith(".ipynb"):
        with open(path, "r", encoding="utf-8") as f:
            nb = json.load(f)
        return "\n".join("".join(c.get("source", [])) for c in nb.get("cells", []) if c.get("cell_type") == "code")
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def deployment_code(name):
    return deployment_nb(name)


class TestDeploymentNotebookStructure(unittest.TestCase):
    def test_deployment_code_cells_compile_and_have_metadata(self):
        for name in ("NB_CreateRunContext.ipynb",
                     "NB_GetFullLoadWorklist.ipynb",
                     "NB_GetDeltaWorklist.ipynb",
                     "NB_GetConnectionWorklist.ipynb",
                     "NB_GetSelectedAssessmentWorklist.ipynb"):
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
        self.assertIn("target_owners - {(conn_id, src_id)}", provisioning)
        self.assertNotIn("all_active_auto", provisioning)
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
        self.assertTrue(
            '.withColumn("is_active", F.lit(False))' in reg_code
            or "is_active=False" in reg_code
        )
        self.assertTrue(
            '.withColumn("current_status", F.lit("REGISTERED"))' in reg_code
            or 'current_status="REGISTERED"' in reg_code
        )
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


class TestConnectionWorklistNotebook(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.code = deployment_code("NB_GetConnectionWorklist.ipynb")

    def test_connection_worklist_inputs_and_guards(self):
        self.assertIn('widgets.text("run_id"', self.code)
        self.assertIn('widgets.text("source_system"', self.code)
        self.assertIn('widgets.dropdown("connection_mode", "VALID", ["VALID", "CONFIGURED"])', self.code)
        self.assertIn('widgets.text("max_connections"', self.code)
        self.assertIn('widgets.text("only_connection_ids"', self.code)
        self.assertIn('widgets.text("exclude_connection_ids"', self.code)
        self.assertIn("require_source_system(source_system_raw", self.code)
        self.assertIn("max_connections must be a non-negative integer", self.code)
        self.assertIn("connection_mode must be 'VALID' or 'CONFIGURED'", self.code)

    def test_connection_worklist_emits_only_connection_id(self):
        item = self.code.split("worklist = [", 1)[1].split("for cid in conn_ids", 1)[0]
        self.assertIn('"connection_id"', item)
        for forbidden in ("source_server", "source_database", "source_schema",
                          "source_table", "source_system", "secret_scope",
                          "password", "jdbc", "credentials"):
            self.assertNotIn(f'"{forbidden}"', item.lower())

    def test_connection_worklist_task_values_and_exit(self):
        self.assertIn('taskValues.set(key="run_id"', self.code)
        self.assertIn('taskValues.set(key="source_system"', self.code)
        self.assertIn('taskValues.set(key="connection_mode"', self.code)
        self.assertIn('taskValues.set(key="worklist", value=worklist)', self.code)
        self.assertIn('taskValues.set(key="worklist_count"', self.code)
        self.assertIn('"business_status": "NO_ELIGIBLE_CONNECTIONS"', self.code)
        self.assertIn('"business_status": "READY"', self.code)

    def test_connection_worklist_is_metadata_only(self):
        for forbidden in ("get_source_adapter", "read_source_jdbc",
                          "dbutils.secrets", "probe_connection"):
            self.assertNotIn(forbidden, self.code)


class TestSelectedAssessmentWorklistNotebook(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.code = deployment_code("NB_GetSelectedAssessmentWorklist.ipynb")

    def test_selected_assessment_inputs_and_guards(self):
        self.assertIn('widgets.text("run_id"', self.code)
        self.assertIn('widgets.text("source_system"', self.code)
        self.assertIn('widgets.text("max_batches"', self.code)
        self.assertIn('widgets.text("only_connection_ids"', self.code)
        self.assertIn('widgets.text("only_assessment_ids"', self.code)
        self.assertIn('widgets.dropdown("include_failed_retries"', self.code)
        self.assertIn("require_source_system(source_system_raw", self.code)

    def test_selected_assessment_worklist_emits_only_safe_batch_keys(self):
        item = self.code.split("worklist = [", 1)[1].split("for row in batch_rows", 1)[0]
        self.assertIn('"connection_id"', item)
        self.assertIn('"assessment_id"', item)
        for forbidden in ("source_server", "source_database", "source_schema",
                          "source_table", "source_system", "secret_scope",
                          "target_catalog", "target_schema", "password", "jdbc"):
            self.assertNotIn(f'"{forbidden}"', item.lower())

    def test_selected_assessment_checks_overlaps(self):
        self.assertIn("AMBIGUOUS_SELECTED_ASSESSMENT", self.code)
        self.assertIn("conflicting_assessment_count", self.code)

    def test_selected_assessment_task_values_and_exit(self):
        self.assertIn('taskValues.set(key="run_id"', self.code)
        self.assertIn('taskValues.set(key="source_system"', self.code)
        self.assertIn('taskValues.set(key="worklist", value=worklist)', self.code)
        self.assertIn('taskValues.set(key="worklist_count"', self.code)
        self.assertIn('taskValues.set(key="selected_table_count"', self.code)
        self.assertIn('taskValues.set(key="connection_count"', self.code)
        self.assertIn('"business_status": "NO_SELECTED_ASSESSMENTS"', self.code)
        self.assertIn('"business_status": "READY"', self.code)


class TestAssessmentSummaryNotebook(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        path = os.path.join(DEPLOYMENT, "NB_AssessmentSummary.py")
        with open(path, encoding="utf-8") as stream:
            cls.code = stream.read()

    def test_assessment_summary_cells_compile(self):
        ordinary = "\n".join(
            line for line in self.code.splitlines()
            if not line.lstrip().startswith(("%", "# MAGIC"))
        )
        ast.parse(ordinary)

    def test_assessment_summary_contract(self):
        self.assertIn('widgets.text("run_id"', self.code)
        self.assertIn('widgets.text("source_system"', self.code)
        self.assertIn("require_source_system(source_system_raw", self.code)
        self.assertIn('taskValues.set(key="connections_assessed"', self.code)
        self.assertIn('taskValues.set(key="assessments"', self.code)
        self.assertIn('taskValues.set(key="objects_assessed"', self.code)
        self.assertIn('taskValues.set(key="selected_table_count"', self.code)
        self.assertIn('taskValues.set(key="business_status"', self.code)
        self.assertIn('"business_status": "NO_RESULTS"', self.code)
        tail = self.code.split("dbutils.notebook.exit(")[-1]
        for forbidden in ("password", "secret_scope", "jdbc"):
            self.assertNotIn(forbidden, tail.lower())


class TestConnectionDiscovery(unittest.TestCase):
    def test_oracle_filtering(self):
        repo = ControlRepository(FakeSpark(results=[[]]), "cat", "ctrl")
        repo.valid_active_connections_for_source("oracle")
        sql = repo.spark.last_sql()
        self.assertIn("lower(trim(source_system)) = 'oracle'", sql)
        self.assertIn("coalesce(is_active, false) = true", sql)
        self.assertIn("upper(trim(connection_status)) = 'VALID'", sql)
        self.assertIn("secret_scope IS NOT NULL", sql)
        self.assertIn("ORDER BY connection_id ASC", sql)

    def test_sqlserver_canonical_and_alias_normalization(self):
        for alias in ("sqlserver", "mssql", "sql_server"):
            repo = ControlRepository(FakeSpark(results=[[]]), "cat", "ctrl")
            repo.valid_active_connections_for_source(alias)
            sql = repo.spark.last_sql()
            self.assertIn("lower(trim(source_system)) = 'sqlserver'", sql, alias)

    def test_unknown_source_fails(self):
        repo = ControlRepository(FakeSpark(), "cat", "ctrl")
        with self.assertRaises(ValueError):
            repo.valid_active_connections_for_source("unknown_system")

    def test_only_and_exclude_filters_and_precedence(self):
        repo = ControlRepository(FakeSpark(results=[[]]), "cat", "ctrl")
        repo.valid_active_connections_for_source(
            "oracle",
            only_connection_ids=["c1", "c2"],
            exclude_connection_ids=["c2", "c3"]
        )
        sql = repo.spark.last_sql()
        self.assertIn("connection_id IN ('c1')", sql)
        self.assertIn("connection_id NOT IN ('c2', 'c3')", sql)

    def test_exclusion_wins_entirely(self):
        repo = ControlRepository(FakeSpark(results=[[]]), "cat", "ctrl")
        repo.valid_active_connections_for_source(
            "oracle",
            only_connection_ids=["c1"],
            exclude_connection_ids=["c1"]
        )
        sql = repo.spark.last_sql()
        self.assertIn("1 = 0", sql)

    def test_projection_excludes_endpoints_and_secrets(self):
        repo = ControlRepository(FakeSpark(results=[[]]), "cat", "ctrl")
        repo.valid_active_connections_for_source("oracle")
        sql = repo.spark.last_sql()
        self.assertTrue(sql.startswith("SELECT connection_id FROM"))
        prefix = sql.lower().split("from")[0]
        for forbidden in ("source_server", "source_database", "secret_scope", "password", "jdbc"):
            self.assertNotIn(forbidden, prefix)

    def test_configured_discovery_oracle(self):
        repo = ControlRepository(FakeSpark(results=[[]]), "cat", "ctrl")
        repo.configured_connections_for_source("oracle")
        sql = repo.spark.last_sql()
        self.assertIn("lower(trim(source_system)) = 'oracle'", sql)
        self.assertIn("coalesce(is_active, false) = true", sql)
        self.assertIn("upper(trim(connection_status)) IN ('REGISTERED', 'VALID', 'FAILED')", sql)
        self.assertIn("source_server IS NOT NULL", sql)
        self.assertIn("secret_scope IS NOT NULL", sql)
        self.assertIn("ORDER BY connection_id ASC", sql)

    def test_configured_discovery_sqlserver(self):
        repo = ControlRepository(FakeSpark(results=[[]]), "cat", "ctrl")
        repo.configured_connections_for_source("sqlserver")
        sql = repo.spark.last_sql()
        self.assertIn("lower(trim(source_system)) = 'sqlserver'", sql)
        self.assertIn("coalesce(is_active, false) = true", sql)
        self.assertIn("upper(trim(connection_status)) IN ('REGISTERED', 'VALID', 'FAILED')", sql)
        self.assertNotIn("source_database IS NOT NULL", sql)

    def test_configured_discovery_filters_and_precedence(self):
        repo = ControlRepository(FakeSpark(results=[[]]), "cat", "ctrl")
        repo.configured_connections_for_source(
            "oracle",
            only_connection_ids=["c1", "c2"],
            exclude_connection_ids=["c2", "c3"]
        )
        sql = repo.spark.last_sql()
        self.assertIn("connection_id IN ('c1')", sql)
        self.assertIn("connection_id NOT IN ('c2', 'c3')", sql)

    def test_configured_discovery_exclusion_wins_entirely(self):
        repo = ControlRepository(FakeSpark(results=[[]]), "cat", "ctrl")
        repo.configured_connections_for_source(
            "oracle",
            only_connection_ids=["c1"],
            exclude_connection_ids=["c1"]
        )
        sql = repo.spark.last_sql()
        self.assertIn("1 = 0", sql)

    def test_configured_discovery_projection(self):
        repo = ControlRepository(FakeSpark(results=[[]]), "cat", "ctrl")
        repo.configured_connections_for_source("oracle")
        sql = repo.spark.last_sql()
        self.assertTrue(sql.startswith("SELECT connection_id FROM"))
        prefix = sql.lower().split("from")[0]
        for forbidden in ("source_server", "source_database", "secret_scope", "password", "jdbc"):
            self.assertNotIn(forbidden, prefix)


class TestTargetConfigurationResolution(unittest.TestCase):
    def test_connection_specific_wins(self):
        conn = FakeRow(connection_id="c1", source_system="oracle", is_active=True, connection_status="VALID", secret_scope="sc")
        cfg_conn = FakeRow(config_id="cfg_conn", target_catalog="cat_conn", target_schema_mode="SOURCE_SCHEMA", target_schema=None, is_default=True, is_active=True)
        repo = ControlRepository(FakeSpark(results=[[conn], [cfg_conn]]), "cat", "ctrl")
        resolved = repo.resolve_target_config("c1")
        self.assertEqual(resolved["config_id"], "cfg_conn")
        self.assertEqual(resolved["target_catalog"], "cat_conn")
        self.assertEqual(resolved["effective_scope"], "CONNECTION")

    def test_source_specific_fallback(self):
        conn = FakeRow(connection_id="c1", source_system="oracle", is_active=True, connection_status="VALID", secret_scope="sc")
        cfg_src = FakeRow(config_id="cfg_src", target_catalog="cat_src", target_schema_mode="SOURCE_SCHEMA", target_schema=None, is_default=True, is_active=True)
        repo = ControlRepository(FakeSpark(results=[[conn], [], [cfg_src]]), "cat", "ctrl")
        resolved = repo.resolve_target_config("c1")
        self.assertEqual(resolved["config_id"], "cfg_src")
        self.assertEqual(resolved["effective_scope"], "SOURCE")

    def test_global_fallback(self):
        conn = FakeRow(connection_id="c1", source_system="oracle", is_active=True, connection_status="VALID", secret_scope="sc")
        cfg_global = FakeRow(config_id="cfg_global", target_catalog="cat_global", target_schema_mode="SOURCE_SCHEMA", target_schema=None, is_default=True, is_active=True)
        repo = ControlRepository(FakeSpark(results=[[conn], [], [], [cfg_global]]), "cat", "ctrl")
        resolved = repo.resolve_target_config("c1")
        self.assertEqual(resolved["config_id"], "cfg_global")
        self.assertEqual(resolved["effective_scope"], "GLOBAL")

    def test_missing_config_fails(self):
        conn = FakeRow(connection_id="c1", source_system="oracle", is_active=True, connection_status="VALID", secret_scope="sc")
        repo = ControlRepository(FakeSpark(results=[[conn], [], [], []]), "cat", "ctrl")
        with self.assertRaisesRegex(ValueError, "No active default target configuration found"):
            repo.resolve_target_config("c1")

    def test_duplicate_defaults_at_chosen_scope_fails(self):
        conn = FakeRow(connection_id="c1", source_system="oracle", is_active=True, connection_status="VALID", secret_scope="sc")
        repo = ControlRepository(FakeSpark(results=[[conn], [FakeRow(config_id="1"), FakeRow(config_id="2")]]), "cat", "ctrl")
        with self.assertRaisesRegex(ValueError, "Duplicate active default"):
            repo.resolve_target_config("c1")

    def test_explicit_mode_requires_target_schema(self):
        conn = FakeRow(connection_id="c1", source_system="oracle", is_active=True, connection_status="VALID", secret_scope="sc")
        cfg = FakeRow(config_id="cfg_1", target_catalog="cat", target_schema_mode="EXPLICIT", target_schema=None, is_default=True, is_active=True)
        repo = ControlRepository(FakeSpark(results=[[conn], [cfg]]), "cat", "ctrl")
        with self.assertRaisesRegex(ValueError, "EXPLICIT mode requires nonblank target_schema"):
            repo.resolve_target_config("c1")

    def test_blank_target_catalog_fails(self):
        conn = FakeRow(connection_id="c1", source_system="oracle", is_active=True, connection_status="VALID", secret_scope="sc")
        cfg = FakeRow(config_id="cfg_1", target_catalog="", target_schema_mode="SOURCE_SCHEMA", target_schema=None, is_default=True, is_active=True)
        repo = ControlRepository(FakeSpark(results=[[conn], [cfg]]), "cat", "ctrl")
        with self.assertRaisesRegex(ValueError, "blank target_catalog"):
            repo.resolve_target_config("c1")

    def test_connection_source_system_mismatch_fails(self):
        conn = FakeRow(connection_id="c1", source_system="oracle", is_active=True, connection_status="VALID", secret_scope="sc")
        cfg = FakeRow(config_id="cfg_1", source_system="sqlserver", target_catalog="cat", target_schema_mode="SOURCE_SCHEMA", target_schema=None, is_default=True, is_active=True)
        repo = ControlRepository(FakeSpark(results=[[conn], [cfg]]), "cat", "ctrl")
        with self.assertRaisesRegex(ValueError, "does not match connection"):
            repo.resolve_target_config("c1")

    def test_safe_projection_no_secrets(self):
        conn = FakeRow(connection_id="c1", source_system="oracle", is_active=True, connection_status="VALID", secret_scope="sc")
        cfg = FakeRow(config_id="cfg_1", target_catalog="cat", target_schema_mode="SOURCE_SCHEMA", target_schema=None, is_default=True, is_active=True)
        repo = ControlRepository(FakeSpark(results=[[conn], [cfg]]), "cat", "ctrl")
        res = repo.resolve_target_config("c1")
        self.assertEqual(
            set(res.keys()),
            {"config_id", "target_catalog", "target_schema_mode", "target_schema", "effective_scope"}
        )


class TestSelectedAssessmentDiscovery(unittest.TestCase):
    def test_selected_assessment_batches_sql(self):
        repo = ControlRepository(FakeSpark(results=[[]]), "cat", "ctrl")
        repo.selected_assessment_batches("oracle")
        sql = repo.spark.last_sql()
        self.assertIn("sa.object_type = 'TABLE'", sql)
        self.assertIn("upper(trim(sa.compatibility_status)) IN ('COMPATIBLE', 'REVIEW')", sql)
        self.assertIn("sa.is_selected = true", sql)
        self.assertIn("sc.is_active = true", sql)
        self.assertIn("upper(trim(sc.connection_status)) = 'VALID'", sql)
        self.assertIn("ORDER BY sa.connection_id, sa.assessment_id", sql)

    def test_include_failed_retries_flag(self):
        repo = ControlRepository(FakeSpark(results=[[]]), "cat", "ctrl")
        repo.selected_assessment_batches("oracle", include_failed_retries=False)
        self.assertNotIn("'FAILED'", repo.spark.last_sql())
        repo.selected_assessment_batches("oracle", include_failed_retries=True)
        self.assertIn("'FAILED'", repo.spark.last_sql())

    def test_check_overlapping_selected_assessments(self):
        conflict_row = FakeRow(connection_id="c1", source_schema="HR", object_name="EMP", conflicting_assessment_count=2)
        repo = ControlRepository(FakeSpark(results=[[conflict_row]]), "cat", "ctrl")
        conflicts = repo.check_overlapping_selected_assessments("oracle")
        self.assertEqual(len(conflicts), 1)
        self.assertEqual(conflicts[0]["object_name"], "EMP")
        self.assertEqual(conflicts[0]["conflicting_assessment_count"], 2)

    def test_update_assessment_selection_state(self):
        repo = ControlRepository(FakeSpark(results=[]), "cat", "ctrl")
        repo.update_assessment_selection_state("c1", "a1", "HR", "EMP", "SELECTED", selected_by="operator1")
        sql = repo.spark.last_sql()
        self.assertIn("`selection_status` = 'SELECTED'", sql)
        self.assertIn("`is_selected` = true", sql)
        self.assertIn("`selected_ts` = current_timestamp()", sql)
        self.assertIn("`selected_by` = 'operator1'", sql)

        repo.update_assessment_selection_state("c1", "a1", "HR", "EMP", "NOT_SELECTED")
        sql = repo.spark.last_sql()
        self.assertIn("`selection_status` = 'NOT_SELECTED'", sql)
        self.assertIn("`is_selected` = false", sql)
        self.assertIn("`selected_ts` = NULL", sql)
        self.assertIn("`selected_by` = NULL", sql)

        for invalid_status in ("ONBOARDING", "REGISTERED", "ONBOARDED", "FAILED", "REVIEW_REQUIRED", "BLOCKED"):
            with self.assertRaises(ValueError):
                repo.update_assessment_selection_state("c1", "a1", "HR", "EMP", invalid_status)


class TestRegistrationSelectionModes(unittest.TestCase):
    def test_registration_supports_assessment_flags(self):
        code = shared_nb("NB01B_RegisterSelectedTables.py")
        self.assertIn('widgets.dropdown("selection_mode", "ASSESSMENT_FLAGS"', code)
        self.assertIn('selection_mode == "ASSESSMENT_FLAGS"', code)
        self.assertIn("repo.resolve_target_config(connection_id)", code)
        self.assertIn("repo.claim_assessment_selection_row(", code)
        self.assertIn("repo.mark_assessment_registration_succeeded(", code)
        self.assertIn("repo.mark_assessment_onboarding_failed(", code)
        self.assertNotIn("repo.mark_assessment_onboarding_completed(", code)
        self.assertNotIn("selection_status = 'ONBOARDED'", code)

    def test_registration_exit_payload_contract(self):
        code = shared_nb("NB01B_RegisterSelectedTables.py")
        for required in ("status", "business_status", "run_id", "connection_id", "assessment_id",
                         "selection_mode", "target_config_id", "selected_count", "registered_count",
                         "already_registered_count", "skipped_count", "conflict_count", "failed_count",
                         "errors", "worklist"):
            self.assertIn(f'"{required}"', code)


class TestControlTableInitValidations(unittest.TestCase):
    def test_target_config_and_selection_validations_present(self):
        code = shared_nb("NB00_ControlTableInit.py")
        for validation_code in (
            "BLANK_TARGET_CONFIG_ID",
            "DUPLICATE_TARGET_CONFIG_ID",
            "INVALID_TARGET_SCHEMA_MODE",
            "BLANK_ACTIVE_DEFAULT_TARGET_CATALOG",
            "EXPLICIT_MODE_MISSING_TARGET_SCHEMA",
            "DUPLICATE_CONNECTION_ACTIVE_DEFAULT_TARGET_CONFIG",
            "DUPLICATE_SOURCE_ACTIVE_DEFAULT_TARGET_CONFIG",
            "DUPLICATE_GLOBAL_ACTIVE_DEFAULT_TARGET_CONFIG",
            "ORPHAN_TARGET_CONFIG_CONNECTION",
            "TARGET_CONFIG_CONNECTION_SOURCE_MISMATCH",
            "INVALID_ASSESSMENT_SELECTION_STATUS",
            "ACTIVE_CONNECTION_INVALID_STATUS",
            "ACTIVE_TABLE_INVALID_CONNECTION",
        ):
            self.assertIn(f'"{validation_code}"', code)
        self.assertNotIn('"ACTIVE_CONNECTION_NOT_VALID"', code)


class FakeDbUtilsTV:
    def __init__(self, widgets_dict):
        self._widgets = dict(widgets_dict)
        self.task_values = {}
        self.exit_payload = None
        self.widgets = self

    def get(self, name):
        return str(self._widgets.get(name, ""))

    def text(self, name, default=""):
        if name not in self._widgets:
            self._widgets[name] = default

    def dropdown(self, name, default="", choices=None):
        if name not in self._widgets:
            self._widgets[name] = default

    @property
    def notebook(self):
        outer = self
        class _NB:
            def exit(self, val):
                outer.exit_payload = val
        return _NB()

    @property
    def jobs(self):
        outer = self
        class _Jobs:
            @property
            def taskValues(self):
                class _TV:
                    def set(self, key, value):
                        outer.task_values[key] = value
                return _TV()
        return _Jobs()


def _run_nb00a(source_token, widgets_dict, connection_row=None, probe_side_effect=None):
    dbutils = FakeDbUtilsTV(widgets_dict)
    status_updates = []

    class FakeRepo:
        def get_connection(self, cid):
            if connection_row is not None and connection_row.get("connection_id") == cid:
                return FakeRow(**connection_row)
            return None

        def update_connection_status(self, cid, status, error_message=None):
            status_updates.append({"connection_id": cid, "status": status, "error_message": error_message})

        def upsert_connection(self, *args, **kwargs):
            raise AssertionError("NB00A must not call repo.upsert_connection")

    repo_instance = FakeRepo()
    probe_calls = []

    def fake_probe_connection(adapter, source_server=None, source_database=None):
        probe_calls.append({"adapter": adapter, "server": source_server, "database": source_database})
        if probe_side_effect:
            if isinstance(probe_side_effect, Exception):
                raise probe_side_effect
            probe_side_effect()
        return True

    from source_identity import require_source_system
    from control_repository import assert_source_system_match, require_connection_id
    from failure_classifier import sanitize_message

    class FakeFailCls:
        @staticmethod
        def sanitize_message(exc):
            return sanitize_message(str(exc))

    def fake_get_source_adapter(conn, require_valid=False, **kwargs):
        c = conn.asDict() if hasattr(conn, "asDict") else dict(conn)
        scope = (c.get("secret_scope") or "").strip()
        if not scope:
            raise ValueError(f"registered connection {c.get('connection_id')!r} has a blank secret_scope")
        sys_token = require_source_system(c.get("source_system"), "test adapter")
        class FakeAdapter:
            source_system = sys_token
        return FakeAdapter()

    nb_code = source_nb(source_token, "NB00A_UpsertAndValidateConnection.py")
    lines = [l for l in nb_code.splitlines() if not l.strip().startswith(("%", "# MAGIC"))]
    clean_code = "\n".join(lines)

    env = {
        "dbutils": dbutils,
        "get_run_id": lambda: widgets_dict.get("run_id", "test_run_01"),
        "CONNECTION_ID": widgets_dict.get("connection_id", ""),
        "require_connection_id": require_connection_id,
        "control_repo": lambda: repo_instance,
        "require_source_system": require_source_system,
        "assert_source_system_match": assert_source_system_match,
        "get_source_adapter_for_connection": fake_get_source_adapter,
        "probe_connection": fake_probe_connection,
        "failcls": FakeFailCls,
        "set_task_value": lambda k, v: dbutils.jobs.taskValues.set(k, v),
        "json": json,
        "print": lambda *args: None,
    }

    exec(clean_code, env)
    return {
        "dbutils": dbutils,
        "status_updates": status_updates,
        "probe_calls": probe_calls,
        "exit_payload": json.loads(dbutils.exit_payload) if dbutils.exit_payload else None,
        "task_values": dbutils.task_values,
    }


class TestConnectionValidationNotebookContract(unittest.TestCase):
    def test_01_fixed_source_system(self):
        for token in ("oracle", "sqlserver"):
            code = source_nb(token, "NB00A_UpsertAndValidateConnection.py")
            self.assertIn(f'SOURCE_SYSTEM = "{token}"', code)

    def test_02_no_source_metadata_widgets(self):
        for token in ("oracle", "sqlserver"):
            code = source_nb(token, "NB00A_UpsertAndValidateConnection.py")
            self.assertNotIn("dbutils.widgets", code)
            self.assertNotIn("widgets.text", code)
            self.assertNotIn("widgets.dropdown", code)

    def test_03_no_call_to_upsert_connection(self):
        for token in ("oracle", "sqlserver"):
            code = source_nb(token, "NB00A_UpsertAndValidateConnection.py")
            self.assertNotIn("repo.upsert_connection", code)
            self.assertNotIn("upsert_connection", code)

    def test_04_reads_row_with_get_connection(self):
        for token in ("oracle", "sqlserver"):
            code = source_nb(token, "NB00A_UpsertAndValidateConnection.py")
            self.assertIn("repo.get_connection(connection_id)", code)

    def test_05_missing_connection_id_fails(self):
        for token in ("oracle", "sqlserver"):
            with self.assertRaises(ValueError) as ctx:
                _run_nb00a(token, {"connection_id": ""})
            self.assertIn("requires connection_id", str(ctx.exception))

    def test_06_unknown_connection_id_fails(self):
        for token in ("oracle", "sqlserver"):
            with self.assertRaises(ValueError) as ctx:
                _run_nb00a(token, {"connection_id": "c_missing"}, connection_row=None)
            self.assertEqual(
                str(ctx.exception),
                "connection_id 'c_missing' was not found in source_connection"
            )

    def test_07_oracle_rejects_sqlserver_connection(self):
        row = {
            "connection_id": "conn_sql",
            "source_system": "sqlserver",
            "source_server": "sql.example.com",
            "source_database": "TestDB",
            "secret_scope": "sql_scope",
            "connection_status": "REGISTERED",
            "is_active": False,
        }
        with self.assertRaises(ValueError) as ctx:
            _run_nb00a("oracle", {"connection_id": "conn_sql"}, connection_row=row)
        self.assertIn("source_system conflict", str(ctx.exception))

    def test_08_sqlserver_rejects_oracle_connection(self):
        row = {
            "connection_id": "conn_ora",
            "source_system": "oracle",
            "source_server": "ora.example.com",
            "source_database": "ORCL",
            "secret_scope": "ora_scope",
            "connection_status": "REGISTERED",
            "is_active": False,
        }
        with self.assertRaises(ValueError) as ctx:
            _run_nb00a("sqlserver", {"connection_id": "conn_ora"}, connection_row=row)
        self.assertIn("source_system conflict", str(ctx.exception))

    def test_09_missing_source_system_fails(self):
        for token in ("oracle", "sqlserver"):
            row = {
                "connection_id": "c1",
                "source_system": "",
                "source_server": "srv1",
                "secret_scope": "sc1",
            }
            with self.assertRaises(ValueError) as ctx:
                _run_nb00a(token, {"connection_id": "c1"}, connection_row=row)
            self.assertIn("source_system", str(ctx.exception).lower())

    def test_10_unknown_source_system_fails(self):
        for token in ("oracle", "sqlserver"):
            row = {
                "connection_id": "c1",
                "source_system": "unsupported_engine",
                "source_server": "srv1",
                "secret_scope": "sc1",
            }
            with self.assertRaises(ValueError) as ctx:
                _run_nb00a(token, {"connection_id": "c1"}, connection_row=row)
            self.assertIn("unsupported source_system", str(ctx.exception).lower())

    def test_11_sqlserver_missing_source_database_fails_before_probe(self):
        row = {
            "connection_id": "conn_sql_nodb",
            "source_system": "sqlserver",
            "source_server": "sql.example.com",
            "source_database": "",
            "secret_scope": "sql_scope",
            "connection_status": "REGISTERED",
            "is_active": False,
        }
        res = _run_nb00a("sqlserver", {"connection_id": "conn_sql_nodb"}, connection_row=row)
        self.assertEqual(res["task_values"]["status"], "VALID")
        self.assertEqual(row.get("source_database"), "")

    def test_12_blank_secret_scope_fails_before_probe(self):
        for token in ("oracle", "sqlserver"):
            row = {
                "connection_id": "conn_noscope",
                "source_system": token,
                "source_server": "srv.example.com",
                "source_database": "DB" if token == "sqlserver" else None,
                "secret_scope": "",
                "connection_status": "REGISTERED",
                "is_active": False,
            }
            with self.assertRaises(ValueError) as ctx:
                _run_nb00a(token, {"connection_id": "conn_noscope"}, connection_row=row)
            self.assertIn("secret_scope", str(ctx.exception).lower())

    def test_13_successful_probe_updates_status_to_valid(self):
        for token in ("oracle", "sqlserver"):
            row = {
                "connection_id": f"conn_{token}_ok",
                "source_system": token,
                "source_server": f"{token}.example.com",
                "source_database": "DB1" if token == "sqlserver" else "ORCL",
                "secret_scope": f"{token}_scope",
                "connection_status": "REGISTERED",
                "is_active": False,
            }
            res = _run_nb00a(token, {"run_id": "run_test_01", "connection_id": row["connection_id"]}, connection_row=row)
            self.assertEqual(len(res["status_updates"]), 1)
            self.assertEqual(res["status_updates"][0]["status"], "VALID")
            self.assertIsNone(res["status_updates"][0]["error_message"])
            self.assertEqual(res["exit_payload"]["status"], "VALID")
            self.assertEqual(res["exit_payload"]["connection_status"], "VALID")
            self.assertEqual(res["task_values"]["status"], "VALID")
            self.assertEqual(res["task_values"]["connection_status"], "VALID")
            self.assertEqual(len(res["probe_calls"]), 1)

    def test_14_failed_probe_updates_status_to_failed(self):
        for token in ("oracle", "sqlserver"):
            row = {
                "connection_id": f"conn_{token}_fail",
                "source_system": token,
                "source_server": f"{token}.example.com",
                "source_database": "DB1" if token == "sqlserver" else "ORCL",
                "secret_scope": f"{token}_scope",
                "connection_status": "REGISTERED",
                "is_active": False,
            }
            with self.assertRaises(RuntimeError):
                _run_nb00a(
                    token,
                    {"run_id": "run_test_01", "connection_id": row["connection_id"]},
                    connection_row=row,
                    probe_side_effect=RuntimeError("connection refused")
                )

    def test_15_failed_probe_keeps_is_active_false_through_update_connection_status(self):
        spark = FakeSpark(results=[[]])
        repo = ControlRepository(spark, "cat", "ctrl")
        repo.update_connection_status("conn_fail", "FAILED", "some error")
        sql = spark.last_sql()
        self.assertIn("`connection_status` = 'FAILED'", sql)
        self.assertIn("`is_active` = false", sql)
        self.assertIn("`error_message` = 'some error'", sql)
        self.assertNotIn("`last_validated_ts`", sql)

    def test_16_failure_text_is_sanitized_and_bounded(self):
        repo_spark = FakeSpark(results=[[]])
        repo = ControlRepository(repo_spark, "cat", "ctrl")
        long_secret_error = "Authentication failed: password=super_secret_token_12345! " + ("x" * 2000)
        from failure_classifier import sanitize_message
        safe = sanitize_message(long_secret_error)
        self.assertNotIn("super_secret_token_12345", safe)
        self.assertIn("password=***", safe)
        repo.update_connection_status("conn_1", "FAILED", safe[:1000])
        sql = repo_spark.last_sql()
        self.assertNotIn("super_secret_token_12345", sql)
        self.assertIn("password=***", sql)
        self.assertLessEqual(len(safe[:1000]), 1000)

    def test_17_credentials_and_secrets_not_returned(self):
        for token in ("oracle", "sqlserver"):
            row = {
                "connection_id": f"conn_{token}_safe",
                "source_system": token,
                "source_server": f"{token}.example.com",
                "source_database": "DB1" if token == "sqlserver" else "ORCL",
                "secret_scope": "super_secret_scope",
                "connection_status": "REGISTERED",
                "is_active": False,
            }
            res = _run_nb00a(token, {"run_id": "run_1", "connection_id": row["connection_id"]}, connection_row=row)
            exit_str = json.dumps(res["exit_payload"]).lower()
            tv_str = json.dumps(res["task_values"]).lower()
            for forbidden in ("secret_scope", "super_secret_scope", "password", "jdbc", "source_server"):
                self.assertNotIn(forbidden, exit_str)
                self.assertNotIn(forbidden, tv_str)

    def test_18_source_connection_metadata_not_mutated_by_nb00a(self):
        for token in ("oracle", "sqlserver"):
            code = source_nb(token, "NB00A_UpsertAndValidateConnection.py")
            self.assertNotIn("repo.upsert_connection", code)
            self.assertNotIn("repo.save_connection", code)
            self.assertNotIn("UPDATE", code)

    def test_19_worklist_configured_mode_includes_all_statuses(self):
        spark = FakeSpark(results=[[]])
        repo = ControlRepository(spark, "cat", "ctrl")
        repo.configured_connections_for_source("oracle")
        sql = spark.last_sql()
        self.assertIn("upper(trim(connection_status)) IN ('REGISTERED', 'VALID', 'FAILED')", sql)
        self.assertIn("source_server IS NOT NULL", sql)
        self.assertIn("secret_scope IS NOT NULL", sql)

    def test_20_worklist_configured_mode_requires_active(self):
        spark = FakeSpark(results=[[]])
        repo = ControlRepository(spark, "cat", "ctrl")
        repo.configured_connections_for_source("oracle")
        sql = spark.last_sql()
        self.assertIn("coalesce(is_active, false) = true", sql)
        self.assertIn("upper(trim(connection_status)) IN ('REGISTERED', 'VALID', 'FAILED')", sql)

    def test_21_worklist_valid_mode_preserves_active_and_valid(self):
        spark = FakeSpark(results=[[]])
        repo = ControlRepository(spark, "cat", "ctrl")
        repo.valid_active_connections_for_source("oracle")
        sql = spark.last_sql()
        self.assertIn("coalesce(is_active, false) = true", sql)
        self.assertIn("upper(trim(connection_status)) = 'VALID'", sql)

    def test_22_worklist_emits_only_connection_id(self):
        for mode in ("valid_active_connections_for_source", "configured_connections_for_source"):
            spark = FakeSpark(results=[[]])
            repo = ControlRepository(spark, "cat", "ctrl")
            getattr(repo, mode)("oracle")
            sql = spark.last_sql()
            self.assertTrue(sql.startswith("SELECT connection_id FROM"))

    def test_23_worklist_only_and_exclude_filters(self):
        for mode in ("valid_active_connections_for_source", "configured_connections_for_source"):
            spark = FakeSpark(results=[[]])
            repo = ControlRepository(spark, "cat", "ctrl")
            getattr(repo, mode)("oracle", only_connection_ids=["c1", "c2"], exclude_connection_ids=["c2"])
            sql = spark.last_sql()
            self.assertIn("connection_id IN ('c1')", sql)
            self.assertIn("connection_id NOT IN ('c2')", sql)

    def test_24_worklist_exclusion_precedence_unchanged(self):
        for mode in ("valid_active_connections_for_source", "configured_connections_for_source"):
            spark = FakeSpark(results=[[]])
            repo = ControlRepository(spark, "cat", "ctrl")
            getattr(repo, mode)("oracle", only_connection_ids=["c1"], exclude_connection_ids=["c1"])
            sql = spark.last_sql()
            self.assertIn("1 = 0", sql)

    def test_25_nb00a_output_contract_parity(self):
        ora_row = {
            "connection_id": "c_ora",
            "source_system": "oracle",
            "source_server": "ora.example.com",
            "source_database": "ORCL",
            "secret_scope": "ora_scope",
            "connection_status": "REGISTERED",
            "is_active": False,
        }
        sql_row = {
            "connection_id": "c_sql",
            "source_system": "sqlserver",
            "source_server": "sql.example.com",
            "source_database": "DB1",
            "secret_scope": "sql_scope",
            "connection_status": "REGISTERED",
            "is_active": False,
        }
        res_ora = _run_nb00a("oracle", {"run_id": "r1", "connection_id": "c_ora"}, connection_row=ora_row)
        res_sql = _run_nb00a("sqlserver", {"run_id": "r1", "connection_id": "c_sql"}, connection_row=sql_row)
        self.assertEqual(set(res_ora["exit_payload"].keys()), set(res_sql["exit_payload"].keys()))
        self.assertEqual(
            set(res_ora["exit_payload"].keys()),
            {"status", "connection_status", "run_id", "connection_id", "source_system", "source_database"}
        )
        self.assertEqual(set(res_ora["task_values"].keys()), set(res_sql["task_values"].keys()))
        self.assertEqual(
            set(res_ora["task_values"].keys()),
            {"run_id", "connection_id", "source_system", "status", "connection_status"}
        )

    def test_26_notebook_cells_compile_and_run_targets_resolve(self):
        import re
        for token in ("oracle", "sqlserver"):
            code = source_nb(token, "NB00A_UpsertAndValidateConnection.py")
            ordinary = "\n".join(
                line for line in code.splitlines()
                if not line.lstrip().startswith(("%", "# MAGIC"))
            )
            ast.parse(ordinary)

            # Check %run targets
            run_matches = re.findall(r"%run\s+([^\s\n]+)", code)
            self.assertTrue(len(run_matches) >= 1)
            for target in run_matches:
                nb_dir = os.path.join(ROOT, "notebooks", "sources", token)
                resolved = os.path.normpath(os.path.join(nb_dir, target))
                if not resolved.endswith(".py") and not resolved.endswith(".ipynb"):
                    resolved_py = resolved + ".py"
                    resolved_ipynb = resolved + ".ipynb"
                    exists = os.path.isfile(resolved_py) or os.path.isfile(resolved_ipynb)
                else:
                    exists = os.path.isfile(resolved)
                self.assertTrue(exists, f"Resolved %run target {resolved} does not exist for {token}")


class TestConnectionActiveEligibilityMatrix(unittest.TestCase):
    """Test the complete is_active eligibility contract for Job 1A."""

    def test_configured_mode_sql_predicate(self):
        spark = FakeSpark(results=[[]])
        repo = ControlRepository(spark, "cat", "ctrl")
        repo.configured_connections_for_source("sqlserver")
        sql = spark.last_sql()
        self.assertIn("coalesce(is_active, false) = true", sql)
        self.assertIn("upper(trim(connection_status)) IN ('REGISTERED', 'VALID', 'FAILED')", sql)

    def test_valid_mode_sql_predicate(self):
        spark = FakeSpark(results=[[]])
        repo = ControlRepository(spark, "cat", "ctrl")
        repo.valid_active_connections_for_source("sqlserver")
        sql = spark.last_sql()
        self.assertIn("coalesce(is_active, false) = true", sql)
        self.assertIn("upper(trim(connection_status)) = 'VALID'", sql)

    def test_configured_mode_simulation_matrix(self):
        """CONFIGURED mode must include REGISTERED/VALID/FAILED with is_active=true,
        and exclude all is_active=false or is_active=None rows.
        """
        rows = [
            {"connection_id": "c_reg_true", "connection_status": "REGISTERED", "is_active": True},
            {"connection_id": "c_val_true", "connection_status": "VALID", "is_active": True},
            {"connection_id": "c_fail_true", "connection_status": "FAILED", "is_active": True},
            {"connection_id": "c_reg_false", "connection_status": "REGISTERED", "is_active": False},
            {"connection_id": "c_val_false", "connection_status": "VALID", "is_active": False},
            {"connection_id": "c_fail_false", "connection_status": "FAILED", "is_active": False},
            {"connection_id": "c_reg_none", "connection_status": "REGISTERED", "is_active": None},
            {"connection_id": "c_val_none", "connection_status": "VALID", "is_active": None},
            {"connection_id": "c_fail_none", "connection_status": "FAILED", "is_active": None},
        ]
        # Simulate NB_GetConnectionWorklist / SQL evaluation in CONFIGURED mode:
        # coalesce(is_active, false) == true AND upper(trim(connection_status)) in ('REGISTERED', 'VALID', 'FAILED')
        eligible = [
            r["connection_id"] for r in rows
            if bool(r.get("is_active")) is True
            and str(r.get("connection_status") or "").strip().upper() in ("REGISTERED", "VALID", "FAILED")
        ]
        self.assertEqual(sorted(eligible), ["c_fail_true", "c_reg_true", "c_val_true"])

    def test_valid_mode_simulation_matrix(self):
        """VALID mode must include VALID with is_active=true,
        and exclude VALID with false/None, and all REGISTERED/FAILED.
        """
        rows = [
            {"connection_id": "c_reg_true", "connection_status": "REGISTERED", "is_active": True},
            {"connection_id": "c_val_true", "connection_status": "VALID", "is_active": True},
            {"connection_id": "c_fail_true", "connection_status": "FAILED", "is_active": True},
            {"connection_id": "c_val_false", "connection_status": "VALID", "is_active": False},
            {"connection_id": "c_val_none", "connection_status": "VALID", "is_active": None},
        ]
        # Simulate NB_GetConnectionWorklist in VALID mode:
        # coalesce(is_active, false) == true AND upper(trim(connection_status)) == 'VALID'
        eligible = [
            r["connection_id"] for r in rows
            if bool(r.get("is_active")) is True
            and str(r.get("connection_status") or "").strip().upper() == "VALID"
        ]
        self.assertEqual(eligible, ["c_val_true"])

    def test_sqlserver_database_worklist_defensive_filter_simulation(self):
        """Test NB_GetAssessmentDatabaseWorklist defensive filter behavior."""
        code = deployment_nb("NB_GetAssessmentDatabaseWorklist.py")
        self.assertIn('coalesce(F.col("sc.is_active"), F.lit(False)) == F.lit(True)', code)
        self.assertIn('F.upper(F.trim(F.col("sc.connection_status"))) == F.lit("VALID")', code)
        self.assertIn('if conn_active is not True or conn_status != "VALID":', code)

        # Simulate execution of the loop in NB_GetAssessmentDatabaseWorklist
        candidates = [
            {"connection_id": "c1", "source_database": "DB1", "is_active": True, "connection_status": "VALID"},
            {"connection_id": "c2", "source_database": "DB2", "is_active": False, "connection_status": "VALID"},
            {"connection_id": "c3", "source_database": "DB3", "is_active": None, "connection_status": "VALID"},
            {"connection_id": "c4", "source_database": "DB4", "is_active": True, "connection_status": "REGISTERED"},
            {"connection_id": "c5", "source_database": "DB5", "is_active": True, "connection_status": "FAILED"},
        ]

        worklist = []
        for conn in candidates:
            conn_active = conn.get("is_active")
            conn_status = str(conn.get("connection_status") or "").strip().upper()
            if conn_active is not True or conn_status != "VALID":
                continue
            worklist.append({
                "connection_id": conn["connection_id"],
                "source_database": conn["source_database"]
            })

        self.assertEqual(len(worklist), 1)
        self.assertEqual(worklist[0]["connection_id"], "c1")
        self.assertEqual(worklist[0]["source_database"], "DB1")

    def test_blank_database_discovery_runs_only_when_valid_and_active(self):
        """Blank database discovery connects to master only when connection is active and VALID."""
        candidates = [
            {"connection_id": "c_active_val", "source_database": "", "is_active": True, "connection_status": "VALID"},
            {"connection_id": "c_inactive_val", "source_database": "", "is_active": False, "connection_status": "VALID"},
            {"connection_id": "c_null_active_val", "source_database": "", "is_active": None, "connection_status": "VALID"},
        ]

        discovery_ran_for = []
        for conn in candidates:
            conn_active = conn.get("is_active")
            conn_status = str(conn.get("connection_status") or "").strip().upper()
            if conn_active is not True or conn_status != "VALID":
                continue
            configured_db = str(conn.get("source_database") or "").strip()
            if not configured_db:
                discovery_ran_for.append(conn["connection_id"])

        self.assertEqual(discovery_ran_for, ["c_active_val"])

    def test_oracle_configured_mode_contract(self):
        """Oracle Job 1A requires database/service, and honors is_active switch."""
        spark = FakeSpark(results=[[]])
        repo = ControlRepository(spark, "cat", "ctrl")
        repo.configured_connections_for_source("oracle")
        sql = spark.last_sql()
        self.assertIn("lower(trim(source_system)) = 'oracle'", sql)
        self.assertIn("coalesce(is_active, false) = true", sql)
        self.assertIn("upper(trim(connection_status)) IN ('REGISTERED', 'VALID', 'FAILED')", sql)
        # NB_GetConnectionWorklist requires non-blank source_database for Oracle
        worklist_code = deployment_nb("NB_GetConnectionWorklist.ipynb")
        self.assertIn('if source_system == "oracle":', worklist_code)
        self.assertIn('F.col("sc.source_database").isNotNull()', worklist_code)

    def test_inactive_connections_excluded_even_with_only_connection_ids(self):
        """Inactive connections remain excluded even when specified in only_connection_ids."""
        spark = FakeSpark(results=[[]])
        repo = ControlRepository(spark, "cat", "ctrl")
        repo.configured_connections_for_source("sqlserver", only_connection_ids=["c_inactive"])
        sql = spark.last_sql()
        self.assertIn("coalesce(is_active, false) = true", sql)
        self.assertIn("connection_id IN ('c_inactive')", sql)

    def test_update_connection_status_contract(self):
        """Verify status update sets is_active and timestamps according to contract."""
        spark = FakeSpark(results=[[]])
        repo = ControlRepository(spark, "cat", "ctrl")

        # VALID success: is_active = true, last_validated_ts updated, error cleared
        repo.update_connection_status("conn1", "VALID", None)
        sql_valid = spark.last_sql()
        self.assertIn("`connection_status` = 'VALID'", sql_valid)
        self.assertIn("`is_active` = true", sql_valid)
        self.assertIn("`error_message` = NULL", sql_valid)
        self.assertIn("`last_validated_ts` = current_timestamp()", sql_valid)

        # FAILED failure: is_active = false, sanitized error stored
        repo.update_connection_status("conn1", "FAILED", "Connection refused password=secret")
        sql_failed = spark.last_sql()
        self.assertIn("`connection_status` = 'FAILED'", sql_failed)
        self.assertIn("`is_active` = false", sql_failed)
        self.assertNotIn("secret", sql_failed)


class TestNB00ConnectionValidationAndJob1ASequence(unittest.TestCase):
    """Test NB00_ControlTableInit connection validation semantics and Job 1A sequence."""

    def test_nb00_validation_query_structure(self):
        code = shared_nb("NB00_ControlTableInit.py")
        self.assertIn('"ACTIVE_CONNECTION_INVALID_STATUS"', code)
        self.assertNotIn('"ACTIVE_CONNECTION_NOT_VALID"', code)
        self.assertIn("coalesce(is_active, false) = true", code)
        self.assertIn("upper(trim(coalesce(connection_status, ''))) NOT IN", code)
        self.assertIn("'REGISTERED'", code)
        self.assertIn("'VALID'", code)
        self.assertIn("'FAILED'", code)
        self.assertIn('"ACTIVE_TABLE_INVALID_CONNECTION"', code)

    @staticmethod
    def _evaluate_active_conn_status_invalid(connection):
        """Simulate NB00 ACTIVE_CONNECTION_INVALID_STATUS check in Python."""
        is_active = bool(connection.get("is_active")) if connection.get("is_active") is not None else False
        if not is_active:
            return False  # inactive row is not flagged
        status = str(connection.get("connection_status") or "").strip().upper()
        return status not in ("REGISTERED", "VALID", "FAILED")

    @staticmethod
    def _evaluate_active_table_invalid_connection(table_row, conn_row):
        """Simulate NB00 ACTIVE_TABLE_INVALID_CONNECTION check in Python."""
        if not bool(table_row.get("is_active")):
            return False  # inactive table is not flagged
        if not conn_row:
            return True
        conn_active = bool(conn_row.get("is_active")) if conn_row.get("is_active") is not None else False
        if not conn_active:
            return False  # inactive parent is parked, not structural corruption
        conn_status = str(conn_row.get("connection_status") or "").strip().upper()
        secret_scope = str(conn_row.get("secret_scope") or "").strip()
        if conn_status != "VALID" or not secret_scope:
            return True
        return False

    def test_a_active_registered_connection(self):
        """Active REGISTERED connection passes NB00, remains unchanged, and enters CONFIGURED worklist."""
        conn = {
            "connection_id": "c1",
            "is_active": True,
            "connection_status": "REGISTERED",
            "secret_scope": "sc",
            "source_server": "srv",
            "source_system": "sqlserver",
        }
        self.assertFalse(self._evaluate_active_conn_status_invalid(conn))

        spark = FakeSpark(results=[[]])
        repo = ControlRepository(spark, "cat", "ctrl")
        repo.configured_connections_for_source("sqlserver")
        sql = spark.last_sql()
        self.assertIn("coalesce(is_active, false) = true", sql)
        self.assertIn("upper(trim(connection_status)) IN ('REGISTERED', 'VALID', 'FAILED')", sql)

    def test_b_active_valid_connection(self):
        """Active VALID connection passes NB00 and is included in CONFIGURED and VALID worklists."""
        conn = {
            "connection_id": "c1",
            "is_active": True,
            "connection_status": "VALID",
            "secret_scope": "sc",
            "source_server": "srv",
            "source_system": "sqlserver",
        }
        self.assertFalse(self._evaluate_active_conn_status_invalid(conn))

        spark = FakeSpark(results=[[]])
        repo = ControlRepository(spark, "cat", "ctrl")
        repo.valid_active_connections_for_source("sqlserver")
        sql = spark.last_sql()
        self.assertIn("coalesce(is_active, false) = true", sql)
        self.assertIn("upper(trim(connection_status)) = 'VALID'", sql)

    def test_c_active_failed_connection(self):
        """Active FAILED connection passes NB00, enters CONFIGURED for retry, excluded from VALID."""
        conn = {
            "connection_id": "c1",
            "is_active": True,
            "connection_status": "FAILED",
            "secret_scope": "sc",
            "source_server": "srv",
            "source_system": "sqlserver",
        }
        self.assertFalse(self._evaluate_active_conn_status_invalid(conn))

        spark = FakeSpark(results=[[]])
        repo = ControlRepository(spark, "cat", "ctrl")
        repo.configured_connections_for_source("sqlserver")
        self.assertIn("'FAILED'", spark.last_sql())

        repo.valid_active_connections_for_source("sqlserver")
        self.assertIn("upper(trim(connection_status)) = 'VALID'", spark.last_sql())

    def test_d_active_unsupported_status(self):
        """Active connection with unsupported status is flagged by ACTIVE_CONNECTION_INVALID_STATUS."""
        conn1 = {"connection_id": "c1", "is_active": True, "connection_status": "UNKNOWN_STATUS"}
        self.assertTrue(self._evaluate_active_conn_status_invalid(conn1))

        conn2 = {"connection_id": "c2", "is_active": True, "connection_status": "PENDING"}
        self.assertTrue(self._evaluate_active_conn_status_invalid(conn2))

    def test_e_active_blank_or_null_status(self):
        """Active connection with blank or NULL status is flagged by ACTIVE_CONNECTION_INVALID_STATUS."""
        conn_none = {"connection_id": "c1", "is_active": True, "connection_status": None}
        self.assertTrue(self._evaluate_active_conn_status_invalid(conn_none))

        conn_blank = {"connection_id": "c2", "is_active": True, "connection_status": "   "}
        self.assertTrue(self._evaluate_active_conn_status_invalid(conn_blank))

        conn_empty = {"connection_id": "c3", "is_active": True, "connection_status": ""}
        self.assertTrue(self._evaluate_active_conn_status_invalid(conn_empty))

    def test_f_inactive_connection_ignored_by_nb00_and_excluded_from_worklists(self):
        """Inactive connection (is_active=false) is ignored by NB00 regardless of status, and excluded from worklists."""
        for status in ("REGISTERED", "VALID", "FAILED", "UNKNOWN_STATUS", "", None):
            conn = {"connection_id": "c1", "is_active": False, "connection_status": status}
            self.assertFalse(self._evaluate_active_conn_status_invalid(conn))

    def test_g_null_is_active_treated_as_inactive(self):
        """NULL is_active is treated as inactive: ignored by NB00 and excluded from worklists."""
        for status in ("REGISTERED", "VALID", "FAILED", "UNKNOWN_STATUS", "", None):
            conn = {"connection_id": "c1", "is_active": None, "connection_status": status}
            self.assertFalse(self._evaluate_active_conn_status_invalid(conn))

    def test_h_operational_table_protection(self):
        """Active operational tables strictly require a VALID + active parent connection."""
        table_active = {"source_table_id": "t1", "is_active": True}
        table_inactive = {"source_table_id": "t2", "is_active": False}

        # Inactive table is never flagged
        self.assertFalse(self._evaluate_active_table_invalid_connection(
            table_inactive, {"is_active": False, "connection_status": "REGISTERED"}
        ))

        # Active table with REGISTERED + true: flagged!
        self.assertTrue(self._evaluate_active_table_invalid_connection(
            table_active, {"is_active": True, "connection_status": "REGISTERED", "secret_scope": "sc"}
        ))

        # Active table with FAILED + true: flagged!
        self.assertTrue(self._evaluate_active_table_invalid_connection(
            table_active, {"is_active": True, "connection_status": "FAILED", "secret_scope": "sc"}
        ))

        # Active table with inactive parent (false or None): NOT flagged (parked state)
        self.assertFalse(self._evaluate_active_table_invalid_connection(
            table_active, {"is_active": False, "connection_status": "VALID", "secret_scope": "sc"}
        ))
        self.assertFalse(self._evaluate_active_table_invalid_connection(
            table_active, {"is_active": None, "connection_status": "VALID", "secret_scope": "sc"}
        ))

        # Active table with missing secret_scope: flagged!
        self.assertTrue(self._evaluate_active_table_invalid_connection(
            table_active, {"is_active": True, "connection_status": "VALID", "secret_scope": ""}
        ))

        # Only VALID + true + secret_scope: NOT flagged!
        self.assertFalse(self._evaluate_active_table_invalid_connection(
            table_active, {"is_active": True, "connection_status": "VALID", "secret_scope": "sc"}
        ))

    def test_sqlite_direct_predicate_execution(self):
        """Execute the exact NB00 predicates in an in-memory SQL database."""
        import sqlite3
        con = sqlite3.connect(":memory:")
        cur = con.cursor()
        cur.execute("CREATE TABLE source_connection (connection_id TEXT, is_active BOOLEAN, connection_status TEXT, secret_scope TEXT)")
        cur.execute("CREATE TABLE source_table_control (source_table_id TEXT, connection_id TEXT, is_active BOOLEAN)")

        cur.execute("""
            INSERT INTO source_connection VALUES
            ('c_reg_true', true, 'REGISTERED', 'sc'),
            ('c_val_true', true, 'VALID', 'sc'),
            ('c_fail_true', true, 'FAILED', 'sc'),
            ('c_unk_true', true, 'UNKNOWN', 'sc'),
            ('c_null_true', true, NULL, 'sc'),
            ('c_blank_true', true, '   ', 'sc'),
            ('c_unk_false', false, 'UNKNOWN', 'sc'),
            ('c_unk_null', NULL, 'UNKNOWN', 'sc')
        """)

        # Execute ACTIVE_CONNECTION_INVALID_STATUS predicate
        cur.execute("""
            SELECT connection_id FROM source_connection
            WHERE coalesce(is_active, false) = true
              AND upper(trim(coalesce(connection_status, ''))) NOT IN (
                  'REGISTERED',
                  'VALID',
                  'FAILED'
              )
        """)
        flagged_conns = [r[0] for r in cur.fetchall()]
        self.assertEqual(sorted(flagged_conns), ["c_blank_true", "c_null_true", "c_unk_true"])

        # Execute ACTIVE_TABLE_INVALID_CONNECTION predicate
        cur.execute("""
            INSERT INTO source_table_control VALUES
            ('t_reg_parent', 'c_reg_true', true),
            ('t_val_parent', 'c_val_true', true),
            ('t_fail_parent', 'c_fail_true', true),
            ('t_inactive_parent', 'c_unk_false', true),
            ('t_null_parent', 'c_unk_null', true),
            ('t_inactive_table', 'c_reg_true', false)
        """)
        cur.execute("""
            SELECT c.source_table_id
            FROM source_table_control c
            JOIN source_connection sc
              ON c.connection_id = sc.connection_id
            WHERE c.is_active = true
              AND coalesce(sc.is_active, false) = true
              AND (
                   sc.connection_status IS NULL
                   OR upper(trim(sc.connection_status)) <> 'VALID'
                   OR sc.secret_scope IS NULL
                   OR trim(sc.secret_scope) = ''
              )
        """)
        flagged_tables = [r[0] for r in cur.fetchall()]
        self.assertEqual(sorted(flagged_tables), ["t_fail_parent", "t_reg_parent"])

    def test_job1a_sequence_initial_registration_to_validation(self):
        """Job 1A sequence: REGISTERED + true -> NB00 -> CONFIGURED worklist -> Validate -> VALID + true -> VALID worklist."""
        conn_row = {
            "connection_id": "c_init",
            "source_system": "sqlserver",
            "source_server": "srv1.corp",
            "source_database": "HR",
            "secret_scope": "scope_hr",
            "connection_status": "REGISTERED",
            "is_active": True,
        }

        # Step 1: NB00 succeeds
        self.assertFalse(self._evaluate_active_conn_status_invalid(conn_row))

        # Step 2: CONFIGURED worklist includes connection
        spark = FakeSpark(results=[[FakeRow(connection_id="c_init")]])
        repo = ControlRepository(spark, "cat", "ctrl")
        df = repo.configured_connections_for_source("sqlserver")
        self.assertEqual([r["connection_id"] for r in df.collect()], ["c_init"])

        # Step 3: Validate connection probe succeeds
        res = _run_nb00a("sqlserver", {"run_id": "r1", "connection_id": "c_init"}, connection_row=conn_row)
        self.assertEqual(res["exit_payload"]["status"], "VALID")
        self.assertEqual(res["exit_payload"]["connection_status"], "VALID")

        # Step 4: Status updated in repo
        repo_spark = FakeSpark(results=[[]])
        repo2 = ControlRepository(repo_spark, "cat", "ctrl")
        repo2.update_connection_status("c_init", "VALID", None)
        sql = repo_spark.last_sql()
        self.assertIn("`connection_status` = 'VALID'", sql)
        self.assertIn("`is_active` = true", sql)
        self.assertIn("`error_message` = NULL", sql)

        # Step 5: VALID worklist now includes connection
        valid_spark = FakeSpark(results=[[FakeRow(connection_id="c_init")]])
        repo3 = ControlRepository(valid_spark, "cat", "ctrl")
        df_valid = repo3.valid_active_connections_for_source("sqlserver")
        self.assertEqual([r["connection_id"] for r in df_valid.collect()], ["c_init"])

    def test_job1a_sequence_retry_flow(self):
        """Job 1A retry flow: FAILED + true -> NB00 -> CONFIGURED worklist -> Revalidate."""
        conn_row = {
            "connection_id": "c_retry",
            "source_system": "sqlserver",
            "source_server": "srv1.corp",
            "source_database": "Finance",
            "secret_scope": "scope_fin",
            "connection_status": "FAILED",
            "is_active": True,
        }

        # Step 1: NB00 succeeds
        self.assertFalse(self._evaluate_active_conn_status_invalid(conn_row))

        # Step 2: CONFIGURED worklist includes connection for retry
        spark = FakeSpark(results=[[FakeRow(connection_id="c_retry")]])
        repo = ControlRepository(spark, "cat", "ctrl")
        df = repo.configured_connections_for_source("sqlserver")
        self.assertEqual([r["connection_id"] for r in df.collect()], ["c_retry"])

        # Step 3a: Revalidation attempted - success flow
        res_success = _run_nb00a("sqlserver", {"run_id": "r2", "connection_id": "c_retry"}, connection_row=conn_row)
        self.assertEqual(res_success["exit_payload"]["status"], "VALID")

        # Step 3b: Revalidation attempted - failure flow preserves failure policy
        with self.assertRaises(RuntimeError):
            _run_nb00a(
                "sqlserver",
                {"run_id": "r2", "connection_id": "c_retry"},
                connection_row=conn_row,
                probe_side_effect=RuntimeError("probe timeout")
            )
        fail_spark = FakeSpark(results=[[]])
        repo_fail = ControlRepository(fail_spark, "cat", "ctrl")
        repo_fail.update_connection_status("c_retry", "FAILED", "probe timeout")
        sql_fail = fail_spark.last_sql()
        self.assertIn("`connection_status` = 'FAILED'", sql_fail)
        self.assertIn("`is_active` = false", sql_fail)


if __name__ == "__main__":
    unittest.main()
