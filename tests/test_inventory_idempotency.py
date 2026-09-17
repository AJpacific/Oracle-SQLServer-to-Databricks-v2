"""Pure and static coverage for retry-safe source inventory persistence."""

import os
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
SRC = os.path.join(ROOT, "src")
for path in (SRC, HERE, ROOT):
    if path not in sys.path:
        sys.path.insert(0, path)

import inventory_common as inv  # noqa: E402
from _nbsource import shared_nb, source_nb  # noqa: E402


def _record(column_name="ID", run_id="run-1", source_table_id="sid-1",
            connection_id="conn-1", source_system="oracle",
            source_schema="HR", source_table="EMPLOYEES"):
    values = {
        "run_id": run_id,
        "source_table_id": source_table_id,
        "connection_id": connection_id,
        "source_system": source_system,
        "source_server": "source.example",
        "source_database": "db",
        "source_schema": source_schema,
        "source_table": source_table,
        "column_name": column_name,
        "ordinal_position": 1,
        "is_nullable": "NO",
        "data_type": "NUMBER",
        "character_maximum_length": None,
        "numeric_precision": 10,
        "numeric_scale": 0,
        "datetime_precision": None,
        "is_identity": False,
        "is_computed": False,
        "is_hidden": False,
        "is_rowversion": False,
        "source_type_schema": None,
    }
    return tuple(values[field] for field in inv.INVENTORY_FIELDS)


class TestInventoryKeys(unittest.TestCase):
    def test_merge_key_contract(self):
        self.assertEqual(
            inv.INVENTORY_MERGE_KEYS,
            ("run_id", "source_table_id", "column_name"))

    def test_duplicate_incoming_key_is_reported(self):
        duplicate = _record()
        self.assertEqual(
            inv.find_duplicate_inventory_keys([duplicate, duplicate]),
            [("run-1", "sid-1", "ID")])

    def test_duplicate_error_identifies_full_key(self):
        duplicate = _record(column_name="PASSWORD_HASH")
        with self.assertRaises(ValueError) as context:
            inv.validate_inventory_batch([duplicate, duplicate])
        message = str(context.exception)
        self.assertIn("run_id='run-1'", message)
        self.assertIn("source_table_id='sid-1'", message)
        self.assertIn("column_name='PASSWORD_HASH'", message)

    def test_distinct_columns_validate(self):
        records = [_record("ID"), _record("NAME")]
        normalized = inv.validate_inventory_batch(records)
        self.assertEqual([item["column_name"] for item in normalized],
                         ["ID", "NAME"])

    def test_batch_must_be_one_run_and_table(self):
        with self.assertRaises(ValueError):
            inv.validate_inventory_batch([
                _record("ID", run_id="run-1"),
                _record("NAME", run_id="run-2"),
            ])
        with self.assertRaises(ValueError):
            inv.validate_inventory_batch([
                _record("ID", source_table_id="sid-1"),
                _record("NAME", source_table_id="sid-2"),
            ])

    def test_connection_and_identity_must_agree(self):
        with self.assertRaises(ValueError):
            inv.validate_inventory_batch([
                _record("ID", connection_id="conn-1"),
                _record("NAME", connection_id="conn-2"),
            ])
        with self.assertRaises(ValueError):
            inv.validate_inventory_batch([
                _record("ID", source_schema="HR"),
                _record("NAME", source_schema="SALES"),
            ])

    def test_missing_key_component_fails(self):
        for field in inv.INVENTORY_MERGE_KEYS:
            item = dict(zip(inv.INVENTORY_FIELDS, _record()))
            item[field] = ""
            with self.subTest(field=field), self.assertRaises(ValueError):
                inv.validate_inventory_batch([item])


class TestInventoryPersistenceWiring(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.common = shared_nb("_common.py")
        cls.block = cls.common.split("def persist_inventory_rows(rows):", 1)[1]
        cls.block = cls.block.split("def persist_sql_object_records", 1)[0]

    def test_validation_precedes_every_inventory_write(self):
        validate = self.block.index("inv_common.validate_inventory_batch(rows)")
        connection_check = self.block.index("prior_connections")
        delete = self.block.index("DELETE FROM")
        append = self.block.index('.mode("append")')
        self.assertLess(validate, connection_check)
        self.assertLess(connection_check, delete)
        self.assertLess(delete, append)

    def test_delete_is_exact_run_table_scope(self):
        delete = self.block.split("DELETE FROM", 1)[1].split('""")', 1)[0]
        self.assertIn("WHERE run_id =", delete)
        self.assertIn("AND source_table_id =", delete)
        self.assertNotIn("connection_id =", delete)

    def test_new_runs_and_other_tables_are_not_deleted(self):
        # Both identity parts appear in the predicate, so a different run or
        # source_table_id remains outside the exact replacement scope.
        self.assertIn("escape_string_literal(run_id)", self.block)
        self.assertIn("escape_string_literal(source_table_id)", self.block)

    def test_connection_rebinding_fails_before_delete(self):
        self.assertIn("FROM {ctrl_table('source_table_control')}", self.block)
        self.assertIn("control_connection_id != connection_id", self.block)
        self.assertIn("SELECT DISTINCT connection_id", self.block)
        self.assertIn("prior_connections != {connection_id}", self.block)
        self.assertLess(
            self.block.index("control_connection_id != connection_id"),
            self.block.index("DELETE FROM"))
        self.assertLess(
            self.block.index("prior_connections != {connection_id}"),
            self.block.index("DELETE FROM"))

    def test_same_run_retry_removes_dropped_columns(self):
        # Replacing A/B/C with A/B first removes the old complete scope; C is
        # therefore absent after the append of the new complete set.
        self.assertIn("Exact replacement removes stale columns", self.block)
        self.assertLess(self.block.index("DELETE FROM"),
                        self.block.index('.mode("append")'))

    def test_duplicate_batch_cannot_delete_existing_snapshot(self):
        self.assertLess(
            self.block.index("validate_inventory_batch"),
            self.block.index("DELETE FROM"))

    def test_source_notebooks_persist_before_inventoried_status(self):
        for source in ("oracle", "sqlserver"):
            code = source_nb(source, "NB01_SourceInventory.py")
            success = code.split("table_inventory_rows =", 1)[1]
            persist = success.index("persist_inventory_rows(table_inventory_rows)")
            update = success.index("repo.update_control(")
            self.assertLess(persist, update, source)
            self.assertIn("INVENTORY_FAILED", success)
            self.assertIn("failcls.sanitize_message(e)", success)

    def test_source_notebooks_persist_one_table_at_a_time(self):
        for source in ("oracle", "sqlserver"):
            code = source_nb(source, "NB01_SourceInventory.py")
            self.assertIn("table_inventory_rows = [", code, source)
            self.assertNotIn("inventory_rows.extend(", code, source)
            self.assertEqual(
                code.count("persist_inventory_rows(table_inventory_rows)"), 1,
                source)


class TestInventoryTaskStatus(unittest.TestCase):
    def _result_block(self, source):
        code = source_nb(source, "NB01_SourceInventory.py")
        return code.split("inventory_result =", 1)[1]

    def test_failed_inventory_raises_after_processing(self):
        for source in ("oracle", "sqlserver"):
            block = self._result_block(source)
            self.assertIn("if failed:", block, source)
            self.assertIn("raise RuntimeError(", block, source)
            self.assertLess(block.index("print(json.dumps(inventory_result))"),
                            block.index("raise RuntimeError("), source)
            self.assertLess(block.index("raise RuntimeError("),
                            block.index("dbutils.notebook.exit("), source)

    def test_successful_inventory_exits_succeeded(self):
        for source in ("oracle", "sqlserver"):
            block = self._result_block(source)
            self.assertIn('"status": "FAILED" if failed else "SUCCEEDED"',
                          block, source)
            self.assertIn('"business_status": "PARTIAL" if failed and succeeded',
                          block, source)
            self.assertTrue(block.rstrip().endswith(
                "dbutils.notebook.exit(json.dumps(inventory_result))"), source)

    def test_source_output_contracts_are_identical(self):
        blocks = [self._result_block(source)
                  for source in ("oracle", "sqlserver")]
        self.assertEqual(blocks[0], blocks[1])
        for key in ("status", "execution_status", "business_status", "run_id",
                    "connection_id", "source_system", "tables_succeeded",
                    "tables_failed", "columns_written", "tables", "failed",
                    "columns"):
            self.assertIn(f'"{key}"', blocks[0])


if __name__ == "__main__":
    unittest.main()