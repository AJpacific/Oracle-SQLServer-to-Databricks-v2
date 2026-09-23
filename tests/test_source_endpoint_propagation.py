"""
Unit tests proving source endpoint identity (source_server, source_database)
propagation across Job 1B control tables and notebooks.

Validates:
1. NB00 creates source_server and source_database in the 5 downstream tables.
2. NB00 additive _ensure_columns uses _SOURCE_ID_FULL for all 5 tables.
3. NB02 carries source_server and source_database into normalized_source_inventory.
4. NB03 carries source_server and source_database into resolved_column_mappings.
5. NB04 validates SQL Server mapping rows with nonblank source_database.
6. NB04 rejects SQL Server operational rows with blank source_database.
7. NB04 writes both identity fields into mapping_validation_results.
8. NB07 SELECT includes source_server and source_database before identity assertion.
9. NB07 writes both fields into table_load_decisions and review_queue.
10. Two SQL Server databases with identical schema.table remain isolated.
11. Oracle behavior and validations remain unchanged.
12. Shared notebooks contain no dialect branching (if source_system == 'sqlserver').
"""

import builtins
import os
import re
import sys
import unittest
from unittest.mock import MagicMock

# Mock Databricks notebook environment before importing _common
if not hasattr(builtins, "dbutils"):
    mock_dbutils = MagicMock()
    mock_dbutils.widgets.get.return_value = "dummy"
    builtins.dbutils = mock_dbutils
if not hasattr(builtins, "spark"):
    builtins.spark = MagicMock()

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.join(os.path.dirname(HERE), "src")
for p in (SRC, HERE, os.path.dirname(HERE)):
    if p not in sys.path:
        sys.path.insert(0, p)

from _nbsource import shared_nb
from source_adapters.factory import get_source_adapter
from source_identity import compute_source_table_id
from notebooks.shared._common import (
    assert_source_identity_match,
    assert_table_connection_match,
    resolve_effective_source_database,
)


class TestNB00EndpointIdentityPropagation(unittest.TestCase):
    """Proves NB00 DDL and additive migration include source endpoint identity."""

    FIVE_TABLES = (
        "normalized_source_inventory",
        "resolved_column_mappings",
        "mapping_validation_results",
        "table_load_decisions",
        "review_queue",
    )

    def setUp(self):
        self.code = shared_nb("NB00_ControlTableInit.py")

    def test_ddl_includes_source_server_and_source_database_in_five_tables(self):
        """1. NB00 creates source_server and source_database in the five tables."""
        for tbl in self.FIVE_TABLES:
            create_block = self.code.split(f"CREATE TABLE IF NOT EXISTS {{ctrl('{tbl}')}}", 1)[1]
            create_block = create_block.split("USING DELTA", 1)[0]
            self.assertIn("source_server STRING,", create_block, f"Missing source_server in {tbl} DDL")
            self.assertIn("source_database STRING,", create_block, f"Missing source_database in {tbl} DDL")

            # Verify placement: after source_system and before source_schema
            pos_system = create_block.index("source_system")
            pos_server = create_block.index("source_server")
            pos_database = create_block.index("source_database")
            pos_schema = create_block.index("source_schema")
            self.assertLess(pos_system, pos_server, f"source_server before source_system in {tbl}")
            self.assertLess(pos_server, pos_database, f"source_database before source_server in {tbl}")
            self.assertLess(pos_database, pos_schema, f"source_schema before source_database in {tbl}")

    def test_additive_ensure_columns_uses_source_id_full_for_five_tables(self):
        """2. The additive _ensure_columns path uses _SOURCE_ID_FULL for all five tables."""
        for tbl in self.FIVE_TABLES:
            pattern = rf'_ensure_columns\(\s*"{tbl}"\s*,\s*_SOURCE_ID_FULL'
            self.assertTrue(
                bool(re.search(pattern, self.code)),
                f"{tbl} must use _SOURCE_ID_FULL in _ensure_columns"
            )

        # reconciliation_results stays on _SOURCE_ID_ONLY
        self.assertTrue(
            bool(re.search(r'_ensure_columns\(\s*"reconciliation_results"\s*,\s*_SOURCE_ID_ONLY\s*\)', self.code)),
            "reconciliation_results must remain on _SOURCE_ID_ONLY"
        )

    def test_structural_downstream_check_configured_for_five_tables(self):
        """Structural validation checks missing SQL Server database across the five tables."""
        for code, tbl in (
            ("MISSING_SQLSERVER_NORMALIZED_SOURCE_DATABASE", "normalized_source_inventory"),
            ("MISSING_SQLSERVER_RESOLVED_MAPPING_SOURCE_DATABASE", "resolved_column_mappings"),
            ("MISSING_SQLSERVER_MAPPING_VALIDATION_SOURCE_DATABASE", "mapping_validation_results"),
            ("MISSING_SQLSERVER_TABLE_DECISION_SOURCE_DATABASE", "table_load_decisions"),
            ("MISSING_SQLSERVER_REVIEW_QUEUE_SOURCE_DATABASE", "review_queue"),
        ):
            self.assertIn(code, self.code)
            self.assertIn(tbl, self.code)

        # Reports safe counts and sets REPAIR_REQUIRED without failing NB00
        self.assertIn("REPAIR_REQUIRED", self.code)
        self.assertIn("_missing_sqlserver_database_downstream_count", self.code)


class TestNB02EndpointIdentityPropagation(unittest.TestCase):
    """Proves NB02 carries source endpoint identity from source_inventory to normalized."""

    def setUp(self):
        self.code = shared_nb("NB02_TypeNormalization.py")

    def test_out_append_includes_source_server_and_source_database(self):
        """3. NB02 carries source_server and source_database into normalized_source_inventory."""
        self.assertIn('r["source_server"], r["source_database"]', self.code)

        # Verify ordering in out.append
        out_idx = self.code.index("out.append((")
        out_block = self.code[out_idx:self.code.index("))", out_idx) + 2]
        self.assertIn("run_id, src_id, conn_id, src_system", out_block)
        self.assertIn('r["source_server"], r["source_database"]', out_block)
        self.assertIn('r["source_schema"], r["source_table"]', out_block)

        idx_sys = out_block.index("src_system")
        idx_server = out_block.index('r["source_server"]')
        idx_schema = out_block.index('r["source_schema"]')
        self.assertLess(idx_sys, idx_server)
        self.assertLess(idx_server, idx_schema)

    def test_normalized_schema_includes_endpoint_identity_fields(self):
        """normalized_schema StructType includes source_server and source_database."""
        schema_idx = self.code.index("normalized_schema = StructType([")
        schema_block = self.code[schema_idx:self.code.index("])", schema_idx) + 2]
        self.assertIn('StructField("source_server", StringType(), True)', schema_block)
        self.assertIn('StructField("source_database", StringType(), True)', schema_block)

        pos_sys = schema_block.index('"source_system"')
        pos_server = schema_block.index('"source_server"')
        pos_db = schema_block.index('"source_database"')
        pos_schema = schema_block.index('"source_schema"')
        self.assertLess(pos_sys, pos_server)
        self.assertLess(pos_server, pos_db)
        self.assertLess(pos_db, pos_schema)

    def test_idempotent_delete_and_replace_preserved(self):
        """NB02 preserves retry-safe exact scope delete."""
        self.assertIn("DELETE FROM {ctrl('normalized_source_inventory')}", self.code)
        self.assertIn("WHERE run_id =", self.code)
        self.assertIn("AND connection_id =", self.code)


class TestNB03EndpointIdentityPropagation(unittest.TestCase):
    """Proves NB03 carries source endpoint identity into resolved_column_mappings."""

    def setUp(self):
        self.code = shared_nb("NB03_MappingRulesGeneration.py")

    def test_mapped_append_includes_source_server_and_source_database(self):
        """4. NB03 carries both fields into resolved_column_mappings."""
        # Both success and blocked/exception paths must include source_server and source_database
        matches = re.findall(
            r'r\["source_server"\],\s*r\["source_database"\]',
            self.code
        )
        self.assertGreaterEqual(len(matches), 2, "Both mapped.append branches must propagate identity")

    def test_resolved_mapping_schema_includes_endpoint_identity_fields(self):
        """resolved_mapping_schema StructType includes source_server and source_database."""
        schema_idx = self.code.index("resolved_mapping_schema = StructType([")
        schema_block = self.code[schema_idx:self.code.index("])", schema_idx) + 2]
        self.assertIn('StructField("source_server", StringType(), True)', schema_block)
        self.assertIn('StructField("source_database", StringType(), True)', schema_block)

        pos_sys = schema_block.index('"source_system"')
        pos_server = schema_block.index('"source_server"')
        pos_db = schema_block.index('"source_database"')
        pos_schema = schema_block.index('"source_schema"')
        self.assertLess(pos_sys, pos_server)
        self.assertLess(pos_server, pos_db)
        self.assertLess(pos_db, pos_schema)

    def test_idempotent_delete_and_replace_preserved(self):
        """NB03 preserves retry-safe exact scope delete."""
        self.assertIn("DELETE FROM {ctrl('resolved_column_mappings')}", self.code)
        self.assertIn("WHERE run_id =", self.code)
        self.assertIn("AND connection_id =", self.code)


class TestNB04MappingValidationEndpointIdentity(unittest.TestCase):
    """Proves NB04 validates SQL Server mapping rows and rejects blank database."""

    def setUp(self):
        self.code = shared_nb("NB04_MappingValidation.py")

    def test_assert_source_identity_match_preserved(self):
        """NB04 keeps assert_source_identity_match(r, connection) unchanged."""
        self.assertIn("assert_source_identity_match(r, connection)", self.code)
        self.assertIn("assert_table_connection_match(r, connection_id)", self.code)

    def test_nb04_validates_sqlserver_mapping_row_with_nonblank_database(self):
        """5. NB04 can validate a SQL Server mapping row having a nonblank source_database."""
        # Multi-database discovery connection with blank source_database
        connection = {
            "connection_id": "conn_sql_multi",
            "source_system": "sqlserver",
            "source_server": "sqlprod01.corp",
            "source_database": "",
            "is_active": True,
        }
        # Mapping row carrying operational source_database from resolved_column_mappings
        mapping_row = {
            "run_id": "run_001",
            "source_table_id": "table_001",
            "connection_id": "conn_sql_multi",
            "source_system": "sqlserver",
            "source_server": "sqlprod01.corp",
            "source_database": "BillingDB",
            "source_schema": "dbo",
            "source_table": "invoices",
        }
        # Must succeed without error
        self.assertTrue(assert_source_identity_match(mapping_row, connection))

    def test_nb04_rejects_sqlserver_operational_row_with_blank_database(self):
        """6. NB04 still rejects SQL Server operational rows with a blank source_database."""
        connection = {
            "connection_id": "conn_sql_multi",
            "source_system": "sqlserver",
            "source_server": "sqlprod01.corp",
            "source_database": "",
            "is_active": True,
        }
        # Defective mapping row with blank source_database
        defective_row = {
            "run_id": "run_001",
            "source_table_id": "table_001",
            "connection_id": "conn_sql_multi",
            "source_system": "sqlserver",
            "source_server": "sqlprod01.corp",
            "source_database": "",
            "source_schema": "dbo",
            "source_table": "invoices",
        }
        with self.assertRaises(ValueError) as ctx:
            assert_source_identity_match(defective_row, connection)
        self.assertIn("SQL Server operational row requires nonblank source_database", str(ctx.exception))

    def test_nb04_propagates_identity_into_validation_results(self):
        """7. NB04 writes both identity fields into mapping_validation_results when findings exist."""
        # _add function includes source_server and source_database
        add_idx = self.code.index("def _add(severity, rule, message):")
        add_block = self.code[add_idx:add_idx + 300]
        self.assertIn('r["source_server"]', add_block)
        self.assertIn('r["source_database"]', add_block)

        # cols list includes source_server and source_database in required order
        cols_idx = self.code.index("cols = [")
        cols_block = self.code[cols_idx:self.code.index("]", cols_idx) + 1]
        self.assertIn('"source_server"', cols_block)
        self.assertIn('"source_database"', cols_block)

        pos_sys = cols_block.index('"source_system"')
        pos_server = cols_block.index('"source_server"')
        pos_db = cols_block.index('"source_database"')
        pos_schema = cols_block.index('"source_schema"')
        self.assertLess(pos_sys, pos_server)
        self.assertLess(pos_server, pos_db)
        self.assertLess(pos_db, pos_schema)

    def test_idempotent_delete_and_replace_preserved(self):
        """NB04 preserves retry-safe exact scope delete."""
        self.assertIn("DELETE FROM {ctrl('mapping_validation_results')}", self.code)
        self.assertIn("WHERE run_id =", self.code)
        self.assertIn("AND connection_id =", self.code)


class TestNB07TableDecisionEndpointPropagation(unittest.TestCase):
    """Proves NB07 SELECT includes endpoint identity and writes to decisions & review_queue."""

    def setUp(self):
        self.code = shared_nb("NB07_TableDecisionGeneration.py")

    def test_select_includes_source_server_and_source_database(self):
        """8. NB07 SELECT includes source_server and source_database before calling assert_source_identity_match()."""
        select_block = self.code.split("maps = spark.sql(", 1)[1].split(".collect()", 1)[0]
        self.assertIn("source_server", select_block)
        self.assertIn("source_database", select_block)
        self.assertIn("source_table_id", select_block)
        self.assertIn("connection_id", select_block)
        self.assertIn("source_system", select_block)

        # assert_source_identity_match called after SELECT
        self.assertIn("assert_source_identity_match(r, connection)", self.code)

    def test_writes_both_fields_into_table_decisions_and_review_queue(self):
        """9. NB07 writes both fields into table_load_decisions and review_queue."""
        # agg dictionary stores server and database
        self.assertIn('agg[key]["server"] = r["source_server"]', self.code)
        self.assertIn('agg[key]["database"] = r["source_database"]', self.code)

        # decisions.append includes c["server"], c["database"]
        self.assertIn('c["server"], c["database"]', self.code)

        # table_decisions_schema includes source_server and source_database
        schema_idx = self.code.index("table_decisions_schema = StructType([")
        schema_block = self.code[schema_idx:self.code.index("])", schema_idx) + 2]
        self.assertIn('StructField("source_server", StringType(), True)', schema_block)
        self.assertIn('StructField("source_database", StringType(), True)', schema_block)

        # pending select includes source_server and source_database
        pending_idx = self.code.index("pending = (")
        pending_block = self.code[pending_idx:self.code.index("if pending.count()", pending_idx)]
        self.assertIn('"source_server"', pending_block)
        self.assertIn('"source_database"', pending_block)

    def test_grouping_keyed_by_connection_id_and_source_table_id(self):
        """Decisions remain grouped strictly by connection_id + source_table_id."""
        self.assertIn('key = (r["connection_id"], r["source_table_id"])', self.code)


class TestDatabaseIsolationAndOracleContract(unittest.TestCase):
    """Proves two databases remain isolated and Oracle behavior is preserved."""

    def test_two_sqlserver_databases_with_same_table_remain_independent(self):
        """10. Two SQL Server databases containing same schema.table remain independent."""
        conn_id = "sql_conn_shared"
        server = "sqlcluster01"
        schema = "dbo"
        table = "customers"

        id_db1 = compute_source_table_id(conn_id, "sqlserver", server, "DatabaseA", schema, table)
        id_db2 = compute_source_table_id(conn_id, "sqlserver", server, "DatabaseB", schema, table)

        # IDs must be different because source_database is part of the v2 identity
        self.assertNotEqual(id_db1, id_db2)

        # Assertions succeed independently for each database
        connection = {
            "connection_id": conn_id,
            "source_system": "sqlserver",
            "source_server": server,
            "source_database": "",  # multi-database discovery
            "is_active": True,
        }
        row1 = {
            "connection_id": conn_id,
            "source_table_id": id_db1,
            "source_system": "sqlserver",
            "source_server": server,
            "source_database": "DatabaseA",
            "source_schema": schema,
            "source_table": table,
        }
        row2 = {
            "connection_id": conn_id,
            "source_table_id": id_db2,
            "source_system": "sqlserver",
            "source_server": server,
            "source_database": "DatabaseB",
            "source_schema": schema,
            "source_table": table,
        }
        self.assertTrue(assert_source_identity_match(row1, connection))
        self.assertTrue(assert_source_identity_match(row2, connection))

    def test_oracle_behavior_remains_unchanged(self):
        """11. Oracle behavior remains unchanged: database/service mandatory and must match."""
        oracle_conn = {
            "connection_id": "ora_conn_01",
            "source_system": "oracle",
            "source_server": "oradev01",
            "source_database": "ORCLPDB",
            "is_active": True,
        }
        # Matching operational row succeeds
        ora_row_valid = {
            "connection_id": "ora_conn_01",
            "source_system": "oracle",
            "source_server": "oradev01",
            "source_database": "ORCLPDB",
            "source_schema": "HR",
            "source_table": "EMPLOYEES",
        }
        self.assertTrue(assert_source_identity_match(ora_row_valid, oracle_conn))

        # Blank operational database in Oracle row fails
        ora_row_blank = {
            "connection_id": "ora_conn_01",
            "source_system": "oracle",
            "source_server": "oradev01",
            "source_database": "",
            "source_schema": "HR",
            "source_table": "EMPLOYEES",
        }
        with self.assertRaises(ValueError):
            assert_source_identity_match(ora_row_blank, oracle_conn)

        # Mismatched database in Oracle row fails
        ora_row_mismatch = {
            "connection_id": "ora_conn_01",
            "source_system": "oracle",
            "source_server": "oradev01",
            "source_database": "OTHERPDB",
            "source_schema": "HR",
            "source_table": "EMPLOYEES",
        }
        with self.assertRaises(ValueError):
            assert_source_identity_match(ora_row_mismatch, oracle_conn)

    def test_no_dialect_branching_in_shared_notebooks(self):
        """12. No shared notebook introduces source-specific branching such as: if source_system == 'sqlserver'."""
        shared_nbs = (
            "NB02_TypeNormalization.py",
            "NB03_MappingRulesGeneration.py",
            "NB04_MappingValidation.py",
            "NB07_TableDecisionGeneration.py",
            "NB08_TargetProvisioning.py",
        )
        for nb_name in shared_nbs:
            content = shared_nb(nb_name)
            # Check for forbidden source_system dialect branching
            self.assertNotIn('source_system == "sqlserver"', content.lower(), f"Branching found in {nb_name}")
            self.assertNotIn("source_system == 'sqlserver'", content.lower(), f"Branching found in {nb_name}")
            self.assertNotIn('source_system == "oracle"', content.lower(), f"Branching found in {nb_name}")
            self.assertNotIn("source_system == 'oracle'", content.lower(), f"Branching found in {nb_name}")


if __name__ == "__main__":
    unittest.main()
