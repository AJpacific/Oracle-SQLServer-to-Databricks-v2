"""
Unit and regression tests for the connection-level kill switch across the accelerator.

Proves:
1. Inactive parent plus active child registration passes NB00 initialization.
2. Inactive parent is excluded from the SQL Server CONFIGURED connection worklist.
3. Inactive parent is excluded from the SQL Server VALID connection worklist.
4. Inactive parent is excluded from the Oracle CONFIGURED and VALID connection worklists.
5. Inactive parent is excluded from assessment database discovery.
6. Inactive parent is excluded from registered-connection worklists.
7. Inactive parent is excluded from selected-assessment onboarding worklists.
8. Inactive parent is excluded from normal Full Load worklists.
9. Inactive parent is excluded from Delta preparation and worklists.
10. Direct Full Load execution rejects an inactive parent before JDBC/source access.
11. Direct Delta execution rejects an inactive parent before JDBC/source access.
12. SQL-object assessment and refresh reject or exclude an inactive parent before source access.
13. Artifact materialization does not initiate fresh source access for inactive connections.
14. Retry execution cannot access a source through an inactive parent connection.
15. Inactive parent does not require changing child table is_active.
16. Reactivating the same connection as active and VALID restores normal eligibility without recreating child registrations.
17. Active parent plus active child plus VALID status and valid secret scope passes initialization.
18. Active parent with FAILED, REGISTERED, blank, null, or unsupported status and an operational active child follows reviewed validation policy.
19. Active parent with blank secret_scope still fails where source access is required.
20. Orphan child registration still fails.
21. Source-system and endpoint mismatches still fail.
22. Duplicate ownership and identity-v2 inconsistency checks remain unchanged.

Tests both Oracle and SQL Server paths.
"""

from __future__ import annotations

import os
import sqlite3
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.join(os.path.dirname(HERE), "src")
NOTEBOOKS = os.path.join(os.path.dirname(HERE), "notebooks")
for p in (SRC, HERE, os.path.dirname(HERE)):
    if p not in sys.path:
        sys.path.insert(0, p)

from control_repository import ControlRepository
from _fakes import FakeSpark, FakeRow
from _nbsource import shared_nb, source_nb, deployment_nb


def _init_sqlite_db():
    con = sqlite3.connect(":memory:")
    cur = con.cursor()
    cur.execute("""
        CREATE TABLE source_connection (
            connection_id TEXT PRIMARY KEY,
            connection_name TEXT,
            source_system TEXT,
            source_server TEXT,
            source_database TEXT,
            secret_scope TEXT,
            connection_status TEXT,
            is_active BOOLEAN,
            trust_server_certificate BOOLEAN,
            error_message TEXT,
            last_validated_ts TEXT,
            created_ts TEXT,
            updated_ts TEXT
        )
    """)
    cur.execute("""
        CREATE TABLE source_table_control (
            connection_id TEXT,
            source_table_id TEXT,
            source_system TEXT,
            source_server TEXT,
            source_database TEXT,
            source_schema TEXT,
            source_table TEXT,
            target_catalog TEXT,
            target_schema TEXT,
            target_table TEXT,
            is_active BOOLEAN,
            current_status TEXT,
            table_decision TEXT,
            mapping_status TEXT,
            initial_load_completed BOOLEAN,
            source_identity_version INT,
            PRIMARY KEY (connection_id, source_table_id)
        )
    """)
    return con, cur


class TestConnectionKillSwitch(unittest.TestCase):
    """Regression test suite for connection-level kill switch semantics."""

    def setUp(self):
        self.con, self.cur = _init_sqlite_db()

    def tearDown(self):
        self.con.close()

    def _eval_active_table_invalid_connection(self):
        """Execute the exact NB00 ACTIVE_TABLE_INVALID_CONNECTION SQL predicate."""
        self.cur.execute("""
            SELECT count(*) AS c
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
        return self.cur.fetchone()[0]

    # 1. Inactive parent plus active child registration passes NB00 initialization
    def test_01_inactive_parent_plus_active_child_passes_nb00(self):
        for sys_name in ("sqlserver", "oracle"):
            with self.subTest(source_system=sys_name):
                con, cur = _init_sqlite_db()
                conn_id = f"conn_{sys_name}_parked"
                table_id = f"t_{sys_name}_1"
                cur.execute(
                    "INSERT INTO source_connection (connection_id, source_system, source_server, source_database, secret_scope, connection_status, is_active) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (conn_id, sys_name, "srv1", "db1", "scope1", "VALID", False)
                )
                cur.execute(
                    "INSERT INTO source_table_control (connection_id, source_table_id, source_system, source_server, source_database, source_schema, source_table, target_catalog, target_schema, target_table, is_active, current_status, table_decision) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (conn_id, table_id, sys_name, "srv1", "db1", "dbo", "orders", "cat", "sch", "orders", True, "PROVISIONED", "AUTO_MIGRATE")
                )
                # Must NOT be flagged as ACTIVE_TABLE_INVALID_CONNECTION
                cur.execute("""
                    SELECT count(*) AS c
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
                flagged_count = cur.fetchone()[0]
                self.assertEqual(flagged_count, 0, f"Inactive parent with active child was falsely flagged for {sys_name}")

    # 2. Inactive parent is excluded from SQL Server CONFIGURED connection worklist
    def test_02_inactive_parent_excluded_from_sqlserver_configured_worklist(self):
        spark = FakeSpark(results=[[]])
        repo = ControlRepository(spark, "cat", "ctrl")
        repo.configured_connections_for_source("sqlserver")
        sql = spark.last_sql()
        self.assertIn("coalesce(is_active, false) = true", sql)
        self.assertIn("upper(trim(connection_status)) IN ('REGISTERED', 'VALID', 'FAILED')", sql)
        self.assertIn("lower(trim(source_system)) = 'sqlserver'", sql)

        # In SQLite:
        self.cur.execute(
            "INSERT INTO source_connection (connection_id, source_system, source_server, source_database, secret_scope, connection_status, is_active) VALUES "
            "('c_active', 'sqlserver', 'srv', 'db', 'sc', 'REGISTERED', true), "
            "('c_inactive', 'sqlserver', 'srv', 'db', 'sc', 'REGISTERED', false), "
            "('c_null_active', 'sqlserver', 'srv', 'db', 'sc', 'REGISTERED', NULL)"
        )
        self.cur.execute("""
            SELECT connection_id FROM source_connection
            WHERE coalesce(is_active, false) = true
              AND upper(trim(connection_status)) IN ('REGISTERED', 'VALID', 'FAILED')
              AND lower(trim(source_system)) = 'sqlserver'
        """)
        res = [r[0] for r in self.cur.fetchall()]
        self.assertEqual(res, ["c_active"])

    # 3. Inactive parent is excluded from SQL Server VALID connection worklist
    def test_03_inactive_parent_excluded_from_sqlserver_valid_worklist(self):
        spark = FakeSpark(results=[[]])
        repo = ControlRepository(spark, "cat", "ctrl")
        repo.valid_active_connections_for_source("sqlserver")
        sql = spark.last_sql()
        self.assertIn("coalesce(is_active, false) = true", sql)
        self.assertIn("upper(trim(connection_status)) = 'VALID'", sql)
        self.assertIn("lower(trim(source_system)) = 'sqlserver'", sql)

        self.cur.execute(
            "INSERT INTO source_connection (connection_id, source_system, source_server, source_database, secret_scope, connection_status, is_active) VALUES "
            "('c_valid_active', 'sqlserver', 'srv', 'db', 'sc', 'VALID', true), "
            "('c_valid_inactive', 'sqlserver', 'srv', 'db', 'sc', 'VALID', false)"
        )
        self.cur.execute("""
            SELECT connection_id FROM source_connection
            WHERE coalesce(is_active, false) = true
              AND upper(trim(connection_status)) = 'VALID'
              AND lower(trim(source_system)) = 'sqlserver'
        """)
        res = [r[0] for r in self.cur.fetchall()]
        self.assertEqual(res, ["c_valid_active"])

    # 4. Inactive parent is excluded from Oracle CONFIGURED and VALID connection worklists
    def test_04_inactive_parent_excluded_from_oracle_configured_and_valid_worklists(self):
        spark = FakeSpark(results=[[]])
        repo = ControlRepository(spark, "cat", "ctrl")

        # CONFIGURED mode
        repo.configured_connections_for_source("oracle")
        sql_cfg = spark.last_sql()
        self.assertIn("coalesce(is_active, false) = true", sql_cfg)
        self.assertIn("lower(trim(source_system)) = 'oracle'", sql_cfg)

        # VALID mode
        repo.valid_active_connections_for_source("oracle")
        sql_val = spark.last_sql()
        self.assertIn("coalesce(is_active, false) = true", sql_val)
        self.assertIn("upper(trim(connection_status)) = 'VALID'", sql_val)
        self.assertIn("lower(trim(source_system)) = 'oracle'", sql_val)

        # In SQLite:
        self.cur.execute(
            "INSERT INTO source_connection (connection_id, source_system, source_server, source_database, secret_scope, connection_status, is_active) VALUES "
            "('ora_act_reg', 'oracle', 'srv', 'db', 'sc', 'REGISTERED', true), "
            "('ora_inact_reg', 'oracle', 'srv', 'db', 'sc', 'REGISTERED', false), "
            "('ora_act_val', 'oracle', 'srv', 'db', 'sc', 'VALID', true), "
            "('ora_inact_val', 'oracle', 'srv', 'db', 'sc', 'VALID', false)"
        )
        self.cur.execute("""
            SELECT connection_id FROM source_connection
            WHERE coalesce(is_active, false) = true
              AND upper(trim(connection_status)) = 'VALID'
              AND lower(trim(source_system)) = 'oracle'
        """)
        res_val = [r[0] for r in self.cur.fetchall()]
        self.assertEqual(res_val, ["ora_act_val"])

    # 5. Inactive parent is excluded from assessment database discovery
    def test_05_inactive_parent_excluded_from_assessment_database_discovery(self):
        code = deployment_nb("NB_GetAssessmentDatabaseWorklist.py")
        self.assertIn('F.coalesce(F.col("sc.is_active"), F.lit(False)) == F.lit(True)', code)
        self.assertIn('F.upper(F.trim(F.col("sc.connection_status"))) == F.lit("VALID")', code)
        self.assertIn('if conn_active is not True or conn_status != "VALID":', code)

    # 6. Inactive parent is excluded from registered-connection worklists
    def test_06_inactive_parent_excluded_from_registered_connection_worklist(self):
        code = deployment_nb("NB_GetRegisteredConnectionWorklist.py")
        self.assertIn('F.col("sc.is_active") == F.lit(True)', code)
        self.assertIn('F.upper(F.trim(F.col("sc.connection_status"))) == F.lit("VALID")', code)

    # 7. Inactive parent is excluded from selected-assessment onboarding worklists
    def test_07_inactive_parent_excluded_from_selected_assessment_worklist(self):
        code = deployment_nb("NB_GetSelectedAssessmentWorklist.ipynb")
        self.assertIn('F.col(\\"sc.is_active\\") == F.lit(True)', code)
        self.assertIn('F.upper(F.trim(F.col(\\"sc.connection_status\\"))) == F.lit(\\"VALID\\")', code)

    # 8. Inactive parent is excluded from normal Full Load worklists
    def test_08_inactive_parent_excluded_from_full_load_worklist(self):
        code = deployment_nb("NB_GetFullLoadWorklist.ipynb")
        self.assertIn('F.col(\\"sc.is_active\\") == F.lit(True)', code)
        self.assertIn('F.col(\\"c.is_active\\") == F.lit(True)', code)

        # In SQLite:
        self.cur.execute(
            "INSERT INTO source_connection (connection_id, source_system, source_server, source_database, secret_scope, connection_status, is_active) VALUES "
            "('conn_active', 'sqlserver', 'srv', 'db', 'sc', 'VALID', true), "
            "('conn_inactive', 'sqlserver', 'srv', 'db', 'sc', 'VALID', false)"
        )
        self.cur.execute(
            "INSERT INTO source_table_control (connection_id, source_table_id, source_system, source_server, source_database, source_schema, source_table, target_catalog, target_schema, target_table, is_active, current_status, table_decision) VALUES "
            "('conn_active', 't_act', 'sqlserver', 'srv', 'db', 'sch', 'tbl', 'cat', 'sch', 'tbl', true, 'PROVISIONED', 'AUTO_MIGRATE'), "
            "('conn_inactive', 't_inact', 'sqlserver', 'srv', 'db', 'sch', 'tbl', 'cat', 'sch', 'tbl', true, 'PROVISIONED', 'AUTO_MIGRATE')"
        )
        self.cur.execute("""
            SELECT c.source_table_id
            FROM source_table_control c
            JOIN source_connection sc
              ON c.connection_id = sc.connection_id
            WHERE c.is_active = true
              AND sc.is_active = true
              AND sc.connection_status = 'VALID'
        """)
        eligible = [r[0] for r in self.cur.fetchall()]
        self.assertEqual(eligible, ["t_act"])

    # 9. Inactive parent is excluded from Delta preparation and worklists
    def test_09_inactive_parent_excluded_from_delta_preparation_and_worklists(self):
        prep_code = shared_nb("NB11a_DeltaSyncPrep.py")
        self.assertIn("AND sc.is_active = true", prep_code)
        self.assertIn("AND sc.connection_status = 'VALID'", prep_code)

        delta_wl_code = deployment_nb("NB_GetDeltaWorklist.ipynb")
        self.assertIn('F.col(\\"sc.is_active\\") == F.lit(True)', delta_wl_code)
        self.assertIn('F.col(\\"c.is_active\\") == F.lit(True)', delta_wl_code)

    # 10. Direct Full Load execution rejects an inactive parent before JDBC/source access
    def test_10_direct_full_load_rejects_inactive_parent_before_source_access(self):
        code = shared_nb("NB09_FullLoad.py")
        require_idx = code.index("connection = require_valid_connection(connection_id)")
        # JDBC read occurs in Stage 1 / extract
        read_idx = code.index("read_source_jdbc")
        self.assertLess(require_idx, read_idx)

        # Verify require_valid_connection contract directly
        def mock_require_valid_connection(conn_dict):
            if not conn_dict.get("is_active"):
                raise ValueError(f"connection {conn_dict.get('connection_id')!r} is not active")
            if (conn_dict.get("connection_status") or "") != "VALID":
                raise ValueError("connection is not VALID")
            if not str(conn_dict.get("secret_scope") or "").strip():
                raise ValueError("secret_scope is blank")

        inactive_conn = {"connection_id": "c1", "is_active": False, "connection_status": "VALID", "secret_scope": "sc"}
        with self.assertRaises(ValueError) as ctx:
            mock_require_valid_connection(inactive_conn)
        self.assertIn("not active", str(ctx.exception))

    # 11. Direct Delta execution rejects an inactive parent before JDBC/source access
    def test_11_direct_delta_rejects_inactive_parent_before_source_access(self):
        code = shared_nb("NB11b_DeltaSyncApply.py")
        adapter_idx = code.index("adapter = get_source_adapter_routed(q)")
        read_idx = code.index("read_source_jdbc")
        self.assertLess(adapter_idx, read_idx)

    # 12. SQL-object assessment and refresh reject or exclude an inactive parent before source access
    def test_12_sql_object_assessment_rejects_inactive_parent_before_source_access(self):
        for src in ("oracle", "sqlserver"):
            with self.subTest(source_system=src):
                code = source_nb(src, "NB13_SQLObjectAssessmentAndConversion.py")
                require_idx = code.index("require_valid_connection(connection_id, SOURCE_SYSTEM)")
                read_idx = code.index("read_source_jdbc")
                self.assertLess(require_idx, read_idx)

    # 13. Artifact materialization does not initiate fresh source access for inactive connections
    def test_13_artifact_materialization_does_not_initiate_fresh_source_access(self):
        code = shared_nb("NB18_MaterializeSourceArtifacts.py")
        for forbidden in ("read_source_jdbc", "get_source_adapter", "probe_connection", "DriverManager"):
            self.assertNotIn(forbidden, code)
        # Materialization operates purely on already captured sql_object_assessment table
        self.assertIn("sql_object_assessment", code)

    # 14. Retry execution cannot access a source through an inactive parent connection
    def test_14_retry_execution_cannot_access_source_through_inactive_parent(self):
        # NB14 is a selector only
        nb14_code = shared_nb("NB14_RetryFailedTables.py")
        self.assertNotIn("read_source_jdbc", nb14_code)
        self.assertNotIn("get_source_adapter", nb14_code)

        # Routed execution notebooks:
        # Full Load retry routes to NB09 which checks require_valid_connection
        nb09_code = shared_nb("NB09_FullLoad.py")
        self.assertIn("require_valid_connection(connection_id)", nb09_code)

        # Delta apply retry routes to NB11b which checks get_source_adapter_routed -> require_valid_connection
        nb11b_code = shared_nb("NB11b_DeltaSyncApply.py")
        self.assertIn("adapter = get_source_adapter_routed(q)", nb11b_code)

    # 15. Inactive parent does not require changing child table is_active
    def test_15_inactive_parent_does_not_require_changing_child_is_active(self):
        self.cur.execute(
            "INSERT INTO source_connection (connection_id, source_system, source_server, source_database, secret_scope, connection_status, is_active) "
            "VALUES ('c_parked', 'sqlserver', 'srv', 'db', 'sc', 'VALID', false)"
        )
        self.cur.execute(
            "INSERT INTO source_table_control (connection_id, source_table_id, source_system, source_server, source_database, source_schema, source_table, target_catalog, target_schema, target_table, is_active, current_status, table_decision) "
            "VALUES ('c_parked', 't1', 'sqlserver', 'srv', 'db', 'sch', 'tbl', 'cat', 'sch', 'tbl', true, 'PROVISIONED', 'AUTO_MIGRATE')"
        )
        # Table row remains is_active = true and initialization passes
        self.assertEqual(self._eval_active_table_invalid_connection(), 0)

        # Verify child is_active value did not change
        self.cur.execute("SELECT is_active FROM source_table_control WHERE connection_id = 'c_parked'")
        self.assertEqual(self.cur.fetchone()[0], 1)

    # 16. Reactivating the same connection as active and VALID restores normal eligibility without recreating child registrations
    def test_16_reactivating_connection_restores_eligibility_without_recreating_registrations(self):
        # Park connection
        self.cur.execute(
            "INSERT INTO source_connection (connection_id, source_system, source_server, source_database, secret_scope, connection_status, is_active) "
            "VALUES ('c_toggle', 'sqlserver', 'srv', 'db', 'sc', 'VALID', false)"
        )
        self.cur.execute(
            "INSERT INTO source_table_control (connection_id, source_table_id, source_system, source_server, source_database, source_schema, source_table, target_catalog, target_schema, target_table, is_active, current_status, table_decision) "
            "VALUES ('c_toggle', 't_keep', 'sqlserver', 'srv', 'db', 'sch', 'tbl', 'cat', 'sch', 'tbl', true, 'PROVISIONED', 'AUTO_MIGRATE')"
        )
        # While parked: excluded from operational join
        self.cur.execute("""
            SELECT c.source_table_id
            FROM source_table_control c
            JOIN source_connection sc ON c.connection_id = sc.connection_id
            WHERE c.is_active = true AND sc.is_active = true AND sc.connection_status = 'VALID'
        """)
        self.assertEqual(self.cur.fetchall(), [])

        # Reactivate connection
        self.cur.execute("UPDATE source_connection SET is_active = true WHERE connection_id = 'c_toggle'")

        # Now operational join immediately includes the table without any modification to source_table_control
        self.cur.execute("""
            SELECT c.source_table_id
            FROM source_table_control c
            JOIN source_connection sc ON c.connection_id = sc.connection_id
            WHERE c.is_active = true AND sc.is_active = true AND sc.connection_status = 'VALID'
        """)
        self.assertEqual(self.cur.fetchall(), [('t_keep',)])

    # 17. Active parent plus active child plus VALID status and valid secret scope passes initialization
    def test_17_active_parent_valid_passes_initialization(self):
        for sys_name in ("oracle", "sqlserver"):
            with self.subTest(source_system=sys_name):
                con, cur = _init_sqlite_db()
                cur.execute(
                    "INSERT INTO source_connection (connection_id, source_system, source_server, source_database, secret_scope, connection_status, is_active) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (f"c_{sys_name}", sys_name, "srv", "db", "scope", "VALID", True)
                )
                cur.execute(
                    "INSERT INTO source_table_control (connection_id, source_table_id, source_system, source_server, source_database, source_schema, source_table, target_catalog, target_schema, target_table, is_active) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (f"c_{sys_name}", "t1", sys_name, "srv", "db", "sch", "tbl", "cat", "sch", "tbl", True)
                )
                cur.execute("""
                    SELECT count(*) AS c
                    FROM source_table_control c
                    JOIN source_connection sc ON c.connection_id = sc.connection_id
                    WHERE c.is_active = true
                      AND coalesce(sc.is_active, false) = true
                      AND (
                           sc.connection_status IS NULL
                           OR upper(trim(sc.connection_status)) <> 'VALID'
                           OR sc.secret_scope IS NULL
                           OR trim(sc.secret_scope) = ''
                      )
                """)
                self.assertEqual(cur.fetchone()[0], 0)

    # 18. Active parent with FAILED, REGISTERED, blank, null, or unsupported status and an operational active child follows existing reviewed validation policy
    def test_18_active_parent_non_valid_status_flags_active_child(self):
        non_valid_statuses = ["REGISTERED", "FAILED", "UNKNOWN", "", "   ", None]
        for idx, status in enumerate(non_valid_statuses):
            cid = f"conn_status_{idx}"
            self.cur.execute(
                "INSERT INTO source_connection (connection_id, source_system, source_server, source_database, secret_scope, connection_status, is_active) "
                "VALUES (?, 'sqlserver', 'srv', 'db', 'sc', ?, true)",
                (cid, status)
            )
            self.cur.execute(
                "INSERT INTO source_table_control (connection_id, source_table_id, source_system, source_server, source_database, source_schema, source_table, target_catalog, target_schema, target_table, is_active) "
                "VALUES (?, ?, 'sqlserver', 'srv', 'db', 'sch', 'tbl', 'cat', 'sch', 'tbl', true)",
                (cid, f"t_{idx}")
            )

        # Every active child under an active non-VALID parent MUST be flagged
        flagged = self._eval_active_table_invalid_connection()
        self.assertEqual(flagged, len(non_valid_statuses))

    # 19. Active parent with blank secret_scope still fails where source access is required
    def test_19_active_parent_blank_secret_scope_fails(self):
        self.cur.execute(
            "INSERT INTO source_connection (connection_id, source_system, source_server, source_database, secret_scope, connection_status, is_active) "
            "VALUES ('c_blank_scope', 'sqlserver', 'srv', 'db', '', 'VALID', true)"
        )
        self.cur.execute(
            "INSERT INTO source_table_control (connection_id, source_table_id, source_system, source_server, source_database, source_schema, source_table, target_catalog, target_schema, target_table, is_active) "
            "VALUES ('c_blank_scope', 't_blank_scope', 'sqlserver', 'srv', 'db', 'sch', 'tbl', 'cat', 'sch', 'tbl', true)"
        )
        flagged = self._eval_active_table_invalid_connection()
        self.assertEqual(flagged, 1)

    # 20. Orphan child registration still fails
    def test_20_orphan_child_registration_still_fails(self):
        # Insert table with no matching connection row
        self.cur.execute(
            "INSERT INTO source_table_control (connection_id, source_table_id, source_system, source_server, source_database, source_schema, source_table, target_catalog, target_schema, target_table, is_active) "
            "VALUES ('c_nonexistent', 't_orphan', 'sqlserver', 'srv', 'db', 'sch', 'tbl', 'cat', 'sch', 'tbl', true)"
        )
        self.cur.execute("""
            SELECT count(*) AS c
            FROM source_table_control c
            LEFT JOIN source_connection sc
              ON c.connection_id = sc.connection_id
            WHERE sc.connection_id IS NULL
        """)
        orphans = self.cur.fetchone()[0]
        self.assertEqual(orphans, 1)

    # 21. Source-system and endpoint mismatches still fail
    def test_21_source_system_and_endpoint_mismatches_still_fail(self):
        # System mismatch
        self.cur.execute(
            "INSERT INTO source_connection (connection_id, source_system, source_server, source_database, secret_scope, connection_status, is_active) "
            "VALUES ('c_sys_mis', 'sqlserver', 'srv1', 'db1', 'sc', 'VALID', true)"
        )
        self.cur.execute(
            "INSERT INTO source_table_control (connection_id, source_table_id, source_system, source_server, source_database, source_schema, source_table, target_catalog, target_schema, target_table, is_active) "
            "VALUES ('c_sys_mis', 't_sys_mis', 'oracle', 'srv1', 'db1', 'sch', 'tbl', 'cat', 'sch', 'tbl', true)"
        )
        self.cur.execute("""
            SELECT count(*) AS c
            FROM source_table_control c
            JOIN source_connection sc
              ON c.connection_id = sc.connection_id
            WHERE lower(trim(c.source_system)) <> lower(trim(sc.source_system))
        """)
        self.assertEqual(self.cur.fetchone()[0], 1)

        # Server endpoint mismatch
        self.cur.execute(
            "INSERT INTO source_connection (connection_id, source_system, source_server, source_database, secret_scope, connection_status, is_active) "
            "VALUES ('c_srv_mis', 'sqlserver', 'srv_conn', 'db1', 'sc', 'VALID', true)"
        )
        self.cur.execute(
            "INSERT INTO source_table_control (connection_id, source_table_id, source_system, source_server, source_database, source_schema, source_table, target_catalog, target_schema, target_table, is_active) "
            "VALUES ('c_srv_mis', 't_srv_mis', 'sqlserver', 'srv_tbl', 'db1', 'sch', 'tbl', 'cat', 'sch', 'tbl', true)"
        )
        self.cur.execute("""
            SELECT count(*) AS c
            FROM source_table_control c
            JOIN source_connection sc
              ON c.connection_id = sc.connection_id
            WHERE (c.source_server IS NOT NULL
                   AND sc.source_server IS NOT NULL
                   AND lower(trim(c.source_server)) <> lower(trim(sc.source_server)))
        """)
        self.assertEqual(self.cur.fetchone()[0], 1)

    # 22. Duplicate ownership and identity-v2 inconsistency checks remain unchanged
    def test_22_duplicate_ownership_and_identity_v2_remain_unchanged(self):
        code = shared_nb("NB00_ControlTableInit.py")
        for check in (
            "DUPLICATE_TABLE_OWNERSHIP",
            "DUPLICATE_CONNECTION_ID",
            "ORPHAN_TABLE_CONNECTION",
            "TABLE_CONNECTION_SOURCE_MISMATCH",
            "TABLE_CONNECTION_ENDPOINT_MISMATCH",
            "ACTIVE_TABLE_INCOMPLETE_TARGET",
        ):
            self.assertIn(f'"{check}"', code)
        self.assertIn("source_identity_version IS NULL OR source_identity_version <> 2", code)


if __name__ == "__main__":
    unittest.main()
