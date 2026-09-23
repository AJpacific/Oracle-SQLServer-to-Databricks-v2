"""
Unit tests for the pipeline-driven connection registry (Commit 1).

Covered: connection-input validation/normalization, idempotent upsert SQL,
active_tables connection scoping, that no credential columns are ever written,
trustServerCertificate defaulting to false, and source-system conflict guarding.
"""

import os
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.join(os.path.dirname(HERE), "src")
for p in (SRC, HERE, os.path.dirname(HERE)):
    if p not in sys.path:
        sys.path.insert(0, p)

from control_repository import (  # noqa: E402
    ControlRepository, normalize_connection_input, assert_source_system_match,
    require_connection_id,
)
from source_adapters.factory import get_source_adapter  # noqa: E402
from source_identity import (  # noqa: E402
    SOURCE_IDENTITY_VERSION, compute_legacy_source_table_id,
    compute_source_table_id, normalize_source_system, require_source_system,
)
from _fakes import FakeSpark, FakeRow  # noqa: E402


class TestConnectionInput(unittest.TestCase):
    def _base(self, **over):
        d = {
            "connection_id": "c1",
            "connection_name": "Connection One",
            "source_system": "oracle",
            "source_database": "ORCL",
            "secret_scope": "scope-1",
        }
        d.update(over)
        return d

    def test_sqlserver_populated_database(self):
        # A. SQL Server populated database: accepted, source_database remains BI_HomeCredit
        out = normalize_connection_input(self._base(
            connection_id="ss_1",
            source_system="sqlserver",
            source_database="BI_HomeCredit",
            secret_scope="ss-1",
        ))
        self.assertEqual(out["source_system"], "sqlserver")
        self.assertEqual(out["source_database"], "BI_HomeCredit")

    def test_sqlserver_null_database(self):
        # B. SQL Server NULL database: accepted as discovery mode, source_database is None
        out = normalize_connection_input(self._base(
            connection_id="ss_1",
            source_system="sqlserver",
            source_database=None,
            secret_scope="ss-1",
        ))
        self.assertEqual(out["source_system"], "sqlserver")
        self.assertIsNone(out["source_database"])

    def test_sqlserver_empty_database(self):
        # C. SQL Server empty database: accepted as discovery mode
        out = normalize_connection_input(self._base(
            connection_id="ss_1",
            source_system="sqlserver",
            source_database="",
            secret_scope="ss-1",
        ))
        self.assertEqual(out["source_system"], "sqlserver")
        self.assertIsNone(out["source_database"])

    def test_sqlserver_whitespace_database(self):
        # D. SQL Server whitespace database: accepted as discovery mode after normalization
        out = normalize_connection_input(self._base(
            connection_id="ss_1",
            source_system="sqlserver",
            source_database="   ",
            secret_scope="ss-1",
        ))
        self.assertEqual(out["source_system"], "sqlserver")
        self.assertIsNone(out["source_database"])

    def test_sqlserver_blank_database_not_persisted_as_master(self):
        # Blank SQL Server connection must never be converted to master
        for blank_db in (None, "", "   "):
            with self.subTest(blank_db=blank_db):
                out = normalize_connection_input(self._base(
                    connection_id="ss_1",
                    source_system="sqlserver",
                    source_database=blank_db,
                    secret_scope="ss-1",
                ))
                self.assertNotEqual(out.get("source_database"), "master")
                self.assertIsNone(out.get("source_database"))

    def test_oracle_populated_database(self):
        # E. Oracle populated database/service: accepted
        out = normalize_connection_input(self._base(
            connection_id="ora_1",
            source_system="oracle",
            source_database="ORCL",
        ))
        self.assertEqual(out["source_system"], "oracle")
        self.assertEqual(out["source_database"], "ORCL")
        self.assertFalse(out["trust_server_certificate"])

    def test_oracle_missing_database_fails(self):
        # F. Oracle missing database/service (None, "", "   "): rejected with clear validation error
        for missing in (None, "", "   "):
            with self.subTest(missing=missing), self.assertRaises(ValueError) as ctx:
                normalize_connection_input(self._base(
                    source_system="oracle",
                    source_database=missing,
                ))
            self.assertIn("oracle connections require source_database", str(ctx.exception))

    def test_invalid_source_system_fails(self):
        # G. Unsupported source system
        with self.assertRaises(ValueError):
            normalize_connection_input(self._base(source_system="db2"))

    def test_missing_source_system_fails(self):
        # G. Missing source system
        for missing in (None, "", "   "):
            with self.subTest(missing=missing), self.assertRaises(ValueError):
                normalize_connection_input(self._base(source_system=missing))

    def test_missing_required_fields_fail(self):
        # H. Other required connection fields
        with self.assertRaises(ValueError):
            normalize_connection_input(self._base(connection_id=""))
        with self.assertRaises(ValueError):
            normalize_connection_input(self._base(connection_name=""))
        with self.assertRaises(ValueError):
            normalize_connection_input(self._base(secret_scope=""))

    def test_trust_default_false(self):
        # H. Trust server certificate default and explicit setting
        self.assertFalse(
            normalize_connection_input(self._base())["trust_server_certificate"])
        self.assertTrue(normalize_connection_input(
            self._base(trust_server_certificate=True))["trust_server_certificate"])

    def test_require_connection_id_trims_and_rejects_blank(self):
        # H. require_connection_id validation
        self.assertEqual(require_connection_id("  c1  "), "c1")
        for value in (None, "", "  "):
            with self.subTest(value=value), self.assertRaises(ValueError):
                require_connection_id(value)


class TestSystemConflict(unittest.TestCase):
    def test_conflict_raises(self):
        with self.assertRaises(ValueError):
            assert_source_system_match("oracle", "sqlserver")

    def test_match_ok(self):
        assert_source_system_match("mssql", "sqlserver")  # no raise

    def test_missing_system_raises(self):
        with self.assertRaises(ValueError):
            assert_source_system_match(None, "oracle")
        with self.assertRaises(ValueError):
            assert_source_system_match("oracle", "")


class TestRequiredSourceIdentity(unittest.TestCase):
    def test_missing_source_system_raises_with_field_name(self):
        for missing in (None, "", "  "):
            with self.subTest(missing=missing), self.assertRaises(ValueError) as ctx:
                require_source_system(missing, "pipeline row")
            self.assertIn("source_system", str(ctx.exception))
            self.assertNotIn("password", str(ctx.exception).lower())

    def test_unknown_source_system_raises(self):
        with self.assertRaises(ValueError):
            require_source_system("postgresql")

    def test_supported_sources_and_aliases_normalize(self):
        self.assertEqual(require_source_system(" Oracle "), "oracle")
        self.assertEqual(require_source_system("SQLSERVER"), "sqlserver")
        self.assertEqual(require_source_system("mssql"), "sqlserver")

    def test_valid_identity_generation_is_stable_across_aliases(self):
        canonical = compute_source_table_id(
            "sales-read", "sqlserver", "Host", "Db", "dbo", "Orders")
        alias = compute_source_table_id(
            "sales-read", "mssql", "host", "db", "dbo", "Orders")
        self.assertEqual(canonical, alias)
        self.assertEqual(len(canonical), 64)

    def test_identity_never_assumes_a_source(self):
        with self.assertRaises(ValueError):
            compute_source_table_id("c1", None, "host", "db", "S", "T")

    def test_identity_requires_connection_id(self):
        for missing in (None, "", "   "):
            with self.subTest(missing=missing), self.assertRaises(ValueError) as ctx:
                compute_source_table_id(
                    missing, "oracle", "host", "db", "S", "T")
            self.assertIn("connection_id", str(ctx.exception))

    def test_same_physical_table_has_connection_owned_ids(self):
        read_id = compute_source_table_id(
            "ORA_FIN_READ", "oracle", "finance-host", "FINPDB",
            "FINANCE", "INVOICE")
        migration_id = compute_source_table_id(
            "ORA_FIN_MIGRATION", "oracle", "finance-host", "FINPDB",
            "FINANCE", "INVOICE")
        self.assertNotEqual(read_id, migration_id)

    def test_identity_is_stable_and_versioned(self):
        args = ("ORA_FIN_READ", "oracle", "finance-host", "FINPDB",
                "FINANCE", "INVOICE")
        self.assertEqual(compute_source_table_id(*args),
                         compute_source_table_id(*args))
        self.assertEqual(SOURCE_IDENTITY_VERSION, 2)
        legacy = compute_legacy_source_table_id(*args[1:])
        self.assertNotEqual(compute_source_table_id(*args), legacy)


class TestUpsertConnectionSQL(unittest.TestCase):
    def _repo(self, results):
        return ControlRepository(FakeSpark(results), "cat", "control")

    def test_insert_when_absent(self):
        # get_connection -> [] (absent), then INSERT
        repo = self._repo(results=[[]])
        repo.upsert_connection({
            "connection_id": "oracle_1", "connection_name": "Oracle One",
            "source_system": "oracle", "secret_scope": "oracle-source-1",
            "connection_status": "REGISTERED", "is_active": True,
        })
        sqls = repo.spark.executed
        self.assertTrue(any("INSERT INTO" in s for s in sqls))
        insert_sql = [s for s in sqls if "INSERT INTO" in s][0]
        self.assertIn("oracle_1", insert_sql)
        self.assertIn("'REGISTERED'", insert_sql)
        self.assertIn("false", insert_sql)

    def test_update_when_present(self):
        existing = [FakeRow(connection_id="oracle_1")]
        repo = self._repo(results=[existing])
        repo.upsert_connection({
            "connection_id": "oracle_1", "connection_name": "Renamed",
            "source_system": "oracle", "secret_scope": "oracle-source-1",
        })
        sqls = repo.spark.executed
        self.assertTrue(any(s.strip().startswith("UPDATE") for s in sqls))

    def test_same_endpoint_with_different_connection_ids_is_allowed(self):
        repo = self._repo(results=[[], []])
        for connection_id, scope in (("ORA_FIN_READ", "read-scope"),
                                     ("ORA_FIN_MIGRATION", "write-scope")):
            repo.upsert_connection({
                "connection_id": connection_id,
                "connection_name": connection_id,
                "source_system": "oracle",
                "source_server": "finance-host",
                "source_database": "FINPDB",
                "secret_scope": scope,
            })
        inserts = [sql for sql in repo.spark.executed if "INSERT INTO" in sql]
        self.assertEqual(len(inserts), 2)
        self.assertIn("read-scope", inserts[0])
        self.assertIn("write-scope", inserts[1])

    def test_existing_connection_source_system_cannot_change(self):
        existing = [FakeRow(
            connection_id="c1", source_system="oracle",
            source_server="host", source_database=None,
            secret_scope="scope", trust_server_certificate=False)]
        repo = self._repo(results=[existing])
        with self.assertRaisesRegex(ValueError, "source_system conflict"):
            repo.upsert_connection({
                "connection_id": "c1", "connection_name": "Changed",
                "source_system": "sqlserver", "source_server": "host",
                "source_database": "Db", "secret_scope": "scope",
            })

    def test_secret_scope_change_forces_revalidation(self):
        existing = [FakeRow(
            connection_id="c1", source_system="oracle",
            source_server="host", source_database=None,
            secret_scope="old-scope", trust_server_certificate=False,
            connection_status="VALID", is_active=True)]
        repo = self._repo(results=[existing])
        repo.upsert_connection({
            "connection_id": "c1", "connection_name": "Rotated",
            "source_system": "oracle", "source_server": "host",
            "source_database": None, "secret_scope": "new-scope",
        })
        sql = repo.spark.last_sql()
        self.assertIn("`connection_status` = 'REGISTERED'", sql)
        self.assertIn("`is_active` = false", sql)
        self.assertIn("`last_validated_ts` = NULL", sql)

    def test_direct_upsert_rejects_blank_source_system(self):
        repo = self._repo(results=[])
        with self.assertRaises(ValueError):
            repo.upsert_connection({
                "connection_id": "bad", "connection_name": "Bad",
                "source_system": "", "secret_scope": "scope",
            })

    def test_no_credential_columns_persisted(self):
        repo = self._repo(results=[[]])
        # Even if a caller mistakenly supplies secret-like keys, they are ignored.
        repo.upsert_connection({
            "connection_id": "oracle_1", "connection_name": "Oracle One",
            "source_system": "oracle", "secret_scope": "oracle-source-1",
            "user": "scott", "password": "tiger", "jdbc_url": "jdbc:...",
        })
        joined = " ".join(repo.spark.executed).lower()
        for banned in ("password", "tiger", "scott", "jdbc_url"):
            self.assertNotIn(banned, joined)

    def test_active_tables_scopes_by_connection(self):
        repo = self._repo(results=[[]])
        repo.active_tables(connection_id="oracle_1")
        self.assertIn("connection_id =", repo.spark.last_sql())
        self.assertIn("oracle_1", repo.spark.last_sql())

    def test_active_tables_without_connection_unchanged(self):
        repo = self._repo(results=[[]])
        repo.active_tables()
        self.assertNotIn("connection_id", repo.spark.last_sql())

    def test_active_tables_for_connection_requires_and_scopes_id(self):
        repo = self._repo(results=[[]])
        with self.assertRaises(ValueError):
            repo.active_tables_for_connection("")
        repo.active_tables_for_connection("oracle_1", decision="AUTO_MIGRATE")
        self.assertIn("connection_id =", repo.spark.last_sql())
        self.assertIn("table_decision =", repo.spark.last_sql())

    def test_duplicate_connection_id_fails(self):
        duplicate = [FakeRow(connection_id="c1"), FakeRow(connection_id="c1")]
        with self.assertRaises(ValueError):
            self._repo(results=[duplicate]).get_connection("c1")

    def test_get_connection_rejects_blank(self):
        with self.assertRaises(ValueError):
            self._repo(results=[]).get_connection("  ")

    def test_valid_active_connections_requires_operational_state(self):
        repo = self._repo(results=[[]])
        repo.valid_active_connections(["c1", "c2"])
        sql = repo.spark.last_sql()
        self.assertIn("coalesce(is_active, false) = true", sql)
        self.assertIn("upper(trim(connection_status)) = 'VALID'", sql)
        self.assertIn("trim(secret_scope) <> ''", sql)

    def test_get_source_table_uses_composite_key_and_rejects_duplicates(self):
        duplicate = [FakeRow(source_table_id="s1"), FakeRow(source_table_id="s1")]
        repo = self._repo(results=[duplicate])
        with self.assertRaises(ValueError):
            repo.get_source_table("c1", "s1")
        sql = repo.spark.last_sql()
        self.assertIn("connection_id =", sql)
        self.assertIn("source_table_id =", sql)

    def test_update_control_for_connection_uses_composite_key_and_sanitizes(self):
        repo = self._repo(results=[])
        repo.update_control_for_connection(
            "c1", "s1", {"error_message": "password=hunter2 failed"})
        sql = repo.spark.last_sql()
        self.assertIn("connection_id =", sql)
        self.assertIn("source_table_id =", sql)
        self.assertNotIn("hunter2", sql)

    def test_status_controls_activation(self):
        repo = self._repo(results=[])
        repo.update_connection_status("c1", "VALID")
        self.assertIn("`is_active` = true", repo.spark.last_sql())
        repo.update_connection_status("c1", "FAILED", "password=secret")
        self.assertIn("`is_active` = false", repo.spark.last_sql())
        self.assertNotIn("secret", repo.spark.last_sql())

    def test_update_connection_status_valid_sets_validated_ts(self):
        repo = self._repo(results=[])
        repo.update_connection_status("oracle_1", "VALID", None)
        self.assertIn("last_validated_ts", repo.spark.last_sql())


class TestSecretScopeSelection(unittest.TestCase):
    def test_adapter_carries_connection_secret_scope(self):
        # The correct scope is chosen by connection metadata, not a global scope.
        adapter = get_source_adapter(
            "sqlserver", source_server="host", source_database="Db",
            secret_scope="sqlserver-source-7", config={})
        self.assertEqual(adapter.secret_scope, "sqlserver-source-7")

    def test_trust_server_certificate_defaults_false_in_url(self):
        # A fake secret provider avoids any live secret access.
        secrets = {("ss", "sqlserver-user"): "u", ("ss", "sqlserver-password"): "p"}
        adapter = get_source_adapter(
            "sqlserver", source_server="myhost", source_database="Db",
            secret_provider=lambda scope, key: secrets.get((scope, key)),
            secret_scope="ss", config={})
        url, _ = adapter.get_jdbc_url_and_props()
        self.assertIn("trustServerCertificate=false", url)
        self.assertIn("encrypt=true", url)

    def test_trust_server_certificate_true_when_enabled(self):
        secrets = {("ss", "sqlserver-user"): "u", ("ss", "sqlserver-password"): "p"}
        adapter = get_source_adapter(
            "sqlserver", source_server="myhost", source_database="Db",
            secret_provider=lambda scope, key: secrets.get((scope, key)),
            secret_scope="ss", config={"trust_server_certificate": True})
        url, _ = adapter.get_jdbc_url_and_props()
        self.assertIn("trustServerCertificate=true", url)


if __name__ == "__main__":
    unittest.main()
