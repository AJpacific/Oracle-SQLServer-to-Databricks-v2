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
for p in (SRC, os.path.dirname(HERE)):
    if p not in sys.path:
        sys.path.insert(0, p)

from control_repository import (  # noqa: E402
    ControlRepository, normalize_connection_input, assert_source_system_match,
)
from source_adapters.factory import get_source_adapter  # noqa: E402
from _fakes import FakeSpark, FakeRow  # noqa: E402


class TestConnectionInput(unittest.TestCase):
    def _base(self, **over):
        d = {
            "connection_id": "oracle_1",
            "connection_name": "Oracle One",
            "source_system": "oracle",
            "secret_scope": "oracle-source-1",
        }
        d.update(over)
        return d

    def test_valid_oracle_input_normalizes(self):
        out = normalize_connection_input(self._base())
        self.assertEqual(out["source_system"], "oracle")
        self.assertFalse(out["trust_server_certificate"])

    def test_sqlserver_requires_database(self):
        with self.assertRaises(ValueError):
            normalize_connection_input(self._base(
                connection_id="ss_1", source_system="sqlserver",
                secret_scope="ss-1"))

    def test_sqlserver_with_database_ok(self):
        out = normalize_connection_input(self._base(
            connection_id="ss_1", source_system="mssql", secret_scope="ss-1",
            source_database="SourceDb"))
        self.assertEqual(out["source_system"], "sqlserver")
        self.assertEqual(out["source_database"], "SourceDb")

    def test_invalid_source_system_fails(self):
        with self.assertRaises(ValueError):
            normalize_connection_input(self._base(source_system="db2"))

    def test_missing_required_fields_fail(self):
        with self.assertRaises(ValueError):
            normalize_connection_input(self._base(connection_id=""))
        with self.assertRaises(ValueError):
            normalize_connection_input(self._base(secret_scope=""))

    def test_trust_default_false(self):
        self.assertFalse(
            normalize_connection_input(self._base())["trust_server_certificate"])
        self.assertTrue(normalize_connection_input(
            self._base(trust_server_certificate=True))["trust_server_certificate"])


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

    def test_update_when_present(self):
        existing = [FakeRow(connection_id="oracle_1")]
        repo = self._repo(results=[existing])
        repo.upsert_connection({
            "connection_id": "oracle_1", "connection_name": "Renamed",
            "source_system": "oracle", "secret_scope": "oracle-source-1",
        })
        sqls = repo.spark.executed
        self.assertTrue(any(s.strip().startswith("UPDATE") for s in sqls))

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
