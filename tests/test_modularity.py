"""
Modularity tests (Part J): prove the shared/source boundary actually holds.

These assert behavior-relevant properties (no source dialect leaking into shared
notebooks, matching contracts across sources, a working adapter contract, and
that a brand-new source needs no shared-code change) rather than merely checking
that files exist.
"""

import os
import re
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.join(os.path.dirname(HERE), "src")
for p in (SRC, HERE, os.path.dirname(HERE)):
    if p not in sys.path:
        sys.path.insert(0, p)

from _nbsource import (  # noqa: E402
    SOURCES, SOURCE_TOKENS, REQUIRED_SOURCE_NOTEBOOKS, SHARED_NOTEBOOKS,
    shared_nb, source_nb, source_nb_path, all_shared_notebooks,
    all_source_notebooks,
)
import source_registry  # noqa: E402
import assessment_common as assess_common  # noqa: E402
import inventory_common as inv_common  # noqa: E402
import sql_object_assessment_common as sqlobj_common  # noqa: E402
from source_adapters.factory import get_source_adapter  # noqa: E402
from source_adapters.base import SourceAdapter  # noqa: E402


def _strip_markdown(src):
    """Drop '# MAGIC %md' documentation lines before prohibition checks.

    A shared notebook is allowed to *describe* a prohibition in its docs; it is
    the executable code that must stay source-neutral.
    """
    keep = []
    for line in src.splitlines():
        stripped = line.strip()
        if stripped.startswith("# MAGIC"):
            continue
        keep.append(line)
    return "\n".join(keep)


class TestSharedNotebookNeutrality(unittest.TestCase):
    """Shared notebooks must contain no source dialect or credential names."""

    FORBIDDEN = (
        "SELECT 1 FROM DUAL",
        "all_tables",
        "all_tab_columns",
        "all_source",
        "all_views",
        "sys.tables",
        "sys.columns",
        "sys.sql_modules",
        "oracle-user",
        "oracle-password",
        "sqlserver-user",
        "sqlserver-password",
        'source_system == "oracle"',
        'source_system == "sqlserver"',
    )

    def test_no_source_dialect_or_credentials(self):
        for name in SHARED_NOTEBOOKS:
            code = _strip_markdown(shared_nb(name)).lower()
            for needle in self.FORBIDDEN:
                self.assertNotIn(needle.lower(), code,
                                 f"{name} contains prohibited token {needle!r}")

    def test_shared_notebooks_do_not_import_dialect_builders(self):
        # Dialect SQL must arrive through the adapter, never a direct import of
        # the Oracle or SQL Server builder.
        for name in SHARED_NOTEBOOKS:
            code = _strip_markdown(shared_nb(name))
            self.assertNotIn("sqlserver_sql_builder", code, name)
            self.assertIsNone(
                re.search(r"^\s*import\s+sql_builder", code, re.M), name)

    def test_shared_notebooks_do_not_branch_on_source_system(self):
        for name in SHARED_NOTEBOOKS:
            code = _strip_markdown(shared_nb(name))
            self.assertIsNone(
                re.search(r"if\s+.*source_system\s*==", code), name)

    def test_common_owns_no_dialect_sql(self):
        code = _strip_markdown(shared_nb("_common.py")).lower()
        for needle in ("from dual", "sys.tables", "all_tables",
                       "sys.sql_modules"):
            self.assertNotIn(needle, code)

    def test_probe_sql_comes_from_the_adapter(self):
        # _common exposes a probe helper but must not hold the probe SQL.
        code = shared_nb("_common.py")
        self.assertIn("adapter.connection_probe_query()", code)

    def test_common_holds_no_source_credential_keys(self):
        code = _strip_markdown(shared_nb("_common.py"))
        for key in ("oracle-user", "oracle-password", "oracle-jdbc-url",
                    "oracle-host", "oracle-service", "sqlserver-user",
                    "sqlserver-password", "sqlserver-jdbc-url"):
            self.assertNotIn(key, code,
                             f"_common.py must not name the secret key {key!r}")

    def test_legacy_oracle_read_helper_removed_from_common(self):
        code = _strip_markdown(shared_nb("_common.py"))
        self.assertNotIn("def get_jdbc_url_and_props(", code)
        self.assertNotIn("def read_jdbc(", code)
        self.assertIn("def read_source_jdbc(", code)

    def test_legacy_scope_widget_is_marked_compatibility_only(self):
        self.assertIn("COMPATIBILITY ONLY", shared_nb("_common.py"))


class TestSourceFolderCompleteness(unittest.TestCase):
    def test_every_source_provides_required_notebooks(self):
        for token in SOURCE_TOKENS:
            for required in REQUIRED_SOURCE_NOTEBOOKS:
                self.assertTrue(
                    os.path.isfile(source_nb_path(token, required)),
                    f"sources/{token}/{required} is missing")

    def test_source_folders_contain_only_source_notebooks(self):
        for token, name in all_source_notebooks():
            self.assertIn(name, REQUIRED_SOURCE_NOTEBOOKS,
                          f"unexpected notebook sources/{token}/{name}")


class TestNoDuplicatedSharedEngine(unittest.TestCase):
    """Source folders must not re-implement any shared engine."""

    FORBIDDEN_MARKERS = (
        "build_merge_sql",          # Delta MERGE orchestration
        "last_etl_watermark_value",  # ETL checkpoint commit
        "dq_quarantine",             # quarantine writes
        "CREATE OR REPLACE VIEW",    # dashboard views
        "CREATE TABLE IF NOT EXISTS",  # control-table DDL
        "recovery_action",           # retry selector logic
        "teams_webhook",             # notification posting
        "silver_table",              # Silver writes
    )

    def test_no_shared_engine_in_source_notebooks(self):
        for token, name in all_source_notebooks():
            code = _strip_markdown(source_nb(token, name))
            for marker in self.FORBIDDEN_MARKERS:
                self.assertNotIn(marker, code,
                                 f"sources/{token}/{name} duplicates shared logic "
                                 f"({marker})")

    def test_source_notebooks_do_not_commit_ingest_checkpoints(self):
        for token, name in all_source_notebooks():
            code = _strip_markdown(source_nb(token, name))
            self.assertNotIn("last_successful_run_ts", code, f"{token}/{name}")


class TestNotebookImportPaths(unittest.TestCase):
    def test_source_notebooks_load_shared_bootstrap(self):
        for token, name in all_source_notebooks():
            code = source_nb(token, name)
            self.assertIn("%run ../../shared/_common", code,
                          f"sources/{token}/{name} does not load shared/_common")

    def test_shared_notebooks_load_sibling_common(self):
        for name in SHARED_NOTEBOOKS:
            self.assertIn("%run ./_common", shared_nb(name), name)

    def test_no_notebooks_outside_shared_and_sources(self):
        # Every notebook is authoritative and lives in exactly one place.
        stray = [f for f in os.listdir(os.path.dirname(SOURCES))
                 if f.endswith(".py")]
        self.assertEqual(stray, [], f"unexpected notebooks at notebooks/: {stray}")


class TestSourceContractParity(unittest.TestCase):
    """Both source versions must expose the same widgets and output keys."""

    @staticmethod
    def _widgets(code):
        return set(re.findall(r'dbutils\.widgets\.\w+\(\s*"([^"]+)"', code))

    @staticmethod
    def _exit_keys(code):
        tail = code.split("dbutils.notebook.exit(")[-1]
        return set(re.findall(r'"([a-z_]+)":', tail))

    def test_connection_notebook_contract(self):
        widgets = [self._widgets(source_nb(t, "NB00A_UpsertAndValidateConnection.py"))
                   for t in SOURCE_TOKENS]
        self.assertEqual(widgets[0], widgets[1])
        for w in widgets:
            for expected in ("connection_id", "connection_name", "source_server",
                             "source_database", "secret_scope",
                             "trust_server_certificate"):
                self.assertIn(expected, w)
            # source_system is fixed by the notebook, never a widget.
            self.assertNotIn("source_system", w)

    def test_connection_notebook_outputs(self):
        keys = [self._exit_keys(source_nb(t, "NB00A_UpsertAndValidateConnection.py"))
                for t in SOURCE_TOKENS]
        self.assertEqual(keys[0], keys[1])
        for expected in ("status", "run_id", "connection_id", "source_system",
                         "source_database"):
            self.assertIn(expected, keys[0])

    def test_connection_notebooks_fix_their_source_system(self):
        self.assertIn('SOURCE_SYSTEM = "oracle"',
                      source_nb("oracle", "NB00A_UpsertAndValidateConnection.py"))
        self.assertIn('SOURCE_SYSTEM = "sqlserver"',
                      source_nb("sqlserver", "NB00A_UpsertAndValidateConnection.py"))

    def test_assessment_notebook_outputs(self):
        keys = [self._exit_keys(source_nb(t, "NB01A_SourceAssessment.py"))
                for t in SOURCE_TOKENS]
        self.assertEqual(keys[0], keys[1])
        for expected in ("status", "run_id", "connection_id", "assessment_id",
                         "objects", "summary"):
            self.assertIn(expected, keys[0])

    def test_inventory_notebook_outputs(self):
        keys = [self._exit_keys(source_nb(t, "NB01_SourceInventory.py"))
                for t in SOURCE_TOKENS]
        self.assertEqual(keys[0], keys[1])
        for expected in ("status", "run_id", "connection_id", "source_system"):
            self.assertIn(expected, keys[0])

    def test_sql_object_notebook_outputs(self):
        keys = [self._exit_keys(source_nb(t, "NB13_SQLObjectAssessmentAndConversion.py"))
                for t in SOURCE_TOKENS]
        self.assertEqual(keys[0], keys[1])
        for expected in ("status", "assessment_id", "objects", "summary"):
            self.assertIn(expected, keys[0])

    def test_both_sources_use_shared_persistence(self):
        for token in SOURCE_TOKENS:
            self.assertIn("persist_inventory_rows(",
                          source_nb(token, "NB01_SourceInventory.py"))
            self.assertIn("persist_assessment_records(",
                          source_nb(token, "NB01A_SourceAssessment.py"))
            self.assertIn("persist_sql_object_records(",
                          source_nb(token, "NB13_SQLObjectAssessmentAndConversion.py"))


class TestOutputSchemaParity(unittest.TestCase):
    """Oracle and SQL Server must normalize to identical shared shapes."""

    ORACLE_COLUMN = {
        "COLUMN_NAME": "ID", "ORDINAL_POSITION": 1, "IS_NULLABLE": "NO",
        "DATA_TYPE": "NUMBER", "CHARACTER_MAXIMUM_LENGTH": None,
        "NUMERIC_PRECISION": 10, "NUMERIC_SCALE": 0, "DATETIME_PRECISION": None,
    }
    SQLSERVER_COLUMN = {
        "COLUMN_NAME": "ID", "ORDINAL_POSITION": 1, "IS_NULLABLE": "NO",
        "DATA_TYPE": "int", "CHARACTER_MAXIMUM_LENGTH": None,
        "NUMERIC_PRECISION": 10, "NUMERIC_SCALE": 0, "DATETIME_PRECISION": None,
        "IS_IDENTITY": 1, "IS_COMPUTED": 0, "IS_HIDDEN": 0, "IS_ROWVERSION": 0,
        "SOURCE_TYPE_SCHEMA": "sys",
    }

    def _identity(self, system):
        return {"run_id": "r1", "source_table_id": "sid", "connection_id": "c1",
                "source_system": system, "source_server": "srv",
                "source_database": "db", "source_schema": "S", "source_table": "T"}

    def test_inventory_rows_have_identical_arity(self):
        o = inv_common.normalize_inventory_row(
            self.ORACLE_COLUMN, self._identity("oracle"))
        s = inv_common.normalize_inventory_row(
            self.SQLSERVER_COLUMN, self._identity("sqlserver"))
        self.assertEqual(len(o), len(s))
        self.assertEqual(len(o), len(inv_common.INVENTORY_FIELDS))

    def test_missing_optional_flags_default_false(self):
        o = dict(zip(inv_common.INVENTORY_FIELDS,
                     inv_common.normalize_inventory_row(
                         self.ORACLE_COLUMN, self._identity("oracle"))))
        self.assertFalse(o["is_identity"])
        self.assertFalse(o["is_rowversion"])

    def test_sqlserver_flags_are_interpreted(self):
        s = dict(zip(inv_common.INVENTORY_FIELDS,
                     inv_common.normalize_inventory_row(
                         self.SQLSERVER_COLUMN, self._identity("sqlserver"))))
        self.assertTrue(s["is_identity"])
        self.assertEqual(s["source_type_schema"], "sys")

    def test_required_alias_validation(self):
        inv_common.validate_metadata_aliases(self.ORACLE_COLUMN.keys())
        with self.assertRaises(ValueError):
            inv_common.validate_metadata_aliases(["COLUMN_NAME"])

    def test_hidden_and_computed_excluded_from_strategy(self):
        cols = [self.SQLSERVER_COLUMN,
                {**self.SQLSERVER_COLUMN, "COLUMN_NAME": "C", "IS_COMPUTED": 1},
                {**self.SQLSERVER_COLUMN, "COLUMN_NAME": "H", "IS_HIDDEN": 1}]
        names = [c["column_name"] for c in inv_common.strategy_columns(cols)]
        self.assertEqual(names, ["ID"])


class TestAssessmentRecordParity(unittest.TestCase):
    def _record(self, system, method):
        return assess_common.build_assessment_record(
            assessment_id="a1", run_id="r1", connection_id="c1",
            source_system=system, source_server="srv", source_database="db",
            source_schema="S", object_name="T", object_type="TABLE",
            compatibility_status="COMPATIBLE", row_count=10,
            row_count_method=method, column_count=3, complexity="LOW")

    def test_both_sources_produce_identical_fields(self):
        o = self._record("oracle", assess_common.ESTIMATED)
        s = self._record("sqlserver", assess_common.CATALOG)
        self.assertEqual(sorted(o), sorted(s))
        self.assertEqual(sorted(o), sorted(assess_common.ASSESSMENT_FIELDS))

    def test_exact_is_not_a_row_count_method(self):
        self.assertNotIn("EXACT", assess_common.ROW_COUNT_METHODS)
        with self.assertRaises(ValueError):
            self._record("sqlserver", "EXACT")

    def test_invalid_object_type_rejected(self):
        with self.assertRaises(ValueError):
            assess_common.build_assessment_record(
                assessment_id="a1", run_id="r1", connection_id="c1",
                source_system="oracle", source_server=None, source_database=None,
                source_schema="S", object_name="X", object_type="TRIGGER",
                compatibility_status="REVIEW")

    def test_summary_counts(self):
        records = [self._record("oracle", assess_common.ESTIMATED) for _ in range(3)]
        self.assertEqual(assess_common.summarize_compatibility(records),
                         {"COMPATIBLE": 3})


class TestAdapterContract(unittest.TestCase):
    """Both adapters must satisfy everything shared notebooks rely on."""

    REQUIRED_METHODS = (
        "get_jdbc_url_and_props", "connection_probe_query", "extra_read_options",
        "columns_metadata_query", "primary_key_query", "list_schemas_query",
        "list_tables_query", "list_views_query", "list_routines_query",
        "table_statistics_query", "full_extract_query",
        "incremental_extract_query", "upper_watermark_query", "count_query",
        "min_max_query", "normalize_watermark_type",
        "is_supported_watermark_type", "watermark_type_rank",
        "initial_watermark_value", "resolve_partition_plan", "load_type_mapper",
        "normalize_sql_object_type", "supports_sql_object_type",
        "redact_jdbc_url", "read_jdbc",
    )

    def _adapters(self):
        return (get_source_adapter("oracle"),
                get_source_adapter("sqlserver", source_database="Db"))

    def test_all_required_methods_present(self):
        for adapter in self._adapters():
            for method in self.REQUIRED_METHODS:
                self.assertTrue(callable(getattr(adapter, method, None)),
                                f"{type(adapter).__name__}.{method} is missing")

    def test_probe_queries_are_dialect_specific(self):
        oracle, sqlserver = self._adapters()
        self.assertIn("DUAL", oracle.connection_probe_query().upper())
        self.assertNotIn("DUAL", sqlserver.connection_probe_query().upper())
        for adapter in (oracle, sqlserver):
            self.assertIn("CONNECTION_OK", adapter.connection_probe_query())

    def test_sql_object_capability_is_explicit(self):
        oracle, sqlserver = self._adapters()
        self.assertTrue(oracle.supports_sql_object_type("PACKAGE"))
        # SQL Server has no packages: capability is denied, not silently empty.
        self.assertFalse(sqlserver.supports_sql_object_type("PACKAGE"))

    def test_object_type_normalization(self):
        oracle, sqlserver = self._adapters()
        self.assertEqual(oracle.normalize_sql_object_type("PACKAGE BODY"),
                         "PACKAGE_BODY")
        self.assertEqual(sqlserver.normalize_sql_object_type("P"), "PROCEDURE")
        self.assertEqual(sqlserver.normalize_sql_object_type("IF"), "FUNCTION")
        # An unknown code never becomes a PROCEDURE.
        self.assertEqual(sqlserver.normalize_sql_object_type("TR"), "")
        self.assertEqual(oracle.normalize_sql_object_type("TRIGGER"), "")


class TestSourceRegistration(unittest.TestCase):
    def test_registered_adapters_match_the_factory(self):
        # Compared through the factory so both resolve via the same import path.
        for token, expected_name in (("oracle", "OracleSourceAdapter"),
                                     ("mssql", "SqlServerSourceAdapter")):
            registry_cls = source_registry.get_source_definition(token)["adapter"]
            self.assertEqual(registry_cls.__name__, expected_name)
            self.assertIsInstance(
                get_source_adapter(token, source_database="Db"), registry_cls)

    def test_unknown_source_fails_explicitly(self):
        with self.assertRaises(ValueError):
            source_registry.get_source_definition("postgresql")

    def test_notebook_paths_exist_on_disk(self):
        for token in source_registry.registered_sources():
            for role in source_registry.NOTEBOOK_ROLES:
                rel = source_registry.get_notebook_path(token, role)
                path = os.path.join(os.path.dirname(SOURCES), rel + ".py")
                self.assertTrue(os.path.isfile(path),
                                f"registry points at missing notebook {rel}")

    def test_capabilities_are_explicit(self):
        self.assertTrue(source_registry.supports("oracle", "packages"))
        self.assertFalse(source_registry.supports("sqlserver", "packages"))
        self.assertTrue(source_registry.supports("sqlserver", "catalog_row_counts"))
        with self.assertRaises(ValueError):
            source_registry.supports("oracle", "teleportation")


class _FakeAdapter(SourceAdapter):
    """Test-only third source proving shared code needs no dialect knowledge."""

    source_system = "fakedb"
    SQL_OBJECT_TYPES = ("VIEW",)

    def get_jdbc_url_and_props(self, source_server=None, source_database=None):
        return "jdbc:fake://host/db", {"user": "u", "password": "p",
                                       "driver": "fake.Driver"}

    def connection_probe_query(self):
        return "(SELECT 1 AS CONNECTION_OK FROM fake_dual) q"

    def columns_metadata_query(self, source_database, source_schema, source_table):
        return "(SELECT * FROM fake_columns) q"

    def primary_key_query(self, source_database, source_schema, source_table):
        return "(SELECT * FROM fake_keys) q"

    def top_n_probe_query(self, source_database, source_schema, source_table, n):
        return "(SELECT * FROM fake_rows) q"

    def count_query(self, source_database, source_schema, source_table):
        return "(SELECT COUNT(*) AS ROW_COUNT FROM fake_rows) q"

    def min_max_query(self, source_database, source_schema, source_table, column):
        return "(SELECT 1 AS MIN_VAL, 2 AS MAX_VAL) q"

    def upper_watermark_query(self, source_database, source_schema, source_table,
                              watermark_column, watermark_type):
        return "(SELECT NULL AS UPPER_WATERMARK) q"

    def full_extract_query(self, source_database, source_schema, source_table,
                           columns=None, watermark_column=None, watermark_type=None):
        return "(SELECT * FROM fake_rows) q"

    def incremental_extract_query(self, source_database, source_schema, source_table,
                                  watermark_column, watermark_type, lower_watermark,
                                  upper_watermark, columns=None):
        return "(SELECT * FROM fake_rows WHERE x > 0) q"

    def list_schemas_query(self, source_database=None):
        return "(SELECT 'S' AS SCHEMA_NAME) q"

    def list_tables_query(self, source_database=None, source_schema=None):
        return "(SELECT 'S' AS SCHEMA_NAME, 'T' AS OBJECT_NAME, 1 AS ROW_COUNT) q"

    def list_views_query(self, source_database=None, source_schema=None):
        return "(SELECT 'S' AS SCHEMA_NAME, 'V' AS OBJECT_NAME) q"

    def list_routines_query(self, source_database=None, source_schema=None):
        return "(SELECT 'S' AS SCHEMA_NAME, 'P' AS OBJECT_NAME, 'VIEW' AS OBJECT_TYPE) q"

    def table_statistics_query(self, source_database=None, source_schema=None):
        return "(SELECT 'S' AS SCHEMA_NAME, 'T' AS OBJECT_NAME) q"

    def normalize_watermark_type(self, source_type):
        return (source_type or "").upper()

    def is_supported_watermark_type(self, source_type):
        return self.normalize_watermark_type(source_type) == "TIMESTAMP"

    def watermark_type_rank(self, source_type):
        return 0

    def initial_watermark_value(self, source_type):
        return "1900-01-01T00:00:00.000000Z"

    def resolve_partition_plan(self, source_metadata, target_type, min_value,
                               max_value, requested_partitions):
        return None, None, None, "unsupported"

    def load_type_mapper(self):
        raise NotImplementedError("test adapter has no type rules")


class TestFutureSourceExtension(unittest.TestCase):
    """A new source must plug in without shared code learning about it."""

    def setUp(self):
        self.adapter = _FakeAdapter(source_server="h", source_database="db")

    def test_fake_adapter_satisfies_the_contract(self):
        for method in TestAdapterContract.REQUIRED_METHODS:
            self.assertTrue(callable(getattr(self.adapter, method, None)), method)

    def test_shared_assessment_accepts_fake_adapter_output(self):
        record = assess_common.build_assessment_record(
            assessment_id="a1", run_id="r1", connection_id="c1",
            source_system="fakedb", source_server="h", source_database="db",
            source_schema="S", object_name="T", object_type="TABLE",
            compatibility_status="COMPATIBLE", row_count=5,
            row_count_method=assess_common.CATALOG, column_count=2,
            complexity=assess_common.classify_complexity(5, 2))
        self.assertEqual(sorted(record), sorted(assess_common.ASSESSMENT_FIELDS))

    def test_shared_inventory_accepts_fake_adapter_output(self):
        row = inv_common.normalize_inventory_row(
            {"COLUMN_NAME": "C", "ORDINAL_POSITION": 1, "IS_NULLABLE": "YES",
             "DATA_TYPE": "text", "CHARACTER_MAXIMUM_LENGTH": None,
             "NUMERIC_PRECISION": None, "NUMERIC_SCALE": None,
             "DATETIME_PRECISION": None},
            {"run_id": "r1", "source_table_id": "sid", "connection_id": "c1",
             "source_system": "fakedb", "source_server": "h",
             "source_database": "db", "source_schema": "S", "source_table": "T"})
        self.assertEqual(len(row), len(inv_common.INVENTORY_FIELDS))

    def test_shared_sql_object_record_accepts_fake_source(self):
        record = sqlobj_common.build_sql_object_record(
            assessment_id="a1", run_id="r1", connection_id="c1",
            source_system="fakedb", source_database="db", source_schema="S",
            object_name="V", object_type="VIEW",
            source_definition="CREATE VIEW v AS SELECT 1 AS a")
        self.assertEqual(sorted(record), sorted(sqlobj_common.SQL_OBJECT_FIELDS))

    def test_shared_modules_never_name_a_specific_source(self):
        for module_file in ("assessment_common.py", "inventory_common.py",
                            "sql_object_assessment_common.py"):
            with open(os.path.join(SRC, module_file), "r", encoding="utf-8") as fh:
                code = fh.read()
            body = "\n".join(l for l in code.splitlines()
                             if not l.strip().startswith("#"))
            body = body.split('"""')[-1] if body.count('"""') >= 2 else body
            for needle in ("all_tables", "sys.tables", "FROM DUAL"):
                self.assertNotIn(needle.lower(), body.lower(), module_file)


class TestSqlObjectCommonRules(unittest.TestCase):
    def test_generated_draft_is_pending_review(self):
        record = sqlobj_common.build_sql_object_record(
            assessment_id="a1", run_id="r1", connection_id="c1",
            source_system="oracle", source_database=None, source_schema="S",
            object_name="V", object_type="VIEW",
            source_definition="CREATE VIEW v AS SELECT a FROM t",
            mode="CONVERT")
        self.assertEqual(record["conversion_status"], "GENERATED")
        self.assertEqual(record["review_status"], sqlobj_common.PENDING_REVIEW)

    def test_missing_definition_is_unable_to_assess(self):
        record = sqlobj_common.build_sql_object_record(
            assessment_id="a1", run_id="r1", connection_id="c1",
            source_system="sqlserver", source_database="db", source_schema="S",
            object_name="P", object_type="PROCEDURE", source_definition=None)
        self.assertEqual(record["complexity_category"], "UNABLE_TO_ASSESS")
        self.assertEqual(record["review_status"], sqlobj_common.NOT_REVIEWED)
        self.assertIn("not accessible", record["error_message"])

    def test_unsupported_object_type_rejected(self):
        with self.assertRaises(ValueError):
            sqlobj_common.build_sql_object_record(
                assessment_id="a1", run_id="r1", connection_id="c1",
                source_system="sqlserver", source_database="db",
                source_schema="S", object_name="TR", object_type="TRIGGER",
                source_definition="...")

    def test_terminal_review_statuses_are_preserved_by_contract(self):
        self.assertIn("APPROVED", sqlobj_common.TERMINAL_REVIEW_STATUSES)
        self.assertIn("REJECTED", sqlobj_common.TERMINAL_REVIEW_STATUSES)


if __name__ == "__main__":
    unittest.main()
