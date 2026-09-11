"""Regression tests for source-owned datatype mappers."""

import os
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
SRC = os.path.join(ROOT, "src")
for path in (SRC, ROOT):
    if path not in sys.path:
        sys.path.insert(0, path)

from crosssourcetypemapper import (  # noqa: E402
    ColumnMappingResult, CrossSourceTypeMapper, classify_table_compatibility,
)
from source_adapters.base import SourceAdapter  # noqa: E402
from source_adapters.factory import get_source_adapter  # noqa: E402
from src.type_mappers.base import SourceTypeMapper  # noqa: E402
from src.type_mappers.factory import get_type_mapper  # noqa: E402
from src.type_mappers.oracle import OracleTypeMapper  # noqa: E402
from src.type_mappers.sqlserver import SqlServerTypeMapper  # noqa: E402


ORACLE_RULES = os.path.join(ROOT, "config", "type_rules_oracle.yaml")
SQLSERVER_RULES = os.path.join(ROOT, "config", "type_rules_sqlserver.yaml")


def _outcome(result):
    return (result.databricks_delta_type, result.status, result.fidelity)


def _yaml_types(path):
    import yaml
    with open(path, encoding="utf-8") as stream:
        return yaml.safe_load(stream)["types"]


class TestMapperOwnership(unittest.TestCase):
    def test_adapters_return_matching_mapper(self):
        self.assertIsInstance(
            get_source_adapter("oracle").load_type_mapper(), OracleTypeMapper)
        self.assertIsInstance(
            get_source_adapter(
                "sqlserver", source_database="Db").load_type_mapper(),
            SqlServerTypeMapper)

    def test_explicit_factory_and_alias(self):
        self.assertIsInstance(get_type_mapper("oracle"), OracleTypeMapper)
        self.assertIsInstance(get_type_mapper("mssql"), SqlServerTypeMapper)

    def test_unknown_mapper_source_fails(self):
        for source in (None, "", "db2"):
            with self.assertRaises(ValueError):
                get_type_mapper(source)

    def test_compatibility_facade_still_constructs_concrete_mapper(self):
        self.assertIsInstance(
            CrossSourceTypeMapper(dialect="oracle"), OracleTypeMapper)
        self.assertIsInstance(
            CrossSourceTypeMapper(dialect="sqlserver"), SqlServerTypeMapper)
        with self.assertRaises(ValueError):
            CrossSourceTypeMapper(dialect="db2")

    def test_compatibility_exports_are_preserved(self):
        result = ColumnMappingResult(
            "text", "STRING", "AUTO", "EXACT", "", True)
        self.assertEqual(result.status, "AUTO")
        self.assertEqual(
            classify_table_compatibility(["AUTO", "AUTO"]), "COMPATIBLE")

    def test_result_is_immutable(self):
        result = ColumnMappingResult(
            "text", "STRING", "AUTO", "EXACT", "", True)
        with self.assertRaises(Exception):
            result.status = "BLOCKED"

    def test_facade_contains_no_dialect_rules(self):
        with open(os.path.join(SRC, "crosssourcetypemapper.py"),
                  encoding="utf-8") as stream:
            source = stream.read()
        for forbidden in (
                "_BUILTIN_RULES", "_SQLSERVER_BUILTIN_RULES", "NUMBER",
                "VARCHAR2", "rowversion", "datetime2", "tinyint"):
            self.assertNotIn(forbidden, source)

    def test_base_contains_no_source_datatypes(self):
        with open(os.path.join(SRC, "type_mappers", "base.py"),
                  encoding="utf-8") as stream:
            source = stream.read().lower()
        for forbidden in (
                "varchar2", "number(", "rowversion", "datetime2",
                "uniqueidentifier", "tinyint"):
            self.assertNotIn(forbidden, source)

    def test_concrete_mappers_do_not_contain_other_dialect_types(self):
        with open(os.path.join(SRC, "type_mappers", "oracle.py"),
                  encoding="utf-8") as stream:
            oracle_source = stream.read().lower()
        with open(os.path.join(SRC, "type_mappers", "sqlserver.py"),
                  encoding="utf-8") as stream:
            sqlserver_source = stream.read().lower()
        for forbidden in ("rowversion", "datetime2", "tinyint", "uniqueidentifier"):
            self.assertNotIn(forbidden, oracle_source)
        for forbidden in ("varchar2", "nclob", "bfile", "anydata", "vector"):
            self.assertNotIn(forbidden, sqlserver_source)


class TestOracleMappingRegression(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.mapper = OracleTypeMapper.from_yaml_path(ORACLE_RULES)

    def test_oracle_type_matrix(self):
        cases = (
            (("VARCHAR2", None, None), ("STRING", "AUTO", "EXACT")),
            (("CLOB", None, None), ("STRING", "AUTO", "WIDENED")),
            (("DATE", None, None), ("TIMESTAMP", "AUTO", "WIDENED")),
            (("TIMESTAMP(6)", None, None), ("TIMESTAMP", "AUTO", "EXACT")),
            (("TIMESTAMP(6) WITH TIME ZONE", None, None),
             ("TIMESTAMP", "REVIEW", "LOSSY")),
            (("RAW", None, None), ("BINARY", "AUTO", "EXACT")),
            (("BFILE", None, None), ("STRING", "BLOCKED", "UNKNOWN")),
            (("BOOLEAN", None, None), ("BOOLEAN", "AUTO", "EXACT")),
            (("JSON", None, None), ("STRING", "REVIEW", "LOSSY")),
            (("VECTOR", None, None), (None, "BLOCKED", "UNKNOWN")),
            (("APP_CUSTOM_TYPE", None, None),
             (None, "BLOCKED", "UNKNOWN")),
        )
        for args, expected in cases:
            with self.subTest(source_type=args[0]):
                self.assertEqual(
                    _outcome(self.mapper.map_column(
                        args[0], precision=args[1], scale=args[2])), expected)

    def test_number_precision_scale_regression(self):
        cases = (
            ((None, None), ("DECIMAL(38,0)", "AUTO", "EXACT")),
            ((4, 0), ("SMALLINT", "AUTO", "EXACT")),
            ((9, 0), ("INT", "AUTO", "EXACT")),
            ((18, 0), ("BIGINT", "AUTO", "EXACT")),
            ((19, 0), ("DECIMAL(19,0)", "AUTO", "EXACT")),
            ((10, 2), ("DECIMAL(10,2)", "AUTO", "EXACT")),
            ((None, 2), ("DECIMAL(38,10)", "REVIEW", "WIDENED")),
            ((10, -2), ("DECIMAL(12,0)", "REVIEW", "WIDENED")),
            ((40, 0), ("STRING", "BLOCKED", "UNKNOWN")),
        )
        for (precision, scale), expected in cases:
            with self.subTest(precision=precision, scale=scale):
                self.assertEqual(
                    _outcome(self.mapper.map_column(
                        "NUMBER", precision=precision, scale=scale)), expected)

    def test_every_yaml_rule_outcome_and_notes(self):
        for source_type, rule in _yaml_types(ORACLE_RULES).items():
            if source_type.lower() == "number":
                continue
            with self.subTest(source_type=source_type):
                result = self.mapper.map_column(source_type)
                self.assertEqual(result.databricks_delta_type,
                                 rule.get("databricks_delta"))
                self.assertEqual(result.status, rule["status"])
                self.assertEqual(result.fidelity, rule["fidelity"])
                self.assertEqual(result.notes, rule.get("notes") or "")


class TestSqlServerMappingRegression(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.mapper = SqlServerTypeMapper.from_yaml_path(SQLSERVER_RULES)

    def test_sqlserver_type_matrix(self):
        cases = (
            ("bit", ("BOOLEAN", "AUTO", "EXACT")),
            ("tinyint", ("SMALLINT", "AUTO", "WIDENED")),
            ("int", ("INT", "AUTO", "EXACT")),
            ("bigint", ("BIGINT", "AUTO", "EXACT")),
            ("money", ("DECIMAL(19,4)", "AUTO", "EXACT")),
            ("smallmoney", ("DECIMAL(10,4)", "AUTO", "EXACT")),
            ("varchar(max)", ("STRING", "AUTO", "EXACT")),
            ("datetime2(7)", ("TIMESTAMP", "AUTO", "LOSSY")),
            ("timestamp", ("BINARY", "AUTO", "EXACT")),
            ("rowversion", ("BINARY", "AUTO", "EXACT")),
            ("uniqueidentifier", ("STRING", "AUTO", "WIDENED")),
            ("hierarchyid", (None, "BLOCKED", "UNKNOWN")),
            ("geometry", (None, "BLOCKED", "UNKNOWN")),
            ("APP_CUSTOM_TYPE", (None, "BLOCKED", "UNKNOWN")),
        )
        for source_type, expected in cases:
            with self.subTest(source_type=source_type):
                self.assertEqual(
                    _outcome(self.mapper.map_column(source_type)), expected)

    def test_decimal_precision_scale_regression(self):
        cases = (
            ((None, None), ("DECIMAL(18,0)", "AUTO", "EXACT")),
            ((10, 2), ("DECIMAL(10,2)", "AUTO", "EXACT")),
            ((10, -1), ("DECIMAL(10,0)", "AUTO", "EXACT")),
            ((4, 7), ("DECIMAL(4,4)", "AUTO", "EXACT")),
            ((39, 0), (None, "BLOCKED", "UNKNOWN")),
            (("bad", 2), (None, "BLOCKED", "UNKNOWN")),
        )
        for (precision, scale), expected in cases:
            with self.subTest(precision=precision, scale=scale):
                self.assertEqual(
                    _outcome(self.mapper.map_column(
                        "decimal", precision=precision, scale=scale)), expected)

    def test_every_yaml_rule_outcome_and_notes(self):
        for source_type, rule in _yaml_types(SQLSERVER_RULES).items():
            with self.subTest(source_type=source_type):
                result = self.mapper.map_column(source_type)
                self.assertEqual(result.databricks_delta_type,
                                 rule.get("databricks_delta"))
                self.assertEqual(result.status, rule["status"])
                self.assertEqual(result.fidelity, rule["fidelity"])
                self.assertEqual(result.notes, rule.get("notes") or "")


class TestYamlValidation(unittest.TestCase):
    def _write(self, body):
        handle = tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False, encoding="utf-8")
        self.addCleanup(lambda: os.path.exists(handle.name)
                        and os.unlink(handle.name))
        with handle:
            handle.write(body)
        return handle.name

    def test_source_mismatch_fails(self):
        with self.assertRaises(ValueError) as context:
            OracleTypeMapper.from_yaml_path(SQLSERVER_RULES)
        self.assertIn("does not match", str(context.exception))

    def test_target_mismatch_fails(self):
        path = self._write(
            "source_dialect: oracle\ntarget: parquet\ntypes: {}\n")
        with self.assertRaises(ValueError):
            OracleTypeMapper.from_yaml_path(path)

    def test_invalid_status_and_fidelity_fail(self):
        invalid_status = self._write(
            "source_dialect: oracle\ntarget: databricks_delta\n"
            "types:\n  varchar2:\n    databricks_delta: STRING\n"
            "    status: MAYBE\n    fidelity: EXACT\n")
        with self.assertRaises(ValueError):
            OracleTypeMapper.from_yaml_path(invalid_status)

        invalid_fidelity = self._write(
            "source_dialect: sqlserver\ntarget: databricks_delta\n"
            "types:\n  int:\n    databricks_delta: INT\n"
            "    status: AUTO\n    fidelity: PERFECT\n")
        with self.assertRaises(ValueError):
            SqlServerTypeMapper.from_yaml_path(invalid_fidelity)

    def test_malformed_yaml_fails_without_content_echo(self):
        secret = "password=DoNotEcho123"
        path = self._write(
            "source_dialect: oracle\ntarget: databricks_delta\n"
            f"types: [unterminated {secret}\n")
        with self.assertRaises(ValueError) as context:
            OracleTypeMapper.from_yaml_path(path)
        self.assertNotIn("DoNotEcho123", str(context.exception))


class _TestMapper(SourceTypeMapper):
    def map_column(self, source_type, precision=None, scale=None,
                   length=None, is_nullable=True):
        return ColumnMappingResult(
            source_type or "", "STRING", "AUTO", "EXACT", "test", is_nullable)


class _TestAdapter:
    def load_type_mapper(self):
        return _TestMapper()


class TestFutureSourceMapper(unittest.TestCase):
    def test_adapter_can_supply_test_mapper_without_existing_mapper_edits(self):
        mapper = _TestAdapter().load_type_mapper()
        self.assertIsInstance(mapper, SourceTypeMapper)
        self.assertEqual(_outcome(mapper.map_column("future_text")),
                         ("STRING", "AUTO", "EXACT"))


if __name__ == "__main__":
    unittest.main()