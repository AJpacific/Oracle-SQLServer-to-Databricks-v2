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
            (("TIMESTAMP(3)", None, None), ("TIMESTAMP", "AUTO", "EXACT")),
            (("TIMESTAMP(7)", None, None), ("TIMESTAMP", "REVIEW", "LOSSY")),
            (("TIMESTAMP(9)", None, None), ("TIMESTAMP", "REVIEW", "LOSSY")),
            (("TIMESTAMP(6) WITH TIME ZONE", None, None),
             ("TIMESTAMP", "REVIEW", "LOSSY")),
            (("TIMESTAMP(6) WITH LOCAL TIME ZONE", None, None),
             ("TIMESTAMP", "REVIEW", "LOSSY")),
            (("INTERVAL YEAR TO MONTH", None, None), ("STRING", "REVIEW", "LOSSY")),
            (("INTERVAL DAY TO SECOND", None, None), ("STRING", "REVIEW", "LOSSY")),
            (("LONG", None, None), ("STRING", "REVIEW", "LOSSY")),
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

    def test_oracle_date_mapping(self):
        res_null = self.mapper.map_column("DATE", is_nullable=True)
        self.assertEqual(_outcome(res_null), ("TIMESTAMP", "AUTO", "WIDENED"))
        self.assertTrue(res_null.is_nullable)
        self.assertIn("second precision", res_null.notes.lower())

        res_not_null = self.mapper.map_column("date", is_nullable=False)
        self.assertEqual(_outcome(res_not_null), ("TIMESTAMP", "AUTO", "WIDENED"))
        self.assertFalse(res_not_null.is_nullable)

    def test_timestamp_precision_mapping_regression(self):
        # TIMESTAMP(0) through TIMESTAMP(6): AUTO, EXACT, TIMESTAMP
        for p in range(7):
            with self.subTest(precision=p):
                res = self.mapper.map_column("TIMESTAMP", scale=p, is_nullable=True)
                self.assertEqual(_outcome(res), ("TIMESTAMP", "AUTO", "EXACT"))
                self.assertTrue(res.is_nullable)

                res_not_null = self.mapper.map_column("TIMESTAMP", scale=p, is_nullable=False)
                self.assertFalse(res_not_null.is_nullable)

                # Form with embedded precision in type string
                res_embedded = self.mapper.map_column(f"TIMESTAMP({p})")
                self.assertEqual(_outcome(res_embedded), ("TIMESTAMP", "AUTO", "EXACT"))

        # TIMESTAMP(7) through TIMESTAMP(9): REVIEW, LOSSY, TIMESTAMP
        for p in (7, 8, 9):
            with self.subTest(high_precision=p):
                res_high = self.mapper.map_column("TIMESTAMP", scale=p)
                self.assertEqual(_outcome(res_high), ("TIMESTAMP", "REVIEW", "LOSSY"))
                self.assertIn("truncated", res_high.notes.lower())

                res_emb_high = self.mapper.map_column(f"TIMESTAMP({p})")
                self.assertEqual(_outcome(res_emb_high), ("TIMESTAMP", "REVIEW", "LOSSY"))

        # missing precision: REVIEW, UNKNOWN, TIMESTAMP
        res_none = self.mapper.map_column("TIMESTAMP", scale=None)
        self.assertEqual(_outcome(res_none), ("TIMESTAMP", "REVIEW", "UNKNOWN"))
        self.assertIn("unavailable", res_none.notes.lower())

        # invalid / unsupported precision: BLOCKED, UNKNOWN, None
        for bad in (-1, 10, 99, "bad"):
            with self.subTest(bad=bad):
                res_bad = self.mapper.map_column("TIMESTAMP", scale=bad)
                self.assertEqual(_outcome(res_bad), (None, "BLOCKED", "UNKNOWN"))

        # builtin fallback consistency when YAML is absent
        builtin_mapper = OracleTypeMapper()
        self.assertEqual(_outcome(builtin_mapper.map_column("TIMESTAMP", scale=6)),
                         ("TIMESTAMP", "AUTO", "EXACT"))
        self.assertEqual(_outcome(builtin_mapper.map_column("TIMESTAMP", scale=None)),
                         ("TIMESTAMP", "REVIEW", "UNKNOWN"))
        self.assertEqual(_outcome(builtin_mapper.map_column("DATE")),
                         ("TIMESTAMP", "AUTO", "WIDENED"))
        self.assertEqual(_outcome(builtin_mapper.map_column("LONG")),
                         ("STRING", "REVIEW", "LOSSY"))
        self.assertEqual(_outcome(builtin_mapper.map_column("INTERVAL YEAR TO MONTH")),
                         ("STRING", "REVIEW", "LOSSY"))

    def test_oracle_does_not_define_standalone_time_rule(self):
        yaml_rules = _yaml_types(ORACLE_RULES)
        self.assertNotIn("time", yaml_rules)
        res = self.mapper.map_column("time")
        self.assertEqual(res.status, "BLOCKED")
        self.assertIsNone(res.databricks_delta_type)

    def test_oracle_timestamp_metadata_pipeline_integration(self):
        import inventory_common as inv
        adapter = get_source_adapter("oracle")
        # Simulating row from Oracle columns_metadata_query
        raw_metadata_col = {
            "COLUMN_NAME": "CREATED_TS",
            "ORDINAL_POSITION": 3,
            "IS_NULLABLE": "YES",
            "DATA_TYPE": "TIMESTAMP(6)",
            "CHARACTER_MAXIMUM_LENGTH": None,
            "NUMERIC_PRECISION": None,
            "NUMERIC_SCALE": 6,
            "DATETIME_PRECISION": 6,
        }
        identity = {
            "run_id": "r1", "source_table_id": "conn1__hr__emp", "connection_id": "conn1",
            "source_system": "oracle", "source_server": "srv", "source_database": "xe",
            "source_schema": "hr", "source_table": "emp"
        }
        inv_row = inv.normalize_inventory_row(raw_metadata_col, identity)
        inv_dict = dict(zip(inv.INVENTORY_FIELDS, inv_row))
        # Confirm numeric_scale carries data_scale 6
        self.assertEqual(inv_dict["numeric_scale"], 6)

        # Simulate NB03 mapping generation:
        mapping_res = adapter.load_type_mapper().map_column(
            source_type=inv_dict["data_type"],
            precision=inv_dict["numeric_precision"],
            scale=inv_dict["numeric_scale"],
            length=inv_dict["character_maximum_length"],
            is_nullable=(inv_dict["is_nullable"] == "YES"),
        )
        self.assertEqual(mapping_res.databricks_delta_type, "TIMESTAMP")
        self.assertEqual(mapping_res.status, "AUTO")
        self.assertEqual(mapping_res.fidelity, "EXACT")

        # Apply column policy:
        policy = adapter.apply_column_policy(inv_dict, mapping_res)
        self.assertTrue(policy.include_column)
        self.assertTrue(policy.is_writable)
        self.assertFalse(policy.requires_review)
        self.assertEqual(policy.mapping_status, "AUTO")

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
            if source_type.lower() == "timestamp":
                # Fallback rule validation:
                result = self.mapper.map_column(source_type)
                self.assertEqual(result.databricks_delta_type, "TIMESTAMP")
                self.assertEqual(result.status, "REVIEW")
                self.assertEqual(result.fidelity, "UNKNOWN")
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
            ("text", ("STRING", "AUTO", "EXACT")),
            ("ntext", ("STRING", "AUTO", "EXACT")),
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

    def test_time_precision_mapping_regression(self):
        # time(0) through time(6): AUTO, EXACT, TIME(p)
        for p in range(7):
            with self.subTest(precision=p):
                res = self.mapper.map_column("time", scale=p, is_nullable=True)
                self.assertEqual(_outcome(res), (f"TIME({p})", "AUTO", "EXACT"))
                self.assertTrue(res.is_nullable)

                res_not_null = self.mapper.map_column("time", scale=p, is_nullable=False)
                self.assertFalse(res_not_null.is_nullable)

        # time(7): REVIEW, LOSSY, TIME(6), explains 7th digit truncation
        res7 = self.mapper.map_column("time", scale=7)
        self.assertEqual(_outcome(res7), ("TIME(6)", "REVIEW", "LOSSY"))
        self.assertIn("seventh", res7.notes.lower())

        # missing scale: REVIEW, UNKNOWN, TIME(6), explains precision unavailable
        res_none = self.mapper.map_column("time", scale=None)
        self.assertEqual(_outcome(res_none), ("TIME(6)", "REVIEW", "UNKNOWN"))
        self.assertIn("unavailable", res_none.notes.lower())

        # invalid / unsupported scale: BLOCKED, UNKNOWN, None
        for bad_scale in (-1, 8, 99, "bad"):
            with self.subTest(bad_scale=bad_scale):
                res_bad = self.mapper.map_column("time", scale=bad_scale)
                self.assertEqual(_outcome(res_bad), (None, "BLOCKED", "UNKNOWN"))

        # builtin rules fallback consistency when YAML is absent
        builtin_mapper = SqlServerTypeMapper()
        self.assertEqual(_outcome(builtin_mapper.map_column("time", scale=3)),
                         ("TIME(3)", "AUTO", "EXACT"))
        self.assertEqual(_outcome(builtin_mapper.map_column("time", scale=None)),
                         ("TIME(6)", "REVIEW", "UNKNOWN"))
        self.assertEqual(_outcome(builtin_mapper.map_column("text")),
                         ("STRING", "AUTO", "EXACT"))
        self.assertEqual(_outcome(builtin_mapper.map_column("ntext")),
                         ("STRING", "AUTO", "EXACT"))

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
            if source_type.lower() == "time":
                # time precision-aware dispatch takes precedence over generic YAML notes
                result = self.mapper.map_column(source_type)
                self.assertEqual(result.databricks_delta_type, "TIME(6)")
                self.assertEqual(result.status, "REVIEW")
                self.assertEqual(result.fidelity, "UNKNOWN")
                continue
            with self.subTest(source_type=source_type):
                result = self.mapper.map_column(source_type)
                self.assertEqual(result.databricks_delta_type,
                                 rule.get("databricks_delta"))
                self.assertEqual(result.status, rule["status"])
                self.assertEqual(result.fidelity, rule["fidelity"])
                self.assertEqual(result.notes, rule.get("notes") or "")

    def test_time_precision_metadata_pipeline_integration(self):
        import inventory_common as inv
        adapter = get_source_adapter("sqlserver", source_database="TestDb")
        raw_metadata_col = {
            "COLUMN_NAME": "event_time",
            "ORDINAL_POSITION": 2,
            "IS_NULLABLE": "YES",
            "DATA_TYPE": "time",
            "CHARACTER_MAXIMUM_LENGTH": None,
            "NUMERIC_PRECISION": 16,
            "NUMERIC_SCALE": 3,
            "DATETIME_PRECISION": 3,
            "IS_IDENTITY": 0,
            "IS_COMPUTED": 0,
            "IS_HIDDEN": 0,
            "IS_ROWVERSION": 0,
            "SOURCE_TYPE_SCHEMA": "sys",
        }
        identity = {
            "run_id": "r1", "source_table_id": "conn1__dbo__events", "connection_id": "conn1",
            "source_system": "sqlserver", "source_server": "srv", "source_database": "TestDb",
            "source_schema": "dbo", "source_table": "events"
        }
        inv_row = inv.normalize_inventory_row(raw_metadata_col, identity)
        inv_dict = dict(zip(inv.INVENTORY_FIELDS, inv_row))
        # Confirm numeric_scale carries the scale 3
        self.assertEqual(inv_dict["numeric_scale"], 3)
        # Simulate NB03 mapping generation:
        mapping_res = adapter.load_type_mapper().map_column(
            source_type=inv_dict["data_type"],
            precision=inv_dict["numeric_precision"],
            scale=inv_dict["numeric_scale"],
            length=inv_dict["character_maximum_length"],
            is_nullable=(inv_dict["is_nullable"] == "YES"),
        )
        self.assertEqual(mapping_res.databricks_delta_type, "TIME(3)")
        self.assertEqual(mapping_res.status, "AUTO")
        self.assertEqual(mapping_res.fidelity, "EXACT")
        # Apply column policy
        policy = adapter.apply_column_policy(inv_dict, mapping_res)
        self.assertTrue(policy.include_column)
        self.assertTrue(policy.is_writable)
        self.assertFalse(policy.requires_review)
        self.assertEqual(policy.mapping_status, "AUTO")


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