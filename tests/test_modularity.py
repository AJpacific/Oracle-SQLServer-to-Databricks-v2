"""
Modularity tests (Part J): prove the shared/source boundary actually holds.

These assert behavior-relevant properties (no source dialect leaking into shared
notebooks, matching contracts across sources, a working adapter contract, and
that a brand-new source needs no shared-code change) rather than merely checking
that files exist.
"""

import ast
import os
import re
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.join(os.path.dirname(HERE), "src")
for p in (SRC, HERE, os.path.dirname(HERE)):
    if p not in sys.path:
        sys.path.insert(0, p)

import _modscan as modscan  # noqa: E402
import _nbvalidate as nbvalidate  # noqa: E402
from _nbsource import (  # noqa: E402
    SHARED as SHARED_DIR, SOURCES, SOURCE_TOKENS, REQUIRED_SOURCE_NOTEBOOKS,
    SHARED_NOTEBOOKS, shared_nb, source_nb, source_nb_path,
    all_shared_notebooks, all_source_notebooks,
)
import source_registry  # noqa: E402
import assessment_common as assess_common  # noqa: E402
import inventory_common as inv_common  # noqa: E402
import sql_object_assessment_common as sqlobj_common  # noqa: E402
from crosssourcetypemapper import ColumnMappingResult  # noqa: E402
from type_mappers.base import SourceTypeMapper  # noqa: E402
from source_adapters.factory import get_source_adapter  # noqa: E402
from source_adapters.base import (  # noqa: E402
    SourceAdapter, ColumnPolicyResult, SOURCE_HIDDEN_COLUMN,
    SOURCE_GENERATED_COLUMN, SOURCE_BINARY_VERSION_COLUMN,
)


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
    """Shared executable code must contain no source dialect or credential."""

    def test_shared_notebooks_have_no_modularity_findings(self):
        for name in SHARED_NOTEBOOKS:
            path = os.path.join(SHARED_DIR, name)
            findings = modscan.scan(path, is_notebook=True)
            self.assertEqual(findings, [], f"{name}: {findings}")

    def test_shared_common_has_no_modularity_findings(self):
        path = os.path.join(SHARED_DIR, "_common.py")
        findings = modscan.scan(path, is_notebook=True)
        self.assertEqual(findings, [], f"_common.py: {findings}")

    def test_scanner_detects_normalize_source_system_comparison(self):
        # Guard the guard: the AST scan must catch the wrapped-call form that a
        # plain substring search for 'source_system ==' would miss.
        tree = ast.parse(
            'if normalize_source_system(src_system) == "sqlserver":\n    pass\n')
        findings = modscan.find_source_branches(tree, "<synthetic>")
        self.assertEqual(len(findings), 1)
        self.assertIn("sqlserver", findings[0].construct)

    def test_scanner_detects_row_key_comparison(self):
        tree = ast.parse('if row["source_system"] == "oracle":\n    pass\n')
        self.assertEqual(len(modscan.find_source_branches(tree, "<synthetic>")), 1)

    def test_scanner_detects_membership_branch(self):
        tree = ast.parse('if src_system in ("oracle", "db2"):\n    pass\n')
        self.assertEqual(len(modscan.find_source_branches(tree, "<synthetic>")), 1)

    def test_scanner_detects_implicit_source_default(self):
        tree = ast.parse('selected = row.get("source_system") or "oracle"\n')
        findings = modscan.find_source_defaults(tree, "<synthetic>")
        self.assertEqual(len(findings), 1)
        self.assertIn("default", findings[0].construct)

    def test_scanner_detects_dialect_sql_and_credentials(self):
        tree = ast.parse('q = "SELECT * FROM sys.tables"\nk = "oracle-password"\n')
        findings = modscan.find_dialect_strings(tree, "<synthetic>")
        self.assertEqual(len(findings), 2)

    def test_scanner_detects_forbidden_builder_import(self):
        tree = ast.parse("import sqlserver_sql_builder as ssb\n")
        self.assertEqual(len(modscan.find_forbidden_imports(tree, "<synthetic>")), 1)

    def test_scanner_detects_concrete_adapter_construction(self):
        tree = ast.parse("a = OracleSourceAdapter()\n")
        self.assertEqual(
            len(modscan.find_concrete_adapter_use(tree, "<synthetic>")), 1)

    def test_findings_report_file_line_and_owner(self):
        tree = ast.parse('x = 1\nif source_system == "oracle":\n    pass\n')
        finding = modscan.find_source_branches(tree, "/tmp/NBx.py")[0]
        self.assertEqual(finding.line, 2)
        self.assertIn("NBx.py:2", str(finding))
        self.assertIn("adapter", str(finding))

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

    def test_legacy_scope_is_not_in_shared_bootstrap(self):
        code = shared_nb("_common.py")
        self.assertNotIn("legacy_secret_scope_widget()", code)
        self.assertNotIn("oracle-migration", code)
        self.assertNotIn("sqlserver-migration", code)

    def test_common_does_not_route_scope_or_rules_by_source(self):
        code = _strip_markdown(shared_nb("_common.py"))
        self.assertNotIn("def _scope_for_system(", code)
        self.assertNotIn("def _type_rules_path_for(", code)
        # Type rules come from the adapter; operational scope comes from the
        # registered connection row.
        self.assertIn("adapter.type_rules_file()", code)
        self.assertNotIn("legacy_secret_scope_widget()", code)


class TestColumnPolicy(unittest.TestCase):
    """Column policy is owned by the adapter, not by shared notebooks."""

    @staticmethod
    def _mapping(status="AUTO", fidelity="EXACT", notes="", dtype="STRING"):
        return ColumnMappingResult(
            source_type="varchar", databricks_delta_type=dtype, status=status,
            fidelity=fidelity, notes=notes, is_nullable=True)

    def _oracle(self):
        return get_source_adapter("oracle")

    def _sqlserver(self):
        return get_source_adapter("sqlserver", source_database="Db")

    def test_base_policy_preserves_mapping(self):
        policy = SourceAdapter.apply_column_policy(
            self._oracle(), {"column_name": "C"}, self._mapping())
        self.assertTrue(policy.include_column)
        self.assertTrue(policy.is_writable)
        self.assertFalse(policy.requires_review)
        self.assertEqual(policy.mapping_status, "AUTO")

    def test_base_policy_marks_review(self):
        policy = SourceAdapter.apply_column_policy(
            self._oracle(), {"column_name": "C"}, self._mapping(status="REVIEW"))
        self.assertTrue(policy.requires_review)

    def test_oracle_policy_ignores_other_source_flags(self):
        # Oracle must not acquire SQL Server metadata semantics.
        policy = self._oracle().apply_column_policy(
            {"column_name": "C", "is_hidden": 1, "is_computed": 1},
            self._mapping())
        self.assertTrue(policy.include_column)
        self.assertTrue(policy.is_writable)
        self.assertIsNone(policy.policy_code)

    def test_sqlserver_hidden_column_excluded(self):
        policy = self._sqlserver().apply_column_policy(
            {"column_name": "C", "is_hidden": 1}, self._mapping())
        self.assertFalse(policy.include_column)
        self.assertFalse(policy.is_writable)
        self.assertEqual(policy.mapping_status, "BLOCKED")
        self.assertEqual(policy.policy_code, SOURCE_HIDDEN_COLUMN)

    def test_sqlserver_computed_column_requires_review(self):
        policy = self._sqlserver().apply_column_policy(
            {"column_name": "C", "is_computed": 1}, self._mapping())
        self.assertTrue(policy.include_column)
        self.assertFalse(policy.is_writable)
        self.assertEqual(policy.mapping_status, "REVIEW")
        self.assertTrue(policy.requires_review)
        self.assertEqual(policy.policy_code, SOURCE_GENERATED_COLUMN)

    def test_sqlserver_rowversion_keeps_mapping_but_not_writable(self):
        policy = self._sqlserver().apply_column_policy(
            {"column_name": "V", "is_rowversion": 1},
            self._mapping(dtype="BINARY"))
        self.assertTrue(policy.include_column)
        self.assertFalse(policy.is_writable)
        self.assertEqual(policy.mapping_status, "AUTO")
        self.assertEqual(policy.policy_code, SOURCE_BINARY_VERSION_COLUMN)

    def test_sqlserver_ordinary_column_unchanged(self):
        policy = self._sqlserver().apply_column_policy(
            {"column_name": "C"}, self._mapping())
        self.assertTrue(policy.include_column)
        self.assertTrue(policy.is_writable)
        self.assertIsNone(policy.policy_code)

    def test_hidden_takes_precedence_over_computed(self):
        policy = self._sqlserver().apply_column_policy(
            {"column_name": "C", "is_hidden": 1, "is_computed": 1},
            self._mapping())
        self.assertEqual(policy.policy_code, SOURCE_HIDDEN_COLUMN)

    def test_flags_accept_string_and_int_forms(self):
        for raw in (1, True, "1", "true", "YES"):
            policy = self._sqlserver().apply_column_policy(
                {"column_name": "C", "is_hidden": raw}, self._mapping())
            self.assertFalse(policy.include_column, raw)
        for raw in (0, False, "0", "false", None):
            policy = self._sqlserver().apply_column_policy(
                {"column_name": "C", "is_hidden": raw}, self._mapping())
            self.assertTrue(policy.include_column, raw)

    def test_policy_result_is_immutable(self):
        policy = self._sqlserver().apply_column_policy(
            {"column_name": "C"}, self._mapping())
        with self.assertRaises(Exception):
            policy.include_column = False

    def test_shared_mapping_consumes_adapter_policy(self):
        code = shared_nb("NB03_MappingRulesGeneration.py")
        self.assertIn("adapter.apply_column_policy(", code)
        self.assertIn("policy.mapping_status", code)
        self.assertIn("policy.policy_code", code)

    def test_shared_validation_uses_canonical_fields(self):
        code = shared_nb("NB04_MappingValidation.py")
        for field in ("include_column", "is_writable", "requires_review",
                      "policy_code"):
            self.assertIn(field, code)
        self.assertNotIn("SQLSERVER_HIDDEN_COLUMN", code)
        self.assertNotIn("SQLSERVER_COMPUTED_COLUMN", code)


class TestTypeRulesResolution(unittest.TestCase):
    def test_oracle_names_its_rules_file(self):
        self.assertEqual(get_source_adapter("oracle").type_rules_file(),
                         "type_rules_oracle.yaml")

    def test_sqlserver_names_its_rules_file(self):
        adapter = get_source_adapter("sqlserver", source_database="Db")
        self.assertEqual(adapter.type_rules_file(), "type_rules_sqlserver.yaml")

    def test_named_rules_files_exist(self):
        config = os.path.join(os.path.dirname(HERE), "config")
        for token, database in (("oracle", None), ("sqlserver", "Db")):
            adapter = get_source_adapter(token, source_database=database)
            self.assertTrue(
                os.path.isfile(os.path.join(config, adapter.type_rules_file())),
                adapter.type_rules_file())

    def test_unknown_source_fails_and_never_defaults(self):
        for unknown in ("postgresql", "db2", "", None):
            with self.assertRaises(ValueError):
                get_source_adapter(unknown)

    def test_sqlserver_requires_database_metadata(self):
        adapter = get_source_adapter("sqlserver", source_database="Db")
        with self.assertRaises(ValueError):
            adapter.validate_connection_metadata(
                {"connection_id": "c1", "secret_scope": "s", "source_database": ""})

    def test_blank_secret_scope_fails_without_leaking(self):
        adapter = get_source_adapter("oracle")
        with self.assertRaises(ValueError) as ctx:
            adapter.validate_connection_metadata(
                {"connection_id": "c1", "secret_scope": "  "})
        message = str(ctx.exception)
        self.assertIn("secret_scope", message)
        self.assertNotIn("password", message.lower())


class TestNotebookValidation(unittest.TestCase):
    """Static notebook checks that compileall cannot perform."""

    def test_every_notebook_cell_compiles(self):
        for path in nbvalidate.all_notebook_paths():
            errors = nbvalidate.compile_cells(path)
            self.assertEqual(errors, [], f"{os.path.basename(path)}: {errors}")

    def test_every_run_target_exists(self):
        for path in nbvalidate.all_notebook_paths():
            for directive, resolved in nbvalidate.run_targets(path):
                self.assertTrue(
                    os.path.isfile(resolved),
                    f"{os.path.relpath(path)}: %run {directive} -> missing "
                    f"{os.path.relpath(resolved)}")

    def test_source_notebooks_resolve_shared_bootstrap(self):
        for token, name in all_source_notebooks():
            path = source_nb_path(token, name)
            targets = nbvalidate.run_targets(path)
            self.assertTrue(targets, f"{token}/{name} has no %run")
            self.assertTrue(any(t.endswith("_common") for t, _ in targets),
                            f"{token}/{name} does not bootstrap shared/_common")

    def test_assessment_notebooks_resolve_the_classifier(self):
        # The exact defect class: a symbol used but never provided anywhere.
        import builtins
        common_exports = nbvalidate.exported_names(
            os.path.join(SHARED_DIR, "_common.py"))
        self.assertIn("classify_table_compatibility", common_exports)
        self.assertIn("assess_common", common_exports)
        ambient = set(dir(builtins)) | {"spark", "dbutils", "display", "sc"}
        for token in SOURCE_TOKENS:
            path = source_nb_path(token, "NB01A_SourceAssessment.py")
            undefined = (nbvalidate.referenced_names(path)
                         - common_exports - ambient)
            self.assertEqual(
                undefined, set(),
                f"{token}/NB01A references names no shared symbol provides: "
                f"{sorted(undefined)}")

    def test_single_authoritative_classifier(self):
        definitions = []
        for dirpath, dirnames, filenames in os.walk(os.path.dirname(HERE)):
            dirnames[:] = [d for d in dirnames
                           if d not in ("__pycache__", ".git", "tests")]
            for name in filenames:
                if not name.endswith(".py"):
                    continue
                full = os.path.join(dirpath, name)
                with open(full, "r", encoding="utf-8") as fh:
                    if "def classify_table_compatibility(" in fh.read():
                        definitions.append(full)
        self.assertEqual(len(definitions), 1,
                         f"expected one classifier definition, found {definitions}")


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
        keys = set(re.findall(r'"([a-z_]+)":', tail))
        if keys:
            return keys
        result = re.search(r"json\.dumps\(([A-Za-z_][A-Za-z0-9_]*)\)", tail)
        if not result:
            return set()
        assignment = code.rsplit(f"{result.group(1)} = {{", 1)
        if len(assignment) != 2:
            return set()
        result_dict = assignment[1].split("\n}", 1)[0]
        return set(re.findall(r'"([a-z_]+)":', result_dict))

    def test_connection_notebook_contract(self):
        widgets = [self._widgets(source_nb(t, "NB00A_UpsertAndValidateConnection.py"))
                   for t in SOURCE_TOKENS]
        self.assertEqual(widgets[0], widgets[1])
        for w in widgets:
            # NB00A validates an existing connection by connection_id;
            # it does not expose metadata widgets or recreate connection_id widget.
            for forbidden in ("connection_name", "source_server", "source_database",
                              "secret_scope", "trust_server_certificate", "source_system",
                              "connection_id"):
                self.assertNotIn(forbidden, w)

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
                         "objects", "summary", "execution_status",
                         "business_status", "objects_assessed", "error_count",
                         "errors", "compatibility_summary"):
            self.assertIn(expected, keys[0])

    def test_inventory_notebook_outputs(self):
        keys = [self._exit_keys(source_nb(t, "NB01_SourceInventory.py"))
                for t in SOURCE_TOKENS]
        self.assertEqual(keys[0], keys[1])
        for expected in ("status", "run_id", "connection_id", "source_system",
                         "execution_status", "business_status",
                         "tables_succeeded", "tables_failed", "columns_written"):
            self.assertIn(expected, keys[0])

    def test_sql_object_notebook_outputs(self):
        keys = [self._exit_keys(source_nb(t, "NB13_SQLObjectAssessmentAndConversion.py"))
                for t in SOURCE_TOKENS]
        self.assertEqual(keys[0], keys[1])
        for expected in ("status", "assessment_id", "objects", "summary",
                         "execution_status", "business_status",
                         "discovered_objects", "persisted_objects",
                         "inaccessible_definitions", "unsupported_object_types",
                         "discovery_failures", "errors"):
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
        "type_rules_file", "legacy_secret_scope_widget",
        "validate_connection_metadata", "apply_column_policy",
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


class _FakeTypeMapper(SourceTypeMapper):
    def map_column(self, source_type, precision=None, scale=None,
                   length=None, is_nullable=True):
        return ColumnMappingResult(
            source_type=source_type or "", databricks_delta_type="STRING",
            status="AUTO", fidelity="EXACT", notes="test-only mapping",
            is_nullable=bool(is_nullable))


class _FakeAdapter(SourceAdapter):
    """Test-only third source proving shared code needs no dialect knowledge."""

    source_system = "fakedb"
    SQL_OBJECT_TYPES = ("VIEW",)

    def type_rules_file(self):
        return "type_rules_fakedb.yaml"

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
        return _FakeTypeMapper()


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
            findings = modscan.scan(os.path.join(SRC, module_file),
                                    is_notebook=False)
            self.assertEqual(findings, [], f"{module_file}: {findings}")

    def test_fake_adapter_provides_its_own_type_rules(self):
        # A new source names its rules file; shared code never infers it.
        self.assertEqual(self.adapter.type_rules_file(), "type_rules_fakedb.yaml")
        self.assertIsNone(self.adapter.legacy_secret_scope_widget())

    def test_fake_adapter_provides_its_own_type_mapper(self):
        mapper = self.adapter.load_type_mapper()
        self.assertIsInstance(mapper, SourceTypeMapper)
        result = mapper.map_column("future_text")
        self.assertEqual(result.databricks_delta_type, "STRING")
        self.assertEqual(result.status, "AUTO")

    def test_fake_adapter_uses_base_column_policy(self):
        mapping = ColumnMappingResult(
            source_type="text", databricks_delta_type="STRING", status="AUTO",
            fidelity="EXACT", notes="", is_nullable=True)
        policy = self.adapter.apply_column_policy(
            {"column_name": "C", "is_hidden": 1, "is_computed": 1}, mapping)
        # The base policy does not interpret another source's metadata flags.
        self.assertTrue(policy.include_column)
        self.assertTrue(policy.is_writable)
        self.assertEqual(policy.mapping_status, "AUTO")
        self.assertIsNone(policy.policy_code)

    def test_fake_adapter_builds_extractions_generically(self):
        self.assertIn("SELECT", self.adapter.full_extract_query("db", "S", "T"))
        self.assertIn("SELECT", self.adapter.incremental_extract_query(
            "db", "S", "T", "wm", "TIMESTAMP", "a", "b"))
        self.assertIn("CONNECTION_OK", self.adapter.connection_probe_query())

    def test_fake_adapter_connection_metadata_validation(self):
        self.adapter.validate_connection_metadata(
            {"connection_id": "c1", "secret_scope": "scope"})
        with self.assertRaises(ValueError):
            self.adapter.validate_connection_metadata(
                {"connection_id": "c1", "secret_scope": ""})


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
