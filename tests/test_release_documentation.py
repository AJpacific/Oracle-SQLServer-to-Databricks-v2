"""Static checks for release evidence and production-readiness documentation."""

import os
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
DOCS = os.path.join(ROOT, "docs")


def _read(path):
    with open(path, encoding="utf-8") as stream:
        return stream.read()


class TestReleaseDocumentation(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.readme = _read(os.path.join(ROOT, "README.md"))
        cls.installation = _read(os.path.join(DOCS, "installation.md"))
        cls.supported = _read(
            os.path.join(DOCS, "supported_features_and_limitations.md"))
        cls.jobs = _read(os.path.join(DOCS, "databricks_job_task_mapping.md"))
        cls.adding = _read(os.path.join(DOCS, "adding_a_new_source.md"))
        cls.checklist_path = os.path.join(
            DOCS, "production_readiness_checklist.md")
        cls.checklist = _read(cls.checklist_path)

    def test_current_diagnostic_paths_are_documented(self):
        for path in (
                "notebooks/sources/oracle/TEST_CONNECTION.py",
                "notebooks/sources/sqlserver/TEST_CONNECTION.py"):
            self.assertIn(path, self.readme)
        self.assertNotIn("00_TEST_ORACLE_CONNECTION.py", self.readme)
        self.assertNotIn("00_TEST_SQLSERVER_CONNECTION.py", self.readme)

    def test_missing_source_system_and_manual_repair_are_documented(self):
        for document in (self.readme, self.installation, self.supported):
            self.assertIn("missing", document.lower())
            self.assertIn("source_system", document)
            self.assertIn("UPDATE <catalog>.<control_schema>.source_table_control",
                          document)
            self.assertIn("SET source_system = 'oracle'", document)
        self.assertIn("never executes this repair", self.supported)

    def test_inventory_exact_replacement_is_documented(self):
        for document in (self.readme, self.installation, self.supported,
                         self.jobs):
            self.assertIn("run_id", document)
            self.assertIn("source_table_id", document)
            self.assertIn("inventory", document.lower())
        self.assertIn("dropped source column", self.readme)
        self.assertIn("Duplicate incoming", self.installation)

    def test_mapper_structure_and_facade_are_documented(self):
        for path in (
                "src/type_mappers/base.py", "src/type_mappers/oracle.py",
                "src/type_mappers/sqlserver.py", "src/type_mappers/factory.py",
                "src/crosssourcetypemapper.py"):
            self.assertIn(path, self.readme + self.adding)
        self.assertIn("adapter.load_type_mapper().map_column", self.readme)
        self.assertIn("compatibility-only", self.adding)

    def test_test_evidence_paths_are_documented(self):
        for name in (
                "compileall-output.txt", "pytest-output.txt",
                "unittest-output.txt", "test-summary.json"):
            self.assertIn(name, self.readme)
            self.assertIn(name, self.installation)

    def test_production_checklist_exists_and_has_status_vocabulary(self):
        self.assertTrue(os.path.isfile(self.checklist_path))
        for status in (
                "NOT_EXECUTED", "PASSED", "FAILED", "BLOCKED",
                "NOT_APPLICABLE"):
            self.assertIn(f"`{status}`", self.checklist)

    def test_runtime_checklist_rows_default_to_not_executed(self):
        section = ""
        runtime_sections = {"B", "C", "D", "E", "F", "G"}
        for line in self.checklist.splitlines():
            if line.startswith("## "):
                section = line[3:4]
            if section not in runtime_sections or not line.startswith("|"):
                continue
            cells = [cell.strip() for cell in line.strip("|").split("|")]
            if not cells or cells[0] == "Check" or set(cells[0]) == {"-"}:
                continue
            self.assertEqual(len(cells), 8, line)
            self.assertEqual(cells[5], "NOT_EXECUTED", line)

    def test_sqlserver_and_oracle_policy_remain_source_documented(self):
        for term in ("computed columns", "hidden", "rowversion", "datetime2"):
            self.assertIn(term.lower(), self.supported.lower())
        for term in ("Oracle mapping policy", "NUMBER", "VECTOR"):
            self.assertIn(term, self.supported)

    def test_pure_tests_are_not_claimed_as_live_validation(self):
        supported = " ".join(self.supported.split())
        installation = " ".join(self.installation.split())
        jobs = " ".join(self.jobs.split())
        self.assertIn("do not establish runtime production readiness",
                      supported)
        self.assertIn("cannot be inferred from unit tests", installation)
        self.assertIn("cannot validate a deployed Databricks Job", jobs)

    def test_targeted_safety_behavior_is_documented(self):
        combined = self.readme + self.supported + self.checklist
        self.assertIn("ESTIMATED_8K_BLOCKS", combined)
        self.assertIn("show_sample_values", combined)
        self.assertIn("business_status=PARTIAL", combined)
        self.assertIn("partial inventory", combined.lower())
        for technology in ("Spark", "JDBC", "Delta", "Oracle", "SQL Server",
                           "Databricks Job"):
            self.assertIn(technology, combined)


class TestSharedNotebookDescriptions(unittest.TestCase):
    def _top(self, name):
        path = os.path.join(ROOT, "notebooks", "shared", name)
        return "\n".join(_read(path).splitlines()[:12])

    def test_required_shared_descriptions_are_source_neutral(self):
        for name in (
                "_common.py", "NB02_TypeNormalization.py",
                "NB03_MappingRulesGeneration.py", "NB09_FullLoad.py",
                "NB11a_DeltaSyncPrep.py", "NB15_BronzeToSilverETL.py"):
            top = self._top(name)
            self.assertNotIn("Oracle or SQL Server", top, name)
            self.assertNotIn("SQL Server computed", top, name)
            self.assertNotIn("SQL Server hidden", top, name)
            self.assertNotIn("SQL Server datetime2", top, name)


if __name__ == "__main__":
    unittest.main()