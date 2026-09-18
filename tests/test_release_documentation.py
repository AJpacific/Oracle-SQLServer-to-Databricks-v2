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
        cls.identity_migration = _read(
            os.path.join(DOCS, "source_identity_v2_migration.md"))

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

    def test_connection_owned_identity_and_migration_are_documented(self):
        combined = " ".join((self.readme, self.installation, self.supported,
                             self.jobs, self.identity_migration))
        for phrase in (
                "connection-owned", "only `run_id`, `connection_id`, and",
                "NB_MigrateSourceTableIdentityV2", "dry_run=true",
                "not called automatically"):
            self.assertIn(phrase, combined)
        self.assertIn("Job YAML", combined)
        self.assertIn("NOT_EXECUTED", self.identity_migration)

    def test_documented_notebook_paths_exist(self):
        bt = chr(96)
        parts = self.jobs.split(bt)
        refs = [parts[i].strip() for i in range(1, len(parts), 2)
                if any(parts[i].strip().startswith(p) for p in ("deployment/", "shared/", "sources/"))]
        self.assertTrue(len(refs) > 0, "No notebook references found in job mapping document")
        notebooks_dir = os.path.join(ROOT, "notebooks")
        for r in set(refs):
            clean = r.split()[0].rstrip("*,")
            if "<source>" in clean:
                for s in ("oracle", "sqlserver"):
                    cand = clean.replace("<source>", s)
                    found = any(os.path.exists(os.path.join(notebooks_dir, cand + ext))
                                for ext in ("", ".py", ".ipynb"))
                    self.assertTrue(found, f"Documented source notebook does not exist: {cand} (from {r})")
            else:
                found = any(os.path.exists(os.path.join(notebooks_dir, clean + ext))
                            for ext in ("", ".py", ".ipynb"))
                self.assertTrue(found, f"Documented notebook does not exist: {clean} (from {r})")

    def test_checklist_contains_no_volatile_durations(self):
        import re
        py_match = None
        un_match = None
        for line in self.checklist.splitlines():
            if line.startswith("| Run pytest suite"):
                self.assertNotRegex(line, r"\bin [0-9].*s\b")
                py_match = re.search(r"pytest:\s+Exit\s+(\d+),\s+(\d+)\s+passed,\s+(\d+)\s+subtests passed", line)
                self.assertIsNotNone(py_match, "Pytest checklist row must include exit code, passed count, and subtests count")
                self.assertEqual(int(py_match.group(1)), 0)
                self.assertGreater(int(py_match.group(2)), 0)
                self.assertGreater(int(py_match.group(3)), 0)
                self.assertIn("PASSED", line)
            elif line.startswith("| Run unittest discovery"):
                self.assertNotRegex(line, r"\bin [0-9].*s\b")
                un_match = re.search(r"unittest:\s+Exit\s+(\d+),\s+(\d+)\s+tests passed,\s+OK", line)
                self.assertIsNotNone(un_match, "Unittest checklist row must include exit code, test count, and OK status")
                self.assertEqual(int(un_match.group(1)), 0)
                self.assertGreater(int(un_match.group(2)), 0)
                self.assertIn("PASSED", line)

        self.assertIsNotNone(py_match)
        self.assertIsNotNone(un_match)
        self.assertEqual(int(py_match.group(2)), int(un_match.group(2)))


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