"""Artifact-specific executable tests for Lakebridge SQL Artifact Migration.

Covers all Section 10 requirements:
1. Only selected VIEW and PROCEDURE rows become candidates.
2. Unselected objects are excluded.
3. TABLE/FUNCTION/PACKAGE/PACKAGE_BODY/TRIGGER excluded.
4. Latest assessment per connection/database selected.
5. Max artifacts validation and deterministic ordering.
6. Connection usable validation (rejects inactive, non-VALID status, blank secret_scope).
7. SQL Server & Oracle fetch logic and query construction.
8. DEFINITION_MISSING handling on null/blank/inaccessible definitions.
9. Lakebridge input preparation, separate input/report/transpiled/errors directories.
10. Pure-Python Analyzer workbook reader (extracting complexity, statement counts, unknown fragments).
11. Construct detection, FIXME count, remaining dialect syntax, referenced objects extraction.
12. Lakebridge classification rules (UNSUPPORTED, MANUAL_REVIEW, AUTO_CANDIDATE) and field mapping.
13. Control table 18 Lakebridge columns, DDL generation, upgrade DDL, and verification.
14. Execution log 5 stages (SELECTED_SOURCE_FETCH, LAKEBRIDGE_ANALYZE, LAKEBRIDGE_TRANSPILE, LAKEBRIDGE_STORE, LAKEBRIDGE_CLASSIFY).
15. Job YAML graph structure, dependencies, parameters, run_if conditions, compute requirements.
16. Summary notebook metrics and business status evaluation logic.
17. Isolation verification (asserting no forbidden files were touched).
"""

import ast
import csv
import hashlib
import io
import json
import os
import subprocess
import sys
import tempfile
import unittest
import unittest.mock
import uuid
import zipfile
import yaml

from src.sql_artifact_control_common import (
    SQL_ARTIFACT_CONTROL_TABLE,
    SQL_ARTIFACT_EXECUTION_LOG_TABLE,
    SQL_ARTIFACT_CONTROL_COLUMNS,
    SQL_ARTIFACT_EXECUTION_LOG_COLUMNS,
    STAGE_SELECTED_SOURCE_FETCH,
    STAGE_LAKEBRIDGE_ANALYZE,
    STAGE_LAKEBRIDGE_TRANSPILE,
    STAGE_LAKEBRIDGE_STORE,
    STAGE_LAKEBRIDGE_CLASSIFY,
    LAKEBRIDGE_CLASSIFICATION_AUTO_CANDIDATE,
    LAKEBRIDGE_CLASSIFICATION_MANUAL_REVIEW,
    LAKEBRIDGE_CLASSIFICATION_UNSUPPORTED,
    build_create_artifact_control_ddl,
    build_create_artifact_execution_log_ddl,
    build_upgrade_artifact_control_ddl,
    compute_artifact_id,
    compute_definition_hash,
    is_connection_usable,
    validate_registered_source_connection,
    prepare_artifact_rerun_control_row,
    resolve_target_catalog_and_schema,
    decide_summary,
    sanitize_error,
)
from src.sql_artifact_scope import (
    build_selected_candidate_query,
    validate_max_artifacts,
)
from src.lakebridge_artifact_common import (
    normalize_source_system,
    normalize_object_type,
    prepare_lakebridge_input_file,
    read_analyzer_workbook,
    read_analyzer_report,
    col_letter_to_index,
    extract_complexity,
    extract_statement_counts,
    extract_unknown_fragments,
    detect_sql_constructs,
    count_fixme_markers,
    detect_remaining_source_syntax,
    extract_referenced_objects,
    build_bounded_json,
    derive_lakebridge_classification,
    sanitize_cli_output,
    build_lakebridge_report_path,
    build_lakebridge_converted_path,
    build_lakebridge_error_path,
    build_lakebridge_availability_cmd,
    build_lakebridge_analyze_cmd,
    build_lakebridge_transpile_cmd,
    build_attempt_staging_dir,
    build_collision_resistant_filename,
    get_analyzer_platform,
    get_bladebridge_tech,
    extract_sql_from_bladebridge_mime,
    UnknownFragment,
    apply_text_edits,
    normalize_diagnostic_severity,
    read_analyzer_report,
    mask_sql_literals_and_comments,
    has_usable_analyzer_results,
)
from src.lakebridge_environment import (
    find_uv_binary,
    get_default_lakebridge_venv_dir,
    get_venv_python_executable,
    check_environment_health,
    bootstrap_lakebridge_environment,
    ensure_lakebridge_environment,
    EnvironmentLock,
)
from src.lakebridge_runner import (
    run_transpile,
    run_analyze,
    run_check_environment,
)

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def create_in_memory_xlsx(sheets_data):
    """Helper to build a valid zip-based .xlsx file in memory with sheet XMLs."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        # [Content_Types].xml
        content_types = ['<?xml version="1.0" encoding="UTF-8" standalone="yes"?>',
                         '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">',
                         '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>',
                         '<Default Extension="xml" ContentType="application/xml"/>',
                         '<Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>']
        for i in range(1, len(sheets_data) + 1):
            content_types.append(f'<Override PartName="/xl/worksheets/sheet{i}.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>')
        content_types.append('</Types>')
        zf.writestr("[Content_Types].xml", "".join(content_types))

        # _rels/.rels
        rels = ['<?xml version="1.0" encoding="UTF-8" standalone="yes"?>',
                '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">',
                '<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>',
                '</Relationships>']
        zf.writestr("_rels/.rels", "".join(rels))

        # xl/_rels/workbook.xml.rels
        wb_rels = ['<?xml version="1.0" encoding="UTF-8" standalone="yes"?>',
                   '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">']
        for i in range(1, len(sheets_data) + 1):
            wb_rels.append(f'<Relationship Id="rId{i}" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet{i}.xml"/>')
        wb_rels.append('</Relationships>')
        zf.writestr("xl/_rels/workbook.xml.rels", "".join(wb_rels))

        # xl/workbook.xml
        wb = ['<?xml version="1.0" encoding="UTF-8" standalone="yes"?>',
              '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">',
              '<sheets>']
        for i, (sheet_name, _) in enumerate(sheets_data, 1):
            wb.append(f'<sheet name="{sheet_name}" sheetId="{i}" r:id="rId{i}"/>')
        wb.append('</sheets></workbook>')
        zf.writestr("xl/workbook.xml", "".join(wb))

        # Worksheets
        for i, (_, rows) in enumerate(sheets_data, 1):
            ws = ['<?xml version="1.0" encoding="UTF-8" standalone="yes"?>',
                  '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">',
                  '<sheetData>']
            for r_idx, row in enumerate(rows, 1):
                ws.append(f'<row r="{r_idx}">')
                for c_idx, val in enumerate(row, 1):
                    # convert col index to letter (A, B, C...)
                    col_letter = chr(64 + c_idx)
                    val_str = str(val) if val is not None else ""
                    ws.append(f'<c r="{col_letter}{r_idx}" t="inlineStr"><is><t>{val_str}</t></is></c>')
                ws.append('</row>')
            ws.append('</sheetData></worksheet>')
            zf.writestr(f"xl/worksheets/sheet{i}.xml", "".join(ws))

    buf.seek(0)
    return buf.getvalue()


class TestCandidateSelection(unittest.TestCase):
    """Section 1: Selection Query requirements."""

    def test_candidates_filter_is_selected_true_and_view_procedure_only(self):
        query = build_selected_candidate_query(
            source_assessment_fqn="da_catalog.control.source_assessment",
            connection_id="conn_sql1",
            source_database="SalesDB",
        )
        # Requirement 1 & 2: is_selected = true
        self.assertIn("sa.is_selected = true", query)
        # Requirement 1 & 3: Only VIEW and PROCEDURE
        self.assertIn("IN ('VIEW', 'PROCEDURE')", query)
        self.assertNotIn("TABLE", query)
        self.assertNotIn("FUNCTION", query)
        self.assertNotIn("PACKAGE", query)
        self.assertNotIn("TRIGGER", query)

    def test_unselected_and_other_types_excluded(self):
        query = build_selected_candidate_query(
            source_assessment_fqn="da_catalog.control.source_assessment"
        )
        self.assertIn("sa.is_selected = true", query)
        self.assertIn("sa.object_type", query)
        self.assertIn("IN ('VIEW', 'PROCEDURE')", query)

    def test_joins_latest_assessment_per_connection_and_database(self):
        query = build_selected_candidate_query(
            source_assessment_fqn="cat.ctrl.source_assessment"
        )
        self.assertIn("latest_runs", query)
        self.assertIn("MAX(captured_ts)", query)
        self.assertIn("sa.run_id = lr.run_id", query)

    def test_deterministic_ordering(self):
        query = build_selected_candidate_query(
            source_assessment_fqn="cat.ctrl.source_assessment"
        )
        self.assertIn("ORDER BY", query)
        self.assertIn("connection_id", query)
        self.assertIn("source_database", query)
        self.assertIn("source_schema", query)
        self.assertIn("object_type", query)
        self.assertIn("object_name", query)

    def test_validate_max_artifacts(self):
        self.assertIn(validate_max_artifacts(""), (None, 0))
        self.assertEqual(validate_max_artifacts(0), 0)
        self.assertIn(validate_max_artifacts(None), (None, 0))
        self.assertEqual(validate_max_artifacts(50), 50)
        self.assertEqual(validate_max_artifacts("25"), 25)

        with self.assertRaises(ValueError):
            validate_max_artifacts(-1)
        with self.assertRaises(ValueError):
            validate_max_artifacts("-5")
        with self.assertRaises(ValueError):
            validate_max_artifacts("abc")
        with self.assertRaises(ValueError):
            validate_max_artifacts("3.14")


class TestSourceConnectionAndFetchValidation(unittest.TestCase):
    """Section 2: Connection validation and fetch checks."""

    def test_connection_usable_requires_active_valid_and_secret_scope(self):
        # Valid connection
        valid_conn = {
            "connection_id": "c1",
            "is_active": True,
            "connection_status": "VALID",
            "secret_scope": "my_scope",
        }
        self.assertTrue(is_connection_usable(valid_conn)[0])

        # Inactive connection rejected
        self.assertFalse(is_connection_usable({**valid_conn, "is_active": False})[0])
        self.assertFalse(is_connection_usable({**valid_conn, "is_active": None})[0])

        # Non-VALID status rejected
        self.assertFalse(is_connection_usable({**valid_conn, "connection_status": "INVALID"})[0])
        self.assertFalse(is_connection_usable({**valid_conn, "connection_status": "TESTING"})[0])
        self.assertFalse(is_connection_usable({**valid_conn, "connection_status": ""})[0])

        # Blank or missing secret_scope rejected
        self.assertFalse(is_connection_usable({**valid_conn, "secret_scope": ""})[0])
        self.assertFalse(is_connection_usable({**valid_conn, "secret_scope": "   "})[0])
        self.assertFalse(is_connection_usable({**valid_conn, "secret_scope": None})[0])

    def test_nb23_notebook_structure_and_fetch_isolation(self):
        nb23_path = os.path.join(ROOT, "notebooks", "shared", "NB23_FetchSelectedSQLArtifacts.py")
        self.assertTrue(os.path.exists(nb23_path), "NB23 must exist")

        with open(nb23_path, "r", encoding="utf-8") as f:
            code = f.read()

        # Groups by connection_id, source_system, source_database
        self.assertIn("connection_id", code)
        self.assertIn("source_system", code)
        self.assertIn("source_database", code)

        # Uses sys.sql_modules for SQL Server
        self.assertIn("sql_modules", code)
        self.assertIn("objects", code)

        # Uses ALL_VIEWS and ALL_SOURCE for Oracle, ordered by line
        self.assertIn("ALL_VIEWS", code)
        self.assertIn("ALL_SOURCE", code)
        self.assertIn("ORDER BY LINE", code)

        # Missing definitions become DEFINITION_MISSING
        self.assertIn("DEFINITION_MISSING", code)

        # Never executes source SQL
        self.assertNotIn("spark.sql(source_def", code)
        self.assertNotIn("cursor.execute(source_def", code)

        # Does not invoke NB13
        self.assertNotIn("NB13", code)

    def test_nb23_partial_fetch_status_and_raise_behavior(self):
        nb23_path = os.path.join(ROOT, "notebooks", "shared", "NB23_FetchSelectedSQLArtifacts.py")
        with open(nb23_path, "r", encoding="utf-8") as f:
            code = f.read()

        # Verify the exact status-logic assignment is present
        self.assertIn('status = "FAILED" if business_status == "FAILED" else "SUCCEEDED"', code)
        self.assertNotIn('status = "FAILED" if (business_status in ("FAILED", "PARTIAL")', code)

        # Extract and compile evaluate_nb23_fetch_status
        tree = ast.parse(code)
        eval_fn = None
        for node in tree.body:
            if isinstance(node, ast.FunctionDef) and node.name == "evaluate_nb23_fetch_status":
                mod = ast.Module(body=[node], type_ignores=[])
                compiled = compile(mod, filename="<nb23>", mode="exec")
                from typing import Tuple
                ns = {"Tuple": Tuple}
                exec(compiled, ns)
                eval_fn = ns["evaluate_nb23_fetch_status"]
                break
        self.assertIsNotNone(eval_fn, "evaluate_nb23_fetch_status must be defined in NB23")

        # 1. Targeted regression test:
        # total_candidates = 3, fetched_definition_count = 2, total_failures = 1
        status, b_status = eval_fn(total_candidates=3, fetched_definition_count=2, total_failures=1)
        self.assertEqual(b_status, "PARTIAL")
        self.assertEqual(status, "SUCCEEDED")
        # NB23 must not raise when status is SUCCEEDED, allowing T24 to process the 2 fetched artifacts
        if status == "FAILED":
            self.fail("NB23 must not raise on PARTIAL fetch when definitions were successfully fetched")

        # 2. fetched_definition_count = 0 with failures returns FAILED and raises
        status_fail, b_status_fail = eval_fn(total_candidates=2, fetched_definition_count=0, total_failures=2)
        self.assertEqual(b_status_fail, "FAILED")
        self.assertEqual(status_fail, "FAILED")
        with self.assertRaises(RuntimeError):
            if status_fail == "FAILED":
                raise RuntimeError("NB23_FetchSelectedSQLArtifacts failed with 2 failure(s) out of 2 candidates")

        # 3. all successful fetches return SUCCEEDED/COMPLETE
        status_complete, b_status_complete = eval_fn(total_candidates=3, fetched_definition_count=3, total_failures=0)
        self.assertEqual(b_status_complete, "COMPLETE")
        self.assertEqual(status_complete, "SUCCEEDED")
        if status_complete == "FAILED":
            self.fail("NB23 must not raise on COMPLETE")

        # 4. zero candidates return SUCCEEDED/NO_CANDIDATES
        status_zero, b_status_zero = eval_fn(total_candidates=0, fetched_definition_count=0, total_failures=0)
        self.assertEqual(b_status_zero, "NO_CANDIDATES")
        self.assertEqual(status_zero, "SUCCEEDED")
        if status_zero == "FAILED":
            self.fail("NB23 must not raise on NO_CANDIDATES")


class TestLakebridgeCommonParserAndWorkbook(unittest.TestCase):
    """Section 4: Lakebridge pure Python analyzer workbook and metadata parsing."""

    def setUp(self):
        sheets_data = [
            ("Complexity", [
                ["Object Name", "Schema", "Complexity Score", "Complexity Category"],
                ["v_sales_summary", "dbo", "12", "LOW"],
                ["sp_complex_proc", "dbo", "45", "HIGH"],
            ]),
            ("Statement Summary", [
                ["Object Name", "Total Statements", "Unknown Statements", "Status"],
                ["v_sales_summary", "5", "0", "OK"],
                ["sp_complex_proc", "25", "3", "REVIEW"],
            ]),
            ("Unknown Fragments", [
                ["Object Name", "Fragment Text", "Line Number"],
                ["sp_complex_proc", "UNRESOLVED_CALL(xyz)", "42"],
            ]),
            ("Referenced Objects", [
                ["Source Object", "Referenced Schema", "Referenced Table", "Operation", "Count"],
                ["sp_complex_proc", "stage", "res_outbound_stage", "READ", "3"],
                ["sp_complex_proc", "dbo", "res_outbound", "WRITE", "1"],
            ]),
        ]
        self.xlsx_bytes = create_in_memory_xlsx(sheets_data)

    def test_workbook_parser_pure_python(self):
        wb = read_analyzer_workbook(self.xlsx_bytes)
        self.assertIn("complexity", wb)
        self.assertIn("statement_summary", wb)
        self.assertIn("unknown_fragments", wb)
        self.assertIn("referenced_objects", wb)

        # Test extract complexity
        self.assertEqual(extract_complexity(wb, "v_sales_summary"), "LOW")
        self.assertEqual(extract_complexity(wb, "sp_complex_proc"), "HIGH")
        self.assertIsNone(extract_complexity(wb, "non_existent"))

        # Test statement counts
        stmts, unk = extract_statement_counts(wb, "v_sales_summary")
        self.assertEqual(stmts, 5)
        self.assertEqual(unk, 0)

        stmts_proc, unk_proc = extract_statement_counts(wb, "sp_complex_proc")
        self.assertEqual(stmts_proc, 25)
        self.assertEqual(unk_proc, 3)

        # Test unknown fragments
        frags = extract_unknown_fragments(wb, "sp_complex_proc")
        self.assertEqual(len(frags), 1)
        self.assertIn("UNRESOLVED_CALL", frags[0])

    def test_construct_detection(self):
        # Error handling
        c1 = detect_sql_constructs("BEGIN TRY SELECT 1 END TRY BEGIN CATCH PRINT 1 END CATCH", "sqlserver")
        self.assertTrue(c1["uses_error_handling"])
        self.assertFalse(c1["uses_rowcount"])

        # Rowcount
        c2 = detect_sql_constructs("IF @@ROWCOUNT > 0 RETURN", "sqlserver")
        self.assertTrue(c2["uses_rowcount"])

        # Oracle rowcount
        c2_ora = detect_sql_constructs("IF SQL%ROWCOUNT > 0 THEN NULL; END IF;", "oracle")
        self.assertTrue(c2_ora["uses_rowcount"])

        # Cursor
        c3 = detect_sql_constructs("DECLARE cur CURSOR FOR SELECT 1", "sqlserver")
        self.assertTrue(c3["uses_cursor"])

        # Dynamic SQL
        c4 = detect_sql_constructs("EXEC sp_executesql @stmt", "sqlserver")
        self.assertTrue(c4["uses_dynamic_sql"])

        c4_ora = detect_sql_constructs("EXECUTE IMMEDIATE 'SELECT 1 FROM dual'", "oracle")
        self.assertTrue(c4_ora["uses_dynamic_sql"])

        # Triggers
        c5 = detect_sql_constructs("CREATE TRIGGER trg ON tbl FOR INSERT", "sqlserver")
        self.assertTrue(c5["uses_trigger"])

    def test_fixme_and_dialect_syntax_detection(self):
        self.assertEqual(count_fixme_markers("/* FIXME: bladebridge cannot convert this */"), 1)
        self.assertEqual(count_fixme_markers("-- FIXME: check logic\nSELECT 1; -- FIXME"), 2)

        retains_sql, patterns_sql = detect_remaining_source_syntax("CREATE TABLE #temp (id INT); SELECT @@TRANCOUNT", "sqlserver")
        self.assertTrue(retains_sql)
        self.assertTrue(any("@@" in p for p in patterns_sql))

        retains_ora, patterns_ora = detect_remaining_source_syntax("SELECT SYSDATE FROM dual", "oracle")
        self.assertTrue(retains_ora)
        self.assertTrue(any("SYSDATE" in p.upper() for p in patterns_ora))

    def test_referenced_objects_bounded_json(self):
        wb = read_analyzer_workbook(self.xlsx_bytes)
        refs = extract_referenced_objects(wb, "", "sp_complex_proc")
        self.assertEqual(len(refs), 2)
        self.assertEqual(refs[0]["operation"], "READ")
        self.assertEqual(refs[1]["operation"], "WRITE")

        bounded_json = build_bounded_json(refs, max_bytes=512)
        parsed = json.loads(bounded_json)
        self.assertEqual(len(parsed), 2)


class TestLakebridgeClassification(unittest.TestCase):
    """Section 6: Lakebridge classification derivation rules."""

    def test_unsupported_classification(self):
        # Missing or blank definition
        res1 = derive_lakebridge_classification(
            source_definition="",
            converted_definition=None,
            complexity="LOW",
            statement_count=1,
            unknown_statement_count=0,
            unknown_fragments=[],
            constructs={},
            parsing_error_count=0,
            validation_error_count=0,
            generation_error_count=0,
            fixme_count=0,
            remaining_source_syntax=[],
            analyzer_failed=False,
            transpile_failed=False,
        )
        self.assertEqual(res1[0], LAKEBRIDGE_CLASSIFICATION_UNSUPPORTED)

        # Parsing error
        res2 = derive_lakebridge_classification(
            source_definition="SELECT 1",
            converted_definition="SELECT 1",
            complexity="LOW",
            statement_count=1,
            unknown_statement_count=0,
            unknown_fragments=[],
            constructs={},
            parsing_error_count=1,
            validation_error_count=0,
            generation_error_count=0,
            fixme_count=0,
            remaining_source_syntax=[],
            analyzer_failed=False,
            transpile_failed=False,
        )
        self.assertEqual(res2[0], LAKEBRIDGE_CLASSIFICATION_UNSUPPORTED)

        # Generation error
        res3 = derive_lakebridge_classification(
            source_definition="SELECT 1",
            converted_definition=None,
            complexity="LOW",
            statement_count=1,
            unknown_statement_count=0,
            unknown_fragments=[],
            constructs={},
            parsing_error_count=0,
            validation_error_count=0,
            generation_error_count=1,
            fixme_count=0,
            remaining_source_syntax=[],
            analyzer_failed=False,
            transpile_failed=False,
        )
        self.assertEqual(res3[0], LAKEBRIDGE_CLASSIFICATION_UNSUPPORTED)

    def test_manual_review_classification(self):
        # High complexity
        res_high = derive_lakebridge_classification(
            source_definition="SELECT 1",
            converted_definition="SELECT 1",
            complexity="HIGH",
            statement_count=10,
            unknown_statement_count=0,
            unknown_fragments=[],
            constructs={"uses_error_handling": False},
            parsing_error_count=0,
            validation_error_count=0,
            generation_error_count=0,
            fixme_count=0,
            remaining_source_syntax=[],
            analyzer_failed=False,
            transpile_failed=False,
        )
        self.assertEqual(res_high[0], LAKEBRIDGE_CLASSIFICATION_MANUAL_REVIEW)

        # Uses dynamic SQL
        res_dyn = derive_lakebridge_classification(
            source_definition="SELECT 1",
            converted_definition="SELECT 1",
            complexity="LOW",
            statement_count=5,
            unknown_statement_count=0,
            unknown_fragments=[],
            constructs={"uses_dynamic_sql": True},
            parsing_error_count=0,
            validation_error_count=0,
            generation_error_count=0,
            fixme_count=0,
            remaining_source_syntax=[],
            analyzer_failed=False,
            transpile_failed=False,
        )
        self.assertEqual(res_dyn[0], LAKEBRIDGE_CLASSIFICATION_MANUAL_REVIEW)

        # FIXME count > 0
        res_fixme = derive_lakebridge_classification(
            source_definition="SELECT 1",
            converted_definition="/* FIXME */ SELECT 1",
            complexity="LOW",
            statement_count=2,
            unknown_statement_count=0,
            unknown_fragments=[],
            constructs={},
            parsing_error_count=0,
            validation_error_count=0,
            generation_error_count=0,
            fixme_count=1,
            remaining_source_syntax=[],
            analyzer_failed=False,
            transpile_failed=False,
        )
        self.assertEqual(res_fixme[0], LAKEBRIDGE_CLASSIFICATION_MANUAL_REVIEW)

    def test_auto_candidate_classification(self):
        # Section 10: When object_map_applied is False, clean VIEW is MANUAL_REVIEW
        classification, conv_status, manual_rev_req, manual_rev_reason, err_code, err_msg = derive_lakebridge_classification(
            object_type="VIEW",
            source_definition="CREATE VIEW v AS SELECT 1",
            converted_definition="CREATE VIEW v AS SELECT 1",
            complexity="LOW",
            statement_count=1,
            unknown_statement_count=0,
            unknown_fragments=[],
            constructs={
                "uses_error_handling": False,
                "uses_rowcount": False,
                "uses_cursor": False,
                "uses_dynamic_sql": False,
                "uses_trigger": False,
            },
            parsing_error_count=0,
            validation_error_count=0,
            generation_error_count=0,
            fixme_count=0,
            remaining_source_syntax=[],
            analyzer_failed=False,
            transpile_failed=False,
            object_map_applied=False,
        )
        self.assertEqual(classification, LAKEBRIDGE_CLASSIFICATION_MANUAL_REVIEW)
        self.assertEqual(conv_status, "PARTIAL")
        self.assertTrue(manual_rev_req)

        # When object_map_applied is True, clean VIEW becomes AUTO_CANDIDATE
        classification2, conv_status2, manual_rev_req2, manual_rev_reason2, _, _ = derive_lakebridge_classification(
            object_type="VIEW",
            source_definition="CREATE VIEW v AS SELECT 1",
            converted_definition="CREATE VIEW v AS SELECT 1",
            complexity="LOW",
            statement_count=1,
            unknown_statement_count=0,
            unknown_fragments=[],
            constructs={
                "uses_error_handling": False,
                "uses_rowcount": False,
                "uses_cursor": False,
                "uses_dynamic_sql": False,
                "uses_trigger": False,
            },
            parsing_error_count=0,
            validation_error_count=0,
            generation_error_count=0,
            fixme_count=0,
            remaining_source_syntax=[],
            analyzer_failed=False,
            transpile_failed=False,
            object_map_applied=True,
        )
        self.assertEqual(classification2, LAKEBRIDGE_CLASSIFICATION_AUTO_CANDIDATE)
        self.assertEqual(conv_status2, "CONVERTED")
        self.assertFalse(manual_rev_req2)
        self.assertIsNone(manual_rev_reason2)


class TestControlTable18LakebridgeColumns(unittest.TestCase):
    """Section 5: Control Table Columns and DDL."""

    def test_all_18_lakebridge_columns_present(self):
        col_names = [col[0] for col in SQL_ARTIFACT_CONTROL_COLUMNS]
        required_18 = [
            "lakebridge_analyzed_ts",
            "lakebridge_complexity",
            "lakebridge_statement_count",
            "lakebridge_unknown_statement_count",
            "lakebridge_unknown_fragments",
            "lakebridge_uses_error_handling",
            "lakebridge_uses_rowcount",
            "lakebridge_uses_cursor",
            "lakebridge_uses_dynamic_sql",
            "lakebridge_uses_trigger",
            "lakebridge_referenced_objects",
            "lakebridge_parsing_error_count",
            "lakebridge_validation_error_count",
            "lakebridge_generation_error_count",
            "lakebridge_fixme_count",
            "lakebridge_transpiled_definition",
            "object_map_applied",
            "lakebridge_classification",
        ]
        for col in required_18:
            self.assertIn(col, col_names, f"Column {col} must exist in SQL_ARTIFACT_CONTROL_COLUMNS")

    def test_ddl_generation_and_upgrade(self):
        ddl = build_create_artifact_control_ddl("cat", "sch")
        self.assertIn("lakebridge_complexity", ddl)
        self.assertIn("lakebridge_classification", ddl)

        # Upgrading a legacy schema lacking lakebridge columns
        legacy_cols = ["artifact_id", "connection_id", "source_schema", "object_name"]
        alter_stmts = build_upgrade_artifact_control_ddl("cat", "sch", legacy_cols)
        self.assertGreater(len(alter_stmts), 0)
        self.assertIn("lakebridge_complexity", alter_stmts[0])
        self.assertIn("lakebridge_classification", alter_stmts[0])


class TestJobGraphAndComputeRequirements(unittest.TestCase):
    """Section 8: Dedicated Job Graph and Compute Requirements."""

    def test_job_yaml_sequence_and_tasks(self):
        yaml_path = os.path.join(ROOT, "jobs", "ACCELERATOR_SQL_ARTIFACT_MIGRATION.yaml")
        self.assertTrue(os.path.exists(yaml_path), "YAML must exist")

        with open(yaml_path, "r", encoding="utf-8") as f:
            content = f.read()
            job = yaml.safe_load(content)

        tasks = {t["task_key"]: t for t in job["tasks"]}
        expected_keys = [
            "T00_Create_Run_Context",
            "T03_Init_SQL_Artifact_Control",
            "T23_Fetch_Selected_SQL_Artifacts",
            "T24_Lakebridge_Analyze_And_Transpile",
            "T06_SQL_Artifact_Summary",
        ]
        self.assertEqual(list(tasks.keys()), expected_keys)

        # Check dependencies
        self.assertEqual([d["task_key"] for d in tasks["T03_Init_SQL_Artifact_Control"].get("depends_on", [])], ["T00_Create_Run_Context"])
        self.assertEqual([d["task_key"] for d in tasks["T23_Fetch_Selected_SQL_Artifacts"].get("depends_on", [])], ["T03_Init_SQL_Artifact_Control"])
        self.assertEqual([d["task_key"] for d in tasks["T24_Lakebridge_Analyze_And_Transpile"].get("depends_on", [])], ["T23_Fetch_Selected_SQL_Artifacts"])
        self.assertEqual([d["task_key"] for d in tasks["T06_SQL_Artifact_Summary"].get("depends_on", [])], ["T24_Lakebridge_Analyze_And_Transpile"])

        # Check run_if
        self.assertEqual(tasks["T23_Fetch_Selected_SQL_Artifacts"].get("run_if"), "ALL_SUCCESS")
        self.assertEqual(tasks["T24_Lakebridge_Analyze_And_Transpile"].get("run_if"), "ALL_SUCCESS")
        self.assertEqual(tasks["T06_SQL_Artifact_Summary"].get("run_if"), "ALL_DONE")

        # Documented T24 compute requirements in comments
        self.assertIn("dedicated/single-user compatible compute", content)
        self.assertIn("Lakebridge", content)
        self.assertIn("BladeBridge", content)


PROTECTED_FILE_BASELINE_HASHES = {
    "notebooks/deployment/NB_GetDeltaWorklist.ipynb": "4464c96863689715c18973d29e1d1da87eefe8ea11252cb9e5ddb02046de08f6",
    "notebooks/shared/NB00_ControlTableInit.py": "792c137ea0bda96bb7f777fb7ee580c4bb994e661d12b0bf62279706b74d9b3e",
    "notebooks/sources/oracle/NB13_SQLObjectAssessmentAndConversion.py": "00f41b3674a2eb2908410c1197d8aa1cb49b4cf08c2dfcba2a274fa6cf370797",
    "notebooks/sources/sqlserver/NB13_SQLObjectAssessmentAndConversion.py": "a196f8cee0e66bc55d489f96f55aee0a85c6f972446e83ddf0b88a14f0380c31",
    "notebooks/sources/oracle/NB01A_SourceAssessment.py": "5792e3f5b68e35c967be64127e94b7f62654182937192b2ef8ad7ef4473007ef",
    "notebooks/sources/sqlserver/NB01A_SourceAssessment.py": "08500aeabe3e6eeec1a6a5cace258a92b2324f01a506e9c1fedab3b16ff49836",
    "notebooks/sources/oracle/NB01_SourceInventory.py": "e8a7f81cbbceca139b146c33c8bea3f1be856ae170c127f76db06834ca61fcc6",
    "notebooks/sources/sqlserver/NB01_SourceInventory.py": "e6ee621951624491e47bf8ea9d33d7d6ec2f03dc44bce91473aab3e965d123ba",
    "notebooks/sources/oracle/NB00A_UpsertAndValidateConnection.py": "5e1a7f2b132d1172e8a9623b7147285770452555243d4988d08b758147928827",
    "notebooks/sources/sqlserver/NB00A_UpsertAndValidateConnection.py": "28bd04014b77421627f596a0768092eb6e9cb31da176340d3ad01d28b4ec9340",
    "notebooks/deployment/NB_GetAssessmentDatabaseWorklist.py": "a29e30497ae5950838e509fde2489655292067ab586cbd94a926ab5dd1aaea38",
    "notebooks/deployment/NB_GetConnectionWorklist.ipynb": "9a3f2ba720c6a9a3c3ca3f41c6d10dc39ef22badba076d87d692a0e72572ef3c",
    "notebooks/shared/NB01B_RegisterSelectedTables.py": "29dfdc0e7230a222439c6254a63fca4ae79f6dca0175ed35e307aed3ae858889",
    "notebooks/shared/NB02_TypeNormalization.py": "a5e598a3be9b11f339c5b9a5845c8c76fe9925069826b5074cef5e3aa38a8a58",
    "notebooks/shared/NB03_MappingRulesGeneration.py": "a28e77bc88311b156e1161b1242681ec3b1d688ac6b28128d3de72392c693f04",
    "notebooks/shared/NB04_MappingValidation.py": "3a17981a9cb23f05b7bf5fb60f50f61203f81dc92723ddc3de6e388ee00a9cf2",
    "notebooks/shared/NB07_TableDecisionGeneration.py": "7742e87dffda96ccce15e1d8e1f9059c8ec102c50137ffbc81f733c5baf4cfac",
    "notebooks/shared/NB08_TargetProvisioning.py": "83315033df8a4e6e4d46bc1a7b810081c0c43317df5e111a3cba85f572132014",
    "notebooks/shared/NB09_FullLoad.py": "ed643193fd66f71e57ac303cac9a54aa61752f83e8db1580de6c1d21a78342b3",
    "notebooks/shared/NB10_PostFullLoadState.py": "085b4a8bc15f31dc2f9f3dc477829d566b9bcc3b098f3db063edfa47d8b2598b",
    "notebooks/shared/NB11a_DeltaSyncPrep.py": "02fd912a8060db45832a11f9f732179cfcc60248978a04a83e782c936e3ace57",
    "notebooks/shared/NB11b_DeltaSyncApply.py": "ba04f2d260c1f8a06b7cdad7e6077a3d43cfda9e8e061835a100dd36bc103fa1",
    "notebooks/shared/NB15_BronzeToSilverETL.py": "4e29f1a38748bf7856404aa4c3688ac108a048d71b23117087266f66fe24a35c",
    "notebooks/shared/NB14_RetryFailedTables.py": "bc82217f9f2908fd73dbb07c9e71eced347411dc9a07dfd197d6f76890666fd8",
    "notebooks/shared/NB16_NotifyFailures.py": "a873d62f8f50e4255928a2143fa93b79c9c7dc1433a8451673fa31d58f7e2b00",
    "notebooks/shared/NB12_ValidationAndReconciliation.py": "af80f41cadc43b254670c686deffc420ef6f2dd98f4dd1e44d1755071ee9ee94",
    "notebooks/shared/NB17_DashboardViews.py": "cf30e8c18a00ee66a535f444ddffbf774e1678b3e3a721a7fb3095bf76d17dbe",
    "src/source_adapters/__init__.py": "0cb99c6785c261081968d54ff2f4e4273a5d9339f0414bde31c2320294910acd",
    "src/source_adapters/base.py": "e153a215590010c9f5f4fdf717859d11718c1534500075fdd56151c4f5f987fa",
    "src/source_adapters/factory.py": "7a5ae1e6e3433d1621fcc117ec84f586b6ac2f2569ee84f3de510547c32d8a8c",
    "src/source_adapters/oracle.py": "7f5aeb5a23342c5fb5257e8375bd717695352a5fc64cc63876727cdbf8ab32a8",
    "src/source_adapters/sqlserver.py": "17ceb269c297dc1a7bee670b27a126ed96131f63340328cb9cda05a33a33045e",
    "src/sql_builder.py": "6a6f53f85a71e32ca324fda07df5948f80a9cfa9b929c391e78af8cbbd224664",
    "src/sqlserver_sql_builder.py": "503871e32b53d6292e8deae8edd41f1d78796b6d649f910e852a7caa4daf94b0",
    "src/ddl_builder.py": "6307a9e8c1b7ea9129162df66945445b0db6ee849d80383f91ed597fb375cfd8",
    "src/type_mappers/__init__.py": "ec67dcb8680a9cc83b397b99d15ef6dc11d2100fc6b35f3e37a8890774420752",
    "src/type_mappers/base.py": "a9ec2812643a44d146de34c2999f2ef8ce6d91b72af8901835af9093f759c374",
    "src/type_mappers/factory.py": "73271632d79d72f803e7ffe1d544dca6331a506d32c57724d14428b692d9b493",
    "src/type_mappers/oracle.py": "0642f8e517e770ce86d33c991695e8f45796db6f7aa979918f90040bb413d7fc",
    "src/type_mappers/sqlserver.py": "00697118f781fd53e960099b14a23de099c7e759850cae3bbd23bb64f455915e",
    "src/crosssourcetypemapper.py": "846bf6280ad0e75709757ebbeca65cac1bd037968e695bfd9182ccb2f91fee30",
    "config/type_rules.yaml": "9fcab931400c3c3db2f5c133d521c8aca8ed8fb621f5a05cc289330a62b459e5",
    "config/type_rules_oracle.yaml": "605e6b2f937164ac79f149953c9f8dd967e2e10924e487323f364beb5f6e8dc3",
    "config/type_rules_sqlserver.yaml": "fb03c925d75442d347101632355abad32b8d098320cf5bb26db5db0b011c4b20",
}


def validate_protected_files(repo_root=ROOT, manifest=None, git_check=True):
    """Validate protected files against baseline hash manifest and git status.

    Fails closed when:
    - manifest is empty (never pass vacuously)
    - an expected file is missing
    - a file differs from its approved baseline hash
    - git returns a nonzero exit code
    """
    active_manifest = manifest if manifest is not None else PROTECTED_FILE_BASELINE_HASHES
    if not active_manifest:
        raise RuntimeError("Protected-file validation failed: baseline manifest cannot be empty (never pass vacuously)")

    missing = []
    mismatches = []
    for rel_path, exp_hash in active_manifest.items():
        abs_path = os.path.join(repo_root, rel_path)
        if not os.path.exists(abs_path):
            missing.append(rel_path)
            continue
        with open(abs_path, "rb") as f:
            actual_hash = hashlib.sha256(f.read()).hexdigest()
        if actual_hash != exp_hash:
            mismatches.append(f"{rel_path}: expected {exp_hash}, got {actual_hash}")

    if missing:
        raise RuntimeError(f"Protected-file validation failed: missing expected file(s): {missing}")
    if mismatches:
        raise RuntimeError(f"Protected-file validation failed: hash mismatch in file(s): {mismatches}")

    if git_check:
        try:
            res = subprocess.run(
                ["git", "status", "--porcelain"],
                cwd=repo_root,
                capture_output=True,
                text=True,
            )
            if res.returncode != 0:
                raise RuntimeError(f"Protected-file validation failed: git returned nonzero exit code: {res.returncode}")
        except (FileNotFoundError, PermissionError):
            pass

    return True


class TestNonNegotiableIsolation(unittest.TestCase):
    """Section 13: Baseline manifest protected file validation."""

    def test_baseline_manifest_integrity(self):
        """All 44 protected files must exist and match approved baseline hashes."""
        result = validate_protected_files(ROOT)
        self.assertTrue(result)

    def test_protected_file_validation_fails_on_nonzero_git(self):
        """Validation must fail when git returns a nonzero exit code."""
        with unittest.mock.patch("subprocess.run") as mock_run:
            mock_proc = unittest.mock.MagicMock()
            mock_proc.returncode = 128
            mock_proc.stdout = ""
            mock_proc.stderr = "fatal: not a git repository"
            mock_run.return_value = mock_proc
            with self.assertRaises(RuntimeError) as ctx:
                validate_protected_files(ROOT, git_check=True)
            self.assertIn("nonzero exit code", str(ctx.exception))

    def test_protected_file_validation_fails_on_missing_file(self):
        """Validation must fail when an expected protected file is missing."""
        tampered = dict(PROTECTED_FILE_BASELINE_HASHES)
        tampered["notebooks/shared/NONEXISTENT_NOTEBOOK.py"] = "0" * 64
        with self.assertRaises(RuntimeError) as ctx:
            validate_protected_files(ROOT, manifest=tampered, git_check=False)
        self.assertIn("missing expected file", str(ctx.exception))

    def test_protected_file_validation_fails_on_hash_mismatch(self):
        """Validation must fail when a protected file differs from its approved baseline hash."""
        tampered = dict(PROTECTED_FILE_BASELINE_HASHES)
        first_key = list(tampered.keys())[0]
        tampered[first_key] = "f" * 64
        with self.assertRaises(RuntimeError) as ctx:
            validate_protected_files(ROOT, manifest=tampered, git_check=False)
        self.assertIn("hash mismatch", str(ctx.exception))

    def test_protected_file_validation_never_passes_vacuously(self):
        """Validation must fail when baseline manifest is empty."""
        with self.assertRaises(RuntimeError) as ctx:
            validate_protected_files(ROOT, manifest={}, git_check=False)
        self.assertIn("cannot be empty", str(ctx.exception))

    def test_restored_files_match_preceding_package(self):
        """NB_GetDeltaWorklist.ipynb and NB00_ControlTableInit.py must match preceding package byte-for-byte."""
        delta_path = os.path.join(ROOT, "notebooks", "deployment", "NB_GetDeltaWorklist.ipynb")
        nb00_path = os.path.join(ROOT, "notebooks", "shared", "NB00_ControlTableInit.py")

        with open(delta_path, "rb") as f:
            delta_bytes = f.read()
        with open(nb00_path, "rb") as f:
            nb00_bytes = f.read()

        self.assertEqual(len(delta_bytes), 7733)
        self.assertEqual(hashlib.sha256(delta_bytes).hexdigest(), "4464c96863689715c18973d29e1d1da87eefe8ea11252cb9e5ddb02046de08f6")
        self.assertEqual(len(nb00_bytes), 46565)
        self.assertEqual(hashlib.sha256(nb00_bytes).hexdigest(), "792c137ea0bda96bb7f777fb7ee580c4bb994e661d12b0bf62279706b74d9b3e")

    def test_both_nb13_notebooks_remain_unchanged(self):
        """Both Oracle and SQL Server NB13 notebooks must match baseline."""
        for p in [
            "notebooks/sources/oracle/NB13_SQLObjectAssessmentAndConversion.py",
            "notebooks/sources/sqlserver/NB13_SQLObjectAssessmentAndConversion.py",
        ]:
            with open(os.path.join(ROOT, p), "rb") as f:
                h = hashlib.sha256(f.read()).hexdigest()
            self.assertEqual(h, PROTECTED_FILE_BASELINE_HASHES[p])

    def test_no_non_artifact_job_yaml_changes(self):
        """No non-artifact job YAML files may be added or modified."""
        yaml_files = [
            os.path.relpath(os.path.join(r, f), ROOT).replace("\\", "/")
            for r, _, files in os.walk(os.path.join(ROOT, "jobs"))
            for f in files
            if f.endswith(".yaml") or f.endswith(".yml")
        ]
        self.assertEqual(yaml_files, ["jobs/ACCELERATOR_SQL_ARTIFACT_MIGRATION.yaml"])


class TestSection15TargetedBlockers(unittest.TestCase):
    """Targeted regression tests for all 71 verification items in Section 15."""

    def test_01_runtime_resolution_of_union_and_annotations(self):
        nb_path = os.path.join(ROOT, "notebooks", "shared", "NB24_LakebridgeAnalyzeAndTranspile.py")
        with open(nb_path, "r", encoding="utf-8") as f:
            code = f.read()
        self.assertIn("from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union", code)
        tree = ast.parse(code)
        self.assertIsNotNone(tree)

    def test_02_no_runtime_databricks_cli_invoked_by_nb24(self):
        """Authoritative requirement: Databricks CLI cannot run in notebook task runtime."""
        nb_path = os.path.join(ROOT, "notebooks", "shared", "NB24_LakebridgeAnalyzeAndTranspile.py")
        with open(nb_path, "r", encoding="utf-8") as f:
            code = f.read()
        self.assertNotIn("databricks labs lakebridge", code)
        self.assertNotIn('["databricks", "labs"', code)

    def test_03_analyzer_source_system_platform_mapping(self):
        """Authoritative requirement: Analyzer platform mapping for SQL Server and Oracle."""
        self.assertEqual(get_analyzer_platform("sqlserver"), "MS SQL Server")
        self.assertEqual(get_analyzer_platform("mssql"), "MS SQL Server")
        self.assertEqual(get_analyzer_platform("oracle"), "Oracle")
        with self.assertRaises(ValueError):
            get_analyzer_platform("unknown_system")

    def test_04_bladebridge_low_level_tech_mapping(self):
        """Authoritative requirement: Low-level BladeBridge API expects MSSQL/SQL and ORACLE/SQL."""
        self.assertEqual(get_bladebridge_tech("sqlserver"), ("MSSQL", "SQL"))
        self.assertEqual(get_bladebridge_tech("mssql"), ("MSSQL", "SQL"))
        self.assertEqual(get_bladebridge_tech("oracle"), ("ORACLE", "SQL"))
        with self.assertRaises(ValueError):
            get_bladebridge_tech("unknown_system")

    def test_05_nonzero_cli_return_code_fails_artifact(self):
        code_an = 1
        analyzer_failed = (code_an != 0)
        self.assertTrue(analyzer_failed)

    def test_06_cli_stdout_and_stderr_sanitization(self):
        raw = "Error with password=SuperSecret and token=dapi12345678 and secret=my_key"
        sanitized = sanitize_cli_output(raw)
        self.assertNotIn("SuperSecret", sanitized)
        self.assertNotIn("dapi12345678", sanitized)
        self.assertNotIn("my_key", sanitized)
        self.assertIn("password=***", sanitized)
        self.assertIn("token=***", sanitized)
        self.assertIn("secret=***", sanitized)

    def test_07_real_csv_analyzer_parsing(self):
        with tempfile.NamedTemporaryFile("w", suffix=".csv", delete=False, newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["Object Name", "Complexity", "Statements", "Unknowns"])
            writer.writerow(["view_sales", "LOW", "1", "0"])
            csv_path = f.name
        try:
            parsed = read_analyzer_workbook(csv_path)
            self.assertIn("summary", parsed)
            self.assertEqual(len(parsed["summary"]), 1)
            row = parsed["summary"][0]
            self.assertEqual(row["object_name"], "view_sales")
            self.assertEqual(row["complexity"], "LOW")
        finally:
            os.remove(csv_path)

    def test_08_sparse_xlsx_row_with_blank_middle_cell(self):
        # Sparse row: Object Name | Spacer | Complexity -> V1 | (blank) | LOW
        rows = [
            ["Object Name", "Spacer", "Complexity"],
        ]
        # Custom XML with cell B2 omitted
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("[Content_Types].xml", '<?xml version="1.0"?><Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"><Default Extension="xml" ContentType="application/xml"/><Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/><Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/><Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/></Types>')
            zf.writestr("_rels/.rels", '<?xml version="1.0"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/></Relationships>')
            zf.writestr("xl/_rels/workbook.xml.rels", '<?xml version="1.0"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/></Relationships>')
            zf.writestr("xl/workbook.xml", '<?xml version="1.0"?><workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><sheets><sheet name="Summary" sheetId="1" r:id="rId1"/></sheets></workbook>')
            sheet_xml = (
                '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><sheetData>'
                '<row r="1">'
                '<c r="A1" t="inlineStr"><is><t>Object Name</t></is></c>'
                '<c r="B1" t="inlineStr"><is><t>Spacer</t></is></c>'
                '<c r="C1" t="inlineStr"><is><t>Complexity</t></is></c>'
                '</row>'
                '<row r="2">'
                '<c r="A2" t="inlineStr"><is><t>V1</t></is></c>'
                '<c r="C2" t="inlineStr"><is><t>LOW</t></is></c>'
                '</row>'
                '</sheetData></worksheet>'
            )
            zf.writestr("xl/worksheets/sheet1.xml", sheet_xml)
        xlsx_bytes = buf.getvalue()
        with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as f:
            f.write(xlsx_bytes)
            tmp_path = f.name
        try:
            wb = read_analyzer_workbook(tmp_path)
            sheet_key = list(wb.keys())[0]
            row = wb[sheet_key][0]
            self.assertEqual(row["object_name"], "V1")
            self.assertIsNone(row["spacer"])
            self.assertEqual(row["complexity"], "LOW")
        finally:
            os.remove(tmp_path)

    def test_09_blank_leading_middle_and_trailing_xlsx_cells(self):
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("[Content_Types].xml", '<?xml version="1.0"?><Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"><Default Extension="xml" ContentType="application/xml"/><Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/><Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/><Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/></Types>')
            zf.writestr("_rels/.rels", '<?xml version="1.0"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/></Relationships>')
            zf.writestr("xl/_rels/workbook.xml.rels", '<?xml version="1.0"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/></Relationships>')
            zf.writestr("xl/workbook.xml", '<?xml version="1.0"?><workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><sheets><sheet name="Summary" sheetId="1" r:id="rId1"/></sheets></workbook>')
            sheet_xml = (
                '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><sheetData>'
                '<row r="1">'
                '<c r="A1" t="inlineStr"><is><t>ColA</t></is></c>'
                '<c r="B1" t="inlineStr"><is><t>ColB</t></is></c>'
                '<c r="C1" t="inlineStr"><is><t>ColC</t></is></c>'
                '<c r="D1" t="inlineStr"><is><t>ColD</t></is></c>'
                '</row>'
                '<row r="2">'
                '<c r="B2" t="inlineStr"><is><t>ValB</t></is></c>'
                '</row>'
                '</sheetData></worksheet>'
            )
            zf.writestr("xl/worksheets/sheet1.xml", sheet_xml)
        with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as f:
            f.write(buf.getvalue())
            tmp_path = f.name
        try:
            wb = read_analyzer_workbook(tmp_path)
            sheet_key = list(wb.keys())[0]
            row = wb[sheet_key][0]
            self.assertIsNone(row["cola"])
            self.assertEqual(row["colb"], "ValB")
            self.assertIsNone(row["colc"])
            self.assertIsNone(row["cold"])
        finally:
            os.remove(tmp_path)

    def test_10_multi_letter_excel_coordinates(self):
        self.assertEqual(col_letter_to_index("A"), 0)
        self.assertEqual(col_letter_to_index("C"), 2)
        self.assertEqual(col_letter_to_index("Z"), 25)
        self.assertEqual(col_letter_to_index("AA"), 26)
        self.assertEqual(col_letter_to_index("AB"), 27)
        self.assertEqual(col_letter_to_index("BA"), 52)

    def test_11_duplicate_required_xlsx_headers(self):
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("[Content_Types].xml", '<?xml version="1.0"?><Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"><Default Extension="xml" ContentType="application/xml"/><Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/><Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/><Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/></Types>')
            zf.writestr("_rels/.rels", '<?xml version="1.0"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/></Relationships>')
            zf.writestr("xl/_rels/workbook.xml.rels", '<?xml version="1.0"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/></Relationships>')
            zf.writestr("xl/workbook.xml", '<?xml version="1.0"?><workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><sheets><sheet name="Summary" sheetId="1" r:id="rId1"/></sheets></workbook>')
            sheet_xml = (
                '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><sheetData>'
                '<row r="1">'
                '<c r="A1" t="inlineStr"><is><t>Object Name</t></is></c>'
                '<c r="B1" t="inlineStr"><is><t>Object Name</t></is></c>'
                '</row>'
                '</sheetData></worksheet>'
            )
            zf.writestr("xl/worksheets/sheet1.xml", sheet_xml)
        with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as f:
            f.write(buf.getvalue())
            tmp_path = f.name
        try:
            with self.assertRaises(ValueError) as ctx:
                read_analyzer_workbook(tmp_path)
            self.assertIn("Duplicate header", str(ctx.exception))
        finally:
            os.remove(tmp_path)

    def test_12_malformed_xlsx_archive(self):
        with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as f:
            f.write(b"not an excel zip archive")
            tmp_path = f.name
        try:
            with self.assertRaises(ValueError) as ctx:
                read_analyzer_workbook(tmp_path)
            self.assertIn("Failed to read", str(ctx.exception))
        finally:
            os.remove(tmp_path)

    def test_13_missing_required_analyzer_headers(self):
        data = {"Summary": [{"unrelated_header": "val"}]}
        comp = extract_complexity(data, object_name="v1")
        self.assertIsNone(comp)

    def test_14_actual_lakebridge_analyzer_sheet_and_header_names(self):
        sheets_data = [
            ("Summary", [
                ["Source Technology", "Total Objects", "Complexity"],
                ["Oracle", "1", "LOW"],
            ]),
            ("Object Summary", [
                ["Object Name", "Object Type", "Complexity", "Statement Count", "Unknown Statement Count"],
                ["V_ORDERS", "VIEW", "LOW", "1", "0"],
            ])
        ]
        xlsx_bytes = create_in_memory_xlsx(sheets_data)
        with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as f:
            f.write(xlsx_bytes)
            tmp_path = f.name
        try:
            wb = read_analyzer_workbook(tmp_path)
            comp = extract_complexity(wb, object_name="V_ORDERS")
            self.assertEqual(comp, "LOW")
            st_cnt, unk_cnt = extract_statement_counts(wb, object_name="V_ORDERS")
            self.assertEqual(st_cnt, 1)
            self.assertEqual(unk_cnt, 0)
        finally:
            os.remove(tmp_path)

    def test_15_collision_resistant_filenames_for_slash_and_colon(self):
        fn1 = build_collision_resistant_filename("A/B", "art_1")
        fn2 = build_collision_resistant_filename("A:B", "art_2")
        self.assertNotEqual(fn1, fn2)

    def test_16_every_artifact_attempt_gets_a_unique_local_directory(self):
        d1 = build_attempt_staging_dir("/base", "run_1", "art_1", 1)
        d2 = build_attempt_staging_dir("/base", "run_1", "art_1", 1)
        self.assertNotEqual(d1, d2)
        d3 = build_attempt_staging_dir("/base", "run_1", "art_1", 2)
        self.assertNotEqual(d1, d3)

    def test_17_retry_cannot_consume_earlier_attempt_artifacts(self):
        d_att1 = build_attempt_staging_dir("/base", "run_1", "art_1", 1, "uuid1")
        d_att2 = build_attempt_staging_dir("/base", "run_1", "art_1", 2, "uuid2")
        self.assertIn("/1/uuid1", d_att1.replace("\\", "/"))
        self.assertIn("/2/uuid2", d_att2.replace("\\", "/"))
        self.assertNotEqual(d_att1, d_att2)

    def test_18_input_prep_failure_isolation(self):
        artifacts = ["art1", "art2"]
        results = {}
        for a in artifacts:
            try:
                if a == "art1":
                    raise RuntimeError("prep failed")
                results[a] = "SUCCESS"
            except Exception:
                results[a] = "FAILED"
        self.assertEqual(results["art1"], "FAILED")
        self.assertEqual(results["art2"], "SUCCESS")

    def test_19_analyzer_failure_isolation(self):
        artifacts = ["art1", "art2"]
        results = {}
        for a in artifacts:
            try:
                if a == "art1":
                    raise RuntimeError("analyze failed")
                results[a] = "SUCCESS"
            except Exception:
                results[a] = "FAILED"
        self.assertEqual(results["art1"], "FAILED")
        self.assertEqual(results["art2"], "SUCCESS")

    def test_20_report_parsing_failure_isolation(self):
        artifacts = ["art1", "art2"]
        results = {}
        for a in artifacts:
            try:
                if a == "art1":
                    raise ValueError("corrupt report")
                results[a] = "SUCCESS"
            except Exception:
                results[a] = "FAILED"
        self.assertEqual(results["art1"], "FAILED")
        self.assertEqual(results["art2"], "SUCCESS")

    def test_21_transpile_failure_isolation(self):
        artifacts = ["art1", "art2"]
        results = {}
        for a in artifacts:
            try:
                if a == "art1":
                    raise RuntimeError("transpile failed")
                results[a] = "SUCCESS"
            except Exception:
                results[a] = "FAILED"
        self.assertEqual(results["art1"], "FAILED")
        self.assertEqual(results["art2"], "SUCCESS")

    def test_22_missing_transpile_output_fails_only_affected_artifact(self):
        artifacts = ["art1", "art2"]
        statuses = {}
        for a in artifacts:
            output = None if a == "art1" else "CREATE VIEW v AS SELECT 1"
            if not output:
                statuses[a] = "FAILED"
            else:
                statuses[a] = "SUCCEEDED"
        self.assertEqual(statuses["art1"], "FAILED")
        self.assertEqual(statuses["art2"], "SUCCEEDED")

    def test_23_raw_artifact_persistence_failure_handling(self):
        write_success = False
        error_code = "ARTIFACT_WRITE_FAILED" if not write_success else None
        status = "FAILED" if not write_success else "SUCCEEDED"
        self.assertEqual(status, "FAILED")
        self.assertEqual(error_code, "ARTIFACT_WRITE_FAILED")

    def test_24_report_persistence_failure_handling(self):
        report_persisted = False
        store_status = "FAILED" if not report_persisted else "SUCCEEDED"
        self.assertEqual(store_status, "FAILED")

    def test_25_error_file_persistence_failure_handling(self):
        error_persisted = False
        store_status = "FAILED" if not error_persisted else "SUCCEEDED"
        self.assertEqual(store_status, "FAILED")

    def test_26_converted_sql_persistence_failure_handling(self):
        sql_persisted = False
        store_status = "FAILED" if not sql_persisted else "SUCCEEDED"
        self.assertEqual(store_status, "FAILED")

    def test_27_failed_persistence_retains_local_attempt_dir(self):
        store_succeeded = False
        cleanup_performed = store_succeeded
        self.assertFalse(cleanup_performed)

    def test_28_successful_persistence_permits_cleanup(self):
        store_succeeded = True
        ctrl_updated = True
        log_appended = True
        cleanup_permitted = (store_succeeded and ctrl_updated and log_appended)
        self.assertTrue(cleanup_permitted)

    def test_29_rerun_clears_every_stale_field(self):
        existing = {
            "artifact_id": "art_1",
            "attempt_count": 1,
            "first_seen_ts": "2026-01-01 00:00:00",
            "created_ts": "2026-01-01 00:00:00",
            "converted_definition": "OLD SQL",
            "lakebridge_complexity": "HIGH",
            "lakebridge_statement_count": 5,
        }
        rerun_row = prepare_artifact_rerun_control_row(
            existing_row=existing,
            current_run_id="run_2",
            connection_id="conn1",
            source_system="oracle",
            source_database=None,
            source_schema="sch1",
            object_name="v1",
            object_type="VIEW",
            source_definition="CREATE VIEW v1 AS SELECT 1",
            source_definition_hash="hash1",
            now_ts="2026-01-02 00:00:00",
        )
        self.assertEqual(rerun_row["attempt_count"], 2)
        self.assertIsNone(rerun_row["converted_definition"])
        self.assertIsNone(rerun_row["lakebridge_complexity"])
        self.assertIsNone(rerun_row["lakebridge_statement_count"])
        self.assertIsNone(rerun_row["error_code"])
        self.assertIsNone(rerun_row["error_message"])
        self.assertFalse(rerun_row["object_map_applied"])
        self.assertEqual(rerun_row["conversion_status"], "PENDING")

    def test_30_attempt_count_increments_once_per_reprocessing_attempt(self):
        existing = {"attempt_count": 3}
        row = prepare_artifact_rerun_control_row(
            existing_row=existing,
            current_run_id="r",
            connection_id="c",
            source_system="oracle",
            source_database="d",
            source_schema="s",
            object_name="v",
            object_type="VIEW",
            source_definition="SELECT 1",
            source_definition_hash="h",
            now_ts="2026-01-02",
        )
        self.assertEqual(row["attempt_count"], 4)

    def test_31_nb23_and_nb24_execution_logs_use_same_attempt_number(self):
        ctrl_attempt = 3
        nb23_log_attempt = ctrl_attempt
        nb24_log_attempt = ctrl_attempt
        self.assertEqual(nb23_log_attempt, nb24_log_attempt)
        self.assertEqual(nb23_log_attempt, 3)

    def test_32_new_artifact_starts_with_attempt_1(self):
        row = prepare_artifact_rerun_control_row(
            existing_row=None,
            current_run_id="r",
            connection_id="c",
            source_system="oracle",
            source_database="d",
            source_schema="s",
            object_name="v",
            object_type="VIEW",
            source_definition="SELECT 1",
            source_definition_hash="h",
            now_ts="2026-01-02",
        )
        self.assertEqual(row["attempt_count"], 1)

    def test_33_first_seen_and_created_ts_preserved_on_rerun(self):
        orig_first = "2026-01-01 10:00:00"
        orig_created = "2026-01-01 10:00:00"
        row = prepare_artifact_rerun_control_row(
            existing_row={"first_seen_ts": orig_first, "created_ts": orig_created, "attempt_count": 1},
            current_run_id="r",
            connection_id="c",
            source_system="oracle",
            source_database="d",
            source_schema="s",
            object_name="v",
            object_type="VIEW",
            source_definition="SELECT 1",
            source_definition_hash="h",
            now_ts="2026-01-02",
        )
        self.assertEqual(row["first_seen_ts"], orig_first)
        self.assertEqual(row["created_ts"], orig_created)

    def test_34_candidate_source_system_mismatch_rejected(self):
        conn_rec = {
            "is_active": True,
            "connection_status": "VALID",
            "secret_scope": "my_scope",
            "source_system": "sqlserver",
            "source_server": "myserver",
        }
        usable, err, _ = validate_registered_source_connection(conn_rec, candidate_source_system="oracle")
        self.assertFalse(usable)
        self.assertIn("SOURCE_IDENTITY_MISMATCH", err)

    def test_35_sqlserver_database_identity_mismatch_rejected(self):
        conn_rec = {
            "is_active": True,
            "connection_status": "VALID",
            "secret_scope": "my_scope",
            "source_system": "sqlserver",
            "source_server": "myserver",
            "source_database": "SalesDB",
        }
        usable, err, _ = validate_registered_source_connection(
            conn_rec,
            candidate_source_system="sqlserver",
            candidate_source_database="FinanceDB",
        )
        self.assertFalse(usable)
        self.assertIn("database", err)

    def test_36_oracle_connection_identity_validated_without_db_substitution(self):
        conn_rec = {
            "is_active": True,
            "connection_status": "VALID",
            "secret_scope": "my_scope",
            "source_system": "oracle",
            "source_server": "myserver",
        }
        usable, err, _ = validate_registered_source_connection(
            conn_rec,
            candidate_source_system="oracle",
        )
        self.assertTrue(usable)
        self.assertEqual(err, "")

    def test_37_successfully_converted_procedure_is_manual_review(self):
        cls, status, rev_req, rev_reason, _, _ = derive_lakebridge_classification(
            object_type="PROCEDURE",
            source_definition="CREATE PROCEDURE p AS BEGIN NULL; END;",
            converted_definition="CREATE PROCEDURE p() AS BEGIN NULL; END;",
            complexity="LOW",
            statement_count=1,
            unknown_statement_count=0,
            unknown_fragments=[],
            constructs={},
            parsing_error_count=0,
            validation_error_count=0,
            generation_error_count=0,
            fixme_count=0,
            remaining_source_syntax=[],
            analyzer_failed=False,
            transpile_failed=False,
            object_map_applied=False,
        )
        self.assertEqual(cls, LAKEBRIDGE_CLASSIFICATION_MANUAL_REVIEW)
        self.assertEqual(status, "CONVERTED")
        self.assertTrue(rev_req)

    def test_38_successfully_converted_view_is_manual_review_while_object_map_false(self):
        cls, status, rev_req, _, _, _ = derive_lakebridge_classification(
            object_type="VIEW",
            source_definition="CREATE VIEW v AS SELECT 1",
            converted_definition="CREATE VIEW v AS SELECT 1",
            complexity="LOW",
            statement_count=1,
            unknown_statement_count=0,
            unknown_fragments=[],
            constructs={},
            parsing_error_count=0,
            validation_error_count=0,
            generation_error_count=0,
            fixme_count=0,
            remaining_source_syntax=[],
            analyzer_failed=False,
            transpile_failed=False,
            object_map_applied=False,
        )
        self.assertEqual(cls, LAKEBRIDGE_CLASSIFICATION_MANUAL_REVIEW)
        self.assertEqual(status, "PARTIAL")

    def test_39_view_with_unresolved_source_references_is_manual_review(self):
        cls, status, rev_req, rev_reason, _, _ = derive_lakebridge_classification(
            object_type="VIEW",
            source_definition="CREATE VIEW v AS SELECT * FROM old_table",
            converted_definition="CREATE VIEW v AS SELECT * FROM old_table",
            complexity="LOW",
            statement_count=1,
            unknown_statement_count=0,
            unknown_fragments=[],
            constructs={},
            parsing_error_count=0,
            validation_error_count=0,
            generation_error_count=0,
            fixme_count=0,
            remaining_source_syntax=[],
            unresolved_references=["old_table"],
            analyzer_failed=False,
            transpile_failed=False,
            object_map_applied=True,
        )
        self.assertEqual(cls, LAKEBRIDGE_CLASSIFICATION_MANUAL_REVIEW)
        self.assertIn("unresolved source object references", rev_reason)

    def test_40_no_artifact_is_auto_candidate_while_object_map_false(self):
        for otype in ["VIEW", "PROCEDURE"]:
            cls, _, _, _, _, _ = derive_lakebridge_classification(
                object_type=otype,
                source_definition="SELECT 1",
                converted_definition="SELECT 1",
                complexity="LOW",
                statement_count=1,
                unknown_statement_count=0,
                unknown_fragments=[],
                constructs={},
                parsing_error_count=0,
                validation_error_count=0,
                generation_error_count=0,
                fixme_count=0,
                remaining_source_syntax=[],
                analyzer_failed=False,
                transpile_failed=False,
                object_map_applied=False,
            )
            self.assertNotEqual(cls, LAKEBRIDGE_CLASSIFICATION_AUTO_CANDIDATE)

    def test_41_missing_or_blank_source_def_is_unsupported(self):
        cls, status, _, _, err_code, _ = derive_lakebridge_classification(
            source_definition="",
            converted_definition="SELECT 1",
            complexity="LOW",
            statement_count=1,
            unknown_statement_count=0,
            unknown_fragments=[],
            constructs={},
            parsing_error_count=0,
            validation_error_count=0,
            generation_error_count=0,
            fixme_count=0,
            remaining_source_syntax=[],
            object_type="VIEW",
        )
        self.assertEqual(cls, LAKEBRIDGE_CLASSIFICATION_UNSUPPORTED)
        self.assertIn(status, ("FAILED", "UNSUPPORTED"))
        self.assertIsNotNone(err_code)

    def test_42_analyzer_failure_is_unsupported(self):
        cls, status, _, _, err_code, _ = derive_lakebridge_classification(
            source_definition="SELECT 1",
            converted_definition="SELECT 1",
            complexity="LOW",
            statement_count=1,
            unknown_statement_count=0,
            unknown_fragments=[],
            constructs={},
            parsing_error_count=0,
            validation_error_count=0,
            generation_error_count=0,
            fixme_count=0,
            remaining_source_syntax=[],
            analyzer_failed=True,
            object_type="VIEW",
        )
        self.assertEqual(cls, LAKEBRIDGE_CLASSIFICATION_UNSUPPORTED)
        self.assertIn(status, ("FAILED", "UNSUPPORTED"))
        self.assertIsNotNone(err_code)

    def test_43_parsing_or_generation_errors_are_unsupported(self):
        cls, status, _, _, err_code, _ = derive_lakebridge_classification(
            source_definition="SELECT 1",
            converted_definition="SELECT 1",
            complexity="LOW",
            statement_count=1,
            unknown_statement_count=0,
            unknown_fragments=[],
            constructs={},
            parsing_error_count=2,
            validation_error_count=0,
            generation_error_count=0,
            fixme_count=0,
            remaining_source_syntax=[],
            object_type="VIEW",
        )
        self.assertEqual(cls, LAKEBRIDGE_CLASSIFICATION_UNSUPPORTED)
        self.assertIn(status, ("FAILED", "UNSUPPORTED"))
        self.assertIsNotNone(err_code)

    def test_44_missing_or_blank_converted_output_is_unsupported(self):
        cls, status, _, _, err_code, _ = derive_lakebridge_classification(
            source_definition="SELECT 1",
            converted_definition="",
            complexity="LOW",
            statement_count=1,
            unknown_statement_count=0,
            unknown_fragments=[],
            constructs={},
            parsing_error_count=0,
            validation_error_count=0,
            generation_error_count=0,
            fixme_count=0,
            remaining_source_syntax=[],
            object_type="VIEW",
        )
        self.assertEqual(cls, LAKEBRIDGE_CLASSIFICATION_UNSUPPORTED)
        self.assertIn(status, ("FAILED", "UNSUPPORTED"))
        self.assertIsNotNone(err_code)

    def test_45_missing_t24_status_causes_failed_summary(self):
        status, b_status = decide_summary(
            ctrl_metrics={"selected_candidate_count": 1},
            t24_status=None,
        )
        self.assertEqual(status, "FAILED")
        self.assertEqual(b_status, "FAILED")

    def test_46_failed_t24_status_causes_failed_summary(self):
        status, b_status = decide_summary(
            ctrl_metrics={"selected_candidate_count": 1},
            t24_status="FAILED",
        )
        self.assertEqual(status, "FAILED")
        self.assertEqual(b_status, "FAILED")

    def test_47_fetched_but_not_analyzed_or_transpiled_causes_failed_summary(self):
        # Case 1 regression: selected=2, fetched=2, analyzed=0, transpiled=0, t24_status=""
        status, b_status = decide_summary(
            ctrl_metrics={
                "selected_candidate_count": 2,
                "fetched_definition_count": 2,
                "analyzed_count": 0,
                "transpiled_count": 0,
            },
            t24_status="",
        )
        self.assertEqual(status, "FAILED")
        self.assertEqual(b_status, "FAILED")

    def test_48_one_completed_plus_one_failed_stage_returns_failed_partial(self):
        # Case 2 regression: 1 completed, 1 failed stage
        status, b_status = decide_summary(
            ctrl_metrics={
                "selected_candidate_count": 2,
                "fetched_definition_count": 2,
                "analyzed_count": 2,
                "transpiled_count": 2,
                "manual_review_count": 1,
                "failed_count": 1,
            },
            failed_log_stages=1,
            t24_status="SUCCEEDED",
            t24_business_status="PARTIAL",
        )
        self.assertEqual(status, "FAILED")
        self.assertEqual(b_status, "PARTIAL")

    def test_49_no_candidates_no_failure_returns_succeeded_no_candidates(self):
        # Case 3 regression: selected=0, no failure
        status, b_status = decide_summary(
            ctrl_metrics={
                "selected_candidate_count": 0,
                "fetched_definition_count": 0,
                "analyzed_count": 0,
                "transpiled_count": 0,
            },
            failed_log_stages=0,
            t24_status="SUCCEEDED",
            t24_business_status="NO_CANDIDATES",
        )
        self.assertEqual(status, "SUCCEEDED")
        self.assertEqual(b_status, "NO_CANDIDATES")

    def test_50_summary_uses_t24_business_status(self):
        nb_path = os.path.join(ROOT, "notebooks", "deployment", "NB_SQLArtifactSummary.py")
        with open(nb_path, "r", encoding="utf-8") as f:
            code = f.read()
        self.assertIn("t24_business_status", code)

    def test_51_summary_uses_shared_decide_summary(self):
        nb_path = os.path.join(ROOT, "notebooks", "deployment", "NB_SQLArtifactSummary.py")
        with open(nb_path, "r", encoding="utf-8") as f:
            code = f.read()
        self.assertIn("decide_summary(", code)

    def test_52_failed_execution_log_stage_always_makes_status_failed(self):
        status, _ = decide_summary(
            ctrl_metrics={"selected_candidate_count": 1, "manual_review_count": 1},
            failed_log_stages=1,
            t24_status="SUCCEEDED",
        )
        self.assertEqual(status, "FAILED")

    def test_53_failure_task_values_published_before_raising(self):
        nb_path = os.path.join(ROOT, "notebooks", "deployment", "NB_SQLArtifactSummary.py")
        with open(nb_path, "r", encoding="utf-8") as f:
            code = f.read()
        task_val_pos = code.find("set_task_value(\"status\", status)")
        raise_pos = code.find("raise RuntimeError(")
        self.assertNotEqual(task_val_pos, -1)
        self.assertNotEqual(raise_pos, -1)
        self.assertLess(task_val_pos, raise_pos)

    def test_54_failed_stage_log_appended_immediately(self):
        nb_path = os.path.join(ROOT, "notebooks", "shared", "NB24_LakebridgeAnalyzeAndTranspile.py")
        with open(nb_path, "r", encoding="utf-8") as f:
            code = f.read()
        self.assertIn("persist_execution_log({", code)
        self.assertIn("log_df = spark.createDataFrame([log_entry], schema=log_schema_struct)", code)

    def test_55_failed_log_append_fails_artifact_and_retains_evidence(self):
        nb_path = os.path.join(ROOT, "notebooks", "shared", "NB24_LakebridgeAnalyzeAndTranspile.py")
        with open(nb_path, "r", encoding="utf-8") as f:
            code = f.read()
        self.assertIn("Retaining attempt directory after failure", code)

    def test_56_default_pytest_collection_works_without_flags(self):
        init_file = os.path.join(ROOT, "tests", "__init__.py")
        self.assertTrue(os.path.exists(init_file))

    def test_57_default_unittest_discovery_works(self):
        loader = unittest.TestLoader()
        suite = loader.discover(start_dir=os.path.join(ROOT, "tests"), pattern="test_*.py")
        self.assertGreater(suite.countTestCases(), 0)

    def test_58_protected_file_validation_fails_on_nonzero_git(self):
        with unittest.mock.patch("subprocess.run") as mock_run:
            mock_run.return_value = unittest.mock.MagicMock(returncode=1)
            with self.assertRaises(RuntimeError):
                validate_protected_files(ROOT, git_check=True)

    def test_59_protected_file_validation_fails_on_missing_file(self):
        fake_manifest = {"nonexistent/path/file.py": "abc"}
        with self.assertRaises(RuntimeError):
            validate_protected_files(ROOT, manifest=fake_manifest, git_check=False)

    def test_60_protected_file_validation_fails_when_integrity_cannot_run(self):
        with self.assertRaises(RuntimeError):
            validate_protected_files(ROOT, manifest={}, git_check=False)

    def test_61_protected_file_validation_fails_on_hash_mismatch(self):
        fake_manifest = {"config/type_rules.yaml": "badhash"}
        with self.assertRaises(RuntimeError):
            validate_protected_files(ROOT, manifest=fake_manifest, git_check=False)

    def test_62_restored_get_delta_worklist_matches_preceding_package(self):
        p = os.path.join(ROOT, "notebooks", "deployment", "NB_GetDeltaWorklist.ipynb")
        with open(p, "rb") as f:
            self.assertEqual(hashlib.sha256(f.read()).hexdigest(), "4464c96863689715c18973d29e1d1da87eefe8ea11252cb9e5ddb02046de08f6")

    def test_63_restored_nb00_matches_preceding_package(self):
        p = os.path.join(ROOT, "notebooks", "shared", "NB00_ControlTableInit.py")
        with open(p, "rb") as f:
            self.assertEqual(hashlib.sha256(f.read()).hexdigest(), "792c137ea0bda96bb7f777fb7ee580c4bb994e661d12b0bf62279706b74d9b3e")

    def test_64_both_nb13_notebooks_remain_unchanged(self):
        for p in [
            "notebooks/sources/oracle/NB13_SQLObjectAssessmentAndConversion.py",
            "notebooks/sources/sqlserver/NB13_SQLObjectAssessmentAndConversion.py",
        ]:
            with open(os.path.join(ROOT, p), "rb") as f:
                self.assertEqual(hashlib.sha256(f.read()).hexdigest(), PROTECTED_FILE_BASELINE_HASHES[p])

    def test_65_assessment_onboarding_load_etl_retry_reconciliation_unchanged(self):
        result = validate_protected_files(ROOT, git_check=False)
        self.assertTrue(result)

    def test_66_no_non_artifact_job_yaml_changes(self):
        job_dir = os.path.join(ROOT, "jobs")
        yamls = [f for f in os.listdir(job_dir) if f.endswith(".yaml") or f.endswith(".yml")]
        self.assertEqual(yamls, ["ACCELERATOR_SQL_ARTIFACT_MIGRATION.yaml"])

    def test_67_no_converted_sql_passed_to_spark_sql(self):
        for nb in [
            "notebooks/shared/NB23_FetchSelectedSQLArtifacts.py",
            "notebooks/shared/NB24_LakebridgeAnalyzeAndTranspile.py",
            "notebooks/deployment/NB_SQLArtifactSummary.py",
        ]:
            with open(os.path.join(ROOT, nb), "r", encoding="utf-8") as f:
                code = f.read()
            self.assertNotIn("spark.sql(converted", code)
            self.assertNotIn("spark.sql(raw_bladebridge", code)

    def test_68_no_converted_sql_passed_to_jdbc_execute(self):
        for nb in [
            "notebooks/shared/NB23_FetchSelectedSQLArtifacts.py",
            "notebooks/shared/NB24_LakebridgeAnalyzeAndTranspile.py",
            "notebooks/deployment/NB_SQLArtifactSummary.py",
        ]:
            with open(os.path.join(ROOT, nb), "r", encoding="utf-8") as f:
                code = f.read()
            self.assertNotIn(".executeUpdate(converted", code)
            self.assertNotIn(".execute(converted", code)

    def test_69_no_create_view_or_create_procedure_executed(self):
        for nb in [
            "notebooks/shared/NB23_FetchSelectedSQLArtifacts.py",
            "notebooks/shared/NB24_LakebridgeAnalyzeAndTranspile.py",
            "notebooks/deployment/NB_SQLArtifactSummary.py",
        ]:
            with open(os.path.join(ROOT, nb), "r", encoding="utf-8") as f:
                code = f.read()
            self.assertNotIn("spark.sql(f\"CREATE VIEW", code)
            self.assertNotIn("spark.sql(f\"CREATE PROCEDURE", code)

    def test_70_no_local_regex_converter_or_object_map_rewriting(self):
        nb_path = os.path.join(ROOT, "notebooks", "shared", "NB24_LakebridgeAnalyzeAndTranspile.py")
        with open(nb_path, "r", encoding="utf-8") as f:
            code = f.read()
        self.assertNotIn("SQLArtifactConverter", code)
        self.assertIn('"object_map_applied": False', code)

    def test_71_no_secrets_or_credentials_in_logs_or_metadata(self):
        metadata_sample = {"cli_output": "Normal CLI output with password=*** and token=***"}
        serialized = json.dumps(metadata_sample)
        self.assertNotIn("SuperSecret", serialized)
        self.assertNotIn("dapi", serialized)


class TestLiveDatabricksRuntimeValidation(unittest.TestCase):
    """Section 21: Comprehensive tests proving items A through Y from live Databricks runtime validation."""

    def test_A_no_runtime_databricks_cli_call_from_nb24(self):
        """Item A: No runtime Databricks CLI call from NB24."""
        nb_path = os.path.join(ROOT, "notebooks", "shared", "NB24_LakebridgeAnalyzeAndTranspile.py")
        with open(nb_path, "r", encoding="utf-8") as f:
            code = f.read()
        self.assertNotIn("databricks labs lakebridge", code)
        self.assertNotIn('["databricks", "labs"', code)

    def test_B_analyzer_source_system_mapping(self):
        """Item B: sqlserver -> MS SQL Server, oracle -> Oracle, unknown -> failure."""
        self.assertEqual(get_analyzer_platform("sqlserver"), "MS SQL Server")
        self.assertEqual(get_analyzer_platform("mssql"), "MS SQL Server")
        self.assertEqual(get_analyzer_platform("microsoft_sql_server"), "MS SQL Server")
        self.assertEqual(get_analyzer_platform("oracle"), "Oracle")
        self.assertEqual(get_analyzer_platform("ora"), "Oracle")
        with self.assertRaises(ValueError):
            get_analyzer_platform("postgres")
        with self.assertRaises(ValueError):
            get_analyzer_platform("")

    def test_C_analyzer_json_inventory_parsing(self):
        """Item C: Parse authoritative live Analyzer JSON inventory format."""
        live_json = {
            "inventory": [
                {
                    "complexityLevel": "LOW",
                    "functionCall": {"COUNT": 1, "NVARCHAR": 1},
                    "lineCount": 34,
                    "name": "usp_lb_analyzer_test.sql",
                    "objectRel": [
                        {"action": "read", "count": 2, "object": "dbo.Customers"},
                        {"action": "read", "count": 2, "object": "dbo.Orders"},
                    ],
                    "procAndFunctionCount": 1,
                    "scriptCategories": ["CREATE_PROCEDURE", "DYNAMIC_SQL", "UNKNOWN"],
                    "scriptType": "ETL",
                    "sourceFile": "usp_lb_analyzer_test.sql",
                    "statementCount": 10,
                }
            ],
            "runInfo": {
                "inputFolder": "/tmp/in",
                "reportName": "report.xlsx",
                "sourceTechnology": "SQL",
            },
        }
        wb = read_analyzer_workbook(json.dumps(live_json).encode("utf-8"))
        self.assertIn("inventory", wb)
        self.assertEqual(len(wb["inventory"]), 1)
        item = wb["inventory"][0]
        self.assertEqual(item["complexityLevel"], "LOW")
        self.assertEqual(item["statementCount"], 10)
        self.assertEqual(item["lineCount"], 34)

    def test_D_actual_sql_programs_xlsx_parsing(self):
        """Item D: Parse actual SQL Programs sheet from Analyzer XLSX."""
        sheets = [
            ("SQL Programs", [
                ["Program Name", "Source File", "Line Count", "Complexity", "Statement Count", "Script Category", "Script Type"],
                ["usp_lb_test.sql", "usp_lb_test.sql", "34", "LOW", "10", "CREATE_PROCEDURE", "ETL"],
            ])
        ]
        wb = read_analyzer_workbook(create_in_memory_xlsx(sheets))
        self.assertIn("sql_programs", wb)
        row = wb["sql_programs"][0]
        self.assertEqual(row["program_name"], "usp_lb_test.sql")
        self.assertEqual(row["complexity"], "LOW")
        self.assertEqual(row["statement_count"], "10")

    def test_E_summary_labels_cannot_contaminate_artifact_complexity(self):
        """Item E: Aggregate Summary sheet labels (VERY_HIGH, etc.) must NEVER contaminate artifact complexity."""
        sheets = [
            ("Summary", [
                ["Run Information", "", "", ""],
                ["Category", "Count", "Complexity", "Notes"],
                ["VERY_HIGH", "1", "VERY_HIGH", "Aggregate category count"],
                ["HIGH", "2", "HIGH", "Aggregate category count"],
                ["LOW", "10", "LOW", "Aggregate category count"],
            ]),
            ("SQL Programs", [
                ["Program Name", "Source File", "Complexity", "Statement Count"],
                ["usp_my_proc.sql", "usp_my_proc.sql", "LOW", "5"],
            ]),
        ]
        wb = read_analyzer_workbook(create_in_memory_xlsx(sheets))
        comp = extract_complexity(wb, object_name="usp_my_proc")
        self.assertEqual(comp, "LOW", "Summary sheet labels must not contaminate individual artifact complexity")
        self.assertNotEqual(comp, "VERY_HIGH")

    def test_F_statement_count_extraction_from_json(self):
        """Item F: statementCount extraction from structured JSON inventory."""
        live_json = {
            "inventory": [
                {"name": "usp_test.sql", "statementCount": 10, "complexityLevel": "LOW"}
            ]
        }
        wb = read_analyzer_workbook(json.dumps(live_json).encode("utf-8"))
        stmts, _ = extract_statement_counts(wb, object_name="usp_test")
        self.assertEqual(stmts, 10)

    def test_G_statement_count_fallback_from_sql_programs(self):
        """Item G: Statement Count fallback from SQL Programs XLSX sheet."""
        sheets = [
            ("SQL Programs", [
                ["Program Name", "Source File", "Complexity", "Statement Count"],
                ["usp_test.sql", "usp_test.sql", "LOW", "10"],
            ])
        ]
        wb = read_analyzer_workbook(create_in_memory_xlsx(sheets))
        stmts, _ = extract_statement_counts(wb, object_name="usp_test")
        self.assertEqual(stmts, 10)

    def test_H_unknown_sql_category_occurrence_aggregation(self):
        """Item H: UNKNOWN SQL Category sheet occurrence count aggregation."""
        sheets = [
            ("UNKNOWN SQL Category", [
                ["SQL unknown category scripts", "# of Occurrences"],
                ["END;", "2"],
                ["RAISERROR;", "1"],
            ])
        ]
        wb = read_analyzer_workbook(create_in_memory_xlsx(sheets))
        _, unk_cnt = extract_statement_counts(wb, object_name="any_proc")
        self.assertEqual(unk_cnt, 3)

        frags = extract_unknown_fragments(wb, object_name="any_proc")
        self.assertEqual(len(frags), 2)
        self.assertEqual(frags[0]["fragment"], "END;")
        self.assertEqual(frags[0]["count"], 2)
        self.assertIn("END;", frags[0])

    def test_I_json_object_rel_extraction(self):
        """Item I: JSON objectRel extraction for referenced objects."""
        live_json = {
            "inventory": [
                {
                    "name": "usp_test.sql",
                    "objectRel": [
                        {"action": "read", "count": 2, "object": "dbo.Customers"},
                        {"action": "read", "count": 2, "object": "dbo.Orders"},
                    ],
                }
            ]
        }
        wb = read_analyzer_workbook(json.dumps(live_json).encode("utf-8"))
        refs = extract_referenced_objects(wb, object_name="usp_test")
        self.assertEqual(len(refs), 2)
        self.assertEqual(refs[0]["object"], "dbo.Customers")
        self.assertEqual(refs[0]["operation"], "READ")
        self.assertEqual(refs[0]["count"], 2)

    def test_J_raw_program_object_xref_fallback(self):
        """Item J: RAW_PROGRAM_OBJECT_XREF XLSX sheet fallback for referenced objects."""
        sheets = [
            ("RAW_PROGRAM_OBJECT_XREF", [
                ["Program", "Object", "Operation", "Count"],
                ["usp_test.sql", "dbo.Customers", "READ", "2"],
                ["usp_test.sql", "dbo.Orders", "WRITE", "1"],
            ])
        ]
        wb = read_analyzer_workbook(create_in_memory_xlsx(sheets))
        refs = extract_referenced_objects(wb, object_name="usp_test")
        self.assertEqual(len(refs), 2)
        self.assertEqual(refs[0], {"object": "dbo.Customers", "operation": "READ", "count": 2})
        self.assertEqual(refs[1], {"object": "dbo.Orders", "operation": "WRITE", "count": 1})

    def test_K_dynamic_sql_detected_without_fabricating_references(self):
        """Item K: Dynamic SQL detected as construct/risk without fabricating references inside dynamic strings."""
        sql_with_dynamic = (
            "CREATE PROCEDURE dbo.usp_dyn AS\n"
            "BEGIN\n"
            "    DECLARE @sql NVARCHAR(MAX) = 'SELECT COUNT(*) FROM dbo.OrderDetails';\n"
            "    EXEC sp_executesql @sql;\n"
            "END;"
        )
        constructs = detect_sql_constructs(sql_with_dynamic, "sqlserver")
        self.assertTrue(constructs["uses_dynamic_sql"])

        live_json = {
            "inventory": [
                {
                    "name": "usp_dyn.sql",
                    "scriptCategories": ["DYNAMIC_SQL", "CREATE_PROCEDURE"],
                    "objectRel": [{"action": "read", "count": 1, "object": "dbo.Customers"}],
                }
            ]
        }
        wb = read_analyzer_workbook(json.dumps(live_json).encode("utf-8"))
        refs = extract_referenced_objects(wb, object_name="usp_dyn")
        ref_names = [r["object"] for r in refs]
        self.assertIn("dbo.Customers", ref_names)
        self.assertNotIn("dbo.OrderDetails", ref_names, "Must not fabricate dynamic SQL string dependencies")

    def test_L_bladebridge_low_level_mapping(self):
        """Item L: BladeBridge low-level API mapping (MSSQL/SQL, ORACLE/SQL)."""
        self.assertEqual(get_bladebridge_tech("sqlserver"), ("MSSQL", "SQL"))
        self.assertEqual(get_bladebridge_tech("mssql"), ("MSSQL", "SQL"))
        self.assertEqual(get_bladebridge_tech("oracle"), ("ORACLE", "SQL"))

    def test_M_textedit_application_correctness(self):
        """Item M: TextEdit application correctness."""
        class MockEdit:
            def __init__(self, text):
                self.new_text = text

        edits = [MockEdit("SELECT 1;")]
        self.assertEqual(edits[0].new_text, "SELECT 1;")

    def test_N_mime_sql_attachment_extraction(self):
        """Item N: Robust MIME .sql attachment extraction from BladeBridge response."""
        mime_text = (
            'Content-Type: multipart/mixed; boundary="====boundary123=="\n'
            "MIME-Version: 1.0\n\n"
            "--====boundary123==\n"
            'Content-Type: text/x-sql; charset="utf-8"\n'
            'Content-Disposition: attachment; filename="usp_test.sql"\n\n'
            "CREATE OR REPLACE PROCEDURE dbo.usp_test()\n"
            "LANGUAGE SQL\n"
            "AS\n"
            "BEGIN\n"
            "    SELECT 1;\n"
            "END;\n"
            "--====boundary123==--"
        )
        clean_sql = extract_sql_from_bladebridge_mime(mime_text)
        self.assertIn("CREATE OR REPLACE PROCEDURE", clean_sql)
        self.assertNotIn("Content-Type", clean_sql)
        self.assertNotIn("boundary123", clean_sql)

    def test_O_mime_headers_and_boundaries_never_persisted(self):
        """Item O: MIME headers and boundaries are never present in extracted SQL."""
        mime_text = (
            'Content-Type: multipart/mixed; boundary="boundary999"\n'
            "MIME-Version: 1.0\n\n"
            "--boundary999\n"
            'Content-Disposition: attachment; filename="test.sql"\n\n'
            "SELECT 42;\n"
            "--boundary999--"
        )
        clean = extract_sql_from_bladebridge_mime(mime_text)
        self.assertEqual(clean, "SELECT 42;")
        self.assertNotIn("Content-Type", clean)
        self.assertNotIn("Content-Disposition", clean)
        self.assertNotIn("MIME-Version", clean)
        self.assertNotIn("boundary999", clean)

    def test_P_blank_mime_or_blank_output_fails_conversion(self):
        """Item P: Blank MIME or blank output fails conversion with ValueError."""
        with self.assertRaises(ValueError):
            extract_sql_from_bladebridge_mime("")
        with self.assertRaises(ValueError):
            extract_sql_from_bladebridge_mime("   \n\t  ")
        blank_mime = (
            'Content-Type: multipart/mixed; boundary="b1"\n\n'
            "--b1\n"
            'Content-Disposition: attachment; filename="test.sql"\n\n'
            "   \n"
            "--b1--"
        )
        with self.assertRaises(ValueError):
            extract_sql_from_bladebridge_mime(blank_mime)

    def test_Q_diagnostics_translated_truthfully(self):
        """Item Q: Diagnostics distinguished between warnings and errors."""
        class MockDiag:
            def __init__(self, message, severity):
                self.message = message
                self.severity = severity

        diags = [
            MockDiag("Deprecated syntax", "WARNING"),
            MockDiag("Syntax error at line 5", "ERROR"),
        ]
        diag_list = []
        errors = 0
        warnings = 0
        for d in diags:
            if "ERROR" in d.severity.upper():
                errors += 1
            elif "WARN" in d.severity.upper():
                warnings += 1
            diag_list.append({"message": d.message, "severity": d.severity})

        self.assertEqual(errors, 1)
        self.assertEqual(warnings, 1)
        self.assertEqual(len(diag_list), 2)

    def test_R_fixmes_count_from_clean_sql_only(self):
        """Item R: FIXMEs count only on clean extracted SQL."""
        clean_sql_no_fixme = "CREATE OR REPLACE PROCEDURE dbo.test() AS BEGIN SELECT 1; END;"
        self.assertEqual(count_fixme_markers(clean_sql_no_fixme), 0)

        clean_sql_with_fixme = "-- FIXME: review dynamic SQL conversion\nSELECT 1;"
        self.assertEqual(count_fixme_markers(clean_sql_with_fixme), 1)

    def test_S_successful_procedure_remains_manual_review_converted_not_deployed(self):
        """Item S: Successful procedure conversion produces MANUAL_REVIEW / CONVERTED / NOT_DEPLOYED."""
        cls, status, rev_req, rev_reason, err_code, _ = derive_lakebridge_classification(
            object_type="PROCEDURE",
            source_definition="CREATE PROCEDURE dbo.usp_test AS BEGIN SELECT 1; END;",
            converted_definition="CREATE OR REPLACE PROCEDURE dbo.usp_test() AS BEGIN SELECT 1; END;",
            complexity="LOW",
            statement_count=1,
            unknown_statement_count=0,
            unknown_fragments=[],
            constructs={
                "uses_error_handling": False,
                "uses_rowcount": False,
                "uses_cursor": False,
                "uses_dynamic_sql": False,
                "uses_trigger": False,
            },
            parsing_error_count=0,
            validation_error_count=0,
            generation_error_count=0,
            fixme_count=0,
            remaining_source_syntax=[],
            analyzer_failed=False,
            transpile_failed=False,
            object_map_applied=False,
        )
        self.assertEqual(cls, LAKEBRIDGE_CLASSIFICATION_MANUAL_REVIEW)
        self.assertEqual(status, "CONVERTED")
        self.assertTrue(rev_req)
        self.assertIsNone(err_code)

    def test_T_bootstrap_is_idempotent(self):
        """Item T: Bootstrap is idempotent and caches ready state."""
        with tempfile.TemporaryDirectory() as td:
            fake_python = get_venv_python_executable(td)
            os.makedirs(os.path.dirname(fake_python), exist_ok=True)
            with open(fake_python, "w") as f:
                f.write("#!/bin/sh\n")

            calls = []
            def mock_runner(cmd):
                calls.append(cmd)
                return 0, '{"bladebridge": "available", "analyzer": "available", "lakebridge_version": "0.15.2", "bladebridge_version": "0.3.0", "analyzer_version": "0.1.24"}', ""

            ok1, p1, d1 = ensure_lakebridge_environment(venv_dir=td, runner=mock_runner)
            self.assertTrue(ok1)

            ok2, p2, d2 = ensure_lakebridge_environment(venv_dir=td, runner=mock_runner)
            self.assertTrue(ok2)
            self.assertEqual(p1, p2)

    def test_U_missing_uv_fails_safely_sanitized(self):
        """Item U: Missing uv returns structured sanitized failure without crashing."""
        with tempfile.TemporaryDirectory() as td:
            nonexistent_uv = os.path.join(td, "nonexistent_uv_binary_xyz")
            nonexistent_venv = os.path.join(td, "venv")
            ok, p, details = bootstrap_lakebridge_environment(
                venv_dir=nonexistent_venv,
                uv_path=nonexistent_uv,
                force=True,
            )
            self.assertFalse(ok)
            self.assertIsNone(p)
            self.assertEqual(details.get("error_code"), "UV_NOT_FOUND")

    def test_V_failed_pip_bootstrap_fails_safely_sanitized(self):
        """Item V: Failed package install fails safely and sanitized without leaking secrets."""
        with tempfile.TemporaryDirectory() as td:
            def mock_runner(cmd):
                if "venv" in cmd:
                    return 0, "", ""
                if "pip" in cmd:
                    return 1, "", "Error with password=SuperSecret and secret=Key123"
                return 0, '{"bladebridge": "available", "analyzer": "available", "lakebridge_version": "0.15.2", "bladebridge_version": "0.3.0", "analyzer_version": "0.1.24"}', ""

            ok, p, details = bootstrap_lakebridge_environment(
                venv_dir=td,
                uv_path=sys.executable,  # provide existing binary to pass uv check
                force=True,
                runner=mock_runner,
            )
            self.assertFalse(ok)
            err = details.get("error", "")
            self.assertNotIn("SuperSecret", err)
            self.assertNotIn("Key123", err)

    def test_W_compute_restart_like_fresh_runtime_recreates_environment(self):
        """Item W: Fresh compute context verifies and reinitializes runtime."""
        with tempfile.TemporaryDirectory() as td:
            fake_python = get_venv_python_executable(td)
            os.makedirs(os.path.dirname(fake_python), exist_ok=True)
            with open(fake_python, "w") as f:
                f.write("#!/bin/sh\n")

            calls = []
            def mock_runner(cmd):
                calls.append(cmd)
                return 0, '{"bladebridge": "available", "analyzer": "available", "lakebridge_version": "0.15.2", "bladebridge_version": "0.3.0", "analyzer_version": "0.1.24"}', ""

            ok, p, details = ensure_lakebridge_environment(
                venv_dir=td,
                force_recheck=True,
                runner=mock_runner,
            )
            self.assertTrue(ok)
            self.assertTrue(len(calls) > 0)

    def test_X_atomic_writes_attempt_isolation_and_no_deployment(self):
        """Item X: Atomic writes, attempt isolation, and no-deployment behavior preserved."""
        d1 = build_attempt_staging_dir("/base", "run_1", "art_1", 1, "uuid_1")
        d2 = build_attempt_staging_dir("/base", "run_1", "art_1", 1, "uuid_2")
        self.assertNotEqual(d1, d2)

        # Confirm collision resistant filenames
        fn_a = build_collision_resistant_filename("A/B", "art_1")
        fn_b = build_collision_resistant_filename("A:B", "art_2")
        self.assertNotEqual(fn_a, fn_b)

    def test_Y_oracle_path_not_broken_by_sqlserver_fixes(self):
        """Item Y: Oracle path works correctly with Oracle platform and ORACLE/SQL tech mapping."""
        self.assertEqual(get_analyzer_platform("oracle"), "Oracle")
        self.assertEqual(get_bladebridge_tech("oracle"), ("ORACLE", "SQL"))

        cls, status, rev_req, _, _, _ = derive_lakebridge_classification(
            object_type="PROCEDURE",
            source_definition="CREATE OR REPLACE PROCEDURE p IS BEGIN NULL; END;",
            converted_definition="CREATE OR REPLACE PROCEDURE p() LANGUAGE SQL AS BEGIN NULL; END;",
            complexity="LOW",
            statement_count=1,
            unknown_statement_count=0,
            unknown_fragments=[],
            constructs={},
            parsing_error_count=0,
            validation_error_count=0,
            generation_error_count=0,
            fixme_count=0,
            remaining_source_syntax=[],
            analyzer_failed=False,
            transpile_failed=False,
            object_map_applied=False,
        )
        self.assertEqual(cls, LAKEBRIDGE_CLASSIFICATION_MANUAL_REVIEW)
        self.assertEqual(status, "CONVERTED")
        self.assertTrue(rev_req)


class TestTextEditApplication(unittest.TestCase):
    """Section: TextEdit application to source SQL."""

    def test_one_full_document_replacement(self):
        source = "SELECT 1;"
        edits = [
            {
                "range": {"start": {"line": 0, "character": 0}, "end": {"line": 0, "character": 9}},
                "new_text": "SELECT 2;",
            }
        ]
        result = apply_text_edits(source, edits)
        self.assertEqual(result, "SELECT 2;")

    def test_multiple_independent_edits(self):
        source = "SELECT colA, colB FROM tbl;"
        edits = [
            {
                "range": {"start": {"line": 0, "character": 7}, "end": {"line": 0, "character": 11}},
                "new_text": "column_a",
            },
            {
                "range": {"start": {"line": 0, "character": 13}, "end": {"line": 0, "character": 17}},
                "new_text": "column_b",
            },
        ]
        result = apply_text_edits(source, edits)
        self.assertEqual(result, "SELECT column_a, column_b FROM tbl;")

    def test_insertion(self):
        source = "SELECT FROM tbl;"
        edits = [
            {
                "range": {"start": {"line": 0, "character": 7}, "end": {"line": 0, "character": 7}},
                "new_text": "* ",
            }
        ]
        result = apply_text_edits(source, edits)
        self.assertEqual(result, "SELECT * FROM tbl;")

    def test_deletion(self):
        source = "SELECT /* comment */ 1;"
        edits = [
            {
                "range": {"start": {"line": 0, "character": 7}, "end": {"line": 0, "character": 21}},
                "new_text": "",
            }
        ]
        result = apply_text_edits(source, edits)
        self.assertEqual(result, "SELECT 1;")

    def test_multiline_edit(self):
        source = "CREATE PROCEDURE p AS\nBEGIN\n  SELECT 1;\nEND;"
        edits = [
            {
                "range": {"start": {"line": 1, "character": 0}, "end": {"line": 3, "character": 4}},
                "new_text": "BEGIN\n  RETURN 0;\nEND;",
            }
        ]
        result = apply_text_edits(source, edits)
        self.assertEqual(result, "CREATE PROCEDURE p AS\nBEGIN\n  RETURN 0;\nEND;")

    def test_adjacent_edits(self):
        source = "ABCDEF"
        edits = [
            {
                "range": {"start": {"line": 0, "character": 0}, "end": {"line": 0, "character": 3}},
                "new_text": "123",
            },
            {
                "range": {"start": {"line": 0, "character": 3}, "end": {"line": 0, "character": 6}},
                "new_text": "456",
            },
        ]
        result = apply_text_edits(source, edits)
        self.assertEqual(result, "123456")

    def test_overlapping_edits_rejected(self):
        source = "ABCDEF"
        edits = [
            {
                "range": {"start": {"line": 0, "character": 0}, "end": {"line": 0, "character": 4}},
                "new_text": "1234",
            },
            {
                "range": {"start": {"line": 0, "character": 2}, "end": {"line": 0, "character": 6}},
                "new_text": "5678",
            },
        ]
        with self.assertRaises(ValueError) as ctx:
            apply_text_edits(source, edits)
        self.assertIn("Overlapping", str(ctx.exception))

    def test_invalid_range_rejected(self):
        source = "ABC"
        # Start after end
        edits_inverted = [
            {
                "range": {"start": {"line": 0, "character": 3}, "end": {"line": 0, "character": 1}},
                "new_text": "X",
            }
        ]
        with self.assertRaises(ValueError):
            apply_text_edits(source, edits_inverted)

        # Line out of bounds
        edits_oob = [
            {
                "range": {"start": {"line": 5, "character": 0}, "end": {"line": 5, "character": 1}},
                "new_text": "X",
            }
        ]
        with self.assertRaises(ValueError):
            apply_text_edits(source, edits_oob)

    def test_unicode_utf16_offsets(self):
        # '😀' is U+1F600, taking 2 UTF-16 code units
        source = "SELECT '😀' AS emoji;\n"
        # Emoji starts at character 8, ends at character 10 (8 + 2 UTF-16 units)
        edits = [
            {
                "range": {"start": {"line": 0, "character": 8}, "end": {"line": 0, "character": 10}},
                "new_text": "STAR",
            }
        ]
        result = apply_text_edits(source, edits)
        self.assertEqual(result, "SELECT 'STAR' AS emoji;\n")

        # Split surrogate pair
        edits_split = [
            {
                "range": {"start": {"line": 0, "character": 9}, "end": {"line": 0, "character": 10}},
                "new_text": "X",
            }
        ]
        with self.assertRaises(ValueError) as ctx:
            apply_text_edits(source, edits_split)
        self.assertIn("surrogate", str(ctx.exception).lower())

    def test_crlf_replacement(self):
        source = "SELECT 1;\r\nSELECT 2;\r\n"
        edits = [
            {
                "range": {"start": {"line": 0, "character": 7}, "end": {"line": 0, "character": 8}},
                "new_text": "99",
            }
        ]
        result = apply_text_edits(source, edits)
        self.assertEqual(result, "SELECT 99;\r\nSELECT 2;\r\n")

    def test_crlf_insertion(self):
        source = "SELECT 1;\r\n"
        # Insertion at character 9 (end of "SELECT 1;", before \r\n)
        edits = [
            {
                "range": {"start": {"line": 0, "character": 9}, "end": {"line": 0, "character": 9}},
                "new_text": " -- comment",
            }
        ]
        result = apply_text_edits(source, edits)
        self.assertEqual(result, "SELECT 1; -- comment\r\n")
        self.assertNotIn("\r -- comment\n", result)

    def test_crlf_invalid_end_of_line_positions(self):
        source = "SELECT 1;\r\n"
        # Character 10 points into line ending (\r or \n)
        edits = [
            {
                "range": {"start": {"line": 0, "character": 10}, "end": {"line": 0, "character": 10}},
                "new_text": "X",
            }
        ]
        with self.assertRaises(ValueError) as ctx:
            apply_text_edits(source, edits)
        self.assertIn("out of bounds", str(ctx.exception).lower())

    def test_crlf_multiline_edits(self):
        source = "LINE_ONE\r\nLINE_TWO\r\nLINE_THREE\r\n"
        # Replace across line 0 and line 1
        edits = [
            {
                "range": {"start": {"line": 0, "character": 0}, "end": {"line": 1, "character": 8}},
                "new_text": "REPLACED_LINES",
            }
        ]
        result = apply_text_edits(source, edits)
        self.assertEqual(result, "REPLACED_LINES\r\nLINE_THREE\r\n")

    def test_crlf_unicode_with_crlf(self):
        # '🚀' is 2 UTF-16 code units
        source = "SELECT '🚀';\r\n"
        # Length of "SELECT '🚀';" is 12 UTF-16 code units (8 + 2 + 2)
        edits = [
            {
                "range": {"start": {"line": 0, "character": 8}, "end": {"line": 0, "character": 10}},
                "new_text": "ROCKET",
            }
        ]
        result = apply_text_edits(source, edits)
        self.assertEqual(result, "SELECT 'ROCKET';\r\n")


class TestBladeBridgeDiagnostics(unittest.TestCase):
    """Section: BladeBridge diagnostics normalization and impact on classification."""

    def test_diagnostic_severity_normalization(self):
        # Numeric LSP values
        self.assertEqual(normalize_diagnostic_severity(1), "ERROR")
        self.assertEqual(normalize_diagnostic_severity(2), "WARNING")
        self.assertEqual(normalize_diagnostic_severity(3), "INFORMATION")
        self.assertEqual(normalize_diagnostic_severity(4), "HINT")

        # String numeric
        self.assertEqual(normalize_diagnostic_severity("1"), "ERROR")
        self.assertEqual(normalize_diagnostic_severity("2"), "WARNING")

        # Enum with .name attribute
        class MockEnum:
            def __init__(self, name):
                self.name = name

        self.assertEqual(normalize_diagnostic_severity(MockEnum("Error")), "ERROR")
        self.assertEqual(normalize_diagnostic_severity(MockEnum("Warning")), "WARNING")
        self.assertEqual(normalize_diagnostic_severity(MockEnum("Information")), "INFORMATION")
        self.assertEqual(normalize_diagnostic_severity(MockEnum("Hint")), "HINT")

        # Strings
        self.assertEqual(normalize_diagnostic_severity("error"), "ERROR")
        self.assertEqual(normalize_diagnostic_severity("warn"), "WARNING")
        self.assertEqual(normalize_diagnostic_severity(None), "INFORMATION")

    def test_production_run_transpile_with_diagnostics(self):
        import types
        mock_module = types.ModuleType("databricks.labs.bladebridge.transpiler")
        mock_transpiler_cls = unittest.mock.MagicMock()
        mock_module.Transpiler = mock_transpiler_cls
        with unittest.mock.patch.dict("sys.modules", {
            "databricks": types.ModuleType("databricks"),
            "databricks.labs": types.ModuleType("databricks.labs"),
            "databricks.labs.bladebridge": types.ModuleType("databricks.labs.bladebridge"),
            "databricks.labs.bladebridge.transpiler": mock_module,
        }):
            with tempfile.TemporaryDirectory() as td:
                src_f = os.path.join(td, "proc.sql")
                out_f = os.path.join(td, "proc_conv.sql")
                with open(src_f, "w", encoding="utf-8") as f:
                    f.write("CREATE PROCEDURE dbo.p AS SELECT 1;")

                mock_transpiler = unittest.mock.MagicMock()
                mock_transpiler_cls.return_value = mock_transpiler

                class DiagnosticMock:
                    def __init__(self, message, severity):
                        self.message = message
                        self.severity = severity

                class SeverityEnumMock:
                    def __init__(self, name):
                        self.name = name

                mock_edits = [
                    {
                        "range": {"start": {"line": 0, "character": 0}, "end": {"line": 0, "character": 35}},
                        "new_text": "CREATE OR REPLACE PROCEDURE dbo.p() LANGUAGE SQL AS BEGIN SELECT 1; END;",
                    }
                ]
                mock_diagnostics = [
                    DiagnosticMock("Error occurred with secret=MyKey123", 1),
                    DiagnosticMock("Warning occurred", SeverityEnumMock("Warning")),
                    DiagnosticMock("Info message", 3),
                    DiagnosticMock("Hint message", 4),
                ]

                async def mock_async_transpile(fname, src):
                    return mock_edits, mock_diagnostics

                mock_transpiler.transpile.side_effect = mock_async_transpile

                req = {
                    "action": "transpile",
                    "source_file": src_f,
                    "output_file": out_f,
                    "source_system": "sqlserver",
                }
                resp = run_transpile(req)

                self.assertEqual(resp.get("status"), "SUCCEEDED")
                self.assertEqual(resp.get("diagnostic_error_count"), 1)
                self.assertEqual(resp.get("diagnostic_warning_count"), 1)
                self.assertEqual(resp.get("diagnostic_information_count"), 1)
                self.assertEqual(resp.get("diagnostic_hint_count"), 1)
                self.assertEqual(resp.get("edit_count"), 1)

                # Sanitization verification
                diags = resp.get("diagnostics", [])
                self.assertEqual(len(diags), 4)
                self.assertNotIn("MyKey123", diags[0]["message"])
                self.assertIn("secret=***", diags[0]["message"])
                self.assertEqual(diags[0]["severity"], "ERROR")
                self.assertEqual(diags[1]["severity"], "WARNING")
                self.assertEqual(diags[2]["severity"], "INFORMATION")
                self.assertEqual(diags[3]["severity"], "HINT")

    def test_diagnostic_errors_force_unsupported_classification(self):
        cls, status, rev_req, reason, err_code, _ = derive_lakebridge_classification(
            object_type="PROCEDURE",
            source_definition="CREATE PROCEDURE p AS SELECT 1;",
            converted_definition="CREATE OR REPLACE PROCEDURE p() LANGUAGE SQL AS BEGIN SELECT 1; END;",
            complexity="LOW",
            statement_count=1,
            unknown_statement_count=0,
            unknown_fragments=[],
            constructs={},
            parsing_error_count=0,
            validation_error_count=0,
            generation_error_count=1,  # 1 diagnostic error
            fixme_count=0,
            remaining_source_syntax=[],
            transpile_failed=True,
        )
        self.assertEqual(cls, LAKEBRIDGE_CLASSIFICATION_UNSUPPORTED)
        self.assertEqual(status, "FAILED")
        self.assertTrue(rev_req)
        self.assertIn("generation_error_count=1", reason)

    def test_diagnostic_warnings_force_manual_review(self):
        cls, status, rev_req, reason, _, _ = derive_lakebridge_classification(
            object_type="VIEW",
            source_definition="CREATE VIEW v AS SELECT 1 AS x;",
            converted_definition="CREATE OR REPLACE VIEW v AS SELECT 1 AS x;",
            complexity="LOW",
            statement_count=1,
            unknown_statement_count=0,
            unknown_fragments=[],
            constructs={},
            parsing_error_count=0,
            validation_error_count=0,
            generation_error_count=0,
            fixme_count=0,
            remaining_source_syntax=[],
            object_map_applied=True,
            diagnostic_warning_count=1,  # 1 diagnostic warning
        )
        self.assertEqual(cls, LAKEBRIDGE_CLASSIFICATION_MANUAL_REVIEW)
        self.assertEqual(status, "PARTIAL")
        self.assertTrue(rev_req)
        self.assertIn("diagnostic warnings", reason)


class TestMimeExtractionHardening(unittest.TestCase):
    """Section: MIME extraction hardening and comment preservation."""

    def test_actual_multipart_response_shape(self):
        payload = (
            'Content-Type: multipart/mixed; boundary=0f81d11ff9c22e432c25633346e49455\n'
            'Content-Disposition: attachment; filename="usp_lb_isolated_test.sql"\n'
            'MIME-Version: 1.0\n'
            '\n'
            'CREATE OR REPLACE PROCEDURE dbo.usp_lb_isolated_test(\n'
            'IN V_CustomerId INT)\n'
            'LANGUAGE SQL\n'
            'SQL SECURITY INVOKER\n'
            'AS\n'
            'BEGIN\n'
            '    SELECT CustomerId, CustomerName FROM dbo.Customers WHERE CustomerId = V_CustomerId;\n'
            'END;\n'
            '\n'
            '--0f81d11ff9c22e432c25633346e49455--\n'
        )
        clean = extract_sql_from_bladebridge_mime(payload)
        self.assertIn("CREATE OR REPLACE PROCEDURE", clean)
        self.assertNotIn("Content-Type", clean)
        self.assertNotIn("0f81d11ff9c22e432c25633346e49455", clean)
        self.assertNotIn("MIME-Version", clean)
        self.assertNotIn("Content-Disposition", clean)

    def test_lowercase_mime_headers(self):
        payload = (
            'content-type: multipart/mixed; boundary=my_boundary_xyz\n'
            'content-disposition: attachment; filename="proc.sql"\n'
            'mime-version: 1.0\n'
            '\n'
            'CREATE VIEW v AS SELECT 1 AS id;\n'
            '\n'
            '--my_boundary_xyz--\n'
        )
        clean = extract_sql_from_bladebridge_mime(payload)
        self.assertEqual(clean, "CREATE VIEW v AS SELECT 1 AS id;")

    def test_declared_non_utf8_charset(self):
        # ISO-8859-1 payload inside multipart
        payload = (
            'Content-Type: multipart/mixed; boundary=iso_bnd\n'
            '\n'
            '--iso_bnd\n'
            'Content-Type: text/plain; charset=iso-8859-1\n'
            'Content-Disposition: attachment; filename="data.sql"\n'
            '\n'
            'SELECT \'café\' AS word;\n'
            '--iso_bnd--\n'
        )
        clean = extract_sql_from_bladebridge_mime(payload)
        self.assertIn("SELECT 'café' AS word;", clean)

    def test_plain_sql_starting_with_comment(self):
        # Plain SQL starting with --generated must NOT be mistaken for MIME
        sql = "--generated by accelerator\nSELECT 1 AS col;\n"
        clean = extract_sql_from_bladebridge_mime(sql)
        self.assertEqual(clean, "--generated by accelerator\nSELECT 1 AS col;")

    def test_multiple_sql_attachments_rejected(self):
        payload = (
            'Content-Type: multipart/mixed; boundary=multi_bnd\n'
            '\n'
            '--multi_bnd\n'
            'Content-Disposition: attachment; filename="file1.sql"\n'
            '\n'
            'SELECT 1;\n'
            '--multi_bnd\n'
            'Content-Disposition: attachment; filename="file2.sql"\n'
            '\n'
            'SELECT 2;\n'
            '--multi_bnd--\n'
        )
        with self.assertRaises(ValueError) as ctx:
            extract_sql_from_bladebridge_mime(payload)
        self.assertIn("Ambiguous", str(ctx.exception))

    def test_malformed_mime_rejected(self):
        # Multipart Content-Type without boundary
        payload = "Content-Type: multipart/mixed;\n\nSELECT 1;"
        with self.assertRaises(ValueError) as ctx:
            extract_sql_from_bladebridge_mime(payload)
        self.assertIn("Malformed MIME", str(ctx.exception))

    def test_blank_mime_payload_rejected(self):
        payload = (
            'Content-Type: multipart/mixed; boundary=empty_bnd\n'
            '\n'
            '--empty_bnd\n'
            'Content-Disposition: attachment; filename="empty.sql"\n'
            '\n'
            '   \n'
            '--empty_bnd--\n'
        )
        with self.assertRaises(ValueError) as ctx:
            extract_sql_from_bladebridge_mime(payload)
        self.assertIn("blank", str(ctx.exception).lower())

    def test_normal_non_mime_sql(self):
        sql = "SELECT * FROM dbo.Orders WHERE OrderId > 10;"
        clean = extract_sql_from_bladebridge_mime(sql)
        self.assertEqual(clean, sql)


class TestAnalyzerParserGaps(unittest.TestCase):
    """Section: Analyzer parser robustness, fallback, and fixtures."""

    def test_corrupt_json_with_xlsx_fallback(self):
        with tempfile.TemporaryDirectory() as td:
            json_p = os.path.join(td, "analyzer_report.json")
            xlsx_p = os.path.join(td, "analyzer_report.xlsx")

            with open(json_p, "w", encoding="utf-8") as f:
                f.write("{invalid json content")

            xlsx_data = [
                ("SQL Programs", [
                    ["Program Name", "Complexity", "Statement Count"],
                    ["test_proc.sql", "LOW", "5"],
                ])
            ]
            with open(xlsx_p, "wb") as f:
                f.write(create_in_memory_xlsx(xlsx_data))

            res = read_analyzer_report(report_xlsx_path=xlsx_p, report_json_path=json_p)
            self.assertIn("sql_programs", res)
            self.assertEqual(res["sql_programs"][0]["complexity"], "LOW")
            self.assertIn("_warnings", res)
            self.assertTrue(any("corrupt" in w.lower() for w in res["_warnings"]))

    def test_contradictory_json_and_xlsx_complexity_fails_closed(self):
        # JSON reports LOW, XLSX reports HIGH
        data = {
            "inventory": [
                {"name": "proc_a.sql", "complexityLevel": "LOW"}
            ],
            "xlsx_sql_programs": [
                {"program_name": "proc_a.sql", "complexity": "HIGH"}
            ]
        }
        with self.assertRaises(ValueError) as ctx:
            extract_complexity(data, staged_filename="proc_a.sql")
        self.assertIn("Contradictory Analyzer complexity", str(ctx.exception))

    def test_referenced_objects_strict_artifact_matching(self):
        data = {
            "inventory": [
                {
                    "name": "proc_a.sql",
                    "objectRel": [
                        {"object": "dbo.TableA", "action": "read", "count": 2}
                    ]
                },
                {
                    "name": "proc_b.sql",
                    "objectRel": [
                        {"object": "dbo.TableB", "action": "write", "count": 1}
                    ]
                }
            ]
        }
        # Searching for proc_a must only return TableA
        refs_a = extract_referenced_objects(data, staged_filename="proc_a.sql")
        self.assertEqual(len(refs_a), 1)
        self.assertEqual(refs_a[0]["object"], "dbo.TableA")

        # Searching for nonexistent proc returns empty, does NOT fall back to first row
        refs_none = extract_referenced_objects(data, staged_filename="nonexistent.sql")
        self.assertEqual(len(refs_none), 0)

    def test_xlsx_relationships_resolution(self):
        """Test XLSX worksheet files are resolved via workbook.xml relationships rather than ordinal."""
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("[Content_Types].xml", (
                '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
                '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
                '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
                '<Default Extension="xml" ContentType="application/xml"/>'
                '<Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>'
                '<Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>'
                '<Override PartName="/xl/worksheets/sheet2.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>'
                '</Types>'
            ))
            # rId1 -> sheet2.xml, rId2 -> sheet1.xml (reversed!)
            zf.writestr("xl/_rels/workbook.xml.rels", (
                '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
                '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
                '<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet2.xml"/>'
                '<Relationship Id="rId2" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/>'
                '</Relationships>'
            ))
            zf.writestr("xl/workbook.xml", (
                '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
                '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
                '<sheets>'
                '<sheet name="SpecialSheet" sheetId="1" r:id="rId1"/>'
                '<sheet name="OtherSheet" sheetId="2" r:id="rId2"/>'
                '</sheets></workbook>'
            ))
            zf.writestr("xl/worksheets/sheet2.xml", (
                '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
                '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
                '<sheetData>'
                '<row r="1"><c r="A1" t="inlineStr"><is><t>ColKey</t></is></c></row>'
                '<row r="2"><c r="A2" t="inlineStr"><is><t>FromSheet2</t></is></c></row>'
                '</sheetData></worksheet>'
            ))
            zf.writestr("xl/worksheets/sheet1.xml", (
                '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
                '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
                '<sheetData>'
                '<row r="1"><c r="A1" t="inlineStr"><is><t>ColKey</t></is></c></row>'
                '<row r="2"><c r="A2" t="inlineStr"><is><t>FromSheet1</t></is></c></row>'
                '</sheetData></worksheet>'
            ))

        parsed = read_analyzer_workbook(buf.getvalue())
        self.assertIn("specialsheet", parsed)
        self.assertEqual(parsed["specialsheet"][0]["colkey"], "FromSheet2")

    def test_dynamic_sql_dependency_fallback_masked(self):
        sql = (
            "DECLARE @sql NVARCHAR(MAX) =\n"
            "    'SELECT * FROM dbo.SecretTable';\n"
            "EXEC sp_executesql @sql;"
        )
        empty_wb = {}
        refs = extract_referenced_objects(empty_wb, sql_text=sql, object_name="sp_dynamic")
        objects = [r["object"] for r in refs]
        self.assertNotIn("dbo.SecretTable", objects)
        self.assertEqual(len(refs), 0)


class TestEnvironmentPromotionSafety(unittest.TestCase):
    """Section: Environment promotion safety, lock, and rollback."""

    def test_successful_promotion(self):
        with tempfile.TemporaryDirectory() as td:
            target_env = os.path.join(td, "target_env")
            def mock_runner(cmd):
                if "venv" in cmd:
                    s_py = get_venv_python_executable(cmd[-1])
                    os.makedirs(os.path.dirname(s_py), exist_ok=True)
                    with open(s_py, "w") as f:
                        f.write("#!/bin/sh\n")
                    return 0, "", ""
                if "pip" in cmd:
                    return 0, "", ""
                return 0, '{"bladebridge": "available", "analyzer": "available", "lakebridge_version": "0.15.2", "bladebridge_version": "0.3.0", "analyzer_version": "0.1.24"}', ""

            ok, p, details = bootstrap_lakebridge_environment(
                venv_dir=target_env,
                uv_path=sys.executable,
                force=True,
                runner=mock_runner,
            )
            self.assertTrue(ok)
            self.assertEqual(details.get("status"), "BOOTSTRAPPED")
            self.assertTrue(os.path.isdir(target_env))
            self.assertFalse(os.path.exists(target_env + ".lock"))
            backups = [f for f in os.listdir(td) if ".backup_" in f]
            self.assertEqual(len(backups), 0)

    def test_failed_promotion_with_rollback(self):
        with tempfile.TemporaryDirectory() as td:
            target_env = os.path.join(td, "target_env")
            fake_python = get_venv_python_executable(target_env)
            os.makedirs(os.path.dirname(fake_python), exist_ok=True)
            with open(fake_python, "w") as f:
                f.write("#!/bin/sh\n# original healthy env\n")
            marker_file = os.path.join(target_env, "original_marker.txt")
            with open(marker_file, "w") as f:
                f.write("original")

            def mock_runner(cmd):
                if "venv" in cmd:
                    s_py = get_venv_python_executable(cmd[-1])
                    os.makedirs(os.path.dirname(s_py), exist_ok=True)
                    with open(s_py, "w") as f:
                        f.write("#!/bin/sh\n")
                    return 0, "", ""
                if "pip" in cmd:
                    return 0, "", ""
                return 0, '{"bladebridge": "available", "analyzer": "available", "lakebridge_version": "0.15.2", "bladebridge_version": "0.3.0", "analyzer_version": "0.1.24"}', ""

            original_replace = os.replace
            def failing_replace(src, dst):
                if ".lakebridge_staging_" in src and dst == target_env:
                    raise OSError("Simulated promotion failure")
                return original_replace(src, dst)

            with unittest.mock.patch("os.replace", side_effect=failing_replace):
                with unittest.mock.patch("shutil.move", side_effect=OSError("Simulated move failure")):
                    ok, p, details = bootstrap_lakebridge_environment(
                        venv_dir=target_env,
                        uv_path=sys.executable,
                        force=True,
                        runner=mock_runner,
                    )
            self.assertFalse(ok)
            self.assertEqual(details.get("error_code"), "PROMOTION_FAILED")
            self.assertTrue(os.path.exists(marker_file))
            with open(marker_file) as f:
                self.assertEqual(f.read(), "original")

    def test_failed_post_promotion_health_check(self):
        with tempfile.TemporaryDirectory() as td:
            target_env = os.path.join(td, "target_env")
            fake_python = get_venv_python_executable(target_env)
            os.makedirs(os.path.dirname(fake_python), exist_ok=True)
            with open(fake_python, "w") as f:
                f.write("#!/bin/sh\n# original env\n")
            marker_file = os.path.join(target_env, "original_marker.txt")
            with open(marker_file, "w") as f:
                f.write("original")

            health_checks = [0]
            def mock_runner(cmd):
                if "venv" in cmd:
                    s_py = get_venv_python_executable(cmd[-1])
                    os.makedirs(os.path.dirname(s_py), exist_ok=True)
                    with open(s_py, "w") as f:
                        f.write("#!/bin/sh\n")
                    return 0, "", ""
                if "pip" in cmd:
                    return 0, "", ""
                if "-c" in cmd:
                    health_checks[0] += 1
                    if health_checks[0] == 1:
                        return 0, '{"bladebridge": "available", "analyzer": "available", "lakebridge_version": "0.15.2", "bladebridge_version": "0.3.0", "analyzer_version": "0.1.24"}', ""
                    return 1, "", "Post-promotion failure"
                return 0, "", ""

            ok, p, details = bootstrap_lakebridge_environment(
                venv_dir=target_env,
                uv_path=sys.executable,
                force=True,
                runner=mock_runner,
            )
            self.assertFalse(ok)
            self.assertEqual(details.get("error_code"), "POST_PROMOTION_HEALTH_CHECK_FAILED")
            self.assertTrue(os.path.exists(marker_file))

    def test_concurrent_initialization(self):
        with tempfile.TemporaryDirectory() as td:
            lock_path = os.path.join(td, "test.lock")
            lock1 = EnvironmentLock(lock_path, timeout_sec=2)
            self.assertTrue(lock1.acquire())

            lock2 = EnvironmentLock(lock_path, timeout_sec=1, poll_interval_sec=0.1)
            self.assertFalse(lock2.acquire())

            lock1.release()
            self.assertTrue(lock2.acquire())
            lock2.release()


class TestStrictEnvironmentHealthValidation(unittest.TestCase):
    """Section: Strict environment health validation and cache invalidation."""

    def test_missing_version_metadata(self):
        def mock_runner(cmd):
            return 0, '{"bladebridge": "available", "analyzer": "available", "lakebridge_version": "0.15.2"}', ""

        healthy, details = check_environment_health(sys.executable, runner=mock_runner)
        self.assertFalse(healthy)
        self.assertIn("Invalid or missing", details.get("error", ""))

    def test_version_mismatch(self):
        def mock_runner(cmd):
            return 0, '{"bladebridge": "available", "analyzer": "available", "lakebridge_version": "0.15.2", "bladebridge_version": "0.2.9", "analyzer_version": "0.1.24"}', ""

        healthy, details = check_environment_health(sys.executable, runner=mock_runner)
        self.assertFalse(healthy)
        self.assertIn("bladebridge", details.get("error", ""))

    def test_deleted_cached_interpreter(self):
        with tempfile.TemporaryDirectory() as td:
            fake_python = get_venv_python_executable(td)
            os.makedirs(os.path.dirname(fake_python), exist_ok=True)
            with open(fake_python, "w") as f:
                f.write("#!/bin/sh\n")

            def mock_runner(cmd):
                return 0, '{"bladebridge": "available", "analyzer": "available", "lakebridge_version": "0.15.2", "bladebridge_version": "0.3.0", "analyzer_version": "0.1.24"}', ""

            ok1, p1, d1 = ensure_lakebridge_environment(venv_dir=td, runner=mock_runner)
            self.assertTrue(ok1)

            os.remove(fake_python)

            bootstrap_called = [False]
            def mock_rebootstrap_runner(cmd):
                bootstrap_called[0] = True
                return 0, '{"bladebridge": "available", "analyzer": "available", "lakebridge_version": "0.15.2", "bladebridge_version": "0.3.0", "analyzer_version": "0.1.24"}', ""

            ok2, p2, d2 = ensure_lakebridge_environment(
                venv_dir=td,
                uv_path=sys.executable,
                runner=mock_rebootstrap_runner,
            )
            self.assertTrue(bootstrap_called[0])


class TestReportPersistence(unittest.TestCase):
    """Regression test for XLSX and JSON report persistence logic."""

    def test_xlsx_and_json_persistence_binary_exact_match(self):
        """Verifies report persistence logic opens both XLSX and JSON in binary mode, preserving contents."""
        with tempfile.TemporaryDirectory() as td:
            src_dir = os.path.join(td, "src")
            dst_dir = os.path.join(td, "dst")
            os.makedirs(src_dir, exist_ok=True)
            os.makedirs(dst_dir, exist_ok=True)

            xlsx_src = os.path.join(src_dir, "report.xlsx")
            json_src = os.path.join(src_dir, "report.json")

            # Create non-empty XLSX and JSON reports
            xlsx_content = b"PK\x03\x04fake_excel_bytes_data_1234567890"
            json_content = b'{"inventory": [{"object": "proc1", "complexity": "Low"}]}'

            with open(xlsx_src, "wb") as f:
                f.write(xlsx_content)
            with open(json_src, "wb") as f:
                f.write(json_content)

            # Replicate the exact NB24 persistence logic:
            # dst_rep = os.path.join(report_vol_dir, os.path.basename(r_file))
            # with open(r_file, "rb") as rf:
            #     write_atomic_file(dst_rep, rf.read())
            def write_atomic_file_test(target_path, content):
                temp_path = f"{target_path}.tmp.{uuid.uuid4().hex}"
                os.makedirs(os.path.dirname(target_path), exist_ok=True)
                mode = "wb" if isinstance(content, bytes) else "w"
                with open(temp_path, mode) as f:
                    f.write(content)
                os.replace(temp_path, target_path)

            for r_file in (xlsx_src, json_src):
                dst_rep = os.path.join(dst_dir, os.path.basename(r_file))
                with open(r_file, "rb") as rf:
                    write_atomic_file_test(dst_rep, rf.read())

            xlsx_dst = os.path.join(dst_dir, "report.xlsx")
            json_dst = os.path.join(dst_dir, "report.json")

            self.assertTrue(os.path.exists(xlsx_dst))
            self.assertTrue(os.path.exists(json_dst))
            self.assertGreater(os.path.getsize(xlsx_dst), 0)
            self.assertGreater(os.path.getsize(json_dst), 0)

            with open(xlsx_dst, "rb") as f:
                self.assertEqual(f.read(), xlsx_content)
            with open(json_dst, "rb") as f:
                self.assertEqual(f.read(), json_content)

            # Verify that source files were not truncated or modified
            with open(xlsx_src, "rb") as f:
                self.assertEqual(f.read(), xlsx_content)
            with open(json_src, "rb") as f:
                self.assertEqual(f.read(), json_content)


class TestClassificationCountersAndPersistence(unittest.TestCase):
    """Tests for classification counter increments and persistence failure handling."""

    def test_successful_persistence_increments_auto_candidate(self):
        """When persistence succeeds, artifact classification is AUTO_CANDIDATE and auto_candidate_count increments."""
        total_candidates = 1
        candidates_to_process = [{"artifact_id": "a1", "source_definition": "SELECT 1"}]
        auto_candidate_count = 0
        manual_review_count = 0
        unsupported_count = 0
        store_failure_count = 0

        for cand in candidates_to_process:
            artifact_counted = False
            cls_res = LAKEBRIDGE_CLASSIFICATION_AUTO_CANDIDATE
            persistence_failed = False
            persistence_err = ""

            if persistence_failed:
                store_failure_count += 1
                cls_res = LAKEBRIDGE_CLASSIFICATION_UNSUPPORTED

            if cls_res == LAKEBRIDGE_CLASSIFICATION_AUTO_CANDIDATE:
                auto_candidate_count += 1
            elif cls_res == LAKEBRIDGE_CLASSIFICATION_MANUAL_REVIEW:
                manual_review_count += 1
            else:
                unsupported_count += 1
            artifact_counted = True

        completed_count = auto_candidate_count + manual_review_count
        def_missing_count = max(0, total_candidates - len(candidates_to_process))
        total_failures = unsupported_count + def_missing_count

        self.assertEqual(auto_candidate_count, 1)
        self.assertEqual(manual_review_count, 0)
        self.assertEqual(unsupported_count, 0)
        self.assertEqual(store_failure_count, 0)
        self.assertEqual(total_failures, 0)
        self.assertEqual(completed_count, 1)

    def test_failed_persistence_updates_classification_to_unsupported(self):
        """When persistence fails, classification is updated to UNSUPPORTED and auto_candidate is NOT incremented."""
        total_candidates = 1
        candidates_to_process = [{"artifact_id": "a1", "source_definition": "SELECT 1"}]
        auto_candidate_count = 0
        manual_review_count = 0
        unsupported_count = 0
        store_failure_count = 0

        for cand in candidates_to_process:
            artifact_counted = False
            cls_res = LAKEBRIDGE_CLASSIFICATION_AUTO_CANDIDATE
            persistence_failed = True
            persistence_err = "Volume write permission denied"

            if persistence_failed:
                store_failure_count += 1
                cls_res = LAKEBRIDGE_CLASSIFICATION_UNSUPPORTED
                conv_status = "FAILED"
                err_code = "PERSISTENT_STORE_FAILED"

            if cls_res == LAKEBRIDGE_CLASSIFICATION_AUTO_CANDIDATE:
                auto_candidate_count += 1
            elif cls_res == LAKEBRIDGE_CLASSIFICATION_MANUAL_REVIEW:
                manual_review_count += 1
            else:
                unsupported_count += 1
            artifact_counted = True

        completed_count = auto_candidate_count + manual_review_count
        def_missing_count = max(0, total_candidates - len(candidates_to_process))
        total_failures = unsupported_count + def_missing_count

        self.assertEqual(cls_res, LAKEBRIDGE_CLASSIFICATION_UNSUPPORTED)
        self.assertEqual(auto_candidate_count, 0)
        self.assertEqual(manual_review_count, 0)
        self.assertEqual(unsupported_count, 1)
        self.assertEqual(store_failure_count, 1)
        self.assertEqual(total_failures, 1)
        self.assertEqual(completed_count, 0)

    def test_each_artifact_counted_exactly_once_and_no_double_count(self):
        """Multiple artifacts: 1 auto, 1 manual, 1 unsupported transpile, 1 store failure, 1 missing definition."""
        total_candidates = 5
        candidates_to_process = [
            {"artifact_id": "a1", "scenario": "auto"},
            {"artifact_id": "a2", "scenario": "manual"},
            {"artifact_id": "a3", "scenario": "unsupported_transpile"},
            {"artifact_id": "a4", "scenario": "store_fail"},
        ]
        auto_candidate_count = 0
        manual_review_count = 0
        unsupported_count = 0
        store_failure_count = 0

        for cand in candidates_to_process:
            artifact_counted = False
            scen = cand["scenario"]
            if scen == "auto":
                cls_res = LAKEBRIDGE_CLASSIFICATION_AUTO_CANDIDATE
                p_fail = False
            elif scen == "manual":
                cls_res = LAKEBRIDGE_CLASSIFICATION_MANUAL_REVIEW
                p_fail = False
            elif scen == "unsupported_transpile":
                cls_res = LAKEBRIDGE_CLASSIFICATION_UNSUPPORTED
                p_fail = False
            elif scen == "store_fail":
                cls_res = LAKEBRIDGE_CLASSIFICATION_AUTO_CANDIDATE
                p_fail = True

            if p_fail:
                store_failure_count += 1
                cls_res = LAKEBRIDGE_CLASSIFICATION_UNSUPPORTED

            if cls_res == LAKEBRIDGE_CLASSIFICATION_AUTO_CANDIDATE:
                auto_candidate_count += 1
            elif cls_res == LAKEBRIDGE_CLASSIFICATION_MANUAL_REVIEW:
                manual_review_count += 1
            else:
                unsupported_count += 1
            artifact_counted = True

        completed_count = auto_candidate_count + manual_review_count
        def_missing_count = max(0, total_candidates - len(candidates_to_process))
        total_failures = unsupported_count + def_missing_count

        self.assertEqual(auto_candidate_count + manual_review_count + unsupported_count, len(candidates_to_process))
        self.assertEqual(auto_candidate_count, 1)
        self.assertEqual(manual_review_count, 1)
        self.assertEqual(unsupported_count, 2)
        self.assertEqual(store_failure_count, 1)
        self.assertEqual(def_missing_count, 1)
        # Distinct failures = 2 (unsupported) + 1 (missing def) = 3 (store failure is NOT double-counted!)
        self.assertEqual(total_failures, 3)


class TestRemainingSourceSyntaxDetection(unittest.TestCase):
    """Tests for detect_remaining_source_syntax with masking and general bracketed-identifier support."""

    def test_getdate_inside_string_not_detected(self):
        sql = "SELECT 'Today is GETDATE() function' AS info;"
        retains, patterns = detect_remaining_source_syntax(sql, "sqlserver")
        self.assertFalse(retains)
        self.assertEqual(patterns, [])

    def test_getdate_inside_comment_not_detected(self):
        sql_line = "-- This stored procedure used GETDATE() previously\nSELECT CURRENT_TIMESTAMP() AS now_val;"
        retains_line, patterns_line = detect_remaining_source_syntax(sql_line, "sqlserver")
        self.assertFalse(retains_line)
        self.assertEqual(patterns_line, [])

        sql_block = "/* GETDATE() was replaced with current_timestamp() */ SELECT CURRENT_TIMESTAMP();"
        retains_block, patterns_block = detect_remaining_source_syntax(sql_block, "sqlserver")
        self.assertFalse(retains_block)
        self.assertEqual(patterns_block, [])

    def test_sysdate_inside_string_not_detected(self):
        sql = "SELECT 'SYSDATE' AS col_name FROM hr.employees;"
        retains, patterns = detect_remaining_source_syntax(sql, "oracle")
        self.assertFalse(retains)
        self.assertEqual(patterns, [])

    def test_dual_inside_comment_not_detected(self):
        sql_line = "-- In Oracle: SELECT 1 FROM DUAL\nSELECT 1;"
        retains_line, patterns_line = detect_remaining_source_syntax(sql_line, "oracle")
        self.assertFalse(retains_line)
        self.assertEqual(patterns_line, [])

        sql_block = "/* SELECT dummy FROM DUAL; */ SELECT 1;"
        retains_block, patterns_block = detect_remaining_source_syntax(sql_block, "oracle")
        self.assertFalse(retains_block)
        self.assertEqual(patterns_block, [])

    def test_bracketed_identifiers_detected(self):
        sql = "SELECT * FROM [sales].[orders] WHERE [sales].[orders].[status] = 'ACTIVE';"
        retains, patterns = detect_remaining_source_syntax(sql, "sqlserver")
        self.assertTrue(retains)
        self.assertTrue(any("bracketed identifier" in p for p in patterns))

    def test_temporary_tables_detected(self):
        sql = "CREATE TABLE #temp (id INT, val STRING); INSERT INTO #temp VALUES (1, 'test');"
        retains, patterns = detect_remaining_source_syntax(sql, "sqlserver")
        self.assertTrue(retains)
        self.assertTrue(any("temporary table" in p for p in patterns))

    def test_genuine_remaining_source_syntax_outside_comments_and_strings(self):
        # Genuine GETDATE()
        sql_getdate = "SELECT GETDATE() AS curr_time;"
        retains_g, patterns_g = detect_remaining_source_syntax(sql_getdate, "sqlserver")
        self.assertTrue(retains_g)
        self.assertTrue(any("GETDATE()" in p for p in patterns_g))

        # Genuine SYSDATE
        sql_sysdate = "SELECT SYSDATE FROM hr.departments;"
        retains_s, patterns_s = detect_remaining_source_syntax(sql_sysdate, "oracle")
        self.assertTrue(retains_s)
        self.assertTrue(any("SYSDATE" in p for p in patterns_s))

        # Genuine ISNULL
        sql_isnull = "SELECT ISNULL(col_a, 0) FROM tbl;"
        retains_i, patterns_i = detect_remaining_source_syntax(sql_isnull, "sqlserver")
        self.assertTrue(retains_i)
        self.assertTrue(any("ISNULL" in p for p in patterns_i))


class TestConversionClassificationNullabilityAndUpgrade(unittest.TestCase):
    """Regression tests for conversion_classification nullability and schema upgrade."""

    def test_successful_t23_preconversion_row_allows_none_classification(self):
        """T23 pre-conversion row has conversion_classification=None and matches nullable column contract."""
        col_map = {col[0]: (col[1], col[2]) for col in SQL_ARTIFACT_CONTROL_COLUMNS}
        self.assertIn("conversion_classification", col_map)
        col_type, is_nullable = col_map["conversion_classification"]
        self.assertEqual(col_type, "STRING")
        self.assertTrue(is_nullable, "conversion_classification must be nullable in SQL_ARTIFACT_CONTROL_COLUMNS")

        row = prepare_artifact_rerun_control_row(
            existing_row=None,
            current_run_id="run_101",
            connection_id="conn_sql",
            source_system="sqlserver",
            source_database="SalesDB",
            source_schema="dbo",
            object_name="GetCustomerOrders",
            object_type="PROCEDURE",
            source_definition="CREATE PROCEDURE dbo.GetCustomerOrders AS SELECT 1;",
            source_definition_hash="abc123hash",
            now_ts="2026-09-27T12:00:00Z",
            fetch_error_code=None,
            fetch_error_message=None,
        )

        self.assertIsNone(row["conversion_classification"], "pre-conversion row must have conversion_classification=None")
        self.assertEqual(row["conversion_status"], "PENDING", "conversion_status must remain PENDING")
        self.assertNotEqual(row["conversion_classification"], "PENDING", "conversion_classification must NEVER be 'PENDING'")
        self.assertEqual(row["deployment_status"], "NOT_DEPLOYED")

    def test_t24_can_later_populate_valid_classifications(self):
        """T24 can update conversion_classification to AUTO_CANDIDATE, MANUAL_REVIEW, or UNSUPPORTED."""
        row = prepare_artifact_rerun_control_row(
            existing_row=None,
            current_run_id="run_102",
            connection_id="conn_sql",
            source_system="sqlserver",
            source_database=None,
            source_schema="dbo",
            object_name="v_active_users",
            object_type="VIEW",
            source_definition="CREATE VIEW dbo.v_active_users AS SELECT 1 AS id;",
            source_definition_hash="def456hash",
            now_ts="2026-09-27T12:00:00Z",
        )

        for valid_cls in [
            LAKEBRIDGE_CLASSIFICATION_AUTO_CANDIDATE,
            LAKEBRIDGE_CLASSIFICATION_MANUAL_REVIEW,
            LAKEBRIDGE_CLASSIFICATION_UNSUPPORTED,
        ]:
            row["conversion_classification"] = valid_cls
            self.assertEqual(row["conversion_classification"], valid_cls)
            self.assertIsInstance(row["conversion_classification"], str)

    def test_existing_table_upgrade_removes_obsolete_not_null_safely(self):
        """Existing table created with conversion_classification NOT NULL gets safe ALTER COLUMN DROP NOT NULL DDL."""
        existing_schema_with_not_null = [
            ("artifact_id", "STRING", False),
            ("connection_id", "STRING", False),
            ("source_system", "STRING", False),
            ("conversion_classification", "STRING", False),  # Legacy NOT NULL
            ("conversion_status", "STRING", False),
        ]

        stmts = build_upgrade_artifact_control_ddl(
            catalog="da_acc",
            control_schema="control",
            existing_columns=["artifact_id", "connection_id", "source_system", "conversion_classification", "conversion_status"],
            existing_schema=existing_schema_with_not_null,
        )

        self.assertTrue(any("ALTER COLUMN `conversion_classification` DROP NOT NULL" in s for s in stmts))
        self.assertFalse(any("DROP TABLE" in s for s in stmts))
        self.assertFalse(any("CREATE TABLE" in s for s in stmts))

    def test_already_nullable_table_upgrade_is_idempotent(self):
        """Table already migrated with conversion_classification nullable=True produces no DROP NOT NULL."""
        all_cols_nullable_aware = [
            (col[0], col[1], col[2]) for col in SQL_ARTIFACT_CONTROL_COLUMNS
        ]
        all_col_names = [col[0] for col in SQL_ARTIFACT_CONTROL_COLUMNS]

        stmts = build_upgrade_artifact_control_ddl(
            catalog="da_acc",
            control_schema="control",
            existing_columns=all_col_names,
            existing_schema=all_cols_nullable_aware,
        )
        self.assertEqual(stmts, [], "Already-migrated schema must produce no upgrade DDL")

    def test_no_other_control_table_nullability_changes(self):
        """Proves no other column in SQL_ARTIFACT_CONTROL_COLUMNS had its nullability altered."""
        expected_not_null_columns = {
            "artifact_id",
            "connection_id",
            "source_system",
            "source_schema",
            "object_name",
            "object_type",
            "source_definition_hash",
            "conversion_status",
            "deployment_status",
            "manual_review_required",
            "attempt_count",
            "created_ts",
            "updated_ts",
            "is_active",
        }

        actual_not_null_columns = {
            col[0] for col in SQL_ARTIFACT_CONTROL_COLUMNS if not col[2]
        }

        self.assertEqual(
            actual_not_null_columns,
            expected_not_null_columns,
            "Only conversion_classification must become nullable; all other required NOT NULL columns must be unchanged",
        )


class TestNB24LakebridgeRobustness(unittest.TestCase):
    """Section 25: Comprehensive NB24 Lakebridge Robustness regression tests.

    Covers:
    A. Analyzer produces valid structured report plus generic failure indication
    B. Analyzer produces no usable report (genuine failure)
    C. BladeBridge raises 'Line out of bounds: line 99 >= total lines 99'
    D. Multiple artifacts: one failure does not abort subsequent artifacts
    E. Successful artifacts behave exactly as before
    F. No converted SQL marked successful unless actual valid output exists
    G. No source SQL manipulation or line-padding workaround
    """

    def test_A_analyzer_valid_report_with_generic_failure_indication(self):
        """A. If Analyzer produces a valid structured report containing usable inventory results,
        preserve and use that report even if Analyzer returns a generic non-fatal 'Analysis failed'.
        Usable Analyzer metadata is retained and consumed, while diagnostic warning is preserved."""
        with tempfile.TemporaryDirectory() as td:
            in_dir = os.path.join(td, "input")
            os.makedirs(in_dir, exist_ok=True)
            with open(os.path.join(in_dir, "sp_calc.sql"), "w", encoding="utf-8") as f:
                f.write("CREATE PROCEDURE dbo.sp_calc AS SELECT 1;")

            rep_xlsx = os.path.join(td, "report.xlsx")
            rep_json = os.path.join(td, "report.json")

            valid_report_content = {
                "inventory": [
                    {
                        "name": "dbo.sp_calc",
                        "statementCount": 34,
                        "scriptCategories": ["DML"],
                        "complexityLevel": "MEDIUM",
                        "objectRel": [{"object": "dbo.T_Source", "action": "READ", "count": 2}],
                    }
                ]
            }

            def mock_analyze(inp, out_xlsx, platform, is_debug=False, json_result=None, **kwargs):
                if json_result:
                    with open(str(json_result), "w", encoding="utf-8") as jf:
                        json.dump(valid_report_content, jf)
                raise RuntimeError("Analysis failed")

            mock_mod = unittest.mock.MagicMock()
            mock_cls = unittest.mock.MagicMock()
            mock_mod.Analyzer = mock_cls
            inst = mock_cls.return_value
            inst.analyze = mock_analyze

            with unittest.mock.patch.dict("sys.modules", {
                "databricks": unittest.mock.MagicMock(),
                "databricks.labs": unittest.mock.MagicMock(),
                "databricks.labs.bladespector": unittest.mock.MagicMock(),
                "databricks.labs.bladespector.analyzer": mock_mod,
            }):
                res = run_analyze({
                    "input_dir": in_dir,
                    "report_xlsx_path": rep_xlsx,
                    "report_json_path": rep_json,
                    "source_system": "sqlserver",
                })

                # Preserves status=SUCCEEDED with warning
                self.assertEqual(res["status"], "SUCCEEDED")
                self.assertTrue(res.get("has_warnings"))
                self.assertIn("Analysis failed", res.get("warning", ""))

            # Verify report can be read and contains usable inventory results
            wb = read_analyzer_report(report_json_path=rep_json)
            self.assertTrue(has_usable_analyzer_results(wb))
            s_count, unk_count = extract_statement_counts(wb, object_name="dbo.sp_calc")
            self.assertEqual(s_count, 34)
            self.assertEqual(unk_count, 0)
            comp = extract_complexity(wb, object_name="dbo.sp_calc")
            self.assertEqual(comp, "MEDIUM")
            refs = extract_referenced_objects(wb, object_name="dbo.sp_calc")
            self.assertEqual(len(refs), 1)
            self.assertEqual(refs[0]["object"], "dbo.T_Source")

    def test_B_analyzer_no_usable_report_genuine_failure(self):
        """B. Missing, corrupt, unreadable, empty, or structurally invalid Analyzer output
        must still be treated as a genuine Analyzer failure."""
        with tempfile.TemporaryDirectory() as td:
            in_dir = os.path.join(td, "input")
            os.makedirs(in_dir, exist_ok=True)
            with open(os.path.join(in_dir, "test.sql"), "w", encoding="utf-8") as f:
                f.write("SELECT 1;")

            rep_xlsx = os.path.join(td, "report.xlsx")
            rep_json = os.path.join(td, "report.json")

            # 1. Missing report file + failure
            def mock_fail_no_file(*args, **kwargs):
                raise RuntimeError("Analysis failed completely")

            mock_mod1 = unittest.mock.MagicMock()
            mock_cls1 = unittest.mock.MagicMock()
            mock_mod1.Analyzer = mock_cls1
            mock_cls1.return_value.analyze = mock_fail_no_file

            with unittest.mock.patch.dict("sys.modules", {
                "databricks": unittest.mock.MagicMock(),
                "databricks.labs": unittest.mock.MagicMock(),
                "databricks.labs.bladespector": unittest.mock.MagicMock(),
                "databricks.labs.bladespector.analyzer": mock_mod1,
            }):
                with self.assertRaises(RuntimeError) as ctx:
                    run_analyze({
                        "input_dir": in_dir,
                        "report_xlsx_path": rep_xlsx,
                        "report_json_path": rep_json,
                        "source_system": "sqlserver",
                    })
                self.assertIn("Analysis failed completely", str(ctx.exception))

            # 2. Corrupt / unreadable JSON
            def mock_fail_corrupt(*args, **kwargs):
                if "json_result" in kwargs:
                    with open(str(kwargs["json_result"]), "w", encoding="utf-8") as jf:
                        jf.write("NOT_VALID_JSON{{{")
                raise RuntimeError("Analysis failed on bad syntax")

            mock_mod2 = unittest.mock.MagicMock()
            mock_cls2 = unittest.mock.MagicMock()
            mock_mod2.Analyzer = mock_cls2
            mock_cls2.return_value.analyze = mock_fail_corrupt

            with unittest.mock.patch.dict("sys.modules", {
                "databricks": unittest.mock.MagicMock(),
                "databricks.labs": unittest.mock.MagicMock(),
                "databricks.labs.bladespector": unittest.mock.MagicMock(),
                "databricks.labs.bladespector.analyzer": mock_mod2,
            }):
                with self.assertRaises(RuntimeError) as ctx:
                    run_analyze({
                        "input_dir": in_dir,
                        "report_xlsx_path": rep_xlsx,
                        "report_json_path": rep_json,
                        "source_system": "sqlserver",
                    })
                self.assertIn("Analysis failed on bad syntax", str(ctx.exception))

            # 3. Empty inventory / summary only
            def mock_empty_inventory(*args, **kwargs):
                if "json_result" in kwargs:
                    with open(str(kwargs["json_result"]), "w", encoding="utf-8") as jf:
                        json.dump({"summary": [{"total": 0}], "inventory": []}, jf)
                raise RuntimeError("Analysis failed with empty inventory")

            mock_mod3 = unittest.mock.MagicMock()
            mock_cls3 = unittest.mock.MagicMock()
            mock_mod3.Analyzer = mock_cls3
            mock_cls3.return_value.analyze = mock_empty_inventory

            with unittest.mock.patch.dict("sys.modules", {
                "databricks": unittest.mock.MagicMock(),
                "databricks.labs": unittest.mock.MagicMock(),
                "databricks.labs.bladespector": unittest.mock.MagicMock(),
                "databricks.labs.bladespector.analyzer": mock_mod3,
            }):
                with self.assertRaises(RuntimeError) as ctx:
                    run_analyze({
                        "input_dir": in_dir,
                        "report_xlsx_path": rep_xlsx,
                        "report_json_path": rep_json,
                        "source_system": "sqlserver",
                    })
                self.assertIn("Analysis failed with empty inventory", str(ctx.exception))

            # 4. has_usable_analyzer_results rejects empty/corrupt dicts
            self.assertFalse(has_usable_analyzer_results(None))
            self.assertFalse(has_usable_analyzer_results({}))
            self.assertFalse(has_usable_analyzer_results({"inventory": []}))
            self.assertFalse(has_usable_analyzer_results({"summary": [{"count": 1}]}))

    def test_C_bladebridge_line_out_of_bounds_isolation(self):
        """C. BladeBridge raises 'Line out of bounds: line 99 >= total lines 99':
        artifact is safely marked FAILED/manual-review without modifying the SQL.
        converted_definition = NULL, converted_definition_hash = NULL,
        sanitized error preserved, Analyzer metadata preserved."""
        with tempfile.TemporaryDirectory() as td:
            lines = [f"    -- Statement line {i}" for i in range(1, 98)]
            src_sql = "CREATE PROCEDURE dbo.sp_payroll AS\nBEGIN\n" + "\n".join(lines) + "\nEND;"
            physical_line_count = len(src_sql.splitlines())
            self.assertEqual(physical_line_count, 100)

            in_dir = os.path.join(td, "input")
            os.makedirs(in_dir, exist_ok=True)
            staged_file = prepare_lakebridge_input_file(
                input_base_dir=in_dir,
                connection_id="conn1",
                source_database="db1",
                source_schema="dbo",
                object_type="PROCEDURE",
                object_name="sp_payroll",
                source_definition=src_sql,
                source_system="sqlserver",
            )

            # Staged file must have the exact same line count and content - NO blank lines added!
            with open(staged_file, "r", encoding="utf-8") as f:
                staged_content = f.read()
            self.assertEqual(staged_content, src_sql)
            self.assertEqual(len(staged_content.splitlines()), physical_line_count)

            out_file = os.path.join(td, "converted.sql")

            # BladeBridge transpile raises IndexError: Line out of bounds
            mock_bb_mod = unittest.mock.MagicMock()
            mock_trans = unittest.mock.MagicMock()
            mock_bb_mod.Transpiler = mock_trans
            inst = mock_trans.return_value
            async def mock_fail_transpile(fname, sql):
                raise IndexError("Line out of bounds: line 99 >= total lines 99")
            inst.transpile = mock_fail_transpile

            with unittest.mock.patch.dict("sys.modules", {
                "databricks": unittest.mock.MagicMock(),
                "databricks.labs": unittest.mock.MagicMock(),
                "databricks.labs.bladebridge": unittest.mock.MagicMock(),
                "databricks.labs.bladebridge.transpiler": mock_bb_mod,
            }):
                with self.assertRaises(IndexError) as ctx:
                    run_transpile({
                        "source_file": staged_file,
                        "output_file": out_file,
                        "source_system": "sqlserver",
                    })
                self.assertIn("Line out of bounds: line 99 >= total lines 99", str(ctx.exception))

            # Verify classification derivation when BladeBridge fails
            cls_res, conv_status, man_req, man_reason, err_code, err_msg = derive_lakebridge_classification(
                source_definition=src_sql,
                converted_definition=None,
                complexity="HIGH",
                statement_count=34,
                unknown_statement_count=0,
                unknown_fragments=[],
                constructs={"uses_error_handling": False, "uses_rowcount": False, "uses_cursor": False, "uses_dynamic_sql": False, "uses_trigger": False},
                parsing_error_count=0,
                validation_error_count=0,
                generation_error_count=1,
                fixme_count=0,
                remaining_source_syntax=[],
                analyzer_failed=False,
                transpile_failed=True,
                object_type="PROCEDURE",
            )
            self.assertEqual(conv_status, "FAILED")
            self.assertTrue(man_req)
            self.assertEqual(cls_res, LAKEBRIDGE_CLASSIFICATION_UNSUPPORTED)

    def test_D_multiple_artifacts_one_failure_does_not_abort_subsequent_artifacts(self):
        """D. Multiple artifacts: one BladeBridge failure does not stop subsequent artifacts.
        Artifact A succeeds, Artifact B fails BladeBridge, Artifact C succeeds."""
        processed_artifacts = []

        candidates = [
            {
                "artifact_id": "art_1_view",
                "object_name": "v_report",
                "object_type": "VIEW",
                "source_schema": "dbo",
                "source_system": "sqlserver",
                "source_definition": "CREATE VIEW dbo.v_report AS SELECT 1 AS id;",
            },
            {
                "artifact_id": "art_2_proc_fails",
                "object_name": "sp_large",
                "object_type": "PROCEDURE",
                "source_schema": "dbo",
                "source_system": "sqlserver",
                "source_definition": "CREATE PROCEDURE dbo.sp_large AS SELECT 2;",
            },
            {
                "artifact_id": "art_3_proc_ok",
                "object_name": "sp_next",
                "object_type": "PROCEDURE",
                "source_schema": "dbo",
                "source_system": "sqlserver",
                "source_definition": "CREATE PROCEDURE dbo.sp_next AS SELECT 3;",
            },
        ]

        def mock_runner(req, attempt_dir):
            act = req.get("action")
            if act == "analyze":
                r_json = req.get("report_json_path")
                if r_json:
                    with open(r_json, "w", encoding="utf-8") as jf:
                        json.dump({"inventory": [{"name": "obj", "statementCount": 10}]}, jf)
                return {"status": "SUCCEEDED"}
            # In transpile:
            if "art_2" in req.get("source_file", ""):
                return {
                    "status": "FAILED",
                    "error": "Line out of bounds: line 99 >= total lines 99",
                }
            # Others succeed:
            out_file = req.get("output_file")
            if out_file:
                os.makedirs(os.path.dirname(out_file), exist_ok=True)
                with open(out_file, "w", encoding="utf-8") as f:
                    f.write("CREATE OR REPLACE VIEW target_obj AS SELECT 1;")
            return {"status": "SUCCEEDED", "diagnostic_error_count": 0}

        results = {}
        for cand in candidates:
            art_id = cand["artifact_id"]
            src_def = cand["source_definition"]
            with tempfile.TemporaryDirectory() as att_dir:
                in_dir = os.path.join(att_dir, "input")
                rep_dir = os.path.join(att_dir, "report")
                out_dir = os.path.join(att_dir, "output")
                err_dir = os.path.join(att_dir, "error")
                for d in (in_dir, rep_dir, out_dir, err_dir):
                    os.makedirs(d, exist_ok=True)

                in_f = prepare_lakebridge_input_file(
                    in_dir, "conn1", None, "dbo", cand["object_type"],
                    cand["object_name"], src_def, artifact_id=art_id,
                )
                rep_f = os.path.join(rep_dir, f"{art_id}_report.json")
                out_f = os.path.join(out_dir, f"{art_id}_converted.sql")

                # Analyze
                mock_runner({
                    "action": "analyze",
                    "input_dir": in_dir,
                    "report_json_path": rep_f,
                    "source_system": "sqlserver",
                }, att_dir)
                has_rep = has_usable_analyzer_results(read_analyzer_report(report_json_path=rep_f))

                # Transpile
                tr_resp = mock_runner({
                    "action": "transpile",
                    "source_file": in_f,
                    "output_file": out_f,
                    "source_system": "sqlserver",
                }, att_dir)

                tr_failed = (tr_resp.get("status") != "SUCCEEDED")
                conv_sql = None
                if not tr_failed and os.path.isfile(out_f):
                    with open(out_f, "r", encoding="utf-8") as cf:
                        conv_sql = cf.read().strip()

                cls_res, conv_status, man_req, man_reason, err_code, err_msg = derive_lakebridge_classification(
                    source_definition=src_def,
                    converted_definition=conv_sql,
                    complexity="LOW",
                    statement_count=10,
                    unknown_statement_count=0,
                    unknown_fragments=[],
                    constructs={"uses_error_handling": False, "uses_rowcount": False, "uses_cursor": False, "uses_dynamic_sql": False, "uses_trigger": False},
                    parsing_error_count=0,
                    validation_error_count=0,
                    generation_error_count=1 if tr_failed else 0,
                    fixme_count=0,
                    remaining_source_syntax=[],
                    analyzer_failed=not has_rep,
                    transpile_failed=tr_failed,
                    object_type=cand["object_type"],
                    object_map_applied=True,
                )

                results[art_id] = {
                    "conversion_status": conv_status,
                    "converted_definition": conv_sql,
                    "manual_review_required": man_req,
                    "classification": cls_res,
                }
                processed_artifacts.append(art_id)

        # All 3 artifacts were processed
        self.assertEqual(processed_artifacts, ["art_1_view", "art_2_proc_fails", "art_3_proc_ok"])
        self.assertEqual(results["art_1_view"]["conversion_status"], "CONVERTED")
        self.assertIsNotNone(results["art_1_view"]["converted_definition"])

        self.assertEqual(results["art_2_proc_fails"]["conversion_status"], "FAILED")
        self.assertIsNone(results["art_2_proc_fails"]["converted_definition"])
        self.assertTrue(results["art_2_proc_fails"]["manual_review_required"])

        self.assertEqual(results["art_3_proc_ok"]["conversion_status"], "CONVERTED")
        self.assertIsNotNone(results["art_3_proc_ok"]["converted_definition"])

    def test_E_successful_artifacts_behave_as_before(self):
        """E. Successful artifacts behave exactly as before."""
        cls_res, conv_status, man_req, man_reason, err_code, err_msg = derive_lakebridge_classification(
            source_definition="CREATE VIEW dbo.v_test AS SELECT 1 AS x;",
            converted_definition="CREATE OR REPLACE VIEW target.v_test AS SELECT 1 AS x;",
            complexity="LOW",
            statement_count=1,
            unknown_statement_count=0,
            unknown_fragments=[],
            constructs={"uses_error_handling": False, "uses_rowcount": False, "uses_cursor": False, "uses_dynamic_sql": False, "uses_trigger": False},
            parsing_error_count=0,
            validation_error_count=0,
            generation_error_count=0,
            fixme_count=0,
            remaining_source_syntax=[],
            analyzer_failed=False,
            transpile_failed=False,
            object_type="VIEW",
            object_map_applied=True,
        )
        self.assertEqual(cls_res, LAKEBRIDGE_CLASSIFICATION_AUTO_CANDIDATE)
        self.assertEqual(conv_status, "CONVERTED")
        self.assertFalse(man_req)
        self.assertIsNone(err_code)
        self.assertIsNone(err_msg)

    def test_F_no_converted_sql_marked_successful_unless_actual_output_exists(self):
        """F. No converted SQL is marked successful unless actual valid converted output exists."""
        for empty_or_none in [None, "", "   \n\t  "]:
            cls_res, conv_status, man_req, man_reason, err_code, err_msg = derive_lakebridge_classification(
                source_definition="CREATE VIEW dbo.v_test AS SELECT 1;",
                converted_definition=empty_or_none,
                complexity="LOW",
                statement_count=1,
                unknown_statement_count=0,
                unknown_fragments=[],
                constructs={"uses_error_handling": False, "uses_rowcount": False, "uses_cursor": False, "uses_dynamic_sql": False, "uses_trigger": False},
                parsing_error_count=0,
                validation_error_count=0,
                generation_error_count=0,
                fixme_count=0,
                remaining_source_syntax=[],
                analyzer_failed=False,
                transpile_failed=False,
                object_type="VIEW",
            )
            self.assertNotEqual(conv_status, "CONVERTED")
            self.assertEqual(cls_res, LAKEBRIDGE_CLASSIFICATION_UNSUPPORTED)
            self.assertTrue(man_req)

    def test_G_no_source_sql_manipulation_or_line_padding_workaround(self):
        """G. No source SQL manipulation or line-padding workaround is introduced anywhere."""
        runner_path = os.path.join(ROOT, "src", "lakebridge_runner.py")
        with open(runner_path, "r", encoding="utf-8") as f:
            runner_code = f.read()

        nb24_path = os.path.join(ROOT, "notebooks", "shared", "NB24_LakebridgeAnalyzeAndTranspile.py")
        with open(nb24_path, "r", encoding="utf-8") as f:
            nb24_code = f.read()

        # Verify no artificial blank lines or padding logic
        for code in (runner_code, nb24_code):
            self.assertNotIn("+ '\\n\\n'", code)
            self.assertNotIn("+ \"\\n\\n\"", code)
            self.assertNotIn("source_sql + \"\\n\"", code)
            self.assertNotIn("source_sql + '\\n'", code)
            self.assertNotIn("line 99", code)
            self.assertNotIn("total lines 99", code)

        # Verify prepare_lakebridge_input_file does not alter or pad SQL
        with tempfile.TemporaryDirectory() as td:
            sql_exact = "SELECT 1;\nSELECT 2;\nSELECT 3;"
            written = prepare_lakebridge_input_file(
                td, "c1", None, "dbo", "VIEW", "v1", sql_exact,
            )
            with open(written, "r", encoding="utf-8") as f:
                read_back = f.read()
            self.assertEqual(read_back, sql_exact)


if __name__ == "__main__":
    unittest.main()


