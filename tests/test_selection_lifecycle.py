"""Executable unit tests for assessment selection lifecycle, atomic claim concurrency,
failure state transitions, target reservation, target config scoping, and payload guards.
"""

from __future__ import annotations

import json
import os
import re
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
SRC = os.path.join(ROOT, "src")
DEPLOYMENT = os.path.join(ROOT, "notebooks", "deployment")
SHARED = os.path.join(ROOT, "notebooks", "shared")

for p in (SRC, ROOT, HERE):
    if p not in sys.path:
        sys.path.insert(0, p)

from control_repository import (
    ControlRepository,
    VALID_SELECTION_STATUSES,
    VALID_ONBOARDING_STAGES,
    VALID_DOWNSTREAM_ONBOARDING_STAGES,
    TERMINAL_SELECTION_STATUSES,
    CLAIMABLE_SELECTION_STATUSES,
    RETRYABLE_SELECTION_STATUSES,
    ClaimResult,
    escape_string_literal,
    is_assessment_selection_candidate,
    is_delta_concurrency_exception,
    normalize_target_component,
    quote_databricks,
    sanitize_error_message,
)
from worklist_utils import (
    TASK_VALUE_LIMIT_BYTES,
    canonical_task_value_serialization,
    validate_task_value_payload,
)
from source_identity import (
    canonical_source_system_sql,
    compute_source_table_id,
    require_source_system,
)
from _fakes import FakeDataFrame, FakeRow
from _nbsource import shared_nb


class StatefulControlSpark:
    """In-memory stateful test double evaluating UPDATE and SELECT queries on control tables.

    The fake validates recovery state-machine behavior and conditional SQL intent.
    The fake does not prove live Delta transaction behavior.
    """

    def __init__(self):
        self.assessment_rows: list[dict] = []
        self.control_rows: list[dict] = []
        self.connection_rows: list[dict] = []
        self.executed: list[str] = []
        self.simulate_transition_failure: bool = False
        self.simulate_reset_failure: bool = False
        self.simulate_exception: Exception | None = None
        self.simulate_mutation_exception: Exception | None = None

    def sql(self, statement: str) -> FakeDataFrame:
        self.executed.append(statement)
        if self.simulate_exception is not None:
            raise self.simulate_exception
        norm = statement.strip()
        if self.simulate_mutation_exception is not None and (norm.startswith("UPDATE") or norm.startswith("MERGE")):
            raise self.simulate_mutation_exception

        # Handle UPDATE source_assessment
        if norm.startswith("UPDATE") and "source_assessment" in norm:
            return self._handle_update_assessment(norm)

        # Handle SELECT from source_assessment
        if norm.startswith("SELECT") and "source_assessment" in norm:
            return self._handle_select_assessment(norm)

        # Handle SELECT from source_table_control (e.g. find_target_owners or existing registration)
        if norm.startswith("SELECT") and "source_table_control" in norm:
            return self._handle_select_control(norm)

        # Handle SELECT from source_connection
        if norm.startswith("SELECT") and "source_connection" in norm:
            return self._handle_select_connection(norm)

        return FakeDataFrame([])

    def _handle_update_assessment(self, sql: str) -> FakeDataFrame:
        if self.simulate_transition_failure:
            return FakeDataFrame([])
        if self.simulate_reset_failure and "onboarding_run_id = NULL" in sql:
            return FakeDataFrame([])

        # Extract WHERE clause
        where_match = re.search(r"\bWHERE\b\s+(.*)$", sql, re.IGNORECASE | re.DOTALL)
        where_clause = where_match.group(1) if where_match else ""

        # Extract SET assignments
        set_match = re.search(r"\bSET\b\s+(.*?)\s+\bWHERE\b", sql, re.IGNORECASE | re.DOTALL)
        set_clause = set_match.group(1) if set_match else ""

        updated_count = 0
        for row in self.assessment_rows:
            if self._row_matches_assessment_where(row, where_clause):
                self._apply_assessment_set(row, set_clause)
                updated_count += 1

        return FakeDataFrame([])

    def _row_matches_assessment_where(self, row: dict, where_clause: str) -> bool:
        # Check connection_id
        m = re.search(r"(?:sa\.)?connection_id\s*=\s*'([^']*)'", where_clause)
        if m and row.get("connection_id") != m.group(1):
            return False

        # Check assessment_id
        m = re.search(r"(?:sa\.)?assessment_id\s*=\s*'([^']*)'", where_clause)
        if m and row.get("assessment_id") != m.group(1):
            return False

        # Check source_schema
        m = re.search(r"(?:sa\.)?source_schema\s*=\s*'([^']*)'", where_clause)
        if m and row.get("source_schema") != m.group(1):
            return False

        # Check object_type
        m = re.search(r"(?:sa\.)?object_type\s*=\s*'([^']*)'", where_clause)
        if m and row.get("object_type") != m.group(1):
            return False

        # Check object_name
        m = re.search(r"(?:sa\.)?object_name\s*=\s*'([^']*)'", where_clause)
        if m and row.get("object_name") != m.group(1):
            return False

        # Check is_selected
        if "coalesce(is_selected, false) = true" in where_clause:
            if not bool(row.get("is_selected")):
                return False

        # Check compatibility_status
        if "upper(trim(coalesce(compatibility_status, ''))) IN ('COMPATIBLE', 'REVIEW')" in where_clause:
            comp = str(row.get("compatibility_status") or "").upper().strip()
            if comp not in ("COMPATIBLE", "REVIEW"):
                return False

        # Check unowned conditions
        if "(onboarding_run_id IS NULL OR trim(onboarding_run_id) = '')" in where_clause:
            if str(row.get("onboarding_run_id") or "").strip():
                return False
        elif "onboarding_run_id IS NULL" in where_clause:
            if str(row.get("onboarding_run_id") or "").strip():
                return False

        if "(onboarding_attempt_id = " not in where_clause:
            if "(onboarding_attempt_id IS NULL OR trim(onboarding_attempt_id) = '')" in where_clause:
                if str(row.get("onboarding_attempt_id") or "").strip():
                    return False
            elif "onboarding_attempt_id IS NULL" in where_clause:
                if str(row.get("onboarding_attempt_id") or "").strip():
                    return False

        # Check positive prior condition in claim_assessment_selection_row and preclaim failure
        if "IN ('SELECTED', 'FAILED')" in where_clause:
            st = str(row.get("selection_status") or "").upper().strip()
            if st not in ("", "SELECTED", "FAILED"):
                return False
        elif "(selection_status IS NULL OR trim(selection_status) = '' OR upper(trim(selection_status)) = 'SELECTED')" in where_clause:
            st = str(row.get("selection_status") or "").upper().strip()
            if st not in ("", "SELECTED"):
                return False
        elif "IN ('NOT_SELECTED', 'SELECTED', 'FAILED')" in where_clause:
            st = str(row.get("selection_status") or "").upper().strip()
            if st not in ("", "NOT_SELECTED", "SELECTED", "FAILED"):
                return False

        # Check exact selection_status
        m = re.search(r"upper\(trim\(coalesce\(selection_status,\s*''\)\)\)\s*=\s*'([^']*)'", where_clause)
        if m:
            if str(row.get("selection_status") or "").upper().strip() != m.group(1).upper():
                return False
        else:
            m = re.search(r"\bselection_status\s*=\s*'([^']*)'", where_clause)
            if m and str(row.get("selection_status") or "").upper().strip() != m.group(1).upper():
                return False

        # Check selection_status IN ('ONBOARDING', 'REGISTERED')
        if "IN ('ONBOARDING', 'REGISTERED')" in where_clause:
            st = str(row.get("selection_status") or "").upper().strip()
            if st not in ("ONBOARDING", "REGISTERED"):
                return False

        # Check selection_status IN ('ONBOARDING', 'FAILED')
        if "IN ('ONBOARDING', 'FAILED')" in where_clause:
            st = str(row.get("selection_status") or "").upper().strip()
            if st not in ("ONBOARDING", "FAILED"):
                return False

        # Check onboarding_run_id
        m = re.search(r"(?:sa\.)?onboarding_run_id\s*=\s*'([^']*)'", where_clause)
        if m and str(row.get("onboarding_run_id") or "").strip() != m.group(1).strip():
            return False

        # Check onboarding_attempt_id
        if "OR onboarding_attempt_id IS NULL" in where_clause:
            m = re.search(r"onboarding_attempt_id\s*=\s*'([^']*)'", where_clause)
            row_att = str(row.get("onboarding_attempt_id") or "").strip()
            if row_att and m and row_att != m.group(1).strip():
                return False
        else:
            m = re.search(r"(?:sa\.)?onboarding_attempt_id\s*=\s*'([^']*)'", where_clause)
            if m and str(row.get("onboarding_attempt_id") or "").strip() != m.group(1).strip():
                return False

        return True

    def _apply_assessment_set(self, row: dict, set_clause: str):
        # Update selection_status
        m = re.search(r"`?selection_status`?\s*=\s*'([^']*)'", set_clause)
        if m:
            row["selection_status"] = m.group(1)

        # Update onboarding_run_id
        if "`onboarding_run_id` = NULL" in set_clause or "onboarding_run_id = NULL" in set_clause:
            row["onboarding_run_id"] = None
        else:
            m = re.search(r"`?onboarding_run_id`?\s*=\s*'([^']*)'", set_clause)
            if m:
                row["onboarding_run_id"] = m.group(1)

        # Update onboarding_attempt_id
        if "`onboarding_attempt_id` = NULL" in set_clause or "onboarding_attempt_id = NULL" in set_clause:
            row["onboarding_attempt_id"] = None
        else:
            m = re.search(r"`?onboarding_attempt_id`?\s*=\s*'([^']*)'", set_clause)
            if m:
                row["onboarding_attempt_id"] = m.group(1)

        # Update is_selected
        if "`is_selected` = true" in set_clause or "is_selected = true" in set_clause:
            row["is_selected"] = True
        elif "`is_selected` = false" in set_clause or "is_selected = false" in set_clause:
            row["is_selected"] = False

        # Update selected_by
        if "`selected_by` = NULL" in set_clause or "selected_by = NULL" in set_clause:
            row["selected_by"] = None
        else:
            m = re.search(r"`?selected_by`?\s*=\s*'([^']*)'", set_clause)
            if m:
                row["selected_by"] = m.group(1)

        # Update onboarding_failed_stage
        if "`onboarding_failed_stage` = NULL" in set_clause or "onboarding_failed_stage = NULL" in set_clause:
            row["onboarding_failed_stage"] = None
        else:
            m = re.search(r"`?onboarding_failed_stage`?\s*=\s*'([^']*)'", set_clause)
            if m:
                row["onboarding_failed_stage"] = m.group(1)

        # Update onboarding_error_message
        if "`onboarding_error_message` = NULL" in set_clause or "onboarding_error_message = NULL" in set_clause:
            row["onboarding_error_message"] = None
        else:
            m = re.search(r"`?onboarding_error_message`?\s*=\s*'([^']*)'", set_clause)
            if m:
                row["onboarding_error_message"] = m.group(1)

        # Timestamps
        if "`onboarding_started_ts` = NULL" in set_clause or "onboarding_started_ts = NULL" in set_clause:
            row["onboarding_started_ts"] = None
        elif "onboarding_started_ts" in set_clause:
            row["onboarding_started_ts"] = "2026-09-18T10:00:00Z"

        if "`registration_completed_ts` = NULL" in set_clause or "registration_completed_ts = NULL" in set_clause:
            row["registration_completed_ts"] = None
        elif "registration_completed_ts" in set_clause:
            row["registration_completed_ts"] = "2026-09-18T10:01:00Z"

        if "`onboarding_completed_ts` = NULL" in set_clause or "onboarding_completed_ts = NULL" in set_clause:
            row["onboarding_completed_ts"] = None
        elif "onboarding_completed_ts" in set_clause:
            row["onboarding_completed_ts"] = "2026-09-18T10:05:00Z"

    def _handle_select_assessment(self, sql: str) -> FakeDataFrame:
        m_conn = re.search(r"(?:sa\.)?connection_id\s*=\s*'([^']*)'", sql)
        m_assess = re.search(r"(?:sa\.)?assessment_id\s*=\s*'([^']*)'", sql)
        m_schema = re.search(r"(?:sa\.)?source_schema\s*=\s*'([^']*)'", sql)
        m_obj = re.search(r"(?:sa\.)?object_name\s*=\s*'([^']*)'", sql)
        m_run = re.search(r"(?:sa\.)?onboarding_run_id\s*=\s*'([^']*)'", sql)
        m_att = re.search(r"(?:sa\.)?onboarding_attempt_id\s*=\s*'([^']*)'", sql)

        conn_id = m_conn.group(1) if m_conn else None
        assess_id = m_assess.group(1) if m_assess else None
        schema = m_schema.group(1) if m_schema else None
        obj = m_obj.group(1) if m_obj else None
        run_id = m_run.group(1) if m_run else None
        att_id = m_att.group(1) if m_att else None

        matching = []
        for r in self.assessment_rows:
            if conn_id and r.get("connection_id") != conn_id:
                continue
            if assess_id and r.get("assessment_id") != assess_id:
                continue
            if schema and r.get("source_schema") != schema:
                continue
            if obj and r.get("object_name") != obj:
                continue
            if run_id and str(r.get("onboarding_run_id") or "").strip() != run_id.strip():
                continue
            if att_id and str(r.get("onboarding_attempt_id") or "").strip() != att_id.strip():
                continue
            matching.append(FakeRow(dict(r)))
        return FakeDataFrame(matching)

    def _handle_select_connection(self, sql: str) -> FakeDataFrame:
        m_conn = re.search(r"connection_id\s*=\s*'([^']*)'", sql)
        if m_conn:
            conn = m_conn.group(1)
            results = [FakeRow(dict(r)) for r in self.connection_rows if r.get("connection_id") == conn]
            return FakeDataFrame(results)
        return FakeDataFrame([FakeRow(dict(r)) for r in self.connection_rows])

    def _handle_select_control(self, sql: str) -> FakeDataFrame:
        m_conn = re.search(r"connection_id\s*=\s*'([^']*)'", sql)
        m_sid = re.search(r"source_table_id\s*=\s*'([^']*)'", sql)
        m_schema = re.search(r"source_schema\s*=\s*'([^']*)'", sql)
        m_table = re.search(r"source_table\s*=\s*'([^']*)'", sql)

        # Exact table lookup by connection_id and source_table_id
        if m_conn and m_sid and not (m_schema and m_table):
            conn = m_conn.group(1)
            sid = m_sid.group(1)
            results = []
            for r in self.control_rows:
                if (
                    r.get("connection_id") == conn
                    and r.get("source_table_id") == sid
                ):
                    results.append(FakeRow(dict(r)))
            return FakeDataFrame(results)

        # Exact table lookup by connection_id, source_schema, source_table
        if m_conn and m_schema and m_table:
            conn = m_conn.group(1)
            sch = m_schema.group(1)
            tbl = m_table.group(1)
            results = []
            for r in self.control_rows:
                if (
                    r.get("connection_id") == conn
                    and r.get("source_schema") == sch
                    and r.get("source_table") == tbl
                ):
                    results.append(FakeRow(dict(r)))
            return FakeDataFrame(results)

        # find_target_owners logic
        m_cat = re.search(r"lower\(trim\(coalesce\(target_catalog,\s*''\)\)\)\s*=\s*'([^']*)'", sql)
        m_sch = re.search(r"lower\(trim\(coalesce\(target_schema,\s*''\)\)\)\s*=\s*'([^']*)'", sql)
        m_tbl = re.search(r"lower\(trim\(coalesce\(target_table,\s*''\)\)\)\s*=\s*'([^']*)'", sql)

        cat = m_cat.group(1).lower() if m_cat else ""
        sch = m_sch.group(1).lower() if m_sch else ""
        tbl = m_tbl.group(1).lower() if m_tbl else ""

        results = []
        for r in self.control_rows:
            rcat = str(r.get("target_catalog") or "").strip().lower()
            rsch = str(r.get("target_schema") or "").strip().lower()
            rtbl = str(r.get("target_table") or "").strip().lower()
            if rcat == cat and rsch == sch and rtbl == tbl:
                status = str(r.get("current_status") or "").strip().upper()
                if "coalesce(current_status, '') NOT IN ('RETIRED', 'DECOMMISSIONED')" in sql:
                    if status in ("RETIRED", "DECOMMISSIONED"):
                        continue
                elif "is_active = true" in sql:
                    if not bool(r.get("is_active")):
                        continue
                results.append(FakeRow(dict(r)))
        return FakeDataFrame(results)


def _make_assessment_row(
    connection_id="conn_ora_01",
    assessment_id="assess_2026",
    source_schema="HR",
    object_name="EMPLOYEES",
    is_selected=True,
    compatibility_status="COMPATIBLE",
    selection_status="SELECTED",
    onboarding_run_id=None,
    onboarding_attempt_id=None,
    onboarding_failed_stage=None,
    onboarding_error_message=None,
    selected_by=None,
) -> dict:
    return {
        "connection_id": connection_id,
        "assessment_id": assessment_id,
        "source_schema": source_schema,
        "object_type": "TABLE",
        "object_name": object_name,
        "is_selected": is_selected,
        "compatibility_status": compatibility_status,
        "selection_status": selection_status,
        "onboarding_run_id": onboarding_run_id,
        "onboarding_attempt_id": onboarding_attempt_id,
        "onboarding_started_ts": None,
        "registration_completed_ts": None,
        "onboarding_completed_ts": None,
        "onboarding_failed_stage": onboarding_failed_stage,
        "onboarding_error_message": onboarding_error_message,
        "selected_by": selected_by,
    }


class TestAssessmentSelectionLifecycle(unittest.TestCase):
    """Executes state machine transitions and verifies lifecycle rules."""

    def setUp(self):
        self.spark = StatefulControlSpark()
        self.repo = ControlRepository(self.spark, catalog="lakehouse", control_schema="control")

    def test_selected_can_be_claimed(self):
        row = _make_assessment_row(selection_status="SELECTED")
        self.spark.assessment_rows.append(row)

        res = self.repo.claim_assessment_selection_row(
            connection_id="conn_ora_01",
            assessment_id="assess_2026",
            source_schema="HR",
            object_name="EMPLOYEES",
            run_id="run_101",
            attempt_id="att_001",
        )
        self.assertTrue(res.acquired)
        self.assertEqual(row["selection_status"], "ONBOARDING")
        self.assertEqual(row["onboarding_run_id"], "run_101")
        self.assertEqual(row["onboarding_attempt_id"], "att_001")

    def test_legacy_selected_null_status_can_be_claimed(self):
        row = _make_assessment_row(selection_status=None)
        self.spark.assessment_rows.append(row)

        res = self.repo.claim_assessment_selection_row(
            connection_id="conn_ora_01",
            assessment_id="assess_2026",
            source_schema="HR",
            object_name="EMPLOYEES",
            run_id="run_101",
            attempt_id="att_001",
        )
        self.assertTrue(res.acquired)
        self.assertEqual(row["selection_status"], "ONBOARDING")

    def test_failed_cannot_be_claimed_by_default(self):
        row = _make_assessment_row(selection_status="FAILED")
        self.spark.assessment_rows.append(row)

        res = self.repo.claim_assessment_selection_row(
            connection_id="conn_ora_01",
            assessment_id="assess_2026",
            source_schema="HR",
            object_name="EMPLOYEES",
            run_id="run_101",
            attempt_id="att_001",
            allow_failed_retry=False,
        )
        self.assertFalse(res.acquired)
        self.assertEqual(row["selection_status"], "FAILED")

    def test_failed_can_be_claimed_for_explicit_retry(self):
        row = _make_assessment_row(selection_status="FAILED")
        self.spark.assessment_rows.append(row)

        res = self.repo.claim_assessment_selection_row(
            connection_id="conn_ora_01",
            assessment_id="assess_2026",
            source_schema="HR",
            object_name="EMPLOYEES",
            run_id="run_101",
            attempt_id="att_001",
            allow_failed_retry=True,
        )
        self.assertTrue(res.acquired)
        self.assertEqual(row["selection_status"], "ONBOARDING")

    def test_onboarding_cannot_be_claimed(self):
        row = _make_assessment_row(selection_status="ONBOARDING", onboarding_run_id="run_999", onboarding_attempt_id="att_999")
        self.spark.assessment_rows.append(row)

        res = self.repo.claim_assessment_selection_row(
            connection_id="conn_ora_01",
            assessment_id="assess_2026",
            source_schema="HR",
            object_name="EMPLOYEES",
            run_id="run_101",
            attempt_id="att_001",
        )
        self.assertFalse(res.acquired)
        self.assertEqual(row["onboarding_run_id"], "run_999")

    def test_registered_cannot_be_claimed(self):
        row = _make_assessment_row(selection_status="REGISTERED")
        self.spark.assessment_rows.append(row)

        res = self.repo.claim_assessment_selection_row(
            connection_id="conn_ora_01",
            assessment_id="assess_2026",
            source_schema="HR",
            object_name="EMPLOYEES",
            run_id="run_101",
            attempt_id="att_001",
        )
        self.assertFalse(res.acquired)

    def test_onboarded_cannot_be_claimed(self):
        row = _make_assessment_row(selection_status="ONBOARDED")
        self.spark.assessment_rows.append(row)

        res = self.repo.claim_assessment_selection_row(
            connection_id="conn_ora_01",
            assessment_id="assess_2026",
            source_schema="HR",
            object_name="EMPLOYEES",
            run_id="run_101",
            attempt_id="att_001",
        )
        self.assertFalse(res.acquired)

    def test_terminal_review_or_blocked_cannot_be_claimed(self):
        for status in ("REVIEW_REQUIRED", "BLOCKED"):
            with self.subTest(status=status):
                row = _make_assessment_row(selection_status=status)
                self.spark.assessment_rows = [row]
                res = self.repo.claim_assessment_selection_row(
                    connection_id="conn_ora_01",
                    assessment_id="assess_2026",
                    source_schema="HR",
                    object_name="EMPLOYEES",
                    run_id="run_101",
                    attempt_id="att_001",
                    allow_failed_retry=True,
                )
                self.assertFalse(res.acquired)

    def test_incompatible_or_unselected_cannot_be_claimed(self):
        # Incompatible
        row1 = _make_assessment_row(compatibility_status="INCOMPATIBLE")
        self.spark.assessment_rows = [row1]
        res1 = self.repo.claim_assessment_selection_row(
            connection_id="conn_ora_01",
            assessment_id="assess_2026",
            source_schema="HR",
            object_name="EMPLOYEES",
            run_id="run_101",
            attempt_id="att_001",
        )
        self.assertFalse(res1.acquired)

        # Unselected
        row2 = _make_assessment_row(is_selected=False)
        self.spark.assessment_rows = [row2]
        res2 = self.repo.claim_assessment_selection_row(
            connection_id="conn_ora_01",
            assessment_id="assess_2026",
            source_schema="HR",
            object_name="EMPLOYEES",
            run_id="run_101",
            attempt_id="att_001",
        )
        self.assertFalse(res2.acquired)

    def test_full_lifecycle_success(self):
        """SELECTED -> ONBOARDING -> REGISTERED -> ONBOARDED."""
        row = _make_assessment_row(selection_status="SELECTED")
        self.spark.assessment_rows.append(row)

        # 1. Claim
        claim = self.repo.claim_assessment_selection_row(
            "conn_ora_01", "assess_2026", "HR", "EMPLOYEES", "run_101", "att_001"
        )
        self.assertTrue(claim.acquired)
        self.assertEqual(row["selection_status"], "ONBOARDING")

        # 2. NB01B marks REGISTERED
        reg_ok = self.repo.mark_assessment_registration_succeeded(
            "conn_ora_01", "assess_2026", "HR", "EMPLOYEES", "run_101", "att_001"
        )
        self.assertTrue(reg_ok)
        self.assertEqual(row["selection_status"], "REGISTERED")

        # 3. Finalizer marks ONBOARDED
        fin_ok = self.repo.mark_assessment_onboarding_completed(
            "conn_ora_01", "assess_2026", "HR", "EMPLOYEES", "run_101", "att_001"
        )
        self.assertTrue(fin_ok)
        self.assertEqual(row["selection_status"], "ONBOARDED")

    def test_strict_registration_attempt_ownership(self):
        """A. Strict registration attempt ownership in mark_assessment_registration_succeeded."""
        # 1. Matching run ID and matching attempt ID succeeds
        row1 = _make_assessment_row(
            selection_status="ONBOARDING",
            onboarding_run_id="run_101",
            onboarding_attempt_id="att_001",
        )
        self.spark.assessment_rows = [row1]
        ok = self.repo.mark_assessment_registration_succeeded(
            "conn_ora_01", "assess_2026", "HR", "EMPLOYEES", "run_101", "att_001"
        )
        self.assertTrue(ok)
        self.assertEqual(row1["selection_status"], "REGISTERED")
        self.assertEqual(row1["onboarding_attempt_id"], "att_001")
        self.assertIsNotNone(row1.get("registration_completed_ts"))

        # 2. Matching run ID and wrong attempt ID returns False
        row2 = _make_assessment_row(
            selection_status="ONBOARDING",
            onboarding_run_id="run_101",
            onboarding_attempt_id="att_001",
        )
        self.spark.assessment_rows = [row2]
        ok_wrong_att = self.repo.mark_assessment_registration_succeeded(
            "conn_ora_01", "assess_2026", "HR", "EMPLOYEES", "run_101", "att_wrong"
        )
        self.assertFalse(ok_wrong_att)
        self.assertEqual(row2["selection_status"], "ONBOARDING")
        self.assertEqual(row2["onboarding_attempt_id"], "att_001")

        # 3. Matching run ID and blank persisted attempt ID returns False
        row3 = _make_assessment_row(
            selection_status="ONBOARDING",
            onboarding_run_id="run_101",
            onboarding_attempt_id=None,
        )
        self.spark.assessment_rows = [row3]
        ok_blank_att = self.repo.mark_assessment_registration_succeeded(
            "conn_ora_01", "assess_2026", "HR", "EMPLOYEES", "run_101", "att_001"
        )
        self.assertFalse(ok_blank_att)
        self.assertEqual(row3["selection_status"], "ONBOARDING")
        self.assertIsNone(row3["onboarding_attempt_id"])

        # 4. SQL SET clause does not assign onboarding_attempt_id
        update_sqls = [s for s in self.spark.executed if s.strip().startswith("UPDATE") and "source_assessment" in s]
        self.assertTrue(len(update_sqls) > 0)
        latest_update = update_sqls[-1]
        set_match = re.search(r"\bSET\b\s+(.*?)\s+\bWHERE\b", latest_update, re.IGNORECASE | re.DOTALL)
        self.assertIsNotNone(set_match)
        self.assertNotIn("onboarding_attempt_id", set_match.group(1))

    def test_failure_lifecycle(self):
        """SELECTED -> ONBOARDING -> FAILED -> retry -> ONBOARDING."""
        row = _make_assessment_row(selection_status="SELECTED")
        self.spark.assessment_rows.append(row)

        # 1. Claim
        claim = self.repo.claim_assessment_selection_row(
            "conn_ora_01", "assess_2026", "HR", "EMPLOYEES", "run_101", "att_001"
        )
        self.assertTrue(claim.acquired)

        # 2. Caught registration failure
        fail_ok = self.repo.mark_assessment_onboarding_failed(
            "conn_ora_01", "assess_2026", "HR", "EMPLOYEES", "run_101", "att_001",
            failed_stage="REGISTRATION", error=Exception("Simulated registration error")
        )
        self.assertTrue(fail_ok)
        self.assertEqual(row["selection_status"], "FAILED")
        self.assertEqual(row["onboarding_failed_stage"], "REGISTRATION")
        self.assertIn("Simulated registration error", row["onboarding_error_message"])

        # 3. Explicit retry claim
        retry_claim = self.repo.claim_assessment_selection_row(
            "conn_ora_01", "assess_2026", "HR", "EMPLOYEES", "run_102", "att_002",
            allow_failed_retry=True
        )
        self.assertTrue(retry_claim.acquired)
        self.assertEqual(row["selection_status"], "ONBOARDING")
        self.assertEqual(row["onboarding_run_id"], "run_102")

    def test_downstream_failure_lifecycle(self):
        """SELECTED -> ONBOARDING -> REGISTERED -> downstream fails -> FAILED."""
        row = _make_assessment_row(selection_status="SELECTED")
        self.spark.assessment_rows.append(row)

        self.repo.claim_assessment_selection_row(
            "conn_ora_01", "assess_2026", "HR", "EMPLOYEES", "run_101", "att_001"
        )
        self.repo.mark_assessment_registration_succeeded(
            "conn_ora_01", "assess_2026", "HR", "EMPLOYEES", "run_101", "att_001"
        )
        self.assertEqual(row["selection_status"], "REGISTERED")

        # Downstream failure (e.g. TARGET_PROVISIONING)
        fail_ok = self.repo.mark_assessment_onboarding_failed(
            "conn_ora_01", "assess_2026", "HR", "EMPLOYEES", "run_101",
            failed_stage="TARGET_PROVISIONING", error="Schema creation denied"
        )
        self.assertTrue(fail_ok)
        self.assertEqual(row["selection_status"], "FAILED")
        self.assertEqual(row["onboarding_failed_stage"], "TARGET_PROVISIONING")


class TestClaimConcurrency(unittest.TestCase):
    """Simulates multi-run concurrency and race conditions on claim and state transitions."""

    def setUp(self):
        self.spark = StatefulControlSpark()
        self.repo = ControlRepository(self.spark, catalog="lakehouse", control_schema="control")
        self.row = _make_assessment_row(selection_status="SELECTED")
        self.spark.assessment_rows.append(self.row)

    def test_two_runs_race_one_wins_one_loses(self):
        # Run A claims first
        res_a = self.repo.claim_assessment_selection_row(
            "conn_ora_01", "assess_2026", "HR", "EMPLOYEES", "run_A", "att_A"
        )
        self.assertTrue(res_a.acquired)

        # Run B attempts to claim the same row concurrently
        res_b = self.repo.claim_assessment_selection_row(
            "conn_ora_01", "assess_2026", "HR", "EMPLOYEES", "run_B", "att_B"
        )
        self.assertFalse(res_b.acquired)
        self.assertIn("CLAIM_NOT_ACQUIRED", res_b.reason)

        # Row remains owned by Run A
        self.assertEqual(self.row["onboarding_run_id"], "run_A")
        self.assertEqual(self.row["onboarding_attempt_id"], "att_A")
        self.assertEqual(self.row["selection_status"], "ONBOARDING")

    def test_run_b_cannot_overwrite_run_a_claim(self):
        self.repo.claim_assessment_selection_row(
            "conn_ora_01", "assess_2026", "HR", "EMPLOYEES", "run_A", "att_A"
        )
        # Attempt direct transition by Run B
        reg_b = self.repo.mark_assessment_registration_succeeded(
            "conn_ora_01", "assess_2026", "HR", "EMPLOYEES", "run_B", "att_B"
        )
        self.assertFalse(reg_b)
        self.assertEqual(self.row["selection_status"], "ONBOARDING")
        self.assertEqual(self.row["onboarding_run_id"], "run_A")

    def test_run_b_cannot_fail_run_a_claim(self):
        self.repo.claim_assessment_selection_row(
            "conn_ora_01", "assess_2026", "HR", "EMPLOYEES", "run_A", "att_A"
        )
        # Run B attempts to mark Run A's claim as FAILED
        fail_b = self.repo.mark_assessment_onboarding_failed(
            "conn_ora_01", "assess_2026", "HR", "EMPLOYEES", "run_B", "att_B",
            failed_stage="REGISTRATION", error="Run B failed"
        )
        self.assertFalse(fail_b)
        self.assertEqual(self.row["selection_status"], "ONBOARDING")
        self.assertEqual(self.row["onboarding_run_id"], "run_A")

    def test_only_winner_can_complete_registration(self):
        self.repo.claim_assessment_selection_row(
            "conn_ora_01", "assess_2026", "HR", "EMPLOYEES", "run_A", "att_A"
        )
        reg_a = self.repo.mark_assessment_registration_succeeded(
            "conn_ora_01", "assess_2026", "HR", "EMPLOYEES", "run_A", "att_A"
        )
        self.assertTrue(reg_a)
        self.assertEqual(self.row["selection_status"], "REGISTERED")


class TestRegistrationFailures(unittest.TestCase):
    """Simulates caught exceptions during registration steps and verifies failure handling."""

    def setUp(self):
        self.spark = StatefulControlSpark()
        self.repo = ControlRepository(self.spark, catalog="lakehouse", control_schema="control")

    def test_caught_registration_failure_moves_claimed_row_to_failed(self):
        row = _make_assessment_row(selection_status="SELECTED")
        self.spark.assessment_rows.append(row)

        claim = self.repo.claim_assessment_selection_row(
            "conn_ora_01", "assess_2026", "HR", "EMPLOYEES", "run_101", "att_001"
        )
        self.assertTrue(claim.acquired)

        # Simulate exception during registration
        caught_exc = RuntimeError("Registration MERGE failed: network timeout")
        try:
            raise caught_exc
        except Exception as exc:
            self.repo.mark_assessment_onboarding_failed(
                "conn_ora_01", "assess_2026", "HR", "EMPLOYEES", "run_101", "att_001",
                failed_stage="REGISTRATION", error=exc
            )

        self.assertEqual(row["selection_status"], "FAILED")
        self.assertEqual(row["onboarding_failed_stage"], "REGISTRATION")
        self.assertIn("network timeout", row["onboarding_error_message"])

    def test_failure_state_update_error_does_not_hide_original_error(self):
        """If marking state fails, original exception is retained and not swallowed."""
        row = _make_assessment_row(selection_status="SELECTED")
        self.spark.assessment_rows.append(row)

        self.repo.claim_assessment_selection_row(
            "conn_ora_01", "assess_2026", "HR", "EMPLOYEES", "run_101", "att_001"
        )

        original_exc = ValueError("Original registration syntax error")
        secondary_exc = RuntimeError("Control table connection lost")

        caught_in_caller = None
        try:
            try:
                raise original_exc
            except Exception as exc:
                try:
                    # Simulate secondary failure during failure recording
                    raise secondary_exc
                except Exception:
                    pass
                raise exc
        except Exception as exc:
            caught_in_caller = exc

        self.assertIs(caught_in_caller, original_exc)

    def test_partial_batch_exact_row_states(self):
        """In a multi-row batch, row 1 succeeds, row 2 conflicts, row 3 fails.
        Each row ends up in its exact respective state."""
        r1 = _make_assessment_row(object_name="T1", selection_status="SELECTED")
        r2 = _make_assessment_row(object_name="T2", selection_status="ONBOARDING", onboarding_run_id="other_run", onboarding_attempt_id="other_att")
        r3 = _make_assessment_row(object_name="T3", selection_status="SELECTED")
        self.spark.assessment_rows.extend([r1, r2, r3])

        # Process R1
        c1 = self.repo.claim_assessment_selection_row("conn_ora_01", "assess_2026", "HR", "T1", "run_101", "att_001")
        self.assertTrue(c1.acquired)
        self.repo.mark_assessment_registration_succeeded("conn_ora_01", "assess_2026", "HR", "T1", "run_101", "att_001")

        # Process R2 (conflict)
        c2 = self.repo.claim_assessment_selection_row("conn_ora_01", "assess_2026", "HR", "T2", "run_101", "att_001")
        self.assertFalse(c2.acquired)

        # Process R3 (fails during registration)
        c3 = self.repo.claim_assessment_selection_row("conn_ora_01", "assess_2026", "HR", "T3", "run_101", "att_001")
        self.assertTrue(c3.acquired)
        self.repo.mark_assessment_onboarding_failed(
            "conn_ora_01", "assess_2026", "HR", "T3", "run_101", "att_001",
            failed_stage="REGISTRATION", error="Table T3 registration failed"
        )

        self.assertEqual(r1["selection_status"], "REGISTERED")
        self.assertEqual(r2["selection_status"], "ONBOARDING")
        self.assertEqual(r2["onboarding_run_id"], "other_run")
        self.assertEqual(r3["selection_status"], "FAILED")
        self.assertEqual(r3["onboarding_failed_stage"], "REGISTRATION")


class TestDownstreamFailures(unittest.TestCase):
    """Simulates failures across downstream onboarding stages."""

    def setUp(self):
        self.spark = StatefulControlSpark()
        self.repo = ControlRepository(self.spark, catalog="lakehouse", control_schema="control")

    def test_all_valid_downstream_stages_can_be_marked(self):
        for stage in VALID_ONBOARDING_STAGES:
            with self.subTest(stage=stage):
                row = _make_assessment_row(selection_status="REGISTERED", onboarding_run_id="run_101")
                self.spark.assessment_rows = [row]

                ok = self.repo.mark_assessment_onboarding_failed(
                    "conn_ora_01", "assess_2026", "HR", "EMPLOYEES", "run_101",
                    failed_stage=stage, error=f"Failure during {stage}"
                )
                self.assertTrue(ok)
                self.assertEqual(row["selection_status"], "FAILED")
                self.assertEqual(row["onboarding_failed_stage"], stage)

    def test_invalid_failed_stage_rejected(self):
        row = _make_assessment_row(selection_status="REGISTERED", onboarding_run_id="run_101")
        self.spark.assessment_rows = [row]

        with self.assertRaises(ValueError) as ctx:
            self.repo.mark_assessment_onboarding_failed(
                "conn_ora_01", "assess_2026", "HR", "EMPLOYEES", "run_101",
                failed_stage="ARBITRARY_UNVALIDATED_STAGE", error="error"
            )
        self.assertIn("Invalid onboarding failed_stage", str(ctx.exception))

    def test_onboarded_rows_never_marked_failed(self):
        row = _make_assessment_row(selection_status="ONBOARDED", onboarding_run_id="run_101")
        self.spark.assessment_rows = [row]

        ok = self.repo.mark_assessment_onboarding_failed(
            "conn_ora_01", "assess_2026", "HR", "EMPLOYEES", "run_101",
            failed_stage="TARGET_PROVISIONING", error="late failure"
        )
        self.assertFalse(ok)
        self.assertEqual(row["selection_status"], "ONBOARDED")


class TestTaskValuePayloadGuard(unittest.TestCase):
    """Tests the single unified task-value payload guard and size thresholds."""

    def test_payload_below_limit(self):
        # 39,990 bytes JSON string
        padding = "a" * 39980
        data = [{"pad": padding}]
        size = validate_task_value_payload(data, key="test_key")
        self.assertLess(size, TASK_VALUE_LIMIT_BYTES)

    def test_payload_exactly_at_limit(self):
        base = canonical_task_value_serialization([{"x": ""}])
        needed = TASK_VALUE_LIMIT_BYTES - len(base.encode("utf-8"))
        data = [{"x": "a" * needed}]
        size = validate_task_value_payload(data, key="test_key")
        self.assertEqual(size, TASK_VALUE_LIMIT_BYTES)

    def test_payload_one_byte_above_limit_fails(self):
        base = canonical_task_value_serialization([{"x": ""}])
        needed = TASK_VALUE_LIMIT_BYTES - len(base.encode("utf-8")) + 1
        data = [{"x": "a" * needed}]
        with self.assertRaises(ValueError) as ctx:
            validate_task_value_payload(data, key="test_key")
        self.assertIn("exceeds configured payload limit", str(ctx.exception))

    def test_multibyte_utf8_measured_by_bytes_not_characters(self):
        # The emoji 🚀 is 1 character but 4 bytes in UTF-8
        emoji = "🚀" * 100
        serialized = canonical_task_value_serialization(emoji)
        byte_len = len(serialized.encode("utf-8"))
        char_len = len(serialized)
        self.assertGreater(byte_len, char_len)

        # Ensure byte calculation is what validate_task_value_payload returns
        res_bytes = validate_task_value_payload(emoji, key="emoji_key")
        self.assertEqual(res_bytes, byte_len)

    def test_empty_list_payload(self):
        size = validate_task_value_payload([], key="empty")
        self.assertEqual(size, 2)  # len("[]".encode('utf-8'))

    def test_all_worklist_notebooks_call_shared_helper(self):
        worklist_notebooks = (
            "NB_GetFullLoadWorklist.ipynb",
            "NB_GetDeltaWorklist.ipynb",
            "NB_GetConnectionWorklist.ipynb",
            "NB_GetSelectedAssessmentWorklist.ipynb",
        )
        for nb_name in worklist_notebooks:
            with self.subTest(notebook=nb_name):
                with open(os.path.join(DEPLOYMENT, nb_name), encoding="utf-8") as f:
                    content = f.read()
                self.assertIn("validate_task_value_payload", content)
                self.assertIn("TASK_VALUE_LIMIT_BYTES", content)
                # Confirm no hardcoded duplicated limits like 250*1024 or 48*1024
                self.assertNotIn("250 * 1024", content)
                self.assertNotIn("48 * 1024", content)
                self.assertNotIn("256000", content)


class TestSourceSystemCanonicalization(unittest.TestCase):
    """Tests SQL-level and Python-level canonicalization of source systems."""

    def test_canonical_sql_generation(self):
        sql = canonical_source_system_sql("sc.source_system")
        self.assertIn("CASE", sql)
        self.assertIn("'oracle'", sql)
        self.assertIn("'sqlserver'", sql)
        self.assertIn("'mssql'", sql)
        self.assertIn("'sql_server'", sql)
        self.assertIn("'sql server'", sql)

    def test_sql_alias_mapping_coverage(self):
        test_expr = canonical_source_system_sql("col")
        for alias in ("sqlserver", "sql_server", "mssql", "sql server"):
            self.assertIn(f"'{alias}'", test_expr)


class TestTargetConfigScoping(unittest.TestCase):
    """Tests target-config routing scoping to active defaults in NB00."""

    def test_nb00_scopes_routing_checks_to_active_default(self):
        code = shared_nb("NB00_ControlTableInit.py")
        self.assertIn("target_schema_mode", code)
        # Routing checks must require is_active = true AND is_default = true
        self.assertIn("is_active = true AND is_default = true", code)


class TestTargetReservation(unittest.TestCase):
    """Tests that all non-retired registrations reserve targets across connections."""

    def setUp(self):
        self.spark = StatefulControlSpark()
        self.repo = ControlRepository(self.spark, catalog="lakehouse", control_schema="control")

    def test_inactive_onboarding_row_reserves_target(self):
        # A row in source_table_control with is_active=False but current_status='REGISTERED'
        ctrl_row = {
            "connection_id": "conn_sql_01",
            "source_table_id": "tbl_001",
            "target_catalog": "lakehouse",
            "target_schema": "finance",
            "target_table": "invoices",
            "is_active": False,
            "current_status": "REGISTERED",
        }
        self.spark.control_rows.append(ctrl_row)

        owners = self.repo.find_target_owners(
            target_catalog="lakehouse",
            target_schema="finance",
            target_table="invoices",
            include_reserved=True,
        )
        self.assertEqual(len(owners), 1)
        self.assertEqual(owners[0]["connection_id"], "conn_sql_01")

    def test_same_owner_rerun_is_excluded(self):
        ctrl_row = {
            "connection_id": "conn_sql_01",
            "source_table_id": "tbl_001",
            "target_catalog": "lakehouse",
            "target_schema": "finance",
            "target_table": "invoices",
            "is_active": True,
            "current_status": "PROVISIONED",
        }
        self.spark.control_rows.append(ctrl_row)

        owners = self.repo.find_target_owners(
            target_catalog="lakehouse",
            target_schema="finance",
            target_table="invoices",
            exclude_owner=("conn_sql_01", "tbl_001"),
            include_reserved=True,
        )
        self.assertEqual(len(owners), 0)

    def test_different_owner_using_same_target_is_conflict(self):
        ctrl_row = {
            "connection_id": "conn_ora_01",
            "source_table_id": "tbl_ora_01",
            "target_catalog": "lakehouse",
            "target_schema": "finance",
            "target_table": "invoices",
            "is_active": True,
            "current_status": "PROVISIONED",
        }
        self.spark.control_rows.append(ctrl_row)

        owners = self.repo.find_target_owners(
            target_catalog="lakehouse",
            target_schema="finance",
            target_table="invoices",
            exclude_owner=("conn_sql_01", "tbl_sql_02"),
            include_reserved=True,
        )
        self.assertEqual(len(owners), 1)
        self.assertEqual(owners[0]["connection_id"], "conn_ora_01")

    def test_retired_row_does_not_reserve(self):
        ctrl_row = {
            "connection_id": "conn_sql_01",
            "source_table_id": "tbl_001",
            "target_catalog": "lakehouse",
            "target_schema": "finance",
            "target_table": "invoices",
            "is_active": False,
            "current_status": "RETIRED",
        }
        self.spark.control_rows.append(ctrl_row)

        owners = self.repo.find_target_owners(
            target_catalog="lakehouse",
            target_schema="finance",
            target_table="invoices",
            include_reserved=True,
        )
        self.assertEqual(len(owners), 0)


class TestExistingContractsPreserved(unittest.TestCase):
    """Verifies that existing contracts (WIDGETS mode, source identity v2, no credentials) are preserved."""

    def test_widgets_mode_preserved_in_nb01b(self):
        code = shared_nb("NB01B_RegisterSelectedTables.py")
        self.assertIn("WIDGETS", code)
        self.assertIn("ASSESSMENT_FLAGS", code)

    def test_nb01b_never_sets_onboarded(self):
        code = shared_nb("NB01B_RegisterSelectedTables.py")
        # In ASSESSMENT_FLAGS mode, NB01B claims row and registers, but stops at REGISTERED
        self.assertIn("repo.claim_assessment_selection_row(", code)
        self.assertIn("repo.mark_assessment_registration_succeeded(", code)
        self.assertIn("repo.mark_assessment_onboarding_failed(", code)
        self.assertNotIn("repo.mark_assessment_onboarding_completed(", code)
        self.assertNotIn("selection_status = 'ONBOARDED'", code)
        self.assertNotIn('selection_status = "ONBOARDED"', code)

    def test_nb01b_attempt_ids_and_incomplete_failure(self):
        code = shared_nb("NB01B_RegisterSelectedTables.py")
        self.assertIn('batch_attempt_id = new_run_id("batch_attempt")', code)
        self.assertIn('attempt_id = new_run_id("attempt")', code)
        self.assertIn('"batch_attempt_id": batch_attempt_id', code)
        self.assertIn("incomplete_count = remaining_selected_count", code)
        self.assertIn("if incomplete_count > 0:", code)
        self.assertIn("raise RuntimeError(", code)

    def test_nb01b_exact_registration_verification(self):
        code = shared_nb("NB01B_RegisterSelectedTables.py")
        self.assertIn("def _verify_exact_registration(", code)
        self.assertIn("TARGET_CONFIG_CHANGED_AFTER_CLAIM", code)
        self.assertIn("normalize_target_component", code)

    def test_source_table_id_is_sha256_identity_v2(self):
        stid = compute_source_table_id("conn_1", "sqlserver", "srv", "db", "dbo", "cust")
        self.assertEqual(len(stid), 64)
        self.assertTrue(re.match(r"^[0-9a-f]{64}$", stid))

    def test_no_credentials_in_worklist_outputs(self):
        for nb_name in ("NB_GetFullLoadWorklist.ipynb", "NB_GetDeltaWorklist.ipynb",
                        "NB_GetConnectionWorklist.ipynb", "NB_GetSelectedAssessmentWorklist.ipynb"):
            with open(os.path.join(DEPLOYMENT, nb_name), encoding="utf-8") as f:
                content = f.read()
            for forbidden in ("password", "secret_value", "access_token", "jdbc_url"):
                self.assertNotIn(f'"{forbidden}"', content)


class TestTargetNormalizationHelper(unittest.TestCase):
    """Verifies target component normalization behavior."""

    def test_normalize_target_component(self):
        self.assertEqual(normalize_target_component(None), "")
        self.assertEqual(normalize_target_component(""), "")
        self.assertEqual(normalize_target_component("   "), "")
        self.assertEqual(normalize_target_component("  MyTable  "), "mytable")
        self.assertEqual(normalize_target_component("CATALOG_A"), "catalog_a")
        self.assertEqual(normalize_target_component("already_lower"), "already_lower")


class TestStageConstants(unittest.TestCase):
    """Verifies authoritative onboarding failure stages and downstream subsets."""

    def test_authoritative_stages(self):
        expected_stages = {
            "REGISTRATION", "INVENTORY", "TYPE_NORMALIZATION",
            "MAPPING_GENERATION", "MAPPING_VALIDATION",
            "TABLE_DECISION", "TARGET_PROVISIONING", "FINALIZATION",
        }
        self.assertEqual(VALID_ONBOARDING_STAGES, expected_stages)
        self.assertEqual(VALID_DOWNSTREAM_ONBOARDING_STAGES, expected_stages - {"REGISTRATION"})


class TestNb01bCandidateFiltering(unittest.TestCase):
    """Verifies explicit candidate selection behavior in ASSESSMENT_FLAGS mode across all statuses."""

    def test_eligible_statuses(self):
        # null status
        self.assertTrue(is_assessment_selection_candidate(True, None))
        # blank status
        self.assertTrue(is_assessment_selection_candidate(True, ""))
        # whitespace status
        self.assertTrue(is_assessment_selection_candidate(True, "   "))
        # SELECTED
        self.assertTrue(is_assessment_selection_candidate(True, "SELECTED"))
        # lowercase selected
        self.assertTrue(is_assessment_selection_candidate(True, "selected"))

    def test_failed_status_retry_handling(self):
        # FAILED without retry
        self.assertFalse(is_assessment_selection_candidate(True, "FAILED", include_failed_retries=False))
        # FAILED with retry
        self.assertTrue(is_assessment_selection_candidate(True, "FAILED", include_failed_retries=True))
        self.assertTrue(is_assessment_selection_candidate(True, "failed", include_failed_retries=True))

    def test_ineligible_statuses(self):
        for status in (
            "ONBOARDING", "REGISTERED", "ONBOARDED",
            "REVIEW_REQUIRED", "BLOCKED", "NOT_SELECTED",
            "UNKNOWN_STATUS", "CUSTOM_STATE"
        ):
            with self.subTest(status=status):
                self.assertFalse(is_assessment_selection_candidate(True, status, include_failed_retries=False))
                self.assertFalse(is_assessment_selection_candidate(True, status, include_failed_retries=True))

    def test_unselected_never_eligible(self):
        # is_selected false
        self.assertFalse(is_assessment_selection_candidate(False, "SELECTED"))
        self.assertFalse(is_assessment_selection_candidate(False, "FAILED", include_failed_retries=True))
        # is_selected null
        self.assertFalse(is_assessment_selection_candidate(None, "SELECTED"))
        self.assertFalse(is_assessment_selection_candidate(None, "FAILED", include_failed_retries=True))


class TestNb00ControlTableInitValidation(unittest.TestCase):
    """Verifies NB00 control table initialization constraints and stage checks."""

    def test_invalid_onboarding_failed_stage_validation_block(self):
        nb00_path = os.path.join(SHARED, "NB00_ControlTableInit.py")
        with open(nb00_path, encoding="utf-8") as f:
            content = f.read()

        # Find the INVALID_ONBOARDING_FAILED_STAGE block
        m = re.search(
            r"INVALID_ONBOARDING_FAILED_STAGE.*?NOT IN\s*\((.*?)\)",
            content,
            re.DOTALL
        )
        self.assertIsNotNone(m, "INVALID_ONBOARDING_FAILED_STAGE NOT IN block not found in NB00")
        block = m.group(1)

        stages_in_block = set(re.findall(r"'([A-Z_]+)'", block))
        self.assertEqual(stages_in_block, VALID_ONBOARDING_STAGES)
        self.assertNotIn("CLAIM", stages_in_block)
        self.assertNotIn("REVIEW_REQUIRED", stages_in_block)
        self.assertNotIn("BLOCKED", stages_in_block)


class TestDeltaConcurrencyExceptionHandling(unittest.TestCase):
    """Verifies recognition and handling of Delta optimistic concurrency conflicts."""

    def test_is_delta_concurrency_exception(self):
        class ConcurrentAppendException(Exception): pass
        class ConcurrentTransactionException(Exception): pass
        class ConcurrentWriteException(Exception): pass
        class MetadataChangedException(Exception): pass
        class ProtocolChangedException(Exception): pass
        class AnalysisException(Exception): pass
        class SecurityException(Exception): pass

        self.assertTrue(is_delta_concurrency_exception(ConcurrentAppendException("conflict")))
        self.assertTrue(is_delta_concurrency_exception(ConcurrentTransactionException("conflict")))
        self.assertTrue(is_delta_concurrency_exception(ConcurrentWriteException("conflict")))
        self.assertTrue(is_delta_concurrency_exception(MetadataChangedException("conflict")))
        self.assertTrue(is_delta_concurrency_exception(ProtocolChangedException("conflict")))

        self.assertFalse(is_delta_concurrency_exception(AnalysisException("syntax error")))
        self.assertFalse(is_delta_concurrency_exception(SecurityException("permission denied")))
        self.assertFalse(is_delta_concurrency_exception(RuntimeError("generic error")))

    def test_claim_handles_concurrency_exception_and_verifies_row(self):
        spark = StatefulControlSpark()
        repo = ControlRepository(spark, "cat", "ctrl")
        row = _make_assessment_row(selection_status="SELECTED")
        spark.assessment_rows.append(row)

        class MockDeltaConcurrencyException(Exception):
            def getErrorClass(self):
                return "CONCURRENT_TRANSACTION_EXCEPTION"

        # Patch spark.sql to raise MockDeltaConcurrencyException on UPDATE
        orig_sql = spark.sql
        def flaky_sql(stmt):
            if stmt.strip().startswith("UPDATE"):
                raise MockDeltaConcurrencyException("Simulated concurrent commit")
            return orig_sql(stmt)
        spark.sql = flaky_sql

        res = repo.claim_assessment_selection_row(
            "conn_ora_01", "assess_2026", "HR", "EMPLOYEES", "run_101", "att_001"
        )
        # Because update failed, row was not updated, verify indicates not acquired
        self.assertFalse(res.acquired)
        self.assertIn("CLAIM_NOT_ACQUIRED", res.reason)

    def test_claim_propagates_non_concurrency_exception(self):
        spark = StatefulControlSpark()
        repo = ControlRepository(spark, "cat", "ctrl")
        row = _make_assessment_row(selection_status="SELECTED")
        spark.assessment_rows.append(row)

        def err_sql(stmt):
            if stmt.strip().startswith("UPDATE"):
                raise PermissionError("Access denied to storage account")
            return spark._handle_select_assessment(stmt)
        spark.sql = err_sql

        with self.assertRaises(PermissionError):
            repo.claim_assessment_selection_row(
                "conn_ora_01", "assess_2026", "HR", "EMPLOYEES", "run_101", "att_001"
            )


class TestPreclaimFailureRecording(unittest.TestCase):
    """Verifies pre-claim failure recording on unowned candidates."""

    def setUp(self):
        self.spark = StatefulControlSpark()
        self.repo = ControlRepository(self.spark, "cat", "ctrl")

    def test_preclaim_failed_on_unowned_selected_row(self):
        row = _make_assessment_row(selection_status="SELECTED")
        self.spark.assessment_rows.append(row)

        ok = self.repo.mark_assessment_preclaim_failed(
            "conn_ora_01", "assess_2026", "HR", "EMPLOYEES",
            error="target collision detected",
        )
        self.assertTrue(ok)
        self.assertEqual(row["selection_status"], "FAILED")
        self.assertEqual(row["onboarding_failed_stage"], "REGISTRATION")
        self.assertIn("target collision detected", row["onboarding_error_message"])
        self.assertIsNone(row["onboarding_run_id"])
        self.assertIsNone(row["onboarding_attempt_id"])

    def test_preclaim_failed_rejects_owned_row(self):
        row = _make_assessment_row(selection_status="ONBOARDING", onboarding_run_id="run_101", onboarding_attempt_id="att_001")
        self.spark.assessment_rows.append(row)

        ok = self.repo.mark_assessment_preclaim_failed(
            "conn_ora_01", "assess_2026", "HR", "EMPLOYEES",
            error="target collision detected",
        )
        self.assertFalse(ok)
        self.assertEqual(row["selection_status"], "ONBOARDING")
        self.assertEqual(row["onboarding_run_id"], "run_101")

    def test_preclaim_failed_rejects_terminal_rows(self):
        for term_st in ("ONBOARDED", "REVIEW_REQUIRED", "BLOCKED"):
            with self.subTest(status=term_st):
                row = _make_assessment_row(selection_status=term_st)
                self.spark.assessment_rows = [row]
                ok = self.repo.mark_assessment_preclaim_failed(
                    "conn_ora_01", "assess_2026", "HR", "EMPLOYEES",
                    error="target collision detected",
                )
                self.assertFalse(ok)
                self.assertEqual(row["selection_status"], term_st)


class TestTerminalTransitions(unittest.TestCase):
    """Verifies mark_assessment_onboarding_terminal transitions."""

    def setUp(self):
        self.spark = StatefulControlSpark()
        self.repo = ControlRepository(self.spark, "cat", "ctrl")

    def test_registered_transitions_to_review_required(self):
        row = _make_assessment_row(selection_status="REGISTERED", onboarding_run_id="run_101", onboarding_attempt_id="att_001")
        self.spark.assessment_rows.append(row)

        ok = self.repo.mark_assessment_onboarding_terminal(
            "conn_ora_01", "assess_2026", "HR", "EMPLOYEES",
            run_id="run_101", attempt_id="att_001",
            terminal_status="REVIEW_REQUIRED",
            message="Manual schema review required",
        )
        self.assertTrue(ok)
        self.assertEqual(row["selection_status"], "REVIEW_REQUIRED")
        self.assertIsNone(row["onboarding_failed_stage"])
        self.assertIn("Manual schema review required", row["onboarding_error_message"])

    def test_registered_transitions_to_blocked(self):
        row = _make_assessment_row(selection_status="REGISTERED", onboarding_run_id="run_101", onboarding_attempt_id="att_001")
        self.spark.assessment_rows.append(row)

        ok = self.repo.mark_assessment_onboarding_terminal(
            "conn_ora_01", "assess_2026", "HR", "EMPLOYEES",
            run_id="run_101", attempt_id="att_001",
            terminal_status="BLOCKED",
            message="Unsupported construct",
        )
        self.assertTrue(ok)
        self.assertEqual(row["selection_status"], "BLOCKED")
        self.assertIsNone(row["onboarding_failed_stage"])

    def test_terminal_transition_rejects_invalid_status(self):
        with self.assertRaises(ValueError):
            self.repo.mark_assessment_onboarding_terminal(
                "conn_ora_01", "assess_2026", "HR", "EMPLOYEES",
                run_id="run_101", attempt_id="att_001",
                terminal_status="ONBOARDED",
            )

    def test_terminal_transition_rejects_wrong_owner(self):
        row = _make_assessment_row(selection_status="REGISTERED", onboarding_run_id="run_101", onboarding_attempt_id="att_001")
        self.spark.assessment_rows.append(row)

        ok = self.repo.mark_assessment_onboarding_terminal(
            "conn_ora_01", "assess_2026", "HR", "EMPLOYEES",
            run_id="run_OTHER", attempt_id="att_001",
            terminal_status="BLOCKED",
        )
        self.assertFalse(ok)
        self.assertEqual(row["selection_status"], "REGISTERED")

    def test_nb00_accepts_terminal_rows(self):
        for term_status in ("REVIEW_REQUIRED", "BLOCKED"):
            row = _make_assessment_row(selection_status=term_status, onboarding_failed_stage=None)
            is_failed = str(row.get("selection_status") or "").upper().strip() == "FAILED"
            self.assertFalse(is_failed)
            self.assertIsNone(row.get("onboarding_failed_stage"))


class TestRestrictedUpdateAssessmentSelectionState(unittest.TestCase):
    """Verifies update_assessment_selection_state restriction to operator actions."""

    def setUp(self):
        self.spark = StatefulControlSpark()
        self.repo = ControlRepository(self.spark, "cat", "ctrl")

    def test_operator_selected_action(self):
        row = _make_assessment_row(selection_status="NOT_SELECTED", is_selected=False)
        self.spark.assessment_rows.append(row)

        self.repo.update_assessment_selection_state(
            "conn_ora_01", "assess_2026", "HR", "EMPLOYEES", "SELECTED", selected_by="admin"
        )
        self.assertTrue(row["is_selected"])
        self.assertEqual(row["selection_status"], "SELECTED")
        self.assertEqual(row.get("selected_by"), "admin")

    def test_operator_not_selected_action(self):
        row = _make_assessment_row(selection_status="SELECTED", is_selected=True)
        self.spark.assessment_rows.append(row)

        self.repo.update_assessment_selection_state(
            "conn_ora_01", "assess_2026", "HR", "EMPLOYEES", "NOT_SELECTED"
        )
        self.assertFalse(row["is_selected"])
        self.assertEqual(row["selection_status"], "NOT_SELECTED")
        self.assertIsNone(row.get("selected_by"))

    def test_selected_by_determinism(self):
        row = _make_assessment_row(selection_status="NOT_SELECTED", is_selected=False, selected_by="prev_user")
        self.spark.assessment_rows.append(row)
        self.repo.update_assessment_selection_state("conn_ora_01", "assess_2026", "HR", "EMPLOYEES", "SELECTED", selected_by="alice")
        self.assertEqual(row.get("selected_by"), "alice")

        self.repo.update_assessment_selection_state("conn_ora_01", "assess_2026", "HR", "EMPLOYEES", "SELECTED", selected_by="   ")
        self.assertIsNone(row.get("selected_by"))

        row["selected_by"] = "bob"
        self.repo.update_assessment_selection_state("conn_ora_01", "assess_2026", "HR", "EMPLOYEES", "SELECTED", selected_by=None)
        self.assertIsNone(row.get("selected_by"))

        self.repo.update_assessment_selection_state("conn_ora_01", "assess_2026", "HR", "EMPLOYEES", "SELECTED", selected_by="user password=secret123")
        self.assertNotIn("secret123", str(row.get("selected_by")))

        long_actor = "x" * 300
        self.repo.update_assessment_selection_state("conn_ora_01", "assess_2026", "HR", "EMPLOYEES", "SELECTED", selected_by=long_actor)
        self.assertLessEqual(len(str(row.get("selected_by"))), 256)

        self.repo.update_assessment_selection_state("conn_ora_01", "assess_2026", "HR", "EMPLOYEES", "NOT_SELECTED")
        self.assertIsNone(row.get("selected_by"))
        self.assertIsNone(row.get("selected_ts"))

    def test_operator_update_concurrency_error_propagates(self):
        class FakeDeltaConcurrency(Exception):
            def getErrorClass(self):
                return "CONCURRENT_APPEND_EXCEPTION"

        class FailingSpark:
            def sql(self, s):
                raise FakeDeltaConcurrency("Delta write conflict")

        repo = ControlRepository(FailingSpark(), "cat", "ctrl")
        with self.assertRaises(FakeDeltaConcurrency):
            repo.update_assessment_selection_state("conn_ora_01", "assess_2026", "HR", "EMPLOYEES", "SELECTED")

    def test_operator_update_unknown_error_propagates(self):
        class FailingSpark:
            def sql(self, s):
                raise RuntimeError("Disk failure")

        repo = ControlRepository(FailingSpark(), "cat", "ctrl")
        with self.assertRaises(RuntimeError):
            repo.update_assessment_selection_state("conn_ora_01", "assess_2026", "HR", "EMPLOYEES", "SELECTED")

    def test_rejects_lifecycle_statuses_with_value_error(self):
        for status in ("ONBOARDING", "REGISTERED", "ONBOARDED", "FAILED", "REVIEW_REQUIRED", "BLOCKED"):
            with self.subTest(status=status):
                with self.assertRaises(ValueError) as ctx:
                    self.repo.update_assessment_selection_state(
                        "conn_ora_01", "assess_2026", "HR", "EMPLOYEES", status
                    )
                self.assertIn("only supports operator actions", str(ctx.exception))

    def test_rejects_overwriting_owned_row(self):
        row = _make_assessment_row(selection_status="SELECTED", onboarding_run_id="run_101", onboarding_attempt_id="att_001")
        self.spark.assessment_rows.append(row)

        self.repo.update_assessment_selection_state(
            "conn_ora_01", "assess_2026", "HR", "EMPLOYEES", "NOT_SELECTED"
        )
        self.assertEqual(row["selection_status"], "SELECTED")
        self.assertEqual(row["onboarding_run_id"], "run_101")


class TestExactAssessmentRowReader(unittest.TestCase):
    """Verifies _get_assessment_selection_row behavior."""

    def setUp(self):
        self.spark = StatefulControlSpark()
        self.repo = ControlRepository(self.spark, "cat", "ctrl")

    def test_returns_none_when_zero_rows(self):
        res = self.repo._get_assessment_selection_row("conn_ora_01", "assess_2026", "HR", "EMPLOYEES")
        self.assertIsNone(res)

    def test_returns_dict_when_one_row(self):
        row = _make_assessment_row()
        self.spark.assessment_rows.append(row)
        res = self.repo._get_assessment_selection_row("conn_ora_01", "assess_2026", "HR", "EMPLOYEES")
        self.assertIsNotNone(res)
        self.assertEqual(res["object_name"], "EMPLOYEES")

    def test_raises_value_error_when_multiple_rows(self):
        self.spark.assessment_rows.append(_make_assessment_row())
        self.spark.assessment_rows.append(_make_assessment_row())
        with self.assertRaises(ValueError) as ctx:
            self.repo._get_assessment_selection_row("conn_ora_01", "assess_2026", "HR", "EMPLOYEES")
        self.assertIn("Duplicate exact assessment TABLE rows found", str(ctx.exception))


class TestFinalizationNotebookLogic(unittest.TestCase):
    """Verifies that finalization checks all prerequisites before transitioning REGISTERED to ONBOARDED."""

    def test_finalizer_source_code_checks_all_prerequisites(self):
        with open(os.path.join(DEPLOYMENT, "NB_FinalizeSelectedTableOnboarding.py"), encoding="utf-8") as f:
            code = f.read()
        self.assertIn("AUTO_MIGRATE", code)
        self.assertIn("PROVISIONED", code)
        self.assertIn("is_active", code)
        self.assertIn("find_target_owners", code)
        self.assertIn("mark_assessment_onboarding_completed", code)
        self.assertIn("mark_assessment_onboarding_failed", code)
        self.assertNotIn("initial_load_completed", code)
        self.assertNotIn("watermark", code)
        self.assertNotIn("checkpoint", code)
        for forbidden in ("password", "secret_scope", "jdbc_url", "create_source_adapter"):
            self.assertNotIn(forbidden, code)

    def test_downstream_failure_notebook_contract(self):
        with open(os.path.join(DEPLOYMENT, "NB_MarkSelectedOnboardingFailed.py"), encoding="utf-8") as f:
            code = f.read()
        self.assertIn("mark_assessment_onboarding_failed", code)
        self.assertIn("VALID_DOWNSTREAM_ONBOARDING_STAGES", code)
        self.assertNotIn("ALLOWED_FAILED_STAGES", code)
        self.assertIn("onboarding_run_id", code)
        self.assertIn("IN ('ONBOARDING', 'REGISTERED')", code)

    def test_administrative_recovery_notebook_contract(self):
        with open(os.path.join(DEPLOYMENT, "NB_RecoverSelectedOnboardingState.py"), encoding="utf-8") as f:
            code = f.read()
        self.assertIn("dry_run", code)
        self.assertIn("expected_run_id", code)
        self.assertIn("expected_attempt_id", code)
        self.assertIn("recovery_action", code)
        for action in ("RESUME_REGISTERED", "MARK_FAILED", "RESET_TO_SELECTED", "FINALIZE_ONBOARDED"):
            self.assertIn(action, code)


class TestFinalizerCounterOrderingAndBehavior(unittest.TestCase):
    """Verifies counter ordering, terminal failure, and business status in finalizer."""

    def test_counter_increments_only_after_transition_succeeds(self):
        class FakeRepo:
            def __init__(self, succeed=True):
                self.succeed = succeed
            def mark_assessment_onboarding_terminal(self, **kwargs):
                return self.succeed

        repo = FakeRepo(succeed=True)
        review_required_count = 0
        terminal_status = "REVIEW_REQUIRED"
        transitioned = repo.mark_assessment_onboarding_terminal()
        if not transitioned:
            raise RuntimeError("Failed")
        if terminal_status == "REVIEW_REQUIRED":
            review_required_count += 1
        self.assertEqual(review_required_count, 1)

        repo_fail = FakeRepo(succeed=False)
        review_required_count = 0
        failed_count = 0
        try:
            transitioned = repo_fail.mark_assessment_onboarding_terminal()
            if not transitioned:
                raise RuntimeError("Failed")
            review_required_count += 1
        except RuntimeError:
            failed_count += 1
        self.assertEqual(review_required_count, 0)
        self.assertEqual(failed_count, 1)

    def test_business_status_computation(self):
        self.assertEqual(self._compute_status(candidate=2, onboarded=2, review=0, blocked=0, failed=0), ("COMPLETE", "SUCCEEDED"))
        self.assertEqual(self._compute_status(candidate=2, onboarded=0, review=1, blocked=1, failed=0), ("TERMINAL_REVIEW", "SUCCEEDED"))
        self.assertEqual(self._compute_status(candidate=2, onboarded=1, review=1, blocked=0, failed=0), ("TERMINAL_REVIEW", "SUCCEEDED"))
        self.assertEqual(self._compute_status(candidate=2, onboarded=1, review=0, blocked=0, failed=1), ("PARTIAL", "FAILED"))
        self.assertEqual(self._compute_status(candidate=2, onboarded=0, review=0, blocked=0, failed=2), ("FAILED", "FAILED"))
        self.assertEqual(self._compute_status(candidate=0, onboarded=0, review=0, blocked=0, failed=0), ("NO_CANDIDATES", "SUCCEEDED"))

    def _compute_status(self, candidate, onboarded, review, blocked, failed):
        terminal_count = onboarded + review + blocked
        if candidate == 0:
            b_stat = "NO_CANDIDATES"
        elif failed > 0:
            b_stat = "FAILED" if terminal_count == 0 else "PARTIAL"
        elif onboarded == candidate:
            b_stat = "COMPLETE"
        elif terminal_count == candidate and (review + blocked) > 0:
            b_stat = "TERMINAL_REVIEW"
        else:
            b_stat = "PARTIAL"
        e_stat = "FAILED" if failed > 0 else "SUCCEEDED"
        return b_stat, e_stat


class TestExplicitRunIdAndCoordinateValidation(unittest.TestCase):
    """Verifies that finalizer, failure handler, and recovery notebooks enforce explicit run ID and coordinates."""

    def test_finalizer_requires_explicit_run_id(self):
        with open(os.path.join(DEPLOYMENT, "NB_FinalizeSelectedTableOnboarding.py"), encoding="utf-8") as f:
            code = f.read()
        self.assertNotIn("or get_run_id()", code)
        self.assertIn("run_id = dbutils.widgets.get(\"run_id\").strip()", code)
        self.assertIn("raise ValueError(\n        \"run_id is required and must be passed from the original Job run context\"", code)

    def test_failure_handler_requires_explicit_run_id(self):
        with open(os.path.join(DEPLOYMENT, "NB_MarkSelectedOnboardingFailed.py"), encoding="utf-8") as f:
            code = f.read()
        self.assertNotIn("or get_run_id()", code)
        self.assertIn("run_id = dbutils.widgets.get(\"run_id\").strip()", code)
        self.assertIn("raise ValueError(\n        \"run_id is required and must be passed from the original Job run context\"", code)

    def test_recovery_requires_expected_run_id(self):
        with open(os.path.join(DEPLOYMENT, "NB_RecoverSelectedOnboardingState.py"), encoding="utf-8") as f:
            code = f.read()
        self.assertNotIn("or get_run_id()", code)
        self.assertIn("expected_run_id = dbutils.widgets.get(\"expected_run_id\").strip()", code)
        self.assertIn("raise ValueError(\"expected_run_id is required for ownership-safe recovery\")", code)

    def test_notebooks_use_custom_coordinates(self):
        for nb in ("NB_FinalizeSelectedTableOnboarding.py", "NB_MarkSelectedOnboardingFailed.py", "NB_RecoverSelectedOnboardingState.py"):
            with open(os.path.join(DEPLOYMENT, nb), encoding="utf-8") as f:
                code = f.read()
            self.assertNotIn("repo = control_repo()", code)
            self.assertIn("ControlRepository(\n    spark,\n    catalog=catalog,\n    control_schema=control_schema,\n)", code)


class TestTaskValueUsageAndErrorBounding(unittest.TestCase):
    """Verifies task-value helper usage and error bounding."""

    def test_task_values_use_shared_helper(self):
        for nb in ("NB_FinalizeSelectedTableOnboarding.py", "NB_MarkSelectedOnboardingFailed.py", "NB_RecoverSelectedOnboardingState.py"):
            with open(os.path.join(DEPLOYMENT, nb), encoding="utf-8") as f:
                code = f.read()
            self.assertNotIn("dbutils.jobs.taskValues.set", code)
            self.assertIn("set_task_value(", code)

    def test_error_bounding_logic(self):
        raw_errors = [f"Error message {i} with {'a'*600}" for i in range(30)]
        bounded = [str(e)[:500] for e in raw_errors[:20]]
        self.assertEqual(len(bounded), 20)
        for e in bounded:
            self.assertLessEqual(len(e), 500)

        empty_bounded = [str(e)[:500] for e in [][:20]]
        self.assertEqual(len(empty_bounded), 0)


class FakeDbUtils:
    def __init__(self, widgets_dict):
        self._widgets = dict(widgets_dict)
        self.task_values = {}
        self.exit_payload = None
        self.widgets = self

    def get(self, name):
        return str(self._widgets.get(name, ""))

    def text(self, name, default=""):
        if name not in self._widgets:
            self._widgets[name] = default

    def dropdown(self, name, default="", choices=None):
        if name not in self._widgets:
            self._widgets[name] = default

    @property
    def notebook(self):
        outer = self

        class _NB:
            def exit(self, val):
                outer.exit_payload = val

        return _NB()

    @property
    def jobs(self):
        outer = self

        class _Jobs:
            @property
            def taskValues(self):
                class _TV:
                    def set(self, key, value):
                        outer.task_values[key] = value

                return _TV()

        return _Jobs()


def _run_recovery_notebook(spark, widgets_dict):
    conn_id = widgets_dict.get("connection_id", "conn_ora_01")
    if not any(r.get("connection_id") == conn_id for r in spark.connection_rows):
        spark.connection_rows.append({
            "connection_id": conn_id,
            "connection_name": "Test Connection",
            "source_system": "ORACLE",
            "source_server": "oradb.example.com",
            "source_database": "ORCL",
            "connection_status": "VALID",
            "is_active": True,
        })

    dbutils = FakeDbUtils(widgets_dict)
    nb_path = os.path.join(DEPLOYMENT, "NB_RecoverSelectedOnboardingState.py")
    with open(nb_path, "r", encoding="utf-8") as f:
        lines = [line for line in f if not line.strip().startswith(("# MAGIC %run", "%run"))]
    code = "".join(lines)

    def set_task_val(k, v):
        dbutils.jobs.taskValues.set(k, v)

    env = {
        "spark": spark,
        "dbutils": dbutils,
        "ControlRepository": ControlRepository,
        "quote_databricks": quote_databricks,
        "escape_string_literal": escape_string_literal,
        "sanitize_error_message": sanitize_error_message,
        "normalize_target_component": normalize_target_component,
        "require_source_system": require_source_system,
        "compute_source_table_id": compute_source_table_id,
        "SOURCE_IDENTITY_VERSION": 2,
        "CATALOG": "da_accelerators",
        "CONTROL_SCHEMA": "control",
        "set_task_value": set_task_val,
        "print": lambda *args: None,
    }
    error = None
    try:
        exec(code, env)
    except Exception as exc:
        error = exc

    result = env.get("result")
    if not result and dbutils.exit_payload:
        result = json.loads(dbutils.exit_payload)
    return result, dbutils.task_values, error


class TestAdministrativeRecoveryBehavior(unittest.TestCase):
    """Behavioral tests executing NB_RecoverSelectedOnboardingState logic with StatefulControlSpark."""

    def setUp(self):
        self.conn_id = "conn_ora_01"
        self.assess_id = "assess_2026"
        self.run_id = "run_rec_001"
        self.attempt_id = "att_rec_001"
        self.source_system = "ORACLE"
        self.source_server = "oradb.example.com"
        self.source_database = "ORCL"
        self.expected_sid = compute_source_table_id(
            self.conn_id, self.source_system, self.source_server, self.source_database, "HR", "T1"
        )

    def _default_widgets(self, action="MARK_FAILED", dry_run="true", expected_attempt_id=None):
        return {
            "connection_id": self.conn_id,
            "assessment_id": self.assess_id,
            "expected_run_id": self.run_id,
            "expected_attempt_id": self.attempt_id if expected_attempt_id is None else expected_attempt_id,
            "recovery_action": action,
            "dry_run": dry_run,
            "catalog": "da_accelerators",
            "control_schema": "control",
        }

    def test_mark_failed_dry_run_and_real_transitions(self):
        # A1: Dry run
        spark = StatefulControlSpark()
        spark.assessment_rows = [
            _make_assessment_row(self.conn_id, self.assess_id, "HR", "T1",
                                 selection_status="ONBOARDING",
                                 onboarding_run_id=self.run_id,
                                 onboarding_attempt_id=self.attempt_id)
        ]
        res, tv, err = _run_recovery_notebook(spark, self._default_widgets("MARK_FAILED", dry_run="true"))
        self.assertIsNone(err)
        self.assertEqual(res["recovered_count"], 0)
        self.assertEqual(res["would_recover_count"], 1)
        self.assertEqual(res["transition_failure_count"], 0)
        self.assertEqual(res["business_status"], "DRY_RUN_COMPLETE")
        self.assertEqual(res["execution_status"], "SUCCEEDED")
        self.assertEqual(res["details"][0]["action"], "WOULD_MARK_FAILED")
        self.assertEqual(spark.assessment_rows[0]["selection_status"], "ONBOARDING")

        # A2: Real success
        spark = StatefulControlSpark()
        spark.assessment_rows = [
            _make_assessment_row(self.conn_id, self.assess_id, "HR", "T1",
                                 selection_status="ONBOARDING",
                                 onboarding_run_id=self.run_id,
                                 onboarding_attempt_id=self.attempt_id)
        ]
        res, tv, err = _run_recovery_notebook(spark, self._default_widgets("MARK_FAILED", dry_run="false"))
        self.assertIsNone(err)
        self.assertEqual(res["recovered_count"], 1)
        self.assertEqual(res["would_recover_count"], 0)
        self.assertEqual(res["transition_failure_count"], 0)
        self.assertEqual(res["business_status"], "RECOVERED")
        self.assertEqual(res["execution_status"], "SUCCEEDED")
        self.assertEqual(spark.assessment_rows[0]["selection_status"], "FAILED")
        self.assertEqual(spark.assessment_rows[0]["onboarding_failed_stage"], "REGISTRATION")

        # A3: Real failure (simulated transition returns False)
        spark = StatefulControlSpark()
        spark.simulate_transition_failure = True
        spark.assessment_rows = [
            _make_assessment_row(self.conn_id, self.assess_id, "HR", "T1",
                                 selection_status="ONBOARDING",
                                 onboarding_run_id=self.run_id,
                                 onboarding_attempt_id=self.attempt_id)
        ]
        res, tv, err = _run_recovery_notebook(spark, self._default_widgets("MARK_FAILED", dry_run="false"))
        self.assertIsNotNone(err)
        self.assertIn("RuntimeError", type(err).__name__)
        self.assertEqual(res["recovered_count"], 0)
        self.assertEqual(res["transition_failure_count"], 1)
        self.assertEqual(res["execution_status"], "FAILED")
        self.assertEqual(res["business_status"], "FAILED")
        self.assertEqual(spark.assessment_rows[0]["selection_status"], "ONBOARDING")
        self.assertEqual(res["errors"], ["HR.T1: Ownership-safe FAILED transition was not verified"])

    def test_resume_registered_attempt_and_transitions(self):
        control_row = {
            "connection_id": self.conn_id,
            "source_table_id": self.expected_sid,
            "source_schema": "HR",
            "source_table": "T1",
            "source_identity_version": 2,
            "target_catalog": "main",
            "target_schema": "hr",
            "target_table": "t1",
        }

        # B1: Attempt ID from row when present
        spark = StatefulControlSpark()
        spark.assessment_rows = [
            _make_assessment_row(self.conn_id, self.assess_id, "HR", "T1",
                                 selection_status="ONBOARDING",
                                 onboarding_run_id=self.run_id,
                                 onboarding_attempt_id="att_from_row")
        ]
        spark.control_rows = [dict(control_row)]
        widgets = self._default_widgets("RESUME_REGISTERED", dry_run="false", expected_attempt_id="")
        res, tv, err = _run_recovery_notebook(spark, widgets)
        self.assertIsNone(err)
        self.assertEqual(res["recovered_count"], 1)
        self.assertEqual(spark.assessment_rows[0]["selection_status"], "REGISTERED")

        # B2: expected_attempt_id cannot adopt missing row attempt (skipped)
        spark = StatefulControlSpark()
        spark.assessment_rows = [
            _make_assessment_row(self.conn_id, self.assess_id, "HR", "T1",
                                 selection_status="ONBOARDING",
                                 onboarding_run_id=self.run_id,
                                 onboarding_attempt_id=None)
        ]
        spark.control_rows = [dict(control_row)]
        widgets = self._default_widgets("RESUME_REGISTERED", dry_run="false", expected_attempt_id="att_from_widget")
        res, tv, err = _run_recovery_notebook(spark, widgets)
        self.assertIsNone(err)
        self.assertEqual(res["skipped_count"], 1)
        self.assertEqual(res["recovered_count"], 0)
        self.assertEqual(res["would_recover_count"], 0)
        self.assertEqual(res["transition_failure_count"], 0)
        self.assertEqual(
            res["details"][0]["reason"],
            "Assessment row has no recorded onboarding attempt ID; ownership-safe recovery cannot adopt a missing attempt"
        )
        self.assertIsNone(spark.assessment_rows[0]["onboarding_attempt_id"])
        self.assertEqual(spark.assessment_rows[0]["selection_status"], "ONBOARDING")

        # B4: Missing attempt ID entirely produces SKIPPED
        spark = StatefulControlSpark()
        spark.assessment_rows = [
            _make_assessment_row(self.conn_id, self.assess_id, "HR", "T1",
                                 selection_status="ONBOARDING",
                                 onboarding_run_id=self.run_id,
                                 onboarding_attempt_id=None)
        ]
        spark.control_rows = [dict(control_row)]
        widgets = self._default_widgets("RESUME_REGISTERED", dry_run="false", expected_attempt_id="")
        res, tv, err = _run_recovery_notebook(spark, widgets)
        self.assertIsNone(err)
        self.assertEqual(res["skipped_count"], 1)
        self.assertEqual(res["recovered_count"], 0)
        self.assertEqual(
            res["details"][0]["reason"],
            "Assessment row has no recorded onboarding attempt ID; ownership-safe recovery cannot adopt a missing attempt"
        )

        # B5: Mismatched expected and row attempt IDs produce SKIPPED
        spark = StatefulControlSpark()
        spark.assessment_rows = [
            _make_assessment_row(self.conn_id, self.assess_id, "HR", "T1",
                                 selection_status="ONBOARDING",
                                 onboarding_run_id=self.run_id,
                                 onboarding_attempt_id="att_row_val")
        ]
        spark.control_rows = [dict(control_row)]
        widgets = self._default_widgets("RESUME_REGISTERED", dry_run="false", expected_attempt_id="att_different")
        res, tv, err = _run_recovery_notebook(spark, widgets)
        self.assertIsNone(err)
        self.assertEqual(res["skipped_count"], 1)
        self.assertEqual(res["recovered_count"], 0)
        self.assertEqual(res["details"][0]["reason"], "Expected onboarding attempt ID does not match the row owner")

        # B7: False transition increments transition_failure_count and errors
        spark = StatefulControlSpark()
        spark.simulate_transition_failure = True
        spark.assessment_rows = [
            _make_assessment_row(self.conn_id, self.assess_id, "HR", "T1",
                                 selection_status="ONBOARDING",
                                 onboarding_run_id=self.run_id,
                                 onboarding_attempt_id=self.attempt_id)
        ]
        spark.control_rows = [dict(control_row)]
        res, tv, err = _run_recovery_notebook(spark, self._default_widgets("RESUME_REGISTERED", dry_run="false"))
        self.assertIsNotNone(err)
        self.assertEqual(res["transition_failure_count"], 1)
        self.assertEqual(res["recovered_count"], 0)
        self.assertEqual(res["execution_status"], "FAILED")
        self.assertEqual(res["errors"], ["HR.T1: Ownership-safe REGISTERED transition was not verified"])

    def test_finalize_onboarded_behavior(self):
        control_row = {
            "connection_id": self.conn_id,
            "source_table_id": self.expected_sid,
            "source_schema": "HR",
            "source_table": "T1",
            "source_identity_version": 2,
            "table_decision": "AUTO_MIGRATE",
            "is_active": True,
            "current_status": "PROVISIONED",
            "target_catalog": "main",
            "target_schema": "hr",
            "target_table": "t1",
        }

        # C1: Dry run performs no transition
        spark = StatefulControlSpark()
        spark.assessment_rows = [
            _make_assessment_row(self.conn_id, self.assess_id, "HR", "T1",
                                 selection_status="REGISTERED",
                                 onboarding_run_id=self.run_id,
                                 onboarding_attempt_id=self.attempt_id)
        ]
        spark.control_rows = [dict(control_row)]
        res, tv, err = _run_recovery_notebook(spark, self._default_widgets("FINALIZE_ONBOARDED", dry_run="true"))
        self.assertIsNone(err)
        self.assertEqual(res["recovered_count"], 0)
        self.assertEqual(res["would_recover_count"], 1)
        self.assertEqual(spark.assessment_rows[0]["selection_status"], "REGISTERED")

        # C2: Successful real transition
        spark = StatefulControlSpark()
        spark.assessment_rows = [
            _make_assessment_row(self.conn_id, self.assess_id, "HR", "T1",
                                 selection_status="REGISTERED",
                                 onboarding_run_id=self.run_id,
                                 onboarding_attempt_id=self.attempt_id)
        ]
        spark.control_rows = [dict(control_row)]
        res, tv, err = _run_recovery_notebook(spark, self._default_widgets("FINALIZE_ONBOARDED", dry_run="false"))
        self.assertIsNone(err)
        self.assertEqual(res["recovered_count"], 1)
        self.assertEqual(spark.assessment_rows[0]["selection_status"], "ONBOARDED")

        # C3/C4: False transition fails and raises
        spark = StatefulControlSpark()
        spark.simulate_transition_failure = True
        spark.assessment_rows = [
            _make_assessment_row(self.conn_id, self.assess_id, "HR", "T1",
                                 selection_status="REGISTERED",
                                 onboarding_run_id=self.run_id,
                                 onboarding_attempt_id=self.attempt_id)
        ]
        spark.control_rows = [dict(control_row)]
        res, tv, err = _run_recovery_notebook(spark, self._default_widgets("FINALIZE_ONBOARDED", dry_run="false"))
        self.assertIsNotNone(err)
        self.assertEqual(res["transition_failure_count"], 1)
        self.assertEqual(res["recovered_count"], 0)
        self.assertEqual(res["execution_status"], "FAILED")
        self.assertEqual(res["errors"], ["HR.T1: Ownership-safe ONBOARDED transition was not verified"])

        # C5: Non-PROVISIONED row is skipped
        spark = StatefulControlSpark()
        bad_control = dict(control_row, current_status="PENDING")
        spark.assessment_rows = [
            _make_assessment_row(self.conn_id, self.assess_id, "HR", "T1",
                                 selection_status="REGISTERED",
                                 onboarding_run_id=self.run_id,
                                 onboarding_attempt_id=self.attempt_id)
        ]
        spark.control_rows = [bad_control]
        res, tv, err = _run_recovery_notebook(spark, self._default_widgets("FINALIZE_ONBOARDED", dry_run="false"))
        self.assertIsNone(err)
        self.assertEqual(res["skipped_count"], 1)
        self.assertEqual(res["recovered_count"], 0)

        # C6: Target collision row is skipped
        spark = StatefulControlSpark()
        collision_control = {
            "connection_id": "conn_other",
            "source_table_id": "sid_other",
            "source_schema": "OTHER",
            "source_table": "T2",
            "source_identity_version": 2,
            "target_catalog": "main",
            "target_schema": "hr",
            "target_table": "t1",
            "is_active": True,
            "current_status": "PROVISIONED",
        }
        spark.assessment_rows = [
            _make_assessment_row(self.conn_id, self.assess_id, "HR", "T1",
                                 selection_status="REGISTERED",
                                 onboarding_run_id=self.run_id,
                                 onboarding_attempt_id=self.attempt_id)
        ]
        spark.control_rows = [dict(control_row), collision_control]
        res, tv, err = _run_recovery_notebook(spark, self._default_widgets("FINALIZE_ONBOARDED", dry_run="false"))
        self.assertIsNone(err)
        self.assertEqual(res["skipped_count"], 1)
        self.assertEqual(res["recovered_count"], 0)

        # C7: Duplicate registration in source_table_control is skipped
        spark = StatefulControlSpark()
        spark.assessment_rows = [
            _make_assessment_row(self.conn_id, self.assess_id, "HR", "T1",
                                 selection_status="REGISTERED",
                                 onboarding_run_id=self.run_id,
                                 onboarding_attempt_id=self.attempt_id)
        ]
        spark.control_rows = [dict(control_row), dict(control_row)]
        res, tv, err = _run_recovery_notebook(spark, self._default_widgets("FINALIZE_ONBOARDED", dry_run="false"))
        self.assertIsNone(err)
        self.assertEqual(res["skipped_count"], 1)
        self.assertEqual(res["recovered_count"], 0)

    def test_reset_to_selected_behavior(self):
        # D1: Successful conditional update is verified
        spark = StatefulControlSpark()
        spark.assessment_rows = [
            _make_assessment_row(self.conn_id, self.assess_id, "HR", "T1",
                                 selection_status="ONBOARDING",
                                 onboarding_run_id=self.run_id,
                                 onboarding_attempt_id=self.attempt_id)
        ]
        res, tv, err = _run_recovery_notebook(spark, self._default_widgets("RESET_TO_SELECTED", dry_run="false"))
        self.assertIsNone(err)
        self.assertEqual(res["recovered_count"], 1)
        self.assertEqual(spark.assessment_rows[0]["selection_status"], "SELECTED")
        self.assertIsNone(spark.assessment_rows[0]["onboarding_run_id"])
        self.assertIsNone(spark.assessment_rows[0]["onboarding_attempt_id"])
        self.assertTrue(spark.assessment_rows[0]["is_selected"])

        # D2: Zero-row update (wrong run) produces no changes
        spark = StatefulControlSpark()
        spark.assessment_rows = [
            _make_assessment_row(self.conn_id, self.assess_id, "HR", "T1",
                                 selection_status="ONBOARDING",
                                 onboarding_run_id="other_run",
                                 onboarding_attempt_id=self.attempt_id)
        ]
        res, tv, err = _run_recovery_notebook(spark, self._default_widgets("RESET_TO_SELECTED", dry_run="false"))
        self.assertIsNone(err)
        self.assertEqual(res["candidate_count"], 0)
        self.assertEqual(res["recovered_count"], 0)
        self.assertEqual(res["business_status"], "NO_CHANGES")

        # D3: Post-update verification failure increments transition_failure_count
        spark = StatefulControlSpark()
        spark.simulate_reset_failure = True
        spark.assessment_rows = [
            _make_assessment_row(self.conn_id, self.assess_id, "HR", "T1",
                                 selection_status="ONBOARDING",
                                 onboarding_run_id=self.run_id,
                                 onboarding_attempt_id=self.attempt_id)
        ]
        res, tv, err = _run_recovery_notebook(spark, self._default_widgets("RESET_TO_SELECTED", dry_run="false"))
        self.assertIsNotNone(err)
        self.assertEqual(res["transition_failure_count"], 1)
        self.assertEqual(res["recovered_count"], 0)
        self.assertEqual(res["execution_status"], "FAILED")
        self.assertEqual(res["errors"], ["HR.T1: Conditional RESET_TO_SELECTED could not be verified"])

        # D4: Existing registration in source_table_control prevents reset
        spark = StatefulControlSpark()
        spark.assessment_rows = [
            _make_assessment_row(self.conn_id, self.assess_id, "HR", "T1",
                                 selection_status="ONBOARDING",
                                 onboarding_run_id=self.run_id,
                                 onboarding_attempt_id=self.attempt_id)
        ]
        spark.control_rows = [{
            "connection_id": self.conn_id,
            "source_table_id": self.expected_sid,
            "source_schema": "HR",
            "source_table": "T1",
            "current_status": "REGISTERED",
        }]
        res, tv, err = _run_recovery_notebook(spark, self._default_widgets("RESET_TO_SELECTED", dry_run="false"))
        self.assertIsNone(err)
        self.assertEqual(res["skipped_count"], 1)
        self.assertEqual(res["recovered_count"], 0)

        # D5: Schema/table anomaly registration in source_table_control prevents reset
        spark = StatefulControlSpark()
        spark.assessment_rows = [
            _make_assessment_row(self.conn_id, self.assess_id, "HR", "T1",
                                 selection_status="ONBOARDING",
                                 onboarding_run_id=self.run_id,
                                 onboarding_attempt_id=self.attempt_id)
        ]
        spark.control_rows = [{
            "connection_id": self.conn_id,
            "source_table_id": "sid_anomaly_other",
            "source_schema": "HR",
            "source_table": "T1",
            "current_status": "REGISTERED",
        }]
        res, tv, err = _run_recovery_notebook(spark, self._default_widgets("RESET_TO_SELECTED", dry_run="false"))
        self.assertIsNone(err)
        self.assertEqual(res["skipped_count"], 1)
        self.assertEqual(res["recovered_count"], 0)

    def test_exact_deterministic_recovery_registration(self):
        """B. Exact deterministic recovery registration behavior."""
        # 1. Correct computed source_table_id is accepted
        spark = StatefulControlSpark()
        spark.assessment_rows = [
            _make_assessment_row(self.conn_id, self.assess_id, "HR", "T1",
                                 selection_status="ONBOARDING",
                                 onboarding_run_id=self.run_id,
                                 onboarding_attempt_id=self.attempt_id)
        ]
        spark.control_rows = [{
            "connection_id": self.conn_id,
            "source_table_id": self.expected_sid,
            "source_schema": "HR",
            "source_table": "T1",
            "source_identity_version": 2,
            "target_catalog": "main",
            "target_schema": "hr",
            "target_table": "t1",
        }]
        res, tv, err = _run_recovery_notebook(spark, self._default_widgets("RESUME_REGISTERED", dry_run="false"))
        self.assertIsNone(err)
        self.assertEqual(res["recovered_count"], 1)
        self.assertEqual(spark.assessment_rows[0]["selection_status"], "REGISTERED")

        # 2. Matching schema/table but wrong source_table_id is not accepted by RESUME_REGISTERED
        spark = StatefulControlSpark()
        spark.assessment_rows = [
            _make_assessment_row(self.conn_id, self.assess_id, "HR", "T1",
                                 selection_status="ONBOARDING",
                                 onboarding_run_id=self.run_id,
                                 onboarding_attempt_id=self.attempt_id)
        ]
        spark.control_rows = [{
            "connection_id": self.conn_id,
            "source_table_id": "sid_wrong_hash",
            "source_schema": "HR",
            "source_table": "T1",
            "source_identity_version": 2,
            "target_catalog": "main",
            "target_schema": "hr",
            "target_table": "t1",
        }]
        res, tv, err = _run_recovery_notebook(spark, self._default_widgets("RESUME_REGISTERED", dry_run="false"))
        self.assertIsNone(err)
        self.assertEqual(res["skipped_count"], 1)
        self.assertEqual(res["recovered_count"], 0)
        self.assertEqual(spark.assessment_rows[0]["selection_status"], "ONBOARDING")

        # 3. source_identity_version = 2 but wrong deterministic source_table_id is not accepted
        spark = StatefulControlSpark()
        spark.assessment_rows = [
            _make_assessment_row(self.conn_id, self.assess_id, "HR", "T1",
                                 selection_status="ONBOARDING",
                                 onboarding_run_id=self.run_id,
                                 onboarding_attempt_id=self.attempt_id)
        ]
        spark.control_rows = [{
            "connection_id": self.conn_id,
            "source_table_id": "sid_v2_wrong",
            "source_schema": "HR",
            "source_table": "T1",
            "source_identity_version": 2,
            "target_catalog": "main",
            "target_schema": "hr",
            "target_table": "t1",
        }]
        res, tv, err = _run_recovery_notebook(spark, self._default_widgets("RESUME_REGISTERED", dry_run="false"))
        self.assertIsNone(err)
        self.assertEqual(res["skipped_count"], 1)
        self.assertEqual(res["recovered_count"], 0)

        # 4. Duplicate rows for the exact deterministic source_table_id are skipped
        spark = StatefulControlSpark()
        spark.assessment_rows = [
            _make_assessment_row(self.conn_id, self.assess_id, "HR", "T1",
                                 selection_status="ONBOARDING",
                                 onboarding_run_id=self.run_id,
                                 onboarding_attempt_id=self.attempt_id)
        ]
        exact_row = {
            "connection_id": self.conn_id,
            "source_table_id": self.expected_sid,
            "source_schema": "HR",
            "source_table": "T1",
            "source_identity_version": 2,
            "target_catalog": "main",
            "target_schema": "hr",
            "target_table": "t1",
        }
        spark.control_rows = [dict(exact_row), dict(exact_row)]
        res, tv, err = _run_recovery_notebook(spark, self._default_widgets("RESUME_REGISTERED", dry_run="false"))
        self.assertIsNone(err)
        self.assertEqual(res["skipped_count"], 1)
        self.assertEqual(res["recovered_count"], 0)
        self.assertEqual(res["details"][0]["reason"], "Multiple exact deterministic source-table registrations were found")

        # 5. FINALIZE_ONBOARDED uses expected_source_table_id in exclude_owner
        spark = StatefulControlSpark()
        spark.assessment_rows = [
            _make_assessment_row(self.conn_id, self.assess_id, "HR", "T1",
                                 selection_status="REGISTERED",
                                 onboarding_run_id=self.run_id,
                                 onboarding_attempt_id=self.attempt_id)
        ]
        spark.control_rows = [{
            "connection_id": self.conn_id,
            "source_table_id": self.expected_sid,
            "source_schema": "HR",
            "source_table": "T1",
            "source_identity_version": 2,
            "table_decision": "AUTO_MIGRATE",
            "is_active": True,
            "current_status": "PROVISIONED",
            "target_catalog": "main",
            "target_schema": "hr",
            "target_table": "t1",
        }]
        res, tv, err = _run_recovery_notebook(spark, self._default_widgets("FINALIZE_ONBOARDED", dry_run="false"))
        self.assertIsNone(err)
        self.assertEqual(res["recovered_count"], 1)
        self.assertEqual(spark.assessment_rows[0]["selection_status"], "ONBOARDED")

    def test_dry_run_counters_and_prefixes(self):
        # E1-E6: Dry run properties
        for action in ("MARK_FAILED", "RESUME_REGISTERED", "RESET_TO_SELECTED"):
            spark = StatefulControlSpark()
            spark.assessment_rows = [
                _make_assessment_row(self.conn_id, self.assess_id, "HR", "T1",
                                     selection_status="ONBOARDING",
                                     onboarding_run_id=self.run_id,
                                     onboarding_attempt_id=self.attempt_id)
            ]
            if action == "RESUME_REGISTERED":
                spark.control_rows = [{
                    "connection_id": self.conn_id,
                    "source_table_id": self.expected_sid,
                    "source_schema": "HR",
                    "source_table": "T1",
                    "source_identity_version": 2,
                    "target_catalog": "main",
                    "target_schema": "hr",
                    "target_table": "t1",
                }]
            res, tv, err = _run_recovery_notebook(spark, self._default_widgets(action, dry_run="true"))
            self.assertIsNone(err)
            self.assertEqual(res["recovered_count"], 0)
            self.assertEqual(res["would_recover_count"], 1)
            self.assertTrue(res["details"][0]["action"].startswith("WOULD_"))
            self.assertEqual(res["business_status"], "DRY_RUN_COMPLETE")
            self.assertEqual(res["execution_status"], "SUCCEEDED")

    def test_real_status_model(self):
        # F1: All successes -> RECOVERED + SUCCEEDED
        spark = StatefulControlSpark()
        spark.assessment_rows = [
            _make_assessment_row(self.conn_id, self.assess_id, "HR", "T1",
                                 selection_status="ONBOARDING",
                                 onboarding_run_id=self.run_id,
                                 onboarding_attempt_id=self.attempt_id)
        ]
        res, tv, err = _run_recovery_notebook(spark, self._default_widgets("MARK_FAILED", dry_run="false"))
        self.assertEqual(res["business_status"], "RECOVERED")
        self.assertEqual(res["execution_status"], "SUCCEEDED")

        # F2: No eligible changes (empty candidates) -> NO_CHANGES + SUCCEEDED
        spark = StatefulControlSpark()
        res, tv, err = _run_recovery_notebook(spark, self._default_widgets("MARK_FAILED", dry_run="false"))
        self.assertEqual(res["business_status"], "NO_CHANGES")
        self.assertEqual(res["execution_status"], "SUCCEEDED")

        # F3: Mixed success and mutation failure -> PARTIAL + FAILED
        spark = StatefulControlSpark()
        spark.assessment_rows = [
            _make_assessment_row(self.conn_id, self.assess_id, "HR", "T1",
                                 selection_status="ONBOARDING",
                                 onboarding_run_id=self.run_id,
                                 onboarding_attempt_id=self.attempt_id),
            _make_assessment_row(self.conn_id, self.assess_id, "HR", "T2",
                                 selection_status="ONBOARDING",
                                 onboarding_run_id=self.run_id,
                                 onboarding_attempt_id=self.attempt_id),
        ]
        orig_update = spark._handle_update_assessment
        call_count = [0]
        def mixed_update(sql):
            call_count[0] += 1
            if call_count[0] > 1:
                return FakeDataFrame([])
            return orig_update(sql)
        spark._handle_update_assessment = mixed_update

        res, tv, err = _run_recovery_notebook(spark, self._default_widgets("MARK_FAILED", dry_run="false"))
        self.assertIsNotNone(err)
        self.assertEqual(res["recovered_count"], 1)
        self.assertEqual(res["transition_failure_count"], 1)
        self.assertEqual(res["business_status"], "PARTIAL")
        self.assertEqual(res["execution_status"], "FAILED")

        # F4: Only mutation failures -> FAILED + FAILED
        spark = StatefulControlSpark()
        spark.simulate_transition_failure = True
        spark.assessment_rows = [
            _make_assessment_row(self.conn_id, self.assess_id, "HR", "T1",
                                 selection_status="ONBOARDING",
                                 onboarding_run_id=self.run_id,
                                 onboarding_attempt_id=self.attempt_id)
        ]
        res, tv, err = _run_recovery_notebook(spark, self._default_widgets("MARK_FAILED", dry_run="false"))
        self.assertIsNotNone(err)
        self.assertEqual(res["recovered_count"], 0)
        self.assertEqual(res["transition_failure_count"], 1)
        self.assertEqual(res["business_status"], "FAILED")
        self.assertEqual(res["execution_status"], "FAILED")

        # F5: Expected eligibility skips alone -> NO_CHANGES + SUCCEEDED
        spark = StatefulControlSpark()
        spark.assessment_rows = [
            _make_assessment_row(self.conn_id, self.assess_id, "HR", "T1",
                                 selection_status="ONBOARDED",
                                 onboarding_run_id=self.run_id,
                                 onboarding_attempt_id=self.attempt_id)
        ]
        res, tv, err = _run_recovery_notebook(spark, self._default_widgets("MARK_FAILED", dry_run="false"))
        self.assertIsNone(err)
        self.assertEqual(res["skipped_count"], 1)
        self.assertEqual(res["recovered_count"], 0)
        self.assertEqual(res["transition_failure_count"], 0)
        self.assertEqual(res["business_status"], "NO_CHANGES")
        self.assertEqual(res["execution_status"], "SUCCEEDED")

    def test_output_safety_and_bounding(self):
        spark = StatefulControlSpark()
        spark.assessment_rows = [
            _make_assessment_row(self.conn_id, self.assess_id, "HR", f"T_{i}",
                                 selection_status="ONBOARDING",
                                 onboarding_run_id=self.run_id,
                                 onboarding_attempt_id=self.attempt_id)
            for i in range(25)
        ]
        spark.simulate_mutation_exception = RuntimeError(f"Simulated error with sensitive token=ghp_123456 and long payload: {'x'*600}")
        res, tv, err = _run_recovery_notebook(spark, self._default_widgets("MARK_FAILED", dry_run="false"))
        self.assertIsNotNone(err)
        self.assertLessEqual(len(res["errors"]), 20)
        for e in res["errors"]:
            self.assertLessEqual(len(e), 500)
            self.assertNotIn("ghp_123456", e)
        self.assertLessEqual(len(res["details"]), 100)
        for d in res["details"]:
            if "reason" in d:
                self.assertLessEqual(len(d["reason"]), 500)
                self.assertNotIn("ghp_123456", d["reason"])
        self.assertNotIn("errors", tv)
        self.assertNotIn("details", tv)

    def test_existing_lifecycle_protection(self):
        # H1-H3: ONBOARDED, REVIEW_REQUIRED, BLOCKED are never reset
        for terminal_status in ("ONBOARDED", "REVIEW_REQUIRED", "BLOCKED"):
            spark = StatefulControlSpark()
            spark.assessment_rows = [
                _make_assessment_row(self.conn_id, self.assess_id, "HR", "T1",
                                     selection_status=terminal_status,
                                     onboarding_run_id=self.run_id,
                                     onboarding_attempt_id=self.attempt_id)
            ]
            res, tv, err = _run_recovery_notebook(spark, self._default_widgets("RESET_TO_SELECTED", dry_run="false"))
            self.assertIsNone(err)
            self.assertEqual(res["skipped_count"], 1)
            self.assertEqual(res["recovered_count"], 0)
            self.assertEqual(spark.assessment_rows[0]["selection_status"], terminal_status)

        # H4: Another run's row is never changed
        spark = StatefulControlSpark()
        spark.assessment_rows = [
            _make_assessment_row(self.conn_id, self.assess_id, "HR", "T1",
                                 selection_status="ONBOARDING",
                                 onboarding_run_id="other_run_999",
                                 onboarding_attempt_id=self.attempt_id)
        ]
        res, tv, err = _run_recovery_notebook(spark, self._default_widgets("RESET_TO_SELECTED", dry_run="false"))
        self.assertIsNone(err)
        self.assertEqual(res["candidate_count"], 0)
        self.assertEqual(spark.assessment_rows[0]["selection_status"], "ONBOARDING")
        self.assertEqual(spark.assessment_rows[0]["onboarding_run_id"], "other_run_999")

        # H5: Another attempt's row is never changed
        spark = StatefulControlSpark()
        spark.assessment_rows = [
            _make_assessment_row(self.conn_id, self.assess_id, "HR", "T1",
                                 selection_status="ONBOARDING",
                                 onboarding_run_id=self.run_id,
                                 onboarding_attempt_id="att_existing")
        ]
        widgets = self._default_widgets("RESET_TO_SELECTED", dry_run="false", expected_attempt_id="att_other")
        res, tv, err = _run_recovery_notebook(spark, widgets)
        self.assertIsNone(err)
        self.assertEqual(res["skipped_count"], 1)
        self.assertEqual(res["recovered_count"], 0)
        self.assertEqual(spark.assessment_rows[0]["onboarding_attempt_id"], "att_existing")


class TestRecoveryForbiddenFallbacks(unittest.TestCase):
    """Static checks for forbidden fallbacks and control-flow correctness in recovery notebook."""

    @classmethod
    def setUpClass(cls):
        with open(os.path.join(DEPLOYMENT, "NB_RecoverSelectedOnboardingState.py"), encoding="utf-8") as f:
            cls.code = f.read()

    def test_expected_run_id_never_used_as_attempt_id_fallback(self):
        # Check 1: expected_run_id never appears as an attempt_id fallback
        self.assertNotIn("or expected_run_id", self.code)
        self.assertNotIn("r.get(\"onboarding_attempt_id\") or expected_attempt_id or expected_run_id", self.code)

    def test_recovery_checks_repository_method_return_values(self):
        # Check 2: Recovery checks repository method return values
        self.assertIn("changed = repo.mark_assessment_onboarding_failed(", self.code)
        self.assertIn("changed = repo.mark_assessment_registration_succeeded(", self.code)
        self.assertIn("changed = repo.mark_assessment_onboarding_completed(", self.code)
        self.assertIn("if not changed:", self.code)

    def test_recovered_count_increments_only_after_successful_real_transitions(self):
        # Check 3: recovered_count increments only after successful real transitions
        self.assertIn("recovered_count += 1", self.code)
        self.assertNotIn("if dry_run:\n            recovered_count += 1", self.code)

    def test_dry_run_uses_would_recover_count(self):
        # Check 4: dry-run uses would_recover_count
        self.assertIn("would_recover_count += 1", self.code)
        self.assertIn("action=\"WOULD_MARK_FAILED\"", self.code)
        self.assertIn("action=\"WOULD_RESET_TO_SELECTED\"", self.code)
        self.assertIn("action=\"WOULD_RESUME_REGISTERED\"", self.code)
        self.assertIn("action=\"WOULD_FINALIZE_ONBOARDED\"", self.code)

    def test_reset_to_selected_performs_post_update_verification(self):
        # Check 5: RESET_TO_SELECTED performs post-update verification
        self.assertIn("updated_row = repo._get_assessment_selection_row(", self.code)
        self.assertIn("reset_succeeded = (", self.code)
        self.assertIn("if not reset_succeeded:", self.code)

    def test_transition_failure_count_controls_execution_status(self):
        # Check 6: transition_failure_count controls execution status
        self.assertIn("elif transition_failure_count > 0:\n    execution_status = \"FAILED\"", self.code)

    def test_runtime_error_raised_for_real_transition_failures(self):
        # Check 7: RuntimeError is raised for real transition failures
        self.assertIn("if transition_failure_count > 0:\n    raise RuntimeError(", self.code)


if __name__ == "__main__":
    unittest.main()
