"""
Unit tests for the ETL work unit (Fix 4).

The defect these guard: a retry processed the frozen interval but audited the
current control-row watermark, so a second retry could receive a different
interval. Every stage now reads one immutable work unit.
"""

import os
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.join(os.path.dirname(HERE), "src")
for p in (SRC, HERE, os.path.dirname(HERE)):
    if p not in sys.path:
        sys.path.insert(0, p)

import etl_work_unit as etlwu  # noqa: E402

LOWER = "2026-01-01T00:00:00.000000Z"
UPPER = "2026-02-01T00:00:00.000000Z"
LATER = "2026-03-01T00:00:00.000000Z"


class TestFullWorkUnit(unittest.TestCase):
    def test_full_has_no_interval(self):
        unit = etlwu.build_full_work_unit()
        self.assertEqual(unit.mode, etlwu.FULL)
        self.assertIsNone(unit.lower_watermark)
        self.assertIsNone(unit.upper_watermark)
        self.assertFalse(unit.is_incremental)

    def test_full_audit_fields_are_null_bounds(self):
        fields = etlwu.build_full_work_unit().audit_fields(attempt_number=1)
        self.assertIsNone(fields["lower_watermark"])
        self.assertIsNone(fields["upper_watermark"])
        self.assertEqual(fields["attempt_number"], 1)

    def test_full_never_advances_a_checkpoint(self):
        unit = etlwu.build_full_work_unit()
        self.assertIsNone(etlwu.checkpoint_value(unit, True))


class TestIncrementalWorkUnit(unittest.TestCase):
    def _unit(self, **over):
        kwargs = {"watermark_column": "updated_at",
                  "cast_to": etlwu.TIMESTAMP_TYPE,
                  "lower": LOWER, "upper": UPPER}
        kwargs.update(over)
        return etlwu.build_incremental_work_unit(**kwargs)

    def test_normal_run_records_captured_bounds(self):
        unit = self._unit()
        self.assertEqual(unit.lower_watermark, LOWER)
        self.assertEqual(unit.upper_watermark, UPPER)
        self.assertFalse(unit.is_retry)

    def test_retry_records_frozen_bounds(self):
        unit = self._unit(is_retry=True, parent_run_id="parent_1")
        fields = unit.audit_fields(attempt_number=2)
        self.assertEqual(fields["lower_watermark"], LOWER)
        self.assertEqual(fields["upper_watermark"], UPPER)
        self.assertEqual(fields["parent_run_id"], "parent_1")
        self.assertEqual(fields["attempt_number"], 2)

    def test_retry_does_not_adopt_a_newer_upper_bound(self):
        # A row arriving after the failure must not widen the replayed interval.
        frozen = self._unit(is_retry=True)
        widened = self._unit(upper=LATER)
        self.assertEqual(frozen.upper_watermark, UPPER)
        self.assertNotEqual(frozen.upper_watermark, widened.upper_watermark)

    def test_repeated_retry_keeps_identical_bounds(self):
        first = self._unit(is_retry=True, parent_run_id="p")
        second = etlwu.build_incremental_work_unit(
            watermark_column="updated_at", cast_to=etlwu.TIMESTAMP_TYPE,
            lower=first.lower_watermark, upper=first.upper_watermark,
            is_retry=True, parent_run_id="p")
        self.assertEqual(first.audit_fields(2), second.audit_fields(2))

    def test_success_and_failure_paths_share_one_boundary(self):
        unit = self._unit(is_retry=True, parent_run_id="p")
        success = unit.audit_fields(attempt_number=2)
        failure = unit.audit_fields(attempt_number=2)
        self.assertEqual(success, failure)

    def test_missing_upper_bound_fails(self):
        with self.assertRaises(ValueError):
            self._unit(upper=None)

    def test_retry_missing_upper_bound_message_mentions_failed_attempt(self):
        with self.assertRaises(ValueError) as ctx:
            self._unit(upper=None, is_retry=True)
        self.assertIn("failed attempt", str(ctx.exception))

    def test_missing_lower_bound_allowed_unless_required(self):
        unit = self._unit(lower=None)
        self.assertIsNone(unit.lower_watermark)
        with self.assertRaises(ValueError):
            self._unit(lower=None, is_retry=True, require_lower=True)

    def test_non_increasing_interval_fails(self):
        with self.assertRaises(ValueError):
            self._unit(lower=UPPER, upper=UPPER)
        with self.assertRaises(ValueError):
            self._unit(lower=LATER, upper=UPPER)

    def test_unparseable_bound_fails(self):
        with self.assertRaises(ValueError):
            self._unit(upper="not-a-timestamp")

    def test_unsupported_cast_fails(self):
        with self.assertRaises(ValueError):
            self._unit(cast_to="string")

    def test_date_bounds_are_truncated_to_date(self):
        unit = self._unit(cast_to=etlwu.DATE_TYPE)
        self.assertEqual(unit.lower_watermark, "2026-01-01")
        self.assertEqual(unit.upper_watermark, "2026-02-01")

    def test_work_unit_is_immutable(self):
        unit = self._unit()
        with self.assertRaises(Exception):
            unit.upper_watermark = LATER


class TestCheckpointValue(unittest.TestCase):
    def _unit(self):
        return etlwu.build_incremental_work_unit(
            "updated_at", etlwu.TIMESTAMP_TYPE, LOWER, UPPER)

    def test_advances_only_to_the_frozen_upper_bound(self):
        self.assertEqual(etlwu.checkpoint_value(self._unit(), True), UPPER)

    def test_failed_reconciliation_does_not_advance(self):
        self.assertIsNone(etlwu.checkpoint_value(self._unit(), False))

    def test_failed_retry_does_not_advance(self):
        unit = etlwu.build_incremental_work_unit(
            "updated_at", etlwu.TIMESTAMP_TYPE, LOWER, UPPER, is_retry=True)
        self.assertIsNone(etlwu.checkpoint_value(unit, False))

    def test_successful_retry_advances_to_frozen_bound_only(self):
        unit = etlwu.build_incremental_work_unit(
            "updated_at", etlwu.TIMESTAMP_TYPE, LOWER, UPPER, is_retry=True)
        self.assertEqual(etlwu.checkpoint_value(unit, True), UPPER)


class TestCastResolution(unittest.TestCase):
    def test_timestamp_variants(self):
        for token in ("timestamp", "timestamp_ntz"):
            self.assertEqual(etlwu.resolve_cast(token), etlwu.TIMESTAMP_TYPE)

    def test_date(self):
        self.assertEqual(etlwu.resolve_cast("date"), etlwu.DATE_TYPE)

    def test_unsupported_types(self):
        for token in ("string", "int", "bigint", "binary", None, ""):
            self.assertIsNone(etlwu.resolve_cast(token))


if __name__ == "__main__":
    unittest.main()
