"""
Unit tests for source-to-Bronze delta reconciliation (Commit 3, Phase 5).

Proves the mandatory correctness rules: exact interval/stage matching, duplicate
and missing-key failures, and that target_count >= source_count is never an
automatic PASS.
"""

import os
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.join(os.path.dirname(HERE), "src")
for p in (SRC, os.path.dirname(HERE)):
    if p not in sys.path:
        sys.path.insert(0, p)

import reconciliation as recon  # noqa: E402


class TestFullLoad(unittest.TestCase):
    def test_exact_match_passes(self):
        self.assertEqual(recon.reconcile_full_load(100, 100).status, recon.PASS)

    def test_mismatch_fails(self):
        self.assertEqual(recon.reconcile_full_load(100, 99).status, recon.FAIL)


class TestWatermark(unittest.TestCase):
    def test_exact_interval_passes(self):
        r = recon.reconcile_watermark(50, 50)
        self.assertEqual(r.status, recon.PASS)
        self.assertEqual(r.checks[0]["check_type"], recon.DELTA_INTERVAL_COUNT)

    def test_interval_mismatch_fails(self):
        self.assertEqual(recon.reconcile_watermark(50, 49).status, recon.FAIL)

    def test_more_applied_than_extracted_is_not_pass(self):
        # target_count >= source_count must NOT be an automatic pass.
        self.assertEqual(recon.reconcile_watermark(50, 60).status, recon.FAIL)


class TestHybrid(unittest.TestCase):
    def test_clean_merge_passes(self):
        self.assertEqual(
            recon.reconcile_hybrid(30, 30, 0, 0).status, recon.PASS)

    def test_duplicate_keys_fail(self):
        self.assertEqual(
            recon.reconcile_hybrid(30, 30, 2, 0).status, recon.FAIL)

    def test_missing_keys_fail(self):
        self.assertEqual(
            recon.reconcile_hybrid(30, 30, 0, 1).status, recon.FAIL)

    def test_extracted_staged_mismatch_fails(self):
        self.assertEqual(
            recon.reconcile_hybrid(30, 28, 0, 0).status, recon.FAIL)


class TestPrimaryKey(unittest.TestCase):
    def test_hard_delete_requires_exact_total(self):
        ok = recon.reconcile_primary_key(100, 100, 0, 0, "HARD_DELETE", 100)
        self.assertEqual(ok.status, recon.PASS)
        bad = recon.reconcile_primary_key(100, 100, 0, 0, "HARD_DELETE", 120)
        self.assertEqual(bad.status, recon.FAIL)

    def test_ignore_deletes_extra_rows_warn_not_fail(self):
        r = recon.reconcile_primary_key(100, 100, 0, 0, "IGNORE_DELETES", 130)
        self.assertEqual(r.status, recon.WARN)
        self.assertTrue(r.passed)

    def test_ignore_deletes_clean_passes(self):
        r = recon.reconcile_primary_key(100, 100, 0, 0, "IGNORE_DELETES", 100)
        self.assertEqual(r.status, recon.PASS)

    def test_duplicate_keys_fail_even_with_ignore_deletes(self):
        r = recon.reconcile_primary_key(100, 100, 5, 0, "IGNORE_DELETES", 100)
        self.assertEqual(r.status, recon.FAIL)

    def test_check_types_present(self):
        r = recon.reconcile_primary_key(10, 10, 0, 0, "HARD_DELETE", 10)
        types = {c["check_type"] for c in r.checks}
        self.assertIn(recon.STAGE_COUNT, types)
        self.assertIn(recon.DUPLICATE_PRIMARY_KEY, types)
        self.assertIn(recon.MERGED_KEY_EXISTENCE, types)
        self.assertIn(recon.FULL_SNAPSHOT_COUNT, types)


if __name__ == "__main__":
    unittest.main()
