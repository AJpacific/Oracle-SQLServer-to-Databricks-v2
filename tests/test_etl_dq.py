"""
Unit tests for DQ rule helpers and ETL reconciliation (Commits 6 & 7).
"""

import os
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.join(os.path.dirname(HERE), "src")
for p in (SRC, os.path.dirname(HERE)):
    if p not in sys.path:
        sys.path.insert(0, p)

import dq_rules as dq  # noqa: E402
import reconciliation as recon  # noqa: E402


class TestRuleClassification(unittest.TestCase):
    def test_validation_vs_transformation(self):
        self.assertTrue(dq.is_validation("NOT_NULL"))
        self.assertTrue(dq.is_validation("DUPLICATE_KEY"))
        self.assertTrue(dq.is_transformation("TRIM_STRING"))
        self.assertFalse(dq.is_validation("TRIM_STRING"))

    def test_unsupported_rule_rejected(self):
        with self.assertRaises(ValueError):
            dq.validate_rule({"rule_type": "SELECT * FROM t", "column_name": "c"})

    def test_arbitrary_sql_not_accepted(self):
        with self.assertRaises(ValueError):
            dq.validate_rule({"rule_type": "CUSTOM_SQL", "rule_value": "1=1"})

    def test_column_required(self):
        with self.assertRaises(ValueError):
            dq.validate_rule({"rule_type": "NOT_NULL"})


class TestAllowedValues(unittest.TestCase):
    def test_parses_json_array(self):
        self.assertEqual(dq.parse_allowed_values('["A", "B", "C"]'), ["A", "B", "C"])

    def test_non_array_fails(self):
        with self.assertRaises(ValueError):
            dq.parse_allowed_values('{"a": 1}')

    def test_bad_json_fails(self):
        with self.assertRaises(ValueError):
            dq.parse_allowed_values("A,B,C")

    def test_validate_allowed_values_rule(self):
        r = dq.validate_rule({"rule_type": "ALLOWED_VALUES", "column_name": "s",
                              "rule_value": '["X","Y"]'})
        self.assertEqual(r["rule_type"], "ALLOWED_VALUES")


class TestStandardizeCase(unittest.TestCase):
    def test_upper_lower_ok(self):
        self.assertEqual(dq.normalize_case_mode("upper"), "UPPER")
        self.assertEqual(dq.normalize_case_mode("Lower"), "LOWER")

    def test_invalid_case_fails(self):
        with self.assertRaises(ValueError):
            dq.normalize_case_mode("title")


class TestSeverity(unittest.TestCase):
    def test_default_rejects(self):
        self.assertTrue(dq.is_rejecting_severity(None))
        self.assertTrue(dq.is_rejecting_severity("ERROR"))

    def test_warn_does_not_reject(self):
        self.assertFalse(dq.is_rejecting_severity("WARN"))


class TestEtlReconciliation(unittest.TestCase):
    def test_full_input_accounting_and_silver(self):
        # one record failing multiple rules counts once (rejected_distinct=1).
        r = recon.reconcile_etl_full(input_count=10, valid_count=9,
                                     rejected_distinct_count=1, silver_count=9)
        self.assertEqual(r.status, recon.PASS)

    def test_full_input_accounting_mismatch_fails(self):
        r = recon.reconcile_etl_full(10, 9, 2, 9)  # 9+2 != 10
        self.assertEqual(r.status, recon.FAIL)

    def test_full_silver_mismatch_fails(self):
        r = recon.reconcile_etl_full(10, 9, 1, 8)  # silver != valid
        self.assertEqual(r.status, recon.FAIL)

    def test_incremental_merge_clean_passes(self):
        r = recon.reconcile_etl_incremental_merge(10, 10, 0, 0, 0)
        self.assertEqual(r.status, recon.PASS)

    def test_incremental_merge_duplicate_keys_fail(self):
        r = recon.reconcile_etl_incremental_merge(10, 10, 0, 3, 0)
        self.assertEqual(r.status, recon.FAIL)

    def test_incremental_merge_missing_keys_fail(self):
        r = recon.reconcile_etl_incremental_merge(10, 10, 0, 0, 2)
        self.assertEqual(r.status, recon.FAIL)

    def test_interval_replacement(self):
        self.assertEqual(
            recon.reconcile_etl_interval(10, 8, 2, 8).status, recon.PASS)
        self.assertEqual(
            recon.reconcile_etl_interval(10, 8, 2, 7).status, recon.FAIL)


class TestDatabricksTypeValidationAndDDL(unittest.TestCase):
    def test_time_type_validation_accepted(self):
        # Bare TIME is accepted
        self.assertEqual(dq.normalize_cast_type("time"), "TIME")
        self.assertEqual(dq.normalize_cast_type("TIME"), "TIME")
        # TIME(0) through TIME(6) are accepted
        for p in range(7):
            self.assertEqual(dq.normalize_cast_type(f"TIME({p})"), f"TIME({p})")
            self.assertEqual(dq.normalize_cast_type(f"time( {p} )"), f"TIME( {p} )".upper())

    def test_time_type_validation_rejected(self):
        # TIME(7) and invalid precisions must be rejected
        for invalid_type in ("TIME(7)", "time(7)", "TIME(8)", "TIME(-1)", "TIME(bad)", "TIME()"):
            with self.subTest(invalid_type=invalid_type):
                with self.assertRaises(ValueError):
                    dq.normalize_cast_type(invalid_type)

    def test_ddl_build_create_table_preserves_time_precision(self):
        import ddl_builder as ddl
        stmt = ddl.build_create_table(
            catalog="main",
            schema="sales",
            table="orders",
            columns=[
                ("order_time", "TIME(3)", True),
                ("order_note", "STRING", True),
                ("id", "INT", False),
            ]
        )
        self.assertIn("`order_time` TIME(3)", stmt)
        self.assertIn("`order_note` STRING", stmt)
        self.assertIn("`id` INT NOT NULL", stmt)
        self.assertIn("USING DELTA", stmt)


if __name__ == "__main__":
    unittest.main()

