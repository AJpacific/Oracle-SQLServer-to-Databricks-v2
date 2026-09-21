import json
import os
import re
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.join(os.path.dirname(HERE), "src")
for p in (SRC, os.path.dirname(HERE), HERE):
    if p not in sys.path:
        sys.path.insert(0, p)

from _fakes import FakeDataFrame, FakeRow
from _nbsource import shared_nb
from control_repository import escape_string_literal, quote_databricks
from worklist_utils import (
    TASK_VALUE_LIMIT_BYTES,
    canonical_task_value_serialization,
)
import failure_classifier as fc  # noqa: E402


def _failed_row(operation, source_table_id="table_001", attempt_number=1,
                failure_stage=fc.SOURCE_READ, retry_eligible=True,
                run_id="run_1", ended_ts="2026-01-01T00:00:02Z",
                started_ts="2026-01-01T00:00:01Z",
                lower_watermark=None, upper_watermark=None,
                connection_id="connection_1"):
    return {
        "run_id": run_id,
        "source_table_id": source_table_id,
        "connection_id": connection_id,
        "operation": operation,
        "failure_stage": failure_stage,
        "error_category": fc.TRANSIENT_CONNECTION,
        "retry_eligible": retry_eligible,
        "attempt_number": attempt_number,
        "lower_watermark": lower_watermark,
        "upper_watermark": upper_watermark,
        "started_ts": started_ts,
        "ended_ts": ended_ts,
        "status": "FAILED",
    }


class FakeNotebookExit(Exception):
    def __init__(self, payload):
        self.payload = payload


class FakeWidgets:
    def __init__(self, values=None):
        self._values = {k: str(v) for k, v in (values or {}).items()}

    def dropdown(self, name, default, choices):
        if name not in self._values:
            self._values[name] = str(default)

    def text(self, name, default):
        if name not in self._values:
            self._values[name] = str(default)

    def get(self, name):
        if name not in self._values:
            raise KeyError(f"Widget {name} not declared")
        return self._values[name]


class FakeDbutils:
    def __init__(self, widgets):
        self.widgets = widgets
        self.notebook = type("FakeNotebook", (), {"exit": self._exit})()
        self.jobs = type("FakeJobs", (), {"taskValues": type("FakeTV", (), {"set": self._set_tv})()})()
        self.task_values = {}

    def _exit(self, payload):
        raise FakeNotebookExit(payload)

    def _set_tv(self, key, value):
        self.task_values[key] = value


class NB14SparkDouble:
    def __init__(self, rows=None):
        self._raw_rows = [FakeRow(r) for r in (rows or [])]
        self.executed = []

    def sql(self, query):
        self.executed.append(query)

        run_ids_match = re.search(r"run_id IN \((.*?)\)", query, re.DOTALL)
        allowed_runs = None
        if run_ids_match:
            raw_runs = run_ids_match.group(1)
            allowed_runs = set()
            for m in re.finditer(r"'((?:''|[^'])*)'", raw_runs):
                allowed_runs.add(m.group(1).replace("''", "'"))

        op_match = re.search(r"operation = '((?:''|[^'])*)'", query)
        op_filter = op_match.group(1).replace("''", "'") if op_match else None

        table_match = re.search(r"source_table_id = '((?:''|[^'])*)'", query)
        table_filter = table_match.group(1).replace("''", "'") if table_match else None

        conn_match = re.search(r"connection_id = '((?:''|[^'])*)'", query)
        conn_filter = conn_match.group(1).replace("''", "'") if conn_match else None

        filtered = []
        for r in self._raw_rows:
            if str(r.get("status") or "FAILED").upper() != "FAILED":
                continue
            if allowed_runs is not None and str(r.get("run_id") or "") not in allowed_runs:
                continue
            if op_filter and str(r.get("operation") or "") != op_filter:
                continue
            if table_filter and str(r.get("source_table_id") or "") != table_filter:
                continue
            if conn_filter and str(r.get("connection_id") or "") != conn_filter:
                continue
            filtered.append(r)

        groups = {}
        for r in filtered:
            key = (
                str(r.get("connection_id") or ""),
                str(r.get("source_table_id") or ""),
                str(r.get("operation") or ""),
                str(r.get("run_id") or ""),
            )
            groups.setdefault(key, []).append(r)

        def _sort_key(row):
            attempt = int(row.get("attempt_number") or 1)
            ended = str(row.get("ended_ts") or "")
            started = str(row.get("started_ts") or "")
            rid = str(row.get("run_id") or "")
            return (attempt, ended, started, rid)

        picked = []
        for key in sorted(groups):
            rows_in_group = groups[key]
            rows_in_group.sort(key=_sort_key, reverse=True)
            picked.append(rows_in_group[0])

        picked.sort(key=lambda r: (
            str(r.get("connection_id") or ""),
            str(r.get("source_table_id") or ""),
            str(r.get("operation") or "")
        ))

        return FakeDataFrame([FakeRow(r) for r in picked])

    def last_sql(self):
        return self.executed[-1] if self.executed else ""


def run_nb14_harness(widget_values=None, table_run_log_rows=None, spark=None, connection_id=""):
    widgets = FakeWidgets(widget_values or {})
    dbutils = FakeDbutils(widgets)
    if spark is None:
        spark = NB14SparkDouble(table_run_log_rows)

    task_values = {}

    def set_task_value(k, v):
        task_values[k] = v
        dbutils._set_tv(k, v)

    _run_counter = [0]

    def new_run_id(prefix="run"):
        _run_counter[0] += 1
        return f"{prefix}_{_run_counter[0]:03d}"

    env = {
        "__file__": "NB14_RetryFailedTables.py",
        "__name__": "__main__",
        "spark": spark,
        "dbutils": dbutils,
        "failcls": fc,
        "escape_string_literal": escape_string_literal,
        "quote_databricks": quote_databricks,
        "new_run_id": new_run_id,
        "canonical_task_value_serialization": canonical_task_value_serialization,
        "set_task_value": set_task_value,
        "CATALOG": "test_cat",
        "CONTROL_SCHEMA": "test_ctrl",
        "CONNECTION_ID": connection_id,
        "TASK_VALUE_LIMIT_BYTES": TASK_VALUE_LIMIT_BYTES,
        "json": json,
    }

    raw_code = shared_nb("NB14_RetryFailedTables.py")
    clean_lines = []
    for line in raw_code.splitlines():
        if line.strip().startswith("# MAGIC %run"):
            clean_lines.append("")
        elif line.strip().startswith("# MAGIC"):
            clean_lines.append("")
        elif line.strip().startswith("# COMMAND ----------"):
            clean_lines.append("")
        else:
            clean_lines.append(line)
    code_to_exec = "\n".join(clean_lines)

    result_payload = None
    try:
        exec(code_to_exec, env)
    except FakeNotebookExit as exit_exc:
        result_payload = exit_exc.payload

    parsed_result = json.loads(result_payload) if result_payload else None
    return {
        "result": parsed_result,
        "result_payload": result_payload,
        "task_values": task_values,
        "spark": spark,
        "env": env,
    }


def get_nb14_parse_func():
    code = shared_nb("NB14_RetryFailedTables.py")
    lines = []
    recording = False
    for line in code.splitlines():
        if line.strip().startswith("MAX_ORIGINAL_RUN_IDS ="):
            lines.append(line)
        elif line.strip().startswith("def _parse_original_run_ids"):
            recording = True
            lines.append(line)
        elif recording:
            if line and not line.startswith(" ") and not line.startswith("\t"):
                break
            lines.append(line)
    env = {}
    exec("\n".join(lines), env)
    return env["_parse_original_run_ids"]


class TestParseOriginalRunIds(unittest.TestCase):
    def setUp(self):
        self.parse = get_nb14_parse_func()

    def test_one_id(self):
        self.assertEqual(self.parse("run-a"), ["run-a"])

    def test_two_ids(self):
        self.assertEqual(self.parse("run-a,run-b"), ["run-a", "run-b"])

    def test_whitespace(self):
        self.assertEqual(self.parse(" run-a , run-b "), ["run-a", "run-b"])

    def test_duplicates(self):
        self.assertEqual(self.parse("run-a,run-b,run-a"), ["run-a", "run-b"])

    def test_empty_entries(self):
        self.assertEqual(self.parse("run-a,,run-b,"), ["run-a", "run-b"])

    def test_first_seen_order(self):
        self.assertEqual(
            self.parse("run-b,run-a,run-b,run-c"),
            ["run-b", "run-a", "run-c"]
        )

    def test_case_preservation(self):
        self.assertEqual(self.parse("Run-A,run-a"), ["Run-A", "run-a"])

    def test_blank(self):
        with self.assertRaises(ValueError) as ctx:
            self.parse("")
        self.assertIn("at least one nonblank", str(ctx.exception))

    def test_commas_only(self):
        with self.assertRaises(ValueError) as ctx:
            self.parse(",,,")
        self.assertIn("at least one nonblank", str(ctx.exception))

    def test_whitespace_and_commas(self):
        with self.assertRaises(ValueError) as ctx:
            self.parse(" , , ")
        self.assertIn("at least one nonblank", str(ctx.exception))

    def test_twenty_unique_accepted(self):
        runs = [f"run_{i:02d}" for i in range(1, 21)]
        parsed = self.parse(",".join(runs))
        self.assertEqual(parsed, runs)
        self.assertEqual(len(parsed), 20)

    def test_twenty_one_unique_rejected(self):
        runs = [f"run_{i:02d}" for i in range(1, 22)]
        with self.assertRaises(ValueError) as ctx:
            self.parse(",".join(runs))
        self.assertIn("supports at most 20 unique values", str(ctx.exception))


class TestOldParameterRemoval(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.SRC = shared_nb("NB14_RetryFailedTables.py")

    def test_no_original_run_id_widget_declaration(self):
        self.assertNotIn('dbutils.widgets.text("original_run_id"', self.SRC)

    def test_no_original_run_id_widget_get(self):
        self.assertNotIn('dbutils.widgets.get("original_run_id"', self.SRC)

    def test_original_run_ids_declared_exactly_once(self):
        count = len(re.findall(r'dbutils\.widgets\.text\("original_run_ids"', self.SRC))
        self.assertEqual(count, 1)

    def test_no_fallback_to_original_run_id(self):
        # Removing "original_run_ids" should leave zero occurrences of "original_run_id"
        stripped = self.SRC.replace("original_run_ids", "")
        self.assertNotIn("original_run_id", stripped)

    def test_parent_run_id_retained(self):
        self.assertIn("parent_run_id", self.SRC)


class TestSqlSafety(unittest.TestCase):
    def test_independent_escaping_and_filter_format(self):
        spark = NB14SparkDouble()
        run_nb14_harness(
            widget_values={"original_run_ids": "run-a,run-b"},
            spark=spark,
        )
        last_sql = spark.last_sql()
        self.assertIn("run_id IN ('run-a', 'run-b')", last_sql)
        self.assertNotIn("IN ()", last_sql)

    def test_no_raw_widget_interpolation(self):
        spark = NB14SparkDouble()
        run_nb14_harness(
            widget_values={"original_run_ids": " run-a , run-b "},
            spark=spark,
        )
        last_sql = spark.last_sql()
        self.assertIn("run_id IN ('run-a', 'run-b')", last_sql)
        self.assertNotIn("run_id IN ( run-a , run-b )", last_sql)

    def test_apostrophe_handled_safely(self):
        spark = NB14SparkDouble()
        run_nb14_harness(
            widget_values={"original_run_ids": "run-a,run'oops"},
            spark=spark,
        )
        last_sql = spark.last_sql()
        self.assertIn("run_id IN ('run-a', 'run''oops')", last_sql)

    def test_blank_input_fails_before_sql(self):
        spark = NB14SparkDouble()
        with self.assertRaises(ValueError):
            run_nb14_harness(
                widget_values={"original_run_ids": "  ,  "},
                spark=spark,
            )
        self.assertEqual(len(spark.executed), 0)

    def test_twenty_one_ids_fail_before_sql(self):
        spark = NB14SparkDouble()
        runs = ",".join(f"run_{i}" for i in range(21))
        with self.assertRaises(ValueError):
            run_nb14_harness(
                widget_values={"original_run_ids": runs},
                spark=spark,
            )
        self.assertEqual(len(spark.executed), 0)


class TestMultiParentIdentity(unittest.TestCase):
    def test_same_connection_table_op_under_two_runs_produces_two_items(self):
        rows = [
            _failed_row("FULL_LOAD", source_table_id="t1", connection_id="c1", run_id="run-a"),
            _failed_row("FULL_LOAD", source_table_id="t1", connection_id="c1", run_id="run-b"),
        ]
        out = run_nb14_harness(
            widget_values={"original_run_ids": "run-a,run-b"},
            table_run_log_rows=rows,
        )
        worklist = out["result"]["worklist"]
        self.assertEqual(len(worklist), 2)
        parents = [item["parent_run_id"] for item in worklist]
        self.assertEqual(parents, ["run-a", "run-b"])
        for item in worklist:
            self.assertEqual(item["connection_id"], "c1")
            self.assertEqual(item["source_table_id"], "t1")
            self.assertEqual(item["operation"], "FULL_LOAD")

    def test_parent_run_in_partition_by(self):
        spark = NB14SparkDouble()
        run_nb14_harness(
            widget_values={"original_run_ids": "run-a,run-b"},
            spark=spark,
        )
        self.assertIn(
            "PARTITION BY connection_id, source_table_id, operation, run_id",
            spark.last_sql()
        )

    def test_dedup_isolation_across_parents(self):
        rows = [
            _failed_row("FULL_LOAD", source_table_id="t1", connection_id="c1", run_id="run-a", attempt_number=1),
            _failed_row("FULL_LOAD", source_table_id="t1", connection_id="c1", run_id="run-b", attempt_number=1),
        ]
        out = run_nb14_harness(
            widget_values={"original_run_ids": "run-a,run-b"},
            table_run_log_rows=rows,
        )
        worklist = out["result"]["worklist"]
        self.assertEqual(len(worklist), 2)
        self.assertEqual({w["parent_run_id"] for w in worklist}, {"run-a", "run-b"})

    def test_one_run_cannot_suppress_another_or_replace_parent_run_id(self):
        rows = [
            _failed_row("FULL_LOAD", source_table_id="t1", connection_id="c1", run_id="run-a"),
            _failed_row("FULL_LOAD", source_table_id="t1", connection_id="c1", run_id="run-b"),
        ]
        out = run_nb14_harness(
            widget_values={"original_run_ids": "run-a,run-b"},
            table_run_log_rows=rows,
        )
        worklist = out["result"]["worklist"]
        item_a = [w for w in worklist if w["parent_run_id"] == "run-a"][0]
        item_b = [w for w in worklist if w["parent_run_id"] == "run-b"][0]
        self.assertEqual(item_a["parent_run_id"], "run-a")
        self.assertEqual(item_b["parent_run_id"], "run-b")


class TestAttemptIsolation(unittest.TestCase):
    def test_attempt_numbers_calculated_independently(self):
        rows = [
            _failed_row("FULL_LOAD", source_table_id="t1", connection_id="c1", run_id="run-a", attempt_number=3),
            _failed_row("FULL_LOAD", source_table_id="t1", connection_id="c1", run_id="run-b", attempt_number=1),
        ]
        out = run_nb14_harness(
            widget_values={"original_run_ids": "run-a,run-b", "max_retries": "3"},
            table_run_log_rows=rows,
        )
        worklist = out["result"]["worklist"]
        self.assertEqual(len(worklist), 2)
        item_a = [w for w in worklist if w["parent_run_id"] == "run-a"][0]
        item_b = [w for w in worklist if w["parent_run_id"] == "run-b"][0]
        self.assertEqual(item_a["previous_attempt_number"], 3)
        self.assertEqual(item_a["attempt_number"], 4)
        self.assertEqual(item_b["previous_attempt_number"], 1)
        self.assertEqual(item_b["attempt_number"], 2)

    def test_retry_limits_isolated_exhausted_enters_manual_review(self):
        rows = [
            # max_retries = 3 means attempt 4 is exhausted (retry_count = 3 >= 3)
            _failed_row("FULL_LOAD", source_table_id="t1", connection_id="c1", run_id="run-a", attempt_number=4),
            _failed_row("FULL_LOAD", source_table_id="t1", connection_id="c1", run_id="run-b", attempt_number=1),
        ]
        out = run_nb14_harness(
            widget_values={"original_run_ids": "run-a,run-b", "max_retries": "3"},
            table_run_log_rows=rows,
        )
        worklist = out["result"]["worklist"]
        manual = out["result"]["manual_review_items"]
        self.assertEqual(len(worklist), 1)
        self.assertEqual(worklist[0]["parent_run_id"], "run-b")
        self.assertEqual(worklist[0]["attempt_number"], 2)
        self.assertEqual(len(manual), 1)
        self.assertEqual(manual[0]["parent_run_id"], "run-a")
        self.assertEqual(manual[0]["reason"], fc.MAX_RETRIES_REACHED)


class TestFullLoadMultiRun(unittest.TestCase):
    def test_full_load_multi_run_different_tables(self):
        rows = [
            _failed_row("FULL_LOAD", source_table_id="t1", connection_id="c1", run_id="run-a", attempt_number=1),
            _failed_row("FULL_LOAD", source_table_id="t2", connection_id="c2", run_id="run-b", attempt_number=2),
        ]
        out = run_nb14_harness(
            widget_values={"original_run_ids": "run-a,run-b", "operation": "FULL_LOAD", "pipeline_name": "INGEST"},
            table_run_log_rows=rows,
        )
        worklist = out["result"]["worklist"]
        self.assertEqual(len(worklist), 2)
        item_a = [w for w in worklist if w["parent_run_id"] == "run-a"][0]
        item_b = [w for w in worklist if w["parent_run_id"] == "run-b"][0]
        self.assertEqual(item_a["recovery_action"], fc.RETRY_FULL_LOAD)
        self.assertEqual(item_a["connection_id"], "c1")
        self.assertEqual(item_a["source_table_id"], "t1")
        self.assertEqual(item_a["attempt_number"], 2)
        self.assertEqual(item_b["recovery_action"], fc.RETRY_FULL_LOAD)
        self.assertEqual(item_b["connection_id"], "c2")
        self.assertEqual(item_b["source_table_id"], "t2")
        self.assertEqual(item_b["attempt_number"], 3)
        self.assertNotIn("run-a,run-b", [w["parent_run_id"] for w in worklist])

    def test_full_load_multi_run_same_table(self):
        rows = [
            _failed_row("FULL_LOAD", source_table_id="t1", connection_id="c1", run_id="run-a", attempt_number=1),
            _failed_row("FULL_LOAD", source_table_id="t1", connection_id="c1", run_id="run-b", attempt_number=1),
        ]
        out = run_nb14_harness(
            widget_values={"original_run_ids": "run-a,run-b", "operation": "FULL_LOAD"},
            table_run_log_rows=rows,
        )
        worklist = out["result"]["worklist"]
        self.assertEqual(len(worklist), 2)
        self.assertEqual({w["parent_run_id"] for w in worklist}, {"run-a", "run-b"})


class TestDeltaMultiRun(unittest.TestCase):
    def test_delta_multi_run_actions(self):
        rows = [
            _failed_row("DELTA_MERGE", source_table_id="t1", connection_id="c1", run_id="run-a",
                        failure_stage=fc.SOURCE_READ, retry_eligible=True,
                        lower_watermark="lw_a", upper_watermark="up_a"),
            _failed_row("DELTA_MERGE", source_table_id="t2", connection_id="c2", run_id="run-b",
                        failure_stage=fc.CHECKPOINT, retry_eligible=False),
            _failed_row("DELTA_APPEND", source_table_id="t3", connection_id="c3", run_id="run-c",
                        failure_stage=fc.QUEUE_FINALIZATION, retry_eligible=False),
        ]
        out = run_nb14_harness(
            widget_values={"original_run_ids": "run-a,run-b,run-c", "pipeline_name": "INGEST", "operation": ""},
            table_run_log_rows=rows,
        )
        worklist = out["result"]["worklist"]
        self.assertEqual(len(worklist), 3)

        item_a = [w for w in worklist if w["parent_run_id"] == "run-a"][0]
        self.assertEqual(item_a["recovery_action"], fc.RETRY_DELTA_APPLY)
        self.assertEqual(item_a["source_table_id"], "t1")
        self.assertEqual(item_a["retry_lower_watermark"], "lw_a")
        self.assertEqual(item_a["retry_upper_watermark"], "up_a")

        item_b = [w for w in worklist if w["parent_run_id"] == "run-b"][0]
        self.assertEqual(item_b["recovery_action"], fc.RETRY_CHECKPOINT_ONLY)
        self.assertEqual(item_b["source_table_id"], "t2")

        item_c = [w for w in worklist if w["parent_run_id"] == "run-c"][0]
        self.assertEqual(item_c["recovery_action"], fc.RETRY_QUEUE_FINALIZATION_ONLY)
        self.assertEqual(item_c["source_table_id"], "t3")

        for item in worklist:
            self.assertNotEqual(item["parent_run_id"], "run-a,run-b,run-c")


class TestFrozenBounds(unittest.TestCase):
    def test_frozen_bounds_preserved_per_parent_run(self):
        rows = [
            _failed_row("DELTA_MERGE", source_table_id="t1", connection_id="c1", run_id="run-a",
                        lower_watermark="low_100", upper_watermark="high_200"),
            _failed_row("DELTA_MERGE", source_table_id="t2", connection_id="c2", run_id="run-b",
                        lower_watermark="low_300", upper_watermark="high_400"),
            _failed_row("DELTA_MERGE", source_table_id="t3", connection_id="c3", run_id="run-c",
                        failure_stage=fc.CHECKPOINT, retry_eligible=False,
                        lower_watermark=None, upper_watermark=None),
        ]
        out = run_nb14_harness(
            widget_values={"original_run_ids": "run-a,run-b,run-c", "pipeline_name": "INGEST"},
            table_run_log_rows=rows,
        )
        worklist = out["result"]["worklist"]
        item_a = [w for w in worklist if w["parent_run_id"] == "run-a"][0]
        item_b = [w for w in worklist if w["parent_run_id"] == "run-b"][0]
        item_c = [w for w in worklist if w["parent_run_id"] == "run-c"][0]

        self.assertEqual(item_a["retry_lower_watermark"], "low_100")
        self.assertEqual(item_a["retry_upper_watermark"], "high_200")
        self.assertEqual(item_b["retry_lower_watermark"], "low_300")
        self.assertEqual(item_b["retry_upper_watermark"], "high_400")
        self.assertIsNone(item_c["retry_lower_watermark"])
        self.assertIsNone(item_c["retry_upper_watermark"])


class TestManualReviewSeparation(unittest.TestCase):
    def test_exhausted_and_eligible_coexist(self):
        rows = [
            _failed_row("FULL_LOAD", source_table_id="t1", connection_id="c1", run_id="run-a", attempt_number=4),
            _failed_row("FULL_LOAD", source_table_id="t1", connection_id="c1", run_id="run-b", attempt_number=1),
        ]
        out = run_nb14_harness(
            widget_values={"original_run_ids": "run-a,run-b", "max_retries": "3"},
            table_run_log_rows=rows,
        )
        worklist = out["result"]["worklist"]
        manual = out["result"]["manual_review_items"]
        self.assertEqual(len(worklist), 1)
        self.assertEqual(worklist[0]["parent_run_id"], "run-b")
        self.assertEqual(len(manual), 1)
        self.assertEqual(manual[0]["parent_run_id"], "run-a")
        self.assertEqual(manual[0]["reason"], fc.MAX_RETRIES_REACHED)

    def test_two_manual_reviews_same_table_different_runs(self):
        rows = [
            _failed_row("FULL_LOAD", source_table_id="t1", connection_id="c1", run_id="run-a", attempt_number=4),
            _failed_row("FULL_LOAD", source_table_id="t1", connection_id="c1", run_id="run-b", attempt_number=5),
        ]
        out = run_nb14_harness(
            widget_values={"original_run_ids": "run-a,run-b", "max_retries": "3"},
            table_run_log_rows=rows,
        )
        manual = out["result"]["manual_review_items"]
        self.assertEqual(len(manual), 2)
        parents = {m["parent_run_id"] for m in manual}
        self.assertEqual(parents, {"run-a", "run-b"})


class TestSourceTableFilter(unittest.TestCase):
    def test_source_table_id_filter_across_multiple_parent_runs(self):
        rows = [
            _failed_row("FULL_LOAD", source_table_id="t1", connection_id="c1", run_id="run-a"),
            _failed_row("FULL_LOAD", source_table_id="t2", connection_id="c1", run_id="run-a"),
            _failed_row("FULL_LOAD", source_table_id="t1", connection_id="c2", run_id="run-b"),
            _failed_row("FULL_LOAD", source_table_id="t2", connection_id="c3", run_id="run-c"),
        ]
        out = run_nb14_harness(
            widget_values={"original_run_ids": "run-a,run-b,run-c", "source_table_id": "t1"},
            table_run_log_rows=rows,
        )
        worklist = out["result"]["worklist"]
        self.assertEqual(len(worklist), 2)
        for item in worklist:
            self.assertEqual(item["source_table_id"], "t1")
        self.assertEqual({w["parent_run_id"] for w in worklist}, {"run-a", "run-b"})


class TestOutputContract(unittest.TestCase):
    def test_result_payload_and_task_values(self):
        rows = [
            _failed_row("FULL_LOAD", source_table_id="t1", connection_id="c1", run_id="run-a", attempt_number=1),
            _failed_row("FULL_LOAD", source_table_id="t2", connection_id="c2", run_id="run-b", attempt_number=4),
        ]
        out = run_nb14_harness(
            widget_values={"original_run_ids": "run-a,run-b", "max_retries": "3"},
            table_run_log_rows=rows,
        )
        result = out["result"]
        self.assertEqual(result["original_run_ids"], ["run-a", "run-b"])
        self.assertEqual(result["original_run_count"], 2)
        self.assertNotIn("original_run_id", result)

        task_values = out["task_values"]
        self.assertNotIn("original_run_id", task_values)
        self.assertEqual(task_values["original_run_count"], 2)

        # Verify task values use valid JSON
        parsed_ids = json.loads(task_values["original_run_ids"])
        self.assertEqual(parsed_ids, ["run-a", "run-b"])
        parsed_worklist = json.loads(task_values["worklist"])
        self.assertEqual(len(parsed_worklist), 1)
        self.assertEqual(parsed_worklist[0]["parent_run_id"], "run-a")
        parsed_manual = json.loads(task_values["manual_review_items"])
        self.assertEqual(len(parsed_manual), 1)
        self.assertEqual(parsed_manual[0]["parent_run_id"], "run-b")

        # Verify worklist fields
        for field in fc.WORKLIST_FIELDS:
            self.assertIn(field, parsed_worklist[0])

        # Verify manual review fields + parent_run_id
        for field in fc.MANUAL_REVIEW_FIELDS:
            self.assertIn(field, parsed_manual[0])
        self.assertIn("parent_run_id", parsed_manual[0])


class TestClassifyFailure(unittest.TestCase):
    def test_timeout_is_retryable(self):
        c = fc.classify_failure(Exception("Read timed out"), fc.SOURCE_READ)
        self.assertEqual(c.category, fc.TIMEOUT)
        self.assertTrue(c.retry_eligible)

    def test_transient_connection_retryable(self):
        c = fc.classify_failure(Exception("Connection refused: connect"), fc.CONNECTION)
        self.assertEqual(c.category, fc.TRANSIENT_CONNECTION)
        self.assertTrue(c.retry_eligible)

    def test_transient_compute_retryable(self):
        c = fc.classify_failure(Exception("java.lang.OutOfMemoryError"), fc.TARGET_WRITE)
        self.assertEqual(c.category, fc.TRANSIENT_COMPUTE)
        self.assertTrue(c.retry_eligible)

    def test_source_permission_not_retryable(self):
        c = fc.classify_failure(Exception("ORA-01031: insufficient privileges"),
                                fc.SOURCE_READ)
        self.assertEqual(c.category, fc.SOURCE_PERMISSION)
        self.assertFalse(c.retry_eligible)

    def test_object_missing_not_retryable(self):
        c = fc.classify_failure(Exception("ORA-00942: table or view does not exist"),
                                fc.METADATA)
        self.assertEqual(c.category, fc.SOURCE_OBJECT_MISSING)
        self.assertFalse(c.retry_eligible)

    def test_mapping_error_not_retryable(self):
        c = fc.classify_failure(Exception("blocked datatype requires an explicit mapping"),
                                fc.MAPPING)
        self.assertEqual(c.category, fc.MAPPING_ERROR)
        self.assertFalse(c.retry_eligible)

    def test_configuration_error_not_retryable(self):
        c = fc.classify_failure(Exception("MERGE strategy requires primary_key_columns"),
                                fc.PROVISIONING)
        self.assertEqual(c.category, fc.CONFIGURATION_ERROR)
        self.assertFalse(c.retry_eligible)

    def test_reconciliation_error_not_retryable(self):
        c = fc.classify_failure(Exception("some generic failure"), fc.RECONCILIATION)
        self.assertEqual(c.category, fc.RECONCILIATION_ERROR)
        self.assertFalse(c.retry_eligible)

    def test_checkpoint_error_not_retryable(self):
        c = fc.classify_failure(Exception("generic"), fc.CHECKPOINT)
        self.assertEqual(c.category, fc.CHECKPOINT_ERROR)
        self.assertFalse(c.retry_eligible)

    def test_target_write_retryable_only_when_idempotent(self):
        idem = fc.classify_failure(Exception("write failed"), fc.TARGET_WRITE,
                                   idempotent=True)
        self.assertEqual(idem.category, fc.TARGET_WRITE_ERROR)
        self.assertTrue(idem.retry_eligible)
        non = fc.classify_failure(Exception("write failed"), fc.TARGET_WRITE,
                                  idempotent=False)
        self.assertFalse(non.retry_eligible)

    def test_message_is_sanitized(self):
        c = fc.classify_failure(
            Exception("login failed for user=sa;password=Secret123!"), fc.CONNECTION)
        self.assertNotIn("Secret123", c.sanitized_message)
        self.assertIn("***", c.sanitized_message)

    def test_unknown_stage_normalized(self):
        c = fc.classify_failure(Exception("weird"), "NOT_A_STAGE")
        self.assertEqual(c.stage, fc.UNKNOWN)


class TestRecoveryAction(unittest.TestCase):
    def test_checkpoint_only_does_not_reapply(self):
        self.assertEqual(fc.recovery_action("DELTA_MERGE", fc.CHECKPOINT),
                         "RETRY_CHECKPOINT_ONLY")

    def test_finalization_only_does_not_reapply(self):
        self.assertEqual(fc.recovery_action("DELTA_APPEND", fc.QUEUE_FINALIZATION),
                         "RETRY_QUEUE_FINALIZATION_ONLY")

    def test_full_load_retries_full(self):
        self.assertEqual(fc.recovery_action("FULL_LOAD", fc.SOURCE_READ),
                         "RETRY_FULL_LOAD")

    def test_delta_retries_delta(self):
        self.assertEqual(fc.recovery_action("DELTA_MERGE", fc.SOURCE_READ),
                         "RETRY_DELTA_APPLY")

    def test_etl_retries_etl(self):
        self.assertEqual(fc.recovery_action("ETL_INCREMENTAL", fc.DQ_VALIDATION),
                         "RETRY_ETL")

    def test_generic_etl_retries_etl(self):
        self.assertEqual(fc.recovery_action("ETL", fc.ETL_READ), "RETRY_ETL")

    def test_unknown_is_manual(self):
        self.assertEqual(fc.recovery_action("SOMETHING", fc.METADATA), "MANUAL_REVIEW")


class TestRetrySelectionPolicy(unittest.TestCase):
    def test_same_table_operations_remain_independent_before_filtering(self):
        selected = fc.latest_failed_attempts([
            _failed_row("FULL_LOAD"),
            _failed_row("ETL_INCREMENTAL", failure_stage=fc.ETL_READ),
        ])
        self.assertEqual(
            {fc.retry_work_identity(row) for row in selected},
            {("connection_1", "table_001", "FULL_LOAD"),
             ("connection_1", "table_001", "ETL_INCREMENTAL")})

    def test_same_table_operation_on_two_connections_remains_independent(self):
        selected = fc.latest_failed_attempts([
            _failed_row("FULL_LOAD", connection_id="ORA_FIN_READ"),
            _failed_row("FULL_LOAD", connection_id="ORA_FIN_MIGRATION"),
        ])
        self.assertEqual(len(selected), 2)
        self.assertEqual(
            {row["connection_id"] for row in selected},
            {"ORA_FIN_READ", "ORA_FIN_MIGRATION"})

    def test_same_table_ingest_operations_are_both_selected(self):
        selected = fc.select_failed_attempts([
            _failed_row("FULL_LOAD"),
            _failed_row("CHECKPOINT_RECOVERY", failure_stage=fc.CHECKPOINT,
                        retry_eligible=False),
        ], "INGEST")
        self.assertEqual(
            {row["operation"] for row in selected},
            {"FULL_LOAD", "CHECKPOINT_RECOVERY"})

    def test_latest_attempt_is_selected_per_operation(self):
        selected = fc.latest_failed_attempts([
            _failed_row("FULL_LOAD", attempt_number=1),
            _failed_row("FULL_LOAD", attempt_number=2),
            _failed_row("ETL_INCREMENTAL", attempt_number=1,
                        failure_stage=fc.ETL_READ),
        ])
        attempts = {row["operation"]: row["attempt_number"] for row in selected}
        self.assertEqual(attempts, {"FULL_LOAD": 2, "ETL_INCREMENTAL": 1})

    def test_attempts_do_not_cross_operations(self):
        rows = [
            _failed_row("FULL_LOAD", attempt_number=3),
            _failed_row("ETL_INCREMENTAL", attempt_number=1,
                        failure_stage=fc.ETL_READ),
        ]
        etl_rows = fc.select_failed_attempts(rows, "ETL")
        worklist, manual_items, _duplicates = fc.build_retry_collections(
            etl_rows, "child", "parent", "ETL", max_retries=3)
        self.assertEqual(manual_items, [])
        self.assertEqual(worklist[0]["previous_attempt_number"], 1)
        self.assertEqual(worklist[0]["attempt_number"], 2)

    def test_blank_operation_returns_all_owned_operations(self):
        rows = [
            _failed_row("FULL_LOAD"),
            _failed_row("DELTA_MERGE", source_table_id="table_002"),
            _failed_row("ETL_INCREMENTAL", failure_stage=fc.ETL_READ),
        ]
        selected = fc.select_failed_attempts(rows, "INGEST", "")
        self.assertEqual(
            {row["operation"] for row in selected},
            {"FULL_LOAD", "DELTA_MERGE"})

    def test_exact_operation_filter(self):
        rows = [
            _failed_row("FULL_LOAD"),
            _failed_row("DELTA_FULL_REFRESH", source_table_id="table_002"),
        ]
        selected = fc.select_failed_attempts(rows, "INGEST", "full_load")
        self.assertEqual([row["operation"] for row in selected], ["FULL_LOAD"])

    def test_pipeline_filtering_is_bidirectional(self):
        rows = [
            _failed_row("FULL_LOAD"),
            _failed_row("ETL_INCREMENTAL", failure_stage=fc.ETL_READ),
        ]
        self.assertEqual(
            [row["operation"] for row in
             fc.select_failed_attempts(rows, "INGEST")],
            ["FULL_LOAD"])
        self.assertEqual(
            [row["operation"] for row in fc.select_failed_attempts(rows, "ETL")],
            ["ETL_INCREMENTAL"])

    def test_pipeline_operation_mismatch_fails(self):
        with self.assertRaisesRegex(ValueError, "does not belong"):
            fc.select_failed_attempts([], "ETL", "FULL_LOAD")

    def test_unknown_pipeline_fails(self):
        with self.assertRaisesRegex(ValueError, "Unsupported pipeline_name"):
            fc.select_failed_attempts([], "UNKNOWN")

    def test_attempt_number_precedes_timestamps(self):
        selected = fc.latest_failed_attempts([
            _failed_row("FULL_LOAD", attempt_number=1,
                        ended_ts="2026-01-03T00:00:00Z", run_id="run_z"),
            _failed_row("FULL_LOAD", attempt_number=2,
                        ended_ts="2026-01-01T00:00:00Z", run_id="run_a"),
        ])
        self.assertEqual(selected[0]["attempt_number"], 2)

    def test_connection_is_part_of_retry_identity(self):
        selected = fc.latest_failed_attempts([
            _failed_row("FULL_LOAD", run_id="run_a", connection_id="old"),
            _failed_row("FULL_LOAD", run_id="run_b", connection_id="new"),
        ])
        self.assertEqual(len(selected), 2)

    def test_retry_limit_is_operation_specific(self):
        full_work, full_manual, _ = fc.build_retry_collections(
            [_failed_row("FULL_LOAD", attempt_number=4)],
            "child", "parent", "INGEST", max_retries=3)
        etl_work, etl_manual, _ = fc.build_retry_collections(
            [_failed_row("ETL_INCREMENTAL", attempt_number=1,
                         failure_stage=fc.ETL_READ)],
            "child", "parent", "ETL", max_retries=3)
        self.assertEqual(full_work, [])
        self.assertEqual(full_manual[0]["reason"], fc.MAX_RETRIES_REACHED)
        self.assertEqual(etl_manual, [])
        self.assertEqual(etl_work[0]["attempt_number"], 2)

    def test_manual_review_is_not_executable(self):
        worklist, manual_items, _ = fc.build_retry_collections(
            [_failed_row("DELTA_MERGE", failure_stage=fc.RECONCILIATION,
                         retry_eligible=False)],
            "child", "parent", "INGEST", max_retries=3)
        self.assertEqual(worklist, [])
        self.assertEqual(manual_items[0]["reason"], fc.NOT_RETRY_ELIGIBLE)
        self.assertEqual(set(manual_items[0]), set(fc.MANUAL_REVIEW_FIELDS))

    def test_state_only_recovery_remains_selectable(self):
        worklist, manual_items, _ = fc.build_retry_collections(
            [_failed_row("CHECKPOINT_RECOVERY", failure_stage=fc.CHECKPOINT,
                         retry_eligible=False)],
            "child", "parent", "INGEST", max_retries=3)
        self.assertEqual(manual_items, [])
        self.assertEqual(worklist[0]["recovery_action"],
                         fc.RETRY_CHECKPOINT_ONLY)

    def test_worklist_uniqueness(self):
        duplicate = _failed_row("FULL_LOAD")
        worklist, manual_items, duplicates = fc.build_retry_collections(
            [duplicate, duplicate], "child", "parent", "INGEST", max_retries=3)
        self.assertEqual(manual_items, [])
        self.assertEqual(len(worklist), 1)
        self.assertEqual(
            duplicates,
            [("connection_1", "table_001", "FULL_LOAD",
              fc.RETRY_FULL_LOAD)])

    def test_retry_row_requires_connection_id(self):
        with self.assertRaisesRegex(ValueError, "connection_id"):
            fc.retry_work_identity(_failed_row("FULL_LOAD", connection_id=""))

    def test_executable_output_contract(self):
        work_item, manual_item = fc.build_retry_item(
            _failed_row("FULL_LOAD"), "child", "parent", "INGEST", 3)
        self.assertIsNone(manual_item)
        self.assertEqual(tuple(work_item), fc.WORKLIST_FIELDS)
        self.assertEqual(work_item["run_id"], "child")
        self.assertEqual(work_item["parent_run_id"], "parent")
        self.assertEqual(work_item["pipeline_name"], "INGEST")

    def test_frozen_bounds_are_retained(self):
        delta_item, _ = fc.build_retry_item(
            _failed_row("DELTA_MERGE", lower_watermark="lower-delta",
                        upper_watermark="upper-delta"),
            "child", "parent", "INGEST", 3)
        etl_item, _ = fc.build_retry_item(
            _failed_row("ETL_INCREMENTAL", failure_stage=fc.ETL_READ,
                        lower_watermark="lower-etl", upper_watermark="upper-etl"),
            "child", "parent", "ETL", 3)
        self.assertEqual(
            (delta_item["retry_lower_watermark"],
              delta_item["retry_upper_watermark"]),
            ("lower-delta", "upper-delta"))
        self.assertEqual(
            (etl_item["retry_lower_watermark"],
              etl_item["retry_upper_watermark"]),
            ("lower-etl", "upper-etl"))


if __name__ == "__main__":
    unittest.main()

