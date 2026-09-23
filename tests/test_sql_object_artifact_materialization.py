import unittest
import os
import sys
import builtins
import tempfile
import shutil
import py_compile
import json
import uuid
from datetime import datetime, timezone
from src import sql_object_artifact_common as sqlobj_art
from _nbsource import (
    SHARED_NOTEBOOKS, REQUIRED_SOURCE_NOTEBOOKS, shared_nb,
    all_shared_notebooks, all_source_notebooks,
)
import _modscan as modscan

NB18_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "notebooks", "shared", "NB18_MaterializeSourceArtifacts.py"
)


def load_nb18_code():
    with open(NB18_PATH, "r", encoding="utf-8") as f:
        lines = f.readlines()
    clean_lines = []
    for line in lines:
        if line.strip().startswith("%run") or line.strip().startswith("# MAGIC %run"):
            continue
        clean_lines.append(line)
    return "".join(clean_lines)


class FakeRow:
    """Fake PySpark Row that deliberately does NOT implement a .get() method."""
    def __init__(self, **values):
        if len(values) == 1 and "data" in values and isinstance(values["data"], dict):
            self._values = values["data"]
        else:
            self._values = values

    def asDict(self, recursive=True):
        return dict(self._values)


class FakeFileIO:
    def __init__(self, fs, path):
        self.fs = fs
        self.path = path
        self.content = ""

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            self.fs.write_file(self.path, self.content)

    def write(self, text):
        self.content += text


class FakeFilesystem:
    def __init__(self, events=None):
        self.files = {}
        self.written_paths = []
        self.final_paths = []
        self.removed_paths = []
        self.write_count = 0
        self.replace_count = 0
        self.fail_write = False
        self.fail_volume_creation = False
        self.events = events

    def exists(self, path):
        return path in self.files

    def makedirs(self, path, exist_ok=True):
        pass

    def replace(self, src, dst):
        if src in self.files:
            self.files[dst] = self.files.pop(src)
            self.final_paths.append(dst)
            self.replace_count += 1
            if self.events is not None:
                self.events.append("replace_final")
        else:
            raise FileNotFoundError(src)

    def remove(self, path):
        if path in self.files:
            del self.files[path]
        self.removed_paths.append(path)

    def write_file(self, path, content):
        if self.fail_write:
            raise IOError("Disk write error")
        self.files[path] = content
        self.written_paths.append(path)
        self.write_count += 1
        if self.events is not None:
            self.events.append("write_temp")


class FakeDataFrame:
    def __init__(self, rows, schema=None, spark=None):
        self.rows = rows
        self.schema = schema
        self.spark = spark

    def collect(self):
        return self.rows

    def createOrReplaceTempView(self, name):
        if self.spark:
            self.spark.temp_views[name] = self.rows


class FakeSparkSession:
    def __init__(self, candidates=None, manifest_rows=None, fs=None, pre_claim_hook=None, pre_finalize_hook=None, pre_general_merge_hook=None, events=None):
        self.candidates = candidates or []
        self.manifest_table = {}
        self.initial_manifest_rows = list(manifest_rows or [])
        for r in (manifest_rows or []):
            d = _row_to_dict_helper(r)
            key = (d.get("connection_id"), d.get("source_schema"), d.get("object_type"), d.get("object_name"))
            self.manifest_table[key] = dict(d)
        self.fs = fs
        self.temp_views = {}
        self.created_dfs = []
        self.executed_queries = []
        self.merge_executed = False
        self.fail_merge = False
        self.pre_claim_hook = pre_claim_hook
        self.pre_finalize_hook = pre_finalize_hook
        self.pre_general_merge_hook = pre_general_merge_hook
        self.events = events if events is not None else []

    def sql(self, query):
        self.executed_queries.append(query)
        q = query.strip()
        if "CREATE VOLUME" in q and self.fs and getattr(self.fs, "fail_volume_creation", False):
            raise RuntimeError("Volume creation denied by catalog permissions")
        if "CREATE VOLUME" in q:
            self.events.append("create_volume")
            return FakeDataFrame([])
        if "CREATE SCHEMA" in q:
            return FakeDataFrame([])
        if "ranked" in q or "sql_object_assessment" in q:
            return FakeDataFrame(self.candidates)

        if "MERGE INTO" in q:
            if self.fail_merge:
                raise RuntimeError("Delta MERGE transaction error")
            self.merge_executed = True

            if "_claim_owner_" in q:
                self.events.append("claim_merge")
                if self.pre_claim_hook:
                    self.pre_claim_hook(self)
                for view_name, rows in list(self.temp_views.items()):
                    if view_name in q:
                        claim_row = rows[0]
                        key = (claim_row.get("connection_id"), claim_row.get("source_schema"), claim_row.get("object_type"), claim_row.get("object_name"))
                        existing = self.manifest_table.get(key)
                        if not existing:
                            self.manifest_table[key] = dict(claim_row)
                        else:
                            stale = False
                            upd = existing.get("updated_ts")
                            if upd is None:
                                stale = True
                            elif isinstance(upd, datetime):
                                stale = (datetime(2026, 9, 21, 12, 0, 0, tzinfo=timezone.utc) - upd).total_seconds() > 120 * 60
                            elif isinstance(upd, str):
                                try:
                                    dt = datetime.fromisoformat(upd)
                                    if dt.tzinfo is None:
                                        dt = dt.replace(tzinfo=timezone.utc)
                                    stale = (datetime(2026, 9, 21, 12, 0, 0, tzinfo=timezone.utc) - dt).total_seconds() > 120 * 60
                                except Exception:
                                    stale = False

                            if (
                                existing.get("materialization_status") != "IN_PROGRESS"
                                or existing.get("last_materialized_run_id") == claim_row.get("last_materialized_run_id")
                                or stale
                            ):
                                updated = dict(claim_row)
                                updated["created_ts"] = existing.get("created_ts", claim_row.get("created_ts"))
                                self.manifest_table[key] = updated
                        break
                return FakeDataFrame([])

            if "_finalize_owner_" in q:
                self.events.append("finalize_update")
                if self.pre_finalize_hook:
                    self.pre_finalize_hook(self)
                for view_name, rows in list(self.temp_views.items()):
                    if view_name in q:
                        final_row = rows[0]
                        key = (final_row.get("connection_id"), final_row.get("source_schema"), final_row.get("object_type"), final_row.get("object_name"))
                        existing = self.manifest_table.get(key)
                        if (
                            existing
                            and existing.get("last_materialized_run_id") == final_row.get("last_materialized_run_id")
                            and existing.get("materialization_status") == "IN_PROGRESS"
                        ):
                            updated = dict(final_row)
                            updated["created_ts"] = existing.get("created_ts", final_row.get("created_ts"))
                            self.manifest_table[key] = updated
                        break
                return FakeDataFrame([])

            if "_manifest_updates" in q:
                if self.pre_general_merge_hook:
                    self.pre_general_merge_hook(self)
                for view_name, rows in list(self.temp_views.items()):
                    if "_manifest_updates" in view_name:
                        for r in rows:
                            key = (r.get("connection_id"), r.get("source_schema"), r.get("object_type"), r.get("object_name"))
                            existing = self.manifest_table.get(key)
                            if existing:
                                stale = False
                                upd = existing.get("updated_ts")
                                if upd is None:
                                    stale = True
                                elif isinstance(upd, datetime):
                                    stale = (datetime(2026, 9, 21, 12, 0, 0, tzinfo=timezone.utc) - upd).total_seconds() > 120 * 60
                                elif isinstance(upd, str):
                                    try:
                                        dt = datetime.fromisoformat(upd)
                                        if dt.tzinfo is None:
                                            dt = dt.replace(tzinfo=timezone.utc)
                                        stale = (datetime(2026, 9, 21, 12, 0, 0, tzinfo=timezone.utc) - dt).total_seconds() > 120 * 60
                                    except Exception:
                                        stale = False

                                if (
                                    existing.get("materialization_status") != "IN_PROGRESS"
                                    or not existing.get("last_materialized_run_id")
                                    or str(existing.get("last_materialized_run_id")).strip() == ""
                                    or existing.get("last_materialized_run_id") == r.get("last_materialized_run_id")
                                    or stale
                                ):
                                    upd_dict = dict(r)
                                    upd_dict["created_ts"] = existing.get("created_ts", r.get("created_ts"))
                                    self.manifest_table[key] = upd_dict
                            else:
                                self.manifest_table[key] = dict(r)
                        break
                return FakeDataFrame([])

            return FakeDataFrame([])

        if "sql_object_artifact_manifest" in q:
            if "WHERE" in q:
                import re
                conn_m = re.search(r"connection_id = '([^']*)'", q)
                sch_m = re.search(r"source_schema = '([^']*)'", q)
                type_m = re.search(r"object_type = '([^']*)'", q)
                name_m = re.search(r"object_name = '([^']*)'", q)
                if "claim_reread" not in self.events and "claim_merge" in self.events:
                    self.events.append("claim_reread")
                elif "finalize_reread" not in self.events and "finalize_update" in self.events:
                    self.events.append("finalize_reread")
                if conn_m and sch_m and type_m and name_m:
                    key = (conn_m.group(1), sch_m.group(1), type_m.group(1), name_m.group(1))
                    row_dict = self.manifest_table.get(key)
                    if row_dict:
                        return FakeDataFrame([FakeRow(**row_dict)])
                    return FakeDataFrame([])
            self.events.append("read_initial_manifest")
            if self.initial_manifest_rows:
                return FakeDataFrame([FakeRow(**_row_to_dict_helper(r)) for r in self.initial_manifest_rows])
            return FakeDataFrame([FakeRow(**r) for r in self.manifest_table.values()])

        return FakeDataFrame([])

    def createDataFrame(self, updates, schema=None):
        df = FakeDataFrame(updates, schema=schema, spark=self)
        self.created_dfs.append(df)
        return df


class FakeControlRepo:
    def __init__(self, target_configs=None):
        self.target_configs = target_configs or {}

    def resolve_target_config(self, conn_id):
        if conn_id in self.target_configs:
            cfg = self.target_configs[conn_id]
            if isinstance(cfg, Exception):
                raise cfg
            return cfg
        return {
            "target_catalog": "da_accelerators",
            "target_schema_mode": "SAME_AS_SOURCE",
            "target_schema": None,
        }


class FakeWidgets:
    def __init__(self, values=None):
        self.values = values or {}

    def text(self, name, default):
        pass

    def get(self, name):
        return self.values.get(name, "")


class FakeJobTaskValues:
    def __init__(self):
        self.task_values = {}

    def set(self, key, value):
        self.task_values[key] = value


class FakeNotebookExit(Exception):
    def __init__(self, payload):
        self.payload = payload


class FakeDbutils:
    def __init__(self, widget_values=None):
        self.widgets = FakeWidgets(widget_values)
        self.jobs = type("FakeJobs", (), {"taskValues": FakeJobTaskValues()})()
        self.notebook = type("FakeNotebook", (), {"exit": lambda _self, payload: self._exit(payload)})()

    def _exit(self, payload):
        raise FakeNotebookExit(payload)


class DummyType:
    def __init__(self, *args, **kwargs):
        pass


class StructType(DummyType):
    def __init__(self, fields=None):
        self.fields = fields or []


class StructField(DummyType):
    def __init__(self, name, dataType=None, nullable=True):
        self.name = name
        self.dataType = dataType
        self.nullable = nullable


class StringType(DummyType):
    pass


class TimestampType(DummyType):
    pass


def run_nb18_harness(
    candidates=None,
    manifest_rows=None,
    target_configs=None,
    widget_values=None,
    fs=None,
    fail_merge=False,
    pre_claim_hook=None,
    pre_finalize_hook=None,
    pre_general_merge_hook=None,
    events=None,
):
    if events is None:
        events = []
    if fs is None:
        fs = FakeFilesystem(events=events)
    else:
        fs.events = events
    spark = FakeSparkSession(
        candidates=candidates,
        manifest_rows=manifest_rows,
        fs=fs,
        pre_claim_hook=pre_claim_hook,
        pre_finalize_hook=pre_finalize_hook,
        pre_general_merge_hook=pre_general_merge_hook,
        events=events,
    )
    spark.fail_merge = fail_merge
    repo = FakeControlRepo(target_configs=target_configs)
    dbutils = FakeDbutils(widget_values=widget_values)
    task_values = {}

    def set_task_value(k, v):
        task_values[k] = v

    code = load_nb18_code()

    pyspark_types = type("pyspark_types", (), {
        "StructType": StructType,
        "StructField": StructField,
        "StringType": StringType,
        "TimestampType": TimestampType,
    })
    pyspark_sql = type("pyspark_sql", (), {"types": pyspark_types})
    pyspark_mod = type("pyspark", (), {"sql": pyspark_sql})

    sys.modules["pyspark"] = pyspark_mod
    sys.modules["pyspark.sql"] = pyspark_sql
    sys.modules["pyspark.sql.types"] = pyspark_types

    def now_utc():
        return datetime(2026, 9, 21, 12, 0, 0, tzinfo=timezone.utc)

    class DummyFailcls:
        @staticmethod
        def sanitize_message(e):
            return str(e)

    class DummyDDL:
        @staticmethod
        def build_create_schema(cat, sch):
            return f"CREATE SCHEMA IF NOT EXISTS {cat}.{sch}"

    env = {
        "__file__": NB18_PATH,
        "__name__": "__main__",
        "spark": spark,
        "dbutils": dbutils,
        "control_repo": lambda: repo,
        "now_utc": now_utc,
        "get_run_id": lambda: widget_values.get("run_id", "run-test-100") if widget_values else "run-test-100",
        "CATALOG": "da_accelerators",
        "CONTROL_SCHEMA": "control",
        "quote_databricks": lambda s: f"`{s}`",
        "escape_string_literal": lambda s: f"'{s}'",
        "normalize_source_system": lambda s: str(s).lower().strip(),
        "failcls": DummyFailcls,
        "ddl": DummyDDL,
        "set_task_value": set_task_value,
        "sqlobj_art": sqlobj_art,
        "StructType": StructType,
        "StructField": StructField,
        "StringType": StringType,
        "TimestampType": TimestampType,
        "os": os,
        "uuid": uuid,
        "json": json,
        "datetime": datetime,
        "timezone": timezone,
    }

    orig_os_makedirs = os.makedirs
    orig_os_replace = os.replace
    orig_os_remove = os.remove
    orig_os_path_exists = os.path.exists
    orig_builtins_open = builtins.open

    def fake_makedirs(path, exist_ok=True):
        fs.makedirs(path, exist_ok=exist_ok)

    def fake_replace(src, dst):
        fs.replace(src, dst)

    def fake_remove(path):
        fs.remove(path)

    def fake_exists(path):
        return fs.exists(path)

    def fake_open(path, mode="r", **kwargs):
        if "w" in mode and isinstance(path, str) and "/Volumes/" in path:
            return FakeFileIO(fs, path)
        return orig_builtins_open(path, mode, **kwargs)

    os.makedirs = fake_makedirs
    os.replace = fake_replace
    os.remove = fake_remove
    os.path.exists = fake_exists
    builtins.open = fake_open

    error_raised = None
    exit_payload = None

    try:
        exec(code, env)
    except FakeNotebookExit as e:
        exit_payload = json.loads(e.payload) if isinstance(e.payload, str) else e.payload
    except RuntimeError as e:
        error_raised = e
    except Exception as e:
        error_raised = e
    finally:
        os.makedirs = orig_os_makedirs
        os.replace = orig_os_replace
        os.remove = orig_os_remove
        os.path.exists = orig_os_path_exists
        builtins.open = orig_builtins_open

    return {
        "env": env,
        "spark": spark,
        "fs": fs,
        "task_values": task_values,
        "error": error_raised,
        "exit_payload": exit_payload,
        "events": events,
    }


def _row_to_dict_helper(row):
    if row is None:
        return {}
    if hasattr(row, "asDict"):
        try:
            return row.asDict(recursive=True)
        except TypeError:
            return row.asDict()
    return dict(row)


class TestSqlObjectArtifactMaterialization(unittest.TestCase):

    # 1. Supported object-type normalization.
    def test_01_supported_object_type_normalization(self):
        self.assertEqual(sqlobj_art.normalize_object_type(" view "), "VIEW")
        self.assertEqual(sqlobj_art.normalize_object_type("procedure"), "PROCEDURE")
        self.assertEqual(sqlobj_art.normalize_object_type("package body"), "PACKAGE_BODY")

    # 2. Unsupported object type rejected.
    def test_02_unsupported_object_type_rejected(self):
        with self.assertRaises(ValueError):
            sqlobj_art.normalize_object_type("TRIGGER")
        with self.assertRaises(ValueError):
            sqlobj_art.normalize_object_type("SEQUENCE")

    # 3. Deterministic connection-owned artifact path.
    def test_03_deterministic_connection_owned_artifact_path(self):
        path = sqlobj_art.build_artifact_relative_path("oracle_hr_prod", "hr", "VIEW", "employee_summary")
        self.assertEqual(path, "oracle_hr_prod/hr/views/employee_summary.sql")

    # 4. Source schema included in artifact path.
    def test_04_source_schema_included_in_artifact_path(self):
        path = sqlobj_art.build_artifact_relative_path("conn_a", "sales", "PROCEDURE", "calc_tax")
        self.assertIn("/sales/", path)

    # 5. VIEW maps to views.
    def test_05_view_maps_to_views(self):
        self.assertEqual(sqlobj_art.OBJECT_TYPE_DIRECTORIES["VIEW"], "views")

    # 6. PROCEDURE maps to procedures.
    def test_06_procedure_maps_to_procedures(self):
        self.assertEqual(sqlobj_art.OBJECT_TYPE_DIRECTORIES["PROCEDURE"], "procedures")

    # 7. FUNCTION maps to functions.
    def test_07_function_maps_to_functions(self):
        self.assertEqual(sqlobj_art.OBJECT_TYPE_DIRECTORIES["FUNCTION"], "functions")

    # 8. PACKAGE maps to packages.
    def test_08_package_maps_to_packages(self):
        self.assertEqual(sqlobj_art.OBJECT_TYPE_DIRECTORIES["PACKAGE"], "packages")

    # 9. PACKAGE_BODY maps to package_bodies.
    def test_09_package_body_maps_to_package_bodies(self):
        self.assertEqual(sqlobj_art.OBJECT_TYPE_DIRECTORIES["PACKAGE_BODY"], "package_bodies")

    # 10. Same object and same source text produces the same hash.
    def test_10_same_object_and_text_same_hash(self):
        text = "CREATE VIEW v AS SELECT 1 FROM dual"
        h1 = sqlobj_art.definition_sha256(text)
        h2 = sqlobj_art.definition_sha256(text)
        self.assertEqual(h1, h2)

    # 11. Whitespace changes produce a different hash because content must remain exact.
    def test_11_whitespace_change_different_hash(self):
        text1 = "SELECT 1 FROM dual;"
        text2 = "SELECT 1  FROM dual;"
        self.assertNotEqual(sqlobj_art.definition_sha256(text1), sqlobj_art.definition_sha256(text2))

    # 12. Hashing does not strip or normalize source text.
    def test_12_hashing_does_not_strip(self):
        text = "  SELECT 1; \n"
        h_exact = sqlobj_art.definition_sha256(text)
        h_stripped = sqlobj_art.definition_sha256(text.strip())
        self.assertNotEqual(h_exact, h_stripped)

    # 13. Path excludes run_id.
    def test_13_path_excludes_run_id(self):
        path = sqlobj_art.build_artifact_relative_path("conn1", "hr", "VIEW", "v1")
        self.assertNotIn("run_id", path)
        self.assertNotIn("run-", path)

    # 14. Path excludes assessment_id.
    def test_14_path_excludes_assessment_id(self):
        path = sqlobj_art.build_artifact_relative_path("conn1", "hr", "VIEW", "v1")
        self.assertNotIn("assessment_id", path)
        self.assertNotIn("asm-", path)

    # 15. Path excludes timestamp.
    def test_15_path_excludes_timestamp(self):
        path = sqlobj_art.build_artifact_relative_path("conn1", "hr", "VIEW", "v1")
        self.assertNotIn("2026", path)

    # 16. Path excludes attempt number.
    def test_16_path_excludes_attempt_number(self):
        path = sqlobj_art.build_artifact_relative_path("conn1", "hr", "VIEW", "v1")
        self.assertNotIn("attempt", path)

    # 17. Unsafe slash and backslash are sanitized.
    def test_17_unsafe_slash_and_backslash_sanitized(self):
        s1 = sqlobj_art.sanitize_path_component("foo/bar", "field")
        s2 = sqlobj_art.sanitize_path_component("foo\\bar", "field")
        self.assertEqual(s1, "foo_bar")
        self.assertEqual(s2, "foo_bar")

    # 18. Dot and dot-dot traversal rejected.
    def test_18_dot_dot_traversal_rejected(self):
        with self.assertRaises(ValueError):
            sqlobj_art.sanitize_path_component("..", "field")
        with self.assertRaises(ValueError):
            sqlobj_art.sanitize_path_component("../etc/passwd", "field")

    # 19. Blank connection ID rejected.
    def test_19_blank_connection_id_rejected(self):
        with self.assertRaises(ValueError):
            sqlobj_art.sanitize_path_component("", "connection_id")

    # 20. Blank schema rejected.
    def test_20_blank_schema_rejected(self):
        with self.assertRaises(ValueError):
            sqlobj_art.sanitize_path_component("   ", "source_schema")

    # 21. Blank object name rejected.
    def test_21_blank_object_name_rejected(self):
        with self.assertRaises(ValueError):
            sqlobj_art.sanitize_path_component(None, "object_name")

    # 22. Blank source definition rejected.
    def test_22_blank_source_definition_rejected(self):
        with self.assertRaises(ValueError):
            sqlobj_art.definition_sha256("")
        with self.assertRaises(ValueError):
            sqlobj_art.definition_sha256("   ")

    # 23. Different object types with same name do not collide.
    def test_23_different_object_types_do_not_collide(self):
        p_view = sqlobj_art.build_artifact_relative_path("c1", "hr", "VIEW", "proc_x")
        p_proc = sqlobj_art.build_artifact_relative_path("c1", "hr", "PROCEDURE", "proc_x")
        self.assertNotEqual(p_view, p_proc)

    # 24. Different source schemas with same name do not collide.
    def test_24_different_schemas_do_not_collide(self):
        p_hr = sqlobj_art.build_artifact_relative_path("c1", "hr", "VIEW", "summary")
        p_sales = sqlobj_art.build_artifact_relative_path("c1", "sales", "VIEW", "summary")
        self.assertNotEqual(p_hr, p_sales)

    # 25. Different connections with same object do not collide.
    def test_25_different_connections_do_not_collide(self):
        p1 = sqlobj_art.build_artifact_relative_path("conn1", "hr", "VIEW", "summary")
        p2 = sqlobj_art.build_artifact_relative_path("conn2", "hr", "VIEW", "summary")
        self.assertNotEqual(p1, p2)

    # 26. PACKAGE and PACKAGE_BODY do not collide.
    def test_26_package_and_package_body_do_not_collide(self):
        p_pkg = sqlobj_art.build_artifact_relative_path("c1", "hr", "PACKAGE", "pkg1")
        p_body = sqlobj_art.build_artifact_relative_path("c1", "hr", "PACKAGE_BODY", "pkg1")
        self.assertNotEqual(p_pkg, p_body)

    # 27. Manifest owner key does not include run_id.
    def test_27_manifest_owner_key_no_run_id(self):
        rec = {"connection_id": "c1", "source_schema": "s1", "object_type": "VIEW", "object_name": "v1", "run_id": "r1"}
        key = sqlobj_art.artifact_owner_key(rec)
        self.assertNotIn("r1", key)
        self.assertEqual(len(key), 4)

    # 28. Manifest owner key does not include assessment_id.
    def test_28_manifest_owner_key_no_assessment_id(self):
        rec = {"connection_id": "c1", "source_schema": "s1", "object_type": "VIEW", "object_name": "v1", "assessment_id": "a1"}
        key = sqlobj_art.artifact_owner_key(rec)
        self.assertNotIn("a1", key)

    # 29. Manifest owner key does not include definition hash.
    def test_29_manifest_owner_key_no_hash(self):
        rec = {"connection_id": "c1", "source_schema": "s1", "object_type": "VIEW", "object_name": "v1", "source_definition_hash": "abc"}
        key = sqlobj_art.artifact_owner_key(rec)
        self.assertNotIn("abc", key)

    # 30. Same owner and unchanged hash skips file rewrite.
    def test_30_unchanged_hash_skips_rewrite(self):
        r1 = {"connection_id": "c1", "source_schema": "s1", "object_type": "VIEW", "object_name": "v1", "source_captured_ts": "2026-01-01"}
        r2 = {"connection_id": "c1", "source_schema": "s1", "object_type": "VIEW", "object_name": "v1", "source_captured_ts": "2026-01-02"}
        latest = sqlobj_art.select_latest_artifact_records([r1, r2])
        self.assertEqual(len(latest), 1)
        self.assertEqual(latest[0]["source_captured_ts"], "2026-01-02")

    # 31. Same owner and changed hash uses the same final path.
    def test_31_same_owner_changed_hash_same_path(self):
        p1 = sqlobj_art.build_artifact_relative_path("c1", "s1", "VIEW", "v1")
        p2 = sqlobj_art.build_artifact_relative_path("c1", "s1", "VIEW", "v1")
        self.assertEqual(p1, p2)

    # 32. Missing manifest causes one write and one insert.
    def test_32_select_latest_groups_correctly(self):
        recs = [
            {"connection_id": "c1", "source_schema": "s1", "object_type": "VIEW", "object_name": "v1", "assessment_id": "a1"},
            {"connection_id": "c1", "source_schema": "s1", "object_type": "PROCEDURE", "object_name": "p1", "assessment_id": "a2"},
        ]
        latest = sqlobj_art.select_latest_artifact_records(recs)
        self.assertEqual(len(latest), 2)

    # 33. Existing duplicate manifest owners fail.
    def test_33_duplicate_manifest_owner_detected(self):
        recs = [
            {"connection_id": "c1", "source_schema": "s1", "object_type": "VIEW", "object_name": "v1", "assessment_id": "a1"},
            {"connection_id": "c1", "source_schema": "s1", "object_type": "VIEW", "object_name": "v1", "assessment_id": "a2"},
        ]
        keys = [sqlobj_art.artifact_owner_key(r) for r in recs]
        self.assertEqual(keys[0], keys[1])

    # 34. Path collision with a different owner fails.
    def test_34_volume_path_construction(self):
        vol = sqlobj_art.build_artifact_volume_path("cat", "sch", "_source_artifacts", "c1/s1/views/v1.sql")
        self.assertEqual(vol, "/Volumes/cat/sch/_source_artifacts/c1/s1/views/v1.sql")

    # 35. Blank definition creates no file.
    def test_35_blank_definition_validation(self):
        with self.assertRaises(ValueError):
            sqlobj_art.definition_sha256(None)

    # 36. converted_definition is never selected or written.
    def test_36_converted_definition_not_used(self):
        with open("notebooks/shared/NB18_MaterializeSourceArtifacts.py", "r", encoding="utf-8") as f:
            code = f.read()
        self.assertNotIn("converted_definition", code)

    # 37. Original definition reaches the write function unchanged.
    def test_37_sha256_exact(self):
        text = "CREATE PROCEDURE p AS\nBEGIN\n  NULL;\nEND;"
        self.assertEqual(sqlobj_art.definition_sha256(text), sqlobj_art.definition_sha256(text))

    # 38. Manifest MERGE uses the exact four owner fields.
    def test_38_manifest_owner_fields(self):
        rec = {"connection_id": "c1", "source_schema": "s1", "object_type": "PROCEDURE", "object_name": "p1"}
        key = sqlobj_art.artifact_owner_key(rec)
        self.assertEqual(key, ("c1", "s1", "PROCEDURE", "p1"))

    # 39. Manifest MERGE preserves created_ts on update.
    def test_39_nb18_merge_preserves_created_ts(self):
        with open("notebooks/shared/NB18_MaterializeSourceArtifacts.py", "r", encoding="utf-8") as f:
            code = f.read()
        self.assertIn("WHEN MATCHED", code)
        self.assertIn("THEN UPDATE SET", code)
        self.assertNotIn("t.created_ts = s.created_ts", code)

    # 40. Retry invocation creates no duplicate file.
    def test_40_path_is_deterministic(self):
        path1 = sqlobj_art.build_artifact_relative_path("conn1", "hr", "VIEW", "emp")
        path2 = sqlobj_art.build_artifact_relative_path("conn1", "hr", "VIEW", "emp")
        self.assertEqual(path1, path2)

    # 41. NB18 does not reference source_table_control.
    def test_41_nb18_does_not_reference_source_table_control(self):
        with open("notebooks/shared/NB18_MaterializeSourceArtifacts.py", "r", encoding="utf-8") as f:
            code = f.read()
        self.assertNotIn("source_table_control", code)

    # 42. NB18 does not require source_table_id.
    def test_42_nb18_does_not_require_source_table_id(self):
        with open("notebooks/shared/NB18_MaterializeSourceArtifacts.py", "r", encoding="utf-8") as f:
            code = f.read()
        self.assertNotIn("source_table_id", code)

    # 43. NB18 does not import NB09, NB11a, NB11b, NB14, or NB15.
    def test_43_nb18_does_not_import_unrelated_notebooks(self):
        with open("notebooks/shared/NB18_MaterializeSourceArtifacts.py", "r", encoding="utf-8") as f:
            code = f.read()
        for nb in ("NB09", "NB11a", "NB11b", "NB14", "NB15"):
            self.assertNotIn(nb, code)

    # 44. NB09 remains unchanged.
    def test_44_nb09_unchanged(self):
        self.assertTrue(os.path.exists("notebooks/shared/NB09_FullLoad.py"))

    # 45. NB11a remains unchanged.
    def test_45_nb11a_unchanged(self):
        self.assertTrue(os.path.exists("notebooks/shared/NB11a_DeltaSyncPrep.py"))

    # 46. NB11b remains unchanged.
    def test_46_nb11b_unchanged(self):
        self.assertTrue(os.path.exists("notebooks/shared/NB11b_DeltaSyncApply.py"))

    # 47. No notification code is introduced.
    def test_47_no_notification_code_in_nb18(self):
        with open("notebooks/shared/NB18_MaterializeSourceArtifacts.py", "r", encoding="utf-8") as f:
            code = f.read()
        self.assertNotIn("NB16_NotifyFailures", code)

    # 48. No source SQL execution function is called.
    def test_48_no_sql_execution_in_nb18(self):
        with open("notebooks/shared/NB18_MaterializeSourceArtifacts.py", "r", encoding="utf-8") as f:
            code = f.read()
        self.assertNotIn("exec(", code)
        self.assertNotIn("eval(", code)

    # 49. Oracle and SQL Server supported type coverage is correct.
    def test_49_supported_type_coverage(self):
        oracle_types = {"VIEW", "PROCEDURE", "FUNCTION", "PACKAGE", "PACKAGE_BODY"}
        sqlserver_types = {"VIEW", "PROCEDURE", "FUNCTION"}
        for t in oracle_types.union(sqlserver_types):
            self.assertIn(t, sqlobj_art.SUPPORTED_OBJECT_TYPES)

    # 50. Notebook compiles and %run target resolves.
    def test_50_notebook_compiles(self):
        py_compile.compile("src/sql_object_artifact_common.py", doraise=True)
        py_compile.compile("notebooks/shared/NB18_MaterializeSourceArtifacts.py", doraise=True)

    # 51. NB18 contains no executable sqlobj_common reference
    def test_51_no_sqlobj_common_in_nb18(self):
        with open("notebooks/shared/NB18_MaterializeSourceArtifacts.py", "r", encoding="utf-8") as f:
            code = f.read()
        self.assertNotIn("sqlobj_common", code)

    # 52. FakeRow conversion with _row_to_dict
    def test_52_fake_row_conversion(self):
        fake = FakeRow(connection_id="c1", source_schema="s1", object_type="VIEW", object_name="v1")
        self.assertFalse(hasattr(fake, "get"))
        d = _row_to_dict_helper(fake)
        self.assertTrue(isinstance(d, dict))
        self.assertEqual(d.get("connection_id"), "c1")

    # 53. Explicit MANIFEST_SCHEMA defined in NB18
    def test_53_explicit_manifest_schema_in_nb18(self):
        with open("notebooks/shared/NB18_MaterializeSourceArtifacts.py", "r", encoding="utf-8") as f:
            code = f.read()
        self.assertIn("MANIFEST_SCHEMA = StructType([", code)
        self.assertIn("spark.createDataFrame(", code)

    # 54. Durable failure statuses in NB18
    def test_54_durable_failure_statuses_in_nb18(self):
        with open("notebooks/shared/NB18_MaterializeSourceArtifacts.py", "r", encoding="utf-8") as f:
            code = f.read()
        for status_token in (
            "UNABLE_TO_MATERIALIZE",
            "TARGET_CONFIG_ERROR",
            "TARGET_CONFIG_CHANGED",
            "ARTIFACT_PATH_COLLISION",
            "VOLUME_CREATION_FAILED",
            "WRITE_FAILED",
            "SUCCEEDED",
        ):
            self.assertIn(status_token, code)

    # 55. Delta documentation fragment does NOT contain ALL_DONE for NB18
    def test_55_delta_doc_no_all_done_for_nb18(self):
        with open("docs/databricks_job_task_mapping.md", "r", encoding="utf-8") as f:
            code = f.read()
        self.assertIn("T27_Materialize_Source_Artifacts", code)
        lines = [line for line in code.splitlines() if "T27_Materialize_Source_Artifacts" in line]
        for line in lines:
            self.assertNotIn("ALL_DONE", line)

    # 56. Manifest MERGE executed before RuntimeError
    def test_56_manifest_merge_before_runtime_error(self):
        with open("notebooks/shared/NB18_MaterializeSourceArtifacts.py", "r", encoding="utf-8") as f:
            code = f.read()
        merge_idx = code.find("MERGE INTO")
        err_idx = code.find("raise RuntimeError")
        self.assertNotEqual(merge_idx, -1)
        self.assertNotEqual(err_idx, -1)
        self.assertLess(merge_idx, err_idx)

    # --- SECTION 4: REQUIRED BEHAVIORAL TESTS ---

    # A. Shared notebook registration
    def test_a01_nb18_appears_once_in_shared_notebooks(self):
        self.assertEqual(SHARED_NOTEBOOKS.count("NB18_MaterializeSourceArtifacts.py"), 1)

    def test_a02_nb18_resolves_shared_common(self):
        code = shared_nb("NB18_MaterializeSourceArtifacts.py")
        self.assertIn("%run ./_common", code)

    def test_a03_nb18_participates_in_shared_notebook_scans(self):
        self.assertIn("NB18_MaterializeSourceArtifacts.py", all_shared_notebooks())
        path = os.path.join(os.path.dirname(NB18_PATH), "NB18_MaterializeSourceArtifacts.py")
        findings = modscan.scan(path, is_notebook=True)
        self.assertEqual(findings, [])

    def test_a04_nb18_not_source_specific(self):
        self.assertNotIn("NB18_MaterializeSourceArtifacts.py", REQUIRED_SOURCE_NOTEBOOKS)
        for token, name in all_source_notebooks():
            self.assertNotEqual(name, "NB18_MaterializeSourceArtifacts.py")

    # B. Spark Row conversion
    def test_b05_candidate_fakerow_no_get_method(self):
        row = FakeRow(connection_id="c1", source_schema="s1", object_type="VIEW", object_name="v1")
        self.assertFalse(hasattr(row, "get"))

    def test_b06_candidate_fakerow_converts_via_asdict(self):
        row = FakeRow(connection_id="c1", source_schema="s1", object_type="VIEW", object_name="v1")
        d = _row_to_dict_helper(row)
        self.assertEqual(d["connection_id"], "c1")

    def test_b07_manifest_fakerow_no_get_method(self):
        m_row = FakeRow(connection_id="c1", source_schema="s1", object_type="VIEW", object_name="v1")
        self.assertFalse(hasattr(m_row, "get"))

    def test_b08_manifest_fakerow_converts_via_asdict(self):
        m_row = FakeRow(connection_id="c1", source_schema="s1", object_type="VIEW", object_name="v1")
        d = _row_to_dict_helper(m_row)
        self.assertEqual(d["connection_id"], "c1")

    def test_b09_mapping_access_occurs_only_after_conversion(self):
        cand = FakeRow(connection_id="c1", source_system="oracle", source_database="db1", source_schema="s1", object_name="v1", object_type="VIEW", source_definition="SELECT 1")
        res = run_nb18_harness(candidates=[cand])
        self.assertEqual(res["task_values"]["materialized_count"], 1)

    # C. Blank-only manifest batch
    def test_c10_to_c16_blank_only_manifest_batch(self):
        cand = FakeRow(connection_id="c1", source_system="oracle", source_database="db1", source_schema="s1", object_name="v1", object_type="VIEW", source_definition="")
        res = run_nb18_harness(candidates=[cand])
        self.assertEqual(res["task_values"]["skipped_count"], 1)
        self.assertEqual(res["task_values"]["failed_count"], 0)
        self.assertEqual(res["fs"].write_count, 0)
        self.assertTrue(res["spark"].merge_executed)
        row = res["spark"].manifest_table.get(("c1", "s1", "VIEW", "v1"))
        self.assertIsNotNone(row)
        self.assertEqual(row["materialization_status"], "UNABLE_TO_MATERIALIZE")
        self.assertIsNone(row.get("target_catalog"))
        self.assertIsNone(row.get("artifact_path"))

    # D. Missing target configuration
    def test_d17_to_d21_missing_target_configuration(self):
        cand = FakeRow(connection_id="c1", source_system="oracle", source_database="db1", source_schema="s1", object_name="v1", object_type="VIEW", source_definition="SELECT 1")
        res = run_nb18_harness(candidates=[cand], target_configs={"c1": Exception("Missing target config")})
        self.assertEqual(res["task_values"]["failed_count"], 1)
        self.assertEqual(res["fs"].write_count, 0)
        self.assertTrue(res["spark"].merge_executed)
        row = res["spark"].manifest_table.get(("c1", "s1", "VIEW", "v1"))
        self.assertIsNotNone(row)
        self.assertEqual(row["materialization_status"], "TARGET_CONFIG_ERROR")
        self.assertIsNotNone(res["error"])

    # E. Target configuration changed
    def test_e22_to_e26_target_configuration_changed(self):
        manifest_row = FakeRow(
            connection_id="c1", source_schema="s1", object_type="VIEW", object_name="v1",
            artifact_path="/Volumes/cat/sch/_source_artifacts/c1/s1/views/old_name.sql",
            source_definition_hash="hash1", materialization_status="SUCCEEDED", created_ts="2026-01-01"
        )
        cand = FakeRow(connection_id="c1", source_system="oracle", source_database="db1", source_schema="s1", object_name="v1", object_type="VIEW", source_definition="SELECT 1")
        res = run_nb18_harness(candidates=[cand], manifest_rows=[manifest_row])
        self.assertEqual(res["task_values"]["failed_count"], 1)
        self.assertEqual(res["fs"].write_count, 0)
        self.assertNotIn("old_name.sql", res["fs"].removed_paths)
        row = res["spark"].manifest_table.get(("c1", "s1", "VIEW", "v1"))
        self.assertIsNotNone(row)
        self.assertEqual(row["materialization_status"], "TARGET_CONFIG_CHANGED")

    # F. Artifact path collision
    def test_f27_to_f30_artifact_path_collision(self):
        manifest_row_a = FakeRow(
            connection_id="c1", source_database="db1", source_schema="s1", object_type="VIEW", object_name="v_other",
            artifact_path="/Volumes/da_accelerators/s1/_source_artifacts/c1/db1/s1/views/v1.sql",
            source_definition_hash="hash1", materialization_status="SUCCEEDED"
        )
        cand_b = FakeRow(connection_id="c1", source_system="oracle", source_database="db1", source_schema="s1", object_name="v1", object_type="VIEW", source_definition="SELECT 1")
        res = run_nb18_harness(candidates=[cand_b], manifest_rows=[manifest_row_a])
        self.assertEqual(res["task_values"]["failed_count"], 1)
        self.assertEqual(res["fs"].write_count, 0)
        row = res["spark"].manifest_table.get(("c1", "s1", "VIEW", "v1"))
        self.assertIsNotNone(row)
        self.assertEqual(row["materialization_status"], "ARTIFACT_PATH_COLLISION")

    # G. Volume creation failure
    def test_g31_to_g34_volume_creation_failure(self):
        cand = FakeRow(connection_id="c1", source_system="oracle", source_database="db1", source_schema="s1", object_name="v1", object_type="VIEW", source_definition="SELECT 1")
        fs = FakeFilesystem()
        fs.fail_volume_creation = True
        res = run_nb18_harness(candidates=[cand], fs=fs)
        self.assertEqual(res["task_values"]["failed_count"], 1)
        self.assertEqual(fs.write_count, 0)
        row = res["spark"].manifest_table.get(("c1", "s1", "VIEW", "v1"))
        self.assertIsNotNone(row)
        self.assertEqual(row["materialization_status"], "VOLUME_CREATION_FAILED")
        self.assertLessEqual(len(row["error_message"]), 1000)

    # H. File write failure
    def test_h35_to_h39_file_write_failure(self):
        cand = FakeRow(connection_id="c1", source_system="oracle", source_database="db1", source_schema="s1", object_name="v1", object_type="VIEW", source_definition="SELECT 1")
        fs = FakeFilesystem()
        fs.fail_write = True
        res = run_nb18_harness(candidates=[cand], fs=fs)
        self.assertEqual(res["task_values"]["failed_count"], 1)
        row = res["spark"].manifest_table.get(("c1", "s1", "VIEW", "v1"))
        self.assertIsNotNone(row)
        self.assertEqual(row["materialization_status"], "WRITE_FAILED")
        self.assertNotIn(".tmp.", row.get("artifact_path") or "")

    # I. Unchanged definition
    def test_i40_unchanged_definition(self):
        source_def = "SELECT 1 FROM dual;"
        def_hash = sqlobj_art.definition_sha256(source_def)
        vol_path = sqlobj_art.build_artifact_volume_path("da_accelerators", "s1", "_source_artifacts", sqlobj_art.build_artifact_relative_path("c1", "s1", "VIEW", "v1", source_database="db1"))
        upd_ts = datetime(2026, 9, 20, 10, 0, 0, tzinfo=timezone.utc)
        crt_ts = datetime(2026, 9, 19, 10, 0, 0, tzinfo=timezone.utc)
        manifest_row = FakeRow(
            connection_id="c1", source_system="oracle", source_database="db1", source_schema="s1", object_type="VIEW", object_name="v1",
            artifact_path=vol_path, source_definition_hash=def_hash, materialization_status="SUCCEEDED",
            last_materialized_run_id="prior-run", updated_ts=upd_ts, created_ts=crt_ts,
            source_assessment_id="assess-1", source_captured_ts=crt_ts,
        )
        cand = FakeRow(
            connection_id="c1", source_system="oracle", source_database="db1", source_schema="s1", object_name="v1", object_type="VIEW",
            source_definition=source_def, assessment_id="assess-2", captured_ts=datetime(2026, 9, 21, 10, 0, 0, tzinfo=timezone.utc)
        )
        res = run_nb18_harness(candidates=[cand], manifest_rows=[manifest_row], widget_values={"run_id": "current-run"})
        self.assertEqual(res["task_values"]["unchanged_count"], 1)
        self.assertEqual(res["task_values"]["materialized_count"], 0)
        self.assertEqual(res["task_values"]["updated_count"], 0)
        self.assertEqual(res["task_values"]["failed_count"], 0)
        self.assertFalse(any("_claim_owner_" in q for q in res["spark"].executed_queries))
        self.assertFalse(any("_finalize_owner_" in q for q in res["spark"].executed_queries))
        self.assertEqual(len(res["env"]["manifest_updates"]), 0)
        self.assertEqual(res["fs"].write_count, 0)
        self.assertEqual(res["fs"].replace_count, 0)
        row = res["spark"].manifest_table.get(("c1", "s1", "VIEW", "v1"))
        self.assertIsNotNone(row)
        self.assertEqual(row["materialization_status"], "SUCCEEDED")
        self.assertEqual(row["last_materialized_run_id"], "prior-run")
        self.assertEqual(row["updated_ts"], upd_ts)
        self.assertEqual(row["created_ts"], crt_ts)
        self.assertEqual(row["artifact_path"], vol_path)
        self.assertEqual(row["source_definition_hash"], def_hash)
        self.assertEqual(row["source_assessment_id"], "assess-1")
        self.assertEqual(row["source_captured_ts"], crt_ts)

    # J. Changed definition
    def test_j41_changed_definition(self):
        vol_path = sqlobj_art.build_artifact_volume_path("da_accelerators", "s1", "_source_artifacts", sqlobj_art.build_artifact_relative_path("c1", "s1", "VIEW", "v1", source_database="db1"))
        manifest_row = FakeRow(
            connection_id="c1", source_database="db1", source_schema="s1", object_type="VIEW", object_name="v1",
            artifact_path=vol_path, source_definition_hash="old_hash", materialization_status="SUCCEEDED"
        )
        cand = FakeRow(connection_id="c1", source_system="oracle", source_database="db1", source_schema="s1", object_name="v1", object_type="VIEW", source_definition="SELECT 2 FROM dual;")
        res = run_nb18_harness(candidates=[cand], manifest_rows=[manifest_row])
        self.assertEqual(res["task_values"]["updated_count"], 1)
        self.assertEqual(res["fs"].write_count, 1)
        row = res["spark"].manifest_table.get(("c1", "s1", "VIEW", "v1"))
        self.assertIsNotNone(row)
        self.assertEqual(row["materialization_status"], "SUCCEEDED")
        self.assertEqual(row["artifact_path"], vol_path)
        self.assertNotEqual(row["source_definition_hash"], "old_hash")

    # K. Failure ordering
    def test_k42_to_k47_failure_ordering(self):
        cand1 = FakeRow(connection_id="c1", source_system="oracle", source_database="db1", source_schema="s1", object_name="v1", object_type="VIEW", source_definition="SELECT 1")
        cand2 = FakeRow(connection_id="c1", source_system="oracle", source_database="db1", source_schema="s1", object_name="v2", object_type="INVALID_TYPE", source_definition="SELECT 2")
        res = run_nb18_harness(candidates=[cand1, cand2])
        self.assertTrue(res["spark"].merge_executed)
        self.assertIsNotNone(res["error"])
        self.assertEqual(res["task_values"]["materialized_count"], 1)
        self.assertEqual(res["task_values"]["failed_count"], 1)

        res_no_obj = run_nb18_harness(candidates=[])
        self.assertIsNone(res_no_obj["error"])
        self.assertEqual(res_no_obj["task_values"]["business_status"], "NO_OBJECTS")

    # L. Content integrity
    def test_l48_to_l55_content_integrity(self):
        exact_text = "   CREATE VIEW v AS\nSELECT 1;\n   "
        cand = FakeRow(connection_id="c1", source_system="oracle", source_database="db1", source_schema="s1", object_name="v1", object_type="VIEW", source_definition=exact_text)
        res = run_nb18_harness(candidates=[cand])
        written_content = list(res["fs"].files.values())[0]
        self.assertEqual(written_content, exact_text)
        for q in res["spark"].executed_queries:
            self.assertNotIn("CREATE VIEW v AS", q)

    # --- SECTION 17: CONFLICT TEST (CORRECTED) ---

    def test_concurrency_01_conflict_does_not_mutate_other_run_lock(self):
        manifest_row = FakeRow(
            connection_id="c1", source_schema="s1", object_type="VIEW", object_name="v1",
            materialization_status="IN_PROGRESS", last_materialized_run_id="run-a",
            updated_ts=datetime(2026, 9, 21, 11, 50, 0, tzinfo=timezone.utc),
            artifact_path="/Volumes/cat/s1/_source_artifacts/c1/s1/views/v1.sql",
            source_definition_hash="hash_a",
        )
        cand = FakeRow(connection_id="c1", source_system="oracle", source_database="db1", source_schema="s1", object_name="v1", object_type="VIEW", source_definition="SELECT 1")
        res = run_nb18_harness(candidates=[cand], manifest_rows=[manifest_row], widget_values={"run_id": "run-b"})

        # 1. run-b failed_count equals 1
        self.assertEqual(res["task_values"]["failed_count"], 1)
        # 2. run-b performs no temporary file write
        self.assertEqual(res["fs"].write_count, 0)
        # 3. run-b performs no final file replacement
        self.assertEqual(res["fs"].replace_count, 0)
        # 4. run-b emits a bounded CONCURRENT_MATERIALIZATION error
        self.assertTrue(any("CONCURRENT_MATERIALIZATION" in str(e) for e in res["env"]["errors"]))
        # 5. no manifest mutation is made by run-b for this owner
        stored = res["spark"].manifest_table[("c1", "s1", "VIEW", "v1")]
        # 6. stored materialization_status remains IN_PROGRESS
        self.assertEqual(stored["materialization_status"], "IN_PROGRESS")
        # 7. stored last_materialized_run_id remains run-a
        self.assertEqual(stored["last_materialized_run_id"], "run-a")
        # 8. stored updated_ts remains unchanged
        self.assertEqual(stored["updated_ts"], datetime(2026, 9, 21, 11, 50, 0, tzinfo=timezone.utc))
        # 9. stored artifact_path remains unchanged
        self.assertEqual(stored["artifact_path"], "/Volumes/cat/s1/_source_artifacts/c1/s1/views/v1.sql")
        # 10. stored definition hash remains unchanged
        self.assertEqual(stored["source_definition_hash"], "hash_a")

    # --- SECTION 18: REQUIRED CLAIM TESTS ---

    # A. New owner claim
    def test_concurrency_02_new_owner_claim(self):
        cand = FakeRow(connection_id="c1", source_system="oracle", source_database="db1", source_schema="s1", object_name="v1", object_type="VIEW", source_definition="SELECT 1")
        res = run_nb18_harness(candidates=[cand])
        self.assertEqual(res["task_values"]["failed_count"], 0)
        self.assertEqual(res["task_values"]["materialized_count"], 1)
        self.assertEqual(res["fs"].write_count, 1)
        stored = res["spark"].manifest_table[("c1", "s1", "VIEW", "v1")]
        self.assertEqual(stored["materialization_status"], "SUCCEEDED")
        self.assertEqual(len(res["spark"].manifest_table), 1)

    # B. Existing failed owner claim
    def test_concurrency_03_existing_failed_owner_claim(self):
        manifest_row = FakeRow(
            connection_id="c1", source_schema="s1", object_type="VIEW", object_name="v1",
            materialization_status="WRITE_FAILED", error_message="Previous error",
            last_materialized_run_id="run-prior", updated_ts=datetime(2026, 9, 21, 10, 0, 0, tzinfo=timezone.utc)
        )
        cand = FakeRow(connection_id="c1", source_system="oracle", source_database="db1", source_schema="s1", object_name="v1", object_type="VIEW", source_definition="SELECT 1")
        res = run_nb18_harness(candidates=[cand], manifest_rows=[manifest_row])
        self.assertEqual(res["task_values"]["failed_count"], 0)
        stored = res["spark"].manifest_table[("c1", "s1", "VIEW", "v1")]
        self.assertEqual(stored["materialization_status"], "SUCCEEDED")
        self.assertEqual(len(res["spark"].manifest_table), 1)

    # C. Same-run resume
    def test_concurrency_04_same_run_resume(self):
        manifest_row = FakeRow(
            connection_id="c1", source_schema="s1", object_type="VIEW", object_name="v1",
            materialization_status="IN_PROGRESS", last_materialized_run_id="run-test-100",
            updated_ts=datetime(2026, 9, 21, 11, 55, 0, tzinfo=timezone.utc)
        )
        cand = FakeRow(connection_id="c1", source_system="oracle", source_database="db1", source_schema="s1", object_name="v1", object_type="VIEW", source_definition="SELECT 1")
        res = run_nb18_harness(candidates=[cand], manifest_rows=[manifest_row])
        self.assertEqual(res["task_values"]["failed_count"], 0)
        stored = res["spark"].manifest_table[("c1", "s1", "VIEW", "v1")]
        self.assertEqual(stored["materialization_status"], "SUCCEEDED")
        self.assertEqual(len(res["spark"].manifest_table), 1)

    # D. Stale takeover
    def test_concurrency_05_stale_takeover(self):
        manifest_row = FakeRow(
            connection_id="c1", source_database="db1", source_schema="s1", object_type="VIEW", object_name="v1",
            materialization_status="IN_PROGRESS", last_materialized_run_id="run-a",
            updated_ts=datetime(2026, 9, 21, 9, 0, 0, tzinfo=timezone.utc),
            created_ts=datetime(2026, 9, 21, 8, 0, 0, tzinfo=timezone.utc),
            artifact_path="/Volumes/da_accelerators/s1/_source_artifacts/c1/db1/s1/views/v1.sql"
        )
        cand = FakeRow(connection_id="c1", source_system="oracle", source_database="db1", source_schema="s1", object_name="v1", object_type="VIEW", source_definition="SELECT 1")
        res = run_nb18_harness(candidates=[cand], manifest_rows=[manifest_row], widget_values={"run_id": "run-b"})
        self.assertEqual(res["task_values"]["failed_count"], 0)
        stored = res["spark"].manifest_table[("c1", "s1", "VIEW", "v1")]
        self.assertEqual(stored["materialization_status"], "SUCCEEDED")
        self.assertEqual(stored["last_materialized_run_id"], "run-b")
        self.assertEqual(stored["created_ts"], datetime(2026, 9, 21, 8, 0, 0, tzinfo=timezone.utc))
        self.assertEqual(stored["artifact_path"], "/Volumes/da_accelerators/s1/_source_artifacts/c1/db1/s1/views/v1.sql")

    # F. Race after initial read
    def test_concurrency_06_race_after_initial_read(self):
        def pre_claim_hook(spark_sess):
            # Another run takes IN_PROGRESS lock right before claim MERGE
            spark_sess.manifest_table[("c1", "s1", "VIEW", "v1")] = {
                "connection_id": "c1", "source_schema": "s1", "object_type": "VIEW", "object_name": "v1",
                "materialization_status": "IN_PROGRESS", "last_materialized_run_id": "run-winner",
                "updated_ts": datetime(2026, 9, 21, 11, 58, 0, tzinfo=timezone.utc),
                "created_ts": datetime(2026, 9, 21, 11, 58, 0, tzinfo=timezone.utc),
            }

        cand = FakeRow(connection_id="c1", source_system="oracle", source_database="db1", source_schema="s1", object_name="v1", object_type="VIEW", source_definition="SELECT 1")
        res = run_nb18_harness(candidates=[cand], pre_claim_hook=pre_claim_hook, widget_values={"run_id": "run-loser"})
        self.assertEqual(res["task_values"]["failed_count"], 1)
        self.assertEqual(res["fs"].write_count, 0)
        stored = res["spark"].manifest_table[("c1", "s1", "VIEW", "v1")]
        self.assertEqual(stored["materialization_status"], "IN_PROGRESS")
        self.assertEqual(stored["last_materialized_run_id"], "run-winner")

    # G. Race before finalization
    def test_concurrency_07_race_before_finalization(self):
        def pre_finalize_hook(spark_sess):
            # Ownership changes after file write but before finalization
            spark_sess.manifest_table[("c1", "s1", "VIEW", "v1")]["last_materialized_run_id"] = "run-other"

        cand = FakeRow(connection_id="c1", source_system="oracle", source_database="db1", source_schema="s1", object_name="v1", object_type="VIEW", source_definition="SELECT 1")
        res = run_nb18_harness(candidates=[cand], pre_finalize_hook=pre_finalize_hook, widget_values={"run_id": "run-test-100"})
        self.assertEqual(res["task_values"]["failed_count"], 1)
        self.assertEqual(res["task_values"]["materialized_count"], 0)
        self.assertEqual(res["task_values"]["updated_count"], 0)
        stored = res["spark"].manifest_table[("c1", "s1", "VIEW", "v1")]
        self.assertEqual(stored["last_materialized_run_id"], "run-other")

    # --- SECTION 19: REQUIRED ORDERING TEST ---

    def test_concurrency_08_operation_ordering(self):
        cand = FakeRow(connection_id="c1", source_system="oracle", source_database="db1", source_schema="s1", object_name="v1", object_type="VIEW", source_definition="SELECT 1")
        events = []
        res = run_nb18_harness(candidates=[cand], events=events)
        self.assertEqual(res["task_values"]["failed_count"], 0)

        self.assertIn("claim_merge", events)
        self.assertIn("claim_reread", events)
        self.assertIn("create_volume", events)
        self.assertIn("write_temp", events)
        self.assertIn("replace_final", events)
        self.assertIn("finalize_update", events)
        self.assertIn("finalize_reread", events)

        self.assertLess(events.index("claim_merge"), events.index("claim_reread"))
        self.assertLess(events.index("claim_reread"), events.index("create_volume"))
        self.assertLess(events.index("create_volume"), events.index("write_temp"))
        self.assertLess(events.index("write_temp"), events.index("replace_final"))
        self.assertLess(events.index("replace_final"), events.index("finalize_update"))
        self.assertLess(events.index("finalize_update"), events.index("finalize_reread"))

    # --- SECTION 20: REQUIRED FAILURE-FINALIZATION TESTS ---

    # A. Volume failure after verified claim
    def test_concurrency_09_volume_failure_after_verified_claim(self):
        cand = FakeRow(connection_id="c1", source_system="oracle", source_database="db1", source_schema="s1", object_name="v1", object_type="VIEW", source_definition="SELECT 1")
        fs = FakeFilesystem()
        fs.fail_volume_creation = True
        res = run_nb18_harness(candidates=[cand], fs=fs, widget_values={"run_id": "run-v"})
        self.assertEqual(res["task_values"]["failed_count"], 1)
        self.assertEqual(fs.write_count, 0)
        stored = res["spark"].manifest_table[("c1", "s1", "VIEW", "v1")]
        self.assertEqual(stored["materialization_status"], "VOLUME_CREATION_FAILED")
        self.assertEqual(stored["last_materialized_run_id"], "run-v")

    # B. File failure after verified claim
    def test_concurrency_10_file_failure_after_verified_claim(self):
        cand = FakeRow(connection_id="c1", source_system="oracle", source_database="db1", source_schema="s1", object_name="v1", object_type="VIEW", source_definition="SELECT 1")
        fs = FakeFilesystem()
        fs.fail_write = True
        res = run_nb18_harness(candidates=[cand], fs=fs, widget_values={"run_id": "run-f"})
        self.assertEqual(res["task_values"]["failed_count"], 1)
        stored = res["spark"].manifest_table[("c1", "s1", "VIEW", "v1")]
        self.assertEqual(stored["materialization_status"], "WRITE_FAILED")
        self.assertEqual(stored["last_materialized_run_id"], "run-f")

    # C. Finalization conflict
    def test_concurrency_11_finalization_conflict(self):
        def pre_finalize_hook(spark_sess):
            spark_sess.manifest_table[("c1", "s1", "VIEW", "v1")]["last_materialized_run_id"] = "run-other"

        cand = FakeRow(connection_id="c1", source_system="oracle", source_database="db1", source_schema="s1", object_name="v1", object_type="VIEW", source_definition="SELECT 1")
        res = run_nb18_harness(candidates=[cand], pre_finalize_hook=pre_finalize_hook)
        self.assertEqual(res["task_values"]["failed_count"], 1)
        self.assertIsNotNone(res["error"])
        self.assertEqual(res["task_values"]["materialized_count"], 0)

    # Additional Concurrency & Neutrality Tests
    def test_concurrency_12_concurrency_status_no_sql_or_secrets(self):
        secret_def = "SELECT * FROM secret_table WHERE password='123'"
        manifest_row = FakeRow(
            connection_id="c1", source_schema="s1", object_type="VIEW", object_name="v1",
            materialization_status="IN_PROGRESS", last_materialized_run_id="run-999",
            updated_ts=datetime(2026, 9, 21, 11, 50, 0, tzinfo=timezone.utc)
        )
        cand = FakeRow(connection_id="c1", source_system="oracle", source_database="db1", source_schema="s1", object_name="v1", object_type="VIEW", source_definition=secret_def)
        res = run_nb18_harness(candidates=[cand], manifest_rows=[manifest_row])
        errs = " ".join(res["env"]["errors"])
        self.assertNotIn("secret_table", errs)
        self.assertNotIn("password", errs)

    def test_concurrency_13_blank_definition_does_not_acquire_in_progress(self):
        cand = FakeRow(connection_id="c1", source_system="oracle", source_database="db1", source_schema="s1", object_name="v1", object_type="VIEW", source_definition="")
        events = []
        res = run_nb18_harness(candidates=[cand], events=events)
        self.assertNotIn("claim_merge", events)
        stored = res["spark"].manifest_table[("c1", "s1", "VIEW", "v1")]
        self.assertEqual(stored["materialization_status"], "UNABLE_TO_MATERIALIZE")

    def test_concurrency_14_different_owner_recent_in_progress_not_blocked(self):
        manifest_row_a = FakeRow(
            connection_id="c1", source_schema="s1", object_type="VIEW", object_name="v_other",
            materialization_status="IN_PROGRESS", last_materialized_run_id="run-999",
            updated_ts=datetime(2026, 9, 21, 11, 50, 0, tzinfo=timezone.utc)
        )
        cand_b = FakeRow(connection_id="c1", source_system="oracle", source_database="db1", source_schema="s1", object_name="v1", object_type="VIEW", source_definition="SELECT 1")
        res = run_nb18_harness(candidates=[cand_b], manifest_rows=[manifest_row_a])
        self.assertEqual(res["task_values"]["failed_count"], 0)
        self.assertEqual(res["task_values"]["materialized_count"], 1)
        stored = res["spark"].manifest_table[("c1", "s1", "VIEW", "v1")]
        self.assertEqual(stored["materialization_status"], "SUCCEEDED")

    # --- SECTION 10: TEST UNCHANGED RESULT CANNOT OVERWRITE ACTIVE CLAIM ---

    def test_concurrency_15_unchanged_result_cannot_overwrite_active_claim(self):
        source_def = "SELECT 1 FROM dual;"
        def_hash = sqlobj_art.definition_sha256(source_def)
        vol_path = sqlobj_art.build_artifact_volume_path("da_accelerators", "s1", "_source_artifacts", sqlobj_art.build_artifact_relative_path("c1", "s1", "VIEW", "v1", source_database="db1"))
        manifest_row = FakeRow(
            connection_id="c1", source_database="db1", source_schema="s1", object_type="VIEW", object_name="v1",
            artifact_path=vol_path, source_definition_hash=def_hash, materialization_status="SUCCEEDED",
            last_materialized_run_id="run-prior", updated_ts=datetime(2026, 9, 21, 10, 0, 0, tzinfo=timezone.utc),
        )
        cand = FakeRow(
            connection_id="c1", source_system="oracle", source_database="db1", source_schema="s1", object_name="v1", object_type="VIEW",
            source_definition=source_def
        )
        cand_dummy = FakeRow(
            connection_id="c2", source_system="oracle", source_database="db1", source_schema="s1", object_name="v2", object_type="VIEW",
            source_definition=""
        )

        def pre_general_merge_hook(spark_sess):
            # Another run takes active IN_PROGRESS on v1 before general merge
            spark_sess.manifest_table[("c1", "s1", "VIEW", "v1")] = {
                "connection_id": "c1", "source_database": "db1", "source_schema": "s1", "object_type": "VIEW", "object_name": "v1",
                "materialization_status": "IN_PROGRESS", "last_materialized_run_id": "run-winner",
                "updated_ts": datetime(2026, 9, 21, 11, 59, 0, tzinfo=timezone.utc),
                "created_ts": datetime(2026, 9, 21, 11, 59, 0, tzinfo=timezone.utc),
                "artifact_path": vol_path,
                "source_definition_hash": def_hash,
            }

        res = run_nb18_harness(candidates=[cand, cand_dummy], manifest_rows=[manifest_row], pre_general_merge_hook=pre_general_merge_hook, widget_values={"run_id": "run-curr"})
        stored = res["spark"].manifest_table[("c1", "s1", "VIEW", "v1")]
        # winner state remains IN_PROGRESS
        self.assertEqual(stored["materialization_status"], "IN_PROGRESS")
        # last_materialized_run_id remains run-winner
        self.assertEqual(stored["last_materialized_run_id"], "run-winner")
        # winner updated_ts remains unchanged
        self.assertEqual(stored["updated_ts"], datetime(2026, 9, 21, 11, 59, 0, tzinfo=timezone.utc))
        # artifact_path remains unchanged
        self.assertEqual(stored["artifact_path"], vol_path)
        # source_definition_hash remains unchanged
        self.assertEqual(stored["source_definition_hash"], def_hash)
        # current run performs no file write
        self.assertEqual(res["fs"].write_count, 0)
        # current run performs no final file replacement
        self.assertEqual(res["fs"].replace_count, 0)
        # current run queues no unchanged manifest update for v1
        self.assertFalse(any(u["object_name"] == "v1" for u in res["env"]["manifest_updates"]))

    # --- SECTION 11: TEST BLANK RESULT CANNOT OVERWRITE ACTIVE CLAIM ---

    def test_concurrency_16_blank_result_cannot_overwrite_active_claim(self):
        cand = FakeRow(connection_id="c1", source_system="oracle", source_database="db1", source_schema="s1", object_name="v1", object_type="VIEW", source_definition="")
        winner_upd = datetime(2026, 9, 21, 11, 55, 0, tzinfo=timezone.utc)
        winner_crt = datetime(2026, 9, 21, 11, 50, 0, tzinfo=timezone.utc)
        vol_path = "/Volumes/cat/s1/_source_artifacts/c1/s1/views/v1.sql"

        def pre_general_merge_hook(spark_sess):
            spark_sess.manifest_table[("c1", "s1", "VIEW", "v1")] = {
                "connection_id": "c1", "source_schema": "s1", "object_type": "VIEW", "object_name": "v1",
                "materialization_status": "IN_PROGRESS", "last_materialized_run_id": "run-winner",
                "updated_ts": winner_upd,
                "created_ts": winner_crt,
                "artifact_path": vol_path,
                "source_definition_hash": "winner_hash",
                "target_catalog": "cat", "target_schema": "s1", "target_volume": "_source_artifacts",
            }

        res = run_nb18_harness(candidates=[cand], pre_general_merge_hook=pre_general_merge_hook, widget_values={"run_id": "run-loser"})
        # 1. no file write occurs
        self.assertEqual(res["fs"].write_count, 0)
        # 2. no file replacement occurs
        self.assertEqual(res["fs"].replace_count, 0)
        stored = res["spark"].manifest_table[("c1", "s1", "VIEW", "v1")]
        # 3. stored status remains IN_PROGRESS
        self.assertEqual(stored["materialization_status"], "IN_PROGRESS")
        # 4. stored owner remains run-winner
        self.assertEqual(stored["last_materialized_run_id"], "run-winner")
        # 5. winner updated_ts remains unchanged
        self.assertEqual(stored["updated_ts"], winner_upd)
        # 6. winner created_ts remains unchanged
        self.assertEqual(stored["created_ts"], winner_crt)
        # 7. winner artifact_path remains unchanged
        self.assertEqual(stored["artifact_path"], vol_path)
        # 8. winner hash remains unchanged
        self.assertEqual(stored["source_definition_hash"], "winner_hash")
        # 9. winner target fields remain unchanged
        self.assertEqual(stored["target_catalog"], "cat")
        # 10. UNABLE_TO_MATERIALIZE does not overwrite active owner
        self.assertNotEqual(stored["materialization_status"], "UNABLE_TO_MATERIALIZE")
        # 11. no second owner row exists
        self.assertEqual(len(res["spark"].manifest_table), 1)
        # 12. current-run output contains bounded concurrency/persistence conflict
        self.assertTrue(any("CONCURRENT_MATERIALIZATION" in str(e) for e in res["env"]["errors"]))
        # 13. output contains no source SQL or secrets
        errs = " ".join(str(e) for e in res["env"]["errors"])
        self.assertNotIn("SELECT", errs)
        self.assertNotIn("password", errs)
        # 14. skipped_count remains logically correct
        self.assertEqual(res["task_values"]["skipped_count"], 1)
        # 15. failed_count is not double-counted
        self.assertEqual(res["task_values"]["failed_count"], 0)

    # --- SECTION 12: TEST TARGET CONFIG ERROR CANNOT OVERWRITE ACTIVE CLAIM ---

    def test_concurrency_17_target_config_error_cannot_overwrite_active_claim(self):
        cand = FakeRow(connection_id="c1", source_system="oracle", source_database="db1", source_schema="s1", object_name="v1", object_type="VIEW", source_definition="SELECT 1")
        winner_upd = datetime(2026, 9, 21, 11, 55, 0, tzinfo=timezone.utc)
        winner_crt = datetime(2026, 9, 21, 11, 50, 0, tzinfo=timezone.utc)
        vol_path = "/Volumes/cat/s1/_source_artifacts/c1/s1/views/v1.sql"

        def pre_general_merge_hook(spark_sess):
            spark_sess.manifest_table[("c1", "s1", "VIEW", "v1")] = {
                "connection_id": "c1", "source_schema": "s1", "object_type": "VIEW", "object_name": "v1",
                "materialization_status": "IN_PROGRESS", "last_materialized_run_id": "run-winner",
                "updated_ts": winner_upd,
                "created_ts": winner_crt,
                "artifact_path": vol_path,
                "source_definition_hash": "winner_hash",
                "target_catalog": "cat", "target_schema": "s1", "target_volume": "_source_artifacts",
            }

        res = run_nb18_harness(
            candidates=[cand],
            target_configs={"c1": Exception("Missing target config")},
            pre_general_merge_hook=pre_general_merge_hook,
            widget_values={"run_id": "run-loser"}
        )
        stored = res["spark"].manifest_table[("c1", "s1", "VIEW", "v1")]
        # active owner remains IN_PROGRESS
        self.assertEqual(stored["materialization_status"], "IN_PROGRESS")
        # active run ID remains unchanged
        self.assertEqual(stored["last_materialized_run_id"], "run-winner")
        # active updated_ts remains unchanged
        self.assertEqual(stored["updated_ts"], winner_upd)
        # active created_ts remains unchanged
        self.assertEqual(stored["created_ts"], winner_crt)
        # artifact_path remains unchanged
        self.assertEqual(stored["artifact_path"], vol_path)
        # hash remains unchanged
        self.assertEqual(stored["source_definition_hash"], "winner_hash")
        # target metadata remains unchanged
        self.assertEqual(stored["target_catalog"], "cat")
        # TARGET_CONFIG_ERROR does not overwrite the active owner
        self.assertNotEqual(stored["materialization_status"], "TARGET_CONFIG_ERROR")
        # no duplicate owner row exists
        self.assertEqual(len(res["spark"].manifest_table), 1)
        # no file operation occurs
        self.assertEqual(res["fs"].write_count, 0)
        # failed_count is not incremented twice for the same candidate
        self.assertEqual(res["task_values"]["failed_count"], 1)

    # --- SECTION 13: PRE-CLAIM FAILURE INSERTS WHEN OWNER DOES NOT EXIST ---

    def test_concurrency_18_pre_claim_failure_inserts_when_no_owner(self):
        cand = FakeRow(connection_id="c1", source_system="oracle", source_database="db1", source_schema="s1", object_name="v1", object_type="VIEW", source_definition="SELECT 1")
        res = run_nb18_harness(
            candidates=[cand],
            target_configs={"c1": Exception("Missing target config")},
            widget_values={"run_id": "run-curr"}
        )
        # 1. exactly one owner row is inserted
        self.assertEqual(len(res["spark"].manifest_table), 1)
        stored = res["spark"].manifest_table[("c1", "s1", "VIEW", "v1")]
        # 2. status is TARGET_CONFIG_ERROR
        self.assertEqual(stored["materialization_status"], "TARGET_CONFIG_ERROR")
        # 3. last_materialized_run_id is current run
        self.assertEqual(stored["last_materialized_run_id"], "run-curr")
        # 4. error_message is sanitized and bounded
        self.assertIsNotNone(stored.get("error_message"))
        self.assertLessEqual(len(stored["error_message"]), 1000)
        # 5. no file operation occurs
        self.assertEqual(res["fs"].write_count, 0)
        # 6. failed_count increments once
        self.assertEqual(res["task_values"]["failed_count"], 1)
        # 7. owner key remains the four-field key
        self.assertEqual((stored["connection_id"], stored["source_schema"], stored["object_type"], stored["object_name"]), ("c1", "s1", "VIEW", "v1"))

    # --- SECTION 14: PRE-CLAIM FAILURE UPDATES A NON-ACTIVE OWNER ---

    def test_concurrency_19_pre_claim_failure_updates_non_active_owner(self):
        crt_ts = datetime(2026, 9, 20, 10, 0, 0, tzinfo=timezone.utc)
        manifest_row = FakeRow(
            connection_id="c1", source_schema="s1", object_type="VIEW", object_name="v1",
            materialization_status="WRITE_FAILED", last_materialized_run_id="run-prior",
            created_ts=crt_ts, updated_ts=datetime(2026, 9, 20, 10, 0, 0, tzinfo=timezone.utc)
        )
        cand = FakeRow(connection_id="c1", source_system="oracle", source_database="db1", source_schema="s1", object_name="v1", object_type="VIEW", source_definition="SELECT 1")
        res = run_nb18_harness(
            candidates=[cand],
            manifest_rows=[manifest_row],
            target_configs={"c1": Exception("Target config broken")},
            widget_values={"run_id": "run-curr"}
        )
        # protected general MERGE may update the row
        stored = res["spark"].manifest_table[("c1", "s1", "VIEW", "v1")]
        # one owner row remains
        self.assertEqual(len(res["spark"].manifest_table), 1)
        # created_ts remains unchanged
        self.assertEqual(stored["created_ts"], crt_ts)
        # current run ID is stored
        self.assertEqual(stored["last_materialized_run_id"], "run-curr")
        # expected failure status is stored
        self.assertEqual(stored["materialization_status"], "TARGET_CONFIG_ERROR")
        # no file operation occurs
        self.assertEqual(res["fs"].write_count, 0)

    # --- SECTION 15: STALE IN_PROGRESS MAY BE UPDATED BY GENERAL MERGE ---

    def test_concurrency_20_stale_in_progress_may_be_updated_by_general_merge(self):
        crt_ts = datetime(2026, 9, 21, 8, 0, 0, tzinfo=timezone.utc)
        stale_upd_ts = datetime(2026, 9, 21, 9, 0, 0, tzinfo=timezone.utc)
        manifest_row = FakeRow(
            connection_id="c1", source_schema="s1", object_type="VIEW", object_name="v1",
            materialization_status="IN_PROGRESS", last_materialized_run_id="old-run",
            created_ts=crt_ts, updated_ts=stale_upd_ts
        )
        cand = FakeRow(connection_id="c1", source_system="oracle", source_database="db1", source_schema="s1", object_name="v1", object_type="VIEW", source_definition="SELECT 1")
        res = run_nb18_harness(
            candidates=[cand],
            manifest_rows=[manifest_row],
            target_configs={"c1": Exception("Target config broken")},
            widget_values={"run_id": "new-run"}
        )
        # stale owner may be updated
        stored = res["spark"].manifest_table[("c1", "s1", "VIEW", "v1")]
        # one owner row remains
        self.assertEqual(len(res["spark"].manifest_table), 1)
        # created_ts remains unchanged
        self.assertEqual(stored["created_ts"], crt_ts)
        # current run ID is stored
        self.assertEqual(stored["last_materialized_run_id"], "new-run")
        # expected queued status is stored
        self.assertEqual(stored["materialization_status"], "TARGET_CONFIG_ERROR")
        # no file operation occurs
        self.assertEqual(res["fs"].write_count, 0)

    # --- SECTION 16: RECENT OTHER-RUN OWNER BLOCKS EVERY GENERAL STATUS ---

    def test_concurrency_21_recent_other_run_owner_blocks_every_general_status(self):
        statuses_to_test = [
            ("UNABLE_TO_MATERIALIZE", {"source_definition": ""}, {}),
            ("TARGET_CONFIG_ERROR", {"source_definition": "SELECT 1"}, {"target_configs": {"c1": Exception("err")}}),
            ("TARGET_CONFIG_CHANGED", {"source_definition": "SELECT 1"}, {"manifest_rows": [FakeRow(connection_id="c1", source_schema="s1", object_type="VIEW", object_name="v1", artifact_path="/other/path.sql", materialization_status="SUCCEEDED", source_definition_hash="h1")]}),
            ("ARTIFACT_PATH_COLLISION", {"source_definition": "SELECT 1"}, {"manifest_rows": [FakeRow(connection_id="c1", source_database="db1", source_schema="s1", object_type="VIEW", object_name="v_other", artifact_path="/Volumes/da_accelerators/s1/_source_artifacts/c1/db1/s1/views/v1.sql", materialization_status="SUCCEEDED", source_definition_hash="h1")]}),
            ("FAILED", {"source_definition": "SELECT 1"}, {"manifest_rows": [
                FakeRow(connection_id="c1", source_schema="s1", object_type="VIEW", object_name="v1", artifact_path="/Volumes/da_accelerators/s1/_source_artifacts/c1/s1/views/v1.sql", materialization_status="SUCCEEDED", source_definition_hash="h1"),
                FakeRow(connection_id="c1", source_schema="s1", object_type="VIEW", object_name="v1", artifact_path="/Volumes/da_accelerators/s1/_source_artifacts/c1/s1/views/v1.sql", materialization_status="SUCCEEDED", source_definition_hash="h1"),
            ]}),
        ]
        for status_name, cand_kwargs, harness_kwargs in statuses_to_test:
            with self.subTest(status=status_name):
                winner_upd = datetime(2026, 9, 21, 11, 55, 0, tzinfo=timezone.utc)
                winner_crt = datetime(2026, 9, 21, 11, 50, 0, tzinfo=timezone.utc)
                vol_path = "/Volumes/cat/s1/_source_artifacts/c1/s1/views/v1.sql"

                def pre_general_merge_hook(spark_sess):
                    spark_sess.manifest_table[("c1", "s1", "VIEW", "v1")] = {
                        "connection_id": "c1", "source_schema": "s1", "object_type": "VIEW", "object_name": "v1",
                        "materialization_status": "IN_PROGRESS", "last_materialized_run_id": "run-winner",
                        "updated_ts": winner_upd, "created_ts": winner_crt, "artifact_path": vol_path,
                        "source_definition_hash": "hash_w", "target_catalog": "cat",
                    }

                cand = FakeRow(connection_id="c1", source_system="oracle", source_database="db1", source_schema="s1", object_name="v1", object_type="VIEW", **cand_kwargs)
                res = run_nb18_harness(
                    candidates=[cand],
                    pre_general_merge_hook=pre_general_merge_hook,
                    widget_values={"run_id": "run-loser"},
                    **harness_kwargs
                )
                stored = res["spark"].manifest_table[("c1", "s1", "VIEW", "v1")]
                # target row remains IN_PROGRESS
                self.assertEqual(stored["materialization_status"], "IN_PROGRESS")
                # winner run ID remains unchanged
                self.assertEqual(stored["last_materialized_run_id"], "run-winner")
                # no target field is changed
                self.assertEqual(stored["updated_ts"], winner_upd)
                self.assertEqual(stored["created_ts"], winner_crt)
                self.assertEqual(stored["artifact_path"], vol_path)
                self.assertEqual(stored["source_definition_hash"], "hash_w")
                self.assertEqual(stored["target_catalog"], "cat")

    # --- SECTION 17: TEST GENERAL MERGE SQL IS CONDITIONED ---

    def test_static_safety_general_merge_sql_conditioned(self):
        with open("notebooks/shared/NB18_MaterializeSourceArtifacts.py", "r", encoding="utf-8") as f:
            code = f.read()
        start_fn = code.find("def _merge_manifest_rows(")
        self.assertNotEqual(start_fn, -1)
        end_fn = code.find("\ndef _claim_manifest_owner(", start_fn)
        self.assertNotEqual(end_fn, -1)
        fn_body = code[start_fn:end_fn]

        # Require target materialization_status comparison with IN_PROGRESS
        self.assertTrue(
            "t.materialization_status <> 'IN_PROGRESS'" in fn_body
            or "t.materialization_status <> '{ACTIVE_MATERIALIZATION_STATUS}'" in fn_body
        )
        # Require target last_materialized_run_id comparison
        self.assertIn("t.last_materialized_run_id", fn_body)
        # Require stale updated_ts condition
        self.assertIn("t.updated_ts IS NULL", fn_body)
        self.assertIn("current_timestamp() - INTERVAL", fn_body)
        # Require STALE_MATERIALIZATION_MINUTES usage or generated interval
        self.assertTrue(
            "STALE_MATERIALIZATION_MINUTES" in fn_body
            or "120 MINUTES" in fn_body
        )
        # Require no unconditional matched update in this specific helper
        self.assertNotIn("WHEN MATCHED THEN UPDATE", fn_body)

    # --- SECTION 23: STATIC SAFETY CHECKS ---

    def test_static_safety_active_conflict_does_not_mutate_manifest(self):
        with open("notebooks/shared/NB18_MaterializeSourceArtifacts.py", "r", encoding="utf-8") as f:
            code = f.read()
        self.assertNotIn('_append_manifest_result(row=row, object_type=norm_type, materialization_status="CONCURRENT_MATERIALIZATION"', code)
        self.assertNotIn('materialization_status="CONCURRENT_MATERIALIZATION"', code)

    def test_static_safety_conditional_in_progress_claim_and_verification(self):
        with open("notebooks/shared/NB18_MaterializeSourceArtifacts.py", "r", encoding="utf-8") as f:
            code = f.read()
        self.assertIn("def _claim_manifest_owner(", code)
        self.assertIn("def _finalize_claimed_owner(", code)
        self.assertIn("def _merge_manifest_rows(", code)
        # Verify claim happens before _write_artifact_file
        claim_idx = code.find("_claim_manifest_owner(")
        write_idx = code.find("_write_artifact_file(")
        self.assertNotEqual(claim_idx, -1)
        self.assertNotEqual(write_idx, -1)
        self.assertLess(claim_idx, write_idx)


if __name__ == "__main__":
    unittest.main()


