"""lakebridge_artifact_common.py - Pure Python Lakebridge Analyzer & BladeBridge parsing.

Provides pure-Python helpers for:
- normalizing source systems and artifact types
- preparing Lakebridge local input file structures
- parsing Lakebridge Analyzer workbook sheets (.xlsx, .csv, .json)
- extracting complexity, statement counts, unknown fragments, constructs, and referenced objects
- detecting FIXME markers and remaining source-specific syntax
- deriving authoritative Lakebridge classification and mapping to control statuses
- producing bounded JSON and sanitized error logs
- constructing persistent Unity Catalog Volume paths

Pure Python: NO Spark, NO dbutils, NO JDBC, NO direct Delta dependencies.
Fully unit-testable in any standard Python environment.
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import os
import re
import uuid
import zipfile
import xml.etree.ElementTree as ET
from typing import Any, Dict, List, Optional, Tuple, Union

try:
    from src.identifiers import quote_databricks
    from src.failure_classifier import sanitize_message
    from src.sql_object_artifact_common import (
        sanitize_path_component,
        build_artifact_relative_path,
        build_artifact_volume_path,
    )
except ModuleNotFoundError:
    from identifiers import quote_databricks
    from failure_classifier import sanitize_message
    from sql_object_artifact_common import (
        sanitize_path_component,
        build_artifact_relative_path,
        build_artifact_volume_path,
    )

# Lakebridge Classifications
LAKEBRIDGE_CLASSIFICATION_AUTO_CANDIDATE = "AUTO_CANDIDATE"
LAKEBRIDGE_CLASSIFICATION_MANUAL_REVIEW = "MANUAL_REVIEW"
LAKEBRIDGE_CLASSIFICATION_UNSUPPORTED = "UNSUPPORTED"

# Stages
STAGE_SELECTED_SOURCE_FETCH = "SELECTED_SOURCE_FETCH"
STAGE_LAKEBRIDGE_ANALYZE = "LAKEBRIDGE_ANALYZE"
STAGE_LAKEBRIDGE_TRANSPILE = "LAKEBRIDGE_TRANSPILE"
STAGE_LAKEBRIDGE_STORE = "LAKEBRIDGE_STORE"
STAGE_LAKEBRIDGE_CLASSIFY = "LAKEBRIDGE_CLASSIFY"

# Complexity levels
VALID_COMPLEXITIES = ("LOW", "MEDIUM", "HIGH", "VERY_HIGH")


def normalize_source_system(source_system: Any) -> str:
    """Normalize source system name to canonical ('oracle' or 'mssql')."""
    if source_system is None:
        raise ValueError("source_system cannot be blank")
    s = str(source_system).strip().lower()
    if s in ("oracle", "ora"):
        return "oracle"
    if s in ("sqlserver", "sql_server", "mssql", "microsoft_sql_server"):
        return "mssql"
    raise ValueError(f"Unsupported source_system: {source_system!r}")


def normalize_object_type(object_type: Any) -> str:
    """Validate and normalize object type to 'VIEW' or 'PROCEDURE'."""
    if object_type is None:
        raise ValueError("object_type cannot be blank")
    s = str(object_type).strip().upper().replace(" ", "_")
    if s not in ("VIEW", "PROCEDURE"):
        raise ValueError(f"Lakebridge SQL artifact scope supports only VIEW and PROCEDURE, got {object_type!r}")
    return s


def sanitize_filename(name: str) -> str:
    """Convert object name into a safe filesystem filename."""
    s = str(name or "").strip()
    s = re.sub(r"[^a-zA-Z0-9_\-]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s or "unnamed"


def col_letter_to_index(col_letters: str) -> int:
    """Convert Excel column letters (A, C, AA, AB) to 0-based column index."""
    idx = 0
    for char in str(col_letters).upper():
        if 'A' <= char <= 'Z':
            idx = idx * 26 + (ord(char) - ord('A') + 1)
        else:
            break
    return max(0, idx - 1)


def prepare_lakebridge_input_file(
    input_base_dir: str,
    connection_id: str,
    source_database: Optional[str],
    source_schema: str,
    object_type: str,
    object_name: str,
    source_definition: str,
    artifact_id: Optional[str] = None,
    source_system: Optional[str] = None,
) -> str:
    """Write raw source definition to Lakebridge local input directory.

    Guarantees collision resistance: distinct objects (such as 'A/B' and 'A:B')
    never collide on the same local file path.
    Returns the absolute path of the written .sql file.
    """
    if not source_definition or not str(source_definition).strip():
        raise ValueError("Cannot prepare Lakebridge input with empty source_definition")

    if artifact_id:
        file_path = os.path.join(input_base_dir, f"{artifact_id}.sql")
    else:
        rel_path = build_artifact_relative_path(
            connection_id=connection_id,
            source_system=source_system or "source",
            source_database=source_database,
            source_schema=source_schema,
            object_type=object_type,
            object_name=object_name,
        )
        file_path = os.path.join(input_base_dir, rel_path)

    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(source_definition)

    return file_path


def _normalize_token(t: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", str(t or "").strip().lower()).strip("_")


def read_analyzer_workbook(
    file_path_or_bytes: Union[str, bytes],
    required_headers: Optional[List[str]] = None,
) -> Dict[str, List[Dict[str, Any]]]:
    """Pure Python reader for Lakebridge Analyzer workbook (.xlsx, .csv, or .json).

    Implements explicit format dispatch:
      - .xlsx -> XLSX parser preserving coordinates & resolving column indices
      - .csv  -> Python csv module
      - .json -> JSON parser
      - anything else -> explicit ValueError

    Returns: dict of normalized_sheet_name -> list of row dicts with normalized headers.
    """
    if isinstance(file_path_or_bytes, str):
        if not os.path.exists(file_path_or_bytes):
            raise FileNotFoundError(f"Analyzer workbook not found: {file_path_or_bytes}")
        _, ext = os.path.splitext(file_path_or_bytes.lower())
        if ext == ".xlsx":
            target_format = "xlsx"
        elif ext == ".csv":
            target_format = "csv"
        elif ext == ".json":
            target_format = "json"
        else:
            raise ValueError(f"Unsupported Analyzer report format: {ext!r}. Expected .xlsx, .csv, or .json")
    else:
        # Byte inspect
        if file_path_or_bytes.startswith(b"PK\x03\x04"):
            target_format = "xlsx"
        elif file_path_or_bytes.strip().startswith(b"{") or file_path_or_bytes.strip().startswith(b"["):
            target_format = "json"
        elif b"," in file_path_or_bytes[:1000] or b"\n" in file_path_or_bytes[:1000]:
            target_format = "csv"
        else:
            raise ValueError("Unsupported Analyzer report format bytes; expected XLSX (zip), CSV, or JSON")

    # 1. JSON dispatch
    if target_format == "json":
        try:
            if isinstance(file_path_or_bytes, str):
                with open(file_path_or_bytes, "r", encoding="utf-8") as f:
                    data = json.load(f)
            else:
                data = json.loads(file_path_or_bytes.decode("utf-8"))
        except Exception as e:
            raise ValueError(f"Failed to parse Analyzer JSON report: {e}") from e

        if isinstance(data, dict):
            return {
                _normalize_token(k): (v if isinstance(v, list) else [v])
                for k, v in data.items()
            }
        elif isinstance(data, list):
            return {"summary": data}
        raise ValueError("Invalid JSON Analyzer report structure: expected dict or list")

    # 2. CSV dispatch
    if target_format == "csv":
        try:
            if isinstance(file_path_or_bytes, bytes):
                text = file_path_or_bytes.decode("utf-8-sig", errors="replace")
                f = io.StringIO(text)
                raw_rows = list(csv.reader(f))
            else:
                with open(file_path_or_bytes, "r", encoding="utf-8-sig") as f:
                    raw_rows = list(csv.reader(f))
        except Exception as e:
            raise ValueError(f"Failed to parse Analyzer CSV report: {e}") from e

        if not raw_rows:
            return {"summary": []}

        raw_headers = raw_rows[0]
        headers = [_normalize_token(h) for h in raw_headers]

        # Reject duplicate required/non-empty headers
        non_empty_headers = [h for h in headers if h]
        seen_headers = set()
        for h in non_empty_headers:
            if h in seen_headers:
                raise ValueError(f"Duplicate header detected in Analyzer report: {h}")
            seen_headers.add(h)

        if required_headers:
            for req in required_headers:
                req_norm = _normalize_token(req)
                if req_norm not in seen_headers:
                    raise ValueError(f"Missing required header: {req}")

        sheet_rows: List[Dict[str, Any]] = []
        for r in raw_rows[1:]:
            if not any(c.strip() for c in r if c is not None):
                continue
            row_dict = {}
            for i, h in enumerate(headers):
                if h:
                    val = r[i].strip() if i < len(r) and r[i] is not None and r[i].strip() != "" else None
                    row_dict[h] = val
            sheet_rows.append(row_dict)

        return {"summary": sheet_rows}

    # 3. XLSX dispatch
    try:
        if isinstance(file_path_or_bytes, bytes):
            zf = zipfile.ZipFile(io.BytesIO(file_path_or_bytes))
        else:
            zf = zipfile.ZipFile(file_path_or_bytes)
    except Exception as e:
        raise ValueError(f"Failed to read Analyzer workbook archive: {e}") from e

    with zf:
        # Shared strings
        shared_strings: List[str] = []
        if "xl/sharedStrings.xml" in zf.namelist():
            try:
                ss_root = ET.fromstring(zf.read("xl/sharedStrings.xml"))
                for si in ss_root.findall(".//{*}si"):
                    texts = [t.text or "" for t in si.findall(".//{*}t")]
                    shared_strings.append("".join(texts))
            except Exception as e:
                raise ValueError(f"Malformed shared strings in Analyzer workbook: {e}") from e

        # Map sheet names to sheet XML files
        sheet_map: Dict[str, str] = {}
        if "xl/workbook.xml" in zf.namelist():
            try:
                wb_root = ET.fromstring(zf.read("xl/workbook.xml"))
                for idx, sheet_el in enumerate(wb_root.findall(".//{*}sheet"), 1):
                    s_name = sheet_el.attrib.get("name") or f"sheet{idx}"
                    s_id = sheet_el.attrib.get("sheetId") or str(idx)
                    target_file = f"xl/worksheets/sheet{idx}.xml"
                    if target_file not in zf.namelist():
                        target_file = f"xl/worksheets/sheet{s_id}.xml"
                    sheet_map[_normalize_token(s_name)] = target_file
            except Exception as e:
                raise ValueError(f"Malformed workbook XML in Analyzer archive: {e}") from e

        if not sheet_map:
            for name in zf.namelist():
                if name.startswith("xl/worksheets/sheet") and name.endswith(".xml"):
                    base = os.path.splitext(os.path.basename(name))[0]
                    sheet_map[_normalize_token(base)] = name

        result: Dict[str, List[Dict[str, Any]]] = {}

        for norm_sheet_name, sheet_path in sheet_map.items():
            if sheet_path not in zf.namelist():
                continue
            try:
                sheet_xml = zf.read(sheet_path)
                sheet_root = ET.fromstring(sheet_xml)
            except Exception as e:
                raise ValueError(f"Malformed sheet XML in Analyzer workbook {sheet_path}: {e}") from e

            # Parse rows preserving cell coordinates
            rows_by_index: Dict[int, Dict[int, Any]] = {}
            for row_el in sheet_root.findall(".//{*}row"):
                r_num_str = row_el.attrib.get("r")
                seq_col = 0
                for c_el in row_el.findall(".//{*}c"):
                    r_coord = c_el.attrib.get("r")
                    if r_coord:
                        m_coord = re.match(r"^([A-Za-z]+)(\d+)$", r_coord)
                        if m_coord:
                            col_str, row_str = m_coord.groups()
                            col_idx = col_letter_to_index(col_str)
                            row_num = int(row_str)
                        else:
                            col_idx = seq_col
                            row_num = int(r_num_str) if r_num_str and r_num_str.isdigit() else len(rows_by_index) + 1
                    else:
                        col_idx = seq_col
                        row_num = int(r_num_str) if r_num_str and r_num_str.isdigit() else len(rows_by_index) + 1
                    seq_col = col_idx + 1

                    # Extract value
                    c_type = c_el.attrib.get("t")
                    v_el = c_el.find("{*}v")
                    val: Any = None
                    if v_el is not None and v_el.text is not None:
                        val = v_el.text
                        if c_type == "s":
                            try:
                                s_idx = int(val)
                                val = shared_strings[s_idx] if 0 <= s_idx < len(shared_strings) else val
                            except Exception:
                                pass
                        elif c_type == "b":
                            val = (val == "1")
                        elif c_type in ("n", None):
                            try:
                                if "." in val:
                                    val = float(val)
                                else:
                                    val = int(val)
                            except Exception:
                                pass
                    else:
                        is_el = c_el.find(".//{*}t")
                        if is_el is not None and is_el.text:
                            val = is_el.text

                    if row_num not in rows_by_index:
                        rows_by_index[row_num] = {}
                    rows_by_index[row_num][col_idx] = val

            if not rows_by_index:
                result[norm_sheet_name] = []
                continue

            sorted_row_nums = sorted(rows_by_index.keys())
            header_row_num = sorted_row_nums[0]
            header_cells = rows_by_index[header_row_num]

            max_header_col = max(header_cells.keys()) if header_cells else -1
            headers: List[str] = []
            for col_i in range(max_header_col + 1):
                raw_h = header_cells.get(col_i)
                headers.append(_normalize_token(str(raw_h or "")) if raw_h is not None else "")

            # Reject duplicate required/non-empty headers
            non_empty_headers = [h for h in headers if h]
            seen_headers = set()
            for h in non_empty_headers:
                if h in seen_headers:
                    raise ValueError(f"Duplicate header detected in sheet '{norm_sheet_name}': {h}")
                seen_headers.add(h)

            if required_headers:
                for req in required_headers:
                    req_norm = _normalize_token(req)
                    if req_norm not in seen_headers:
                        raise ValueError(f"Missing required header in sheet '{norm_sheet_name}': {req}")

            sheet_rows = []
            for row_num in sorted_row_nums[1:]:
                row_cells = rows_by_index[row_num]
                # Normalize row against detected header width, inserting None for skipped/blank cells
                row_dict = {}
                has_any_value = False
                for col_i, h in enumerate(headers):
                    if h:
                        val = row_cells.get(col_i, None)
                        if val is not None and str(val).strip() != "":
                            row_dict[h] = val
                            has_any_value = True
                        else:
                            row_dict[h] = None
                if has_any_value:
                    sheet_rows.append(row_dict)

            result[norm_sheet_name] = sheet_rows

        return result


def extract_complexity(workbook_data: Dict[str, List[Dict[str, Any]]], object_name: Optional[str] = None) -> Optional[str]:
    """Extract complexity ('LOW', 'MEDIUM', 'HIGH', 'VERY_HIGH', or None) from Analyzer sheets."""
    for s_name, rows in workbook_data.items():
        if any(k in s_name for k in ("complex", "summary", "overview", "assessment")):
            for row in rows:
                if object_name:
                    row_obj = str(row.get("object_name") or row.get("object") or row.get("name") or "").strip().lower()
                    if row_obj and row_obj != object_name.strip().lower():
                        continue
                for col_key, val in row.items():
                    if "complex" in col_key and val:
                        raw = str(val).strip().upper().replace(" ", "_")
                        for comp in ("VERY_HIGH", "HIGH", "MEDIUM", "LOW"):
                            if comp in raw:
                                return comp
    return None


def extract_statement_counts(
    workbook_data: Dict[str, List[Dict[str, Any]]],
    object_name: Optional[str] = None,
) -> Tuple[Optional[int], Optional[int]]:
    """Extract (statement_count, unknown_statement_count)."""
    stmt_count: Optional[int] = None
    unknown_count: Optional[int] = None

    for s_name, rows in workbook_data.items():
        if any(k in s_name for k in ("statement", "summary", "complexity")):
            for row in rows:
                if object_name:
                    row_obj = str(row.get("object_name") or row.get("object") or row.get("name") or "").strip().lower()
                    if row_obj and row_obj != object_name.strip().lower():
                        continue
                for col_key, val in row.items():
                    if ("unknown" in col_key or "unsupported" in col_key) and val is not None:
                        try:
                            unknown_count = int(val)
                        except Exception:
                            pass
                    elif ("statement" in col_key or "stmt" in col_key) and "unknown" not in col_key and val is not None:
                        try:
                            stmt_count = int(val)
                        except Exception:
                            pass

    return stmt_count, unknown_count


def extract_unknown_fragments(
    workbook_data: Dict[str, List[Dict[str, Any]]],
    object_name: Optional[str] = None,
) -> List[str]:
    """Extract unknown fragments reported by Analyzer."""
    fragments: List[str] = []
    for s_name, rows in workbook_data.items():
        if any(k in s_name for k in ("fragment", "unknown", "error", "unsupported")):
            for row in rows:
                if object_name:
                    row_obj = str(row.get("object_name") or row.get("object") or row.get("name") or "").strip().lower()
                    if row_obj and row_obj != object_name.strip().lower():
                        continue
                for col_key, val in row.items():
                    if any(t in col_key for t in ("fragment", "syntax", "unknown", "text", "snippet")) and val:
                        txt = str(val).strip()
                        if txt and txt not in fragments:
                            fragments.append(txt)
    return fragments


def detect_sql_constructs(sql_text: str, source_system: str = "mssql") -> Dict[str, bool]:
    """Detect presence of error handling, rowcount, cursors, dynamic SQL, and triggers.

    Case-insensitive pure-python token search.
    """
    s = sql_text or ""

    # Error handling
    uses_error_handling = bool(
        re.search(r"\bBEGIN\s+TRY\b", s, re.I)
        or re.search(r"\bBEGIN\s+CATCH\b", s, re.I)
        or re.search(r"\bRAISERROR\b", s, re.I)
        or re.search(r"\bTHROW\b", s, re.I)
        or re.search(r"\bEXCEPTION\s+WHEN\b", s, re.I)
        or re.search(r"\bRAISE_APPLICATION_ERROR\b", s, re.I)
    )

    # @@ROWCOUNT
    uses_rowcount = bool(
        re.search(r"@@ROWCOUNT\b", s, re.I)
        or re.search(r"SQL%ROWCOUNT\b", s, re.I)
    )

    # Cursor
    uses_cursor = bool(
        re.search(r"\bCURSOR\b", s, re.I)
        or re.search(r"\bFETCH\s+NEXT\b", s, re.I)
        or re.search(r"\bDEALLOCATE\b", s, re.I)
        or re.search(r"\bOPEN\s+[a-zA-Z0-9_#]+", s, re.I)
    )

    # Dynamic SQL
    uses_dynamic_sql = bool(
        re.search(r"\bsp_executesql\b", s, re.I)
        or re.search(r"\bEXEC(?:UTE)?\s*\(", s, re.I)
        or re.search(r"\bEXECUTE\s+IMMEDIATE\b", s, re.I)
    )

    # Trigger
    uses_trigger = bool(
        re.search(r"\bCREATE\s+(?:OR\s+REPLACE\s+)?TRIGGER\b", s, re.I)
        or re.search(r"\bINSTEAD\s+OF\b", s, re.I)
    )

    return {
        "uses_error_handling": uses_error_handling,
        "uses_rowcount": uses_rowcount,
        "uses_cursor": uses_cursor,
        "uses_dynamic_sql": uses_dynamic_sql,
        "uses_trigger": uses_trigger,
    }


def count_fixme_markers(sql_text: str) -> int:
    """Count number of FIXME markers in converted SQL text."""
    if not sql_text:
        return 0
    return len(re.findall(r"\bFIXME\b", sql_text, re.I))


def detect_remaining_source_syntax(sql_text: str, source_system: str) -> Tuple[bool, List[str]]:
    """Detect if converted output retains untranspiled source-specific dialect constructs."""
    if not sql_text:
        return False, []
    sys_norm = normalize_source_system(source_system)
    retained = []

    if sys_norm == "mssql":
        patterns = [
            (r"\[dbo\]", "[dbo] bracket syntax"),
            (r"\bISNULL\s*\(", "T-SQL ISNULL function"),
            (r"\bGETDATE\s*\(\)", "T-SQL GETDATE()"),
            (r"\bTOP\s+\(?\d+\)?", "T-SQL TOP clause"),
            (r"\bIDENTITY\s*\(", "T-SQL IDENTITY"),
            (r"\bN'[^\']*'", "T-SQL N'...' literal"),
            (r"#[a-zA-Z0-9_]+", "T-SQL temporary table #..."),
            (r"@@[a-zA-Z0-9_]+", "T-SQL system variable @@..."),
        ]
    else:  # oracle
        patterns = [
            (r"\bNVL2\s*\(", "Oracle NVL2 function"),
            (r"\bSYSDATE\b", "Oracle SYSDATE"),
            (r"\bROWNUM\b", "Oracle ROWNUM"),
            (r"\bFROM\s+DUAL\b", "Oracle DUAL table"),
            (r"\bVARCHAR2\b", "Oracle VARCHAR2"),
            (r"\bNUMBER\s*\(", "Oracle NUMBER type"),
        ]

    for pat, desc in patterns:
        if re.search(pat, sql_text, re.I):
            retained.append(desc)

    return bool(retained), retained


def extract_referenced_objects(
    workbook_data: Dict[str, List[Dict[str, Any]]],
    sql_text: Optional[str] = None,
    object_name: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Extract referenced objects and operations (READ/WRITE) from Analyzer workbook or SQL text."""
    refs: List[Dict[str, Any]] = []

    # 1. Try from workbook
    for s_name, rows in workbook_data.items():
        if any(k in s_name for k in ("reference", "dependency", "lineage", "object")):
            for row in rows:
                if object_name:
                    row_caller = str(row.get("source_object") or row.get("caller") or row.get("object_name") or "").strip().lower()
                    if row_caller and row_caller != object_name.strip().lower():
                        continue
                obj = str(row.get("referenced_object") or row.get("object") or row.get("target") or "").strip()
                if not obj and row.get("referenced_table"):
                    ref_sch = str(row.get("referenced_schema") or "").strip()
                    ref_tbl = str(row.get("referenced_table") or "").strip()
                    obj = f"{ref_sch}.{ref_tbl}" if ref_sch else ref_tbl
                op = str(row.get("operation") or row.get("action") or "READ").strip().upper()
                cnt = row.get("count") or 1
                try:
                    cnt = int(cnt)
                except Exception:
                    cnt = 1
                if obj:
                    refs.append({"object": obj, "operation": op, "count": cnt})

    if refs:
        return refs

    # 2. Heuristic extraction from SQL if workbook has no explicit table
    if sql_text:
        found: Dict[Tuple[str, str], int] = {}
        for m in re.finditer(r"\b(?:FROM|JOIN)\s+([a-zA-Z0-9_\.\`\"\[\]]+)", sql_text, re.I):
            target = m.group(1).strip()
            found[(target, "READ")] = found.get((target, "READ"), 0) + 1
        for m in re.finditer(r"\b(?:INSERT\s+INTO|UPDATE|MERGE\s+INTO)\s+([a-zA-Z0-9_\.\`\"\[\]]+)", sql_text, re.I):
            target = m.group(1).strip()
            found[(target, "WRITE")] = found.get((target, "WRITE"), 0) + 1

        for (target, op), cnt in found.items():
            refs.append({"object": target, "operation": op, "count": cnt})

    return refs


def build_bounded_json(data: Any, max_bytes: int = 8192) -> str:
    """Serialize object to JSON bounded within max_bytes."""
    raw = json.dumps(data)
    if len(raw.encode("utf-8")) <= max_bytes:
        return raw

    if isinstance(data, list):
        trimmed = list(data)
        while trimmed and len(json.dumps(trimmed).encode("utf-8")) > max_bytes:
            trimmed.pop()
        return json.dumps(trimmed)
    elif isinstance(data, dict):
        trimmed = dict(data)
        keys = list(trimmed.keys())
        while keys and len(json.dumps(trimmed).encode("utf-8")) > max_bytes:
            k = keys.pop()
            del trimmed[k]
        return json.dumps(trimmed)
    else:
        s = str(data)
        return json.dumps(s[:max_bytes // 2])


def derive_lakebridge_classification(
    source_definition: Optional[str],
    converted_definition: Optional[str],
    complexity: Optional[str],
    statement_count: Optional[int],
    unknown_statement_count: Optional[int],
    unknown_fragments: List[str],
    constructs: Dict[str, bool],
    parsing_error_count: int,
    validation_error_count: int,
    generation_error_count: int,
    fixme_count: int,
    remaining_source_syntax: List[str],
    analyzer_failed: bool = False,
    transpile_failed: bool = False,
    object_map_applied: bool = False,
    require_object_map: bool = False,
    object_type: Optional[str] = None,
    unresolved_references: bool = False,
) -> Tuple[str, str, bool, Optional[str], Optional[str], Optional[str]]:
    """Derive lakebridge_classification and map to control status fields.

    Returns:
      (
        lakebridge_classification,  # 'AUTO_CANDIDATE' | 'MANUAL_REVIEW' | 'UNSUPPORTED'
        conversion_status,          # 'CONVERTED' | 'PARTIAL' | 'FAILED' | 'UNSUPPORTED'
        manual_review_required,     # bool
        manual_review_reason,       # bounded evidence string or None
        error_code,                 # string or None
        error_message,              # string or None
      )
    """
    # 1. UNSUPPORTED
    unsupported_reasons: List[str] = []
    if source_definition is None or not str(source_definition).strip():
        unsupported_reasons.append("source definition is missing or blank")
    if parsing_error_count > 0:
        unsupported_reasons.append(f"parsing_error_count={parsing_error_count}")
    if generation_error_count > 0:
        unsupported_reasons.append(f"generation_error_count={generation_error_count}")
    if analyzer_failed:
        unsupported_reasons.append("Lakebridge Analyzer failed")
    if transpile_failed:
        unsupported_reasons.append("BladeBridge transpilation failed")
    if converted_definition is None or not str(converted_definition).strip():
        unsupported_reasons.append("no converted output produced or blank output")

    if unsupported_reasons:
        reason_str = "; ".join(unsupported_reasons)
        err_code = "DEFINITION_MISSING" if (source_definition is None or not str(source_definition).strip()) else "UNSUPPORTED_CONSTRUCT"
        return (
            LAKEBRIDGE_CLASSIFICATION_UNSUPPORTED,
            "FAILED" if analyzer_failed or transpile_failed or parsing_error_count > 0 or generation_error_count > 0 else "UNSUPPORTED",
            True,
            reason_str[:500],
            err_code,
            reason_str[:500],
        )

    # 2. MANUAL_REVIEW
    manual_reasons: List[str] = []
    norm_otype = (object_type or "").strip().upper()
    if norm_otype == "PROCEDURE":
        manual_reasons.append("object_type is PROCEDURE")

    comp_norm = (complexity or "").strip().upper()
    if comp_norm in ("HIGH", "VERY_HIGH"):
        manual_reasons.append(f"complexity is {comp_norm}")
    if unknown_statement_count is not None and unknown_statement_count > 0:
        manual_reasons.append(f"unknown_statement_count={unknown_statement_count}")
    if unknown_fragments:
        manual_reasons.append(f"{len(unknown_fragments)} unknown fragment(s) detected")
    if fixme_count > 0:
        manual_reasons.append(f"fixme_count={fixme_count}")
    if constructs.get("uses_error_handling"):
        manual_reasons.append("uses error handling constructs")
    if constructs.get("uses_rowcount"):
        manual_reasons.append("uses @@ROWCOUNT construct")
    if constructs.get("uses_cursor"):
        manual_reasons.append("uses cursor constructs")
    if constructs.get("uses_dynamic_sql"):
        manual_reasons.append("uses dynamic SQL constructs")
    if constructs.get("uses_trigger"):
        manual_reasons.append("uses trigger constructs")
    if validation_error_count > 0:
        manual_reasons.append(f"validation_error_count={validation_error_count}")
    if remaining_source_syntax:
        manual_reasons.append(f"retains source syntax: {', '.join(remaining_source_syntax)}")
    if complexity is None or statement_count is None or unknown_statement_count is None:
        manual_reasons.append("required Analyzer metadata is unavailable")
    if unresolved_references:
        manual_reasons.append("contains unresolved source object references")
    if not object_map_applied:
        manual_reasons.append("object_map_applied is false")

    if manual_reasons:
        reason_str = "; ".join(manual_reasons)
        return (
            LAKEBRIDGE_CLASSIFICATION_MANUAL_REVIEW,
            "PARTIAL",
            True,
            reason_str[:500],
            None,
            None,
        )

    # 3. AUTO_CANDIDATE (Only for VIEW when object_map_applied is true and all conditions pass)
    if norm_otype == "VIEW" and object_map_applied:
        return (
            LAKEBRIDGE_CLASSIFICATION_AUTO_CANDIDATE,
            "CONVERTED",
            False,
            None,
            None,
            None,
        )

    return (
        LAKEBRIDGE_CLASSIFICATION_MANUAL_REVIEW,
        "PARTIAL",
        True,
        "manual review required",
        None,
        None,
    )


def sanitize_cli_output(output: Any, max_len: int = 4000) -> str:
    """Sanitize CLI standard out/error and redact secrets."""
    if output is None:
        return ""
    s = sanitize_message(str(output))
    s = re.sub(r"(?i)password\s*=\s*[^;\s]+", "password=***", s)
    s = re.sub(r"(?i)token\s*=\s*[^;\s]+", "token=***", s)
    s = re.sub(r"(?i)secret\s*=\s*[^;\s]+", "secret=***", s)
    return s[:max_len]


def build_lakebridge_report_path(target_catalog: str, target_schema: str, run_id: str, source_system: str) -> str:
    """Build Unity Catalog Volume report path for Lakebridge."""
    safe_cat = sanitize_path_component(target_catalog, "target_catalog")
    safe_sch = sanitize_path_component(target_schema, "target_schema")
    safe_run = sanitize_filename(run_id)
    safe_sys = normalize_source_system(source_system)
    return f"/Volumes/{safe_cat}/{safe_sch}/_lakebridge_reports/{safe_run}/{safe_sys}/"


def build_lakebridge_error_path(target_catalog: str, target_schema: str, run_id: str, source_system: str) -> str:
    """Build Unity Catalog Volume error reports directory for Lakebridge."""
    base = build_lakebridge_report_path(target_catalog, target_schema, run_id, source_system)
    return f"{base}errors/"


def build_lakebridge_converted_path(target_catalog: str, target_schema: str, relative_path: str) -> str:
    """Build Unity Catalog Volume converted file path for Lakebridge."""
    return build_artifact_volume_path(
        target_catalog=target_catalog,
        target_schema=target_schema,
        volume_name="_converted_artifacts",
        relative_path=relative_path,
    )


def build_lakebridge_availability_cmd() -> List[str]:
    """Command to check Databricks Labs Lakebridge CLI availability."""
    return ["databricks", "labs", "lakebridge", "--help"]


def build_lakebridge_analyze_cmd(source_directory: str, report_file: str, source_tech: str) -> List[str]:
    """Command to run Databricks Labs Lakebridge Analyzer."""
    return [
        "databricks", "labs", "lakebridge", "analyze",
        "--source-directory", os.path.abspath(source_directory),
        "--report-file", os.path.abspath(report_file),
        "--source-tech", source_tech,
    ]


def build_lakebridge_transpile_cmd(source_dialect: str, input_source: str, output_folder: str, error_file_path: str) -> List[str]:
    """Command to run Databricks Labs Lakebridge Transpiler."""
    return [
        "databricks", "labs", "lakebridge", "transpile",
        "--source-dialect", source_dialect,
        "--input-source", os.path.abspath(input_source),
        "--output-folder", os.path.abspath(output_folder),
        "--error-file-path", os.path.abspath(error_file_path),
        "--skip-validation", "true",
    ]


def build_attempt_staging_dir(base_root: str, run_id: str, artifact_id: str, attempt_number: int, unique_id: Optional[str] = None) -> str:
    """Build unique local attempt staging directory."""
    u_id = unique_id or uuid.uuid4().hex[:12]
    return os.path.join(
        base_root,
        run_id,
        artifact_id,
        str(attempt_number),
        u_id,
    )


def build_collision_resistant_filename(object_name: str, artifact_id: str, ext: str = ".sql") -> str:
    """Build local filename resistant to collisions (e.g. A/B vs A:B)."""
    h = hashlib.sha256(artifact_id.encode("utf-8")).hexdigest()[:8]
    safe_name = re.sub(r"[^a-zA-Z0-9_#$]+", "_", object_name)
    if not ext.startswith("."):
        ext = f".{ext}"
    return f"{safe_name}_{h}{ext}"

