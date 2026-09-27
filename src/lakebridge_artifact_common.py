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
import email
import email.policy
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


def get_analyzer_platform(source_system: Any) -> str:
    """Map source system to Lakebridge Analyzer platform string.

    Authoritative live mapping:
      sqlserver / mssql -> "MS SQL Server"
      oracle -> "Oracle"
    Raises ValueError for unknown source system.
    """
    norm = normalize_source_system(source_system)
    if norm == "mssql":
        return "MS SQL Server"
    elif norm == "oracle":
        return "Oracle"
    raise ValueError(f"Unknown source system for Lakebridge Analyzer: {source_system!r}")


def get_bladebridge_tech(source_system: Any) -> Tuple[str, str]:
    """Map source system to low-level BladeBridge Python API (source_tech, target_tech).

    Authoritative live mapping:
      sqlserver / mssql -> ("MSSQL", "SQL")
      oracle -> ("ORACLE", "SQL")
    Raises ValueError for unknown source system.
    """
    norm = normalize_source_system(source_system)
    if norm == "mssql":
        return ("MSSQL", "SQL")
    elif norm == "oracle":
        return ("ORACLE", "SQL")
    raise ValueError(f"Unknown source system for BladeBridge tech mapping: {source_system!r}")


def extract_sql_from_bladebridge_mime(raw_text: str) -> str:
    """Extract clean SQL from BladeBridge MIME multipart/mixed response.

    Rules:
    1. Treats content as MIME only when it has a valid multipart Content-Type with a boundary.
    2. Header matching is case-insensitive.
    3. Uses Python's standard email parser with email.policy.default.
    4. Honors each part's declared charset.
    5. Prefers exactly one .sql attachment.
    6. Otherwise prefers exactly one supported plain SQL/text part.
    7. Rejects ambiguous multiple SQL attachments.
    8. Rejects malformed MIME.
    9. Rejects blank extracted output.
    10. Preserves ordinary SQL comments exactly (e.g. '--generated').
    11. Ensures Content-Type, Content-Disposition, Content-Transfer-Encoding, MIME-Version,
        and boundary markers never reach persisted SQL.
    """
    if not raw_text or not str(raw_text).strip():
        raise ValueError("Cannot extract SQL from empty or blank BladeBridge response")

    stripped = str(raw_text).strip()

    # Case-insensitive check for multipart Content-Type with boundary
    has_multipart_content_type = bool(
        re.search(r"(?im)^content-type:\s*multipart/[^;\r\n]+;\s*boundary=", stripped[:1000])
        or (re.search(r"(?im)^content-type:\s*multipart/", stripped[:500]) and "boundary=" in stripped[:1000])
    )

    if not has_multipart_content_type:
        if re.search(r"(?im)^content-type:\s*multipart/", stripped[:500]):
            raise ValueError("Malformed MIME: multipart Content-Type missing boundary")
        if re.match(r"(?im)^content-type:", stripped) and re.search(r"(?im)^mime-version:", stripped[:300]):
            raise ValueError("Malformed MIME headers in BladeBridge output")
        return stripped

    msg = email.message_from_string(stripped, policy=email.policy.default)

    boundary = msg.get_boundary()
    if not boundary:
        m_b = re.search(r'(?i)boundary=["\']?([^"\'\s;]+)["\']?', stripped[:1000])
        if m_b:
            boundary = m_b.group(1)
        else:
            raise ValueError("Malformed MIME: boundary parameter missing from multipart message")

    sql_attachments: List[str] = []
    text_parts: List[str] = []

    for part in msg.walk():
        if part.is_multipart():
            continue

        filename = (part.get_filename() or "").strip()
        content_type = part.get_content_type().lower()
        charset = part.get_content_charset() or "utf-8"

        payload = part.get_payload(decode=True)
        if payload is not None:
            try:
                text = payload.decode(charset, errors="replace").strip()
            except Exception:
                text = payload.decode("utf-8", errors="replace").strip()
        else:
            raw_payload = part.get_payload()
            text = str(raw_payload).strip() if raw_payload is not None else ""

        if not text:
            continue

        if filename.lower().endswith(".sql") or content_type in ("application/sql", "text/x-sql", "text/sql"):
            sql_attachments.append(text)
        elif content_type in ("text/plain", "text/html"):
            text_parts.append(text)
        else:
            text_parts.append(text)

    if len(sql_attachments) > 1:
        raise ValueError(f"Ambiguous MIME response: found {len(sql_attachments)} .sql attachments")
    elif len(sql_attachments) == 1:
        chosen = sql_attachments[0]
    elif len(text_parts) > 1:
        raise ValueError(f"Ambiguous MIME response: found {len(text_parts)} plain text/SQL parts without .sql attachment")
    elif len(text_parts) == 1:
        chosen = text_parts[0]
    else:
        raise ValueError("Malformed MIME: blank MIME payload or no usable SQL attachment found in multipart payload")

    if not chosen or not chosen.strip():
        raise ValueError("MIME extraction resulted in blank SQL payload")

    boundary_pattern = re.compile(r"^--" + re.escape(boundary) + r"(?:--)?\s*$", re.M)
    clean_sql = boundary_pattern.sub("", chosen).strip()

    if not clean_sql:
        raise ValueError("MIME extraction resulted in empty SQL after boundary removal")

    if re.search(r"(?im)^content-(?:type|disposition|transfer-encoding):", clean_sql):
        raise ValueError("MIME headers leaked into extracted SQL")
    if re.search(r"(?im)^mime-version:", clean_sql):
        raise ValueError("MIME-Version header leaked into extracted SQL")

    return clean_sql


def normalize_diagnostic_severity(severity: Any) -> str:
    """Normalize LSP / BladeBridge diagnostic severity to 'ERROR', 'WARNING', 'INFORMATION', or 'HINT'."""
    if severity is None:
        return "INFORMATION"
    if hasattr(severity, "name"):
        n = str(severity.name).strip().upper()
        if n in ("ERROR", "WARNING", "INFORMATION", "HINT"):
            return n
        if n == "INFO":
            return "INFORMATION"
    if isinstance(severity, int) or (isinstance(severity, str) and str(severity).strip().isdigit()):
        val = int(severity)
        if val == 1:
            return "ERROR"
        if val == 2:
            return "WARNING"
        if val == 3:
            return "INFORMATION"
        if val == 4:
            return "HINT"
    s = str(severity).strip().upper()
    if "ERR" in s or s == "1":
        return "ERROR"
    if "WARN" in s or s == "2":
        return "WARNING"
    if "HINT" in s or s == "4":
        return "HINT"
    if "INFO" in s or s == "3":
        return "INFORMATION"
    return "INFORMATION"


def _position_to_offset(
    line_starts: List[int],
    lines: List[str],
    line: int,
    character: int,
    source_length: int,
) -> int:
    """Convert LSP line and UTF-16 character position to Python string character offset.

    Calculates LSP UTF-16 character positions using line content without \\r or \\n,
    while preserving original source offsets. Ensures a position never inserts text between \\r and \\n.
    """
    if line < 0 or character < 0:
        raise ValueError(
            f"Invalid position: line={line}, character={character} cannot be negative"
        )

    # BladeBridge full-document TextEdits can represent the exact EOF as
    # line == total lines and character == 0.
    if line == len(line_starts) and character == 0:
        return source_length

    # Reject every other out-of-range position.
    if line < 0 or line >= len(line_starts):
        raise ValueError(
            f"Line out of bounds: line {line} >= total lines {len(line_starts)}"
        )

    l_start = line_starts[line]
    line_str = lines[line]

    # Calculate line content without \r or \n line terminators
    if line_str.endswith("\r\n"):
        line_content = line_str[:-2]
    elif line_str.endswith(("\r", "\n")):
        line_content = line_str[:-1]
    else:
        line_content = line_str

    if character == 0:
        return l_start

    utf16_count = 0
    char_idx = 0
    while char_idx < len(line_content) and utf16_count < character:
        ch = line_content[char_idx]
        code_units = 2 if ord(ch) > 0xFFFF else 1
        if utf16_count + code_units > character:
            raise ValueError(
                f"Position character {character} points into the middle of a UTF-16 surrogate pair on line {line}"
            )
        utf16_count += code_units
        char_idx += 1

    if utf16_count < character:
        raise ValueError(
            f"Character offset out of bounds: character {character} > line content length in UTF-16 {utf16_count} on line {line}"
        )

    return l_start + char_idx


def apply_text_edits(source_text: str, edits: List[Any]) -> str:
    """Apply LSP / BladeBridge TextEdits to original source SQL text.

    Supports insertion, replacement, deletion, multiline edits, adjacent edits,
    and full-document replacement. Validates ranges, rejects overlapping edits,
    and applies in descending source-offset order preserving all untouched text.
    """
    if source_text is None:
        raise ValueError("source_text cannot be None")
    if not edits:
        return source_text

    lines = source_text.splitlines(keepends=True)
    line_starts = [0]
    for line_s in lines[:-1]:
        line_starts.append(line_starts[-1] + len(line_s))

    if source_text.endswith(("\n", "\r")):
        line_starts.append(len(source_text))
        lines.append("")
    elif not lines:
        lines = [""]

    parsed_edits: List[Dict[str, Any]] = []
    for edit in edits:
        if isinstance(edit, dict):
            new_text = edit.get("new_text")
            if new_text is None:
                new_text = edit.get("newText")
            if new_text is None:
                new_text = edit.get("text")
            if new_text is None:
                raise ValueError(f"TextEdit missing new_text: {edit}")

            rng = edit.get("range")
            if rng is None and "start" in edit and "end" in edit:
                rng = {"start": edit["start"], "end": edit["end"]}
        else:
            new_text = getattr(edit, "new_text", None)
            if new_text is None:
                new_text = getattr(edit, "newText", None)
            if new_text is None:
                new_text = getattr(edit, "text", None)
            if new_text is None:
                raise ValueError(f"TextEdit missing new_text: {edit}")

            rng = getattr(edit, "range", None)
            if rng is None and hasattr(edit, "start") and hasattr(edit, "end"):
                rng = {"start": edit.start, "end": edit.end}

        if rng is None:
            if len(edits) == 1:
                s_off = 0
                e_off = len(source_text)
            else:
                raise ValueError("Multiple TextEdits provided but edit has no range")
        else:
            if isinstance(rng, dict):
                start_obj = rng.get("start")
                end_obj = rng.get("end")
            else:
                start_obj = getattr(rng, "start", None)
                end_obj = getattr(rng, "end", None)

            if start_obj is None or end_obj is None:
                raise ValueError(f"Malformed TextEdit range: {rng}")

            def _get_pos(p: Any) -> Tuple[int, int]:
                if isinstance(p, dict):
                    return int(p["line"]), int(p["character"])
                return int(getattr(p, "line")), int(getattr(p, "character"))

            s_line, s_char = _get_pos(start_obj)
            e_line, e_char = _get_pos(end_obj)

            s_off = _position_to_offset(
                line_starts,
                lines,
                s_line,
                s_char,
                len(source_text),
            )
            e_off = _position_to_offset(
                line_starts,
                lines,
                e_line,
                e_char,
                len(source_text),
            )

        if s_off > e_off:
            raise ValueError(f"Invalid TextEdit range: start offset {s_off} is after end offset {e_off}")

        parsed_edits.append({
            "start": s_off,
            "end": e_off,
            "new_text": str(new_text),
        })

    parsed_edits.sort(key=lambda x: (x["start"], x["end"]))
    for i in range(len(parsed_edits) - 1):
        curr_e = parsed_edits[i]
        next_e = parsed_edits[i + 1]
        if next_e["start"] < curr_e["end"]:
            raise ValueError(
                f"Overlapping TextEdit ranges: edit at [{curr_e['start']}:{curr_e['end']}] "
                f"overlaps with edit at [{next_e['start']}:{next_e['end']}]"
            )

    parsed_edits.sort(key=lambda x: (x["start"], x["end"]), reverse=True)
    result = source_text
    for e in parsed_edits:
        s = e["start"]
        end = e["end"]
        result = result[:s] + e["new_text"] + result[end:]

    return result


class UnknownFragment(dict):
    """Structured unknown SQL fragment supporting dict access, substring checks, and JSON serialization."""

    def __init__(self, fragment: str, count: int = 1):
        super().__init__(fragment=fragment, count=count)
        self.fragment = fragment
        self.count = count

    def __contains__(self, item: Any) -> bool:
        return super().__contains__(item) or (isinstance(item, str) and item in self.fragment)

    def __str__(self) -> str:
        return self.fragment

    def __repr__(self) -> str:
        return f"UnknownFragment(fragment={self.fragment!r}, count={self.count})"


def _artifact_matches(
    val: str,
    object_name: Optional[str] = None,
    staged_filename: Optional[str] = None,
    artifact_id: Optional[str] = None,
) -> bool:
    """Check if value matches the staged filename, artifact ID, or object name."""
    if not val or not str(val).strip():
        return False
    v = str(val).strip().lower()
    v_base = os.path.splitext(os.path.basename(v))[0]

    if staged_filename:
        sf = os.path.basename(staged_filename).strip().lower()
        sf_base = os.path.splitext(sf)[0]
        if v == sf or v == sf_base or v_base == sf_base or os.path.basename(v) == sf:
            return True

    if artifact_id:
        aid = artifact_id.strip().lower()
        if aid in v or aid in v_base:
            return True

    if object_name:
        on = object_name.strip().lower()
        on_clean = re.sub(r"[^a-zA-Z0-9_]+", "_", on).strip("_")
        if v == on or v_base == on or v_base == on_clean or on_clean in v_base or v_base.endswith(on_clean):
            return True

    return False


def _find_artifact_row(
    rows: List[Dict[str, Any]],
    object_name: Optional[str] = None,
    staged_filename: Optional[str] = None,
    artifact_id: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """Match artifact row deterministically by staged filename, artifact ID, or object name."""
    if not rows:
        return None

    for r in rows:
        for key in ("name", "source_file", "sourcefile", "program_name", "program", "caller", "source_object", "artifact_id", "object_name"):
            val = r.get(key)
            if val and _artifact_matches(str(val), object_name, staged_filename, artifact_id):
                return r

    return None


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

        # Parse relationships from xl/_rels/workbook.xml.rels
        rel_map: Dict[str, str] = {}
        if "xl/_rels/workbook.xml.rels" in zf.namelist():
            try:
                rels_root = ET.fromstring(zf.read("xl/_rels/workbook.xml.rels"))
                for rel in rels_root.findall(".//{*}Relationship"):
                    r_id = rel.attrib.get("Id")
                    target = rel.attrib.get("Target")
                    if r_id and target:
                        if not target.startswith("xl/"):
                            target = "xl/" + target.lstrip("/")
                        rel_map[r_id] = target
            except Exception:
                pass

        # Map sheet names to sheet XML files using relationships
        sheet_map: Dict[str, str] = {}
        if "xl/workbook.xml" in zf.namelist():
            try:
                wb_root = ET.fromstring(zf.read("xl/workbook.xml"))
                for idx, sheet_el in enumerate(wb_root.findall(".//{*}sheet"), 1):
                    s_name = sheet_el.attrib.get("name") or f"sheet{idx}"
                    r_id = None
                    for attr_name, attr_val in sheet_el.attrib.items():
                        if attr_name.lower().endswith("id") and attr_name.lower() != "sheetid":
                            r_id = attr_val
                            break
                    target_file = rel_map.get(r_id) if r_id else None
                    if not target_file or target_file not in zf.namelist():
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


def extract_complexity(
    workbook_data: Dict[str, List[Dict[str, Any]]],
    object_name: Optional[str] = None,
    staged_filename: Optional[str] = None,
    artifact_id: Optional[str] = None,
) -> Optional[str]:
    """Extract complexity ('LOW', 'MEDIUM', 'HIGH', 'VERY_HIGH', or None) from Analyzer report.

    CRITICAL: Never reads aggregate categories from 'Summary' sheet.
    Prefers structured JSON inventory, then XLSX 'SQL Programs' sheet.
    Fails closed if JSON and XLSX provide contradictory values.
    """
    json_comp: Optional[str] = None
    xlsx_comp: Optional[str] = None

    # 1. JSON inventory
    if "inventory" in workbook_data:
        row = _find_artifact_row(workbook_data["inventory"], object_name, staged_filename, artifact_id)
        if row:
            for key in ("complexityLevel", "complexity_level", "complexity"):
                comp = row.get(key)
                if comp and str(comp).strip().upper() in VALID_COMPLEXITIES:
                    json_comp = str(comp).strip().upper()
                    break

    # 2. XLSX SQL Programs
    sql_prog_key = "xlsx_sql_programs" if "xlsx_sql_programs" in workbook_data else "sql_programs"
    if sql_prog_key in workbook_data:
        row = _find_artifact_row(workbook_data[sql_prog_key], object_name, staged_filename, artifact_id)
        if row:
            comp = row.get("complexity")
            if comp and str(comp).strip().upper() in VALID_COMPLEXITIES:
                xlsx_comp = str(comp).strip().upper()

    # Fail closed on contradictory classification-critical values
    if json_comp and xlsx_comp and json_comp != xlsx_comp:
        raise ValueError(
            f"Contradictory Analyzer complexity: JSON reports {json_comp} but XLSX reports {xlsx_comp}"
        )

    if json_comp:
        return json_comp
    if xlsx_comp:
        return xlsx_comp

    # 3. Legacy/mock sheets (excluding summary/overview)
    for s_name, rows in workbook_data.items():
        if s_name.startswith("xlsx_"):
            s_name = s_name[5:]
        if s_name in ("summary", "overview", "runinfo", "run_info", "_warnings", "_contradictions"):
            continue  # NEVER search Summary sheet!
        if any(k in s_name for k in ("complex", "assessment", "object", "program")):
            row = _find_artifact_row(rows, object_name, staged_filename, artifact_id)
            if row:
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
    staged_filename: Optional[str] = None,
    artifact_id: Optional[str] = None,
) -> Tuple[Optional[int], Optional[int]]:
    """Extract (statement_count, unknown_statement_count) from Analyzer report."""
    stmt_count: Optional[int] = None
    unknown_count: Optional[int] = None

    # 1. JSON inventory
    if "inventory" in workbook_data:
        row = _find_artifact_row(workbook_data["inventory"], object_name, staged_filename, artifact_id)
        if row and "statementCount" in row and row["statementCount"] is not None:
            try:
                stmt_count = int(row["statementCount"])
            except Exception:
                pass
        if row and "scriptCategories" in row and isinstance(row["scriptCategories"], list):
            if "UNKNOWN" not in row["scriptCategories"] and unknown_count is None:
                unknown_count = 0

    # 2. XLSX SQL Programs
    if stmt_count is None and "sql_programs" in workbook_data:
        row = _find_artifact_row(workbook_data["sql_programs"], object_name, staged_filename, artifact_id)
        if row and row.get("statement_count") is not None:
            try:
                stmt_count = int(row["statement_count"])
            except Exception:
                pass

    # 3. XLSX UNKNOWN SQL Category
    if "unknown_sql_category" in workbook_data:
        tot_unk = 0
        has_rows = False
        for r in workbook_data["unknown_sql_category"]:
            has_rows = True
            cnt_val = r.get("of_occurrences") or r.get("occurrences") or r.get("count") or 1
            try:
                tot_unk += int(cnt_val)
            except Exception:
                tot_unk += 1
        if has_rows:
            unknown_count = tot_unk

    # 4. Legacy/mock sheets fallback
    if stmt_count is None or unknown_count is None:
        for s_name, rows in workbook_data.items():
            if s_name in ("summary", "overview", "runinfo"):
                continue
            if any(k in s_name for k in ("statement", "complexity", "object", "program")):
                row = _find_artifact_row(rows, object_name, staged_filename, artifact_id)
                if row:
                    for col_key, val in row.items():
                        if ("unknown" in col_key or "unsupported" in col_key) and val is not None and unknown_count is None:
                            try:
                                unknown_count = int(val)
                            except Exception:
                                pass
                        elif ("statement" in col_key or "stmt" in col_key) and "unknown" not in col_key and val is not None and stmt_count is None:
                            try:
                                stmt_count = int(val)
                            except Exception:
                                pass

    return stmt_count, unknown_count


def extract_unknown_fragments(
    workbook_data: Dict[str, List[Dict[str, Any]]],
    object_name: Optional[str] = None,
    staged_filename: Optional[str] = None,
    artifact_id: Optional[str] = None,
) -> List[Any]:
    """Extract unknown fragments reported by Analyzer."""
    fragments: List[Any] = []

    # 1. XLSX UNKNOWN SQL Category
    if "unknown_sql_category" in workbook_data:
        for r in workbook_data["unknown_sql_category"]:
            txt = str(r.get("sql_unknown_category_scripts") or "").strip()
            cnt_val = r.get("of_occurrences") or r.get("occurrences") or r.get("count") or 1
            try:
                cnt = int(cnt_val)
            except Exception:
                cnt = 1
            if txt:
                fragments.append(UnknownFragment(txt, cnt))

    if fragments:
        return fragments

    # 2. Legacy/mock sheets fallback
    for s_name, rows in workbook_data.items():
        if s_name in ("summary", "overview", "runinfo"):
            continue
        if any(k in s_name for k in ("fragment", "unknown", "error", "unsupported")):
            for row in rows:
                if object_name or staged_filename or artifact_id:
                    matched = _find_artifact_row([row], object_name, staged_filename, artifact_id)
                    if not matched:
                        continue
                for col_key, val in row.items():
                    if any(t in col_key for t in ("fragment", "syntax", "unknown", "text", "snippet")) and val:
                        txt = str(val).strip()
                        if txt and not any(f.fragment == txt if isinstance(f, UnknownFragment) else f == txt for f in fragments):
                            fragments.append(UnknownFragment(txt, 1))

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


def mask_sql_literals_and_comments(sql_text: str) -> str:
    """Mask string literals and comments so static regex heuristics do not extract dynamic SQL, commented tables, or source syntax."""
    if not sql_text:
        return ""
    pattern = re.compile(r"'(?:''|[^'])*'|/\*[\s\S]*?\*/|--[^\r\n]*")
    return pattern.sub(" ", sql_text)


def detect_remaining_source_syntax(sql_text: str, source_system: str) -> Tuple[bool, List[str]]:
    """Detect if converted output retains untranspiled source-specific dialect constructs."""
    if not sql_text:
        return False, []
    masked_sql = mask_sql_literals_and_comments(sql_text)
    if not masked_sql.strip():
        return False, []

    sys_norm = normalize_source_system(source_system)
    retained = []

    if sys_norm == "mssql":
        patterns = [
            (r"\[[^\]\r\n]+\]", "T-SQL bracketed identifier syntax"),
            (r"\bISNULL\s*\(", "T-SQL ISNULL function"),
            (r"\bGETDATE\s*\(\)", "T-SQL GETDATE()"),
            (r"\bTOP\s+\(?\d+\)?", "T-SQL TOP clause"),
            (r"\bIDENTITY\s*\(", "T-SQL IDENTITY"),
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
        if re.search(pat, masked_sql, re.I):
            retained.append(desc)

    return bool(retained), retained


def extract_referenced_objects(
    workbook_data: Dict[str, List[Dict[str, Any]]],
    sql_text: Optional[str] = None,
    object_name: Optional[str] = None,
    staged_filename: Optional[str] = None,
    artifact_id: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Extract referenced objects from Analyzer JSON objectRel, XLSX, or SQL text."""
    refs: List[Dict[str, Any]] = []

    # 1. JSON inventory objectRel
    if "inventory" in workbook_data:
        row = _find_artifact_row(workbook_data["inventory"], object_name, staged_filename, artifact_id)
        if row and "objectRel" in row and isinstance(row["objectRel"], list):
            for rel in row["objectRel"]:
                obj = rel.get("object")
                act = str(rel.get("action") or "READ").upper()
                cnt = rel.get("count", 1)
                try:
                    cnt = int(cnt)
                except Exception:
                    cnt = 1
                if obj:
                    refs.append({"object": obj, "operation": act, "count": cnt})
        if refs:
            return refs

    # 2. XLSX RAW_PROGRAM_OBJECT_XREF
    if "raw_program_object_xref" in workbook_data:
        for r in workbook_data["raw_program_object_xref"]:
            if object_name or staged_filename or artifact_id:
                prog = str(r.get("program") or "").strip()
                if prog and not _find_artifact_row([{"program": prog}], object_name, staged_filename, artifact_id):
                    continue
            obj = str(r.get("object") or "").strip()
            op = str(r.get("operation") or "READ").upper()
            cnt = r.get("count") or 1
            try:
                cnt = int(cnt)
            except Exception:
                cnt = 1
            if obj:
                refs.append({"object": obj, "operation": op, "count": cnt})
        if refs:
            return refs

    # 3. XLSX Referenced Objects
    if "referenced_objects" in workbook_data:
        for r in workbook_data["referenced_objects"]:
            if object_name or staged_filename or artifact_id:
                caller = str(r.get("source_object") or r.get("caller") or r.get("object_name") or "").strip()
                if caller and not _find_artifact_row([{"caller": caller}], object_name, staged_filename, artifact_id):
                    continue
            obj = str(r.get("object") or r.get("referenced_object") or r.get("target") or "").strip()
            if not obj and r.get("referenced_table"):
                ref_sch = str(r.get("referenced_schema") or "").strip()
                ref_tbl = str(r.get("referenced_table") or "").strip()
                obj = f"{ref_sch}.{ref_tbl}" if ref_sch else ref_tbl
            if not obj:
                continue

            # Check individual operation columns (CREATE, READ, WRITE, DROP, TRUNCATE)
            found_op = False
            for op_col in ("read", "write", "create", "drop", "truncate"):
                if op_col in r and r[op_col] is not None:
                    try:
                        c_val = int(r[op_col])
                        if c_val > 0:
                            refs.append({"object": obj, "operation": op_col.upper(), "count": c_val})
                            found_op = True
                    except Exception:
                        pass
            if not found_op:
                op = str(r.get("operation") or r.get("action") or "READ").upper()
                cnt = r.get("count") or 1
                try:
                    cnt = int(cnt)
                except Exception:
                    cnt = 1
                refs.append({"object": obj, "operation": op, "count": cnt})
        if refs:
            return refs

    # 4. SQL text heuristic fallback
    if sql_text:
        found: Dict[Tuple[str, str], int] = {}
        masked_sql = mask_sql_literals_and_comments(sql_text)
        for m in re.finditer(r"\b(?:FROM|JOIN)\s+([a-zA-Z0-9_\.\`\"\[\]]+)", masked_sql, re.I):
            target = m.group(1).strip()
            found[(target, "READ")] = found.get((target, "READ"), 0) + 1
        for m in re.finditer(r"\b(?:INSERT\s+INTO|UPDATE|MERGE\s+INTO)\s+([a-zA-Z0-9_\.\`\"\[\]]+)", masked_sql, re.I):
            target = m.group(1).strip()
            found[(target, "WRITE")] = found.get((target, "WRITE"), 0) + 1

        for (target, op), cnt in found.items():
            refs.append({"object": target, "operation": op, "count": cnt})

    return refs


def read_analyzer_report(
    report_xlsx_path: Optional[str] = None,
    report_json_path: Optional[str] = None,
) -> Dict[str, List[Dict[str, Any]]]:
    """Read Lakebridge Analyzer report preferring structured JSON inventory, with XLSX fallback/validation."""
    merged: Dict[str, List[Dict[str, Any]]] = {}
    json_err: Optional[str] = None

    if report_json_path and os.path.isfile(report_json_path):
        try:
            json_wb = read_analyzer_workbook(report_json_path)
            merged.update(json_wb)
        except Exception as e:
            json_err = sanitize_message(e)

    if report_xlsx_path and os.path.isfile(report_xlsx_path):
        try:
            xlsx_wb = read_analyzer_workbook(report_xlsx_path)
            for k, v in xlsx_wb.items():
                if k not in merged:
                    merged[k] = v
                else:
                    merged[f"xlsx_{k}"] = v
        except Exception as e:
            if not merged and json_err:
                raise ValueError(
                    f"Both Analyzer JSON ({json_err}) and XLSX ({sanitize_message(e)}) failed to parse"
                ) from e
            if not merged:
                raise

    if json_err:
        if not merged:
            raise ValueError(f"Analyzer JSON report is corrupt: {json_err}")
        if "_warnings" not in merged:
            merged["_warnings"] = []
        merged["_warnings"].append(f"Preferred Analyzer JSON was corrupt: {json_err}. Fell back to XLSX.")

    if not merged:
        if report_xlsx_path and not os.path.exists(report_xlsx_path):
            raise FileNotFoundError(f"Analyzer report file not found: {report_xlsx_path}")
        if report_json_path and not os.path.exists(report_json_path):
            raise FileNotFoundError(f"Analyzer report file not found: {report_json_path}")
        raise ValueError("No valid Analyzer report file provided")

    return merged


def has_usable_analyzer_results(workbook_data: Optional[Dict[str, Any]]) -> bool:
    """Check if Analyzer workbook data contains usable structured inventory results.

    Returns True if and only if valid, readable, non-empty object inventory or program
    records exist in the parsed report data. Returns False for empty, corrupt, or summary-only data.
    """
    if not workbook_data or not isinstance(workbook_data, dict):
        return False

    # 1. Direct inventory list (standard Lakebridge Analyzer / Bladespector JSON report)
    inv = workbook_data.get("inventory")
    if isinstance(inv, list) and len(inv) > 0:
        if any(isinstance(r, dict) and any(v is not None and str(v).strip() != "" for v in r.values()) for r in inv):
            return True

    # 2. SQL programs sheet (standard Lakebridge Analyzer XLSX report)
    sql_prog = workbook_data.get("sql_programs") or workbook_data.get("xlsx_sql_programs")
    if isinstance(sql_prog, list) and len(sql_prog) > 0:
        if any(isinstance(r, dict) and any(v is not None and str(v).strip() != "" for v in r.values()) for r in sql_prog):
            return True

    # 3. Cross reference or referenced objects sheets
    for rel_key in (
        "raw_program_object_xref", "xlsx_raw_program_object_xref",
        "referenced_objects", "xlsx_referenced_objects",
    ):
        rels = workbook_data.get(rel_key)
        if isinstance(rels, list) and len(rels) > 0:
            if any(isinstance(r, dict) and any(v is not None and str(v).strip() != "" for v in r.values()) for r in rels):
                return True

    # 4. Other substantive object sheets (excluding summary/overview/warnings/metadata)
    ignored_sheets = {
        "summary", "overview", "runinfo", "run_info", "_warnings", "_contradictions",
        "xlsx_summary", "xlsx_overview", "xlsx_runinfo", "xlsx_run_info",
    }
    for sheet_name, rows in workbook_data.items():
        if sheet_name in ignored_sheets or sheet_name.startswith("_"):
            continue
        if isinstance(rows, list) and len(rows) > 0:
            if any(isinstance(r, dict) and any(v is not None and str(v).strip() != "" for v in r.values()) for r in rows):
                return True

    return False


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
    diagnostic_warning_count: int = 0,
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
    if diagnostic_warning_count > 0:
        manual_reasons.append(f"BladeBridge diagnostic warnings ({diagnostic_warning_count})")
    if remaining_source_syntax:
        manual_reasons.append(f"retains source syntax: {', '.join(remaining_source_syntax)}")
    if complexity is None or statement_count is None or unknown_statement_count is None:
        manual_reasons.append("required Analyzer metadata is unavailable")
    if unresolved_references:
        manual_reasons.append("contains unresolved source object references")
    if not object_map_applied:
        manual_reasons.append("object_map_applied is false")

    is_partial_conversion = bool(
        fixme_count > 0
        or remaining_source_syntax
        or (unknown_statement_count is not None and unknown_statement_count > 0)
        or unknown_fragments
        or validation_error_count > 0
        or diagnostic_warning_count > 0
    )

    if manual_reasons:
        reason_str = "; ".join(manual_reasons)
        if is_partial_conversion:
            conv_status = "PARTIAL"
        elif norm_otype == "PROCEDURE":
            conv_status = "CONVERTED"
        elif norm_otype == "VIEW":
            conv_status = "CONVERTED" if object_map_applied else "PARTIAL"
        else:
            conv_status = "PARTIAL"
        return (
            LAKEBRIDGE_CLASSIFICATION_MANUAL_REVIEW,
            conv_status,
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

