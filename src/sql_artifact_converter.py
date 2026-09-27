"""
sql_artifact_converter.py - Transpiler, construct analyzer, classifier, and validator
for non-table SQL artifacts (Views, Procedures, Functions, Packages, Triggers).

Classifies each artifact as:
  - AUTO: Deterministic translation, passes validation, safe for automated deployment.
  - MANUAL_REVIEW: Partial translation or requires manual review; never auto-deployed.
  - UNSUPPORTED: Cannot be converted/deployed in Unity Catalog; never auto-deployed.

Pure Python: NO Spark, NO dbutils, NO direct DB connections. Fully unit-testable.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

try:
    from src.identifiers import (
        normalize_target_identifier,
        validate_identifier,
        quote_databricks,
        databricks_fqn,
    )
    from src.sql_artifact_control_common import (
        CLASSIFICATION_AUTO,
        CLASSIFICATION_MANUAL_REVIEW,
        CLASSIFICATION_UNSUPPORTED,
        CONVERSION_STATUS_CONVERTED,
        CONVERSION_STATUS_PARTIAL,
        CONVERSION_STATUS_UNSUPPORTED,
        CONVERSION_STATUS_FAILED,
        normalize_artifact_type,
        sanitize_error,
    )
except ModuleNotFoundError:
    from identifiers import (
        normalize_target_identifier,
        validate_identifier,
        quote_databricks,
        databricks_fqn,
    )
    from sql_artifact_control_common import (
        CLASSIFICATION_AUTO,
        CLASSIFICATION_MANUAL_REVIEW,
        CLASSIFICATION_UNSUPPORTED,
        CONVERSION_STATUS_CONVERTED,
        CONVERSION_STATUS_PARTIAL,
        CONVERSION_STATUS_UNSUPPORTED,
        CONVERSION_STATUS_FAILED,
        normalize_artifact_type,
        sanitize_error,
    )


# --- Known Unsupported / Risky Constructs Patterns ---
UNSUPPORTED_PATTERNS = [
    ("LINKED_SERVER", r"(?i)\b(?:OPENQUERY|OPENDATASOURCE|OPENROWSET)\b", "Linked server / remote data provider query"),
    ("EXPLICIT_TRANSACTION", r"(?i)\b(?:BEGIN|COMMIT|ROLLBACK)\s+(?:TRAN|TRANSACTION)\b", "Explicit transaction control statements"),
    ("TRY_CATCH", r"(?i)\bBEGIN\s+(?:TRY|CATCH)\b", "Structured TRY/CATCH error blocks"),
    ("RAISERROR_THROW", r"(?i)\b(?:RAISERROR|THROW)\b", "Custom error signaling (RAISERROR/THROW)"),
    ("CURSOR", r"(?i)\b(?:DECLARE\s+\w+\s+CURSOR|CURSOR\s+FOR|FETCH\s+NEXT\s+FROM|DEALLOCATE\s+\w+)\b", "Procedural cursor declaration and manipulation"),
    ("EXTENDED_PROCEDURE", r"(?i)\b(?:xp_\w+|sp_send_dbmail|sp_OACreate|sp_OAMethod)\b", "Extended stored procedures and system COM automation"),
    ("DYNAMIC_SQL", r"(?i)\b(?:EXEC\s*\(|EXECUTE\s*\(|sp_executesql\b|DBMS_SQL\b|EXECUTE\s+IMMEDIATE\b)", "Dynamic SQL execution"),
    ("SESSION_CONTEXT", r"(?i)\b(?:SET\s+CONTEXT_INFO|CONTEXT_INFO\s*\(\s*\)|SESSION_CONTEXT\s*\(\s*)", "Session-level context or state manipulation"),
    ("PROPRIETARY_LOCK_HINTS", r"(?i)\bWITH\s*\([^)]*\b(?:NOLOCK|HOLDLOCK|UPDLOCK|TABLOCK|XLOCK)\b[^)]*\)", "Proprietary table-lock hints"),
]

# Patterns that suggest manual review is needed
MANUAL_REVIEW_PATTERNS = [
    ("TEMP_TABLES", r"(?i)\b#[a-zA-Z0-9_]+\b", "Temporary table references (#table)"),
    ("GOTO_LABEL", r"(?i)\b(?:GOTO\s+\w+|\w+:\s*(?:BEGIN|SELECT|INSERT|UPDATE|DELETE|SET))\b", "GOTO statement / jump labels"),
    ("WHILE_LOOP", r"(?i)\bWHILE\b", "Procedural WHILE loop"),
    ("AUTONOMOUS_TRANSACTION", r"(?i)PRAGMA\s+AUTONOMOUS_TRANSACTION", "Oracle PRAGMA AUTONOMOUS_TRANSACTION"),
]

# Common Dialect Transpilation Rules
# (pattern, replacement, description)
SQLSERVER_FUNCTION_RULES = [
    (r"(?i)\bISNULL\s*\(", "COALESCE(", "ISNULL to COALESCE"),
    (r"(?i)\bGETDATE\s*\(\s*\)", "CURRENT_TIMESTAMP()", "GETDATE() to CURRENT_TIMESTAMP()"),
    (r"(?i)\bGETUTCDATE\s*\(\s*\)", "CURRENT_TIMESTAMP()", "GETUTCDATE() to CURRENT_TIMESTAMP()"),
    (r"(?i)\bLEN\s*\(", "LENGTH(", "LEN to LENGTH"),
    (r"(?i)\bSQUARE\s*\(", "POW(", "SQUARE to POW"),
]

ORACLE_FUNCTION_RULES = [
    (r"(?i)\bNVL\s*\(", "COALESCE(", "NVL to COALESCE"),
    (r"(?i)\bSYSDATE\b", "CURRENT_TIMESTAMP()", "SYSDATE to CURRENT_TIMESTAMP()"),
    (r"(?i)\bSYSTIMESTAMP\b", "CURRENT_TIMESTAMP()", "SYSTIMESTAMP to CURRENT_TIMESTAMP()"),
]


@dataclass
class ArtifactConversionResult:
    classification: str  # AUTO, MANUAL_REVIEW, UNSUPPORTED
    conversion_status: str  # CONVERTED, PARTIAL, UNSUPPORTED, FAILED
    converted_definition: Optional[str]
    manual_review_required: bool
    manual_review_reason: Optional[str] = None
    unsupported_features: Optional[str] = None
    target_object_name: Optional[str] = None
    error_code: Optional[str] = None
    error_message: Optional[str] = None
    validation_passed: bool = False


def tokenize_sql_spans(sql: str) -> List[Tuple[str, str]]:
    """Tokenize SQL into classified spans: STRING, LINE_COMMENT, BLOCK_COMMENT, QUOTED_IDENT, or CODE.

    Preserves exact original text so that ''.join(span[1] for span in spans) == sql.
    - STRING: '...' with '' escape sequences.
    - LINE_COMMENT: -- ... up to newline or EOF.
    - BLOCK_COMMENT: /* ... */ up to closing */ or EOF.
    - QUOTED_IDENT: "...", [...], `...`
    - CODE: all other spans.
    """
    spans: List[Tuple[str, str]] = []
    n = len(sql)
    i = 0
    code_start = 0

    while i < n:
        # 1. Line comment: --
        if sql[i:i+2] == "--":
            if i > code_start:
                spans.append(("CODE", sql[code_start:i]))
            end_line = sql.find("\n", i + 2)
            if end_line == -1:
                spans.append(("LINE_COMMENT", sql[i:]))
                i = n
            else:
                spans.append(("LINE_COMMENT", sql[i:end_line + 1]))
                i = end_line + 1
            code_start = i
            continue

        # 2. Block comment: /*
        if sql[i:i+2] == "/*":
            if i > code_start:
                spans.append(("CODE", sql[code_start:i]))
            end_block = sql.find("*/", i + 2)
            if end_block == -1:
                spans.append(("BLOCK_COMMENT", sql[i:]))
                i = n
            else:
                spans.append(("BLOCK_COMMENT", sql[i:end_block + 2]))
                i = end_block + 2
            code_start = i
            continue

        # 3. String literal: '...' with '' escapes
        if sql[i] == "'":
            if i > code_start:
                spans.append(("CODE", sql[code_start:i]))
            j = i + 1
            while j < n:
                if sql[j] == "'":
                    if j + 1 < n and sql[j + 1] == "'":
                        j += 2
                    else:
                        j += 1
                        break
                else:
                    j += 1
            spans.append(("STRING", sql[i:j]))
            i = j
            code_start = i
            continue

        # 4. Quoted identifiers: "...", [...], `...`
        if sql[i] == '"':
            if i > code_start:
                spans.append(("CODE", sql[code_start:i]))
            j = i + 1
            while j < n:
                if sql[j] == '"':
                    if j + 1 < n and sql[j + 1] == '"':
                        j += 2
                    else:
                        j += 1
                        break
                else:
                    j += 1
            spans.append(("QUOTED_IDENT", sql[i:j]))
            i = j
            code_start = i
            continue

        if sql[i] == '`':
            if i > code_start:
                spans.append(("CODE", sql[code_start:i]))
            end_bt = sql.find('`', i + 1)
            if end_bt == -1:
                spans.append(("QUOTED_IDENT", sql[i:]))
                i = n
            else:
                spans.append(("QUOTED_IDENT", sql[i:end_bt + 1]))
                i = end_bt + 1
            code_start = i
            continue

        if sql[i] == '[':
            if i > code_start:
                spans.append(("CODE", sql[code_start:i]))
            end_br = sql.find(']', i + 1)
            if end_br == -1:
                spans.append(("QUOTED_IDENT", sql[i:]))
                i = n
            else:
                spans.append(("QUOTED_IDENT", sql[i:end_br + 1]))
                i = end_br + 1
            code_start = i
            continue

        i += 1

    if code_start < n:
        spans.append(("CODE", sql[code_start:n]))

    return spans


def wrap_oracle_view_definition_if_needed(
    source_system: str,
    object_type: str,
    source_schema: str,
    object_name: str,
    definition: str,
) -> str:
    """If source_system=oracle, object_type=VIEW, and definition does not start with CREATE,

    wrap it as: CREATE OR REPLACE VIEW "<schema>"."<name>" AS <text> before conversion only.
    """
    if str(source_system or "").strip().lower() == "oracle":
        norm_type = str(object_type or "").strip().upper().replace(" ", "_")
        if norm_type == "VIEW":
            raw = str(definition or "")
            spans = tokenize_sql_spans(raw)
            first_code = ""
            for kind, txt in spans:
                if kind == "CODE" and txt.strip():
                    first_code = txt.strip()
                    break
            if not re.match(r"(?i)^CREATE\b", first_code):
                sch = str(source_schema or "").strip()
                name = str(object_name or "").strip()
                return f'CREATE OR REPLACE VIEW "{sch}"."{name}" AS {raw}'
    return definition


def detect_comma_joins(masked_sql: str) -> bool:
    """Detect whether any FROM clause contains comma-separated table sources."""
    for m in re.finditer(r"(?i)\bFROM\b", masked_sql):
        start = m.end()
        i = start
        paren_depth = 0
        n = len(masked_sql)
        while i < n:
            ch = masked_sql[i]
            if ch == "(":
                paren_depth += 1
            elif ch == ")":
                if paren_depth > 0:
                    paren_depth -= 1
                else:
                    break
            elif paren_depth == 0:
                if ch == ";":
                    break
                kw_match = re.match(
                    r"(?i)\b(WHERE|GROUP|HAVING|ORDER|LIMIT|WINDOW|UNION|EXCEPT|INTERSECT|QUALIFY)\b",
                    masked_sql[i:]
                )
                if kw_match:
                    break
                if ch == ",":
                    return True
            i += 1
    return False


def extract_cte_names(sql_text: str) -> set:
    """Extract CTE names defined in WITH clauses from masked code only."""
    cte_names = set()
    spans = tokenize_sql_spans(sql_text)
    # Mask comments and string literals with spaces of the exact same length
    masked_text = "".join(
        text if kind in ("CODE", "QUOTED_IDENT") else (" " * len(text))
        for kind, text in spans
    )
    with_match = re.search(r"(?i)\bWITH\b", masked_text)
    if not with_match:
        return cte_names

    i = with_match.end()
    n = len(masked_text)

    rec_match = re.match(r"\s+RECURSIVE\b", masked_text[i:], re.IGNORECASE)
    if rec_match:
        i += rec_match.end()

    while i < n:
        header_match = re.match(
            r"\s*(?:`([^`]+)`|\[([^\]]+)\]|\"([^\"]+)\"|([a-zA-Z_][a-zA-Z0-9_#$]*))\s*(?:\([^)]*\)\s*)?AS\s*\(",
            masked_text[i:],
            re.IGNORECASE,
        )
        if not header_match:
            break
        name = header_match.group(1) or header_match.group(2) or header_match.group(3) or header_match.group(4)
        if name:
            cte_names.add(name.strip().lower())

        i += header_match.end()
        paren_depth = 1
        while i < n and paren_depth > 0:
            ch = masked_text[i]
            if ch == "(":
                paren_depth += 1
            elif ch == ")":
                paren_depth -= 1
            i += 1

        rest = masked_text[i:].lstrip()
        if rest.startswith(","):
            comma_idx = masked_text.find(",", i)
            i = comma_idx + 1
        else:
            break

    return cte_names


SQL_KEYWORDS = {
    "WHERE", "ON", "JOIN", "INNER", "LEFT", "RIGHT", "FULL", "CROSS", "OUTER",
    "GROUP", "ORDER", "HAVING", "LIMIT", "WINDOW", "UNION", "EXCEPT", "INTERSECT",
    "AND", "OR", "NOT", "SET", "QUALIFY", "CLUSTER", "DISTRIBUTE", "SORT"
}
_KW_PATTERN = "|".join(SQL_KEYWORDS)
_IDENT = r'(?:`[^`]+`|\[[^\]]+\]|\"[^\"]+\"|[a-zA-Z_][a-zA-Z0-9_#$]*)'
_TABLE_REF = rf'(?:\([^\)]+\)|{_IDENT}(?:\.{_IDENT})*)'


def extract_table_qualifiers(sql: str) -> set:
    """Extract valid table qualifiers and aliases defined in FROM / JOIN."""
    qualifiers = set()
    pat = re.compile(
        rf'(?i)\b(?:FROM|JOIN)\s+({_TABLE_REF})(?:\s+(?:AS\s+)?(?!(?:{_KW_PATTERN})\b)([a-zA-Z_][a-zA-Z0-9_#$]*))?'
    )
    for m in pat.finditer(sql):
        t_ref = m.group(1)
        alias = m.group(2)
        if alias:
            qualifiers.add(alias.strip("`\"[]").lower())
        if not t_ref.startswith("("):
            parts = [p.strip("`\"[]").lower() for p in t_ref.split(".")]
            qualifiers.add(parts[-1])
    return qualifiers


def find_view_as_boundary(sql_text: str) -> Tuple[Optional[int], Optional[int], Optional[str]]:
    """Scan sql_text to find the exact top-level AS boundary of CREATE VIEW.

    Ignores:
      - Line comments (-- ... \n)
      - Block comments (/* ... */)
      - Single-quoted string literals ('...')
      - Double-quoted identifiers ("...")
      - Bracketed SQL Server identifiers ([...])
      - Parenthesized column lists ((...))

    Returns:
      (as_start_idx, as_end_idx, error_message)
    """
    if not sql_text or not sql_text.strip():
        return None, None, "View definition is empty"

    i = 0
    n = len(sql_text)
    paren_depth = 0
    found_create = False
    found_view = False
    create_view_count = 0
    as_start = None
    as_end = None

    while i < n:
        # 1. Line comment: --
        if sql_text[i:i+2] == "--":
            end_line = sql_text.find("\n", i + 2)
            if end_line == -1:
                break
            i = end_line + 1
            continue

        # 2. Block comment: /*
        if sql_text[i:i+2] == "/*":
            end_block = sql_text.find("*/", i + 2)
            if end_block == -1:
                return None, None, "Unterminated block comment in SQL"
            i = end_block + 2
            continue

        # 3. Single-quoted string: '
        if sql_text[i] == "'":
            j = i + 1
            while j < n:
                if sql_text[j] == "'":
                    if j + 1 < n and sql_text[j+1] == "'":
                        j += 2
                    else:
                        j += 1
                        break
                else:
                    j += 1
            if j > n or (j == n and sql_text[j-1] != "'"):
                return None, None, "Unterminated single-quoted string in SQL"
            i = j
            continue

        # 4. Double-quoted identifier: "
        if sql_text[i] == '"':
            j = i + 1
            while j < n:
                if sql_text[j] == '"':
                    if j + 1 < n and sql_text[j+1] == '"':
                        j += 2
                    else:
                        j += 1
                        break
                else:
                    j += 1
            if j > n or (j == n and sql_text[j-1] != '"'):
                return None, None, 'Unterminated double-quoted identifier in SQL'
            i = j
            continue

        # 5. Bracketed identifier: [
        if sql_text[i] == '[':
            end_bracket = sql_text.find("]", i + 1)
            if end_bracket == -1:
                return None, None, "Unterminated bracketed identifier in SQL"
            i = end_bracket + 1
            continue

        # 6. Parentheses
        if sql_text[i] == '(':
            paren_depth += 1
            i += 1
            continue
        elif sql_text[i] == ')':
            paren_depth = max(0, paren_depth - 1)
            i += 1
            continue

        # 7. Word boundary check
        char = sql_text[i]
        if char.isalnum() or char == '_':
            start_word = i
            while i < n and (sql_text[i].isalnum() or sql_text[i] == '_'):
                i += 1
            word = sql_text[start_word:i].upper()

            if paren_depth == 0:
                if word == "CREATE":
                    if found_create and found_view and as_start is not None:
                        create_view_count += 1
                    found_create = True
                elif word == "VIEW":
                    if found_create:
                        found_view = True
                elif word == "AS":
                    if found_create and found_view and as_start is None:
                        as_start = start_word
                        as_end = i
            continue

        i += 1

    if create_view_count > 0:
        return None, None, "Multiple ambiguous CREATE VIEW statements in definition"
    if as_start is None:
        return None, None, "Cannot locate top-level AS keyword outside comments and literals in view header"

    return as_start, as_end, None


class SQLArtifactConverter:
    """Handles parsing, conversion, classification, and validation of SQL artifacts."""

    def __init__(self):
        pass

    def detect_constructs(self, sql_text: str) -> Tuple[List[str], List[str]]:
        """Identify unsupported and review-required constructs in source SQL.

        Applies detection strictly to CODE spans only, never to strings or comments.
        """
        spans = tokenize_sql_spans(sql_text)
        masked_text = "".join(
            text if kind == "CODE" else (" " * len(text))
            for kind, text in spans
        )
        unsupported = []
        for code, pattern, desc in UNSUPPORTED_PATTERNS:
            if re.search(pattern, masked_text):
                unsupported.append(f"{code}: {desc}")

        manual = []
        for code, pattern, desc in MANUAL_REVIEW_PATTERNS:
            if re.search(pattern, masked_text):
                manual.append(f"{code}: {desc}")

        return unsupported, manual


    def convert_view(
        self,
        source_system: str,
        source_definition: str,
        target_catalog: str,
        target_schema: str,
        target_object_name: str,
        object_map: Optional[Dict[str, str]] = None,
    ) -> ArtifactConversionResult:
        """Transpile a SQL Server or Oracle VIEW definition to Databricks SQL fail-closed."""
        src_sys = (source_system or "").strip().lower()
        unsupported, manual = self.detect_constructs(source_definition)

        if unsupported:
            return ArtifactConversionResult(
                classification=CLASSIFICATION_UNSUPPORTED,
                conversion_status=CONVERSION_STATUS_UNSUPPORTED,
                converted_definition=None,
                manual_review_required=True,
                unsupported_features="; ".join(unsupported),
                manual_review_reason="View contains unsupported non-migratable constructs",
                target_object_name=target_object_name,
                validation_passed=False,
            )

        as_start, as_end, as_err = find_view_as_boundary(source_definition)
        if as_err or as_start is None:
            return ArtifactConversionResult(
                classification=CLASSIFICATION_MANUAL_REVIEW,
                conversion_status=CONVERSION_STATUS_PARTIAL,
                converted_definition=source_definition,
                manual_review_required=True,
                manual_review_reason=as_err or "Cannot locate AS keyword in view header",
                target_object_name=target_object_name,
                validation_passed=False,
            )

        select_body = source_definition[as_end:].strip()
        # Remove trailing semicolons or slashes (do not strip */ from block comments)
        select_body = re.sub(r"(?:[\s;]|(?<!\*)/)+$", "", select_body)

        # Check for multiple statements separated by semicolon in view body
        if ";" in select_body:
            parts = [p.strip() for p in select_body.split(";") if p.strip()]
            if len(parts) > 1:
                return ArtifactConversionResult(
                    classification=CLASSIFICATION_MANUAL_REVIEW,
                    conversion_status=CONVERSION_STATUS_PARTIAL,
                    converted_definition=source_definition,
                    manual_review_required=True,
                    manual_review_reason="Multiple statements or semicolon-separated payload detected in view body",
                    target_object_name=target_object_name,
                    validation_passed=False,
                )

        # Fail closed on TOP, TOP (...), WITH TIES (on CODE spans only)
        spans_for_checks = tokenize_sql_spans(select_body)
        masked_for_checks = "".join(
            text if kind == "CODE" else (" " * len(text))
            for kind, text in spans_for_checks
        )

        if re.search(r"(?i)\bTOP\b", masked_for_checks):
            return ArtifactConversionResult(
                classification=CLASSIFICATION_MANUAL_REVIEW,
                conversion_status=CONVERSION_STATUS_PARTIAL,
                converted_definition=source_definition,
                manual_review_required=True,
                manual_review_reason="SQL Server TOP syntax not supported for AUTO conversion; manual review required",
                target_object_name=target_object_name,
                validation_passed=False,
            )
        if re.search(r"(?i)\bWITH\s+TIES\b", masked_for_checks):
            return ArtifactConversionResult(
                classification=CLASSIFICATION_MANUAL_REVIEW,
                conversion_status=CONVERSION_STATUS_PARTIAL,
                converted_definition=source_definition,
                manual_review_required=True,
                manual_review_reason="SQL Server WITH TIES syntax not supported for AUTO conversion; manual review required",
                target_object_name=target_object_name,
                validation_passed=False,
            )

        # Fail closed on Oracle clauses: CONNECT BY, START WITH, MODEL, PIVOT, UNPIVOT, hints
        oracle_forbidden = [
            (r"(?i)\bCONNECT\s+BY\b", "Hierarchical CONNECT BY clause"),
            (r"(?i)\bSTART\s+WITH\b", "Hierarchical START WITH clause"),
            (r"(?i)\bMODEL\b", "Oracle MODEL clause"),
            (r"(?i)\bPIVOT\b", "PIVOT clause"),
            (r"(?i)\bUNPIVOT\b", "UNPIVOT clause"),
        ]
        for pat, desc in oracle_forbidden:
            if re.search(pat, masked_for_checks):
                return ArtifactConversionResult(
                    classification=CLASSIFICATION_MANUAL_REVIEW,
                    conversion_status=CONVERSION_STATUS_PARTIAL,
                    converted_definition=source_definition,
                    manual_review_required=True,
                    manual_review_reason=f"Unsupported Oracle clause: {desc}; manual review required",
                    target_object_name=target_object_name,
                    validation_passed=False,
                )
        if any(kind == "BLOCK_COMMENT" and txt.startswith("/*+") for kind, txt in spans_for_checks):
            return ArtifactConversionResult(
                classification=CLASSIFICATION_MANUAL_REVIEW,
                conversion_status=CONVERSION_STATUS_PARTIAL,
                converted_definition=source_definition,
                manual_review_required=True,
                manual_review_reason="Unsupported Oracle clause: Oracle query hint; manual review required",
                target_object_name=target_object_name,
                validation_passed=False,
            )

        # Apply source-specific function and dialect transformations on CODE spans only
        transformed_body = select_body

        if src_sys == "sqlserver":
            spans_before = tokenize_sql_spans(transformed_body)
            parts = []
            for kind, txt in spans_before:
                if kind == "CODE":
                    t = re.sub(r"(?i)\bWITH\s*\(\s*NOLOCK\s*\)", "", txt)
                    for pat, rep, _ in SQLSERVER_FUNCTION_RULES:
                        t = re.sub(pat, rep, t)
                    parts.append(t)
                elif kind == "QUOTED_IDENT":
                    # Bracket stripping on simple identifiers [col] -> col
                    t = re.sub(r"^\[([a-zA-Z0-9_#$]+)\]$", r"\1", txt)
                    parts.append(t)
                else:
                    parts.append(txt)
            transformed_body = "".join(parts)

            # If any brackets still remain in transformed_body outside string literals/comments, fail closed
            has_brackets = any(
                ("[" in txt or "]" in txt)
                for kind, txt in tokenize_sql_spans(transformed_body)
                if kind in ("CODE", "QUOTED_IDENT")
            )
            if has_brackets:
                return ArtifactConversionResult(
                    classification=CLASSIFICATION_MANUAL_REVIEW,
                    conversion_status=CONVERSION_STATUS_PARTIAL,
                    converted_definition=source_definition,
                    manual_review_required=True,
                    manual_review_reason="Unconverted bracket syntax remaining in view body; manual review required",
                    target_object_name=target_object_name,
                    validation_passed=False,
                )

        elif src_sys == "oracle":
            spans_before = tokenize_sql_spans(transformed_body)
            parts = []
            for kind, txt in spans_before:
                if kind == "CODE":
                    t = txt
                    for pat, rep, _ in ORACLE_FUNCTION_RULES:
                        t = re.sub(pat, rep, t)
                    parts.append(t)
                else:
                    parts.append(txt)
            transformed_body = "".join(parts)

        # Safe Policy: Token-aware rewrite of external table/view references in FROM / JOIN.
        # References are rewritten only for the identifier sequence immediately following
        # FROM or JOIN keywords outside comments and string literals.
        spans = tokenize_sql_spans(transformed_body)
        masked_body = "".join(
            (" " * len(text)) if kind in ("LINE_COMMENT", "BLOCK_COMMENT", "STRING") else text
            for kind, text in spans
        )

        # Comma-joins in FROM clause -> MANUAL_REVIEW
        if detect_comma_joins(masked_body):
            return ArtifactConversionResult(
                classification=CLASSIFICATION_MANUAL_REVIEW,
                conversion_status=CONVERSION_STATUS_PARTIAL,
                converted_definition=source_definition,
                manual_review_required=True,
                manual_review_reason="Comma-join syntax detected in view; manual review required",
                target_object_name=target_object_name,
                validation_passed=False,
            )

        cte_names = extract_cte_names(masked_body)

        ident_pat = r'(?:`[^`]+`|\[[^\]]+\]|\"[^\"]+\"|[a-zA-Z_][a-zA-Z0-9_#$]*)'
        table_ref_pat = re.compile(
            rf'(?i)\b(FROM|JOIN)\s+({ident_pat}(?:\.{ident_pat})*)'
        )

        new_text_parts = []
        last_idx = 0

        for m in table_ref_pat.finditer(masked_body):
            kw = m.group(1)
            ref = m.group(2)
            ref_upper = ref.upper()

            # Subquery or DUAL
            if ref_upper in ("DUAL", "(SELECT", "(") or ref.startswith("("):
                continue

            # Check if reference is in object_map
            mapped = None
            if object_map:
                mapped = object_map.get(ref)
                if not mapped:
                    for k, v in object_map.items():
                        if k.lower() == ref.lower():
                            mapped = v
                            break

            if mapped:
                new_text_parts.append(transformed_body[last_idx:m.start(2)])
                new_text_parts.append(mapped)
                last_idx = m.end(2)
                continue

            # Split segments of the reference
            segments = [p.strip("`\"[]").lower() for p in ref.split(".")]
            first_seg = segments[0]

            # If first segment is target catalog, it is already pointing to target catalog
            if first_seg == target_catalog.strip("`\"[]").lower():
                continue

            # If first segment is a CTE name, it references a CTE
            if first_seg in cte_names:
                continue

            # Otherwise, check if it was quoted, undotted, or dotted:
            # 1. Quoted identifier span: if ref starts with ", `, or [
            if ref.startswith(('"', '`', '[')):
                return ArtifactConversionResult(
                    classification=CLASSIFICATION_MANUAL_REVIEW,
                    conversion_status=CONVERSION_STATUS_PARTIAL,
                    converted_definition=source_definition,
                    manual_review_required=True,
                    manual_review_reason=f"Quoted identifier reference '{ref}' following {kw} requires manual review",
                    target_object_name=target_object_name,
                    validation_passed=False,
                )

            # 2. Undotted bare name
            if "." not in ref:
                return ArtifactConversionResult(
                    classification=CLASSIFICATION_MANUAL_REVIEW,
                    conversion_status=CONVERSION_STATUS_PARTIAL,
                    converted_definition=source_definition,
                    manual_review_required=True,
                    manual_review_reason=f"Unresolved bare table reference '{ref}' following {kw} is not in object_map or CTEs; manual review required",
                    target_object_name=target_object_name,
                    validation_passed=False,
                )

            # 3. Dotted reference
            return ArtifactConversionResult(
                classification=CLASSIFICATION_MANUAL_REVIEW,
                conversion_status=CONVERSION_STATUS_PARTIAL,
                converted_definition=source_definition,
                manual_review_required=True,
                manual_review_reason=f"Unresolved source-qualified reference '{ref}' in view definition; manual review required",
                target_object_name=target_object_name,
                validation_passed=False,
            )

        new_text_parts.append(transformed_body[last_idx:])
        transformed_body = "".join(new_text_parts)

        # Construct target Databricks SQL View statement
        target_fqn = databricks_fqn(target_catalog, target_schema, target_object_name)
        converted_sql = f"CREATE OR REPLACE VIEW {target_fqn} AS\n{transformed_body}"

        # If manual review constructs were detected, classify as MANUAL_REVIEW
        if manual:
            return ArtifactConversionResult(
                classification=CLASSIFICATION_MANUAL_REVIEW,
                conversion_status=CONVERSION_STATUS_PARTIAL,
                converted_definition=converted_sql,
                manual_review_required=True,
                manual_review_reason="; ".join(manual),
                target_object_name=target_object_name,
                validation_passed=False,
            )

        # Validate the converted SQL structurally
        val_ok, val_err = self.validate_converted_sql(converted_sql, target_catalog, target_schema, target_object_name)
        if not val_ok:
            return ArtifactConversionResult(
                classification=CLASSIFICATION_MANUAL_REVIEW,
                conversion_status=CONVERSION_STATUS_PARTIAL,
                converted_definition=converted_sql,
                manual_review_required=True,
                manual_review_reason=f"Validation failed: {val_err}",
                target_object_name=target_object_name,
                error_code="VALIDATION_FAILED",
                error_message=val_err,
                validation_passed=False,
            )

        return ArtifactConversionResult(
            classification=CLASSIFICATION_AUTO,
            conversion_status=CONVERSION_STATUS_CONVERTED,
            converted_definition=converted_sql,
            manual_review_required=False,
            target_object_name=target_object_name,
            validation_passed=True,
        )

    def convert_routine(
        self,
        source_system: str,
        object_type: str,
        source_definition: str,
        target_catalog: str,
        target_schema: str,
        target_object_name: str,
    ) -> ArtifactConversionResult:
        """Analyze and convert stored procedures, functions, packages, triggers."""
        otype = normalize_artifact_type(object_type)
        unsupported, manual = self.detect_constructs(source_definition)

        # Package, Package Body, and Triggers cannot be directly auto-deployed into Unity Catalog
        if otype in ("PACKAGE", "PACKAGE_BODY"):
            return ArtifactConversionResult(
                classification=CLASSIFICATION_MANUAL_REVIEW,
                conversion_status=CONVERSION_STATUS_PARTIAL,
                converted_definition=source_definition,
                manual_review_required=True,
                manual_review_reason="Packages require decomposing into individual procedures/functions or notebooks for Databricks",
                unsupported_features="; ".join(unsupported) if unsupported else None,
                target_object_name=target_object_name,
                validation_passed=False,
            )

        if otype == "TRIGGER":
            return ArtifactConversionResult(
                classification=CLASSIFICATION_UNSUPPORTED,
                conversion_status=CONVERSION_STATUS_UNSUPPORTED,
                converted_definition=None,
                manual_review_required=True,
                manual_review_reason="Unity Catalog does not support database triggers; redesign as Delta Live Tables or stream trigger",
                unsupported_features="DATABASE_TRIGGERS: Triggers not supported in Databricks UC",
                target_object_name=target_object_name,
                validation_passed=False,
            )

        if unsupported:
            return ArtifactConversionResult(
                classification=CLASSIFICATION_UNSUPPORTED,
                conversion_status=CONVERSION_STATUS_UNSUPPORTED,
                converted_definition=None,
                manual_review_required=True,
                unsupported_features="; ".join(unsupported),
                manual_review_reason=f"{otype} contains unsupported non-migratable procedural constructs",
                target_object_name=target_object_name,
                validation_passed=False,
            )

        # For PROCEDURE or FUNCTION:
        # Check if it has procedural/manual constructs
        if manual or re.search(r"(?i)\b(?:BEGIN|DECLARE|SET|IF|ELSE)\b", source_definition):
            return ArtifactConversionResult(
                classification=CLASSIFICATION_MANUAL_REVIEW,
                conversion_status=CONVERSION_STATUS_PARTIAL,
                converted_definition=source_definition,
                manual_review_required=True,
                manual_review_reason="Procedural routine requires review before Databricks SQL or Python execution",
                target_object_name=target_object_name,
                validation_passed=False,
            )

        # If simple deterministic scalar function without procedural logic
        target_fqn = databricks_fqn(target_catalog, target_schema, target_object_name)
        # Wrap or preserve
        return ArtifactConversionResult(
            classification=CLASSIFICATION_MANUAL_REVIEW,
            conversion_status=CONVERSION_STATUS_PARTIAL,
            converted_definition=source_definition,
            manual_review_required=True,
            manual_review_reason="Routines require explicit operator review prior to deployment",
            target_object_name=target_object_name,
            validation_passed=False,
        )

    def convert_artifact(
        self,
        source_system: str,
        object_type: str,
        object_name: str,
        source_definition: str,
        target_catalog: str,
        target_schema: str,
        object_map: Optional[Dict[str, str]] = None,
        source_schema: str = "",
    ) -> ArtifactConversionResult:
        """High-level dispatcher for converting any non-table artifact."""
        if not source_definition or not str(source_definition).strip():
            return ArtifactConversionResult(
                classification=CLASSIFICATION_UNSUPPORTED,
                conversion_status=CONVERSION_STATUS_FAILED,
                converted_definition=None,
                manual_review_required=True,
                manual_review_reason="Source definition is empty or inaccessible",
                error_code="DEFINITION_EMPTY",
                error_message="Source definition is empty or inaccessible",
                validation_passed=False,
            )

        try:
            norm_type = normalize_artifact_type(object_type)
        except Exception as e:
            return ArtifactConversionResult(
                classification=CLASSIFICATION_UNSUPPORTED,
                conversion_status=CONVERSION_STATUS_FAILED,
                converted_definition=None,
                manual_review_required=True,
                manual_review_reason=f"Unsupported object type: {object_type}",
                error_code="UNSUPPORTED_TYPE",
                error_message=sanitize_error(e),
                validation_passed=False,
            )

        # Normalize target object name
        try:
            target_object_name = normalize_target_identifier(object_name, identifier_type=norm_type.lower())
        except Exception as e:
            return ArtifactConversionResult(
                classification=CLASSIFICATION_UNSUPPORTED,
                conversion_status=CONVERSION_STATUS_FAILED,
                converted_definition=None,
                manual_review_required=True,
                manual_review_reason=f"Cannot normalize target identifier: {object_name}",
                error_code="IDENTIFIER_NORMALIZATION_FAILED",
                error_message=sanitize_error(e),
                validation_passed=False,
            )

        if norm_type == "VIEW":
            view_def = wrap_oracle_view_definition_if_needed(
                source_system=source_system,
                object_type=norm_type,
                source_schema=source_schema,
                object_name=object_name,
                definition=source_definition,
            )
            return self.convert_view(
                source_system=source_system,
                source_definition=view_def,
                target_catalog=target_catalog,
                target_schema=target_schema,
                target_object_name=target_object_name,
                object_map=object_map,
            )
        else:
            return self.convert_routine(
                source_system=source_system,
                object_type=norm_type,
                source_definition=source_definition,
                target_catalog=target_catalog,
                target_schema=target_schema,
                target_object_name=target_object_name,
            )

    def validate_converted_sql(
        self,
        converted_sql: str,
        target_catalog: str,
        target_schema: str,
        target_object_name: str,
    ) -> Tuple[bool, Optional[str]]:
        """Validate converted SQL before execution."""
        if not converted_sql or not converted_sql.strip():
            return False, "Converted SQL definition is empty"

        # Check for placeholder markers
        for marker in ("TODO", "FIXME", "__UNRESOLVED__", "???"):
            if marker in converted_sql:
                return False, f"Unresolved placeholder '{marker}' present in converted SQL"

        # Check target object FQN is present
        target_fqn = databricks_fqn(target_catalog, target_schema, target_object_name)
        if target_fqn not in converted_sql:
            return False, f"Target FQN {target_fqn} not properly embedded in statement"

        # Output must begin with CREATE OR REPLACE VIEW
        if not re.match(r"(?i)^\s*CREATE\s+OR\s+REPLACE\s+VIEW\s+", converted_sql):
            return False, "Converted SQL does not begin with CREATE OR REPLACE VIEW"

        # Check for multiple statements separated by semicolon
        body_after_header = re.sub(r"(?i)^\s*CREATE\s+OR\s+REPLACE\s+VIEW\s+[^\s]+\s+AS\s+", "", converted_sql)
        cleaned_body = re.sub(r"(?:[\s;]|(?<!\*)/)+$", "", body_after_header)
        if ";" in cleaned_body:
            parts = [p.strip() for p in cleaned_body.split(";") if p.strip()]
            if len(parts) > 1:
                return False, "Multiple statements or semicolon-separated payload detected in converted SQL"

        # Structural validation must reject unresolved dotted references in CODE spans.
        # Any dotted reference whose first part is not the target catalog is rejected,
        # unless it is a table alias/qualifier defined in FROM/JOIN or a CTE name.
        target_cat_norm = target_catalog.strip("`\"[]").lower()
        cte_names = extract_cte_names(converted_sql)
        table_qualifiers = extract_table_qualifiers(converted_sql)

        ident_pat = r'(?:`[^`]+`|\[[^\]]+\]|\"[^\"]+\"|[a-zA-Z_][a-zA-Z0-9_#$]*)'
        dotted_pat = re.compile(rf'(?i)(?<![a-zA-Z0-9_#$`\"\]])({ident_pat}(?:\.{ident_pat})+)')

        code_spans = [txt for kind, txt in tokenize_sql_spans(converted_sql) if kind == "CODE"]
        for c in code_spans:
            for m in dotted_pat.finditer(c):
                full_ref = m.group(1)
                segments = [p.strip("`\"[]").lower() for p in full_ref.split(".")]
                first_seg = segments[0]
                if first_seg == target_cat_norm:
                    continue
                if first_seg in table_qualifiers or first_seg in cte_names:
                    continue
                return False, f"Unresolved dotted reference '{full_ref}' present in converted SQL: first segment is not target catalog '{target_catalog}'"

        # Check for obvious unbalanced parentheses
        open_parens = converted_sql.count("(")
        close_parens = converted_sql.count(")")
        if open_parens != close_parens:
            return False, f"Unbalanced parentheses: open={open_parens}, close={close_parens}"

        # Check for unbalanced single quotes (excluding escaped '')
        stripped_single = re.sub(r"''", "", converted_sql)
        if stripped_single.count("'") % 2 != 0:
            return False, "Unbalanced single quotes in converted SQL"

        return True, None
