"""
sql_object_converter.py - pure classification + limited deterministic conversion
of source views / routines to Databricks SQL.

This module NEVER executes SQL and NEVER deploys anything. It only:
  * classify_sql_object(...) -> a readable complexity category + reason, and
  * convert_sql_object_deterministic(...) -> a best-effort Databricks SQL draft
    for simple views using a small, safe set of textual replacements.

Replacements never touch text inside quoted string literals or comments. When a
conversion cannot be done safely the source is retained and review is required.
Fully unit-testable with no Spark.
"""

from __future__ import annotations

import re

ORACLE = "oracle"
SQLSERVER = "sqlserver"

# complexity categories
AUTO_CONVERT = "AUTO_CONVERT"
CONVERT_WITH_REVIEW = "CONVERT_WITH_REVIEW"
MANUAL_REDESIGN = "MANUAL_REDESIGN"
UNABLE_TO_ASSESS = "UNABLE_TO_ASSESS"

# conversion status
NOT_STARTED = "NOT_STARTED"
GENERATED = "GENERATED"
FAILED = "FAILED"
NOT_SUPPORTED = "NOT_SUPPORTED"
NOT_CONFIGURED = "NOT_CONFIGURED"

# Procedural / non-portable features that force a manual redesign. Matched as
# whole words against text with string-literals and comments masked out.
_MANUAL_INDICATORS = [
    r"\bCURSOR\b", r"\bEXECUTE\s+IMMEDIATE\b", r"\bsp_executesql\b",
    r"\bOPENQUERY\b", r"\bWHILE\b", r"\bLOOP\b", r"\bGOTO\b",
    r"\bCOMMIT\b", r"\bROLLBACK\b", r"\bSAVEPOINT\b", r"\bRAISERROR\b",
    r"\bTHROW\b", r"\bDBMS_SCHEDULER\b", r"\bDBMS_JOB\b",
    r"\bAUTONOMOUS_TRANSACTION\b", r"\bBULK\s+COLLECT\b", r"\bFORALL\b",
    r"\bEXCEPTION\b", r"(?<![#\w])#{1,2}\w+",   # SQL Server temp / global temp
    r"\bGLOBAL\s+TEMPORARY\b", r"\bCREATE\s+TEMPORARY\b",
    r"@@\w+", r"\bTABLE\s+VARIABLE\b",
]

# Source-specific view syntax that is convertible but must be reviewed.
_VIEW_REVIEW_SYNTAX = {
    ORACLE: [r"\bCONNECT\s+BY\b", r"\(\+\)", r"\bROWNUM\b", r"\bDECODE\b",
             r"\bNVL2?\b", r"\bSYSDATE\b", r"\bSYSTIMESTAMP\b", r"\bDUAL\b",
             r"\bFROM\s+DUAL\b"],
    SQLSERVER: [r"\bTOP\b", r"\bISNULL\b", r"\bGETDATE\b", r"\bGETUTCDATE\b",
                r"\bLEN\b", r"\[[^\]]+\]", r"\bCROSS\s+APPLY\b",
                r"\bOUTER\s+APPLY\b", r"\bNOLOCK\b"],
}


def _normalize_system(source_system):
    s = (source_system or "").strip().lower()
    return ORACLE if s == ORACLE else SQLSERVER


def _mask(sql):
    """Replace string literals and comments with spaces of equal length.

    Keeps offsets stable so keyword detection never matches inside a quoted
    string or a comment. Used only for *analysis*, not for output.
    """
    out = list(sql)
    i, n = 0, len(sql)
    while i < n:
        ch = sql[i]
        if ch == "'":
            j = i + 1
            while j < n:
                if sql[j] == "'":
                    if j + 1 < n and sql[j + 1] == "'":
                        j += 2
                        continue
                    break
                j += 1
            for k in range(i, min(j + 1, n)):
                out[k] = " "
            i = j + 1
        elif ch == "-" and i + 1 < n and sql[i + 1] == "-":
            j = i
            while j < n and sql[j] != "\n":
                out[j] = " "
                j += 1
            i = j
        elif ch == "/" and i + 1 < n and sql[i + 1] == "*":
            j = i + 2
            while j < n and not (sql[j] == "*" and j + 1 < n and sql[j + 1] == "/"):
                j += 1
            end = min(j + 2, n)
            for k in range(i, end):
                out[k] = " "
            i = end
        else:
            i += 1
    return "".join(out)


def _matches(patterns, masked):
    hits = []
    for p in patterns:
        if re.search(p, masked, flags=re.IGNORECASE):
            hits.append(p.strip("\\b").replace("\\s+", " "))
    return hits


def classify_sql_object(source_system, object_type, source_definition):
    """Return (complexity_category, classification_reason) - never a score.

    Oracle PACKAGE / PACKAGE_BODY and any object using procedural / non-portable
    features default to MANUAL_REDESIGN. Simple views are AUTO_CONVERT; views
    with source-specific syntax are CONVERT_WITH_REVIEW; procedures/functions are
    REVIEW or MANUAL depending on procedural features. Missing text is
    UNABLE_TO_ASSESS - text is never fabricated.
    """
    if not source_definition or not str(source_definition).strip():
        return UNABLE_TO_ASSESS, "source text unavailable"
    system = _normalize_system(source_system)
    otype = (object_type or "").strip().upper().replace(" ", "_")
    masked = _mask(str(source_definition))

    if system == ORACLE and otype in ("PACKAGE", "PACKAGE_BODY"):
        return MANUAL_REDESIGN, "Oracle package (stateful/procedural) - redesign required"

    manual_hits = _matches(_MANUAL_INDICATORS, masked)
    if manual_hits:
        return MANUAL_REDESIGN, "procedural/non-portable features: " + ", ".join(
            sorted(set(manual_hits))[:8])

    if otype == "VIEW":
        review_hits = _matches(_VIEW_REVIEW_SYNTAX[system], masked)
        if review_hits:
            return CONVERT_WITH_REVIEW, "source-specific view syntax: " + ", ".join(
                sorted(set(review_hits))[:8])
        return AUTO_CONVERT, "simple view - portable SELECT"

    if otype in ("PROCEDURE", "FUNCTION"):
        return CONVERT_WITH_REVIEW, (
            f"{otype.lower()} without procedural flow - review the generated draft")
    return CONVERT_WITH_REVIEW, "review the generated draft"


# ---- limited, safe textual replacements (applied outside strings/comments) --
_ORACLE_REPLACEMENTS = [
    (r"\bCREATE\s+OR\s+REPLACE\s+FORCE\s+VIEW\b", "CREATE OR REPLACE VIEW"),
    (r"\bNVL\s*\(", "COALESCE("),
    (r"\bSYSDATE\b", "current_timestamp()"),
    (r"\bSYSTIMESTAMP\b", "current_timestamp()"),
]
_SQLSERVER_REPLACEMENTS = [
    (r"\bISNULL\s*\(", "COALESCE("),
    (r"\bGETDATE\s*\(\s*\)", "current_timestamp()"),
    (r"\bGETUTCDATE\s*\(\s*\)", "current_timestamp()"),
    (r"\bLEN\s*\(", "length("),
]


def _apply_outside_literals(sql, replacements):
    """Apply (pattern, repl) only to code, never to string literals/comments.

    Rebuilds the string in segments: literal/comment segments are copied
    verbatim; code segments have the replacements applied.
    """
    masked = _mask(sql)
    result = []
    i, n = 0, len(sql)
    seg_start = 0
    # Walk masked to find literal/comment runs (spaces that replaced content).
    # Simpler: process code vs preserved by comparing char-by-char runs where
    # masked==original (code) vs masked==' ' but original!=' ' (masked content).
    def is_masked(idx):
        return masked[idx] == " " and sql[idx] != " "

    while i < n:
        start = i
        state = is_masked(i)
        while i < n and is_masked(i) == state:
            i += 1
        segment = sql[start:i]
        if state:
            result.append(segment)          # preserved literal/comment
        else:
            for pat, rep in replacements:
                segment = re.sub(pat, rep, segment, flags=re.IGNORECASE)
            result.append(segment)
    return "".join(result)


def _convert_brackets(sql):
    """Convert SQL Server [identifier] to Databricks `identifier` in code only."""
    def repl(m):
        inner = m.group(1)
        if re.match(r"^[A-Za-z_][A-Za-z0-9_ $#-]*$", inner):
            return "`" + inner + "`"
        return m.group(0)
    return _apply_outside_literals_custom(sql, r"\[([^\]]+)\]", repl)


def _apply_outside_literals_custom(sql, pattern, repl):
    masked = _mask(sql)
    result = []
    i, n = 0, len(sql)

    def is_masked(idx):
        return masked[idx] == " " and sql[idx] != " "

    while i < n:
        start = i
        state = is_masked(i)
        while i < n and is_masked(i) == state:
            i += 1
        segment = sql[start:i]
        if not state:
            segment = re.sub(pattern, repl, segment)
        result.append(segment)
    return "".join(result)


def _convert_simple_top(sql):
    """Convert a single simple `SELECT TOP (n) ...` to a trailing `LIMIT n`.

    Only applied to a recognized simple SELECT: exactly one TOP, no set
    operators, no nested SELECT. Otherwise the text is returned unchanged.
    """
    masked = _mask(sql)
    if len(re.findall(r"\bTOP\b", masked, flags=re.IGNORECASE)) != 1:
        return sql, False
    if re.search(r"\b(UNION|INTERSECT|EXCEPT)\b", masked, flags=re.IGNORECASE):
        return sql, False
    if len(re.findall(r"\bSELECT\b", masked, flags=re.IGNORECASE)) != 1:
        return sql, False
    m = re.search(r"\bTOP\s*\(?\s*(\d+)\s*\)?", sql, flags=re.IGNORECASE)
    if not m:
        return sql, False
    n = m.group(1)
    converted = (sql[:m.start()] + sql[m.end():]).rstrip().rstrip(";")
    converted = converted + f"\nLIMIT {n}"
    return converted, True


def convert_sql_object_deterministic(source_system, object_type, source_definition):
    """Return (converted_definition, conversion_language, conversion_status).

    Only simple / reviewable VIEWs are converted to Databricks SQL using the
    limited replacement set. Procedures, functions, and packages are not
    deterministically converted - they return MANUAL_REDESIGN_GUIDANCE so a human
    (or an approved AI endpoint) can complete them. Generated output is always a
    draft that must be reviewed before use; nothing is executed here.
    """
    if not source_definition or not str(source_definition).strip():
        return None, None, NOT_STARTED
    system = _normalize_system(source_system)
    otype = (object_type or "").strip().upper().replace(" ", "_")
    complexity, _reason = classify_sql_object(source_system, object_type, source_definition)

    if otype != "VIEW" or complexity in (MANUAL_REDESIGN, UNABLE_TO_ASSESS):
        return (_manual_guidance(system, otype, source_definition),
                "MANUAL_REDESIGN_GUIDANCE", NOT_SUPPORTED)

    sql = str(source_definition)
    if system == ORACLE:
        sql = _apply_outside_literals(sql, _ORACLE_REPLACEMENTS)
    else:
        sql = _apply_outside_literals(sql, _SQLSERVER_REPLACEMENTS)
        sql = _convert_brackets(sql)
        sql, _ = _convert_simple_top(sql)
    return sql, "DATABRICKS_SQL", GENERATED


def _manual_guidance(system, object_type, source_definition):
    """A short, non-executable redesign checklist for a routine/package."""
    return (
        f"-- MANUAL REDESIGN GUIDANCE ({system} {object_type})\n"
        "-- Not auto-converted or executed. Identify: parameters, tables read,\n"
        "-- tables written, temporary objects, transaction boundaries, control\n"
        "-- flow branches, and any dynamic SQL. Re-express as a Databricks SQL\n"
        "-- view, a parameterized PySpark notebook, or a sequenced Databricks Job.\n"
        "-- Original definition preserved in source_definition."
    )
