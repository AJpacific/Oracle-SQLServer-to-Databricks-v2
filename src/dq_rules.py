"""
dq_rules.py - pure data-quality rule helpers for the Bronze-to-Silver ETL.

Only the fixed MVP rule set is supported; arbitrary SQL expressions are never
accepted. The functions here validate/parse rule configuration and classify rule
types. The actual row-level application lives in the ETL notebook (Spark); these
helpers stay pure and unit-testable.
"""

from __future__ import annotations

import json
import re

# ---- rule types (MVP only) -------------------------------------------------
NOT_NULL = "NOT_NULL"
DUPLICATE_KEY = "DUPLICATE_KEY"
DATA_TYPE = "DATA_TYPE"
ALLOWED_VALUES = "ALLOWED_VALUES"
DEFAULT_VALUE = "DEFAULT_VALUE"
TRIM_STRING = "TRIM_STRING"
STANDARDIZE_CASE = "STANDARDIZE_CASE"

VALIDATION_RULES = {NOT_NULL, DUPLICATE_KEY, DATA_TYPE, ALLOWED_VALUES}
TRANSFORMATION_RULES = {DEFAULT_VALUE, TRIM_STRING, STANDARDIZE_CASE}
SUPPORTED_RULE_TYPES = VALIDATION_RULES | TRANSFORMATION_RULES

# Rules that need a target column.
_COLUMN_REQUIRED = {NOT_NULL, DATA_TYPE, ALLOWED_VALUES, DEFAULT_VALUE,
                    TRIM_STRING, STANDARDIZE_CASE}

# Severities that reject (quarantine) a row; anything else only reports.
_REJECTING_SEVERITIES = {"ERROR", "REJECT", "BLOCK", "CRITICAL"}

# Target types a DATA_TYPE rule may safely cast to. An arbitrary expression is
# never accepted - only a recognized Databricks scalar type.
_SIMPLE_CAST_TYPES = {
    "STRING", "BOOLEAN", "BYTE", "TINYINT", "SHORT", "SMALLINT", "INT",
    "INTEGER", "LONG", "BIGINT", "FLOAT", "REAL", "DOUBLE", "DATE",
    "TIMESTAMP", "TIMESTAMP_NTZ", "BINARY",
}
_PARAMETRIC_CAST = re.compile(
    r"^(DECIMAL|NUMERIC)\s*\(\s*\d{1,2}\s*(,\s*\d{1,2}\s*)?\)$")


def normalize_cast_type(rule_value):
    """Return a validated DATA_TYPE target type, else raise ValueError."""
    token = (rule_value or "").strip().upper()
    if not token:
        raise ValueError("DATA_TYPE requires a target type in rule_value")
    if token in _SIMPLE_CAST_TYPES or _PARAMETRIC_CAST.match(token):
        return token
    raise ValueError(f"unsupported DATA_TYPE target type: {rule_value!r}")


def is_validation(rule_type):
    return (rule_type or "").upper() in VALIDATION_RULES


def is_transformation(rule_type):
    return (rule_type or "").upper() in TRANSFORMATION_RULES


def is_supported(rule_type):
    return (rule_type or "").upper() in SUPPORTED_RULE_TYPES


def is_rejecting_severity(severity):
    """A missing severity defaults to ERROR (rejecting)."""
    if severity is None or str(severity).strip() == "":
        return True
    return str(severity).strip().upper() in _REJECTING_SEVERITIES


def parse_allowed_values(rule_value):
    """Parse an ALLOWED_VALUES rule_value (a JSON array) into a list.

    Raises ValueError if it is not a JSON array. Never eval()s or accepts SQL.
    """
    if rule_value is None or str(rule_value).strip() == "":
        raise ValueError("ALLOWED_VALUES requires a non-empty JSON array")
    try:
        parsed = json.loads(rule_value)
    except (ValueError, TypeError) as exc:
        raise ValueError(f"ALLOWED_VALUES must be a JSON array: {exc}")
    if not isinstance(parsed, list):
        raise ValueError("ALLOWED_VALUES must be a JSON array")
    return parsed


def normalize_case_mode(rule_value):
    """Return 'UPPER' or 'LOWER' for a STANDARDIZE_CASE rule, else raise."""
    mode = (rule_value or "").strip().upper()
    if mode not in ("UPPER", "LOWER"):
        raise ValueError("STANDARDIZE_CASE rule_value must be UPPER or LOWER")
    return mode


def validate_rule(rule, available_columns=None, primary_key_columns=None):
    """Validate one dq_rule dict; return a normalized copy or raise ValueError.

    Ensures the rule_type is one of the fixed MVP types (never arbitrary SQL),
    that a column is present where required, that ALLOWED_VALUES /
    STANDARDIZE_CASE / DATA_TYPE carry a well-formed value, and - when the
    caller supplies them - that the referenced column actually exists and a
    DUPLICATE_KEY rule has usable key columns.
    """
    rule = dict(rule or {})
    rule_type = (rule.get("rule_type") or "").upper()
    if rule_type not in SUPPORTED_RULE_TYPES:
        raise ValueError(f"unsupported rule_type: {rule.get('rule_type')!r}")
    column = rule.get("column_name")
    if rule_type in _COLUMN_REQUIRED and not column:
        raise ValueError(f"{rule_type} requires column_name")
    if rule_type == ALLOWED_VALUES:
        parse_allowed_values(rule.get("rule_value"))
    if rule_type == STANDARDIZE_CASE:
        normalize_case_mode(rule.get("rule_value"))
    if rule_type == DATA_TYPE:
        normalize_cast_type(rule.get("rule_value"))

    if available_columns is not None:
        available = {str(c) for c in available_columns}
        if column and column not in available:
            raise ValueError(
                f"{rule_type} references missing column {column!r}")
        if rule_type == DUPLICATE_KEY:
            keys = duplicate_key_columns(rule, primary_key_columns)
            if not keys:
                raise ValueError(
                    "DUPLICATE_KEY has no usable key columns "
                    "(configure column_name or primary_key_columns)")
            missing = [k for k in keys if k not in available]
            if missing:
                raise ValueError(
                    f"DUPLICATE_KEY references missing column(s): {missing}")

    rule["rule_type"] = rule_type
    return rule


def duplicate_key_columns(rule, primary_key_columns=None):
    """Resolve the key columns for a DUPLICATE_KEY rule.

    Uses the rule's configured column(s) when present (comma-separated for a
    composite key), otherwise the table's registered primary key.
    """
    configured = (rule or {}).get("column_name")
    if configured:
        return [c.strip() for c in str(configured).split(",") if c.strip()]
    return [c for c in (primary_key_columns or []) if c]


def reason(rule_type, column_name=None):
    """Stable, readable failure reason token for a rejected row/rule."""
    return f"{rule_type}:{column_name}" if column_name else rule_type
