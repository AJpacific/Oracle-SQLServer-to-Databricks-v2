"""SQL Server source datatype mappings for Databricks Delta."""

from __future__ import annotations

import re

try:
    from src.source_identity import normalize_source_system
    from src.type_mappers.base import (
        AUTO, BLOCKED, EXACT, LOSSY, REVIEW, UNKNOWN, WIDENED,
        ColumnMappingResult, SourceTypeMapper, load_rules_from_yaml,
        validate_rule_overrides,
    )
except ModuleNotFoundError:
    from source_identity import normalize_source_system
    from type_mappers.base import (
        AUTO, BLOCKED, EXACT, LOSSY, REVIEW, UNKNOWN, WIDENED,
        ColumnMappingResult, SourceTypeMapper, load_rules_from_yaml,
        validate_rule_overrides,
    )


MAX_DELTA_DECIMAL_PRECISION = 38


def _family(source_type):
    """Normalize a SQL Server datatype to its mapper rule family."""
    token = (source_type or "").strip().lower()
    token = re.sub(r"\(.*?\)", "", token)
    return re.sub(r"\s+", " ", token).strip()


_BUILTIN_RULES = {
    "bit": ("BOOLEAN", AUTO, EXACT, ""),
    "tinyint": ("SMALLINT", AUTO, WIDENED,
                "SQL Server tinyint is unsigned 0-255; widened to signed SMALLINT"),
    "smallint": ("SMALLINT", AUTO, EXACT, ""),
    "int": ("INT", AUTO, EXACT, ""),
    "bigint": ("BIGINT", AUTO, EXACT, ""),
    "money": ("DECIMAL(19,4)", AUTO, EXACT, ""),
    "smallmoney": ("DECIMAL(10,4)", AUTO, EXACT, ""),
    "real": ("FLOAT", AUTO, EXACT, ""),
    "float": ("DOUBLE", AUTO, WIDENED,
              "SQL Server float(n) mapped to Delta DOUBLE"),
    "char": ("STRING", AUTO, EXACT, ""),
    "varchar": ("STRING", AUTO, EXACT, ""),
    "nchar": ("STRING", AUTO, EXACT, ""),
    "nvarchar": ("STRING", AUTO, EXACT, ""),
    "text": ("STRING", REVIEW, LOSSY,
             "deprecated SQL Server text type; review before migrating"),
    "ntext": ("STRING", REVIEW, LOSSY,
              "deprecated SQL Server ntext type; review before migrating"),
    "binary": ("BINARY", AUTO, EXACT, ""),
    "varbinary": ("BINARY", AUTO, EXACT, ""),
    "image": ("BINARY", REVIEW, LOSSY,
              "deprecated SQL Server image type; review before migrating"),
    "timestamp": ("BINARY", AUTO, EXACT,
                  "SQL Server timestamp is rowversion (an 8-byte binary change "
                  "token), NOT a datetime; mapped to BINARY"),
    "rowversion": ("BINARY", AUTO, EXACT,
                   "SQL Server rowversion is an 8-byte binary change token; "
                   "mapped to BINARY"),
    "uniqueidentifier": ("STRING", AUTO, WIDENED,
                         "GUID serialized to canonical STRING form"),
    "xml": ("STRING", REVIEW, LOSSY,
            "SQL Server XML serialized to STRING; confirm fidelity/schema policy"),
    "date": ("DATE", AUTO, EXACT, ""),
    "datetime": ("TIMESTAMP", AUTO, WIDENED,
                 "SQL Server datetime (~3.33ms resolution) widened to Delta TIMESTAMP"),
    "smalldatetime": ("TIMESTAMP", AUTO, WIDENED,
                      "SQL Server smalldatetime (minute resolution) widened to Delta TIMESTAMP"),
    "datetime2": ("TIMESTAMP", AUTO, LOSSY,
                  "AUTO policy: normalized to microsecond precision (6 fractional digits); "
                  "a datetime2(7) source loses the seventh digit"),
    "datetimeoffset": ("TIMESTAMP", REVIEW, LOSSY,
                       "timezone offset dropped when converted to Delta TIMESTAMP; confirm policy"),
    "time": ("STRING", REVIEW, LOSSY,
             "no Delta time-of-day type; retained as STRING - confirm representation"),
    "sql_variant": (None, BLOCKED, UNKNOWN,
                    "sql_variant stores heterogeneous types; cannot be safely auto-mapped"),
    "hierarchyid": (None, BLOCKED, UNKNOWN,
                    "hierarchyid is a CLR type; needs an explicit approved representation"),
    "geometry": (None, BLOCKED, UNKNOWN,
                 "spatial geometry needs an explicit approved serialization strategy"),
    "geography": (None, BLOCKED, UNKNOWN,
                  "spatial geography needs an explicit approved serialization strategy"),
    "cursor": (None, BLOCKED, UNKNOWN,
               "cursor is not a storable column type"),
    "table": (None, BLOCKED, UNKNOWN,
              "table type is not a storable column type"),
}
_BUILTIN_RULES = {_family(key): value
                  for key, value in _BUILTIN_RULES.items()}


class SqlServerTypeMapper(SourceTypeMapper):
    def __init__(self, rules=None):
        self._rules = validate_rule_overrides(rules or {}, _family)

    @classmethod
    def from_yaml_path(cls, path):
        rules = load_rules_from_yaml(
            path, "sqlserver", normalize_source_system, _family)
        return cls(rules=rules)

    def _lookup(self, family):
        if family in self._rules:
            rule = self._rules[family]
            return (rule["databricks_delta"], rule["status"],
                    rule["fidelity"], rule["notes"])
        return _BUILTIN_RULES.get(family)

    def map_column(self, source_type, precision=None, scale=None,
                   length=None, is_nullable=True):
        family = _family(source_type)
        if family in ("decimal", "numeric"):
            return self._map_decimal(
                source_type, precision, scale, is_nullable)

        rule = self._lookup(family)
        if rule is None:
            return ColumnMappingResult(
                source_type=source_type or "", databricks_delta_type=None,
                status=BLOCKED, fidelity=UNKNOWN,
                notes=("Unsupported SQL Server type requiring "
                       f"an explicit mapping: {source_type}"),
                is_nullable=bool(is_nullable))
        datatype, status, fidelity, notes = rule
        return ColumnMappingResult(
            source_type=source_type or "", databricks_delta_type=datatype,
            status=status, fidelity=fidelity, notes=notes,
            is_nullable=bool(is_nullable))

    def _map_decimal(self, source_type, precision, scale, is_nullable):
        precision_value = precision if precision is not None else 18
        scale_value = scale if scale is not None else 0
        try:
            precision_value = int(precision_value)
            scale_value = int(scale_value)
        except (TypeError, ValueError):
            return ColumnMappingResult(
                source_type=source_type or "DECIMAL",
                databricks_delta_type=None, status=BLOCKED,
                fidelity=UNKNOWN,
                notes=("decimal/numeric with non-integer precision/scale "
                       f"({precision!r},{scale!r})"),
                is_nullable=bool(is_nullable))
        if precision_value > MAX_DELTA_DECIMAL_PRECISION:
            return ColumnMappingResult(
                source_type=source_type or "DECIMAL",
                databricks_delta_type=None, status=BLOCKED,
                fidelity=UNKNOWN,
                notes=(f"DECIMAL({precision_value},{scale_value}) exceeds "
                       "Delta DECIMAL precision 38"),
                is_nullable=bool(is_nullable))
        notes = ""
        if scale_value < 0:
            scale_value = 0
            notes = "negative scale coerced to 0"
        if scale_value > precision_value:
            scale_value = precision_value
            notes = f"scale > precision; clamped scale to {scale_value}"
        return ColumnMappingResult(
            source_type=source_type or "DECIMAL",
            databricks_delta_type=(
                f"DECIMAL({precision_value},{scale_value})"),
            status=AUTO, fidelity=EXACT, notes=notes,
            is_nullable=bool(is_nullable))