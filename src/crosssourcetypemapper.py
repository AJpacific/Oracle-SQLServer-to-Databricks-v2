"""
crosssourcetypemapper.py - deterministic Oracle/SQL Server -> Databricks mapping.

The mapper selects source-specific rules by dialect. Oracle retains its special
NUMBER(p,s) resolution, while SQL Server decimal/numeric uses independent
precision/scale logic and timestamp/rowversion remain binary.

Statuses:  AUTO | REVIEW | BLOCKED
Fidelity:  EXACT | WIDENED | LOSSY | UNKNOWN

Pure module: no Spark, no dbutils. Fully unit-testable.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional


# Max precision Delta DECIMAL supports.
MAX_DELTA_DECIMAL_PRECISION = 38


@dataclass
class ColumnMappingResult:
    source_type: str
    databricks_delta_type: Optional[str]
    status: str            # AUTO | REVIEW | BLOCKED
    fidelity: str          # EXACT | WIDENED | LOSSY | UNKNOWN
    notes: str
    is_nullable: bool


# Built-in fallback rules used when no YAML file is available. Keys are the
# Oracle data_type as reported by ALL_TAB_COLUMNS (upper case, family stripped).
_BUILTIN_RULES = {
    # character
    "VARCHAR2":  ("STRING", "AUTO", "EXACT", ""),
    "NVARCHAR2": ("STRING", "AUTO", "EXACT", ""),
    "CHAR":      ("STRING", "AUTO", "EXACT", ""),
    "NCHAR":     ("STRING", "AUTO", "EXACT", ""),
    "CLOB":      ("STRING", "AUTO", "WIDENED",
                  "CLOB materialized as Delta STRING; validate JDBC LOB handling and payload size"),
    "NCLOB":     ("STRING", "AUTO", "WIDENED",
                  "NCLOB materialized as Delta STRING; validate Unicode round-trip, JDBC LOB handling, payload size"),
    "LONG":      ("STRING", "REVIEW", "LOSSY",
                  "deprecated LONG type; only one per table, review before migrating"),
    # binary floating point
    "BINARY_FLOAT":  ("FLOAT", "AUTO", "EXACT", ""),
    "BINARY_DOUBLE": ("DOUBLE", "AUTO", "EXACT", ""),
    "FLOAT":         ("DOUBLE", "AUTO", "WIDENED",
                      "Oracle FLOAT is binary-precision NUMBER; widened to DOUBLE"),
    # datetime
    "DATE":      ("TIMESTAMP", "AUTO", "WIDENED",
                  "Oracle DATE carries a time component; mapped to Delta TIMESTAMP"),
    "TIMESTAMP": ("TIMESTAMP", "AUTO", "EXACT", ""),
    "TIMESTAMP WITH TIME ZONE": ("TIMESTAMP", "REVIEW", "LOSSY",
                  "timezone offset dropped when converted to Delta TIMESTAMP; confirm policy"),
    "TIMESTAMP WITH LOCAL TIME ZONE": ("TIMESTAMP", "REVIEW", "LOSSY",
                  "session-local timezone semantics lost; confirm policy"),
    "INTERVAL YEAR TO MONTH": ("STRING", "REVIEW", "LOSSY",
                  "no Delta interval type; retained as STRING"),
    "INTERVAL DAY TO SECOND": ("STRING", "REVIEW", "LOSSY",
                  "no Delta interval type; retained as STRING"),
    # binary
    "RAW":       ("BINARY", "AUTO", "EXACT", ""),
    "LONG RAW":  ("BINARY", "REVIEW", "LOSSY",
                  "deprecated LONG RAW; review before migrating"),
    "BLOB":      ("BINARY", "AUTO", "WIDENED",
                  "BLOB materialized as Delta BINARY; validate JDBC LOB streaming and payload size"),
    "BFILE":     ("STRING", "BLOCKED", "UNKNOWN",
                  "BFILE points to an external OS file; cannot be auto-migrated"),
    # identifiers / semi-structured
    "ROWID":     ("STRING", "REVIEW", "LOSSY", "physical ROWID serialized to STRING"),
    "UROWID":    ("STRING", "REVIEW", "LOSSY", "universal ROWID serialized to STRING"),
    "XMLTYPE":   ("STRING", "REVIEW", "LOSSY", "XMLTYPE serialized to STRING"),
    "SDO_GEOMETRY": ("BINARY", "BLOCKED", "UNKNOWN",
                  "spatial type needs an explicit approved serialization strategy"),
    "ANYDATA":   ("STRING", "BLOCKED", "UNKNOWN",
                  "ANYDATA stores heterogeneous types; cannot be safely auto-mapped"),
    # ---- Oracle 23ai / 26ai ----
    "BOOLEAN":   ("BOOLEAN", "AUTO", "EXACT",
                  "Oracle 23ai+ native BOOLEAN; subject to a live JDBC round-trip check"),
    # Native JSON: STRING is the *target representation only*. It stays
    # REVIEW/LOSSY because generic SELECT * extraction does not JSON_SERIALIZE;
    # a JSON column must be explicitly serialized and round-tripped before AUTO.
    "JSON":      ("STRING", "REVIEW", "LOSSY",
                  "native Oracle JSON; target representation only - requires explicit "
                  "serialization and a live JDBC round-trip before AUTO"),
    "VECTOR":    (None, "BLOCKED", "UNKNOWN",
                  "Oracle 23ai/26ai VECTOR; blocked until an approved representation "
                  "preserves dimensions, element format, and dense/sparse semantics"),
}


def _family(source_type: str) -> str:
    """Normalize an Oracle data_type to a rule key.

    ALL_TAB_COLUMNS reports things like 'TIMESTAMP(6)' as 'TIMESTAMP' and
    'TIMESTAMP(6) WITH TIME ZONE' with the scale embedded. We strip any
    parenthesised precision but keep the WITH TIME ZONE / LOCAL suffix, and
    unify underscores with spaces so 'BINARY_FLOAT' and 'BINARY FLOAT' (and
    'SDO_GEOMETRY' / 'SDO GEOMETRY') resolve to the same key.
    """
    t = (source_type or "").strip().upper()
    # Remove a fractional-seconds precision like TIMESTAMP(6) -> TIMESTAMP
    # while preserving trailing words such as WITH TIME ZONE.
    import re
    t = re.sub(r"\((\s*\d+\s*)\)", "", t)
    t = t.replace("_", " ")
    t = re.sub(r"\s+", " ", t).strip()
    return t


# Normalize builtin keys through the same rule normalization as YAML keys so
# underscore/space variants resolve identically.
_BUILTIN_RULES = {_family(_k): _v for _k, _v in _BUILTIN_RULES.items()}


def _family_sqlserver(source_type: str) -> str:
    """Normalize a SQL Server data_type to a rule key.

    SQL Server metadata reports bare type names (``int``, ``varchar``,
    ``decimal``) with length/precision/scale held separately, but a caller may
    still pass ``decimal(10,2)`` or ``varchar(max)``; we strip any parenthesised
    part and lower-case so keys resolve identically to the YAML/builtin rules.
    """
    t = (source_type or "").strip().lower()
    t = re.sub(r"\(.*?\)", "", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t


# Built-in SQL Server fallback rules used when no YAML file is available. Keys
# are the SQL Server type name (lower case). decimal/numeric are resolved in code
# (precision/scale), so they are intentionally absent here.
_SQLSERVER_BUILTIN_RULES = {
    # integer / boolean
    "bit":       ("BOOLEAN", "AUTO", "EXACT", ""),
    "tinyint":   ("SMALLINT", "AUTO", "WIDENED",
                  "SQL Server tinyint is unsigned 0-255; widened to signed SMALLINT"),
    "smallint":  ("SMALLINT", "AUTO", "EXACT", ""),
    "int":       ("INT", "AUTO", "EXACT", ""),
    "bigint":    ("BIGINT", "AUTO", "EXACT", ""),
    # money / approximate numeric
    "money":      ("DECIMAL(19,4)", "AUTO", "EXACT", ""),
    "smallmoney": ("DECIMAL(10,4)", "AUTO", "EXACT", ""),
    "real":       ("FLOAT", "AUTO", "EXACT", ""),
    "float":      ("DOUBLE", "AUTO", "WIDENED",
                   "SQL Server float(n) mapped to Delta DOUBLE"),
    # character
    "char":     ("STRING", "AUTO", "EXACT", ""),
    "varchar":  ("STRING", "AUTO", "EXACT", ""),
    "nchar":    ("STRING", "AUTO", "EXACT", ""),
    "nvarchar": ("STRING", "AUTO", "EXACT", ""),
    "text":     ("STRING", "REVIEW", "LOSSY",
                 "deprecated SQL Server text type; review before migrating"),
    "ntext":    ("STRING", "REVIEW", "LOSSY",
                 "deprecated SQL Server ntext type; review before migrating"),
    # binary
    "binary":     ("BINARY", "AUTO", "EXACT", ""),
    "varbinary":  ("BINARY", "AUTO", "EXACT", ""),
    "image":      ("BINARY", "REVIEW", "LOSSY",
                   "deprecated SQL Server image type; review before migrating"),
    # rowversion is a binary change token, NEVER a datetime.
    "timestamp":  ("BINARY", "AUTO", "EXACT",
                   "SQL Server timestamp is rowversion (an 8-byte binary change "
                   "token), NOT a datetime; mapped to BINARY"),
    "rowversion": ("BINARY", "AUTO", "EXACT",
                   "SQL Server rowversion is an 8-byte binary change token; "
                   "mapped to BINARY"),
    # identifiers / semi-structured
    "uniqueidentifier": ("STRING", "AUTO", "WIDENED",
                         "GUID serialized to canonical STRING form"),
    "xml":  ("STRING", "REVIEW", "LOSSY",
             "SQL Server XML serialized to STRING; confirm fidelity/schema policy"),
    # temporal
    "date":           ("DATE", "AUTO", "EXACT", ""),
    "datetime":       ("TIMESTAMP", "AUTO", "WIDENED",
                       "SQL Server datetime (~3.33ms resolution) widened to Delta TIMESTAMP"),
    "smalldatetime":  ("TIMESTAMP", "AUTO", "WIDENED",
                       "SQL Server smalldatetime (minute resolution) widened to Delta TIMESTAMP"),
    "datetime2":      ("TIMESTAMP", "AUTO", "LOSSY",
                       "AUTO policy: normalized to microsecond precision (6 fractional digits); "
                       "a datetime2(7) source loses the seventh digit"),
    "datetimeoffset": ("TIMESTAMP", "REVIEW", "LOSSY",
                       "timezone offset dropped when converted to Delta TIMESTAMP; confirm policy"),
    "time":           ("STRING", "REVIEW", "LOSSY",
                       "no Delta time-of-day type; retained as STRING - confirm representation"),
    # types that require an explicit approved policy - never silently mapped
    "sql_variant":  (None, "BLOCKED", "UNKNOWN",
                     "sql_variant stores heterogeneous types; cannot be safely auto-mapped"),
    "hierarchyid":  (None, "BLOCKED", "UNKNOWN",
                     "hierarchyid is a CLR type; needs an explicit approved representation"),
    "geometry":     (None, "BLOCKED", "UNKNOWN",
                     "spatial geometry needs an explicit approved serialization strategy"),
    "geography":    (None, "BLOCKED", "UNKNOWN",
                     "spatial geography needs an explicit approved serialization strategy"),
    "cursor":       (None, "BLOCKED", "UNKNOWN",
                     "cursor is not a storable column type"),
    "table":        (None, "BLOCKED", "UNKNOWN",
                     "table type is not a storable column type"),
}
_SQLSERVER_BUILTIN_RULES = {
    _family_sqlserver(_k): _v for _k, _v in _SQLSERVER_BUILTIN_RULES.items()
}


class CrossSourceTypeMapper:
    def __init__(self, rules: Optional[dict] = None, dialect: str = "oracle"):
        # rules: dict keyed by family -> {databricks_delta, status, fidelity, notes}
        self._rules = rules or {}
        self._dialect = (dialect or "oracle").strip().lower()
        self._builtin = (
            _SQLSERVER_BUILTIN_RULES if self._dialect == "sqlserver"
            else _BUILTIN_RULES
        )

    # ------------------------------------------------------------------ loaders
    @classmethod
    def from_yaml_path(cls, path: str) -> "CrossSourceTypeMapper":
        import yaml
        with open(path, "r", encoding="utf-8") as fh:
            doc = yaml.safe_load(fh) or {}
        dialect = (doc.get("source_dialect") or "oracle").strip().lower()
        # SQL Server synonyms in the YAML normalize to the canonical token.
        if dialect in ("sql_server", "mssql", "sqlserver"):
            dialect = "sqlserver"
        key_norm = _family_sqlserver if dialect == "sqlserver" else _family
        raw = doc.get("types", {}) or {}
        rules = {}
        for k, v in raw.items():
            rules[key_norm(k)] = {
                "databricks_delta": v.get("databricks_delta"),
                "status": (v.get("status") or "AUTO").upper(),
                "fidelity": (v.get("fidelity") or "EXACT").upper(),
                "notes": v.get("notes") or "",
            }
        return cls(rules=rules, dialect=dialect)

    def _lookup(self, family: str):
        if family in self._rules:
            r = self._rules[family]
            return (r["databricks_delta"], r["status"], r["fidelity"], r["notes"])
        if family in self._builtin:
            return self._builtin[family]
        return None

    # ------------------------------------------------------------------ mapping
    def map_column(self, source_type, precision=None, scale=None,
                   length=None, is_nullable=True) -> ColumnMappingResult:
        if self._dialect == "sqlserver":
            return self._map_sqlserver(source_type, precision, scale,
                                       length, is_nullable)
        return self._map_oracle(source_type, precision, scale, length, is_nullable)

    def _map_oracle(self, source_type, precision, scale,
                    length, is_nullable) -> ColumnMappingResult:
        family = _family(source_type)

        # Oracle NUMBER needs precision/scale-driven resolution.
        if family == "NUMBER":
            return self._map_number(source_type, precision, scale, is_nullable)

        rule = self._lookup(family)
        if rule is None:
            # Unknown / user-defined Oracle types (object types, VARRAYs, nested
            # tables, REF, application-specific names) are never silently
            # converted to STRING or marked AUTO - they are blocked for review.
            return ColumnMappingResult(
                source_type=source_type or "",
                databricks_delta_type=None,
                status="BLOCKED",
                fidelity="UNKNOWN",
                notes=("Unsupported Oracle type requiring "
                       f"an explicit mapping: {source_type}"),
                is_nullable=bool(is_nullable),
            )

        dtype, status, fidelity, notes = rule
        return ColumnMappingResult(
            source_type=source_type or "",
            databricks_delta_type=dtype,
            status=status,
            fidelity=fidelity,
            notes=notes,
            is_nullable=bool(is_nullable),
        )

    # ------------------------------------------------------- SQL Server mapping
    def _map_sqlserver(self, source_type, precision, scale,
                       length, is_nullable) -> ColumnMappingResult:
        family = _family_sqlserver(source_type)

        # decimal / numeric are resolved from precision/scale, never via a flat
        # rule and never via the Oracle NUMBER path.
        if family in ("decimal", "numeric"):
            return self._map_sqlserver_decimal(source_type, precision, scale,
                                               is_nullable)

        rule = self._lookup(family)
        if rule is None:
            # Unknown / user-defined SQL Server types (CLR UDTs, aliased types,
            # typed XML schemas, application-specific names) are never silently
            # converted to STRING or marked AUTO - they are blocked for review.
            return ColumnMappingResult(
                source_type=source_type or "",
                databricks_delta_type=None,
                status="BLOCKED",
                fidelity="UNKNOWN",
                notes=("Unsupported SQL Server type requiring "
                       f"an explicit mapping: {source_type}"),
                is_nullable=bool(is_nullable),
            )

        dtype, status, fidelity, notes = rule
        return ColumnMappingResult(
            source_type=source_type or "",
            databricks_delta_type=dtype,
            status=status,
            fidelity=fidelity,
            notes=notes,
            is_nullable=bool(is_nullable),
        )

    def _map_sqlserver_decimal(self, source_type, precision, scale, is_nullable):
        # SQL Server decimal/numeric default to (18,0) when precision is absent.
        p = precision if precision is not None else 18
        s = scale if scale is not None else 0
        try:
            p = int(p)
            s = int(s)
        except (TypeError, ValueError):
            return ColumnMappingResult(
                source_type=source_type or "DECIMAL",
                databricks_delta_type=None,
                status="BLOCKED",
                fidelity="UNKNOWN",
                notes=f"decimal/numeric with non-integer precision/scale "
                      f"({precision!r},{scale!r})",
                is_nullable=bool(is_nullable),
            )
        if p > MAX_DELTA_DECIMAL_PRECISION:
            return ColumnMappingResult(
                source_type=source_type or "DECIMAL",
                databricks_delta_type=None,
                status="BLOCKED",
                fidelity="UNKNOWN",
                notes=f"DECIMAL({p},{s}) exceeds Delta DECIMAL precision 38",
                is_nullable=bool(is_nullable),
            )
        notes = ""
        if s < 0:
            s = 0
            notes = "negative scale coerced to 0"
        if s > p:
            s = p
            notes = f"scale > precision; clamped scale to {s}"
        return ColumnMappingResult(
            source_type=source_type or "DECIMAL",
            databricks_delta_type=f"DECIMAL({p},{s})",
            status="AUTO",
            fidelity="EXACT",
            notes=notes,
            is_nullable=bool(is_nullable),
        )

    def _map_number(self, source_type, precision, scale, is_nullable):
        p = precision
        s = scale
        notes = ""
        # Unconstrained NUMBER: both Oracle metadata values absent. Treated as
        # an approved whole-number column under the accelerator's policy.
        if precision is None and scale is None:
            return ColumnMappingResult(
                source_type=source_type or "NUMBER",
                databricks_delta_type="DECIMAL(38,0)",
                status="AUTO",
                fidelity="EXACT",
                notes=(
                    "Unconstrained Oracle NUMBER mapped using "
                    "the approved whole-number policy to "
                    "DECIMAL(38,0)"
                ),
                is_nullable=bool(is_nullable),
            )
        # Precision absent but scale present: ambiguous partial metadata, not an
        # unconstrained NUMBER. Keep the prior safe REVIEW mapping.
        if p is None:
            return ColumnMappingResult(
                source_type=source_type or "NUMBER",
                databricks_delta_type="DECIMAL(38,10)",
                status="REVIEW",
                fidelity="WIDENED",
                notes="NUMBER with scale but no precision; clamped to "
                      "DECIMAL(38,10) - review magnitude/scale",
                is_nullable=bool(is_nullable),
            )

        # Integer-like: scale 0 (or None treated as 0 when precision present).
        if s is None or s == 0:
            if p <= 4:
                dtype, fidelity = "SMALLINT", "EXACT"
            elif p <= 9:
                dtype, fidelity = "INT", "EXACT"
            elif p <= 18:
                dtype, fidelity = "BIGINT", "EXACT"
            elif p <= MAX_DELTA_DECIMAL_PRECISION:
                dtype, fidelity = f"DECIMAL({p},0)", "EXACT"
            else:
                return ColumnMappingResult(
                    source_type=source_type or "NUMBER",
                    databricks_delta_type="STRING",
                    status="BLOCKED",
                    fidelity="UNKNOWN",
                    notes=f"NUMBER({p},0) exceeds Delta DECIMAL precision 38",
                    is_nullable=bool(is_nullable),
                )
            return ColumnMappingResult(
                source_type=source_type or "NUMBER",
                databricks_delta_type=dtype,
                status="AUTO",
                fidelity=fidelity,
                notes=notes,
                is_nullable=bool(is_nullable),
            )

        # Fixed-point decimal.
        if p > MAX_DELTA_DECIMAL_PRECISION:
            return ColumnMappingResult(
                source_type=source_type or "NUMBER",
                databricks_delta_type="STRING",
                status="BLOCKED",
                fidelity="UNKNOWN",
                notes=f"NUMBER({p},{s}) exceeds Delta DECIMAL precision 38",
                is_nullable=bool(is_nullable),
            )
        # Guard against scale > precision (negative-scale NUMBERs etc.).
        eff_scale = s if s is not None else 0
        if eff_scale < 0:
            # negative scale rounds to left of decimal point; widen safely.
            return ColumnMappingResult(
                source_type=source_type or "NUMBER",
                databricks_delta_type=f"DECIMAL({min(p - eff_scale, 38)},0)",
                status="REVIEW",
                fidelity="WIDENED",
                notes=f"NUMBER({p},{s}) has negative scale; widened - review rounding",
                is_nullable=bool(is_nullable),
            )
        if eff_scale > p:
            eff_scale = p
            notes = f"scale {s} > precision {p}; clamped scale to {eff_scale}"
        return ColumnMappingResult(
            source_type=source_type or "NUMBER",
            databricks_delta_type=f"DECIMAL({p},{eff_scale})",
            status="AUTO",
            fidelity="EXACT",
            notes=notes,
            is_nullable=bool(is_nullable),
        )
