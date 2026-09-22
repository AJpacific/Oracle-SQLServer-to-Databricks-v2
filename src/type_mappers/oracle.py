"""Oracle source datatype mappings for Databricks Delta."""

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
    """Normalize an Oracle datatype to its mapper rule family."""
    token = (source_type or "").strip().upper()
    token = re.sub(r"\((\s*\d+\s*)\)", "", token)
    token = token.replace("_", " ")
    return re.sub(r"\s+", " ", token).strip()


_BUILTIN_RULES = {
    "VARCHAR2": ("STRING", AUTO, EXACT, ""),
    "NVARCHAR2": ("STRING", AUTO, EXACT, ""),
    "CHAR": ("STRING", AUTO, EXACT, ""),
    "NCHAR": ("STRING", AUTO, EXACT, ""),
    "CLOB": ("STRING", AUTO, WIDENED,
             "CLOB materialized as Delta STRING; validate JDBC LOB handling and payload size"),
    "NCLOB": ("STRING", AUTO, WIDENED,
              "NCLOB materialized as Delta STRING; validate Unicode round-trip, JDBC LOB handling, payload size"),
    "LONG": ("STRING", REVIEW, LOSSY,
             "Deprecated Oracle LONG type; manual review required because extraction and size restrictions differ from standard character types"),
    "BINARY_FLOAT": ("FLOAT", AUTO, EXACT, ""),
    "BINARY_DOUBLE": ("DOUBLE", AUTO, EXACT, ""),
    "FLOAT": ("DOUBLE", AUTO, WIDENED,
              "Oracle FLOAT is binary-precision NUMBER; widened to DOUBLE"),
    "DATE": ("TIMESTAMP", AUTO, WIDENED,
             "Oracle DATE stores date and time to second precision; mapped to Databricks TIMESTAMP"),
    "TIMESTAMP": ("TIMESTAMP", REVIEW, UNKNOWN,
                  "Fallback mapping used when Oracle TIMESTAMP fractional-second precision is unavailable; precision-aware mapping is implemented in OracleTypeMapper"),
    "TIMESTAMP WITH TIME ZONE": ("TIMESTAMP", REVIEW, LOSSY,
                                 "Oracle TIMESTAMP WITH TIME ZONE contains timezone or region semantics that are not fully preserved by Databricks TIMESTAMP; explicit normalization policy required"),
    "TIMESTAMP WITH LOCAL TIME ZONE": ("TIMESTAMP", REVIEW, LOSSY,
                                       "Oracle TIMESTAMP WITH LOCAL TIME ZONE uses database and session timezone semantics; explicit normalization policy required before automated migration"),
    "INTERVAL YEAR TO MONTH": ("STRING", REVIEW, LOSSY,
                               "Oracle INTERVAL YEAR TO MONTH retained as STRING pending an approved native interval round-trip policy"),
    "INTERVAL DAY TO SECOND": ("STRING", REVIEW, LOSSY,
                               "Oracle INTERVAL DAY TO SECOND retained as STRING pending an approved native interval round-trip policy"),
    "RAW": ("BINARY", AUTO, EXACT, ""),
    "LONG RAW": ("BINARY", REVIEW, LOSSY,
                 "deprecated LONG RAW; review before migrating"),
    "BLOB": ("BINARY", AUTO, WIDENED,
             "BLOB materialized as Delta BINARY; validate JDBC LOB streaming and payload size"),
    "BFILE": ("STRING", BLOCKED, UNKNOWN,
              "BFILE points to an external OS file; cannot be auto-migrated"),
    "ROWID": ("STRING", REVIEW, LOSSY,
              "physical ROWID serialized to STRING"),
    "UROWID": ("STRING", REVIEW, LOSSY,
               "universal ROWID serialized to STRING"),
    "XMLTYPE": ("STRING", REVIEW, LOSSY, "XMLTYPE serialized to STRING"),
    "SDO_GEOMETRY": ("BINARY", BLOCKED, UNKNOWN,
                     "spatial type needs an explicit approved serialization strategy"),
    "ANYDATA": ("STRING", BLOCKED, UNKNOWN,
                "ANYDATA stores heterogeneous types; cannot be safely auto-mapped"),
    "BOOLEAN": ("BOOLEAN", AUTO, EXACT,
                "Oracle 23ai+ native BOOLEAN; subject to a live JDBC round-trip check"),
    "JSON": ("STRING", REVIEW, LOSSY,
             "native Oracle JSON; target representation only - requires explicit "
             "serialization and a live JDBC round-trip before AUTO"),
    "VECTOR": (None, BLOCKED, UNKNOWN,
               "Oracle 23ai/26ai VECTOR; blocked until an approved representation "
               "preserves dimensions, element format, and dense/sparse semantics"),
}
_BUILTIN_RULES = {_family(key): value
                  for key, value in _BUILTIN_RULES.items()}


class OracleTypeMapper(SourceTypeMapper):
    def __init__(self, rules=None):
        self._rules = validate_rule_overrides(rules or {}, _family)

    @classmethod
    def from_yaml_path(cls, path):
        rules = load_rules_from_yaml(
            path, "oracle", normalize_source_system, _family)
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
        if family == "NUMBER":
            return self._map_number(
                source_type, precision, scale, is_nullable)
        if family == "TIMESTAMP":
            effective_precision = scale
            if effective_precision is None and source_type:
                m = re.search(r"\((.*?)\)", source_type)
                if m:
                    effective_precision = m.group(1).strip()
            return self._map_timestamp(
                source_type, effective_precision, is_nullable)

        rule = self._lookup(family)
        if rule is None:
            return ColumnMappingResult(
                source_type=source_type or "", databricks_delta_type=None,
                status=BLOCKED, fidelity=UNKNOWN,
                notes=("Unsupported Oracle type requiring "
                       f"an explicit mapping: {source_type}"),
                is_nullable=bool(is_nullable))
        datatype, status, fidelity, notes = rule
        return ColumnMappingResult(
            source_type=source_type or "", databricks_delta_type=datatype,
            status=status, fidelity=fidelity, notes=notes,
            is_nullable=bool(is_nullable))

    def _map_timestamp(
        self,
        source_type,
        fractional_precision,
        is_nullable,
    ):
        if fractional_precision is None:
            return ColumnMappingResult(
                source_type=source_type or "TIMESTAMP",
                databricks_delta_type="TIMESTAMP",
                status=REVIEW,
                fidelity=UNKNOWN,
                notes=(
                    "Oracle TIMESTAMP fractional-second precision is "
                    "unavailable; mapped to Databricks TIMESTAMP for "
                    "manual review"
                ),
                is_nullable=bool(is_nullable),
            )

        try:
            precision_value = int(fractional_precision)
        except (TypeError, ValueError):
            return ColumnMappingResult(
                source_type=source_type or "TIMESTAMP",
                databricks_delta_type="TIMESTAMP",
                status=REVIEW,
                fidelity=UNKNOWN,
                notes=(
                    f"Invalid Oracle TIMESTAMP fractional-second precision "
                    f"{fractional_precision!r}; mapped to Databricks "
                    "TIMESTAMP for manual review"
                ),
                is_nullable=bool(is_nullable),
            )

        if 0 <= precision_value <= 6:
            return ColumnMappingResult(
                source_type=source_type or "TIMESTAMP",
                databricks_delta_type="TIMESTAMP",
                status=AUTO,
                fidelity=EXACT,
                notes=(
                    f"Oracle TIMESTAMP({precision_value}) mapped to "
                    "Databricks TIMESTAMP with compatible fractional "
                    "precision"
                ),
                is_nullable=bool(is_nullable),
            )

        if 7 <= precision_value <= 9:
            return ColumnMappingResult(
                source_type=source_type or "TIMESTAMP",
                databricks_delta_type="TIMESTAMP",
                status=REVIEW,
                fidelity=LOSSY,
                notes=(
                    f"Oracle TIMESTAMP({precision_value}) mapped to "
                    "Databricks TIMESTAMP; fractional precision above "
                    "six digits may be truncated"
                ),
                is_nullable=bool(is_nullable),
            )

        return ColumnMappingResult(
            source_type=source_type or "TIMESTAMP",
            databricks_delta_type="TIMESTAMP",
            status=REVIEW,
            fidelity=UNKNOWN,
            notes=(
                f"Oracle TIMESTAMP fractional-second precision "
                f"{precision_value} is outside the expected Oracle "
                "range 0 through 9; mapped to Databricks TIMESTAMP "
                "for manual review"
            ),
            is_nullable=bool(is_nullable),
        )

    def _map_number(self, source_type, precision, scale, is_nullable):
        precision_value = precision
        scale_value = scale
        notes = ""
        if precision is None and scale is None:
            return ColumnMappingResult(
                source_type=source_type or "NUMBER",
                databricks_delta_type="DECIMAL(38,0)", status=AUTO,
                fidelity=EXACT,
                notes=("Unconstrained Oracle NUMBER mapped using "
                       "the approved whole-number policy to DECIMAL(38,0)"),
                is_nullable=bool(is_nullable))
        if precision_value is None:
            return ColumnMappingResult(
                source_type=source_type or "NUMBER",
                databricks_delta_type="DECIMAL(38,10)", status=REVIEW,
                fidelity=WIDENED,
                notes=("NUMBER with scale but no precision; clamped to "
                       "DECIMAL(38,10) - review magnitude/scale"),
                is_nullable=bool(is_nullable))

        if scale_value is None or scale_value == 0:
            if precision_value <= 4:
                datatype, fidelity = "SMALLINT", EXACT
            elif precision_value <= 9:
                datatype, fidelity = "INT", EXACT
            elif precision_value <= 18:
                datatype, fidelity = "BIGINT", EXACT
            elif precision_value <= MAX_DELTA_DECIMAL_PRECISION:
                datatype, fidelity = f"DECIMAL({precision_value},0)", EXACT
            else:
                return ColumnMappingResult(
                    source_type=source_type or "NUMBER",
                    databricks_delta_type="STRING", status=BLOCKED,
                    fidelity=UNKNOWN,
                    notes=(f"NUMBER({precision_value},0) exceeds Delta "
                           "DECIMAL precision 38"),
                    is_nullable=bool(is_nullable))
            return ColumnMappingResult(
                source_type=source_type or "NUMBER",
                databricks_delta_type=datatype, status=AUTO,
                fidelity=fidelity, notes=notes,
                is_nullable=bool(is_nullable))

        if precision_value > MAX_DELTA_DECIMAL_PRECISION:
            return ColumnMappingResult(
                source_type=source_type or "NUMBER",
                databricks_delta_type="STRING", status=BLOCKED,
                fidelity=UNKNOWN,
                notes=(f"NUMBER({precision_value},{scale_value}) exceeds "
                       "Delta DECIMAL precision 38"),
                is_nullable=bool(is_nullable))
        effective_scale = scale_value if scale_value is not None else 0
        if effective_scale < 0:
            return ColumnMappingResult(
                source_type=source_type or "NUMBER",
                databricks_delta_type=(
                    f"DECIMAL({min(precision_value - effective_scale, 38)},0)"),
                status=REVIEW, fidelity=WIDENED,
                notes=(f"NUMBER({precision_value},{scale_value}) has negative "
                       "scale; widened - review rounding"),
                is_nullable=bool(is_nullable))
        if effective_scale > precision_value:
            effective_scale = precision_value
            notes = (f"scale {scale_value} > precision {precision_value}; "
                     f"clamped scale to {effective_scale}")
        return ColumnMappingResult(
            source_type=source_type or "NUMBER",
            databricks_delta_type=(
                f"DECIMAL({precision_value},{effective_scale})"),
            status=AUTO, fidelity=EXACT, notes=notes,
            is_nullable=bool(is_nullable))