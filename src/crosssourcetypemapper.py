"""Deprecated compatibility facade for the source-specific type mappers."""

from __future__ import annotations

try:
    from src.type_mappers.base import (
        ColumnMappingResult, SourceTypeMapper, classify_table_compatibility,
    )
    from src.type_mappers.factory import get_type_mapper
except ModuleNotFoundError:
    from type_mappers.base import (
        ColumnMappingResult, SourceTypeMapper, classify_table_compatibility,
    )
    from type_mappers.factory import get_type_mapper


class CrossSourceTypeMapper:
    """Compatibility constructor; new code should request a concrete mapper."""

    def __new__(cls, rules=None, dialect="oracle"):
        return get_type_mapper(dialect, rules=rules)


__all__ = (
    "CrossSourceTypeMapper", "ColumnMappingResult", "SourceTypeMapper",
    "classify_table_compatibility",
)