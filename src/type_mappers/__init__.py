"""Source-specific datatype mapper contracts and implementations."""

from .base import (
    AUTO, BLOCKED, EXACT, LOSSY, REVIEW, UNKNOWN, WIDENED,
    ColumnMappingResult, SourceTypeMapper, classify_table_compatibility,
)
from .factory import get_type_mapper
from .oracle import OracleTypeMapper
from .sqlserver import SqlServerTypeMapper

__all__ = (
    "AUTO", "REVIEW", "BLOCKED", "EXACT", "WIDENED", "LOSSY", "UNKNOWN",
    "ColumnMappingResult", "SourceTypeMapper", "classify_table_compatibility",
    "OracleTypeMapper", "SqlServerTypeMapper", "get_type_mapper",
)