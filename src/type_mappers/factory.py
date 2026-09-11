"""Explicit source type-mapper registry."""

from __future__ import annotations

try:
    from src.source_identity import normalize_source_system
    from src.type_mappers.oracle import OracleTypeMapper
    from src.type_mappers.sqlserver import SqlServerTypeMapper
except ModuleNotFoundError:
    from source_identity import normalize_source_system
    from type_mappers.oracle import OracleTypeMapper
    from type_mappers.sqlserver import SqlServerTypeMapper


_MAPPERS = {
    "oracle": OracleTypeMapper,
    "sqlserver": SqlServerTypeMapper,
}


def get_type_mapper(source_system, rules=None):
    try:
        token = normalize_source_system(source_system)
    except ValueError:
        raise ValueError(
            f"No type mapper registered for source {source_system!r}") from None
    mapper_class = _MAPPERS.get(token)
    if mapper_class is None:
        raise ValueError(
            f"No type mapper registered for source {source_system!r}")
    return mapper_class(rules=rules)