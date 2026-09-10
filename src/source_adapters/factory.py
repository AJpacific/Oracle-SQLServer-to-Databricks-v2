"""
source_adapters.factory - build the right source adapter for a control row.

The pipeline inspects ``source_system`` for every control-table row and obtains
an adapter via :func:`get_source_adapter`. An unknown source raises loudly; it is
never silently defaulted to Oracle.
"""

from __future__ import annotations

try:
    from src.source_identity import normalize_source_system
    from src.source_adapters.oracle import OracleSourceAdapter
    from src.source_adapters.sqlserver import SqlServerSourceAdapter
except ModuleNotFoundError:
    from source_identity import normalize_source_system
    from source_adapters.oracle import OracleSourceAdapter
    from source_adapters.sqlserver import SqlServerSourceAdapter


_ADAPTERS = {
    "oracle": OracleSourceAdapter,
    "sqlserver": SqlServerSourceAdapter,
}


def get_source_adapter(source_system, source_server=None, source_database=None,
                       secret_provider=None, secret_scope=None, config=None):
    """Return a source adapter for ``source_system``.

    ``source_system`` is normalized (oracle | sqlserver | sql_server | mssql).
    ``secret_provider`` is a callable ``(scope, key) -> value | None`` used by the
    adapter's connection methods; the query-building/policy methods need neither
    it nor a Spark session. An unrecognized source raises ValueError.
    """
    canonical = normalize_source_system(source_system)
    adapter_cls = _ADAPTERS.get(canonical)
    if adapter_cls is None:  # defensive; normalize_source_system already guards
        raise ValueError(f"Unsupported source_system: {source_system!r}")
    return adapter_cls(
        secret_provider=secret_provider,
        secret_scope=secret_scope,
        source_server=source_server,
        source_database=source_database,
        config=config,
    )
