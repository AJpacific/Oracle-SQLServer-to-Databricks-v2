"""
source_adapters - source-adapter boundary for the shared accelerator.

The shared orchestration decides *what* to do for each control-table row; a
source adapter decides *how* to connect to and query that row's source system.
Import ``get_source_adapter`` (factory) and ``SourceAdapter`` (contract) from
here.
"""

from __future__ import annotations

try:
    from src.source_adapters.base import SourceAdapter
    from src.source_adapters.factory import get_source_adapter
    from src.source_identity import normalize_source_system
    from src.source_adapters.oracle import OracleSourceAdapter
    from src.source_adapters.sqlserver import SqlServerSourceAdapter
except ModuleNotFoundError:
    from source_adapters.base import SourceAdapter
    from source_adapters.factory import get_source_adapter
    from source_identity import normalize_source_system
    from source_adapters.oracle import OracleSourceAdapter
    from source_adapters.sqlserver import SqlServerSourceAdapter

__all__ = [
    "SourceAdapter",
    "get_source_adapter",
    "normalize_source_system",
    "OracleSourceAdapter",
    "SqlServerSourceAdapter",
]
