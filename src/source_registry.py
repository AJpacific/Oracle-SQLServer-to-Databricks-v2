"""
source_registry.py - explicit registration of every supported source.

This is a static manifest, not a discovery mechanism: it maps a canonical source
token to its adapter class, its source-specific notebook paths, and the
capabilities shared code may rely on. Adding a source later is one explicit
entry here plus one entry in the adapter factory - no directory scanning, no
dynamic import, no reflection, no eval/exec.

Pure: no Spark, no dbutils.
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


# Notebook roles a source must provide. Everything else is shared.
NOTEBOOK_ROLES = ("validate_connection", "assessment", "inventory",
                  "sql_objects", "diagnostic")

# Capabilities shared code may check before asking for an optional operation.
CAPABILITIES = ("sql_object_assessment", "packages", "catalog_row_counts")

SOURCE_DEFINITIONS = {
    "oracle": {
        "display_name": "Oracle",
        "adapter": OracleSourceAdapter,
        "notebooks": {
            "validate_connection":
                "sources/oracle/NB00A_UpsertAndValidateConnection",
            "assessment":
                "sources/oracle/NB01A_SourceAssessment",
            "inventory":
                "sources/oracle/NB01_SourceInventory",
            "sql_objects":
                "sources/oracle/NB13_SQLObjectAssessmentAndConversion",
            "diagnostic":
                "sources/oracle/TEST_CONNECTION",
        },
        "capabilities": {
            "sql_object_assessment": True,
            "packages": True,
            "catalog_row_counts": False,   # dictionary statistics are estimates
        },
    },
    "sqlserver": {
        "display_name": "Microsoft SQL Server",
        "adapter": SqlServerSourceAdapter,
        "notebooks": {
            "validate_connection":
                "sources/sqlserver/NB00A_UpsertAndValidateConnection",
            "assessment":
                "sources/sqlserver/NB01A_SourceAssessment",
            "inventory":
                "sources/sqlserver/NB01_SourceInventory",
            "sql_objects":
                "sources/sqlserver/NB13_SQLObjectAssessmentAndConversion",
            "diagnostic":
                "sources/sqlserver/TEST_CONNECTION",
        },
        "capabilities": {
            "sql_object_assessment": True,
            "packages": False,            # SQL Server has no package objects
            "catalog_row_counts": True,
        },
    },
}


def get_source_definition(source_system):
    """Return the manifest entry for a source; unknown sources raise."""
    token = normalize_source_system(source_system)
    definition = SOURCE_DEFINITIONS.get(token)
    if definition is None:
        raise ValueError(f"source {source_system!r} is not registered")
    return definition


def get_notebook_path(source_system, role):
    """Return the source-specific notebook path for one role."""
    if role not in NOTEBOOK_ROLES:
        raise ValueError(f"unknown notebook role: {role!r}")
    return get_source_definition(source_system)["notebooks"][role]


def supports(source_system, capability):
    """True when a source supports an optional capability."""
    if capability not in CAPABILITIES:
        raise ValueError(f"unknown capability: {capability!r}")
    return bool(get_source_definition(source_system)["capabilities"][capability])


def registered_sources():
    return tuple(sorted(SOURCE_DEFINITIONS))
