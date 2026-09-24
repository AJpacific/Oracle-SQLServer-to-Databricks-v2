"""Safe identifier and connection-token validation helpers."""
from __future__ import annotations

import hashlib
import re

_VALID_IDENTIFIER = re.compile(r"^[A-Za-z_#$][A-Za-z0-9_#$]*$")
_VALID_SQLSERVER_IDENTIFIER = re.compile(r"^[A-Za-z_#$\[][A-Za-z0-9_ #$\[\]-]*$")
_VALID_SERVER = re.compile(r"^[A-Za-z0-9_.\-\\]+(?:,[0-9]{1,5})?(?::[0-9]{1,5})?$")
_VALID_DATABASE = re.compile(r"^[A-Za-z_][A-Za-z0-9_$#-]*$")


class IdentifierError(ValueError):
    """Raised when an identifier or connection token fails validation."""


def normalize_target_identifier(name: str, identifier_type: str = "column") -> str:
    """Deterministically normalize any source identifier to a valid Databricks identifier.

    Rules:
    1. Reject None or blank input.
    2. Trim outer whitespace.
    3. Convert to lowercase.
    4. Replace every consecutive sequence outside [a-z0-9_] with one underscore.
    5. Collapse consecutive underscores into one underscore.
    6. Strip leading and trailing underscores.
    7. If the result begins with a digit, add a type-appropriate prefix.
    8. Enforce length limit (128 chars); if truncation required, append SHA-256 suffix.
    9. Ensure final name passes validate_identifier.
    """
    if name is None:
        raise IdentifierError("Target identifier input is None")
    if not isinstance(name, str):
        raise IdentifierError(f"Target identifier input must be a string, got {type(name)!r}")
    trimmed = name.strip()
    if not trimmed:
        raise IdentifierError("Target identifier input is empty or blank")

    lower_val = trimmed.lower()
    replaced = re.sub(r"[^a-z0-9_]+", "_", lower_val)
    collapsed = re.sub(r"_+", "_", replaced)
    cleaned = collapsed.strip("_")
    if not cleaned:
        raise IdentifierError(
            f"Target identifier {name!r} contains no valid identifier characters"
        )

    if cleaned[0].isdigit():
        prefix_type = str(identifier_type or "column").strip().lower()
        prefix_map = {
            "column": "column_",
            "table": "table_",
            "schema": "schema_",
            "stage": "stage_",
        }
        prefix = prefix_map.get(prefix_type, f"{prefix_type.rstrip('_')}_")
        cleaned = f"{prefix}{cleaned}"

    max_len = 128
    if len(cleaned) > max_len:
        hash_suffix = hashlib.sha256(trimmed.lower().encode("utf-8")).hexdigest()[:8]
        prefix_keep = max_len - 9  # 128 - 9 = 119
        cleaned = f"{cleaned[:prefix_keep].rstrip('_')}_{hash_suffix}"

    return validate_identifier(cleaned)


def validate_identifier(name: str) -> str:
    """Validate a plain Oracle/Databricks identifier."""
    if name is None:
        raise IdentifierError("Identifier is None")
    if not isinstance(name, str):
        raise IdentifierError(f"Identifier must be a string, got {type(name)!r}")
    stripped = name.strip()
    if not stripped:
        raise IdentifierError("Identifier is empty")
    if len(stripped) > 128:
        raise IdentifierError(f"Identifier too long: {stripped[:40]}...")
    if not _VALID_IDENTIFIER.match(stripped):
        raise IdentifierError(f"Invalid identifier characters: {name!r}")
    return stripped


def validate_sqlserver_identifier(name: str) -> str:
    """Validate a SQL Server identifier before bracket quoting.

    Hyphens are allowed because Azure SQL database names commonly contain them
    and bracket quoting keeps them inside the identifier. Semicolons, quotes,
    equals signs, and other connection-string injection characters remain blocked.
    """
    if name is None:
        raise IdentifierError("Identifier is None")
    if not isinstance(name, str):
        raise IdentifierError(f"Identifier must be a string, got {type(name)!r}")
    stripped = name.strip()
    if not stripped:
        raise IdentifierError("Identifier is empty")
    if len(stripped) > 128:
        raise IdentifierError(f"Identifier too long: {stripped[:40]}...")
    if not _VALID_SQLSERVER_IDENTIFIER.match(stripped):
        raise IdentifierError(f"Invalid SQL Server identifier characters: {name!r}")
    return stripped


def quote_databricks(identifier: str) -> str:
    validated = validate_identifier(identifier)
    return "`" + validated.replace("`", "``") + "`"


def quote_oracle(identifier: str) -> str:
    validated = validate_identifier(identifier)
    return '"' + validated.replace('"', '""') + '"'


def quote_oracle_column(column_name: str) -> str:
    """Quote an Oracle source column name safely with double quotes.

    Preserves exact column labels including spaces, periods, slashes, hyphens,
    and brackets. Escapes embedded double quotes.
    """
    if column_name is None:
        raise IdentifierError("Oracle column name is None")
    if not isinstance(column_name, str):
        raise IdentifierError(f"Oracle column name must be a string, got {type(column_name)!r}")
    stripped = column_name.strip()
    if not stripped:
        raise IdentifierError("Oracle column name is empty")
    return '"' + stripped.replace('"', '""') + '"'


def quote_sqlserver(identifier: str) -> str:
    validated = validate_sqlserver_identifier(identifier)
    return "[" + validated.replace("]", "]]" ) + "]"


def quote_sqlserver_column(column_name: str) -> str:
    """Quote a SQL Server source column name safely with brackets.

    Preserves exact column labels including spaces, periods, slashes, hyphens,
    and brackets. Escapes embedded right brackets.
    """
    if column_name is None:
        raise IdentifierError("SQL Server column name is None")
    if not isinstance(column_name, str):
        raise IdentifierError(f"SQL Server column name must be a string, got {type(column_name)!r}")
    stripped = column_name.strip()
    if not stripped:
        raise IdentifierError("SQL Server column name is empty")
    return "[" + stripped.replace("]", "]]") + "]"


def oracle_fqn(schema: str, table: str) -> str:
    return f"{quote_oracle(schema)}.{quote_oracle(table)}"


def sqlserver_fqn(schema: str, table: str, database: str = None) -> str:
    if database:
        return (
            f"{quote_sqlserver(database)}."
            f"{quote_sqlserver(schema)}."
            f"{quote_sqlserver(table)}"
        )
    return f"{quote_sqlserver(schema)}.{quote_sqlserver(table)}"


def databricks_fqn(catalog: str, schema: str, table: str) -> str:
    return (
        f"{quote_databricks(catalog)}."
        f"{quote_databricks(schema)}."
        f"{quote_databricks(table)}"
    )


def escape_string_literal(value) -> str:
    if value is None:
        return "NULL"
    return "'" + str(value).replace("'", "''") + "'"


def validate_server(server: str) -> str:
    """Validate a host, host\\instance, host,port, or host:port token."""
    if server is None:
        raise IdentifierError("Server is None")
    if not isinstance(server, str):
        raise IdentifierError(f"Server must be a string, got {type(server)!r}")
    stripped = server.strip()
    if not stripped:
        raise IdentifierError("Server is empty")
    if len(stripped) > 255:
        raise IdentifierError("Server name too long")
    if not _VALID_SERVER.match(stripped):
        raise IdentifierError(f"Invalid server value: {server!r}")
    return stripped


def validate_database(database: str) -> str:
    """Validate a database name before JDBC URL interpolation.

    Letters, digits, underscore, dollar, hash, and hyphen are permitted. The
    name must begin with a letter or underscore. Semicolon, equals, quotes, and
    whitespace remain blocked to prevent JDBC connection-string injection.
    """
    if database is None:
        raise IdentifierError("Database is None")
    if not isinstance(database, str):
        raise IdentifierError(f"Database must be a string, got {type(database)!r}")
    stripped = database.strip()
    if not stripped:
        raise IdentifierError("Database is empty")
    if len(stripped) > 128:
        raise IdentifierError("Database name too long")
    if not _VALID_DATABASE.match(stripped):
        raise IdentifierError(f"Invalid database value: {database!r}")
    return stripped
