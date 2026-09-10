"""Safe identifier and connection-token validation helpers."""
from __future__ import annotations

import re

_VALID_IDENTIFIER = re.compile(r"^[A-Za-z_#$][A-Za-z0-9_#$]*$")
_VALID_SQLSERVER_IDENTIFIER = re.compile(r"^[A-Za-z_#$\[][A-Za-z0-9_ #$\[\]-]*$")
_VALID_SERVER = re.compile(r"^[A-Za-z0-9_.\-\\]+(?:,[0-9]{1,5})?(?::[0-9]{1,5})?$")
_VALID_DATABASE = re.compile(r"^[A-Za-z_][A-Za-z0-9_$#-]*$")


class IdentifierError(ValueError):
    """Raised when an identifier or connection token fails validation."""


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


def quote_sqlserver(identifier: str) -> str:
    validated = validate_sqlserver_identifier(identifier)
    return "[" + validated.replace("]", "]]" ) + "]"


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
