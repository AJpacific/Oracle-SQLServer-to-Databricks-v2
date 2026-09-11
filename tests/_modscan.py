"""
AST-based modularity scanner.

Substring checks miss real leaks such as
``normalize_source_system(src_system) == "sqlserver"``. This walks the parsed
tree instead, so any comparison of a source-system expression against a source
literal is caught regardless of how it is spelled.

Findings carry file, line, construct, and the layer that should own the logic,
so a violation is actionable rather than a bare assertion failure.
"""

from __future__ import annotations

import ast
import os

try:
    from _nbvalidate import split_cells, strip_magic, read_notebook
except ImportError:
    from tests._nbvalidate import split_cells, strip_magic, read_notebook

SOURCE_LITERALS = {"oracle", "sqlserver", "sql_server", "mssql"}

# Names that denote a source-system value in this codebase.
SOURCE_SYSTEM_NAMES = {"source_system", "src_system", "canonical", "system"}
SOURCE_SYSTEM_CALLS = {"normalize_source_system"}

# Dialect catalog/SQL fragments that must never appear in shared executable code.
ORACLE_SQL_MARKERS = (
    "all_tables", "all_tab_columns", "all_constraints", "all_cons_columns",
    "all_views", "all_source", "select 1 from dual", "dba_", "sys.dual",
)
SQLSERVER_SQL_MARKERS = (
    "sys.tables", "sys.columns", "sys.indexes", "sys.partitions",
    "sys.allocation_units", "sys.sql_modules", "sys.schemas", "sys.objects",
    "information_schema",
)
CREDENTIAL_MARKERS = (
    "oracle-user", "oracle-password", "oracle-jdbc-url", "oracle-host",
    "oracle-port", "oracle-service", "sqlserver-user", "sqlserver-password",
    "sqlserver-jdbc-url", "sqlserver-host", "sqlserver-port",
)

FORBIDDEN_IMPORTS = {
    "sql_builder": "Oracle query builder belongs to OracleSourceAdapter",
    "sqlserver_sql_builder": "SQL Server query builder belongs to SqlServerSourceAdapter",
}
FORBIDDEN_CONSTRUCTION = {
    "OracleSourceAdapter": "resolve adapters through the factory / connection_id",
    "SqlServerSourceAdapter": "resolve adapters through the factory / connection_id",
}


class Finding:
    def __init__(self, path, line, construct, owner):
        self.path = path
        self.line = line
        self.construct = construct
        self.owner = owner

    def __str__(self):
        return (f"{os.path.basename(self.path)}:{self.line}: {self.construct} "
                f"-> belongs in {self.owner}")

    __repr__ = __str__


def _is_source_system_expr(node):
    """True when a node evaluates a source-system value."""
    if isinstance(node, ast.Name) and node.id in SOURCE_SYSTEM_NAMES:
        return True
    if isinstance(node, ast.Attribute) and node.attr in SOURCE_SYSTEM_NAMES:
        return True
    if isinstance(node, ast.Call):
        func = node.func
        name = getattr(func, "id", None) or getattr(func, "attr", None)
        if name in SOURCE_SYSTEM_CALLS:
            return True
        # row["source_system"] style access
        if name == "get" and node.args:
            first = node.args[0]
            if isinstance(first, ast.Constant) and first.value in SOURCE_SYSTEM_NAMES:
                return True
    if isinstance(node, ast.Subscript):
        key = node.slice
        if isinstance(key, ast.Constant) and key.value in SOURCE_SYSTEM_NAMES:
            return True
    if isinstance(node, ast.BoolOp):
        return any(_is_source_system_expr(v) for v in node.values)
    return False


def _source_literal(node):
    """Return the source literal a node represents, else None."""
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        if node.value.strip().lower() in SOURCE_LITERALS:
            return node.value
    if isinstance(node, (ast.Tuple, ast.List, ast.Set)):
        for element in node.elts:
            found = _source_literal(element)
            if found:
                return found
    return None


def find_source_branches(tree, path):
    """Comparisons of a source-system expression against a source literal."""
    findings = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Compare):
            continue
        operands = [node.left] + list(node.comparators)
        has_expr = any(_is_source_system_expr(o) for o in operands)
        literal = next((_source_literal(o) for o in operands
                        if _source_literal(o)), None)
        if has_expr and literal:
            findings.append(Finding(
                path, node.lineno,
                f"source-system comparison against {literal!r}",
                "the source adapter (apply_column_policy / adapter method)"))
    return findings


def find_source_defaults(tree, path):
    """Implicit source literals selected with ``or`` in shared code."""
    findings = []
    for node in ast.walk(tree):
        if not (isinstance(node, ast.BoolOp) and isinstance(node.op, ast.Or)):
            continue
        has_source = any(_is_source_system_expr(value) for value in node.values)
        literal = next((_source_literal(value) for value in node.values
                        if _source_literal(value)), None)
        if has_source and literal:
            findings.append(Finding(
                path, node.lineno,
                f"implicit source-system default to {literal!r}",
                "explicit source identity validation"))
    return findings


def _docstring_nodes(tree):
    """Constant nodes that are docstrings; documentation may name a source."""
    docstrings = set()
    for node in ast.walk(tree):
        if not isinstance(node, (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef,
                                 ast.ClassDef)):
            continue
        body = getattr(node, "body", None) or []
        if (body and isinstance(body[0], ast.Expr)
                and isinstance(body[0].value, ast.Constant)
                and isinstance(body[0].value.value, str)):
            docstrings.add(id(body[0].value))
    return docstrings


def find_dialect_strings(tree, path):
    """Dialect catalog SQL or credential key names in executable code.

    Docstrings are exempt: a shared module may document what a source owns.
    """
    findings = []
    docstrings = _docstring_nodes(tree)
    for node in ast.walk(tree):
        if not (isinstance(node, ast.Constant) and isinstance(node.value, str)):
            continue
        if id(node) in docstrings:
            continue
        lowered = node.value.lower()
        for marker in ORACLE_SQL_MARKERS:
            if marker in lowered:
                findings.append(Finding(path, node.lineno,
                                        f"Oracle catalog SQL {marker!r}",
                                        "src/sql_builder.py or the Oracle adapter"))
        for marker in SQLSERVER_SQL_MARKERS:
            if marker in lowered:
                findings.append(Finding(
                    path, node.lineno, f"SQL Server catalog SQL {marker!r}",
                    "src/sqlserver_sql_builder.py or the SQL Server adapter"))
        for marker in CREDENTIAL_MARKERS:
            if marker == lowered:
                findings.append(Finding(path, node.lineno,
                                        f"source credential key {marker!r}",
                                        "the source adapter"))
    return findings


def find_forbidden_imports(tree, path):
    findings = []
    for node in ast.walk(tree):
        names = []
        if isinstance(node, ast.Import):
            names = [a.name for a in node.names]
        elif isinstance(node, ast.ImportFrom):
            names = [node.module or ""] + [a.name for a in node.names]
        for raw in names:
            leaf = (raw or "").split(".")[-1]
            if leaf in FORBIDDEN_IMPORTS:
                findings.append(Finding(path, node.lineno,
                                        f"import of {leaf!r}",
                                        FORBIDDEN_IMPORTS[leaf]))
    return findings


def find_concrete_adapter_use(tree, path):
    """Direct construction of a concrete adapter (factory resolution is fine)."""
    findings = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            name = getattr(node.func, "id", None)
            if name in FORBIDDEN_CONSTRUCTION:
                findings.append(Finding(path, node.lineno,
                                        f"direct {name}() construction",
                                        FORBIDDEN_CONSTRUCTION[name]))
        elif isinstance(node, ast.ImportFrom):
            for alias in node.names:
                if alias.name in FORBIDDEN_CONSTRUCTION:
                    findings.append(Finding(
                        path, node.lineno, f"import of {alias.name}",
                        FORBIDDEN_CONSTRUCTION[alias.name]))
    return findings


def parse_notebook(path):
    """Parse a notebook's executable cells into one AST (magic stripped).

    Line numbers are preserved by replacing magic lines with blanks rather than
    deleting them.
    """
    source = read_notebook(path)
    lines = []
    for line in source.splitlines():
        stripped = strip_magic(line)
        lines.append(stripped if stripped.strip() else "")
    return ast.parse("\n".join(lines))


def parse_module(path):
    with open(path, "r", encoding="utf-8") as fh:
        return ast.parse(fh.read())


def scan(path, is_notebook=True):
    """Return every modularity finding for one shared file."""
    tree = parse_notebook(path) if is_notebook else parse_module(path)
    return (find_source_branches(tree, path)
            + find_source_defaults(tree, path)
            + find_dialect_strings(tree, path)
            + find_forbidden_imports(tree, path)
            + find_concrete_adapter_use(tree, path))
