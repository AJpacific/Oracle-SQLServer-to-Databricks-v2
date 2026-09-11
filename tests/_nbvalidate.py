"""
Notebook validation helpers for tests.

`compileall` does not treat Databricks notebook cells the way Databricks does:
magic lines (`%run`, `%sql`, `%md`, `# MAGIC ...`) are comments to Python but
directives to Databricks. These helpers split a notebook into cells, strip the
magic, compile the ordinary Python, and resolve `%run` targets against the real
repository layout.

This does NOT replace live Databricks execution; it only catches the static
breakages (bad relative path, syntax error, missing shared symbol) that would
otherwise surface as a runtime failure in a job.
"""

from __future__ import annotations

import ast
import os
import re

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
NOTEBOOKS = os.path.join(ROOT, "notebooks")

CELL_SEPARATOR = "# COMMAND ----------"
_MAGIC_LINE = re.compile(r"^\s*(#\s*MAGIC\b|%run\b|%sql\b|%md\b|%pip\b|%sh\b)")
_RUN_DIRECTIVE = re.compile(r"^\s*(?:#\s*MAGIC\s+)?%run\s+(\S+)", re.M)


def read_notebook(path):
    with open(path, "r", encoding="utf-8") as fh:
        return fh.read()


def split_cells(source):
    """Split notebook source into cells on the Databricks cell separator."""
    return source.split(CELL_SEPARATOR)


def strip_magic(cell):
    """Drop Databricks magic/markdown lines, leaving ordinary Python."""
    return "\n".join(line for line in cell.splitlines()
                     if not _MAGIC_LINE.match(line))


def compile_cells(path):
    """Compile every ordinary Python cell; returns a list of (cell_index, error).

    An empty list means every cell is syntactically valid Python.
    """
    errors = []
    for index, cell in enumerate(split_cells(read_notebook(path))):
        code = strip_magic(cell)
        if not code.strip():
            continue
        try:
            compile(code, f"{os.path.basename(path)}::cell{index}", "exec")
        except SyntaxError as exc:
            errors.append((index, f"{exc.msg} (line {exc.lineno})"))
    return errors


def run_targets(path):
    """Resolve every `%run` directive in a notebook to an absolute .py path."""
    source = read_notebook(path)
    base = os.path.dirname(path)
    targets = []
    for raw in _RUN_DIRECTIVE.findall(source):
        target = raw.strip().rstrip("/")
        resolved = os.path.normpath(os.path.join(base, target)) + ".py"
        targets.append((target, resolved))
    return targets


def all_notebook_paths():
    """Every notebook .py under notebooks/, excluding caches."""
    found = []
    for dirpath, dirnames, filenames in os.walk(NOTEBOOKS):
        dirnames[:] = [d for d in dirnames if d != "__pycache__"]
        for name in sorted(filenames):
            if name.endswith(".py"):
                found.append(os.path.join(dirpath, name))
    return found


def referenced_names(path):
    """Names a notebook reads but never assigns or imports in its own cells.

    These must be provided by the shared bootstrap (`%run ../../shared/_common`),
    so a missing one is exactly the NameError class of defect.
    """
    assigned, loaded = set(), set()
    for cell in split_cells(read_notebook(path)):
        code = strip_magic(cell)
        if not code.strip():
            continue
        try:
            tree = ast.parse(code)
        except SyntaxError:
            continue
        for node in ast.walk(tree):
            if isinstance(node, ast.Name):
                (assigned if isinstance(node.ctx, (ast.Store, ast.Del))
                 else loaded).add(node.id)
            elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef,
                                   ast.ClassDef)):
                assigned.add(node.name)
            elif isinstance(node, ast.arg):
                assigned.add(node.arg)
            elif isinstance(node, (ast.Import, ast.ImportFrom)):
                for alias in node.names:
                    assigned.add((alias.asname or alias.name).split(".")[0])
            elif isinstance(node, ast.ExceptHandler) and node.name:
                assigned.add(node.name)
            elif isinstance(node, (ast.comprehension,)):
                pass
    return loaded - assigned


def exported_names(path):
    """Top-level names a notebook defines (functions, classes, assignments)."""
    exported = set()
    for cell in split_cells(read_notebook(path)):
        code = strip_magic(cell)
        if not code.strip():
            continue
        try:
            tree = ast.parse(code)
        except SyntaxError:
            continue
        for node in tree.body:
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef,
                                 ast.ClassDef)):
                exported.add(node.name)
            elif isinstance(node, ast.Assign):
                for target in node.targets:
                    if isinstance(target, ast.Name):
                        exported.add(target.id)
            elif isinstance(node, (ast.Import, ast.ImportFrom)):
                for alias in node.names:
                    exported.add((alias.asname or alias.name).split(".")[0])
            elif isinstance(node, ast.Try):
                # _common imports through try/except ModuleNotFoundError.
                for sub in ast.walk(node):
                    if isinstance(sub, (ast.Import, ast.ImportFrom)):
                        for alias in sub.names:
                            exported.add((alias.asname or alias.name).split(".")[0])
    return exported
