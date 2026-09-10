"""
Shared test doubles for the pure-Python unit tests.

These fakes let us exercise the query-building / policy code in
ControlRepository and the notebooks' pure helpers without a live Spark session
or any JDBC driver. Only the SQL text and control flow are asserted.
"""

from __future__ import annotations


class FakeRow(dict):
    """A dict that also supports row["col"] and .asDict() like a Spark Row."""

    def asDict(self, recursive=False):
        return dict(self)

    def __getitem__(self, key):
        return dict.__getitem__(self, key)


class FakeDataFrame:
    def __init__(self, rows=None):
        self._rows = rows or []

    def collect(self):
        return self._rows

    def count(self):
        return len(self._rows)


class FakeSpark:
    """Records every SQL string and returns queued collect() results in order.

    Configure ``results`` as a list of row-lists; each ``sql()`` call whose
    result is consumed via ``collect()`` pops the next entry. Any SQL string is
    recorded in ``executed`` for assertions.
    """

    def __init__(self, results=None):
        self.executed = []
        self._results = list(results or [])

    def sql(self, statement):
        self.executed.append(statement)
        rows = self._results.pop(0) if self._results else []
        return FakeDataFrame(rows)

    # convenience for assertions
    def last_sql(self):
        return self.executed[-1] if self.executed else ""
