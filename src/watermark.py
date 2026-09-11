"""
watermark.py - source-neutral watermark serialization.

Canonical watermark strings are a Delta-side concern shared by every source and
by the shared notebooks, so they must not live in a dialect query builder. This
module previously sat inside the Oracle builder, which forced both shared code
and the SQL Server builder to import an Oracle module.

Pure: no Spark, no dbutils, no dialect SQL.
"""

from __future__ import annotations

from datetime import date, datetime, timezone


def canonical_watermark_string(value, strict=False):
    """Serialize a watermark value to a canonical ISO-8601 UTC string.

    Timezone-aware values are converted to UTC and rendered with a trailing 'Z'.
    Naive datetimes and DATE/TIMESTAMP strings are treated as UTC per policy.
    Microseconds are always present. None maps to None. With strict=True an
    unparseable temporal value raises ValueError (use for checkpoint writes);
    otherwise it is returned unchanged (backward-compatible read tolerance).
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, date):
        parsed = datetime(value.year, value.month, value.day)
    else:
        text = str(value).strip()
        if "T" not in text and " " in text:
            text = text.replace(" ", "T", 1)
        text = text.replace(" ", "")  # drop any space before a tz offset
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError:
            if strict:
                raise ValueError(f"Unparseable temporal watermark: {value!r}")
            return str(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    parsed = parsed.astimezone(timezone.utc)
    return parsed.isoformat(timespec="microseconds").replace("+00:00", "Z")
