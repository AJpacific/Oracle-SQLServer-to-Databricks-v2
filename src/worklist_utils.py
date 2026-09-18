"""worklist_utils.py - Shared helpers and authoritative limits for task-value worklists.

Ensures consistent task-value serialization and payload size checks across
NB_GetFullLoadWorklist, NB_GetDeltaWorklist, NB_GetConnectionWorklist,
and NB_GetSelectedAssessmentWorklist.
"""

from __future__ import annotations

import json
from typing import Any

# Authoritative safety guard limit for Databricks task values across all worklists.
# Databricks enforces a 48 KB (49,152 byte) hard limit on task values.
# 40,000 bytes provides a reliable safety margin against formatting and escaping overhead.
TASK_VALUE_LIMIT_BYTES: int = 40_000


def canonical_task_value_serialization(value: Any) -> str:
    """Return the canonical compact JSON representation for payload size measurement."""
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False)


def validate_task_value_payload(value: Any, *, key: str, limit_bytes: int | None = None) -> int:
    """Validate that the serialized task-value payload does not exceed the limit.

    Measures UTF-8 encoded bytes. Does not expose worklist contents in exceptions.
    Returns the exact payload size in bytes on success.
    Raises ValueError if payload exceeds the configured limit.
    """
    effective_limit = TASK_VALUE_LIMIT_BYTES if limit_bytes is None else int(limit_bytes)
    serialized = canonical_task_value_serialization(value)
    payload_bytes = len(serialized.encode("utf-8"))

    if payload_bytes > effective_limit:
        raise ValueError(
            f"Task value {key!r} exceeds configured payload limit: "
            f"{payload_bytes} bytes (limit: {effective_limit} bytes); "
            "reduce worklist scope or use filters"
        )

    return payload_bytes
