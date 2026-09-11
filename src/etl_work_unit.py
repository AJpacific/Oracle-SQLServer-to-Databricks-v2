"""
etl_work_unit.py - the single authoritative Bronze-to-Silver execution boundary.

A Bronze-to-Silver run must filter, replace, reconcile, audit, and checkpoint
against exactly ONE set of boundaries. A retry replays the boundaries frozen by
the failed attempt rather than recomputing a wider interval from the current
Bronze MAX, so a newer row arriving after the failure is not silently swept in.

Resolving the work unit once - before any data is touched - removes the class of
defect where the processed interval and the audited interval disagree.

Pure: no Spark, no dbutils.
"""

from __future__ import annotations

from dataclasses import dataclass

try:
    from src.watermark import canonical_watermark_string
except ModuleNotFoundError:
    from watermark import canonical_watermark_string

FULL = "FULL"
INCREMENTAL = "INCREMENTAL"

# Bronze watermark families an incremental ETL may bound on.
TIMESTAMP_TYPE = "timestamp"
DATE_TYPE = "date"
SUPPORTED_WATERMARK_CASTS = (TIMESTAMP_TYPE, DATE_TYPE)


@dataclass(frozen=True)
class EtlWorkUnit:
    """Immutable boundaries every stage of one ETL execution must use."""

    mode: str
    lower_watermark: str = None
    upper_watermark: str = None
    watermark_column: str = None
    watermark_cast: str = None
    is_retry: bool = False
    parent_run_id: str = None

    @property
    def is_incremental(self):
        return self.mode == INCREMENTAL

    def audit_fields(self, attempt_number=None):
        """Boundary fields for a table_run_log row (success OR failure).

        Both paths call this, so a failed retry hands the same frozen interval
        to the next retry.
        """
        return {
            "lower_watermark": self.lower_watermark,
            "upper_watermark": self.upper_watermark,
            "parent_run_id": self.parent_run_id,
            "attempt_number": attempt_number,
        }


def normalize_bound(value, cast_to):
    """Canonicalize one boundary literal for its target type, else raise."""
    if value is None:
        return None
    canonical = canonical_watermark_string(value, strict=True)
    return canonical[:10] if cast_to == DATE_TYPE else canonical


def resolve_cast(spark_type_name):
    """Map a Bronze watermark column type to its supported cast, else None."""
    token = (spark_type_name or "").strip().lower()
    if token.startswith("timestamp"):
        return TIMESTAMP_TYPE
    if token == "date":
        return DATE_TYPE
    return None


def build_full_work_unit():
    """A full ETL run has no interval; every path must therefore audit nulls."""
    return EtlWorkUnit(mode=FULL)


def build_incremental_work_unit(watermark_column, cast_to, lower, upper,
                                is_retry=False, parent_run_id=None,
                                require_lower=False):
    """Validate and freeze an incremental interval before any data is touched.

    Raises ValueError for a missing upper bound, an unparseable bound, or a
    non-increasing interval, so a bad boundary fails before Bronze is read.
    """
    if cast_to not in SUPPORTED_WATERMARK_CASTS:
        raise ValueError(
            f"ETL watermark column {watermark_column!r} has unsupported type "
            f"for bounding; only DATE and TIMESTAMP are supported")
    if upper is None:
        raise ValueError(
            "incremental ETL requires an upper watermark bound"
            + (" from the failed attempt" if is_retry else ""))
    if require_lower and lower is None:
        raise ValueError("incremental ETL retry requires a lower watermark bound")

    normalized_lower = normalize_bound(lower, cast_to)
    normalized_upper = normalize_bound(upper, cast_to)
    if normalized_lower is not None and not normalized_lower < normalized_upper:
        raise ValueError(
            f"ETL lower bound {normalized_lower!r} must be strictly less than "
            f"upper bound {normalized_upper!r}")

    return EtlWorkUnit(
        mode=INCREMENTAL,
        lower_watermark=normalized_lower,
        upper_watermark=normalized_upper,
        watermark_column=watermark_column,
        watermark_cast=cast_to,
        is_retry=is_retry,
        parent_run_id=parent_run_id,
    )


def checkpoint_value(work_unit, reconciliation_passed):
    """The ETL checkpoint to commit, or None when nothing may advance.

    Only a reconciled incremental run advances, and only to the frozen upper
    bound - never to a value recomputed after processing.
    """
    if not reconciliation_passed or not work_unit.is_incremental:
        return None
    return work_unit.upper_watermark
