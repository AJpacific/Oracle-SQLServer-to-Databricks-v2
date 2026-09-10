"""
partitioning.py - safe JDBC range-partition eligibility for full-load reads.

Spark JDBC range partitioning requires signed integral (long) bounds. An
unconstrained Oracle NUMBER primary key (precision/scale NULL, mapped to
DECIMAL(38,0)) returns planning bounds like '1007.0000000000', which Spark
cannot parse ("For input string: ..."). This module decides, purely from
metadata and MIN/MAX, whether a PK is a genuinely bounded integral column that
is safe to partition on - and normalizes decimal-formatted integral bounds back
to exact ints. It never casts or alters the migrated source values.

Pure module: no Spark, no dbutils. Fully unit-testable.
"""

from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Optional


# Signed 64-bit (Spark/Java long) range.
_LONG_MIN = -(2 ** 63)
_LONG_MAX = 2 ** 63 - 1

# Only true integer target types may drive JDBC partitioning. DECIMAL is
# intentionally excluded so an unconstrained NUMBER -> DECIMAL(38,0) never
# qualifies.
_INTEGRAL_TARGET_TYPES = {"SMALLINT", "INT", "BIGINT"}


def normalize_signed_long_bound(value) -> Optional[int]:
    """Return value as an exact signed 64-bit int, or None if unsafe.

    Uses Decimal(str(value)) (never float) so an Oracle planning bound like
    '1007.0000000000' becomes 1007. Zero and negative integral values are
    accepted; nonzero-fraction, malformed, NaN/inf, and out-of-64-bit-range
    values return None.
    """
    if value is None:
        return None
    try:
        d = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None
    if not d.is_finite():
        return None
    integral = d.to_integral_value()
    if d != integral:
        return None
    n = int(integral)
    if n < _LONG_MIN or n > _LONG_MAX:
        return None
    return n


def is_integer_like_target(target_type) -> bool:
    """True only for SMALLINT/INT/BIGINT. DECIMAL is intentionally excluded."""
    return (target_type or "").strip().upper() in _INTEGRAL_TARGET_TYPES


def is_partition_safe_pk(data_type, numeric_precision, numeric_scale,
                         target_type) -> bool:
    """Oracle PK metadata + resolved target must be a bounded integral NUMBER.

    Requires Oracle data_type NUMBER, scale exactly 0, precision 1..18, and an
    integral target type. Unconstrained NUMBER (precision/scale NULL) fails.
    """
    if (data_type or "").strip().upper() != "NUMBER":
        return False
    if numeric_precision is None or numeric_scale is None:
        return False
    try:
        if int(numeric_scale) != 0:
            return False
        p = int(numeric_precision)
    except (ValueError, TypeError):
        return False
    if p < 1 or p > 18:
        return False
    return is_integer_like_target(target_type)


def resolve_partitioning(data_type, numeric_precision, numeric_scale, target_type,
                         min_val, max_val, requested_partitions):
    """Decide safe JDBC partitioning for a single PK column.

    Returns (effective_partitions, lower, upper, reason). On success reason is
    None and effective_partitions > 1 with normalized integral bounds. On any
    unsafe condition returns (None, None, None, reason) so the caller falls back
    to a correctness-safe unpartitioned read.
    """
    if requested_partitions is None or int(requested_partitions) <= 1:
        return None, None, None, "num_partitions <= 1"
    if not is_partition_safe_pk(data_type, numeric_precision, numeric_scale, target_type):
        return None, None, None, (
            "PK is not a bounded integral NUMBER (need Oracle NUMBER, scale 0, "
            f"precision 1-18, target SMALLINT/INT/BIGINT; got data_type="
            f"{data_type!r} precision={numeric_precision!r} scale={numeric_scale!r} "
            f"target={target_type!r})")
    lo = normalize_signed_long_bound(min_val)
    hi = normalize_signed_long_bound(max_val)
    if lo is None or hi is None:
        return None, None, None, (
            f"MIN/MAX are not exact signed 64-bit integers ({min_val!r}, {max_val!r})")
    if lo >= hi:
        return None, None, None, f"MIN ({lo}) is not < MAX ({hi})"
    effective = min(int(requested_partitions), hi - lo + 1)
    if effective <= 1:
        return None, None, None, f"effective partitions <= 1 for range {lo}..{hi}"
    return effective, lo, hi, None


# SQL Server native integral PK types are eligible for JDBC range partitioning
# directly (no NUMBER wrapper). decimal/numeric PKs use the unpartitioned
# fallback initially, exactly like an unconstrained Oracle NUMBER.
_SQLSERVER_INTEGRAL_SOURCE_TYPES = {"tinyint", "smallint", "int", "bigint"}


def is_partition_safe_sqlserver_pk(data_type, target_type) -> bool:
    """SQL Server PK must be a native integral type mapped to an integral target."""
    if (data_type or "").strip().lower() not in _SQLSERVER_INTEGRAL_SOURCE_TYPES:
        return False
    return is_integer_like_target(target_type)


def resolve_partitioning_sqlserver(data_type, target_type, min_val, max_val,
                                   requested_partitions):
    """Decide safe JDBC partitioning for a single SQL Server PK column.

    Mirrors :func:`resolve_partitioning` but recognizes SQL Server's native
    integral types (tinyint/smallint/int/bigint). decimal/numeric PKs are not
    partitioned initially (correctness-safe unpartitioned fallback).
    """
    if requested_partitions is None or int(requested_partitions) <= 1:
        return None, None, None, "num_partitions <= 1"
    if not is_partition_safe_sqlserver_pk(data_type, target_type):
        return None, None, None, (
            "PK is not a native integral SQL Server type (need tinyint/smallint/"
            f"int/bigint mapped to SMALLINT/INT/BIGINT; got data_type={data_type!r} "
            f"target={target_type!r})")
    lo = normalize_signed_long_bound(min_val)
    hi = normalize_signed_long_bound(max_val)
    if lo is None or hi is None:
        return None, None, None, (
            f"MIN/MAX are not exact signed 64-bit integers ({min_val!r}, {max_val!r})")
    if lo >= hi:
        return None, None, None, f"MIN ({lo}) is not < MAX ({hi})"
    effective = min(int(requested_partitions), hi - lo + 1)
    if effective <= 1:
        return None, None, None, f"effective partitions <= 1 for range {lo}..{hi}"
    return effective, lo, hi, None
