"""
reconciliation.py - pure source-to-Bronze reconciliation decisions.

The delta workflow must reconcile the *exact* work unit it applied BEFORE it
commits a checkpoint, and it must never treat ``target_count >= source_count``
as an automatic pass. This module turns the row counts / key checks that NB11b
gathers from Spark into an explicit PASS / WARN / FAIL decision plus a list of
named checks written to ``reconciliation_results``. It is fully unit-testable
with no Spark.
"""

from __future__ import annotations

# Named reconciliation check types persisted to reconciliation_results.
FULL_SNAPSHOT_COUNT = "FULL_SNAPSHOT_COUNT"
DELTA_INTERVAL_COUNT = "DELTA_INTERVAL_COUNT"
STAGE_COUNT = "STAGE_COUNT"
DUPLICATE_PRIMARY_KEY = "DUPLICATE_PRIMARY_KEY"
MERGED_KEY_EXISTENCE = "MERGED_KEY_EXISTENCE"

PASS = "PASS"
WARN = "WARN"
FAIL = "FAIL"


class ReconResult:
    """Outcome of a reconciliation: an overall status plus its named checks.

    ``checks`` is a list of dicts with keys check_type, source_value,
    target_value, status, message - shaped for direct insertion into
    reconciliation_results.
    """

    def __init__(self, status, checks):
        self.status = status
        self.checks = checks

    @property
    def passed(self):
        return self.status in (PASS, WARN)

    def __repr__(self):
        return f"ReconResult(status={self.status!r}, checks={self.checks!r})"


def _check(check_type, source_value, target_value, status, message):
    return {
        "check_type": check_type,
        "source_value": None if source_value is None else str(source_value),
        "target_value": None if target_value is None else str(target_value),
        "status": status,
        "message": message,
    }


def _rollup(checks):
    if any(c["status"] == FAIL for c in checks):
        return FAIL
    if any(c["status"] == WARN for c in checks):
        return WARN
    return PASS


def reconcile_full_load(source_count, bronze_count) -> ReconResult:
    """FULL_LOAD / DELTA_FULL_REFRESH: exact source and Bronze counts must match."""
    status = PASS if source_count == bronze_count else FAIL
    checks = [_check(FULL_SNAPSHOT_COUNT, source_count, bronze_count, status,
                     f"source={source_count} bronze={bronze_count}")]
    return ReconResult(_rollup(checks), checks)


def reconcile_watermark(extracted_count, applied_interval_count) -> ReconResult:
    """WATERMARK: rows applied in the frozen interval must equal the extracted slice.

    The Bronze count is scoped to the same ``(lower, upper]`` interval that was
    extracted and replaced - never the whole table - so this is not a
    ``>=`` comparison.
    """
    status = PASS if applied_interval_count == extracted_count else FAIL
    checks = [_check(DELTA_INTERVAL_COUNT, extracted_count, applied_interval_count,
                     status,
                     f"extracted={extracted_count} applied_in_interval={applied_interval_count}")]
    return ReconResult(_rollup(checks), checks)


def reconcile_hybrid(extracted_count, staged_count, duplicate_key_count,
                     missing_key_count) -> ReconResult:
    """HYBRID: bounded slice staged then MERGEd by primary key.

    PASS requires extracted == staged, zero duplicate staged keys, and every
    staged key present in Bronze after the merge. Complete source and Bronze
    counts are never compared.
    """
    checks = [
        _check(STAGE_COUNT, extracted_count, staged_count,
               PASS if extracted_count == staged_count else FAIL,
               f"extracted={extracted_count} staged={staged_count}"),
        _check(DUPLICATE_PRIMARY_KEY, None, duplicate_key_count,
               PASS if duplicate_key_count == 0 else FAIL,
               f"duplicate_staged_keys={duplicate_key_count}"),
        _check(MERGED_KEY_EXISTENCE, None, missing_key_count,
               PASS if missing_key_count == 0 else FAIL,
               f"staged_keys_missing_in_bronze={missing_key_count}"),
    ]
    return ReconResult(_rollup(checks), checks)


def reconcile_primary_key(extracted_count, staged_count, duplicate_key_count,
                          missing_key_count, delete_policy,
                          bronze_total_count) -> ReconResult:
    """PRIMARY_KEY: complete source snapshot staged then MERGEd by key.

    Validates extracted == staged, zero duplicate keys, and every staged key
    present in Bronze. HARD_DELETE additionally requires the complete Bronze
    count to equal the source snapshot; IGNORE_DELETES may leave extra Bronze
    rows, which are reported (WARN) rather than failed.
    """
    checks = [
        _check(STAGE_COUNT, extracted_count, staged_count,
               PASS if extracted_count == staged_count else FAIL,
               f"extracted={extracted_count} staged={staged_count}"),
        _check(DUPLICATE_PRIMARY_KEY, None, duplicate_key_count,
               PASS if duplicate_key_count == 0 else FAIL,
               f"duplicate_staged_keys={duplicate_key_count}"),
        _check(MERGED_KEY_EXISTENCE, None, missing_key_count,
               PASS if missing_key_count == 0 else FAIL,
               f"staged_keys_missing_in_bronze={missing_key_count}"),
    ]
    policy = (delete_policy or "").upper()
    if policy == "HARD_DELETE":
        checks.append(_check(
            FULL_SNAPSHOT_COUNT, extracted_count, bronze_total_count,
            PASS if bronze_total_count == extracted_count else FAIL,
            f"source_snapshot={extracted_count} bronze_total={bronze_total_count}"))
    else:
        # IGNORE_DELETES: extra Bronze rows are expected/allowed; only fewer than
        # the snapshot is suspicious (missing-key check already covers that).
        extra = (bronze_total_count or 0) - (extracted_count or 0)
        status = WARN if extra > 0 else PASS
        checks.append(_check(
            FULL_SNAPSHOT_COUNT, extracted_count, bronze_total_count, status,
            f"source_snapshot={extracted_count} bronze_total={bronze_total_count} "
            f"extra_retained={extra} (IGNORE_DELETES)"))
    return ReconResult(_rollup(checks), checks)
