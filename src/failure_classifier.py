"""
failure_classifier.py - pure failure classification for retry decisions.

Turns an exception raised at a known pipeline stage into a sanitized error
category and an automatic-retry-eligibility flag, using simple exception/message
patterns. It never includes secret values or credential-bearing URLs in the
returned message. Fully unit-testable with no Spark.
"""

from __future__ import annotations

import re

# ---- failure stages (shared by INGEST and ETL) -----------------------------
CONNECTION = "CONNECTION"
METADATA = "METADATA"
MAPPING = "MAPPING"
PROVISIONING = "PROVISIONING"
SOURCE_READ = "SOURCE_READ"
TARGET_WRITE = "TARGET_WRITE"
RECONCILIATION = "RECONCILIATION"
CHECKPOINT = "CHECKPOINT"
QUEUE_FINALIZATION = "QUEUE_FINALIZATION"
ETL_READ = "ETL_READ"
DQ_VALIDATION = "DQ_VALIDATION"
SILVER_WRITE = "SILVER_WRITE"
ETL_RECONCILIATION = "ETL_RECONCILIATION"
UNKNOWN = "UNKNOWN"

FAILURE_STAGES = {
    CONNECTION, METADATA, MAPPING, PROVISIONING, SOURCE_READ, TARGET_WRITE,
    RECONCILIATION, CHECKPOINT, QUEUE_FINALIZATION, ETL_READ, DQ_VALIDATION,
    SILVER_WRITE, ETL_RECONCILIATION, UNKNOWN,
}

# ---- error categories ------------------------------------------------------
TRANSIENT_CONNECTION = "TRANSIENT_CONNECTION"
TRANSIENT_COMPUTE = "TRANSIENT_COMPUTE"
TIMEOUT = "TIMEOUT"
SOURCE_PERMISSION = "SOURCE_PERMISSION"
SOURCE_OBJECT_MISSING = "SOURCE_OBJECT_MISSING"
MAPPING_ERROR = "MAPPING_ERROR"
TARGET_PERMISSION = "TARGET_PERMISSION"
TARGET_WRITE_ERROR = "TARGET_WRITE_ERROR"
RECONCILIATION_ERROR = "RECONCILIATION_ERROR"
CHECKPOINT_ERROR = "CHECKPOINT_ERROR"
CONFIGURATION_ERROR = "CONFIGURATION_ERROR"
DQ_ERROR = "DQ_ERROR"

# Categories eligible for automatic retry. TARGET_WRITE_ERROR is retryable only
# when the operation is idempotent (full-load overwrite / MERGE), decided below.
_ALWAYS_RETRYABLE = {TRANSIENT_CONNECTION, TRANSIENT_COMPUTE, TIMEOUT}

# ---- retry selection policy ------------------------------------------------
INGEST_OPERATIONS = frozenset({
    "FULL_LOAD",
    "DELTA_SYNC",
    "DELTA_APPEND",
    "DELTA_MERGE",
    "DELTA_FULL_REFRESH",
    "CHECKPOINT_RECOVERY",
    "QUEUE_FINALIZATION_RECOVERY",
})
ETL_OPERATIONS = frozenset({"ETL", "ETL_FULL", "ETL_INCREMENTAL"})
PIPELINE_OPERATIONS = {
    "INGEST": INGEST_OPERATIONS,
    "ETL": ETL_OPERATIONS,
}

RETRY_CHECKPOINT_ONLY = "RETRY_CHECKPOINT_ONLY"
RETRY_QUEUE_FINALIZATION_ONLY = "RETRY_QUEUE_FINALIZATION_ONLY"
RETRY_FULL_LOAD = "RETRY_FULL_LOAD"
RETRY_DELTA_APPLY = "RETRY_DELTA_APPLY"
RETRY_ETL = "RETRY_ETL"
MANUAL_REVIEW = "MANUAL_REVIEW"

NOT_RETRY_ELIGIBLE = "NOT_RETRY_ELIGIBLE"
MAX_RETRIES_REACHED = "MAX_RETRIES_REACHED"
UNKNOWN_RECOVERY_ACTION = "UNKNOWN_RECOVERY_ACTION"
MISSING_RECOVERY_PREREQUISITE = "MISSING_RECOVERY_PREREQUISITE"
PIPELINE_OPERATION_MISMATCH = "PIPELINE_OPERATION_MISMATCH"

SAFE_STATE_RECOVERY_ACTIONS = frozenset({
    RETRY_CHECKPOINT_ONLY,
    RETRY_QUEUE_FINALIZATION_ONLY,
})

WORKLIST_FIELDS = (
    "run_id", "parent_run_id", "connection_id", "source_table_id",
    "pipeline_name", "operation", "previous_attempt_number",
    "attempt_number", "failure_stage", "error_category", "recovery_action",
    "retry_lower_watermark", "retry_upper_watermark",
)

MANUAL_REVIEW_FIELDS = (
    "source_table_id", "connection_id", "operation", "failure_stage",
    "error_category", "previous_attempt_number", "reason",
)


class FailureClassification:
    def __init__(self, category, retry_eligible, sanitized_message, stage):
        self.category = category
        self.retry_eligible = retry_eligible
        self.sanitized_message = sanitized_message
        self.stage = stage

    def as_dict(self):
        return {
            "error_category": self.category,
            "retry_eligible": self.retry_eligible,
            "failure_stage": self.stage,
            "error_message": self.sanitized_message,
        }

    def __repr__(self):
        return (f"FailureClassification(category={self.category!r}, "
                f"retry_eligible={self.retry_eligible}, stage={self.stage!r})")


def sanitize_message(message) -> str:
    """Strip credentials from an error message before it is stored or logged.

    Covers JDBC/connection-string properties, query-string tokens, HTTP
    Authorization headers (Basic/Bearer), and URL userinfo. Non-sensitive text
    such as host, database, schema, and table names is left readable.
    """
    if message is None:
        return ""
    s = str(message)
    # key=value and key: value connection/query properties.
    s = re.sub(
        r"(?i)\b(password|pwd|passwd|user|username|uid|token|access_token|"
        r"refresh_token|id_token|secret|client_secret|clientsecret|"
        r"secret_value|apikey|api_key|sas|signature|sig|key|credential)\b"
        r"(\s*[=:]\s*)[^;&,\s\"']+",
        r"\1\2***", s)
    # HTTP Authorization headers.
    s = re.sub(r"(?i)\b(Authorization\s*:\s*)(Basic|Bearer)\s+\S+",
               r"\1\2 ***", s)
    s = re.sub(r"(?i)\b(Basic|Bearer)\s+[A-Za-z0-9\-._~+/=]{8,}",
               r"\1 ***", s)
    # URL userinfo (scheme://user:pass@host).
    s = re.sub(r"//[^/@\s]*@", "//***@", s)
    # Webhook URLs: keep the host and the recognizable path segment, drop the
    # token-bearing remainder.
    s = re.sub(r"(?i)(https?://\S*?/(?:webhook|incomingwebhook|services|hooks)/)"
               r"[^\s]*", r"\1***", s)
    return s[:2000]


def redact_url(url) -> str:
    """Return a log-safe URL with credentials and query tokens removed."""
    if not url:
        return ""
    s = sanitize_message(url)
    # Drop a query string entirely: it can carry an unnamed webhook token.
    return re.sub(r"\?\S*", "?***", s)


# Ordered (pattern, category) rules. First match wins.
_MESSAGE_RULES = [
    (r"time[d\s-]*out|timeout|ORA-12170", TIMEOUT),
    (r"out of memory|java\.lang\.OutOfMemoryError|executor lost|"
     r"container killed|job aborted|stage failure|SparkException", TRANSIENT_COMPUTE),
    (r"connection refused|connection reset|connection closed|no route to host|"
     r"could not connect|network is unreachable|socket|ORA-12541|ORA-12514|"
     r"TNS:listener|broken pipe", TRANSIENT_CONNECTION),
    (r"insufficient privileg|ORA-01031|permission denied|access denied|"
     r"login failed|not authorized|SELECT permission|does not have permission",
     SOURCE_PERMISSION),
    (r"table or view does not exist|ORA-00942|invalid object name|"
     r"cannot find|object not found|no such table", SOURCE_OBJECT_MISSING),
    (r"blocked datatype|unsupported .*type|requires an explicit mapping|"
     r"no approved AUTO columns|mapping", MAPPING_ERROR),
    (r"primary[_\s]key|missing primary|invalid watermark|no committed "
     r"last_watermark|non-temporal watermark|collision|configuration|config error",
     CONFIGURATION_ERROR),
]


def classify_failure(exception, stage, idempotent=True) -> FailureClassification:
    """Classify an exception at ``stage`` into (category, retry_eligible).

    Message patterns win first; otherwise the stage implies a category. Retry
    eligibility follows the accelerator's policy: transient connection/compute/
    timeout always retry; an idempotent target write may retry; permission,
    object-missing, mapping, reconciliation, checkpoint, configuration and DQ
    failures are never automatically retried.
    """
    if stage not in FAILURE_STAGES:
        stage = UNKNOWN
    msg = sanitize_message(getattr(exception, "args", None) and
                           " ".join(str(a) for a in exception.args) or exception)
    lowered = msg.lower()
    category = None
    for pattern, cat in _MESSAGE_RULES:
        if re.search(pattern, lowered, flags=re.IGNORECASE):
            # A permission hit during a target-write stage is a target permission.
            if cat == SOURCE_PERMISSION and stage in (TARGET_WRITE, SILVER_WRITE,
                                                      PROVISIONING):
                cat = TARGET_PERMISSION
            category = cat
            break

    if category is None:
        category = {
            CONNECTION: TRANSIENT_CONNECTION,
            METADATA: SOURCE_OBJECT_MISSING,
            MAPPING: MAPPING_ERROR,
            PROVISIONING: CONFIGURATION_ERROR,
            SOURCE_READ: TRANSIENT_CONNECTION,
            TARGET_WRITE: TARGET_WRITE_ERROR,
            RECONCILIATION: RECONCILIATION_ERROR,
            CHECKPOINT: CHECKPOINT_ERROR,
            QUEUE_FINALIZATION: CHECKPOINT_ERROR,
            ETL_READ: TARGET_WRITE_ERROR,
            DQ_VALIDATION: DQ_ERROR,
            SILVER_WRITE: TARGET_WRITE_ERROR,
            ETL_RECONCILIATION: RECONCILIATION_ERROR,
        }.get(stage, "UNKNOWN")

    retry_eligible = category in _ALWAYS_RETRYABLE or (
        category == TARGET_WRITE_ERROR and bool(idempotent))
    return FailureClassification(category, retry_eligible, msg, stage)


def recovery_action(operation, stage):
    """Map a failed (operation, stage) to a safe recovery action for retry.

    Checkpoint-only and queue-finalization-only retries must never reapply data;
    everything else replays the idempotent load. A non-retryable configuration
    problem is routed to MANUAL_REVIEW by the caller, not here.
    """
    op = (operation or "").upper()
    st = (stage or "").upper()
    if st == CHECKPOINT:
        return RETRY_CHECKPOINT_ONLY
    if st == QUEUE_FINALIZATION:
        return RETRY_QUEUE_FINALIZATION_ONLY
    if op in ETL_OPERATIONS or st in (
            ETL_READ, DQ_VALIDATION, SILVER_WRITE, ETL_RECONCILIATION):
        return RETRY_ETL
    if op == "FULL_LOAD":
        return RETRY_FULL_LOAD
    if op in ("DELTA_MERGE", "DELTA_APPEND", "DELTA_FULL_REFRESH", "DELTA_SYNC"):
        return RETRY_DELTA_APPLY
    return MANUAL_REVIEW


def _row_dict(row):
    if hasattr(row, "asDict"):
        return row.asDict(recursive=True)
    return dict(row or {})


def normalize_pipeline_name(value):
    """Return INGEST or ETL; an unknown pipeline never falls through."""
    token = str(value or "").strip().upper()
    if token not in PIPELINE_OPERATIONS:
        raise ValueError(
            f"Unsupported pipeline_name {value!r}; expected INGEST or ETL")
    return token


def operations_for_pipeline(pipeline_name):
    """Return the immutable operation set owned by one pipeline."""
    return PIPELINE_OPERATIONS[normalize_pipeline_name(pipeline_name)]


def validate_pipeline_operation(pipeline_name, operation):
    """Return a normalized operation or raise on a pipeline mismatch."""
    pipeline = normalize_pipeline_name(pipeline_name)
    token = str(operation or "").strip().upper()
    if not token:
        return ""
    if token not in PIPELINE_OPERATIONS[pipeline]:
        raise ValueError(
            f"operation {token!r} does not belong to pipeline {pipeline!r}")
    return token


def retry_work_identity(row):
    """Return the source-table and operation identity of one failed work row."""
    item = _row_dict(row)
    source_table_id = str(item.get("source_table_id") or "").strip()
    operation = str(item.get("operation") or "").strip().upper()
    if not source_table_id:
        raise ValueError("retry row requires source_table_id")
    if not operation:
        raise ValueError("retry row requires operation")
    return source_table_id, operation


def _previous_attempt(row):
    value = _row_dict(row).get("attempt_number")
    return int(value) if value is not None else 1


def _ordered_value(value):
    """Comparable key matching SQL DESC NULLS LAST for testable selection."""
    if value is None:
        return 0, ""
    if hasattr(value, "isoformat"):
        return 1, value.isoformat()
    return 1, str(value)


def _latest_attempt_rank(row):
    item = _row_dict(row)
    return (
        _previous_attempt(item),
        _ordered_value(item.get("ended_ts")),
        _ordered_value(item.get("started_ts")),
        str(item.get("run_id") or ""),
    )


def latest_failed_attempts(records):
    """Select the latest FAILED row per source_table_id and operation."""
    selected = {}
    for record in records or []:
        item = _row_dict(record)
        if str(item.get("status") or "FAILED").strip().upper() != "FAILED":
            continue
        identity = retry_work_identity(item)
        current = selected.get(identity)
        if current is None or _latest_attempt_rank(item) > _latest_attempt_rank(current):
            selected[identity] = item
    return [selected[key] for key in sorted(selected)]


def select_failed_attempts(records, pipeline_name, operation=""):
    """Apply exact pipeline/operation filtering and latest-attempt selection."""
    pipeline = normalize_pipeline_name(pipeline_name)
    operation_filter = validate_pipeline_operation(pipeline, operation)
    owned = PIPELINE_OPERATIONS[pipeline]
    filtered = []
    for record in records or []:
        item = _row_dict(record)
        row_operation = str(item.get("operation") or "").strip().upper()
        if row_operation not in owned:
            continue
        if operation_filter and row_operation != operation_filter:
            continue
        filtered.append(item)
    return latest_failed_attempts(filtered)


def _manual_review_item(row, previous_attempt, reason):
    item = _row_dict(row)
    source_table_id, operation = retry_work_identity(item)
    return {
        "source_table_id": source_table_id,
        "connection_id": item.get("connection_id"),
        "operation": operation,
        "failure_stage": item.get("failure_stage"),
        "error_category": item.get("error_category"),
        "previous_attempt_number": previous_attempt,
        "reason": reason,
    }


def build_retry_item(row, child_run_id, parent_run_id, pipeline_name,
                     max_retries):
    """Build one executable item or one non-executable manual-review item."""
    item = _row_dict(row)
    pipeline = normalize_pipeline_name(pipeline_name)
    source_table_id, operation = retry_work_identity(item)
    previous_attempt = _previous_attempt(item)
    if operation not in PIPELINE_OPERATIONS[pipeline]:
        return None, _manual_review_item(
            item, previous_attempt, PIPELINE_OPERATION_MISMATCH)

    stage = str(item.get("failure_stage") or "").strip().upper()
    action = recovery_action(operation, stage)
    if action == MANUAL_REVIEW:
        return None, _manual_review_item(
            item, previous_attempt, UNKNOWN_RECOVERY_ACTION)
    if ((action == RETRY_CHECKPOINT_ONLY and stage != CHECKPOINT)
            or (action == RETRY_QUEUE_FINALIZATION_ONLY
                and stage != QUEUE_FINALIZATION)):
        return None, _manual_review_item(
            item, previous_attempt, MISSING_RECOVERY_PREREQUISITE)

    retry_count = previous_attempt - 1
    if retry_count >= int(max_retries):
        return None, _manual_review_item(
            item, previous_attempt, MAX_RETRIES_REACHED)

    eligible = bool(item.get("retry_eligible"))
    if not eligible and action not in SAFE_STATE_RECOVERY_ACTIONS:
        return None, _manual_review_item(
            item, previous_attempt, NOT_RETRY_ELIGIBLE)

    work_item = {
        "run_id": child_run_id,
        "parent_run_id": parent_run_id,
        "connection_id": item.get("connection_id"),
        "source_table_id": source_table_id,
        "pipeline_name": pipeline,
        "operation": operation,
        "previous_attempt_number": previous_attempt,
        "attempt_number": previous_attempt + 1,
        "failure_stage": item.get("failure_stage"),
        "error_category": item.get("error_category"),
        "recovery_action": action,
        "retry_lower_watermark": item.get("lower_watermark"),
        "retry_upper_watermark": item.get("upper_watermark"),
    }
    return work_item, None


def deduplicate_retry_items(items):
    """Keep one deterministic latest item per table/operation/action identity."""
    selected = {}
    duplicate_keys = set()
    for raw_item in items or []:
        item = dict(raw_item)
        key = (
            item.get("source_table_id"),
            item.get("operation"),
            item.get("recovery_action"),
        )
        current = selected.get(key)
        if current is not None:
            duplicate_keys.add(key)
            rank = (int(item.get("previous_attempt_number") or 1),
                    int(item.get("attempt_number") or 2),
                    str(item.get("parent_run_id") or ""))
            current_rank = (
                int(current.get("previous_attempt_number") or 1),
                int(current.get("attempt_number") or 2),
                str(current.get("parent_run_id") or ""),
            )
            if rank > current_rank:
                selected[key] = item
        else:
            selected[key] = item
    return ([selected[key] for key in sorted(selected)],
            sorted(duplicate_keys))


def build_retry_collections(rows, child_run_id, parent_run_id, pipeline_name,
                            max_retries):
    """Separate executable work from manual review and detect duplicates."""
    work_items = []
    manual_items = []
    for row in rows or []:
        work_item, manual_item = build_retry_item(
            row, child_run_id, parent_run_id, pipeline_name, max_retries)
        if work_item is not None:
            work_items.append(work_item)
        if manual_item is not None:
            manual_items.append(manual_item)
    worklist, duplicates = deduplicate_retry_items(work_items)
    return worklist, manual_items, duplicates
