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
        return "RETRY_CHECKPOINT_ONLY"
    if st == QUEUE_FINALIZATION:
        return "RETRY_QUEUE_FINALIZATION_ONLY"
    if op in ("ETL_FULL", "ETL_INCREMENTAL") or st in (
            ETL_READ, DQ_VALIDATION, SILVER_WRITE, ETL_RECONCILIATION):
        return "RETRY_ETL"
    if op == "FULL_LOAD":
        return "RETRY_FULL_LOAD"
    if op in ("DELTA_MERGE", "DELTA_APPEND", "DELTA_FULL_REFRESH", "DELTA_SYNC"):
        return "RETRY_DELTA_APPLY"
    return "MANUAL_REVIEW"
