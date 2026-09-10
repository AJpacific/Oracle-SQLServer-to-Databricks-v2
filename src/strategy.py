"""
strategy.py - decide how each Oracle or SQL Server table should be loaded.

Strategies:
  FULL_LOAD     : no reliable way to detect change -> always full replace.
  WATERMARK     : a reliable temporal change-tracking column exists but no PK
                  -> append rows newer than the last watermark.
  PRIMARY_KEY   : a primary key exists but no reliable watermark -> MERGE by PK.
  HYBRID        : both a PK and a reliable watermark exist -> watermark fetches
                  the delta, MERGE by PK applies it (handles updates + inserts).

A watermark must be a *temporal* column. Eligibility is delegated to the
source adapter: Oracle uses DATE/TIMESTAMP families and SQL Server uses
DATE/DATETIME/SMALLDATETIME/DATETIME2/DATETIMEOFFSET. SQL Server timestamp and
rowversion are binary tokens and are never temporal watermarks. Non-temporal
types (NUMBER, IDs, quantities, prices, VARCHAR2, INTERVAL, ...) are never
eligible. Column names only influence *ranking*, never eligibility, so
CREATED_DATE, ORDER_DATE, SNAPSHOT_DATE and custom temporal columns all remain
selectable when no stronger update/change-named temporal column exists.

Pure module: no Spark, no dbutils. Fully unit-testable.
"""

from __future__ import annotations

import re
from typing import List, Optional


FULL_LOAD = "FULL_LOAD"
WATERMARK = "WATERMARK"
PRIMARY_KEY = "PRIMARY_KEY"
HYBRID = "HYBRID"

VALID_STRATEGIES = {FULL_LOAD, WATERMARK, PRIMARY_KEY, HYBRID}

# Only Oracle *temporal* types may be auto-considered as watermarks. Family
# names match _family() output (parenthesised precision stripped). No numeric,
# character, LOB, or interval type is eligible. The supported temporal datatype
# alone determines eligibility; column-name semantics influence ranking only, so
# every supported temporal column remains eligible regardless of its name.
WATERMARK_CANDIDATE_TYPES = {
    "DATE",
    "TIMESTAMP",
    "TIMESTAMP WITH TIME ZONE",
    "TIMESTAMP WITH LOCAL TIME ZONE",
}

# Column-name tokens that raise a temporal column's *ranking* score. Names are
# ranking hints ONLY - they never make a column eligible or ineligible. Any
# supported temporal datatype is eligible regardless of its name.
_UPDATE_TOKENS = {
    "UPDATE", "UPDATED", "MODIFY", "MODIFIED", "CHANGE", "CHANGED",
    "AUDIT", "REVISION", "SYNC",
}
_CREATE_TOKENS = {
    "CREATE", "CREATED", "INSERT", "INSERTED", "LOAD", "INGEST", "INGESTED",
}

# Timezone/datatype ranking category (highest priority = lowest number).
_TZ_CATEGORY_RANK = {
    "TIMESTAMP WITH TIME ZONE": 0,
    "TIMESTAMP WITH LOCAL TIME ZONE": 1,
    "TIMESTAMP": 2,
    "DATE": 3,
}


def _family(oracle_type: str) -> str:
    t = (oracle_type or "").strip().upper()
    t = re.sub(r"\((\s*\d+\s*)\)", "", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t


def normalize_watermark_type(oracle_type: str) -> str:
    """Normalize an Oracle watermark type declaration to its family form.

    Strips optional precision, uppercases, and collapses whitespace, e.g.
    'timestamp(6) with time zone' -> 'TIMESTAMP WITH TIME ZONE'.
    """
    return _family(oracle_type)


def is_supported_watermark_type(oracle_type: str) -> bool:
    """True only for supported Oracle temporal watermark families."""
    return normalize_watermark_type(oracle_type) in WATERMARK_CANDIDATE_TYPES


def oracle_watermark_type_rank(oracle_type: str) -> int:
    """Category rank for an Oracle temporal family (lower wins)."""
    return _TZ_CATEGORY_RANK.get(normalize_watermark_type(oracle_type), 99)


class _OracleWatermarkPolicy:
    """Default (Oracle) temporal-watermark policy.

    A *watermark policy* is the small source-specific contract the ranking logic
    below depends on: which datatypes are temporal, how to normalize a type to a
    family, and the family ranking. Passing ``source=<adapter>`` to the public
    functions swaps in a different dialect's policy (e.g. SQL Server) while the
    shared name-semantic scoring and precision tie-breaks stay identical.
    """

    def is_supported_watermark_type(self, data_type):
        return is_supported_watermark_type(data_type)

    def normalize_watermark_type(self, data_type):
        return normalize_watermark_type(data_type)

    def watermark_type_rank(self, data_type):
        return oracle_watermark_type_rank(data_type)


ORACLE_WATERMARK_POLICY = _OracleWatermarkPolicy()


def _resolve_policy(source):
    """Return the watermark policy to use (the Oracle default when None)."""
    return source if source is not None else ORACLE_WATERMARK_POLICY


def _temporal_precision(oracle_type: str) -> Optional[int]:
    """Declared fractional-seconds precision, e.g. TIMESTAMP(6) -> 6, else None."""
    m = re.search(r"\(\s*(\d+)\s*\)", oracle_type or "")
    return int(m.group(1)) if m else None


def _norm_name(name: str) -> str:
    """Uppercase, trim, unify space/hyphen to underscore, collapse repeats."""
    n = (name or "").strip().upper()
    n = n.replace(" ", "_").replace("-", "_")
    n = re.sub(r"_+", "_", n)
    return n.strip("_")


def _name_tokens(name: str):
    """Tokenize a column name: split camelCase, uppercase, split on non-alnum."""
    s = name or ""
    s = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", s)
    s = s.upper()
    s = re.sub(r"[^A-Z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return [t for t in s.split("_") if t]


def _semantic_score(name: str) -> int:
    """Ranking hint only (never eligibility): 0=update/change, 1=create, 2=other."""
    toks = set(_name_tokens(name))
    if toks & _UPDATE_TOKENS:
        return 0
    if toks & _CREATE_TOKENS:
        return 1
    return 2


def _effective_precision(column: dict) -> int:
    """Fractional-seconds precision from an explicit field or the type string."""
    p = column.get("datetime_precision")
    if isinstance(p, int):
        return p
    return _temporal_precision(column.get("data_type")) or 0


def _sort_key(column: dict, policy=None):
    policy = _resolve_policy(policy)
    fam = policy.normalize_watermark_type(column.get("data_type"))
    precision = _effective_precision(column)
    ordinal = column.get("ordinal_position")
    ordinal = ordinal if isinstance(ordinal, int) else 10 ** 9
    # Composite ascending rank (lower wins): timezone/datatype category, semantic
    # name score, greater fractional precision, lower ordinal, then normalized
    # name as an absolute deterministic tie-break.
    return (policy.watermark_type_rank(column.get("data_type")),
            _semantic_score(column.get("column_name")),
            -precision, ordinal, _norm_name(column.get("column_name")))


def pick_watermark_column(columns: List[dict], policy=None) -> Optional[dict]:
    """Pick the best temporal watermark column from a list of column dicts.

    Eligibility is based purely on the source datatype: every supported temporal
    type is eligible regardless of its name. Non-temporal types (numeric, ID,
    quantity, price, character, INTERVAL, ...) are never eligible. Names only
    influence ranking, never eligibility. Pass ``policy=<adapter>`` to rank with
    a specific source dialect; the default is Oracle.

    Each column dict must have at least: column_name, data_type. Optional
    'ordinal_position' is used as a deterministic tie-breaker.

    Returns the chosen column dict (copied, with an added 'reason') or None.
    """
    policy = _resolve_policy(policy)
    ranked = []
    for c in columns or []:
        if not policy.is_supported_watermark_type(c.get("data_type")):
            continue
        ranked.append((_sort_key(c, policy), c))

    if not ranked:
        return None

    ranked.sort(key=lambda t: t[0])
    _key, chosen_col = ranked[0]
    fam = policy.normalize_watermark_type(chosen_col.get("data_type"))
    score = _semantic_score(chosen_col.get("column_name"))
    label = {0: "update/change", 1: "create/insert", 2: "other-temporal"}[score]
    chosen = dict(chosen_col)
    chosen["reason"] = (
        f"temporal type {fam}; name semantic '{label}' (score {score}); "
        f"precision {_effective_precision(chosen_col)}; "
        f"ordinal {chosen_col.get('ordinal_position')}"
    )
    return chosen


def validate_configured_watermark(columns: List[dict],
                                  configured_name: str, policy=None) -> Optional[dict]:
    """Return the configured column dict if it is a valid temporal watermark.

    The stored watermark_column is matched case-insensitively and honored only
    when the column exists and its source type is a supported temporal type -
    regardless of the column name. Numeric/string/ID/quantity/price or missing
    columns are rejected so automatic discovery can repair the value.
    """
    policy = _resolve_policy(policy)
    if not configured_name:
        return None
    target = (configured_name or "").strip().upper()
    for c in columns or []:
        if (c.get("column_name") or "").strip().upper() != target:
            continue
        if not policy.is_supported_watermark_type(c.get("data_type")):
            return None
        fam = policy.normalize_watermark_type(c.get("data_type"))
        chosen = dict(c)
        chosen["reason"] = f"configured override '{configured_name}'; temporal type {fam}"
        return chosen
    return None


def select_watermark(columns: List[dict], configured_watermark: Optional[str] = None,
                     policy=None):
    """Return (chosen_column_or_None, source).

    source is 'CONFIGURED' when a valid stored watermark is honored,
    'DISCOVERED' when ranking selected one, or None when no temporal column
    exists. A configured value that is invalid/non-temporal is ignored and
    automatic discovery runs.
    """
    policy = _resolve_policy(policy)
    if configured_watermark:
        wm = validate_configured_watermark(columns, configured_watermark, policy)
        if wm is not None:
            return wm, "CONFIGURED"
    wm = pick_watermark_column(columns, policy)
    return (wm, "DISCOVERED" if wm is not None else None)


def resolve_watermark_decision(columns: List[dict], primary_key_columns: List[str],
                               configured_watermark: Optional[str] = None,
                               source=None) -> dict:
    """Resolve the full watermark + strategy decision in one place.

    ``source`` is an optional source adapter / watermark policy that decides
    which datatypes are temporal for this row's dialect (the default is Oracle).
    Returns a dict: strategy, watermark_column, watermark_data_type (normalized),
    source ('CONFIGURED'|'DISCOVERED'|None), reason, source_type (raw source
    type of the selected column).
    """
    policy = _resolve_policy(source)
    pk = [c for c in (primary_key_columns or []) if c]
    wm, wm_source = select_watermark(columns, configured_watermark, policy)
    has_pk = len(pk) > 0
    if wm is not None:
        return {
            "strategy": HYBRID if has_pk else WATERMARK,
            "watermark_column": wm["column_name"],
            "watermark_data_type": policy.normalize_watermark_type(wm["data_type"]),
            "source": wm_source,
            "reason": wm.get("reason"),
            "source_type": wm.get("data_type"),
        }
    return {
        "strategy": PRIMARY_KEY if has_pk else FULL_LOAD,
        "watermark_column": None,
        "watermark_data_type": None,
        "source": None,
        "reason": None,
        "source_type": None,
    }


def detect_strategy(columns: List[dict], primary_key_columns: List[str],
                    configured_watermark: Optional[str] = None, source=None):
    """Return (strategy, watermark_column_or_None, watermark_family_or_None).

    Thin wrapper over resolve_watermark_decision preserving the legacy 3-tuple.
    columns: list of {column_name, data_type, ordinal_position?}
    primary_key_columns: ordered list of PK column names (may be empty).
    configured_watermark: a previously stored watermark_column, honored only
        when still a valid temporal type; invalid values are ignored/cleared.
    source: optional source adapter / watermark policy (default Oracle).
    """
    d = resolve_watermark_decision(columns, primary_key_columns,
                                   configured_watermark, source)
    return d["strategy"], d["watermark_column"], d["watermark_data_type"]


def is_valid_strategy(strategy: str) -> bool:
    return strategy in VALID_STRATEGIES
