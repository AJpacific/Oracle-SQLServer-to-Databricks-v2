"""Source-neutral contracts and validation for source type mappers."""

from __future__ import annotations

import os
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional


AUTO = "AUTO"
REVIEW = "REVIEW"
BLOCKED = "BLOCKED"

EXACT = "EXACT"
WIDENED = "WIDENED"
LOSSY = "LOSSY"
UNKNOWN = "UNKNOWN"

VALID_STATUSES = frozenset({AUTO, REVIEW, BLOCKED})
VALID_FIDELITIES = frozenset({EXACT, WIDENED, LOSSY, UNKNOWN})


@dataclass(frozen=True)
class ColumnMappingResult:
    source_type: str
    databricks_delta_type: Optional[str]
    status: str
    fidelity: str
    notes: str
    is_nullable: bool


class SourceTypeMapper(ABC):
    @abstractmethod
    def map_column(self, source_type, precision=None, scale=None,
                   length=None, is_nullable=True) -> ColumnMappingResult:
        ...


def classify_table_compatibility(statuses):
    """Roll column statuses up without applying any source-specific policy."""
    normalized = [str(status or "").strip().upper()
                  for status in (statuses or [])]
    if not normalized:
        return "UNABLE_TO_ASSESS"
    if any(status == BLOCKED for status in normalized):
        return "MANUAL"
    if any(status == REVIEW for status in normalized):
        return REVIEW
    if all(status == AUTO for status in normalized):
        return "COMPATIBLE"
    return REVIEW


def validate_rule_overrides(rules, normalize_family):
    """Validate and normalize an explicit mapper rule dictionary."""
    normalized = {}
    for raw_family, raw_rule in (rules or {}).items():
        family = normalize_family(raw_family)
        if not family:
            raise ValueError("type mapping family must not be blank")
        if not isinstance(raw_rule, dict):
            raise ValueError(f"type mapping {raw_family!r} must be an object")
        status = str(raw_rule.get("status") or "").strip().upper()
        fidelity = str(raw_rule.get("fidelity") or "").strip().upper()
        if status not in VALID_STATUSES:
            raise ValueError(
                f"type mapping {raw_family!r} has invalid status {status!r}")
        if fidelity not in VALID_FIDELITIES:
            raise ValueError(
                f"type mapping {raw_family!r} has invalid fidelity {fidelity!r}")
        normalized[family] = {
            "databricks_delta": raw_rule.get("databricks_delta"),
            "status": status,
            "fidelity": fidelity,
            "notes": raw_rule.get("notes") or "",
        }
    return normalized


def load_rules_from_yaml(path, expected_source, normalize_source,
                         normalize_family):
    """Load one source-qualified YAML rule file with strict metadata checks."""
    import yaml

    display_path = os.path.basename(path or "") or "<unspecified>"
    try:
        with open(path, "r", encoding="utf-8") as stream:
            document = yaml.safe_load(stream)
    except yaml.YAMLError as exc:
        raise ValueError(
            f"Malformed type-rules YAML {display_path!r}: "
            f"{type(exc).__name__}") from None
    except OSError as exc:
        raise FileNotFoundError(
            f"Unable to read type-rules YAML {display_path!r}: "
            f"{type(exc).__name__}") from None

    if not isinstance(document, dict):
        raise ValueError(
            f"Type-rules YAML {display_path!r} must contain an object")

    source_dialect = document.get("source_dialect")
    if source_dialect is None or not str(source_dialect).strip():
        raise ValueError(
            f"Type-rules YAML {display_path!r} requires source_dialect")
    try:
        actual_source = normalize_source(source_dialect)
    except ValueError:
        raise ValueError(
            f"Type-rules YAML {display_path!r} has unsupported "
            f"source_dialect {source_dialect!r}") from None
    if actual_source != expected_source:
        raise ValueError(
            f"Type-rules YAML {display_path!r} source_dialect "
            f"{source_dialect!r} does not match adapter {expected_source!r}")

    target = str(document.get("target") or "").strip().lower()
    if target != "databricks_delta":
        raise ValueError(
            f"Type-rules YAML {display_path!r} target must be "
            "'databricks_delta'")

    raw_rules = document.get("types")
    if not isinstance(raw_rules, dict):
        raise ValueError(
            f"Type-rules YAML {display_path!r} requires a types object")
    return validate_rule_overrides(raw_rules, normalize_family)