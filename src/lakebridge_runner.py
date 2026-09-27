"""lakebridge_runner.py - Isolated process runner for Lakebridge Analyzer & BladeBridge Transpiler.

Invoked with the isolated Python interpreter inside the seeded uv virtual environment.
Exchanges structured requests and responses through JSON files in per-artifact attempt directories.
Never passes source SQL in CLI arguments.
Never executes or deploys converted SQL.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import pathlib
import re
import sys
from typing import Any, Dict, List, Optional, Tuple

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_HERE)
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

try:
    from src.failure_classifier import sanitize_message
except ModuleNotFoundError:
    try:
        from failure_classifier import sanitize_message
    except ModuleNotFoundError:
        def sanitize_message(message: Any) -> str:
            """Fallback repository-standard error sanitizer."""
            if message is None:
                return ""
            s = str(message)
            s = re.sub(
                r"(?i)\b(password|pwd|passwd|user|username|uid|token|access_token|"
                r"refresh_token|id_token|secret|client_secret|clientsecret|"
                r"secret_value|apikey|api_key|sas|signature|sig|key|credential)\b"
                r"(\s*[=:]\s*)[^;&,\s\"']+",
                r"\1\2***", s)
            s = re.sub(r"(?i)\b(Authorization\s*:\s*)(Basic|Bearer)\s+\S+", r"\1\2 ***", s)
            s = re.sub(r"(?i)\b(Basic|Bearer)\s+[A-Za-z0-9\-._~+/=]{8,}", r"\1 ***", s)
            s = re.sub(r"//[^/@\s]*@", "//***@", s)
            s = re.sub(r"(?i)(https?://\S*?/(?:webhook|incomingwebhook|services|hooks)/)[^\s]*", r"\1***", s)
            s = re.sub(r"(?i)([?&](?:sig|signature|token|key|code|password)=)[^&\s]+", r"\1***", s)
            return s[:2000]

try:
    from src.lakebridge_artifact_common import (
        apply_text_edits,
        extract_sql_from_bladebridge_mime,
        normalize_diagnostic_severity,
        get_analyzer_platform,
        get_bladebridge_tech,
    )
except ModuleNotFoundError:
    from lakebridge_artifact_common import (
        apply_text_edits,
        extract_sql_from_bladebridge_mime,
        normalize_diagnostic_severity,
        get_analyzer_platform,
        get_bladebridge_tech,
    )


def sanitize_error(exc: Any) -> str:
    """Sanitize error using repository standard sanitizer."""
    return sanitize_message(exc)[:2000]


def run_check_environment() -> Dict[str, Any]:
    res = {
        "status": "SUCCEEDED",
        "python_version": sys.version.split()[0],
        "executable": sys.executable,
    }
    try:
        from databricks.labs.bladebridge.transpiler import Transpiler
        if callable(getattr(Transpiler, "transpile", None)):
            res["bladebridge"] = "available"
        else:
            res["bladebridge_error"] = "Transpiler.transpile is not callable"
    except Exception as e:
        res["bladebridge_error"] = sanitize_error(e)

    try:
        from databricks.labs.bladespector.analyzer import Analyzer
        import inspect
        fn = getattr(Analyzer, "analyze", None)
        if fn and callable(fn):
            sig = inspect.signature(fn)
            if len(sig.parameters) >= 3:
                res["analyzer"] = "available"
            else:
                res["analyzer_error"] = f"Analyzer.analyze signature incompatible: {list(sig.parameters.keys())}"
        else:
            res["analyzer_error"] = "Analyzer.analyze is not callable"
    except Exception as e:
        res["analyzer_error"] = sanitize_error(e)

    if res.get("bladebridge") != "available" or res.get("analyzer") != "available":
        res["status"] = "FAILED"
    return res


def run_analyze(request: Dict[str, Any]) -> Dict[str, Any]:
    input_dir = request.get("input_dir")
    report_xlsx = request.get("report_xlsx_path")
    report_json = request.get("report_json_path")
    source_sys = request.get("source_system")
    is_debug = bool(request.get("is_debug", False))

    if not input_dir or not os.path.isdir(input_dir):
        raise ValueError(f"Input directory does not exist: {input_dir}")
    if not report_xlsx:
        raise ValueError("report_xlsx_path is required")

    platform = get_analyzer_platform(source_sys)

    from databricks.labs.bladespector.analyzer import Analyzer
    import inspect

    analyzer_inst = Analyzer()
    analyze_fn = getattr(analyzer_inst, "analyze", Analyzer.analyze)
    sig = inspect.signature(analyze_fn)

    os.makedirs(os.path.dirname(report_xlsx), exist_ok=True)
    if report_json:
        os.makedirs(os.path.dirname(report_json), exist_ok=True)

    kwargs: Dict[str, Any] = {}
    if "is_debug" in sig.parameters:
        kwargs["is_debug"] = is_debug
    if "json_result" in sig.parameters and report_json:
        kwargs["json_result"] = pathlib.Path(report_json)

    analyze_fn(
        pathlib.Path(input_dir),
        pathlib.Path(report_xlsx),
        platform,
        **kwargs,
    )

    xlsx_exists = os.path.isfile(report_xlsx) and os.path.getsize(report_xlsx) > 0
    json_exists = bool(report_json and os.path.isfile(report_json) and os.path.getsize(report_json) > 0)

    if not xlsx_exists and not json_exists:
        raise RuntimeError(f"Analyzer finished but report was not generated at {report_xlsx}")

    return {
        "status": "SUCCEEDED",
        "platform": platform,
        "report_xlsx_path": report_xlsx if xlsx_exists else None,
        "report_json_path": report_json if json_exists else None,
        "xlsx_generated": xlsx_exists,
        "json_generated": json_exists,
    }


def run_transpile(request: Dict[str, Any]) -> Dict[str, Any]:
    source_file = request.get("source_file")
    output_file = request.get("output_file")
    source_sys = request.get("source_system")

    if not source_file or not os.path.isfile(source_file):
        raise ValueError(f"Source file not found: {source_file}")
    if not output_file:
        raise ValueError("output_file path is required")

    with open(source_file, "r", encoding="utf-8") as f:
        source_sql = f.read()

    if not source_sql.strip():
        raise ValueError(f"Source SQL file is empty: {source_file}")

    source_tech, target_tech = get_bladebridge_tech(source_sys)

    from databricks.labs.bladebridge.transpiler import Transpiler

    transpiler = Transpiler(source_tech=source_tech, target_tech=target_tech)
    fname = os.path.basename(source_file)

    edits, diagnostics = asyncio.run(transpiler.transpile(fname, source_sql))

    if not edits:
        raise RuntimeError(f"BladeBridge produced 0 TextEdits for {fname}")

    # 1. Apply LSP TextEdits to original source SQL
    applied_text = apply_text_edits(source_sql, edits)
    if not applied_text.strip():
        raise RuntimeError(f"Applying BladeBridge TextEdits resulted in empty output for {fname}")

    # 2. Extract clean SQL from MIME multipart envelope if present
    clean_sql = extract_sql_from_bladebridge_mime(applied_text)

    # 3. Write clean SQL to output file
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    with open(output_file, "w", encoding="utf-8") as out_f:
        out_f.write(clean_sql)

    # 4. Normalize diagnostics
    diag_errors = 0
    diag_warnings = 0
    diag_info = 0
    diag_hints = 0
    diag_list: List[Dict[str, str]] = []

    for d in diagnostics or []:
        raw_msg = getattr(d, "message", None)
        if raw_msg is None:
            raw_msg = str(d)
        raw_sev = getattr(d, "severity", None)
        norm_sev = normalize_diagnostic_severity(raw_sev)

        if norm_sev == "ERROR":
            diag_errors += 1
        elif norm_sev == "WARNING":
            diag_warnings += 1
        elif norm_sev == "HINT":
            diag_hints += 1
        else:
            diag_info += 1

        diag_list.append({
            "message": sanitize_error(raw_msg)[:500],
            "severity": norm_sev,
        })

    # 5. Count fixmes in clean SQL only
    fixme_count = len(re.findall(r"\bFIXME\b", clean_sql, re.I))

    return {
        "status": "SUCCEEDED",
        "source_tech": source_tech,
        "target_tech": target_tech,
        "output_file": output_file,
        "edit_count": len(edits),
        "fixme_count": fixme_count,
        "diagnostic_error_count": diag_errors,
        "diagnostic_warning_count": diag_warnings,
        "diagnostic_information_count": diag_info,
        "diagnostic_hint_count": diag_hints,
        "diagnostics": diag_list[:50],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Lakebridge isolated runner")
    parser.add_argument("--request", required=True, help="Path to request JSON file")
    parser.add_argument("--response", required=True, help="Path to response JSON file")
    args = parser.parse_args()

    response_data: Dict[str, Any] = {"status": "FAILED"}
    exit_code = 0

    try:
        with open(args.request, "r", encoding="utf-8") as req_file:
            request = json.load(req_file)

        action = request.get("action")
        if action == "check_environment":
            response_data = run_check_environment()
        elif action == "analyze":
            response_data = run_analyze(request)
        elif action == "transpile":
            response_data = run_transpile(request)
        else:
            raise ValueError(f"Unknown action: {action!r}")

        if response_data.get("status") == "FAILED":
            exit_code = 1

    except Exception as exc:
        exit_code = 1
        response_data = {
            "status": "FAILED",
            "error": sanitize_error(exc),
            "error_type": exc.__class__.__name__,
        }

    finally:
        try:
            os.makedirs(os.path.dirname(args.response), exist_ok=True)
            with open(args.response, "w", encoding="utf-8") as resp_file:
                json.dump(response_data, resp_file, indent=2)
        except Exception as write_exc:
            print(f"FATAL: failed to write response file {args.response}: {write_exc}", file=sys.stderr)
            sys.exit(1)

    sys.exit(exit_code)


if __name__ == "__main__":
    main()
