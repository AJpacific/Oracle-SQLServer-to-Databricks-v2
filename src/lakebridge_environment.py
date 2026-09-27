"""lakebridge_environment.py - Databricks environment bootstrap and validation for Lakebridge & BladeBridge.

Provides idempotent, non-interactive bootstrap for automated Databricks Workflow notebooks:
- Locates /usr/local/bin/uv or uv in PATH
- Creates isolated Python 3.12 venv with --seed (working pip)
- Installs pinned versions: databricks-labs-lakebridge==0.15.2, databricks-bb-plugin==0.3.0, databricks-bb-analyzer==0.3.0
- Validates BladeBridge and Bladespector Analyzer Python APIs and signatures
- Provides concurrency-safe atomic promotion via staging environment
- Caches bootstrap status per compute context
- Sanitizes failures and does not leak secrets
- Does NOT invoke the Databricks CLI
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
import uuid
from typing import Any, Callable, Dict, List, Optional, Tuple

try:
    from src.failure_classifier import sanitize_message
except ModuleNotFoundError:
    from failure_classifier import sanitize_message

# Pinned exact package versions validated live on Databricks compute
PINNED_LAKEBRIDGE_VERSION = "0.15.2"
PINNED_BLADEBRIDGE_VERSION = "0.3.0"
PINNED_ANALYZER_VERSION = "0.3.0"

DEFAULT_LAKEBRIDGE_VERSION = PINNED_LAKEBRIDGE_VERSION
DEFAULT_BLADEBRIDGE_VERSION = PINNED_BLADEBRIDGE_VERSION
DEFAULT_ANALYZER_VERSION = PINNED_ANALYZER_VERSION
DEFAULT_ANALYZER_PACKAGE = f"databricks-bb-analyzer=={PINNED_ANALYZER_VERSION}"

# Cache verification within the notebook process
_BOOTSTRAP_CACHE: Dict[Tuple[str, str, str, str], Any] = {}


class EnvironmentLock:
    """Inter-process filesystem lock ensuring concurrency-safe environment operations."""

    def __init__(self, lock_path: str, timeout_sec: int = 120, poll_interval_sec: float = 0.2):
        self.lock_path = lock_path
        self.timeout_sec = timeout_sec
        self.poll_interval_sec = poll_interval_sec
        self._acquired = False

    def acquire(self) -> bool:
        start_time = time.time()
        parent_dir = os.path.dirname(self.lock_path)
        if parent_dir:
            try:
                os.makedirs(parent_dir, exist_ok=True)
            except Exception:
                pass

        while True:
            try:
                os.mkdir(self.lock_path)
                self._acquired = True
                try:
                    meta_file = os.path.join(self.lock_path, "lock.json")
                    with open(meta_file, "w", encoding="utf-8") as f:
                        json.dump({"pid": os.getpid(), "timestamp": time.time()}, f)
                except Exception:
                    pass
                return True
            except (FileExistsError, OSError):
                # Check for stale lock
                try:
                    meta_file = os.path.join(self.lock_path, "lock.json")
                    if os.path.isfile(meta_file):
                        with open(meta_file, "r", encoding="utf-8") as f:
                            data = json.load(f)
                        lock_ts = float(data.get("timestamp", 0))
                        if time.time() - lock_ts > max(self.timeout_sec * 2, 300):
                            shutil.rmtree(self.lock_path, ignore_errors=True)
                            continue
                except Exception:
                    pass

                if time.time() - start_time > self.timeout_sec:
                    return False
                time.sleep(self.poll_interval_sec)

    def release(self) -> None:
        if self._acquired:
            shutil.rmtree(self.lock_path, ignore_errors=True)
            self._acquired = False

    def __enter__(self):
        if not self.acquire():
            raise TimeoutError(f"Could not acquire environment lock at {self.lock_path} within {self.timeout_sec}s")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()


def find_uv_binary(custom_path: Optional[str] = None) -> Optional[str]:
    """Locate the uv binary on the system.

    If custom_path is explicitly provided, validate it strictly and do not fall back.
    If custom_path is None, discover uv from UV_PATH env, standard path, or PATH.
    """
    if custom_path is not None:
        if os.path.isfile(custom_path) and os.access(custom_path, os.X_OK):
            return os.path.abspath(custom_path)
        return None

    env_uv = os.environ.get("UV_PATH")
    if env_uv and os.path.isfile(env_uv) and os.access(env_uv, os.X_OK):
        return os.path.abspath(env_uv)

    # Standard path on target Databricks compute
    standard_path = "/usr/local/bin/uv"
    if os.path.isfile(standard_path) and os.access(standard_path, os.X_OK):
        return standard_path

    # Check system PATH
    which_uv = shutil.which("uv")
    if which_uv:
        return os.path.abspath(which_uv)

    return None


def get_default_lakebridge_venv_dir() -> str:
    """Return default deterministic path for isolated Lakebridge virtual environment."""
    env_dir = os.environ.get("LAKEBRIDGE_VENV_DIR")
    if env_dir:
        return os.path.abspath(env_dir)

    if os.name == "nt":
        return os.path.join(tempfile.gettempdir(), ".lakebridge_uv_env")
    return "/local_disk0/.lakebridge_uv_env"


def get_venv_python_executable(venv_dir: str) -> str:
    """Return path to Python interpreter inside the virtual environment."""
    if os.name == "nt":
        return os.path.join(venv_dir, "Scripts", "python.exe")
    return os.path.join(venv_dir, "bin", "python")


def check_environment_health(
    venv_python: str,
    timeout_sec: int = 30,
    runner: Optional[Callable[[List[str]], Tuple[int, str, str]]] = None,
) -> Tuple[bool, Dict[str, Any]]:
    """Test whether the isolated environment has working Lakebridge, BladeBridge, and Analyzer."""
    if not os.path.isfile(venv_python):
        return False, {"error": f"Python interpreter not found at {venv_python}"}

    check_code = (
        "import json, sys, inspect\n"
        "from importlib.metadata import version\n"
        "res = {'python': sys.version.split()[0], 'executable': sys.executable}\n"
        "try:\n"
        "    res['lakebridge_version'] = version('databricks-labs-lakebridge')\n"
        "except Exception as e:\n"
        "    res['lakebridge_version_error'] = str(e)\n"
        "try:\n"
        "    res['bladebridge_version'] = version('databricks-bb-plugin')\n"
        "except Exception as e:\n"
        "    res['bladebridge_version_error'] = str(e)\n"
        "try:\n"
        "    res['analyzer_version'] = version('databricks-bb-analyzer')\n"
        "except Exception as e:\n"
        "    res['analyzer_version_error'] = str(e)\n"
        "try:\n"
        "    from databricks.labs.bladebridge.transpiler import Transpiler\n"
        "    if callable(getattr(Transpiler, 'transpile', None)):\n"
        "        res['bladebridge'] = 'available'\n"
        "    else:\n"
        "        res['bladebridge_error'] = 'Transpiler.transpile is not callable'\n"
        "except Exception as e:\n"
        "    res['bladebridge_error'] = str(e)\n"
        "try:\n"
        "    from databricks.labs.bladespector.analyzer import Analyzer\n"
        "    fn = getattr(Analyzer, 'analyze', None)\n"
        "    if fn and callable(fn):\n"
        "        sig = inspect.signature(fn)\n"
        "        params = list(sig.parameters.keys())\n"
        "        if len(params) >= 3:\n"
        "            res['analyzer'] = 'available'\n"
        "        else:\n"
        "            res['analyzer_error'] = f'Analyzer.analyze signature incompatible: {params}'\n"
        "    else:\n"
        "        res['analyzer_error'] = 'Analyzer.analyze is not callable'\n"
        "except Exception as e:\n"
        "    res['analyzer_error'] = str(e)\n"
        "print(json.dumps(res))\n"
    )

    cmd = [venv_python, "-c", check_code]
    try:
        if runner:
            exit_code, stdout, stderr = runner(cmd)
        else:
            proc = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=False,
                timeout=timeout_sec,
            )
            exit_code, stdout, stderr = proc.returncode, proc.stdout, proc.stderr

        if exit_code != 0:
            return False, {
                "error": sanitize_message(stderr or stdout),
                "exit_code": exit_code,
            }

        details = json.loads(stdout.strip())
        is_healthy = bool(
            details.get("bladebridge") == "available"
            and details.get("analyzer") == "available"
        )

        if not is_healthy:
            return False, {
                "error": f"API check failed: bladebridge={details.get('bladebridge')}, analyzer={details.get('analyzer')}",
                **details,
            }

        # Require all three installed package versions to exist and exactly match
        lb_err = details.get("lakebridge_version_error")
        lb_v = details.get("lakebridge_version")
        if lb_err or not lb_v or lb_v != PINNED_LAKEBRIDGE_VERSION:
            return False, {
                "error": f"Invalid or missing lakebridge version: {lb_v} (expected {PINNED_LAKEBRIDGE_VERSION}, error: {lb_err})",
                **details,
            }

        bb_err = details.get("bladebridge_version_error")
        bb_v = details.get("bladebridge_version")
        if bb_err or not bb_v or bb_v != PINNED_BLADEBRIDGE_VERSION:
            return False, {
                "error": f"Invalid or missing bladebridge version: {bb_v} (expected {PINNED_BLADEBRIDGE_VERSION}, error: {bb_err})",
                **details,
            }

        az_err = details.get("analyzer_version_error")
        az_v = details.get("analyzer_version")
        if az_err or not az_v or az_v != PINNED_ANALYZER_VERSION:
            return False, {
                "error": f"Invalid or missing analyzer version: {az_v} (expected {PINNED_ANALYZER_VERSION}, error: {az_err})",
                **details,
            }

        return True, details
    except subprocess.TimeoutExpired:
        return False, {"error": f"Environment check timed out after {timeout_sec}s"}
    except Exception as exc:
        return False, {"error": sanitize_message(exc)}


def bootstrap_lakebridge_environment(
    venv_dir: Optional[str] = None,
    uv_path: Optional[str] = None,
    lakebridge_version: str = PINNED_LAKEBRIDGE_VERSION,
    bladebridge_version: str = PINNED_BLADEBRIDGE_VERSION,
    analyzer_version: str = PINNED_ANALYZER_VERSION,
    force: bool = False,
    timeout_sec: int = 300,
    runner: Optional[Callable[[List[str]], Tuple[int, str, str]]] = None,
) -> Tuple[bool, Optional[str], Dict[str, Any]]:
    """Idempotently and concurrency-safely bootstrap the isolated Lakebridge virtual environment using uv.

    Builds into a temporary staging environment, verifies health, backs up previous working environment,
    promotes staging atomically, validates post-promotion health, and rolls back on failure.
    """
    target_dir = os.path.abspath(venv_dir or get_default_lakebridge_venv_dir())
    venv_python = get_venv_python_executable(target_dir)

    # 1. Quick health check if not forced
    if not force and os.path.isfile(venv_python):
        healthy, health_details = check_environment_health(venv_python, runner=runner)
        if healthy:
            return True, venv_python, {
                "status": "EXISTING_HEALTHY",
                "venv_dir": target_dir,
                "venv_python": venv_python,
                **health_details,
            }

    # 2. Locate uv with strict handling for custom_path
    uv_bin = find_uv_binary(uv_path)
    if not uv_bin:
        if uv_path is not None:
            err_msg = f"Explicitly provided uv binary not found or not executable: {uv_path}"
        else:
            err_msg = "uv binary not found at /usr/local/bin/uv or in system PATH"
        return False, None, {
            "status": "FAILED",
            "error_code": "UV_NOT_FOUND",
            "error": err_msg,
        }

    def run_cmd(cmd: List[str]) -> Tuple[int, str, str]:
        if runner:
            return runner(cmd)
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=False,
            timeout=timeout_sec,
        )
        return proc.returncode, proc.stdout, proc.stderr

    # 3. Create parent directory safely
    parent_dir = os.path.dirname(target_dir)
    try:
        os.makedirs(parent_dir, exist_ok=True)
    except Exception as exc:
        return False, None, {
            "status": "FAILED",
            "error_code": "BOOTSTRAP_IO_ERROR",
            "error": f"Failed to create parent directory {parent_dir}: {sanitize_message(exc)}",
        }

    # 4. Inter-process filesystem lock around environment creation, promotion, and health check
    lock_path = target_dir + ".lock"
    lock = EnvironmentLock(lock_path, timeout_sec=min(timeout_sec, 120))
    if not lock.acquire():
        return False, None, {
            "status": "FAILED",
            "error_code": "LOCK_TIMEOUT",
            "error": f"Failed to acquire environment lock for {target_dir} within timeout",
        }

    try:
        # Re-check target_dir inside the lock in case another process just finished building it
        if not force and os.path.isfile(venv_python):
            healthy, health_details = check_environment_health(venv_python, runner=runner)
            if healthy:
                return True, venv_python, {
                    "status": "EXISTING_HEALTHY",
                    "venv_dir": target_dir,
                    "venv_python": venv_python,
                    **health_details,
                }

        staging_dir = os.path.join(parent_dir, f".lakebridge_staging_{os.getpid()}_{uuid.uuid4().hex[:8]}")
        staging_python = get_venv_python_executable(staging_dir)

        try:
            # Create seeded venv with Python 3.12 via uv in staging_dir
            venv_cmd = [uv_bin, "venv", "--seed", "--python", "3.12", staging_dir]
            c_code, c_out, c_err = run_cmd(venv_cmd)
            if c_code != 0:
                shutil.rmtree(staging_dir, ignore_errors=True)
                return False, None, {
                    "status": "FAILED",
                    "error_code": "VENV_CREATION_FAILED",
                    "error": f"Failed to create virtual environment: {sanitize_message(c_err or c_out)}",
                    "exit_code": c_code,
                }

            # Install pinned packages into the staging venv
            packages = [
                f"databricks-labs-lakebridge=={lakebridge_version}",
                f"databricks-bb-plugin=={bladebridge_version}",
                f"databricks-bb-analyzer=={analyzer_version}",
            ]
            install_cmd = [uv_bin, "pip", "install", "--python", staging_python] + packages
            i_code, i_out, i_err = run_cmd(install_cmd)
            if i_code != 0:
                shutil.rmtree(staging_dir, ignore_errors=True)
                return False, None, {
                    "status": "FAILED",
                    "error_code": "PACKAGE_INSTALL_FAILED",
                    "error": f"Failed to install packages: {sanitize_message(i_err or i_out)}",
                    "exit_code": i_code,
                }

            # Verify health of staging environment
            healthy, staging_details = check_environment_health(staging_python, runner=runner)
            if not healthy:
                shutil.rmtree(staging_dir, ignore_errors=True)
                return False, staging_python, {
                    "status": "FAILED",
                    "error_code": "HEALTH_CHECK_FAILED",
                    "error": f"Post-install health check failed: {staging_details.get('error', 'Required APIs unavailable')}",
                    "details": staging_details,
                }

            # 5. Backup previous working environment before promotion
            backup_dir = None
            if os.path.exists(target_dir):
                backup_dir = target_dir + f".backup_{os.getpid()}_{uuid.uuid4().hex[:8]}"
                try:
                    os.replace(target_dir, backup_dir)
                except Exception:
                    try:
                        shutil.move(target_dir, backup_dir)
                    except Exception as b_exc:
                        shutil.rmtree(staging_dir, ignore_errors=True)
                        return False, None, {
                            "status": "FAILED",
                            "error_code": "BACKUP_FAILED",
                            "error": f"Failed to backup existing environment at {target_dir}: {sanitize_message(b_exc)}",
                        }

            # 6. Atomically promote staging_dir to target_dir
            try:
                os.replace(staging_dir, target_dir)
            except Exception:
                try:
                    shutil.move(staging_dir, target_dir)
                except Exception as promo_exc:
                    # Rollback to backup if promotion failed
                    if backup_dir and os.path.exists(backup_dir):
                        try:
                            os.replace(backup_dir, target_dir)
                        except Exception:
                            shutil.move(backup_dir, target_dir)
                    shutil.rmtree(staging_dir, ignore_errors=True)
                    return False, None, {
                        "status": "FAILED",
                        "error_code": "PROMOTION_FAILED",
                        "error": f"Failed to promote virtual environment to {target_dir}: {sanitize_message(promo_exc)}",
                    }

            # 7. Post-promotion health check on target_dir
            post_healthy, post_details = check_environment_health(venv_python, runner=runner)
            if not post_healthy:
                # Post-promotion health check failed! Rollback!
                shutil.rmtree(target_dir, ignore_errors=True)
                if backup_dir and os.path.exists(backup_dir):
                    try:
                        os.replace(backup_dir, target_dir)
                    except Exception:
                        shutil.move(backup_dir, target_dir)
                return False, None, {
                    "status": "FAILED",
                    "error_code": "POST_PROMOTION_HEALTH_CHECK_FAILED",
                    "error": f"Post-promotion health check failed: {post_details.get('error', 'Unhealthy after promotion')}",
                    "details": post_details,
                }

            # 8. Complete success: safely delete backup
            if backup_dir and os.path.exists(backup_dir):
                shutil.rmtree(backup_dir, ignore_errors=True)

            return True, venv_python, {
                "status": "BOOTSTRAPPED",
                "venv_dir": target_dir,
                "venv_python": venv_python,
                **post_details,
            }

        except Exception as exc:
            shutil.rmtree(staging_dir, ignore_errors=True)
            return False, None, {
                "status": "FAILED",
                "error_code": "BOOTSTRAP_ERROR",
                "error": f"Unexpected error during environment bootstrap: {sanitize_message(exc)}",
            }
    finally:
        lock.release()


def ensure_lakebridge_environment(
    venv_dir: Optional[str] = None,
    uv_path: Optional[str] = None,
    force_recheck: bool = False,
    runner: Optional[Callable[[List[str]], Tuple[int, str, str]]] = None,
) -> Tuple[bool, Optional[str], Dict[str, Any]]:
    """Ensure Lakebridge environment is ready, caching per notebook execution.

    Verifies cached Python executable existence and health, including all three
    pinned package versions in the cache key.
    """
    global _BOOTSTRAP_CACHE
    target_dir = os.path.abspath(venv_dir or get_default_lakebridge_venv_dir())
    cache_key = (
        target_dir,
        PINNED_LAKEBRIDGE_VERSION,
        PINNED_BLADEBRIDGE_VERSION,
        PINNED_ANALYZER_VERSION,
    )

    if not force_recheck and cache_key in _BOOTSTRAP_CACHE:
        cached = _BOOTSTRAP_CACHE[cache_key]
        if cached.get("is_ready"):
            cached_python = cached.get("venv_python")
            if (
                cached_python
                and os.path.isfile(cached_python)
                and (os.access(cached_python, os.X_OK) or os.name == "nt")
            ):
                healthy, health_details = check_environment_health(cached_python, runner=runner)
                if healthy:
                    return True, cached_python, cached.get("details", {})
            # If interpreter is deleted, not executable, or unhealthy, invalidate cache
            _BOOTSTRAP_CACHE.pop(cache_key, None)

    success, v_python, details = bootstrap_lakebridge_environment(
        venv_dir=target_dir,
        uv_path=uv_path,
        runner=runner,
    )

    if success:
        _BOOTSTRAP_CACHE[cache_key] = {
            "is_ready": True,
            "venv_python": v_python,
            "details": details,
        }
    else:
        _BOOTSTRAP_CACHE.pop(cache_key, None)

    return success, v_python, details
