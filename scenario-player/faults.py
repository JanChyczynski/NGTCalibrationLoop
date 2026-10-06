"""Shared vocabulary for injecting failures into OMS / cmsRun / upload_conditions.py.

A fault names the system (`target`), how it fails (`mode`), an optional scope
(`calibration`/`run`/`step`) and how many matching calls it survives (`times`).
Whatever consumes it prints error text that reads like the real failure.

pytest arms the fake OMS in-process with tests/stubs/omsapi's set_failure(); a
live scenario arms faults by writing $NGT_DEV_HOME/faults/<target>.json
(seed.py's arm_fault), which the fakes consume in the processes running the
loops. Standard library only, so the fakes in bin/ can import it.
"""

import json
import os
import tempfile
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Optional

TARGETS = ("oms", "cmsrun", "upload_conditions")
OMS_MODES = ("connection_error", "timeout", "http_error", "malformed_json")
JOB_MODES = ("exit_code",)
STEPS = ("step2", "step3", "step4")


class FaultSpecError(ValueError):
    """Raised for an invalid fault entry, e.g. from a scenario YAML's `faults:` list."""


@dataclass(frozen=True)
class FaultSpec:
    target: str
    mode: str
    times: Optional[int] = 1  # None = unlimited
    status: Optional[int] = None
    exit_code: Optional[int] = None
    calibration: Optional[str] = None
    run: Optional[int] = None
    step: Optional[str] = None


def _require(condition, message):
    if not condition:
        raise FaultSpecError(message)


def _is_int(value):
    return isinstance(value, int) and not isinstance(value, bool)


def parse_fault_spec(raw: dict) -> FaultSpec:
    """Validate one `faults:` entry into a FaultSpec, raising FaultSpecError
    naming the offending field. A scenario's `at` is the caller's to pop."""
    raw = dict(raw)

    target = raw.pop("target", None)
    _require(target in TARGETS, f"faults: 'target' must be one of {TARGETS}, got {target!r}")

    mode = raw.pop("mode", None)
    valid_modes = OMS_MODES if target == "oms" else JOB_MODES
    _require(
        mode in valid_modes,
        f"faults: target={target!r} 'mode' must be one of {valid_modes}, got {mode!r}",
    )

    # 1 (single-shot) when omitted; "unlimited", or None as set_failure() passes it, for no limit.
    times_raw = raw.pop("times", 1)
    if times_raw is None or times_raw == "unlimited":
        times = None
    else:
        _require(
            _is_int(times_raw) and times_raw > 0,
            f"faults: 'times' must be a positive integer or 'unlimited', got {times_raw!r}",
        )
        times = times_raw

    status = raw.pop("status", None)
    if target == "oms" and mode == "http_error":
        _require(
            _is_int(status) and 400 <= status <= 599,
            "faults: target=oms mode=http_error requires an integer 'status' between 400 and 599",
        )
    else:
        _require(status is None, "faults: 'status' is only valid for target=oms mode=http_error")

    exit_code = raw.pop("exit_code", None)
    if target in ("cmsrun", "upload_conditions"):
        _require(
            _is_int(exit_code) and exit_code != 0,
            f"faults: target={target!r} requires a nonzero integer 'exit_code'",
        )
    else:
        _require(exit_code is None, "faults: 'exit_code' is only valid for target=cmsrun/upload_conditions")

    calibration = raw.pop("calibration", None)
    if target in ("cmsrun", "upload_conditions"):
        _require(
            isinstance(calibration, str) and calibration,
            f"faults: target={target!r} requires a 'calibration' (e.g. 'EcalPedestals') -- faults on "
            "this target must always be scoped so one can't fire against the wrong calibration's job "
            "in a multi-calibration scenario",
        )
    else:
        _require(calibration is None, "faults: 'calibration' is not valid for target=oms")

    run = raw.pop("run", None)
    _require(run is None or _is_int(run), f"faults: 'run' must be an integer if given, got {run!r}")

    step = raw.pop("step", None)
    if step is not None:
        _require(target == "cmsrun", "faults: 'step' is only valid for target=cmsrun")
        _require(step in STEPS, f"faults: 'step' must be one of {STEPS}, got {step!r}")

    _require(not raw, f"faults: unknown field(s) {sorted(raw)} in a target={target!r} fault entry")

    return FaultSpec(
        target=target,
        mode=mode,
        times=times,
        status=status,
        exit_code=exit_code,
        calibration=calibration,
        run=run,
        step=step,
    )


def fault_spec_to_entry(spec: FaultSpec) -> dict:
    """FaultSpec -> a fault-file entry with a fresh times_remaining counter
    (no `target`: that picks the file)."""
    entry = {"mode": spec.mode, "times_remaining": spec.times}
    for field in ("status", "exit_code", "calibration", "run", "step"):
        value = getattr(spec, field)
        if value is not None:
            entry[field] = value
    return entry


def atomic_write_json(path, data):
    """Write `data` as JSON to `path` atomically (temp file + os.replace), so
    a concurrent reader never observes a partially-written file."""
    path = str(path)
    directory = os.path.dirname(path) or "."
    fd, tmp_path = tempfile.mkstemp(dir=directory, prefix=".tmp-fault-", suffix=".json")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        os.replace(tmp_path, path)
    except BaseException:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


@contextmanager
def locked_json_file(path):
    """Exclusive cross-process lock around a read-modify-write of `path`.
    fcntl is imported lazily, so the module still imports on Windows."""
    import fcntl

    lock_path = str(path) + ".lock"
    with open(lock_path, "a+") as lock_file:
        fcntl.flock(lock_file, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(lock_file, fcntl.LOCK_UN)


def scope_matches(entry: dict, scope: dict) -> bool:
    """True if every scope key the entry sets matches `scope` (compared as strings)."""
    for key in ("calibration", "run", "step"):
        if key in entry and str(entry[key]) != str(scope.get(key)):
            return False
    return True


def take_fault(entries, scope):
    """Count down the first entry matching `scope` (removing it once spent) and
    return it as it was, or None. Edits `entries` in place."""
    for i, entry in enumerate(entries):
        if scope_matches(entry, scope):
            matched = dict(entry)
            remaining = entry.get("times_remaining")
            if remaining is not None:
                remaining -= 1
                if remaining <= 0:
                    entries.pop(i)
                else:
                    entry["times_remaining"] = remaining
            return matched
    return None


def consume_fault(path, scope):
    """take_fault() on the fault file at `path`, written back if anything
    matched. The caller holds locked_json_file(path)."""
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            entries = json.load(f)
    except (json.JSONDecodeError, OSError):
        return None

    matched = take_fault(entries, scope)
    if matched is not None:
        atomic_write_json(path, entries)
    return matched
