#!/usr/bin/env python3
"""
(FAKE) stand-in for CMS conditions upload's `uploadConditions.py`.

Never touches the network or a real conditions DB. Instead it appends one row
to a dummy one -- $NGT_DEV_HOME/conddb/payloads.jsonl, see
scenario-player/bin/_fakeprov.py -- recording which calibration/run/
lumisections the payload it was handed actually covers, read out of that
.db file's own provenance manifest. That ledger is what a scenario's
`expect:` block is validated against (scenario-player/expectations.py).

Only the .db argument is read. The upload-metadata .txt step4 writes beside it
(tag/inputTag/since/destinationDatabase) is deliberately *not* consulted:
those are production code's own outputs rather than evidence about what the
pipeline processed, so keeping them out leaves this with a single input, no
.db/.txt naming convention to depend on, and nothing to do when a
calibration's config names the two differently.

A row is appended only on the success path: an injected fault exits before
writing anything, which is exactly what makes "nothing should reach the
conditions DB" an assertable expectation for the fault scenarios.

Before doing that, checks $NGT_FAULTS_DIR/upload_conditions.json (armed by
scenario-player/seed.py's arm_fault(), called by scenario-player/scenario_player.py for a scenario's
`faults:` entries, or directly via `airflow_automation/airflow_demo/airflow_demo.sh arm-fault`) for a
still-armed, scope-matching simulated failure, and if one exists, prints a
realistic condDB-upload-style error to stderr and exits with the configured
code instead. Scope (calibration/run) is derived from the current working
directory -- see _scope_from_cwd -- since step4.py's upload_conditions()
always runs this job with cwd set to its own harvestJob_* working directory.
"""
import fcntl
import json
import os
import re
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.realpath(__file__)))
import _fakeprov  # noqa: E402  -- sibling module, see its docstring for why not an import from ../


def _as_int(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _scope_from_cwd():
    """{calibration, run} derived from cwd -- see
    ngt_calibration_loop/step4.py's upload_conditions()/_job_dir_name, whose
    job_dir is always <data_base_path>/<calibration>/run<N>/harvestJob_*."""
    parts = os.getcwd().replace("\\", "/").rstrip("/").split("/")
    scope = {"calibration": None, "run": None}
    for i, part in enumerate(parts):
        m = re.match(r"^run(\d+)$", part)
        if m:
            scope["run"] = m.group(1)
            if i > 0:
                scope["calibration"] = parts[i - 1]
            break
    return scope


def _atomic_write_json(path, data):
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


def _consume_fault(path, scope):
    """Cross-process-safe read/match/decrement/write against one fault file.
    Duplicated (stdlib-only, no ngt_calibration_loop import) from
    scenario-player/faults.py's consume_fault/scope_matches, which
    tests/stubs/omsapi uses instead -- this script must stay importable by a
    bare $PATH lookup with nothing but the standard library installed, same
    constraint as scenario-player/bin/cmsDriver.py."""
    lock_path = path + ".lock"
    with open(lock_path, "a+") as lock_file:
        fcntl.flock(lock_file, fcntl.LOCK_EX)
        try:
            if not os.path.exists(path):
                return None
            try:
                with open(path, "r", encoding="utf-8") as f:
                    entries = json.load(f)
            except (json.JSONDecodeError, OSError):
                return None

            for i, entry in enumerate(entries):
                if all(
                    entry.get(k) is None or str(entry[k]) == str(scope.get(k))
                    for k in ("calibration", "run")
                ):
                    matched = dict(entry)
                    remaining = entry.get("times_remaining")
                    if remaining is not None:
                        remaining -= 1
                        if remaining <= 0:
                            entries.pop(i)
                        else:
                            entry["times_remaining"] = remaining
                    _atomic_write_json(path, entries)
                    return matched
            return None
        finally:
            fcntl.flock(lock_file, fcntl.LOCK_UN)


def _check_fault():
    faults_dir = os.environ.get("NGT_FAULTS_DIR")
    if not faults_dir:
        return None
    return _consume_fault(os.path.join(faults_dir, "upload_conditions.json"), _scope_from_cwd())


# Real-looking canned detail per exit code, so an injected failure resembles
# a genuine condDB upload error rather than a bare placeholder -- overridable
# per-fault via the `message:` scenario YAML field.
_CANNED_DETAIL = {
    1: "Authentication failed: proxy certificate at $COND_AUTH_PATH is missing, expired, or unauthorized "
    "for the target tag",
    2: "Connection to the conditions database timed out",
    3: "Tag conflict: a payload with an overlapping IOV already exists for this tag",
}


def _upload_error_message(exit_code, message=None):
    detail = message or _CANNED_DETAIL.get(
        exit_code, "(FAKE) simulated uploadConditions.py failure -- injected by scenario-player/bin/uploadConditions.py fault injection"
    )
    return f"(FAKE) uploadConditions.py: ERROR - {detail}"


def main():
    fault = _check_fault()
    if fault:
        exit_code = fault.get("exit_code", 1)
        print(_upload_error_message(exit_code, fault.get("message")), file=sys.stderr)
        sys.exit(exit_code)

    if len(sys.argv) < 2:
        print("(FAKE) uploadConditions.py: ERROR - expected a .db file argument", file=sys.stderr)
        sys.exit(1)

    db_file = sys.argv[1]
    cond_auth_path = os.environ.get("COND_AUTH_PATH", "<unset>")

    manifest = _fakeprov.read_provenance(db_file)
    if manifest is None:
        # A real .db, or a fake one produced before provenance existed. Record
        # the upload anyway (an unrecorded upload would read as "the pipeline
        # never uploaded", a far more confusing failure) but with no
        # lumisections, so an expectation check fails loudly on the contents
        # rather than silently passing on an empty set.
        print(
            f"(FAKE) uploadConditions.py: WARNING - no provenance manifest in {db_file}; "
            "recording an upload with no lumisections",
            file=sys.stderr,
        )
        manifest = {}

    scope = _scope_from_cwd()
    row = _fakeprov.append_payload(
        {
            "calibration": manifest.get("calibration") or scope["calibration"],
            "run": int(manifest["run"]) if manifest.get("run") is not None else _as_int(scope["run"]),
            "lumisections": sorted(manifest.get("lumisections") or []),
            "n_input_files": len(manifest.get("inputs") or []),
            "db_file": os.path.basename(db_file),
            "job_dir": os.getcwd(),
        }
    )

    ls_summary = ",".join(str(ls) for ls in row["lumisections"]) or "none"
    print(f"(FAKE) uploadConditions.py: would upload {db_file} using COND_AUTH_PATH={cond_auth_path}")
    print(
        f"(FAKE) uploadConditions.py: recorded payload seq={row['seq']} "
        f"{row['calibration']}/run{row['run']} covering LS {ls_summary} -> {_fakeprov.ledger_path()}"
    )


if __name__ == "__main__":
    main()
