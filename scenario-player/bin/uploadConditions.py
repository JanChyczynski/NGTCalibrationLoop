#!/usr/bin/env python3
"""
(FAKE) stand-in for CMS conditions upload's `uploadConditions.py`.

Never touches the network or a real conditions DB -- just reports what it
would have uploaded, so NGTLoopStep4.py's harvesting script can complete its
cycle.

Before doing that, checks $NGT_FAULTS_DIR/upload_conditions.json (armed by
scenario-player/seed.py's arm_fault(), called by scenario-player/scenario_player.py for a scenario's
`faults:` entries, or directly via `scenario-player/seed.py arm-fault`) for a
still-armed, scope-matching simulated failure, and if one exists, prints a
realistic condDB-upload-style error to stderr and exits with the configured
code instead. Scope (calibration/run) is derived from the current working
directory -- see _scope_from_cwd -- since NGTLoopStep4.py's HARVESTING.sh
always runs it in its own harvestJob<NNN> directory.
"""
import os
import re
import sys

# env.sh puts this checkout's scenario-player/bin on $PATH, so scenario-player/faults.py is always
# one directory up.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
import faults  # noqa: E402


def _scope_from_cwd():
    """{calibration, run} derived from cwd, a
    <data_base_path>/<calibration>/run<N>/harvestJob* job dir."""
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


def _check_fault():
    faults_dir = os.environ.get("NGT_FAULTS_DIR")
    if not faults_dir:
        return None
    path = os.path.join(faults_dir, "upload_conditions.json")
    with faults.locked_json_file(path):
        return faults.consume_fault(path, _scope_from_cwd())


# Real-looking detail per exit code, so an injected failure resembles a genuine condDB upload error.
_CANNED_DETAIL = {
    1: "Authentication failed: proxy certificate at $COND_AUTH_PATH is missing, expired, or unauthorized "
    "for the target tag",
    2: "Connection to the conditions database timed out",
    3: "Tag conflict: a payload with an overlapping IOV already exists for this tag",
}


def _upload_error_message(exit_code):
    detail = _CANNED_DETAIL.get(exit_code, f"injected failure, exit code {exit_code}")
    return f"(FAKE) uploadConditions.py: ERROR - {detail}"


def main():
    fault = _check_fault()
    if fault:
        exit_code = fault.get("exit_code", 1)
        print(_upload_error_message(exit_code), file=sys.stderr)
        sys.exit(exit_code)

    db_file = sys.argv[1] if len(sys.argv) > 1 else "<missing db file argument>"
    cond_auth_path = os.environ.get("COND_AUTH_PATH", "<unset>")
    print(f"(FAKE) uploadConditions.py: would upload {db_file} using COND_AUTH_PATH={cond_auth_path}")


if __name__ == "__main__":
    main()
