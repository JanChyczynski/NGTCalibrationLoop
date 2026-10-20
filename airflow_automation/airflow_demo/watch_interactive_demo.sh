#!/bin/bash
# Guided walkthrough of the AssetWatcher DAG design (airflow_automation/airflow_dags/ngt_dags_watch.py)
# against the fake OMS/EOS/CMSSW toolchain in scenario-player/bin/: a scenario played with
# `airflow_demo.sh play <scenario> --interactive --monitor` -- the same scenario player, flags and
# scenario files as the FSM loops' walkthrough (`scenario-player/live_demo.sh play ... --interactive
# --monitor`) -- so each step of the scenario waits for Enter, and every file the pipeline produces
# is reported as it appears. Around it: what to watch for in this design, a reset if the scenario's
# runs were already played, and an offer to stop Airflow at the end. Needs a Linux host (Airflow
# has no Windows support; WSL works).
#
# What to watch for, compared to the FSM loops:
#   * NO cron DAGs at all. Two kinds of AssetWatcher run continuously in the
#     `airflow triggerer` (while ngt_watch_* is unpaused):
#       - RunWatcherTrigger polls OMS and updates the static Asset ngt://runs;
#       - LumisectionFileWatcherTrigger x3 watch each calibration's active run
#         dir and update the static Asset ngt://files/<cal>, one event per file.
#   * ngt_watch_run_primer (schedule=[Asset ngt://runs]) reacts to a new run by
#     calling step2.find_new_run per calibration -- creating the working dirs
#     the file watchers gate on. That's what "starts" per-calibration file
#     watching (the watchers are always running, just idle until a dir exists).
#   * one ngt_watch_process_<cal> DagRun per lumisection.
#   * all 4 assets are static, so they ALL show in the UI's Assets view; the
#     watchers' dedup watermarks live in each Asset's AssetStateStore.
# See watch_asset_scheduling_design.md for the full written explanation.
#
# Usage:
#   ./airflow_automation/airflow_demo/watch_interactive_demo.sh                    # basic_ecal_pedestals.yaml
#   ./airflow_automation/airflow_demo/watch_interactive_demo.sh scenario-player/scenarios/fault_injection_demo.yaml
#                                                        # ...with real Airflow retries of a crashed cmsRun / upload
#   ./airflow_automation/airflow_demo/watch_interactive_demo.sh --yes              # play it on its own timeline
#
# Options:
#   [scenario.yaml]    the scenario to walk through (default: scenario-player/scenarios/basic_ecal_pedestals.yaml)
#   --yes / -y         don't pause: play the scenario on its own timeline, reset if needed, and pause the
#                      watch DAGs at the end without asking
#   --keep-running     don't offer to pause the watch DAGs / stop Airflow at the end

set -uo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
AIRFLOW_DEMO="$REPO_DIR/airflow_automation/airflow_demo/airflow_demo.sh"
# Sets NGT_DEV_HOME/NGT_AIRFLOW_VENV/... -- this checkout's defaults plus any local_*_env.sh overrides.
# shellcheck disable=SC1091
source "$REPO_DIR/airflow_automation/airflow_demo/airflow_env.sh"

SCENARIO="$REPO_DIR/scenario-player/scenarios/basic_ecal_pedestals.yaml"
AUTO_CONTINUE=0
KEEP_RUNNING=0

while [ $# -gt 0 ]; do
  case "$1" in
    --yes|-y) AUTO_CONTINUE=1; shift ;;
    --keep-running) KEEP_RUNNING=1; shift ;;
    -h|--help) awk 'NR > 1 && !/^#/ { exit } NR > 1' "$0"; exit 0 ;;  # the header comment
    --calibration|--run|--ls|--skip-failure-demo)
      echo "$1 is gone: the scenario file now says which runs, calibrations and lumisections to play" >&2
      echo "(see scenario-player/scenarios/; fault_injection_demo.yaml shows a forced failure retrying)." >&2
      exit 1 ;;
    -*) echo "Unknown option: $1" >&2; exit 1 ;;
    *) SCENARIO="$1"; shift ;;
  esac
done

# --- narration helpers --------------------------------------------------------

banner() { echo; echo "================================================================================"; echo "  $*"; echo "================================================================================"; }
note() { echo "  -- $*"; }

pause() {
  if [ "$AUTO_CONTINUE" = "1" ]; then echo "(--yes: auto-continuing)"; return 0; fi
  echo
  read -rp ">>> Press Enter to continue (Ctrl-C to stop here)... " _
}

confirm() {
  local prompt="$1"
  if [ "$AUTO_CONTINUE" = "1" ]; then return 0; fi
  local reply
  read -rp ">>> $prompt [y/N] " reply
  [[ "$reply" =~ ^[Yy] ]]
}

run() { echo "+ $*"; "$@"; }

stale_state() {
  # What would make playing this scenario on $NGT_DEV_HOME as it stands mislead, as
  # "<runs>|<uploads>": the scenario's run numbers this instance has already handled -- a working
  # dir left in $NGT_DEV_HOME (find_new_run skips it) or the run in the run watcher's seen_runs
  # watermark (it never announces a run twice), so playing them again does nothing -- and the
  # uploads an earlier scenario left in the conditions-DB ledger, which this scenario's expect:
  # block would count as unexpected payloads. reset-scenario clears all of it.
  python3 - "$SCENARIO" "$NGT_DEV_HOME" <<'PY' 2>/dev/null
import json
import sys
from pathlib import Path

import yaml

dev_home = Path(sys.argv[2])
try:
    runs = {int(r["run"]) for r in (yaml.safe_load(Path(sys.argv[1]).read_text()) or {}).get("runs", [])}
except Exception:
    runs = set()  # malformed -- let `play` report it
played = {run for run in runs if any((dev_home / "data").glob(f"*/run{run}"))}
try:
    from airflow.models.asset import AssetModel
    from airflow.models.asset_state_store import AssetStateStoreModel
    from airflow.utils.session import create_session

    with create_session() as session:
        for row in (
            session.query(AssetStateStoreModel)
            .join(AssetModel, AssetModel.id == AssetStateStoreModel.asset_id)
            .filter(AssetModel.uri.like("ngt://runs%"), AssetStateStoreModel.key == "seen_runs")
        ):
            seen = json.loads(row.value) if isinstance(row.value, str) else row.value  # stored JSON-encoded
            played |= runs & {int(r) for r in (seen or [])}
except Exception:
    pass  # no database yet: nothing has been played
ledger = dev_home / "conddb" / "payloads.jsonl"
uploads = sum(1 for line in ledger.read_text().splitlines() if line.strip()) if ledger.exists() else 0
print(" ".join(str(run) for run in sorted(played)) + f"|{uploads}")
PY
}

# --- 0. what this is --------------------------------------------------------------------

banner "NGT Calibration Loop -- AssetWatcher (all-static-asset) DAG design live walkthrough"
note "scenario: $SCENARIO"
note "\$NGT_DEV_HOME=$NGT_DEV_HOME"
note "what to watch for: no cron DAGs -- RunWatcherTrigger (in the triggerer) polls OMS and updates"
note "  Asset ngt://runs, which wakes ngt_watch_run_primer to create the per-calibration working dirs;"
note "  the 3 LumisectionFileWatcherTriggers then update ngt://files/<cal> once per new file, and each"
note "  of those events is one ngt_watch_process_<cal> DagRun (Step 2 -> 3 -> 4) -- see this script's"
note "  header and watch_asset_scheduling_design.md"
if [ "$AUTO_CONTINUE" = "1" ]; then
  note "--yes: the scenario plays on its own timeline"
else
  note "each step of the scenario waits for Enter; files the pipeline produces are reported as they appear"
fi
note "Airflow UI: http://localhost:$NGT_AIRFLOW_PORT (once it's up)"
if [ ! -f "$NGT_AIRFLOW_VENV/bin/activate" ]; then
  echo "No Airflow venv at $NGT_AIRFLOW_VENV -- see README.md's Airflow setup section" >&2
  exit 1
fi
command -v tmux >/dev/null || { echo "tmux is required" >&2; exit 1; }
# shellcheck disable=SC1091
source "$NGT_AIRFLOW_VENV/bin/activate"
pause

# --- 1. a scenario needs fresh scratch state ------------------------------------------------

state="$(stale_state)"
played="${state%%|*}"
earlier_uploads="${state##*|}"
if [ -n "$played" ] || [ "${earlier_uploads:-0}" -gt 0 ]; then
  banner "\$NGT_DEV_HOME isn't fresh for this scenario"
  if [ -n "$played" ]; then
    note "run(s) $played were already played against this instance: the watchers would ignore them"
  fi
  if [ "${earlier_uploads:-0}" -gt 0 ]; then
    note "$earlier_uploads earlier upload(s) in the conditions-DB ledger would count against this scenario's check"
  fi
  note "reset-scenario wipes \$NGT_DEV_HOME and the watchers' watermarks (stopping and restarting"
  note "Airflow around it if it's running)"
  if confirm "Reset the scratch state first?"; then
    run "$AIRFLOW_DEMO" reset-scenario || exit 1
  elif [ -n "$played" ]; then
    echo "Not playing it again as is -- reset first, or pick a scenario with other run numbers."
    exit 1
  fi
fi

# --- 2. play it ---------------------------------------------------------------------------

banner "Playing $(basename "$SCENARIO") against Airflow"
if [ "$AUTO_CONTINUE" = "1" ]; then
  run "$AIRFLOW_DEMO" play "$SCENARIO" --monitor < /dev/null
else
  run "$AIRFLOW_DEMO" play "$SCENARIO" --interactive --monitor
fi
status=$?

# --- 3. wrap up ---------------------------------------------------------------------------

banner "Done (scenario_player.py exit status: $status)"
run "$AIRFLOW_DEMO" status
run "$AIRFLOW_DEMO" ui

if [ "$KEEP_RUNNING" = "1" ]; then
  echo; echo "Leaving Airflow running (--keep-running). Stop later with: $AIRFLOW_DEMO stop"
  exit "$status"
fi
if [ "$AUTO_CONTINUE" = "1" ]; then
  echo; echo "(--yes: pausing the watch DAGs -- this stops the watchers -- and leaving Airflow up)"
  run "$AIRFLOW_DEMO" pause-watch > /dev/null
  exit "$status"
fi

echo
if confirm "Pause the watch DAGs again (this stops the 4 AssetWatchers)?"; then
  run "$AIRFLOW_DEMO" pause-watch > /dev/null
fi
if confirm "Tear down Airflow entirely now?"; then
  run "$AIRFLOW_DEMO" stop
else
  echo "Left running. Stop later with: $AIRFLOW_DEMO stop"
fi
exit "$status"
