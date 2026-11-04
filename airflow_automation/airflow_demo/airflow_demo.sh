#!/bin/bash
# Self-contained "live" runner for the Airflow-based NGT calibration loop
# against the same fake OMS/EOS/CMSSW toolchain scenario-player/sim_env.sh's `sim_setup`
# provides (scenario-player/bin/, tests/stubs/omsapi, scenario-player/seed.py) -- so the real Airflow
# scheduler drives real DAG runs, real generated job scripts, and real output
# files flowing Step2 -> Step3 -> Step4 to a fake condDB upload, entirely
# offline. Needs a Linux host (Airflow has no Windows support; WSL works fine
# on Windows).
#
# Manages ONE Airflow instance per checkout (own AIRFLOW_HOME, metadata DB and port -- see
# airflow_env.sh, so several checkouts can run side by side) running
# airflow_automation/airflow_dags/ngt_dags_watch.py (4 DAGs -- all-static-asset,
# AssetWatcher-driven, zero cron; see its module docstring), which the
# seed-run/add-ls/end-run/scenario_player.py-driven fake run feeds.
#
# This file is the Airflow-*specific* engine adapter: everything about
# actually simulating a live run (seed-run/add-ls/end-run/list-runs below)
# just forwards to scenario-player/seed.py, which knows nothing about Airflow -- see
# scenario-player/sim_env.sh's module docstring for why that split exists.
#
# For a timed sequence of runs/lumisections (e.g. for a scripted test run), use `play`,
# which brings this instance up and then hands a scenario file to
# scenario-player/scenario_player.py -- see scenario-player/scenarios/. With --interactive
# --monitor it is also the guided walkthrough, the same as the FSM loops' (live_demo.sh
# play): each step waits for Enter and every file the pipeline produces is reported as it
# appears (what to watch for in ngt_dags_watch.py: watch_asset_scheduling_design.md).
#
# Quick start:
#   ./airflow_automation/airflow_demo/airflow_demo.sh setup
#   ./airflow_automation/airflow_demo/airflow_demo.sh start
#   ./airflow_automation/airflow_demo/airflow_demo.sh unpause-watch
#   ./airflow_automation/airflow_demo/airflow_demo.sh seed-run EcalPedestals 398600 --ls 51 52
#   ./airflow_automation/airflow_demo/airflow_demo.sh ui            # prints the webserver URL
#   ./airflow_automation/airflow_demo/airflow_demo.sh add-ls EcalPedestals 398600 53
#   ./airflow_automation/airflow_demo/airflow_demo.sh end-run 398600
#   ./airflow_automation/airflow_demo/airflow_demo.sh status
#   ./airflow_automation/airflow_demo/airflow_demo.sh stop
# or, for a whole scenario file, from a cold start:
#   ./airflow_automation/airflow_demo/airflow_demo.sh play scenario-player/scenarios/basic_ecal_pedestals.yaml
#
# Inside a container (NGT_AIRFLOW_IN_CONTAINER, set by airflow_containers/Dockerfile) everything but
# the process management works the same: the image is the venv and the container runtime keeps
# Airflow's processes running, so start/stop/status/ui/logs are refused, and a reset-* that mustn't
# run under live processes leaves stopping them to the caller (airflow_containers/compose/
# airflow_compose.sh does; in the one-pod deployment a new pod is the reset).
#
# $NGT_DEV_HOME (default ./demo-env, a gitignored directory inside this repo)
# holds the fake data/EOS/CMSSW scratch state, same default as scenario-player/seed.py
# uses directly -- delete it any time to reset the demo data. The Airflow
# instance itself (metadata DB, AIRFLOW_HOME) is separate/persistent -- `setup` creates and
# migrates this checkout's own database; see airflow_env.sh and README.md's Airflow setup
# section for the one-time Postgres role it needs. Every default is overridable via
# local_airflow_env.sh (see local_airflow_env.sh.example).

set -euo pipefail

# airflow_env.sh sources sim_env.sh itself, and both pick up the gitignored local_*_env.sh
# overrides -- so sourcing it here (not just in activate_venv) makes every per-checkout value
# (ports, venv path, ...) available to usage and to commands that never activate.
# shellcheck disable=SC1091
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/airflow_env.sh"
CALIBRATIONS=(SiStripBad EcalPedestals BeamSpot)
PLAYER="$REPO_DIR/scenario-player/scenario_player.py"
VIEWS="$REPO_DIR/airflow_automation/airflow_demo/watch_views.py"

# Airflow's long-running processes, by the names start/stop/status/logs know them.
COMPONENTS=(scheduler dag-processor api triggerer)

usage() {
  cat <<USAGE
Usage: $0 <command> [args]

Setup:
  setup                                  Create/refresh \$NGT_DEV_HOME scratch data
                                          (fake EOS/CMSSW/OMS state; safe to re-run)

Running Airflow (this checkout's own instance -- UI on :$NGT_AIRFLOW_PORT, background processes):
  start                                   Launch whichever of scheduler, dag-processor, API server
                                          and triggerer aren't running
  stop [scheduler|dag-processor|api|triggerer]   Stop all four, or just that one ('stop triggerer'
                                          then 'start' picks up an edited triggers.py)
  status                                  Whether each is running, stopped or has died
  ui                                      Print the Airflow UI URL
  logs <scheduler|dag-processor|api|triggerer>   Tail -f that process's log

DAG control:
  unpause-watch                           Unpause ngt_dags_watch.py's 4 DAGs (starts the AssetWatchers)
  pause-watch                             Pause them again (stops the AssetWatchers)

Resetting state (see scenario-player/sim_env.sh's setup/reset for the simulator's OWN state --
\$NGT_DEV_HOME -- which none of these touch, except reset-scenario):
  reset-watch                             Clear DAG-run history for ngt_dags_watch.py's 4 DAGs
                                          (also drops the ngt://runs / ngt://files/* Assets +
                                          their AssetStateStore rows)
  reset-airflow                           Full reset: drop + recreate the whole metadata DB
                                          (DAG-run history, variables, connections --
                                          everything) and re-migrate. Back to freshly-installed.
  reset-scenario                          Wipe + recreate \$NGT_DEV_HOME (scenario-player/sim_env.sh's own
                                          reset+setup), stopping/restarting Airflow around it if
                                          running -- calling sim_env.sh's reset directly while
                                          Airflow's processes are live can leave in-flight
                                          deferred work stuck; this is the safe way to do it from
                                          this adapter. Also clears ngt_dags_watch.py's
                                          AssetStateStore watermarks (seen_runs / emitted_run<N>),
                                          which would otherwise describe fake run/file state this
                                          just wiped -- without touching the Assets themselves or
                                          any DAG-run history. Pair with reset-watch/
                                          -airflow if you want DAG-run history cleared too.

Driving a fake run (edits \$NGT_DEV_HOME/oms_runs.json + drops fake RAW files,
identical to any other engine adapter -- delegates straight to scenario-player/seed.py):
  seed-run <calibration> <run> [--ls N ...] [--minutes-ago M]
  add-ls <calibration> <run> <ls>
  end-run <run>
  list-runs
  play <scenario.yaml> [options]          Play a whole timeline from a scenario file instead.
                                          Sets up \$NGT_DEV_HOME (and this checkout's database)
                                          if it isn't there, starts whichever of the four
                                          processes aren't running, unpauses the watch DAGs and
                                          waits for the triggerer to run their watchers, then
                                          passes every argument straight to scenario_player.py
                                          (--interactive, --monitor, --speed, --dry-run -- run it
                                          with --help). With --interactive --monitor this is the
                                          guided walkthrough; --monitor also reports Airflow's side
                                          (Asset events, DagRuns, task retries -- watch_views.py)
                                          and sums it up at the end. Warns when \$NGT_DEV_HOME isn't fresh
                                          for the scenario (reset-scenario clears it): the
                                          watchers ignore a run number they've already handled.

Injecting failures (writes \$NGT_DEV_HOME/faults/<target>.json, polled by
tests/stubs/omsapi / scenario-player/bin/cmsRun / scenario-player/bin/uploadConditions.py -- see
scenario-player/faults.py and scenario-player/scenarios/fault_injection_demo.yaml
for the equivalent scenario-YAML \`faults:\` syntax):
  arm-fault --target <oms|cmsrun|upload_conditions> --mode <mode> [options...]
            (oms: --mode connection_error|timeout|http_error|malformed_json
                  [--status N] [--run N] [--times N|unlimited]
             cmsrun/upload_conditions: --mode exit_code --exit-code N
                  --calibration <calibration> [--run N] [--step step2|step3|step4]
                  [--times N|unlimited])
  list-faults [--target <oms|cmsrun|upload_conditions>]
  clear-faults [--target <oms|cmsrun|upload_conditions>]

calibration is one of: ${CALIBRATIONS[*]}
USAGE
}

activate_venv() {
  [ -z "${NGT_AIRFLOW_IN_CONTAINER:-}" ] || return 0
  if [ ! -f "$NGT_AIRFLOW_VENV/bin/activate" ]; then
    echo "No Airflow venv at $NGT_AIRFLOW_VENV -- see README.md's Airflow setup section" >&2
    exit 1
  fi
  # shellcheck disable=SC1091
  source "$NGT_AIRFLOW_VENV/bin/activate"
}

ensure_airflow_db() {
  # Creates this checkout's metadata database if it doesn't exist yet. Only for the database
  # airflow_env.sh itself derived (NGT_AIRFLOW_DB_*): if AIRFLOW__DATABASE__SQL_ALCHEMY_CONN was
  # pointed somewhere else, that database is the operator's to provision. The `airflow_ngt`
  # role was created with CREATEDB (README.md's Airflow setup section), so no sudo is needed.
  if [ "$AIRFLOW__DATABASE__SQL_ALCHEMY_CONN" != "$NGT_AIRFLOW_DB_CONN" ]; then
    echo "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN is overridden -- not creating a database; make sure it exists."
    return
  fi
  local exists
  exists="$(PGPASSWORD="$NGT_AIRFLOW_DB_PASSWORD" psql -h "$NGT_AIRFLOW_DB_HOST" -p "$NGT_AIRFLOW_DB_PORT" \
    -U "$NGT_AIRFLOW_DB_USER" -d postgres -Atc \
    "SELECT 1 FROM pg_database WHERE datname = '$NGT_AIRFLOW_DB_NAME'")" || {
    echo "Could not query Postgres as $NGT_AIRFLOW_DB_USER@$NGT_AIRFLOW_DB_HOST -- see README.md's Airflow setup section." >&2
    exit 1
  }
  if [ -z "$exists" ]; then
    echo "Creating Postgres database $NGT_AIRFLOW_DB_NAME (owner $NGT_AIRFLOW_DB_USER)"
    PGPASSWORD="$NGT_AIRFLOW_DB_PASSWORD" createdb -h "$NGT_AIRFLOW_DB_HOST" -p "$NGT_AIRFLOW_DB_PORT" \
      -U "$NGT_AIRFLOW_DB_USER" "$NGT_AIRFLOW_DB_NAME"
  fi
}

cmd_setup() {
  activate_venv
  sim_setup
  ensure_airflow_db
  airflow db migrate
  [ -z "${NGT_AIRFLOW_IN_CONTAINER:-}" ] || return 0
  echo
  echo "Set up $NGT_DEV_HOME and the Airflow instance in $AIRFLOW_HOME (database $NGT_AIRFLOW_DB_NAME)"
  echo "Next: $0 start"
}

component_log() {
  echo "$NGT_DEV_HOME/logs/airflow_$1.log"
}

component_pid_file() {
  echo "$AIRFLOW_HOME/airflow_demo_$1.pid"
}

component_pid() {
  # Prints a component's pid while it's alive. The name is checked because a pid file can outlive
  # a reboot, and by then its number can belong to anything.
  local pid
  pid="$(cat "$(component_pid_file "$1")" 2>/dev/null)" || return 1
  [[ "$(ps -o args= -p "$pid" 2>/dev/null)" == *airflow* ]] && echo "$pid"
}

any_component_running() {
  local label
  for label in "${COMPONENTS[@]}"; do
    component_pid "$label" > /dev/null && return 0
  done
  return 1
}

start_component() {
  local label="$1"; shift
  if component_pid "$label" > /dev/null; then
    echo "$label already running"
    return
  fi
  if [ ! -f "$NGT_DEV_HOME/env.sh" ]; then
    echo "No $NGT_DEV_HOME/env.sh -- run: $0 setup" >&2
    exit 1
  fi
  local log; log="$(component_log "$label")"
  if [ -f "$(component_pid_file "$label")" ]; then
    echo "$label had died, restarting it -- why should be in its log: $log"
    stop_components "$label"
  fi
  # $NGT_DEV_HOME/env.sh is sim_env.sh's one spelling-out of the fake environment
  # (NGT_DEV_HOME itself included, which a non-default one needs to reach the fake
  # upload's ledger path) and drops any cmsenv/cmsrel shell functions that would
  # outrank the fake toolchain. The omsapi stub doesn't rely on its PYTHONPATH,
  # though: PYTHONPATH set in the launching shell was not reliably observed to reach
  # Airflow's forked/spawned worker subprocesses in testing, so sim_setup also
  # pip-installs tests/stubs into $NGT_AIRFLOW_VENV.
  #
  # Its own session outlives this script and its terminal, and holds everything the component
  # starts, tasks included (those get process groups of their own), for stop_components to reach.
  # A script's background job isn't a process group leader, so setsid doesn't fork: $! is the
  # component itself, and the session's ID. Appending, so that a restart keeps why it died.
  (
    # shellcheck disable=SC1091
    source "$NGT_DEV_HOME/env.sh"
    exec setsid "$@"
  ) >> "$log" 2>&1 < /dev/null &
  echo "$!" > "$(component_pid_file "$label")"
  echo "Started $label (pid $!, log: $log)"
}

stop_components() {
  # All of each component's session. A live component takes its session down on TERM, its tasks
  # included; the workers of a dead scheduler ignore it (measured: 32 of 33 processes still there
  # after 30 s), so they're killed right away, and anything else that won't go at the end.
  local label pid_file sid sessions=()
  for label in "$@"; do
    pid_file="$(component_pid_file "$label")"
    [ -f "$pid_file" ] || continue
    sid="$(cat "$pid_file")"
    if component_pid "$label" > /dev/null; then
      echo "Stopping $label"
      pkill -TERM -s "$sid" || true
      sessions+=("$sid")
    elif pgrep -s "$sid" -f airflow > /dev/null; then
      pkill -KILL -s "$sid" || true
      sessions+=("$sid")
    fi
    rm -f "$pid_file"
  done
  [ "${#sessions[@]}" -gt 0 ] || return 0
  local all deadline=$((SECONDS + NGT_AIRFLOW_STOP_SECONDS))
  all="$(IFS=,; echo "${sessions[*]}")"
  while pgrep -s "$all" > /dev/null && [ "$SECONDS" -lt "$deadline" ]; do
    sleep 0.5
  done
  pkill -KILL -s "$all" || true
}

cmd_start() {
  activate_venv
  mkdir -p "$NGT_DEV_HOME/logs" "$AIRFLOW_HOME"

  # Airflow 3 splits DAG parsing into its own mandatory process (the
  # scheduler no longer spawns it itself, unlike Airflow 2) -- the scheduler,
  # dag-processor and API server are the baseline. The triggerer is a fourth,
  # hosting ngt_dags_watch.py's AssetWatchers (see its module docstring) --
  # started unconditionally since it's harmless, near-zero-overhead idle
  # time if those DAGs are paused/unused.
  #
  # Not `airflow standalone`, which runs fine on Postgres too but hides all four behind one parent
  # that carries on when one of them dies, can't restart just one, and orphans them all on a TERM.
  start_component scheduler airflow scheduler
  start_component dag-processor airflow dag-processor
  start_component api airflow api-server -p "$NGT_AIRFLOW_PORT"
  start_component triggerer airflow triggerer

  echo
  echo "Give the dag-processor ~10-15s to parse airflow_automation/airflow_dags/ before unpausing DAGs."
  cmd_ui
}

cmd_ui() {
  echo "Airflow UI: http://localhost:$NGT_AIRFLOW_PORT"
  echo "  (Simple Auth Manager, all-admins mode for this offline dev instance -- no login needed)"
}

cmd_status() {
  local label pid
  for label in "${COMPONENTS[@]}"; do
    if pid="$(component_pid "$label")"; then
      echo "$label: running (pid $pid)"
    elif [ -f "$(component_pid_file "$label")" ]; then
      echo "$label: DIED -- see '$0 logs $label'; '$0 start' restarts it"
    else
      echo "$label: stopped"
    fi
  done
}

cmd_logs() {
  local which="$1"
  case "$which" in
    api|scheduler|dag-processor|triggerer) ;;
    *) echo "Usage: $0 logs <scheduler|dag-processor|api|triggerer>" >&2; exit 1 ;;
  esac
  local f; f="$(component_log "$which")"
  [ -f "$f" ] || { echo "No log yet at $f -- has '$0 start' been run?" >&2; exit 1; }
  tail -f "$f"
}

cmd_stop() {
  case "${1:-}" in
    "") stop_components "${COMPONENTS[@]}" ;;
    api|scheduler|dag-processor|triggerer) stop_components "$1" ;;
    *) echo "Usage: $0 stop [scheduler|dag-processor|api|triggerer]" >&2; exit 1 ;;
  esac
}

cmd_pause_unpause_watch() {
  # ngt_dags_watch.py's 4 DAGs (the all-static-asset AssetWatcher design -- see
  # its module docstring). AssetWatchers only run while their consuming DAG is
  # unpaused, so this is also what starts/stops the 4 watchers in the
  # triggerer. Unpause the process DAGs (file watchers) first, then the primer
  # (run watcher), so the file watchers are live before a run is discovered.
  local action="$1"
  activate_venv
  for calibration in "${CALIBRATIONS[@]}"; do
    local lower; lower="$(echo "$calibration" | tr '[:upper:]' '[:lower:]')"
    airflow dags "$action" -y "ngt_watch_process_${lower}"
  done
  airflow dags "$action" -y "ngt_watch_run_primer"
}

_reset_ngt_assets() {
  # $1 label; $2/$3 AssetModel.uri LIKE patterns to remove. asset_state_store rows go
  # with the AssetModel via ON DELETE CASCADE. A dev-script admin action, not task code
  # (Airflow 3 forbids ORM access from task code, but this is a dev script); skipped with a
  # warning if the ORM shape has drifted rather than failing the whole reset.
  local label="$1" uri_a="$2" uri_b="$3"
  URI_A="$uri_a" URI_B="$uri_b" python3 - <<'PY' || echo "  (Asset row cleanup skipped for $label -- use reset-airflow for a full wipe)"
import os
from airflow.utils.session import create_session
from airflow.models.asset import AssetActive, AssetEvent, AssetModel
from sqlalchemy import or_

with create_session() as session:
    q = session.query(AssetModel).filter(
        or_(AssetModel.uri.like(os.environ["URI_A"]), AssetModel.uri.like(os.environ["URI_B"]))
    )
    assets = q.all()
    ids = [a.id for a in assets]
    uris = [a.uri for a in assets]
    if ids:
        session.query(AssetEvent).filter(AssetEvent.asset_id.in_(ids)).delete(synchronize_session=False)
        session.query(AssetActive).filter(AssetActive.uri.in_(uris)).delete(synchronize_session=False)
        session.query(AssetModel).filter(AssetModel.id.in_(ids)).delete(synchronize_session=False)
    print(f"  Removed {len(ids)} Asset row(s).")
PY
}

_reset_ngt_watermarks() {
  # Clears just the asset_state_store rows (seen_runs / emitted_run<N> watermarks --
  # triggers.py's _watermark_get/_set/_delete) for ngt://runs and ngt://files/<cal>, leaving the
  # AssetModel rows -- and therefore DAG-run history and pause state -- untouched. Unlike
  # _reset_ngt_assets (used by reset-watch), this does NOT cascade-delete the Asset itself; it's
  # the surgical counterpart used by reset-scenario, which has no reason to touch DAG state.
  #
  # Needed because these watermarks live in the Airflow metadata DB, scoped to the Asset row --
  # sim_env.sh's reset+setup only knows about $NGT_DEV_HOME, so without this a run/file number
  # reused after a reset-scenario (e.g. replaying the same scenario after a botched attempt)
  # looks "already seen"/"already emitted" to ngt_dags_watch.py's watchers forever, even though
  # the fake data backing that watermark is gone. A dev-script admin action, not task code (cf.
  # dev script, not task code); skipped with a warning, same as _reset_ngt_assets, if
  # the ORM shape has drifted rather than failing the whole reset. Expects activate_venv to have
  # already run (mirrors _reset_ngt_assets).
  python3 - <<'PY' || echo "  (AssetStateStore cleanup skipped -- use reset-watch for a full wipe)"
from airflow.utils.session import create_session
from airflow.models.asset import AssetModel
from airflow.models.asset_state_store import AssetStateStoreModel
from sqlalchemy import or_

with create_session() as session:
    ids = [
        a.id
        for a in session.query(AssetModel).filter(
            or_(AssetModel.uri.like("ngt://runs%"), AssetModel.uri.like("ngt://files/%"))
        )
    ]
    removed = 0
    if ids:
        removed = (
            session.query(AssetStateStoreModel)
            .filter(AssetStateStoreModel.asset_id.in_(ids))
            .delete(synchronize_session=False)
        )
    print(f"  Removed {removed} AssetStateStore row(s) (seen_runs / emitted_run<N> watermarks).")
PY
}

cmd_reset_watch() {
  # Clears ngt_dags_watch.py's 4 DAGs + its 4 static
  # Assets (ngt://runs, ngt://files/<cal>). Their AssetStateStore rows (the
  # watchers' "seen runs" / "emitted files" watermarks) are removed with the
  # AssetModel via ON DELETE CASCADE.
  activate_venv
  airflow dags delete -y ngt_watch_run_primer
  for calibration in "${CALIBRATIONS[@]}"; do
    local lower; lower="$(echo "$calibration" | tr '[:upper:]' '[:lower:]')"
    airflow dags delete -y "ngt_watch_process_${lower}"
  done
  _reset_ngt_assets "ngt_dags_watch.py" "ngt://runs%" "ngt://files/%"
  echo "Cleared DAG-run history for all 4 ngt_watch_* DAGs (ngt_dags_watch.py)."
}

cmd_reset_airflow() {
  # Drops and recreates the whole metadata DB schema -- all DAG-run
  # history, variables, connections, everything -- not just this repo's DAGs.
  # Back to the state right after `airflow db migrate` first ran (all DAGs
  # paused by default). Use reset-watch instead to clear just
  # one design's history and leave the rest of the DB alone.
  #
  # Stops/restarts the 4 processes around the reset (only if
  # they were already running): each one holds in-memory state tied to the
  # pre-reset schema (e.g. the scheduler/dag-processor's own Job row) and
  # reliably crashes on its next DB query once the tables underneath it have
  # been dropped and recreated -- confirmed live (UndefinedTable: relation
  # "job" does not exist) while testing this command. A reset that leaves the
  # instance broken until the user figures out to restart it manually isn't
  # a usable reset.
  local was_running=0
  if any_component_running; then
    was_running=1
    cmd_stop
  fi

  activate_venv
  echo "Resetting the entire Airflow metadata DB (both DAG designs, variables, connections)..."
  airflow db reset -y
  airflow db migrate
  echo "Airflow metadata DB reset and re-migrated -- all DAGs are paused again, as after a fresh install."

  if [ "$was_running" = "1" ]; then
    echo "Restarting the 4 processes (they were running before the reset)..."
    cmd_start
  fi
}

_wait_for_watch_design() {
  # Blocks until ngt_dags_watch.py's 4 DAGs are parsed ($1 = dags: unpausing a DAG the
  # dag-processor hasn't seen yet fails) or until they're also unpaused and the newest live
  # triggerer is running all 4 watchers ($1 = live: a scenario's run seeded before then could end
  # before the run watcher ever sees it). Reads the metadata DB directly, as the _reset_* helpers
  # do; the pause state from there rather than from `airflow dags unpause`'s own output, which
  # isn't reliable evidence. Expects activate_venv to have run.
  WAIT_FOR="$1" python3 - <<'PY'
import os
import sys
import time
from datetime import datetime, timedelta, timezone

from airflow.jobs.job import Job
from airflow.models import DagModel
from airflow.models.trigger import Trigger
from airflow.utils.session import create_session

dag_ids = {"ngt_watch_run_primer"} | {f"ngt_watch_process_{c}" for c in ("sistripbad", "ecalpedestals", "beamspot")}
watchers = ("airflow_dags.triggers.RunWatcherTrigger", "airflow_dags.triggers.LumisectionFileWatcherTrigger")
want_live = os.environ["WAIT_FOR"] == "live"
deadline = time.monotonic() + 180
while True:
    with create_session() as session:
        paused = {d.dag_id: d.is_paused for d in session.query(DagModel).filter(DagModel.dag_id.in_(dag_ids))}
        ready = set(paused) == dag_ids
        if ready and want_live:
            triggerer = (
                session.query(Job)
                .filter(Job.job_type == "TriggererJob", Job.state == "running")
                .order_by(Job.id.desc())
                .first()
            )
            alive = triggerer is not None and triggerer.latest_heartbeat > datetime.now(timezone.utc) - timedelta(seconds=30)
            running = alive and session.query(Trigger).filter(
                Trigger.classpath.in_(watchers), Trigger.triggerer_id == triggerer.id
            ).count()
            ready = not any(paused.values()) and running >= len(dag_ids)
    if ready:
        sys.exit(0)
    if time.monotonic() > deadline:
        sys.exit(
            "ngt_dags_watch.py's DAGs never got parsed -- see `airflow_demo.sh logs dag-processor`"
            if not want_live
            else "the watch DAGs' watchers aren't running -- see `airflow_demo.sh logs triggerer`"
        )
    time.sleep(2)
PY
}

_warn_if_not_fresh() {
  # $1: a scenario file. A scenario played on scratch state an earlier one left behind misleads
  # rather than fails: runs this instance already handled -- a working dir left in $NGT_DEV_HOME
  # (find_new_run skips it) or the run in the run watcher's seen_runs watermark (it never
  # announces a run twice) -- silently do nothing. Only warns: replaying on purpose is legitimate
  # (e.g. to check exactly that dedup). Expects activate_venv to have run.
  local played
  played="$(python3 - "$1" "$NGT_DEV_HOME" <<'PY' 2>/dev/null
import json
import sys
from pathlib import Path

import yaml

dev_home = Path(sys.argv[2])
try:
    runs = {int(r["run"]) for r in (yaml.safe_load(Path(sys.argv[1]).read_text()) or {}).get("runs", [])}
except Exception:
    runs = set()
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
    pass
print(" ".join(str(run) for run in sorted(played)))
PY
)" || return 0
  if [ -n "$played" ]; then
    echo "!! \$NGT_DEV_HOME isn't fresh for this scenario -- '$0 reset-scenario' clears it:"
    echo "!!   run(s) $played were already played against this instance: the watchers will ignore them"
    echo
  fi
}

cmd_play() {
  # scenario-player/live_demo.sh's `play`, for Airflow: everything a scenario needs from a cold
  # start, then the scenario itself. Every argument goes to scenario_player.py untouched, and
  # the ones that play nothing skip the bring-up.
  local arg label scenario="" skip=0 views=0
  for arg in "$@"; do
    if [ "$skip" = 1 ]; then skip=0; continue; fi
    case "$arg" in
      -h|--help|--dry-run|--validate-only|--only-list-calibrations)
        activate_venv
        exec python3 "$PLAYER" "$@"
        ;;
      --monitor) views=1 ;;
      --speed) skip=1 ;;  # its value isn't the scenario
      -*) ;;
      *) [ -n "$scenario" ] || scenario="$arg" ;;
    esac
  done
  activate_venv
  if [ ! -f "$NGT_DEV_HOME/env.sh" ]; then
    echo "(no scratch environment at $NGT_DEV_HOME yet -- setting one up first)"
    cmd_setup
    echo
  fi
  # A malformed scenario fails here, before any of Airflow is touched.
  python3 "$PLAYER" "$@" --only-list-calibrations > /dev/null
  if [ -z "${NGT_AIRFLOW_IN_CONTAINER:-}" ]; then
    for label in "${COMPONENTS[@]}"; do
      if ! component_pid "$label" > /dev/null; then
        cmd_start
        echo
        break
      fi
    done
  fi
  echo "Waiting for the dag-processor to parse ngt_dags_watch.py..."
  _wait_for_watch_design dags
  cmd_pause_unpause_watch unpause > /dev/null
  echo "Waiting for the triggerer to run the 4 watchers..."
  _wait_for_watch_design live
  echo
  _warn_if_not_fresh "$scenario"
  if [ "$views" = 0 ]; then
    python3 "$PLAYER" "$@"
    return
  fi
  # --monitor: the player reports the files the pipeline produces; watch_views.py reports what
  # Airflow did to produce them, alongside, then sums it up once the player is done.
  local since follower status=0
  since="$(date -u +%Y-%m-%dT%H:%M:%S)"
  python3 "$VIEWS" follow "$since" &
  follower=$!
  # The pid expanded now: the trap runs after this function's locals are gone.
  # shellcheck disable=SC2064
  trap "kill $follower 2>/dev/null" EXIT
  python3 "$PLAYER" "$@" || status=$?
  sleep 3  # the follower's last poll
  kill "$follower" 2>/dev/null || true
  wait "$follower" 2>/dev/null || true
  trap - EXIT
  echo
  python3 "$VIEWS" summary "$since"
  return "$status"
}

cmd_reset_scenario() {
  # Wraps scenario-player/sim_env.sh's own sim_reset/sim_setup (wipes and recreates
  # $NGT_DEV_HOME -- every fake OMS run/EOS file/output/log) around a
  # stop/restart of Airflow's processes, for the same reason reset-airflow
  # stops/restarts around its own DB reset: doing this while the triggerer
  # has in-flight watcher triggers (ngt_dags_watch.py's AssetWatchers)
  # polling paths under $NGT_DEV_HOME left that work stuck in a running/
  # deferred state indefinitely, and each process's own console log silently
  # stopped updating (confirmed live) -- restarting cleanly afterward is what
  # actually recovers them. sim_env.sh's own reset/setup deliberately don't
  # know or check any of this (see their comments) -- that coordination lives
  # here, in the Airflow-specific adapter, on purpose. Doesn't touch DAG-run
  # history -- pair with reset-watch/reset-airflow for that.
  #
  # Does clear ngt_dags_watch.py's AssetStateStore watermarks (_reset_ngt_watermarks), though:
  # they live in the Airflow metadata DB, scoped to the Asset row, so sim_env.sh's reset+setup
  # can't reach them, and left alone they'd keep describing fake run/file state this just wiped
  # (see _reset_ngt_watermarks' own comment). That's the only Airflow-DB side effect here --
  # it only touches the watermark rows, not the Assets/DAG history/pause state.
  local was_running=0
  if any_component_running; then
    was_running=1
    cmd_stop
  fi

  sim_reset
  activate_venv
  sim_setup
  _reset_ngt_watermarks
  echo "Reset and re-set-up $NGT_DEV_HOME."

  if [ "$was_running" = "1" ]; then
    echo "Restarting the 4 processes (they were running before the reset)..."
    cmd_start
  fi
}

if [ -n "${NGT_AIRFLOW_IN_CONTAINER:-}" ]; then
  case "${1:-}" in
    start|stop|status|ui|logs)
      echo "In a container the container runtime runs Airflow's processes -- see airflow_automation/airflow_containers/README.md." >&2
      exit 1
      ;;
  esac
fi

case "${1:-}" in
  setup) cmd_setup ;;
  start) cmd_start ;;
  stop) cmd_stop "${2:-}" ;;
  status) cmd_status ;;
  ui) cmd_ui ;;
  logs) cmd_logs "${2:?scheduler, dag-processor, or api required}" ;;
  unpause-watch) cmd_pause_unpause_watch unpause ;;
  pause-watch) cmd_pause_unpause_watch pause ;;
  reset-watch) cmd_reset_watch ;;
  reset-airflow) cmd_reset_airflow ;;
  reset-scenario) cmd_reset_scenario ;;
  seed-run)
    calibration="${2:?calibration required}"; run="${3:?run number required}"; shift 3 || true
    activate_venv
    python3 "$REPO_DIR/scenario-player/seed.py" seed-run --calibration "$calibration" --run "$run" "$@"
    ;;
  add-ls)
    activate_venv
    python3 "$REPO_DIR/scenario-player/seed.py" add-ls --calibration "${2:?calibration required}" --run "${3:?run required}" --ls "${4:?ls number required}"
    ;;
  end-run)
    activate_venv
    python3 "$REPO_DIR/scenario-player/seed.py" end-run --run "${2:?run number required}"
    ;;
  list-runs)
    activate_venv
    python3 "$REPO_DIR/scenario-player/seed.py" list-runs
    ;;
  play)
    # As in live_demo.sh: everything after `play` goes to scenario_player.py untouched, and the
    # scenario path is resolved relative to your CWD, exactly as if you'd run the player directly.
    shift
    cmd_play "$@"
    ;;
  arm-fault)
    shift
    activate_venv
    python3 "$REPO_DIR/scenario-player/seed.py" arm-fault "$@"
    ;;
  list-faults)
    shift
    activate_venv
    python3 "$REPO_DIR/scenario-player/seed.py" list-faults "$@"
    ;;
  clear-faults)
    shift
    activate_venv
    python3 "$REPO_DIR/scenario-player/seed.py" clear-faults "$@"
    ;;
  *) usage; exit 1 ;;
esac
