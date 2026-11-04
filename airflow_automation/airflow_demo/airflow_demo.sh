#!/bin/bash
# The Airflow prototype on this machine, against scenario-player/'s fake OMS/EOS/CMSSW toolchain: real
# DAG runs, job scripts and output files, entirely offline. Needs Linux or WSL, and the venv and Postgres
# role from README.md's Airflow setup section. One instance per checkout and $NGT_DEV_HOME (see
# airflow_env.sh), as one background `airflow standalone`. From a cold start:
#
#   ./airflow_automation/airflow_demo/airflow_demo.sh play scenario-player/scenarios/basic_ecal_pedestals.yaml
#   ./airflow_automation/airflow_demo/airflow_demo.sh stop
#
# In a container (NGT_AIRFLOW_IN_CONTAINER, set by airflow_containers/Dockerfile) the image is the venv
# and the container runtime runs Airflow's processes: start/stop/status/ui/logs are refused, and the
# reset-* leave stopping them to the caller (airflow_compose.sh does; in the one-pod deployment a new
# pod is the reset).

set -euo pipefail

# shellcheck disable=SC1091
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/airflow_env.sh"
PLAYER="$REPO_DIR/scenario-player/scenario_player.py"
SEED="$REPO_DIR/scenario-player/seed.py"
VIEWS="$REPO_DIR/airflow_automation/airflow_demo/watch_views.py"
PID_FILE="$AIRFLOW_HOME/airflow_demo.pid"
LOG="$NGT_DEV_HOME/logs/airflow.log"
# The primer last, so that the file watchers are live before a run is discovered.
WATCH_DAGS=(ngt_watch_process_sistripbad ngt_watch_process_ecalpedestals ngt_watch_process_beamspot ngt_watch_run_primer)

usage() {
  cat <<USAGE
Usage: $0 <command> [args]

Airflow (this checkout's own instance, UI on :$NGT_AIRFLOW_PORT):
  setup             Create \$NGT_DEV_HOME and this instance's database. Safe to re-run
  start             Launch airflow standalone in the background
  stop              Stop it
  status            Whether it's running, and the health of each component
  ui                Print the UI's URL
  logs [component]  Follow its log, or only the lines of scheduler, dag-processor, api-server or triggerer
  unpause-watch     Unpause the 4 watch DAGs, which starts the AssetWatchers
  pause-watch       Pause them again

Resets:
  reset-scenario    Wipe \$NGT_DEV_HOME and the watchers' watermarks, leaving DAG history alone.
                    Stops and restarts Airflow around it, if running
  reset-airflow     Wipe the whole metadata DB. Stops and restarts Airflow around it, if running
  reset-watch       Clear the 4 DAGs' history and Assets, which also pauses them again

A fake run (calibration: SiStripBad, EcalPedestals or BeamSpot):
  seed-run <calibration> <run> [--ls N ...] [--minutes-ago M]
  add-ls <calibration> <run> <ls>
  end-run <run>
  list-runs
  play <scenario.yaml> [options]
                    A whole scenario from a cold start: sets up and starts whatever isn't, unpauses the
                    DAGs and waits for the watchers. The options go to scenario_player.py (--help lists
                    them); --interactive --monitor is the guided walkthrough, and --monitor also reports
                    Airflow's side (watch_views.py). Warns if \$NGT_DEV_HOME isn't fresh for the scenario.

Injected failures (scenario-player/faults.py; the options: python3 scenario-player/seed.py arm-fault --help):
  arm-fault --target <oms|cmsrun|upload_conditions> --mode <mode> [options]
  list-faults [--target <target>]
  clear-faults [--target <target>]
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
  # Only the database airflow_env.sh derived: any other is the operator's to provision. The
  # `airflow_ngt` role has CREATEDB (README.md's Airflow setup section), so no sudo is needed.
  [ "$AIRFLOW__DATABASE__SQL_ALCHEMY_CONN" = "$NGT_AIRFLOW_DB_CONN" ] || return 0
  local -x PGPASSWORD="$NGT_AIRFLOW_DB_PASSWORD"
  local connection=(-h "$NGT_AIRFLOW_DB_HOST" -p "$NGT_AIRFLOW_DB_PORT" -U "$NGT_AIRFLOW_DB_USER")
  local exists
  exists="$(psql "${connection[@]}" -d postgres -Atc "SELECT 1 FROM pg_database WHERE datname = '$NGT_AIRFLOW_DB_NAME'")"
  [ -n "$exists" ] || createdb "${connection[@]}" "$NGT_AIRFLOW_DB_NAME"
}

cmd_setup() {
  sim_setup
  ensure_airflow_db
  airflow db migrate
  [ -n "${NGT_AIRFLOW_IN_CONTAINER:-}" ] || echo "Set up $NGT_DEV_HOME and database $NGT_AIRFLOW_DB_NAME. Next: $0 start"
}

standalone_pid() {
  # The name is checked because a pid file can outlive a reboot, and by then its number can belong to anything.
  local pid
  pid="$(cat "$PID_FILE" 2>/dev/null)" || return 1
  [[ "$(ps -o args= -p "$pid" 2>/dev/null)" == *airflow* ]] && echo "$pid"
}

cmd_start() {
  if standalone_pid > /dev/null; then
    echo "Airflow is already running"
    return
  fi
  if [ ! -f "$NGT_DEV_HOME/env.sh" ]; then
    echo "No $NGT_DEV_HOME/env.sh -- run: $0 setup" >&2
    exit 1
  fi
  cmd_stop
  mkdir -p "$NGT_DEV_HOME/logs" "$AIRFLOW_HOME"

  # env.sh is sim_env.sh's one spelling-out of the fake environment. Its own session outlives this
  # script and its terminal, and holds everything standalone starts, tasks included (those get
  # process groups of their own), for cmd_stop to reach. A script's background job isn't a process
  # group leader, so setsid doesn't fork: $! is standalone itself, and the session's ID. Appending,
  # so that a restart keeps why it died.
  (
    # shellcheck disable=SC1091
    source "$NGT_DEV_HOME/env.sh"
    exec setsid airflow standalone
  ) >> "$LOG" 2>&1 < /dev/null &
  echo "$!" > "$PID_FILE"
  echo "Started airflow standalone (pid $!, log: $LOG)"
  echo "Give the dag-processor ~10-15s to parse airflow_automation/airflow_dags/ before unpausing DAGs."
  cmd_ui
}

cmd_ui() {
  echo "Airflow UI: http://localhost:$NGT_AIRFLOW_PORT"
  echo "  (Simple Auth Manager, all-admins mode for this offline dev instance -- no login needed)"
}

cmd_status() {
  local pid
  if pid="$(standalone_pid)"; then
    echo "airflow standalone is running (pid $pid). Its components:"
    # standalone itself carries on when one of them dies, so only this shows it.
    curl -fsS --noproxy '*' --max-time 5 "http://localhost:$NGT_AIRFLOW_PORT/api/v2/monitor/health" 2>/dev/null \
      | python3 -m json.tool 2>/dev/null || echo "  The API server isn't answering -- see '$0 logs api-server'"
  elif [ -f "$PID_FILE" ]; then
    echo "Airflow has died -- see $LOG"
  else
    echo "Airflow isn't running"
  fi
}

cmd_logs() {
  # standalone starts each of a component's lines with its name.
  tail -f "$LOG" | grep --line-buffered "^${1:-}"
}

cmd_stop() {
  # To the whole session: standalone orphans its components on a TERM, and tasks have process groups
  # of their own. What won't go in time gets killed, like the workers of a dead scheduler, which
  # ignore TERM. Airflow's own escalation on a task that's still running takes up to 15 s. A dead
  # standalone's remains are reached this way too.
  local sid deadline=$((SECONDS + ${NGT_AIRFLOW_STOP_SECONDS:-30}))
  sid="$(cat "$PID_FILE" 2>/dev/null)" || return 0
  rm -f "$PID_FILE"
  pgrep -s "$sid" -f airflow > /dev/null || return 0

  echo "Stopping Airflow"
  pkill -TERM -s "$sid" || true
  while pgrep -s "$sid" > /dev/null && [ "$SECONDS" -lt "$deadline" ]; do
    sleep 0.5
  done
  pkill -KILL -s "$sid" || true
}

cmd_pause_unpause_watch() {
  # AssetWatchers only run while their consuming DAG is unpaused.
  local dag
  for dag in "${WATCH_DAGS[@]}"; do
    airflow dags "$1" -y "$dag"
  done
}

with_airflow_stopped() {
  # Airflow's processes hold state tied to what's being wiped. Seen live: all of them crashing once
  # the tables are dropped under them (UndefinedTable: relation "job" does not exist), and watchers
  # polling a deleted $NGT_DEV_HOME stuck until restarted.
  local was_running=0
  if standalone_pid > /dev/null; then
    was_running=1
    cmd_stop
  fi

  "$@"

  if [ "$was_running" = 1 ]; then
    cmd_start
  fi
}

cmd_reset_watch() {
  # Deleting a DAG deletes its row, so the next parse recreates it paused. The Assets' watermarks
  # (asset_state_store) go with them by ON DELETE CASCADE. Dev-script admin actions, like all the
  # ORM use here: only task code is forbidden it. Skipped if the ORM shape has drifted.
  local dag
  for dag in "${WATCH_DAGS[@]}"; do
    airflow dags delete -y "$dag"
  done
  python3 - <<'PY' || echo "  (Asset row cleanup skipped -- use reset-airflow for a full wipe)"
from airflow.models.asset import AssetActive, AssetEvent, AssetModel
from airflow.utils.session import create_session
from sqlalchemy import or_

with create_session() as session:
    assets = session.query(AssetModel).filter(or_(AssetModel.uri.like("ngt://runs%"), AssetModel.uri.like("ngt://files/%"))).all()
    ids = [a.id for a in assets]
    session.query(AssetEvent).filter(AssetEvent.asset_id.in_(ids)).delete(synchronize_session=False)
    session.query(AssetActive).filter(AssetActive.uri.in_([a.uri for a in assets])).delete(synchronize_session=False)
    session.query(AssetModel).filter(AssetModel.id.in_(ids)).delete(synchronize_session=False)
    print(f"  Removed {len(ids)} Asset row(s).")
PY
  echo "Cleared DAG-run history for all 4 ngt_watch_* DAGs."
}

reset_airflow() {
  echo "Resetting the entire Airflow metadata DB..."
  airflow db reset -y
  airflow db migrate
  echo "Done -- all DAGs are paused again, as after a fresh install."
}

reset_scenario() {
  # The watchers' watermarks (seen_runs, emitted_run<N>) live in the metadata DB, where sim_env.sh
  # can't reach them: left alone, they'd make a run number reused after this look already handled.
  sim_reset
  sim_setup
  python3 - <<'PY' || echo "  (AssetStateStore cleanup skipped -- use reset-watch for a full wipe)"
from airflow.models.asset import AssetModel
from airflow.models.asset_state_store import AssetStateStoreModel
from airflow.utils.session import create_session
from sqlalchemy import or_

with create_session() as session:
    assets = session.query(AssetModel).filter(or_(AssetModel.uri.like("ngt://runs%"), AssetModel.uri.like("ngt://files/%")))
    ids = [a.id for a in assets]
    removed = session.query(AssetStateStoreModel).filter(AssetStateStoreModel.asset_id.in_(ids)).delete(synchronize_session=False)
    print(f"  Removed {removed} AssetStateStore row(s) (seen_runs / emitted_run<N> watermarks).")
PY
  echo "Reset and re-set-up $NGT_DEV_HOME."
}

_wait_for_watch_design() {
  # Blocks until the 4 DAGs are parsed ($1 = dags: unpausing a DAG the dag-processor hasn't seen yet
  # fails) or until they're also unpaused, a scheduler is alive and the newest live triggerer is
  # running all 4 watchers ($1 = live: a run seeded before then could end before the run watcher sees
  # it). The pause state from the DB, since `airflow dags unpause`'s output isn't reliable evidence.
  WATCH_DAGS="${WATCH_DAGS[*]}" WAIT_FOR="$1" python3 - <<'PY'
import os
import sys
import time
from datetime import datetime, timedelta, timezone

from airflow.jobs.job import Job
from airflow.models import DagModel
from airflow.models.trigger import Trigger
from airflow.utils.session import create_session

dag_ids = set(os.environ["WATCH_DAGS"].split())
watchers = ("airflow_dags.triggers.RunWatcherTrigger", "airflow_dags.triggers.LumisectionFileWatcherTrigger")
want_live = os.environ["WAIT_FOR"] == "live"
deadline = time.monotonic() + 180


def live_job(session, job_type):
    job = session.query(Job).filter(Job.job_type == job_type, Job.state == "running").order_by(Job.id.desc()).first()
    return job if job and job.latest_heartbeat > datetime.now(timezone.utc) - timedelta(seconds=30) else None


while True:
    with create_session() as session:
        paused = {d.dag_id: d.is_paused for d in session.query(DagModel).filter(DagModel.dag_id.in_(dag_ids))}
        ready = set(paused) == dag_ids
        if ready and want_live:
            triggerer = live_job(session, "TriggererJob")
            running = triggerer and session.query(Trigger).filter(
                Trigger.classpath.in_(watchers), Trigger.triggerer_id == triggerer.id
            ).count()
            ready = not any(paused.values()) and live_job(session, "SchedulerJob") and running >= len(dag_ids)
    if ready:
        sys.exit(0)
    if time.monotonic() > deadline:
        sys.exit(
            "the watch DAGs never got parsed -- see `airflow_demo.sh logs dag-processor`"
            if not want_live
            else "the watchers or the scheduler aren't running -- see `airflow_demo.sh status`, which `stop` and `start` fix"
        )
    time.sleep(2)
PY
}

_warn_if_not_fresh() {
  # $1: a scenario file. A scenario played on scratch state an earlier one left behind misleads rather
  # than fails: runs this instance already handled -- a working dir left in $NGT_DEV_HOME (find_new_run
  # skips it) or the run in the run watcher's seen_runs watermark -- silently do nothing. Only warns:
  # replaying on purpose is legitimate.
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
  # live_demo.sh's `play`, for Airflow: everything a scenario needs from a cold start, then the
  # scenario. Every argument goes to scenario_player.py untouched, and the ones that play nothing
  # skip the bring-up.
  local arg scenario="" skip=0 views=0
  for arg in "$@"; do
    if [ "$skip" = 1 ]; then skip=0; continue; fi
    case "$arg" in
      -h|--help|--dry-run|--validate-only|--only-list-calibrations) exec python3 "$PLAYER" "$@" ;;
      --monitor) views=1 ;;
      --speed) skip=1 ;;  # its value isn't the scenario
      -*) ;;
      *) [ -n "$scenario" ] || scenario="$arg" ;;
    esac
  done
  if [ ! -f "$NGT_DEV_HOME/env.sh" ]; then
    echo "(no scratch environment at $NGT_DEV_HOME yet -- setting one up first)"
    cmd_setup
    echo
  fi
  # A malformed scenario fails here, before any of Airflow is touched.
  python3 "$PLAYER" "$@" --only-list-calibrations > /dev/null
  if [ -z "${NGT_AIRFLOW_IN_CONTAINER:-}" ] && ! standalone_pid > /dev/null; then
    cmd_start
    echo
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

if [ -n "${NGT_AIRFLOW_IN_CONTAINER:-}" ]; then
  case "${1:-}" in
    start|stop|status|ui|logs)
      echo "In a container the container runtime runs Airflow's processes -- see airflow_automation/airflow_containers/README.md." >&2
      exit 1
      ;;
  esac
fi

case "${1:-}" in
  ""|stop|status|ui|logs) ;;
  *) activate_venv ;;
esac

case "${1:-}" in
  setup) cmd_setup ;;
  start) cmd_start ;;
  stop) cmd_stop ;;
  status) cmd_status ;;
  ui) cmd_ui ;;
  logs) cmd_logs "${2:-}" ;;
  unpause-watch) cmd_pause_unpause_watch unpause ;;
  pause-watch) cmd_pause_unpause_watch pause ;;
  reset-watch) cmd_reset_watch ;;
  reset-airflow) with_airflow_stopped reset_airflow ;;
  reset-scenario) with_airflow_stopped reset_scenario ;;
  seed-run) python3 "$SEED" seed-run --calibration "${2:?calibration required}" --run "${3:?run number required}" "${@:4}" ;;
  add-ls)
    python3 "$SEED" add-ls --calibration "${2:?calibration required}" --run "${3:?run required}" \
      --ls "${4:?ls number required}"
    ;;
  end-run) python3 "$SEED" end-run --run "${2:?run number required}" ;;
  list-runs|arm-fault|list-faults|clear-faults) python3 "$SEED" "$@" ;;
  play) shift; cmd_play "$@" ;;
  *) usage; exit 1 ;;
esac
