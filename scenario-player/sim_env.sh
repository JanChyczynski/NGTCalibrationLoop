#!/bin/bash
# Engine-agnostic pieces of the live-demo/live-test setup: the scratch
# environment location, and how to (re)create the fake OMS/EOS/CMSSW
# toolchain + calibration config inside it.
#
# This file knows nothing about any specific workflow engine -- it's the
# "simulate a live calibration run" half of the live-test setup, deliberately
# kept separate from whatever drives a specific engine. An adapter for any
# engine would source this same file and reuse `sim_setup` unchanged -- only
# the engine-specific script differs.
#
# Usage:
#   source scenario-player/sim_env.sh          -- sets REPO_DIR, NGT_DEV_HOME, NGT_INSTANCE, and the
#                                      sim_setup/sim_reset/ngt_tmux_new_session
#                                      functions; doesn't execute anything on its own.
#   ./scenario-player/sim_env.sh setup|reset   -- run directly instead (see
#                                      sim_setup/sim_reset below for what each does).

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Optional, gitignored per-machine/per-checkout overrides (see local_sim_env.sh.example).
# Sourced FIRST, seeing only $REPO_DIR, so everything below is `${VAR:-default}` and a
# plain `VAR=value` in that file wins -- including values derived from it further down,
# and the environment's. Set NGT_INSTANCE there to rename everything that is derived from it.
# NGT_SKIP_LOCAL_ENV=1 skips it (and airflow_env.sh's local_airflow_env.sh): tests/conftest.py
# sets it, since an NGT_DEV_HOME in it would beat a test's own and send a test that spawns
# this file or live_demo.sh into that real scratch environment.
# shellcheck disable=SC1091
if [ -z "${NGT_SKIP_LOCAL_ENV:-}" ] && [ -f "$(dirname "${BASH_SOURCE[0]}")/local_sim_env.sh" ]; then
  source "$(dirname "${BASH_SOURCE[0]}")/local_sim_env.sh"
fi

# A name for this checkout, unique per $REPO_DIR (basename + a short hash of the full path),
# that engine adapters use to keep several checkouts' state apart -- see
# airflow_automation/airflow_demo/airflow_env.sh. NGT_INSTANCE_HASH is the same hash as a
# number, for deriving per-checkout ports.
if [ -z "${NGT_INSTANCE_HASH:-}" ]; then
  NGT_INSTANCE_HASH="$(printf '%s' "$REPO_DIR" | cksum | cut -d' ' -f1)"
fi
if [ -z "${NGT_INSTANCE:-}" ]; then
  _ngt_base="$(basename "$REPO_DIR" | tr '[:upper:]' '[:lower:]' | tr -c 'a-z0-9\n' '_')"
  NGT_INSTANCE="${_ngt_base:0:30}_$(printf '%06x' $((NGT_INSTANCE_HASH % 16777216)))"
  unset _ngt_base
fi
export NGT_INSTANCE NGT_INSTANCE_HASH

export NGT_DEV_HOME="${NGT_DEV_HOME:-$REPO_DIR/demo-env}"

# The fake toolchain's identity, written into ngtParameters.jsn and the fake CMSSW area.
# Fake, unvalidated placeholder values -- nothing in scenario-player/bin/ reads them.
NGT_SIM_CMSSW_VERSION="${NGT_SIM_CMSSW_VERSION:-CMSSW_16_0_7_patch1}"
NGT_SIM_SCRAM_ARCH="${NGT_SIM_SCRAM_ARCH:-el8_amd64_gcc13}"
NGT_SIM_GLOBAL_TAG="${NGT_SIM_GLOBAL_TAG:-160X_dataRun3_ExpressNGT_v0}"

# faults.py (the shared fault-injection vocabulary) sits next to seed.py/
# scenario_player.py rather than inside an installed package, so put this
# directory on PYTHONPATH for the one consumer that runs elsewhere: the fake
# omsapi's live-demo fault path (tests/stubs/omsapi), which executes inside
# the engine's own worker processes. seed.py/scenario_player.py find it
# as a sibling, and pytest via tests/conftest.py's sys.path setup.
export PYTHONPATH="$REPO_DIR/scenario-player:${PYTHONPATH:-}"

# sim_setup: create/refresh everything under $NGT_DEV_HOME that any engine
# adapter needs -- the fake scenario-player/bin/ toolchain, env.sh, ngtParameters.jsn,
# and (via scenario-player/seed.py, itself fully engine-agnostic) the per-calibration
# fake EOS layout and an empty fake OMS runs file. Safe to re-run.
#
# Needs an active venv (any one) for its `pip install -e tests/stubs` step
# below -- checked upfront so a forgotten `source .../activate` fails with a
# clear message here instead of pip's much less obvious
# "externally-managed-environment" error (Debian/Ubuntu's system Python
# refuses `pip install` outright, per PEP 668).
sim_setup() {
  if [ -z "${VIRTUAL_ENV:-}" ]; then
    echo "sim_setup needs an active Python venv (for 'pip install -e tests/stubs')." >&2
    echo "Activate one first, e.g.:" >&2
    echo "  source ~/airflow3-ngt-venv/bin/activate" >&2
    echo "  source .venv/bin/activate                 # or the plain venv used for pytest" >&2
    return 1
  fi

  mkdir -p "$NGT_DEV_HOME"/{data,logs,cond_auth,calibrationYAML,eos,bin,faults,conddb}
  mkdir -p "$NGT_DEV_HOME/cmssw_home/$NGT_SIM_CMSSW_VERSION/src"

  # -r-less glob copy: skip subdirs (e.g. a stray __pycache__) instead of
  # letting cp's non-zero exit on them silently abort this function under set -e.
  for f in "$REPO_DIR"/scenario-player/bin/*; do
    [ -f "$f" ] && cp "$f" "$NGT_DEV_HOME/bin/"
  done
  chmod +x "$NGT_DEV_HOME"/bin/*

  cat > "$NGT_DEV_HOME/ngtParameters.jsn" <<JSON
{
    "SCRAM_ARCH": "$NGT_SIM_SCRAM_ARCH",
    "CMSSW_VERSION": "$NGT_SIM_CMSSW_VERSION",
    "GLOBAL_TAG": "$NGT_SIM_GLOBAL_TAG",
    "DATA_BASE_PATH": "$NGT_DEV_HOME/data",
    "LOG_BASE_PATH": "$NGT_DEV_HOME/logs",
    "COND_AUTH_PATH": "$NGT_DEV_HOME/cond_auth"
}
JSON

  # The one place that spells out the fake environment: engine adapters source
  # this instead of re-deriving the same exports themselves.
  cat > "$NGT_DEV_HOME/env.sh" <<ENV
export NGT_DEV_HOME="$NGT_DEV_HOME"
export NGT_PARAMETERS_PATH="$NGT_DEV_HOME/ngtParameters.jsn"
export NGT_CALIBRATION_YAML_DIR="$NGT_DEV_HOME/calibrationYAML"
export NGT_OMS_STUB_RUNS_FILE="$NGT_DEV_HOME/oms_runs.json"
export PATH="$NGT_DEV_HOME/bin:\$PATH"
# scenario-player/ for the fake omsapi's live fault path, which imports faults.py.
export PYTHONPATH="$REPO_DIR/tests/stubs:$REPO_DIR/scenario-player:\${PYTHONPATH:-}"

# Where seed.py's arm_fault() leaves armed faults for bin/cmsRun,
# bin/uploadConditions.py and the fake omsapi to consume -- without these the
# fakes never look, and a scenario's faults: entries are silently inert for
# whatever sources this file (the loops live_demo.sh starts get their
# environment from it alone).
export NGT_FAULTS_DIR="$NGT_DEV_HOME/faults"
export NGT_OMS_STUB_FAULTS_FILE="$NGT_DEV_HOME/faults/oms.json"

# A real CMSSW environment provides cmsenv/cmsrel as shell *functions*, and bash
# resolves a function before it searches \$PATH -- an exported one survives into
# the job scripts the loops spawn and would run instead of bin/'s fake. Dropping
# them here covers the whole chain, since a function can only arrive by
# inheritance from the shell that sourced this file.
unset -f cmsenv cmsrel cmsDriver.py cmsRun uploadConditions.py 2>/dev/null || true
ENV

  python3 "$REPO_DIR/scenario-player/seed.py" setup

  # Makes the fake `omsapi` package importable by whatever Python process is
  # actually running the engine's workers. Harmless no-op for an engine that
  # doesn't need it.
  pip install -e "$REPO_DIR/tests/stubs" -q
}

# sim_reset: wipe all simulator state -- every fake run/lumisection/EOS file/
# output $NGT_DEV_HOME holds, and the conddb/ upload ledger a scenario's
# expectations are checked against, regardless of which engine (if any) produced or
# is watching it. Does NOT recreate it afterward (call sim_setup for that) and
# does NOT touch any engine's own state -- an engine that keeps run-history
# outside $NGT_DEV_HOME needs its own separate reset too.
#
# Caution (deliberately not checked/enforced here -- this function stays
# engine-agnostic on purpose, see the module docstring): deleting
# $NGT_DEV_HOME out from under an engine whose processes are *currently
# running* and hold in-flight work referencing it (e.g. a long-lived poll or
# a deferred/suspended wait) can leave that work stuck rather than erroring
# cleanly -- confirmed live with Airflow's triggerer (the AssetWatcher
# triggers in airflow_automation/airflow_dags/triggers.py): an already-running
# trigger kept polling stale state and
# never resolved until the triggerer process itself was restarted, and each
# process's own console log silently stopped updating (still writing to the
# now-unlinked old file). If an engine is running, stop it first, or use its
# adapter's own coordinated reset if it has one (e.g. airflow_automation/airflow_demo/airflow_demo.sh
# reset-scenario) rather than calling this directly.
sim_reset() {
  echo "Removing $NGT_DEV_HOME"
  rm -rf "$NGT_DEV_HOME"
}

# ngt_tmux_new_session: `tmux new-session` (same arguments) for an engine adapter's
# long-running sessions. Whichever tmux client runs first starts the tmux server, and the
# server hands every shell it ever spawns -- an operator's own new windows included -- that
# client's environment. Started from an adapter with this checkout's settings exported
# (NGT_INSTANCE, NGT_DEV_HOME, AIRFLOW_HOME, ...), it would make them the default of every
# later shell, where another checkout's scripts would take them for their own and act on
# this checkout's state. So the client runs without any NGT_*/AIRFLOW_* variable, and a
# session's command has to bring its own: live_demo.sh's source $NGT_DEV_HOME/env.sh,
# airflow_demo.sh's the settings file its start writes.
ngt_tmux_new_session() {
  local v unset_args=()
  for v in $(compgen -e); do
    case "$v" in NGT_*|AIRFLOW_*) unset_args+=(-u "$v") ;; esac
  done
  env "${unset_args[@]}" tmux new-session "$@"
}

# Allow running this file directly for setup/reset (neither needs any
# engine-specific environment) as well as sourcing it for sim_setup/
# NGT_DEV_HOME/REPO_DIR (still the primary, documented usage -- see above).
if [ "${BASH_SOURCE[0]}" = "$0" ]; then
  set -euo pipefail
  case "${1:-}" in
    setup) sim_setup; echo "Set up $NGT_DEV_HOME" ;;
    reset) sim_reset ;;
    *) echo "Usage: $0 <setup|reset>" >&2; exit 1 ;;
  esac
fi
