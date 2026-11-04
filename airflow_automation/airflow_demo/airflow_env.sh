#!/bin/bash
# The settings of a local Airflow 3 instance running this repo's DAGs, against a per-instance database
# on the local Postgres (README.md's Airflow setup section). Sourced by airflow_demo.sh, and usable by
# hand: source airflow_automation/airflow_demo/airflow_env.sh
#
# Whatever could collide between two checkouts, or two $NGT_DEV_HOMEs of one, derives from
# $NGT_INSTANCE or $NGT_INSTANCE_HASH (sim_env.sh), so they can run side by side with no configuration.
# All of it is overridable, from the environment or the gitignored local_airflow_env.sh next to this
# file, which beats the environment: see local_airflow_env.sh.example.

# shellcheck disable=SC1091
source "$(cd "$(dirname "${BASH_SOURCE[0]}")/../../scenario-player" && pwd)/sim_env.sh"

# shellcheck disable=SC1091
if [ -z "${NGT_SKIP_LOCAL_ENV:-}" ] && [ -f "$(dirname "${BASH_SOURCE[0]}")/local_airflow_env.sh" ]; then
  source "$(dirname "${BASH_SOURCE[0]}")/local_airflow_env.sh"
fi

# Shared between checkouts: it holds no per-checkout state, apart from sim_setup's editable installs.
export NGT_AIRFLOW_VENV="${NGT_AIRFLOW_VENV:-$HOME/airflow3-ngt-venv}"

# Per instance, so that two never share an airflow.cfg (and with it the JWT secrets the API server
# and its workers must agree on) or task logs.
export AIRFLOW_HOME="${AIRFLOW_HOME:-$HOME/airflow3-ngt-$NGT_INSTANCE}"

# Per instance: it holds the Asset watermarks, which describe the runs under one $NGT_DEV_HOME.
# `airflow_demo.sh setup` creates it. Set AIRFLOW__DATABASE__SQL_ALCHEMY_CONN to use any other
# database, which `setup` then leaves creating to you.
export NGT_AIRFLOW_DB_HOST="${NGT_AIRFLOW_DB_HOST:-localhost}"
export NGT_AIRFLOW_DB_PORT="${NGT_AIRFLOW_DB_PORT:-5432}"
export NGT_AIRFLOW_DB_USER="${NGT_AIRFLOW_DB_USER:-airflow_ngt}"
export NGT_AIRFLOW_DB_PASSWORD="${NGT_AIRFLOW_DB_PASSWORD:-airflow_ngt}"
export NGT_AIRFLOW_DB_NAME="${NGT_AIRFLOW_DB_NAME:-airflow3_ngt_$NGT_INSTANCE}"
export NGT_AIRFLOW_DB_CONN="postgresql+psycopg2://$NGT_AIRFLOW_DB_USER:$NGT_AIRFLOW_DB_PASSWORD@$NGT_AIRFLOW_DB_HOST:$NGT_AIRFLOW_DB_PORT/$NGT_AIRFLOW_DB_NAME"
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="${AIRFLOW__DATABASE__SQL_ALCHEMY_CONN:-$NGT_AIRFLOW_DB_CONN}"

export AIRFLOW__CORE__DAGS_FOLDER="${AIRFLOW__CORE__DAGS_FOLDER:-$REPO_DIR/airflow_automation/airflow_dags}"
export AIRFLOW__CORE__LOAD_EXAMPLES="${AIRFLOW__CORE__LOAD_EXAMPLES:-False}"
export AIRFLOW__DAG_PROCESSOR__REFRESH_INTERVAL="${AIRFLOW__DAG_PROCESSOR__REFRESH_INTERVAL:-15}"
export AIRFLOW__DAG_PROCESSOR__MIN_FILE_PROCESS_INTERVAL="${AIRFLOW__DAG_PROCESSOR__MIN_FILE_PROCESS_INTERVAL:-10}"
export AIRFLOW__API__EXPOSE_CONFIG="${AIRFLOW__API__EXPOSE_CONFIG:-True}"

# No login, and everyone is admin: right for an offline, single-user instance (README.md's Airflow
# setup section), so it listens on this machine only. A browser on the Windows side of WSL still
# reaches it at http://localhost:<port>, but not at the WSL VM's own IP.
export AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS="${AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS:-True}"
export AIRFLOW__API__HOST="${AIRFLOW__API__HOST:-127.0.0.1}"

# The UI gets 8100-8899, from the instance hash (a collision is possible but unlikely: override
# NGT_AIRFLOW_PORT if it happens). The task-log servers' defaults are the same for every instance.
# Tasks reach the metadata DB through the API server, so they have to be told its port too.
export NGT_AIRFLOW_PORT="${NGT_AIRFLOW_PORT:-$((8100 + NGT_INSTANCE_HASH % 800))}"
export AIRFLOW__API__PORT="$NGT_AIRFLOW_PORT"
export AIRFLOW__LOGGING__WORKER_LOG_SERVER_PORT="${AIRFLOW__LOGGING__WORKER_LOG_SERVER_PORT:-$((NGT_AIRFLOW_PORT + 1000))}"
export AIRFLOW__LOGGING__TRIGGER_LOG_SERVER_PORT="${AIRFLOW__LOGGING__TRIGGER_LOG_SERVER_PORT:-$((NGT_AIRFLOW_PORT + 2000))}"
export AIRFLOW__CORE__EXECUTION_API_SERVER_URL="${AIRFLOW__CORE__EXECUTION_API_SERVER_URL:-http://localhost:$NGT_AIRFLOW_PORT/execution/}"

# How long a file watcher keeps waiting after a run ends. Production's default is 30 min; 20 s is
# comfortably above NGT_FILE_POLL_SECONDS (5 s), so a scenario's end-run resolves in seconds.
export NGT_RUN_END_GRACE_SECONDS="${NGT_RUN_END_GRACE_SECONDS:-20}"

# $REPO_DIR for ngt_calibration_loop; airflow_automation/ so the DAG files can `import airflow_dags`,
# a name also baked into the triggers' serialized classpaths in Airflow's DB.
export PYTHONPATH="$REPO_DIR:$REPO_DIR/airflow_automation:${PYTHONPATH:-}"
