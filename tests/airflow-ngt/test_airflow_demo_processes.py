"""
How airflow_demo.sh runs Airflow's four processes: what they start with, that they're detached, and
that start, status and stop go by what's really alive. These run the real script against a fake
venv whose `airflow` only records how it was started and then waits, with a worker of its own like
the real scheduler's -- no Airflow, no database.
"""

import os
import re
import shutil
import signal
import subprocess
import sys
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

pytestmark = pytest.mark.skipif(
    sys.platform == "win32" or any(shutil.which(tool) is None for tool in ("bash", "setsid", "pgrep", "pkill", "ps")),
    reason="needs bash, setsid, pgrep, pkill and ps",
)

REPO_ROOT = Path(__file__).resolve().parents[2]
AIRFLOW_DEMO = REPO_ROOT / "airflow_automation" / "airflow_demo" / "airflow_demo.sh"
AIRFLOW_ENV = REPO_ROOT / "airflow_automation" / "airflow_demo" / "airflow_env.sh"
COMPONENTS = ("scheduler", "dag-processor", "api", "triggerer")
# Settings airflow_env.sh resolves per checkout, i.e. what must never come from somewhere else.
PER_CHECKOUT = (
    "NGT_INSTANCE",
    "NGT_DEV_HOME",
    "AIRFLOW_HOME",
    "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN",
    "AIRFLOW__CORE__DAGS_FOLDER",
    "AIRFLOW__CORE__EXECUTION_API_SERVER_URL",
)


def session_members(session):
    return subprocess.run(["pgrep", "-s", str(session)], capture_output=True, text=True, check=False).stdout.split()


def wait_until(condition, what, timeout=20):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition():
            return
        time.sleep(0.2)
    pytest.fail(f"never happened: {what}")


@pytest.fixture
def demo(tmp_path):
    out = tmp_path / "recorded"
    out.mkdir()
    venv = tmp_path / "venv"
    (venv / "bin").mkdir(parents=True)
    (venv / "bin" / "activate").write_text(f'export VIRTUAL_ENV="{venv}"\nexport PATH="{venv}/bin:$PATH"\n')
    fake_airflow = venv / "bin" / "airflow"
    # The worker is named to be recognized as Airflow's, and with STUBBORN_WORKER=<command> it ignores
    # TERM, as the real scheduler's do once the scheduler is gone.
    fake_airflow.write_text(
        f"""#!/bin/bash
case "$1" in
  scheduler|dag-processor|api-server|triggerer) ;;
  *) exit 0 ;;
esac
env > "{out}/$1.env"
echo "$*" > "{out}/$1.args"
(
  [ "$1" != "${{STUBBORN_WORKER:-}}" ] || trap "" TERM
  exec -a airflow-worker sleep 300
) &
wait
"""
    )
    fake_airflow.chmod(0o755)

    def instance(name):
        dev_home = tmp_path / name / "demo-env"
        dev_home.mkdir(parents=True)
        (dev_home / "env.sh").write_text(f'export NGT_DEV_HOME="{dev_home}"\nexport NGT_ONLY_ENV_SH_SETS_THIS=1\n')
        return {
            "PATH": os.environ["PATH"],
            "HOME": os.environ.get("HOME", str(tmp_path)),
            "NGT_SKIP_LOCAL_ENV": "1",
            "NGT_AIRFLOW_VENV": str(venv),
            "NGT_DEV_HOME": str(dev_home),
            "AIRFLOW_HOME": str(tmp_path / name / "airflow-home"),
        }

    env = instance("one")
    other = instance("other")

    def run(*args, env=env, **extra):
        return subprocess.run(
            ["bash", str(AIRFLOW_DEMO), *args], env=dict(env, **extra), capture_output=True, text=True, check=False, timeout=60
        )

    def status(env=env):
        return dict(line.split(": ", 1) for line in run("status", env=env).stdout.splitlines())

    def pid(label, env=env):
        return int(re.search(r"pid (\d+)", status(env=env)[label]).group(1))

    def resolved():
        """PER_CHECKOUT as airflow_env.sh itself resolves them, in a clean shell with the same env."""
        script = f'source "{AIRFLOW_ENV}" > /dev/null && ' + " && ".join(
            f'echo "{v}=${{{v}}}"' for v in (*PER_CHECKOUT, "NGT_AIRFLOW_PORT")
        )
        result = subprocess.run(["bash", "-c", script], env=env, capture_output=True, text=True, check=False)
        assert result.returncode == 0, result.stderr
        return dict(line.split("=", 1) for line in result.stdout.splitlines())

    def recorded(command, suffix):
        path = out / f"{command}.{suffix}"
        wait_until(lambda: path.exists() and path.read_text().strip(), f"{command} starting")
        return path.read_text()

    try:
        yield SimpleNamespace(run=run, status=status, pid=pid, resolved=resolved, recorded=recorded, env=env, other=other)
    finally:
        run("stop")
        run("stop", env=other)


def test_each_component_starts_detached_with_this_checkouts_settings(demo):
    started = demo.run("start")
    assert started.returncode == 0, started.stdout + started.stderr

    resolved = demo.resolved()
    for command in ("scheduler", "dag-processor", "api-server", "triggerer"):
        got = dict(line.split("=", 1) for line in demo.recorded(command, "env").splitlines() if "=" in line)
        assert {v: got.get(v) for v in PER_CHECKOUT} == {v: resolved[v] for v in PER_CHECKOUT}, command
        assert got["NGT_ONLY_ENV_SH_SETS_THIS"] == "1", command
        assert got["NGT_RUN_END_GRACE_SECONDS"] == "20", command
    assert demo.recorded("api-server", "args").split() == ["api-server", "-p", resolved["NGT_AIRFLOW_PORT"]]

    # A session of its own is what outlives the terminal, and what stop goes by.
    for label in COMPONENTS:
        pid = demo.pid(label)
        assert subprocess.run(["ps", "-o", "sid=", "-p", str(pid)], capture_output=True, text=True).stdout.strip() == str(pid)


def test_start_twice_starts_nothing_new(demo):
    demo.run("start")
    before = {label: demo.pid(label) for label in COMPONENTS}

    again = demo.run("start")

    assert again.returncode == 0, again.stderr
    assert {label: demo.pid(label) for label in COMPONENTS} == before


def test_a_dead_component_shows_as_dead_and_start_restarts_only_it(demo):
    demo.run("start")
    before = {label: demo.pid(label) for label in COMPONENTS}

    os.kill(before["triggerer"], signal.SIGKILL)

    wait_until(lambda: demo.status()["triggerer"].startswith("DIED"), "status noticing the dead triggerer")
    assert all(demo.status()[label].startswith("running") for label in COMPONENTS if label != "triggerer")

    restarted = demo.run("start")

    assert restarted.returncode == 0, restarted.stderr
    assert "triggerer had died" in restarted.stdout
    after = {label: demo.pid(label) for label in COMPONENTS}
    assert after["triggerer"] != before["triggerer"]
    assert {k: v for k, v in after.items() if k != "triggerer"} == {k: v for k, v in before.items() if k != "triggerer"}


def test_restarting_a_dead_scheduler_clears_the_workers_it_left_behind(demo):
    # They would ignore a TERM here, and hold on to the log server's port for good.
    demo.run("start", STUBBORN_WORKER="scheduler")
    scheduler = demo.pid("scheduler")
    os.kill(scheduler, signal.SIGKILL)
    wait_until(lambda: demo.status()["scheduler"].startswith("DIED"), "status noticing the dead scheduler")
    assert session_members(scheduler)

    demo.run("start")

    wait_until(lambda: not session_members(scheduler), "the dead scheduler's workers going")
    assert demo.status()["scheduler"].startswith("running")


def test_stop_takes_each_components_whole_session_with_it(demo):
    demo.run("start", STUBBORN_WORKER="scheduler")
    sessions = [demo.pid(label) for label in COMPONENTS]
    assert all(len(session_members(session)) >= 2 for session in sessions)

    stopped = demo.run("stop", NGT_AIRFLOW_STOP_SECONDS="2")

    assert stopped.returncode == 0, stopped.stderr
    assert [session_members(session) for session in sessions] == [[], [], [], []]
    assert set(demo.status().values()) == {"stopped"}
    assert demo.run("stop").returncode == 0


def test_stop_one_component_leaves_the_others(demo):
    demo.run("start")
    before = {label: demo.pid(label) for label in COMPONENTS}

    assert demo.run("stop", "triggerer").returncode == 0

    assert demo.status()["triggerer"] == "stopped"
    assert {label: demo.pid(label) for label in COMPONENTS if label != "triggerer"} == {
        k: v for k, v in before.items() if k != "triggerer"
    }
    demo.run("start")
    assert demo.status()["triggerer"].startswith("running")
    assert demo.run("stop", "no-such-component").returncode == 1


def test_a_pid_file_outliving_a_reboot_leaves_alone_whatever_has_that_number_now(demo):
    unrelated = subprocess.Popen(["sleep", "300"], start_new_session=True)
    try:
        home = Path(demo.env["AIRFLOW_HOME"])
        home.mkdir()
        for label in COMPONENTS:
            (home / f"airflow_demo_{label}.pid").write_text(f"{unrelated.pid}\n")

        assert all(state.startswith("DIED") for state in demo.status().values())
        assert demo.run("stop").returncode == 0
        assert unrelated.poll() is None
        assert set(demo.status().values()) == {"stopped"}
    finally:
        unrelated.kill()
        unrelated.wait()


def test_another_instances_processes_are_left_alone(demo):
    demo.run("start")
    demo.run("start", env=demo.other)
    others = {label: demo.pid(label, env=demo.other) for label in COMPONENTS}
    assert not set(others.values()) & {demo.pid(label) for label in COMPONENTS}

    demo.run("stop")

    assert set(demo.status().values()) == {"stopped"}
    assert {label: demo.pid(label, env=demo.other) for label in COMPONENTS} == others


def test_reset_airflow_restarts_what_was_running_and_only_that(demo):
    assert demo.run("reset-airflow").returncode == 0
    assert set(demo.status().values()) == {"stopped"}

    demo.run("start")
    before = {label: demo.pid(label) for label in COMPONENTS}

    assert demo.run("reset-airflow").returncode == 0

    after = {label: demo.pid(label) for label in COMPONENTS}
    assert all(after[label] != before[label] for label in COMPONENTS)


def test_two_dev_homes_of_one_checkout_get_separate_airflow_instances(tmp_path):
    """Airflow home and database follow NGT_INSTANCE, which follows NGT_DEV_HOME."""
    names = ("AIRFLOW_HOME", "NGT_AIRFLOW_DB_NAME")

    def resolved(home):
        script = f'source "{AIRFLOW_ENV}" > /dev/null && ' + " && ".join(f'echo "${{{v}}}"' for v in names)
        env = {"PATH": os.environ["PATH"], "HOME": str(tmp_path), "NGT_SKIP_LOCAL_ENV": "1", "NGT_DEV_HOME": str(home)}
        return subprocess.run(["bash", "-c", script], env=env, capture_output=True, text=True, check=True).stdout.split()

    first, second = resolved(tmp_path / "a"), resolved(tmp_path / "b")
    assert resolved(tmp_path / "a") == first
    assert all(one != other for one, other in zip(first, second))
