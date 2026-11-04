"""
How airflow_demo.sh runs `airflow standalone`: what it starts with, that it's detached, and that
start, status and stop go by what's really alive. These run the real script against a fake venv
whose `airflow` only records how it was started and then waits, with a worker of its own like the
real scheduler's -- no Airflow, no database.
"""

import http.server
import os
import re
import shutil
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

pytestmark = pytest.mark.skipif(
    sys.platform == "win32" or any(shutil.which(tool) is None for tool in ("bash", "setsid", "pgrep", "pkill", "ps", "curl")),
    reason="needs bash, setsid, pgrep, pkill, ps and curl",
)

REPO_ROOT = Path(__file__).resolve().parents[2]
AIRFLOW_DEMO = REPO_ROOT / "airflow_automation" / "airflow_demo" / "airflow_demo.sh"
AIRFLOW_ENV = REPO_ROOT / "airflow_automation" / "airflow_demo" / "airflow_env.sh"
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
    # The worker is named to be recognized as Airflow's, and with STUBBORN_WORKER it ignores TERM, as
    # the real scheduler's do once the scheduler is gone.
    fake_airflow.write_text(
        f"""#!/bin/bash
[ "$1" = standalone ] || exit 0
env > "{out}/standalone.env"
(
  [ -z "${{STUBBORN_WORKER:-}}" ] || trap "" TERM
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

    def status(env=env, **extra):
        return run("status", env=env, **extra).stdout

    def pid(env=env):
        return int(re.search(r"pid (\d+)", status(env=env)).group(1))

    def resolved():
        """PER_CHECKOUT as airflow_env.sh itself resolves them, in a clean shell with the same env."""
        script = f'source "{AIRFLOW_ENV}" > /dev/null && ' + " && ".join(
            f'echo "{v}=${{{v}}}"' for v in (*PER_CHECKOUT, "NGT_AIRFLOW_PORT")
        )
        result = subprocess.run(["bash", "-c", script], env=env, capture_output=True, text=True, check=False)
        assert result.returncode == 0, result.stderr
        return dict(line.split("=", 1) for line in result.stdout.splitlines())

    def recorded_env():
        path = out / "standalone.env"
        wait_until(lambda: path.exists() and "PATH=" in path.read_text(), "standalone starting")
        return dict(line.split("=", 1) for line in path.read_text().splitlines() if "=" in line)

    try:
        yield SimpleNamespace(run=run, status=status, pid=pid, resolved=resolved, recorded_env=recorded_env, env=env, other=other)
    finally:
        run("stop", NGT_AIRFLOW_STOP_SECONDS="2")
        run("stop", env=other, NGT_AIRFLOW_STOP_SECONDS="2")


def test_standalone_starts_detached_with_this_checkouts_settings(demo):
    started = demo.run("start")
    assert started.returncode == 0, started.stdout + started.stderr

    resolved = demo.resolved()
    got = demo.recorded_env()
    assert {v: got.get(v) for v in PER_CHECKOUT} == {v: resolved[v] for v in PER_CHECKOUT}
    assert got["AIRFLOW__API__PORT"] == resolved["NGT_AIRFLOW_PORT"]
    assert got["NGT_ONLY_ENV_SH_SETS_THIS"] == "1"
    assert got["NGT_RUN_END_GRACE_SECONDS"] == "20"

    # A session of its own is what outlives the terminal, and what stop goes by.
    pid = demo.pid()
    assert subprocess.run(["ps", "-o", "sid=", "-p", str(pid)], capture_output=True, text=True).stdout.strip() == str(pid)


def test_start_twice_starts_nothing_new(demo):
    demo.run("start")
    before = demo.pid()

    again = demo.run("start")

    assert again.returncode == 0, again.stderr
    assert demo.pid() == before


def test_stop_takes_the_whole_session_with_it(demo):
    # Airflow's own workers survive standalone on a TERM, and a dead scheduler's ignore one.
    demo.run("start", STUBBORN_WORKER="1")
    session = demo.pid()
    assert len(session_members(session)) >= 2

    stopped = demo.run("stop", NGT_AIRFLOW_STOP_SECONDS="2")

    assert stopped.returncode == 0, stopped.stderr
    assert session_members(session) == []
    assert "isn't running" in demo.status()
    assert demo.run("stop").returncode == 0


def test_start_clears_what_a_dead_standalone_left_behind(demo):
    demo.run("start", STUBBORN_WORKER="1")
    session = demo.pid()
    os.kill(session, signal.SIGKILL)
    wait_until(lambda: "has died" in demo.status(), "status noticing it died")
    assert session_members(session)

    restarted = demo.run("start", NGT_AIRFLOW_STOP_SECONDS="2")

    assert restarted.returncode == 0, restarted.stderr
    assert session_members(session) == []
    assert demo.pid() != session


def test_status_shows_the_components_health_since_standalone_outlives_them(demo):
    class Health(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b'{"scheduler": {"status": "unhealthy"}}')

        def log_message(self, *args):
            pass

    server = http.server.HTTPServer(("127.0.0.1", 0), Health)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        port = str(server.server_address[1])
        demo.run("start", NGT_AIRFLOW_PORT=port)
        assert "unhealthy" in demo.status(NGT_AIRFLOW_PORT=port)
    finally:
        server.shutdown()
        server.server_close()
    assert "isn't answering" in demo.status(NGT_AIRFLOW_PORT=port)


def test_a_pid_file_outliving_a_reboot_leaves_alone_whatever_has_that_number_now(demo):
    unrelated = subprocess.Popen(["sleep", "300"], start_new_session=True)
    try:
        home = Path(demo.env["AIRFLOW_HOME"])
        home.mkdir()
        (home / "airflow_demo.pid").write_text(f"{unrelated.pid}\n")

        assert "has died" in demo.status()
        assert demo.run("stop").returncode == 0
        assert unrelated.poll() is None
        assert "isn't running" in demo.status()
    finally:
        unrelated.kill()
        unrelated.wait()


def test_another_instances_airflow_is_left_alone(demo):
    demo.run("start")
    demo.run("start", env=demo.other)
    other = demo.pid(env=demo.other)
    assert other != demo.pid()

    demo.run("stop")

    assert "isn't running" in demo.status()
    assert demo.pid(env=demo.other) == other


def test_reset_airflow_restarts_it_only_if_it_was_running(demo):
    assert demo.run("reset-airflow").returncode == 0
    assert "isn't running" in demo.status()

    demo.run("start")
    before = demo.pid()

    assert demo.run("reset-airflow").returncode == 0

    assert demo.pid() != before


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
