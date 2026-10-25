"""live_demo.sh only touches its own NGT_INSTANCE's tmux sessions (checked against a fake tmux),
and the default NGT_INSTANCE follows the checkout and a non-default NGT_DEV_HOME."""

import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

LIVE_DEMO = Path(__file__).resolve().parents[2] / "scenario-player" / "live_demo.sh"

pytestmark = pytest.mark.skipif(sys.platform == "win32", reason="needs a POSIX bash")

FAKE_TMUX = """#!/bin/bash
S="$FAKE_TMUX_SESSIONS"
case "$1" in
  list-sessions) cat "$S" ;;
  has-session) grep -qx -- "${3#=}" "$S" ;;
  kill-session) grep -vx -- "${3#=}" "$S" > "$S.new"; mv "$S.new" "$S" ;;
esac
"""


def test_stop_all_kills_only_this_instances_sessions(tmp_path):
    (tmp_path / "tmux").write_text(FAKE_TMUX, encoding="utf-8")
    (tmp_path / "tmux").chmod(0o755)
    sessions = tmp_path / "sessions"
    sessions.write_text("NGTDemo_a_2_EcalPedestals\nNGTDemo_b_2_EcalPedestals\n", encoding="utf-8")
    env = {k: v for k, v in os.environ.items() if not k.startswith("TMUX")}
    env.update(PATH=f"{tmp_path}:{env['PATH']}", FAKE_TMUX_SESSIONS=str(sessions), NGT_INSTANCE="a")

    subprocess.run(["bash", str(LIVE_DEMO), "stop-all", "EcalPedestals"], env=env, check=True)

    assert sessions.read_text(encoding="utf-8").split() == ["NGTDemo_b_2_EcalPedestals"]


def _instance(checkout, **env):
    """NGT_INSTANCE as a clean bash resolves it from a copy of sim_env.sh in a scratch `checkout`."""
    sim_env = Path(checkout) / "scenario-player" / "sim_env.sh"
    sim_env.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy(LIVE_DEMO.with_name("sim_env.sh"), sim_env)
    result = subprocess.run(
        ["env", "-i", "PATH=/usr/bin:/bin", *(f"{k}={v}" for k, v in env.items()), "bash", "-c",
         f'source "{sim_env}" && echo "$NGT_INSTANCE"'],
        capture_output=True, text=True, check=True,
    )
    return result.stdout.strip()


def test_the_instance_follows_the_checkout_and_a_non_default_dev_home(tmp_path):
    checkout = tmp_path / "checkout"
    default = _instance(checkout)
    assert _instance(checkout, NGT_DEV_HOME=checkout / "demo-env") == default
    home_a = _instance(checkout, NGT_DEV_HOME=tmp_path / "a")
    assert _instance(checkout, NGT_DEV_HOME=f"{tmp_path / 'a'}/") == home_a
    others = {_instance(checkout, NGT_DEV_HOME=tmp_path / "b"), _instance(tmp_path / "other")}
    assert len({default, home_a} | others) == 4
    assert _instance(checkout, NGT_INSTANCE="mine") == "mine"
