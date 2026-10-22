"""live_demo.sh only touches its own NGT_INSTANCE's tmux sessions (checked against a fake tmux)."""

import os
import subprocess
import sys
from pathlib import Path

import pytest

LIVE_DEMO = Path(__file__).resolve().parents[2] / "scenario-player" / "live_demo.sh"

FAKE_TMUX = """#!/bin/bash
S="$FAKE_TMUX_SESSIONS"
case "$1" in
  list-sessions) cat "$S" ;;
  has-session) grep -qx -- "${3#=}" "$S" ;;
  kill-session) grep -vx -- "${3#=}" "$S" > "$S.new"; mv "$S.new" "$S" ;;
esac
"""


@pytest.mark.skipif(sys.platform == "win32", reason="needs a POSIX bash")
def test_stop_all_kills_only_this_instances_sessions(tmp_path):
    (tmp_path / "tmux").write_text(FAKE_TMUX, encoding="utf-8")
    (tmp_path / "tmux").chmod(0o755)
    sessions = tmp_path / "sessions"
    sessions.write_text("NGTDemo_a_2_EcalPedestals\nNGTDemo_b_2_EcalPedestals\n", encoding="utf-8")
    env = {k: v for k, v in os.environ.items() if not k.startswith("TMUX")}
    env.update(PATH=f"{tmp_path}:{env['PATH']}", FAKE_TMUX_SESSIONS=str(sessions), NGT_INSTANCE="a")

    subprocess.run(["bash", str(LIVE_DEMO), "stop-all", "EcalPedestals"], env=env, check=True)

    assert sessions.read_text(encoding="utf-8").split() == ["NGTDemo_b_2_EcalPedestals"]
