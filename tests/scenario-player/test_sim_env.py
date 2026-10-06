"""
Tests what sim_env.sh's sim_setup writes into $NGT_DEV_HOME/env.sh -- the one file every live
adapter (scenario-player/live_demo.sh's loops among them) sources for the fake environment. Like
tests/scenario-player/test_fake_toolchain_isolation.py these spawn a real bash, since what matters is what a
shell sees after sourcing it.
"""

import subprocess
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.skipif(sys.platform == "win32", reason="needs a POSIX bash")

REPO_ROOT = Path(__file__).resolve().parents[2]
SIM_ENV = REPO_ROOT / "scenario-player" / "sim_env.sh"


@pytest.fixture
def env_sh(tmp_path):
    """Run sim_setup into a scratch $NGT_DEV_HOME, with a no-op `pip` first on $PATH so its
    `pip install -e tests/stubs` step leaves the test's own venv alone."""
    home = tmp_path / "demo-env"
    fake_bin = tmp_path / "fake-pip"
    fake_bin.mkdir()
    pip = fake_bin / "pip"
    pip.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    pip.chmod(0o755)
    script = (
        f'export NGT_DEV_HOME="{home}" VIRTUAL_ENV="{tmp_path}" PATH="{fake_bin}:$PATH"\n'
        f'source "{SIM_ENV}" && sim_setup > /dev/null\n'
    )
    result = subprocess.run(["bash", "-c", script], capture_output=True, text=True, check=False)
    assert result.returncode == 0, result.stderr
    return home


def _sourced(env_sh, command):
    """Run `command` in a clean bash that has sourced only env.sh."""
    result = subprocess.run(
        ["env", "-i", f"PATH=/usr/bin:/bin:{Path(sys.executable).parent}", "bash", "-c",
         f'source "{env_sh / "env.sh"}" && {command}'],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    return result.stdout.strip()


def test_env_sh_points_the_fakes_at_the_armed_faults(env_sh):
    # Without these, bin/cmsRun, bin/uploadConditions.py and the fake omsapi never look for armed
    # faults, so a scenario's faults: entries would be silently inert for live_demo.sh's loops.
    assert _sourced(env_sh, 'echo "$NGT_FAULTS_DIR"') == str(env_sh / "faults")
    assert _sourced(env_sh, 'echo "$NGT_OMS_STUB_FAULTS_FILE"') == str(env_sh / "faults" / "oms.json")


def test_env_sh_makes_the_fake_omsapis_fault_path_importable(env_sh):
    """The fake omsapi's live fault path imports scenario-player/faults.py, which is not an
    installed package -- env.sh's PYTHONPATH is what lets a loop's own Python find it."""
    found = _sourced(env_sh, f'"{sys.executable}" -c "import faults, omsapi; print(faults.__file__)"')
    assert Path(found).resolve() == (REPO_ROOT / "scenario-player" / "faults.py").resolve()
