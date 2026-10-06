"""
Tests that the fake cmsRun (scenario-player/bin/cmsRun) reads a fault's calibration/run/step scope
off the directory it runs in, for both job-dir namings: the content-hashed alcaPromptJob_<hash>/
harvestJob_<hash> and the numbered alcaPromptJob000/harvestJob000 that NGTLoopStep3/4.py use.
It runs the real script as a subprocess, since the scope comes from its cwd.
"""

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.skipif(sys.platform == "win32", reason="needs fcntl and a POSIX path layout")

CMSRUN = Path(__file__).resolve().parent.parent / "scenario-player" / "bin" / "cmsRun"
CALIBRATION = "EcalPedestals"


def _arm(faults_dir, **scope):
    faults_dir.mkdir(exist_ok=True)
    entry = {"mode": "exit_code", "exit_code": 139, "times_remaining": 1, **scope}
    (faults_dir / "cmsrun.json").write_text(json.dumps([entry]), encoding="utf-8")


def _cmsrun_in(job_dir, faults_dir):
    job_dir.mkdir(parents=True)
    return subprocess.run(
        [sys.executable, str(CMSRUN), "no_such_config.py"],
        cwd=str(job_dir),
        capture_output=True,
        text=True,
        check=False,
        env=dict(os.environ, NGT_FAULTS_DIR=str(faults_dir)),
    )


def _armed(faults_dir):
    return json.loads((faults_dir / "cmsrun.json").read_text(encoding="utf-8"))


@pytest.mark.parametrize(
    "job_dir_name, step",
    [
        (None, "step2"),  # cmsRun runs in the run directory itself
        ("alcaPromptJob_abc1234567", "step3"),
        ("alcaPromptJob000", "step3"),
        ("harvestJob_def7654321", "step4"),
        ("harvestJob001", "step4"),
    ],
)
def test_a_step_scoped_fault_fires_in_every_job_dir_naming(tmp_path, job_dir_name, step):
    faults_dir = tmp_path / "faults"
    _arm(faults_dir, calibration=CALIBRATION, run=398600, step=step)
    run_dir = tmp_path / "data" / CALIBRATION / "run398600"

    result = _cmsrun_in(run_dir / job_dir_name if job_dir_name else run_dir, faults_dir)

    assert result.returncode == 139
    assert "Begin Fatal Exception" in result.stderr
    assert f"run: 398600 calibration: {CALIBRATION}" in result.stderr
    assert _armed(faults_dir) == []  # consumed


@pytest.mark.parametrize("job_dir_name", ["alcaPromptJob000", "alcaPromptJob_abc1234567", "harvestJob_def7654321"])
def test_a_step_scoped_fault_leaves_other_steps_jobs_alone(tmp_path, job_dir_name):
    faults_dir = tmp_path / "faults"
    _arm(faults_dir, calibration=CALIBRATION, step="step4" if job_dir_name.startswith("alca") else "step3")
    run_dir = tmp_path / "data" / CALIBRATION / "run398600"

    result = _cmsrun_in(run_dir / job_dir_name, faults_dir)

    assert result.returncode == 1  # the unarmed fake just can't find its python config
    assert "Fatal Exception" not in result.stderr
    assert len(_armed(faults_dir)) == 1  # still armed


def test_a_fault_scoped_to_another_run_is_left_alone(tmp_path):
    faults_dir = tmp_path / "faults"
    _arm(faults_dir, calibration=CALIBRATION, run=398601)

    result = _cmsrun_in(tmp_path / "data" / CALIBRATION / "run398600" / "alcaPromptJob000", faults_dir)

    assert result.returncode == 1
    assert len(_armed(faults_dir)) == 1
