"""
Tests that identifiers really do propagate through the fake toolchain, by
invoking scenario-player/bin/{cmsDriver.py,cmsRun,uploadConditions.py} as
actual subprocesses rather than importing them.

That matters because the thing under test *is* the subprocess contract: the
step scripts shell out to these by a bare $PATH lookup, communicate through
the generated python config's `# FAKE_INPUTS=`/`# FAKE_OUTPUTS=` markers and
the cwd-derived calibration/run/step scope, and nothing in between is a
Python call this suite could stub. The chain is laid out exactly as
ngt_calibration_loop/step{2,3,4}.py lay it out:

    <data>/<calibration>/run<N>/                      step2 (raw EOS -> .root)
    <data>/<calibration>/run<N>/alcaPromptJob_*/      step3 (-> ALCARECO .root)
    <data>/<calibration>/run<N>/harvestJob_*/         step4 (-> .db, then upload)

If a DAG ever hands a harvest the wrong input files, that is what these
assertions catch -- the payload's lumisections are derived from the inputs
that were actually passed, never from the scenario.
"""
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

import conddb  # scenario-player/conddb.py, on sys.path via tests/conftest.py

REPO_ROOT = Path(__file__).resolve().parent.parent
FAKE_BIN = REPO_ROOT / "scenario-player" / "bin"

pytestmark = pytest.mark.skipif(
    os.name != "posix",
    reason="the fake toolchain is invoked via shebang/flock on Linux (see README's live-demo setup)",
)


@pytest.fixture
def chain(tmp_path, monkeypatch):
    """A $NGT_DEV_HOME with the step2 working dir laid out as step2.py does."""
    monkeypatch.setenv("NGT_DEV_HOME", str(tmp_path))
    monkeypatch.delenv("NGT_CONDDB_DIR", raising=False)
    monkeypatch.delenv("NGT_FAULTS_DIR", raising=False)
    run_dir = tmp_path / "data" / "EcalPedestals" / "run398600"
    run_dir.mkdir(parents=True)
    return run_dir


def _run(script, args, cwd):
    result = subprocess.run(
        [sys.executable, str(FAKE_BIN / script), *args],
        cwd=str(cwd),
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, f"{script} failed: {result.stdout}\n{result.stderr}"
    return result


def _cms_job(cwd, *, step_arg, inputs, python_filename, fileout=None):
    """One cmsDriver.py + cmsRun pair, the way every step's script does it."""
    args = ["expressStep", "-s", step_arg, "--filein", ",".join(inputs), "--python_filename", python_filename]
    if fileout:
        args += ["--fileout", f"file:{fileout}"]
    _run("cmsDriver.py", args, cwd)
    return _run("cmsRun", [python_filename], cwd)


def _seed_raw(run_dir, run, lumisections):
    """Raw "EOS" files named as scenario-player/seed.py's _touch_ls_file does."""
    paths = []
    for ls in lumisections:
        path = run_dir / f"run{run}_ls{ls:04d}.root"
        path.write_bytes(b"(FAKE) raw data placeholder\n")
        paths.append(f"root://eoscms.cern.ch/{path}")
    return paths


def _full_chain(run_dir, lumisections, run=398600):
    """Drive step2 -> step3 -> step4 -> upload and return the ledger rows."""
    raw = _seed_raw(run_dir, run, lumisections)
    step2_out = f"run{run}_LS{min(lumisections):04d}To{max(lumisections):04d}_ecalPedsStep2.root"
    _cms_job(run_dir, step_arg="RAW2DIGI,RECO", inputs=raw, python_filename="s2.py", fileout="tmp.root")
    (run_dir / "tmp.root").rename(run_dir / step2_out)

    alca_dir = run_dir / "alcaPromptJob_abc1234567"
    alca_dir.mkdir(exist_ok=True)
    _cms_job(
        alca_dir,
        step_arg="ALCAOUTPUT:EcalTestPulsesRaw,ALCA:PromptCalibProdEcalPedestals",
        inputs=[f"file:../{step2_out}"],
        python_filename="s3.py",
    )

    harvest_dir = run_dir / "harvestJob_def7654321"
    harvest_dir.mkdir(exist_ok=True)
    _cms_job(
        harvest_dir,
        step_arg="ALCAHARVEST:EcalPedestals",
        inputs=["file:../alcaPromptJob_abc1234567/PromptCalibProdEcalPedestals.root"],
        python_filename="s4.py",
    )
    (harvest_dir / "promptCalibConditions.db").rename(harvest_dir / "NGTCalibEcalPedestals.db")
    _run("uploadConditions.py", ["NGTCalibEcalPedestals.db"], harvest_dir)
    return conddb.read_ledger()


# --- the generated config -------------------------------------------------------


def test_cmsdriver_records_inputs_and_strips_both_filein_prefixes(chain):
    _run(
        "cmsDriver.py",
        [
            "expressStep2",
            "-s",
            "RAW2DIGI,RECO",
            "--filein",
            "root://eoscms.cern.ch//eos/a.root,file:/data/b.root",
            "--fileout",
            "file:out.root",
            "--python_filename",
            "cfg.py",
        ],
        chain,
    )

    config = (chain / "cfg.py").read_text(encoding="utf-8")
    assert "# FAKE_OUTPUTS=out.root" in config
    assert "# FAKE_INPUTS=/eos/a.root,/data/b.root" in config


def test_cmsdriver_still_names_the_fixed_harvest_output(chain):
    """CMSSW's own fixed ALCAHARVEST output name, which step4.py's generated
    script greps for by hand."""
    _run(
        "cmsDriver.py",
        ["expressStep4", "-s", "ALCAHARVEST:EcalPedestals", "--python_filename", "cfg.py"],
        chain,
    )
    assert "# FAKE_OUTPUTS=promptCalibConditions.db" in (chain / "cfg.py").read_text(encoding="utf-8")


# --- provenance propagation ------------------------------------------------------


def test_step2_derives_lumisections_from_the_raw_filenames(chain):
    raw = _seed_raw(chain, 398600, [51, 52])
    _cms_job(chain, step_arg="RAW2DIGI,RECO", inputs=raw, python_filename="s2.py", fileout="out.root")

    manifest = conddb.read_provenance(chain / "out.root")
    assert manifest["step"] == "step2"
    assert manifest["calibration"] == "EcalPedestals"
    assert manifest["run"] == 398600
    assert manifest["lumisections"] == [51, 52]


def test_lumisections_survive_all_three_steps(chain):
    rows = _full_chain(chain, [51, 52, 53])

    assert len(rows) == 1
    assert rows[0]["calibration"] == "EcalPedestals"
    assert rows[0]["run"] == 398600
    assert rows[0]["lumisections"] == [51, 52, 53]
    assert rows[0]["db_file"] == "NGTCalibEcalPedestals.db"
    assert rows[0]["seq"] == 1


def test_a_harvest_merges_the_lumisections_of_several_step3_outputs(chain):
    """The case that matters for a batching bug: two step3 outputs covering
    different lumisections must produce one payload covering the union."""
    first = chain / "alcaPromptJob_1111111111"
    second = chain / "alcaPromptJob_2222222222"
    for job_dir, lumisections in ((first, [51, 52]), (second, [53, 54])):
        job_dir.mkdir()
        raw = _seed_raw(chain, 398600, lumisections)
        _cms_job(job_dir, step_arg="ALCA:PromptCalibProdEcalPedestals", inputs=raw, python_filename="s3.py")

    harvest = chain / "harvestJob_abcdefabcd"
    harvest.mkdir()
    _cms_job(
        harvest,
        step_arg="ALCAHARVEST:EcalPedestals",
        inputs=[
            "file:../alcaPromptJob_1111111111/PromptCalibProdEcalPedestals.root",
            "file:../alcaPromptJob_2222222222/PromptCalibProdEcalPedestals.root",
        ],
        python_filename="s4.py",
    )

    manifest = conddb.read_provenance(harvest / "promptCalibConditions.db")
    assert manifest["lumisections"] == [51, 52, 53, 54]
    assert len(manifest["inputs"]) == 2


def test_a_harvest_fed_the_wrong_inputs_produces_a_wrong_payload(chain):
    """The whole point of deriving provenance from actual inputs: if only one
    of the two available step3 outputs reaches the harvest, the payload is
    visibly short rather than looking correct."""
    for job_dir, lumisections in ((chain / "alcaPromptJob_1111111111", [51, 52]),
                                  (chain / "alcaPromptJob_2222222222", [53, 54])):
        job_dir.mkdir()
        raw = _seed_raw(chain, 398600, lumisections)
        _cms_job(job_dir, step_arg="ALCA:PromptCalibProdEcalPedestals", inputs=raw, python_filename="s3.py")

    harvest = chain / "harvestJob_abcdefabcd"
    harvest.mkdir()
    _cms_job(
        harvest,
        step_arg="ALCAHARVEST:EcalPedestals",
        inputs=["file:../alcaPromptJob_1111111111/PromptCalibProdEcalPedestals.root"],
        python_filename="s4.py",
    )

    manifest = conddb.read_provenance(harvest / "promptCalibConditions.db")
    assert manifest["lumisections"] == [51, 52]  # 53/54 silently dropped -- exactly what expect: catches


# --- the upload ------------------------------------------------------------------


def test_upload_appends_one_row_per_successful_upload(chain):
    rows = _full_chain(chain, [51])
    harvest = chain / "harvestJob_def7654321"

    _run("uploadConditions.py", ["NGTCalibEcalPedestals.db"], harvest)
    rows = conddb.read_ledger()

    assert [row["seq"] for row in rows] == [1, 2]
    assert all(row["lumisections"] == [51] for row in rows)


def test_upload_does_not_read_the_metadata_txt_step4_writes(chain):
    """Deliberately out of scope -- the ledger records only what the pipeline
    processed, so a metadata file naming a different run must not leak in."""
    rows = _full_chain(chain, [51, 52])
    harvest = chain / "harvestJob_def7654321"
    (harvest / "NGTCalibEcalPedestals.txt").write_text(
        json.dumps({"since": 999999, "destinationTags": {"SomeOtherTag": {}}}), encoding="utf-8"
    )

    _run("uploadConditions.py", ["NGTCalibEcalPedestals.db"], harvest)
    latest = conddb.read_ledger()[-1]

    assert latest["run"] == 398600
    assert "since" not in latest
    assert "tag" not in latest


def test_upload_of_a_db_without_provenance_records_an_empty_payload(chain):
    """A loud, failing-but-explicable row beats either crashing or silently
    recording nothing (which would read as "the pipeline never uploaded")."""
    harvest = chain / "harvestJob_0000000000"
    harvest.mkdir()
    (harvest / "NGTCalibEcalPedestals.db").write_bytes(b"not one of ours\n")

    result = _run("uploadConditions.py", ["NGTCalibEcalPedestals.db"], harvest)

    assert "WARNING" in result.stderr
    assert conddb.read_ledger()[0]["lumisections"] == []


def test_upload_writes_no_row_when_a_fault_is_armed(chain, tmp_path):
    """"Nothing should reach the conditions DB" is only assertable if an
    injected upload failure leaves the ledger untouched."""
    faults_dir = tmp_path / "faults"
    faults_dir.mkdir()
    (faults_dir / "upload_conditions.json").write_text(
        json.dumps([{"mode": "exit_code", "exit_code": 1, "calibration": "EcalPedestals", "times_remaining": 1}]),
        encoding="utf-8",
    )

    rows_before = _full_chain(chain, [51])
    assert len(rows_before) == 1

    harvest = chain / "harvestJob_def7654321"
    env = dict(os.environ, NGT_FAULTS_DIR=str(faults_dir), NGT_DEV_HOME=str(tmp_path))
    result = subprocess.run(
        [sys.executable, str(FAKE_BIN / "uploadConditions.py"), "NGTCalibEcalPedestals.db"],
        cwd=str(harvest),
        capture_output=True,
        text=True,
        check=False,
        env=env,
    )

    assert result.returncode == 1
    assert "ERROR" in result.stderr
    assert len(conddb.read_ledger()) == 1  # unchanged
