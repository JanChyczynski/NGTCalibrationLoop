"""
Tests for scenario-player/conddb.py and the ledger/provenance formats it
re-exports from scenario-player/bin/_fakeprov.py -- the dummy conditions DB a
scenario's expectations are checked against.

The ledger's writer side is POSIX-only (fcntl.flock, same constraint as
scenario-player/faults.py's locked_json_file), so the tests that append are
skipped off POSIX rather than silently exercising a different code path.
"""
import json
import os
from concurrent.futures import ThreadPoolExecutor

import pytest

import conddb  # scenario-player/conddb.py, on sys.path via tests/conftest.py

posix_only = pytest.mark.skipif(
    os.name != "posix", reason="the ledger writer uses fcntl.flock (POSIX-only, see faults.py)"
)


@pytest.fixture
def ledger(tmp_path, monkeypatch):
    """Point the ledger at a tmp dir, the way the live setup points it at
    $NGT_DEV_HOME/conddb/."""
    monkeypatch.setenv("NGT_DEV_HOME", str(tmp_path))
    monkeypatch.delenv("NGT_CONDDB_DIR", raising=False)
    return tmp_path / conddb.CONDDB_DIR_NAME / conddb.LEDGER_NAME


# --- lumisection range parsing -------------------------------------------------


@pytest.mark.parametrize(
    "spec,expected",
    [
        (51, {51}),
        ("51", {51}),
        ("51-53", {51, 52, 53}),
        ("51-53,57", {51, 52, 53, 57}),
        ("51 - 53", {51, 52, 53}),
        ([51, 52, 53], {51, 52, 53}),
        ([51, "60-62"], {51, 60, 61, 62}),
        ("70-70", {70}),
    ],
)
def test_parse_ls_spec_accepts_every_documented_spelling(spec, expected):
    assert conddb.parse_ls_spec(spec) == expected


@pytest.mark.parametrize("spec", [None, "", "abc", "53-51", "51-", "-51", True, 1.5, {}])
def test_parse_ls_spec_rejects_nonsense(spec):
    with pytest.raises(conddb.LumisectionSpecError):
        conddb.parse_ls_spec(spec)


@pytest.mark.parametrize(
    "lumisections,expected",
    [
        ({51, 52, 53}, "51-53"),
        ({51, 52, 53, 57}, "51-53,57"),
        ({51}, "51"),
        (set(), "(none)"),
        ({51, 53}, "51,53"),
    ],
)
def test_format_ls_set_collapses_consecutive_runs(lumisections, expected):
    assert conddb.format_ls_set(lumisections) == expected


def test_format_ls_set_round_trips_through_parse_ls_spec():
    original = {51, 52, 53, 57, 58, 90}
    assert conddb.parse_ls_spec(conddb.format_ls_set(original)) == original


# --- the ledger ----------------------------------------------------------------


def test_read_ledger_of_a_missing_file_is_empty(ledger):
    assert conddb.read_ledger() == []


@posix_only
def test_append_payload_assigns_increasing_seq_and_a_timestamp(ledger):
    first = conddb.append_payload({"calibration": "EcalPedestals", "run": 398600, "lumisections": [51]})
    second = conddb.append_payload({"calibration": "EcalPedestals", "run": 398600, "lumisections": [51, 52]})

    assert (first["seq"], second["seq"]) == (1, 2)
    assert first["uploaded_at"] and second["uploaded_at"]
    assert [row["seq"] for row in conddb.read_ledger()] == [1, 2]


@posix_only
def test_append_payload_writes_one_json_object_per_line_with_seq_first(ledger):
    conddb.append_payload({"calibration": "BeamSpot", "run": 1, "lumisections": [1]})
    conddb.append_payload({"calibration": "BeamSpot", "run": 2, "lumisections": [2]})

    lines = ledger.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 2
    assert all(json.loads(line) for line in lines)
    # seq first keeps a hand-read of the file scanning in upload order.
    assert lines[0].startswith('{"seq": 1')


@posix_only
def test_append_payload_is_safe_from_several_processes_worth_of_writers(ledger):
    """Several calibrations upload concurrently in
    scenario-player/scenarios/concurrent_multi_calibration.yaml, and in the
    live demo each upload is a separate OS process -- every row must survive
    and every seq must be unique."""
    with ThreadPoolExecutor(max_workers=8) as pool:
        list(
            pool.map(
                lambda i: conddb.append_payload(
                    {"calibration": "EcalPedestals", "run": 398600, "lumisections": [i]}
                ),
                range(24),
            )
        )

    rows = conddb.read_ledger()
    assert len(rows) == 24
    assert sorted(row["seq"] for row in rows) == list(range(1, 25))


@posix_only
def test_read_ledger_skips_a_torn_trailing_line(ledger):
    """The ledger is read while uploads may still be landing, so a
    half-written final line must not blow up the whole check."""
    conddb.append_payload({"calibration": "EcalPedestals", "run": 398600, "lumisections": [51]})
    with ledger.open("a", encoding="utf-8") as f:
        f.write('{"seq": 2, "calibration": "Ecal')

    rows = conddb.read_ledger()
    assert [row["seq"] for row in rows] == [1]


@posix_only
def test_clear_ledger_removes_the_ledger_and_its_lock(ledger):
    conddb.append_payload({"calibration": "EcalPedestals", "run": 398600, "lumisections": [51]})
    assert ledger.exists()

    conddb.clear_ledger()

    assert not ledger.exists()
    assert not ledger.with_name(ledger.name + ".lock").exists()
    assert conddb.read_ledger() == []


def test_ngt_conddb_dir_overrides_ngt_dev_home(tmp_path, monkeypatch):
    monkeypatch.setenv("NGT_DEV_HOME", str(tmp_path / "devhome"))
    monkeypatch.setenv("NGT_CONDDB_DIR", str(tmp_path / "elsewhere"))
    assert conddb.ledger_path() == str(tmp_path / "elsewhere" / conddb.LEDGER_NAME)


# --- provenance manifests ------------------------------------------------------


def test_write_then_read_provenance_round_trips(tmp_path):
    path = tmp_path / "PromptCalibProdEcalPedestals.root"
    conddb.write_provenance(
        path,
        step="step3",
        calibration="EcalPedestals",
        run="398600",
        lumisections={52, 51},
        inputs=["/some/dir/run398600_LS0051To0052_ecalPedsStep2.root"],
    )

    manifest = conddb.read_provenance(path)
    assert manifest["step"] == "step3"
    assert manifest["calibration"] == "EcalPedestals"
    assert manifest["run"] == 398600  # normalised to an int even from a string
    assert manifest["lumisections"] == [51, 52]  # sorted
    assert manifest["inputs"] == ["run398600_LS0051To0052_ecalPedsStep2.root"]  # basenames only
    assert path.read_text(encoding="utf-8").startswith(conddb.PROVENANCE_MAGIC)


def test_read_provenance_of_a_file_that_is_not_ours_is_none(tmp_path):
    raw = tmp_path / "run398600_ls0051.root"
    raw.write_bytes(b"(FAKE) raw data placeholder\n")
    assert conddb.read_provenance(raw) is None
    assert conddb.read_provenance(tmp_path / "nope.root") is None
