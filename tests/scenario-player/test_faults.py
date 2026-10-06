"""Tests for scenario-player/faults.py: FaultSpec validation and the fault-file helpers."""
import json

import pytest

import faults

CMSRUN = {"target": "cmsrun", "mode": "exit_code", "exit_code": 1, "calibration": "EcalPedestals"}
UPLOAD = {"target": "upload_conditions", "mode": "exit_code", "exit_code": 1, "calibration": "SiStripBad"}


def test_defaults_of_a_minimal_fault():
    spec = faults.parse_fault_spec({"target": "oms", "mode": "connection_error"})
    assert spec == faults.FaultSpec(target="oms", mode="connection_error", times=1)


@pytest.mark.parametrize(
    "raw, field, value",
    [
        ({"target": "oms", "mode": "http_error", "status": 503}, "status", 503),
        ({**CMSRUN, "exit_code": 139, "run": 398600, "step": "step2", "times": 2}, "step", "step2"),
        ({**CMSRUN, "times": 2}, "times", 2),
        (UPLOAD, "calibration", "SiStripBad"),
        ({"target": "oms", "mode": "timeout", "times": "unlimited"}, "times", None),
    ],
)
def test_accepts(raw, field, value):
    assert getattr(faults.parse_fault_spec(raw), field) == value


@pytest.mark.parametrize(
    "raw, match",
    [
        *[({"target": "oms", "mode": "timeout", "times": t}, "times") for t in (0, -1, 1.5, "soon", True)],
        ({"target": "eos", "mode": "exit_code"}, "target"),
        ({"target": "oms", "mode": "exit_code"}, "mode"),
        ({**CMSRUN, "mode": "timeout"}, "mode"),
        ({"target": "oms", "mode": "http_error"}, "status"),
        *[({"target": "oms", "mode": "http_error", "status": s}, "status") for s in (399, 600, "503", 503.0)],
        ({"target": "oms", "mode": "timeout", "status": 503}, "status"),
        ({k: v for k, v in CMSRUN.items() if k != "exit_code"}, "exit_code"),
        ({**CMSRUN, "exit_code": 0}, "exit_code"),
        ({"target": "oms", "mode": "timeout", "exit_code": 1}, "exit_code"),
        ({k: v for k, v in CMSRUN.items() if k != "calibration"}, "calibration"),
        ({k: v for k, v in UPLOAD.items() if k != "calibration"}, "calibration"),
        ({"target": "oms", "mode": "timeout", "calibration": "EcalPedestals"}, "calibration"),
        ({"target": "oms", "mode": "timeout", "step": "step2"}, "step"),
        ({**UPLOAD, "step": "step4"}, "step"),
        ({**CMSRUN, "step": "step5"}, "step"),
        ({"target": "oms", "mode": "timeout", "run": "398600"}, "run"),
        ({"target": "oms", "mode": "timeout", "bogus": 1}, "unknown"),
    ],
)
def test_rejects(raw, match):
    with pytest.raises(faults.FaultSpecError, match=match):
        faults.parse_fault_spec(raw)


# --- the fault files ---------------------------------------------------------------------


def test_fault_spec_to_entry_omits_target_and_unset_fields():
    spec = faults.parse_fault_spec({"target": "oms", "mode": "timeout", "run": 398600})
    assert faults.fault_spec_to_entry(spec) == {"mode": "timeout", "times_remaining": 1, "run": 398600}


def test_atomic_write_json_round_trips_and_leaves_no_temp_file(tmp_path):
    path = tmp_path / "cmsrun.json"
    faults.atomic_write_json(path, [{"a": 1}])
    assert json.loads(path.read_text(encoding="utf-8")) == [{"a": 1}]
    assert list(tmp_path.iterdir()) == [path]


def test_scope_matches():
    scope = {"calibration": "EcalPedestals", "run": 398600, "step": "step2"}
    assert faults.scope_matches({"mode": "exit_code"}, scope)  # unscoped
    assert faults.scope_matches({"calibration": "EcalPedestals", "run": 398600}, scope)
    assert not faults.scope_matches({"calibration": "SiStripBad"}, scope)
    assert not faults.scope_matches({"run": 500201}, scope)
    assert faults.scope_matches({"run": 398600}, {**scope, "run": "398600"})  # compared as strings


def test_consume_fault_missing_file_returns_none(tmp_path):
    assert faults.consume_fault(tmp_path / "nope.json", {}) is None


def _armed(tmp_path, *entries):
    path = tmp_path / "cmsrun.json"
    faults.atomic_write_json(path, list(entries))
    return path


def _left(path):
    return json.loads(path.read_text(encoding="utf-8"))


def test_consume_fault_counts_down_and_removes_a_spent_entry(tmp_path):
    path = _armed(tmp_path, {"mode": "exit_code", "exit_code": 1, "times_remaining": 2})

    assert faults.consume_fault(path, {})["exit_code"] == 1
    assert _left(path) == [{"mode": "exit_code", "exit_code": 1, "times_remaining": 1}]
    faults.consume_fault(path, {})
    assert _left(path) == []


def test_consume_fault_unlimited_never_removes_the_entry(tmp_path):
    path = _armed(tmp_path, {"mode": "exit_code", "exit_code": 1, "times_remaining": None})

    for _ in range(5):
        assert faults.consume_fault(path, {})["exit_code"] == 1
    assert len(_left(path)) == 1


def test_consume_fault_takes_the_first_matching_entry(tmp_path):
    path = _armed(
        tmp_path,
        {"mode": "exit_code", "exit_code": 1, "calibration": "SiStripBad", "times_remaining": 1},
        {"mode": "exit_code", "exit_code": 2, "calibration": "EcalPedestals", "times_remaining": 1},
    )

    assert faults.consume_fault(path, {"calibration": "EcalPedestals", "run": 1, "step": "step2"})["exit_code"] == 2
    assert [e["exit_code"] for e in _left(path)] == [1]
