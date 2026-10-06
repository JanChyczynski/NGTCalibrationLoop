"""Tests for tests/stubs/omsapi's faults: which real `requests` exception each mode raises and
from which call with what text, `times`, `run` scoping, the bool form and the live fault file. They
talk to the stub directly -- how a caller reacts to a fault is for that caller's tests.
"""
import json

import omsapi
import pytest
import requests

import faults

RUN = 398600


def _data(run=None):
    """The loops' query chain; `run` adds the run_number filter that scopes a fault."""
    query = omsapi.OMSAPI("https://cmsoms.cms/agg/api").query("runs")
    if run is not None:
        query = query.filter("run_number", run)
    return query.data()


def _runs(run=None):
    return len(_data(run).json()["data"])


@pytest.fixture
def one_run():
    omsapi.configure_runs([{"run_number": RUN, "end_time": None}])


# --- which call raises -----------------------------------------------------------------


@pytest.mark.parametrize(
    "mode, exc_cls",
    [("connection_error", requests.exceptions.ConnectionError), ("timeout", requests.exceptions.Timeout)],
)
def test_connection_errors_and_timeouts_raise_from_data(mode, exc_cls):
    omsapi.set_failure(mode)

    with pytest.raises(exc_cls, match="cmsoms.cms"):  # reads like a real outage
        _data()


@pytest.mark.parametrize(
    "status, text", [(503, "503 Server Error: Service Unavailable"), (403, "403 Client Error: Forbidden")]
)
def test_http_error_surfaces_only_through_raise_for_status(one_run, status, text):
    omsapi.set_failure("http_error", status=status)

    response = _data()  # the request itself "succeeds", like a real error response

    assert response.status_code == status
    with pytest.raises(requests.exceptions.HTTPError, match=text):
        response.raise_for_status()


def test_malformed_json_surfaces_only_when_the_body_is_parsed(one_run):
    omsapi.set_failure("malformed_json")

    response = _data()
    response.raise_for_status()  # a 200 -- the body is what is wrong

    assert "<html>" in response.text
    with pytest.raises(requests.exceptions.JSONDecodeError):
        response.json()


def test_the_bool_form_fails_every_call_until_turned_off():
    omsapi.set_failure(True)

    for _ in range(3):
        with pytest.raises(RuntimeError):
            _data()

    omsapi.set_failure(False)
    assert _data().json() == {"data": []}


# --- times and scoping -----------------------------------------------------------------


@pytest.mark.parametrize("times", [1, 2])
def test_times_counts_down_and_then_clears(one_run, times):
    omsapi.set_failure("connection_error", **({} if times == 1 else {"times": times}))  # 1: the default

    for _ in range(times):
        with pytest.raises(requests.exceptions.ConnectionError):
            _data()
    assert _runs() == 1


def test_unlimited_times_never_clears():
    omsapi.set_failure("connection_error", times=None)

    for _ in range(4):
        with pytest.raises(requests.exceptions.ConnectionError):
            _data()


def test_a_run_scoped_fault_only_hits_queries_filtering_on_that_run(one_run):
    omsapi.set_failure("connection_error", run=RUN, times=None)

    assert _runs() == 1  # no run_number filter at all, like a new-run query
    assert _runs(RUN + 1) == 0  # another run
    with pytest.raises(requests.exceptions.ConnectionError):
        _data(run=RUN)


def test_an_unscoped_fault_hits_queries_for_any_run():
    omsapi.set_failure("timeout", times=2)

    with pytest.raises(requests.exceptions.Timeout):
        _data(run=RUN)
    with pytest.raises(requests.exceptions.Timeout):
        _data()


@pytest.mark.parametrize("kwargs", [{"mode": "no_such_mode"}, {"mode": "http_error"}, {"mode": "http_error", "status": 200}])
def test_set_failure_rejects_what_the_vocabulary_would(kwargs):
    with pytest.raises(ValueError):
        omsapi.set_failure(**kwargs)


# --- the live fault file ---------------------------------------------------------------


def _arm_file(monkeypatch, tmp_path, entry):
    """What seed.py's arm_fault() leaves for the loops' processes, re-read on every query."""
    faults.atomic_write_json(tmp_path / "oms.json", [entry])
    monkeypatch.setenv("NGT_FAULTS_DIR", str(tmp_path))
    return tmp_path / "oms.json"


def _left(path):
    return json.loads(path.read_text(encoding="utf-8"))


def test_a_file_armed_fault_is_scoped_and_consumed(monkeypatch, tmp_path):
    path = _arm_file(monkeypatch, tmp_path, {"mode": "connection_error", "times_remaining": 1, "run": RUN})

    assert _runs(RUN + 1) == 0  # another run: not consumed
    with pytest.raises(requests.exceptions.ConnectionError):
        _data(run=RUN)
    assert _left(path) == []
    assert _runs(RUN) == 0


def test_a_file_armed_fault_counts_down(monkeypatch, tmp_path):
    path = _arm_file(monkeypatch, tmp_path, {"mode": "timeout", "times_remaining": 2})

    with pytest.raises(requests.exceptions.Timeout):
        _data()
    assert _left(path)[0]["times_remaining"] == 1
    with pytest.raises(requests.exceptions.Timeout):
        _data()
    assert _left(path) == []
