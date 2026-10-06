"""Tests for the fault modes of tests/stubs/omsapi's set_failure(): which real `requests`
exception each mode raises and from which call of the query chain, the `times` countdown,
`run` scoping, message overrides, the original bool form, and the file-based path the live
scenario player uses (it runs in another OS process, so it can't call set_failure() itself).

They talk to the stub directly, so they say nothing about how a particular caller reacts to
a fault -- that is for the tests of whatever code is being faulted.
"""
import json

import omsapi
import pytest
import requests

import faults

RUN = 398600


def _query(run=None):
    """The query chain the loops use; `run` adds the run_number filter that scopes a fault."""
    query = omsapi.OMSAPI("https://cmsoms.cms/agg/api").query("runs")
    if run is not None:
        query = query.filter("run_number", run)
    return query


def _data(run=None):
    return _query(run).data()


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

    with pytest.raises(exc_cls):
        _data()


def test_http_error_surfaces_only_through_raise_for_status(one_run):
    omsapi.set_failure("http_error", status=503)

    response = _data()  # the failed request itself "succeeds" -- like a real 503 response

    assert response.status_code == 503
    with pytest.raises(requests.exceptions.HTTPError, match="503 Server Error: Service Unavailable"):
        response.raise_for_status()


def test_a_4xx_http_error_reads_as_a_client_error(one_run):
    omsapi.set_failure("http_error", status=403)

    with pytest.raises(requests.exceptions.HTTPError, match="403 Client Error: Forbidden"):
        _data().raise_for_status()


def test_malformed_json_surfaces_only_when_the_body_is_parsed(one_run):
    omsapi.set_failure("malformed_json")

    response = _data()
    response.raise_for_status()  # a 200 -- the body is what is wrong

    assert "<html>" in response.text
    with pytest.raises(requests.exceptions.JSONDecodeError):
        response.json()


def test_the_bool_form_fails_every_call_without_a_countdown():
    omsapi.set_failure(True)

    for _ in range(3):
        with pytest.raises(RuntimeError):
            _data()

    omsapi.set_failure(False)
    assert _data().json() == {"data": []}


# --- times, scoping, messages ----------------------------------------------------------


def test_a_fault_is_single_shot_by_default(one_run):
    omsapi.set_failure("connection_error")

    with pytest.raises(requests.exceptions.ConnectionError):
        _data()
    assert len(_data().json()["data"]) == 1


def test_times_counts_down_and_then_clears(one_run):
    omsapi.set_failure("connection_error", times=2)

    for _ in range(2):
        with pytest.raises(requests.exceptions.ConnectionError):
            _data()
    assert len(_data().json()["data"]) == 1


def test_unlimited_times_never_clears():
    omsapi.set_failure("connection_error", times=None)

    for _ in range(4):
        with pytest.raises(requests.exceptions.ConnectionError):
            _data()


def test_a_run_scoped_fault_only_hits_queries_filtering_on_that_run(one_run):
    omsapi.set_failure("connection_error", run=RUN, times=None)

    assert len(_data().json()["data"]) == 1  # no run_number filter at all, like a new-run query
    assert len(_data(run=RUN + 1).json()["data"]) == 0  # another run
    with pytest.raises(requests.exceptions.ConnectionError):
        _data(run=RUN)


def test_an_unscoped_fault_hits_queries_for_any_run():
    omsapi.set_failure("timeout", times=2)

    with pytest.raises(requests.exceptions.Timeout):
        _data(run=RUN)
    with pytest.raises(requests.exceptions.Timeout):
        _data()


def test_the_default_message_looks_like_a_real_oms_outage():
    omsapi.set_failure("connection_error")

    with pytest.raises(requests.exceptions.ConnectionError, match="cmsoms.cms"):
        _data()


def test_a_custom_message_is_used_verbatim():
    omsapi.set_failure("connection_error", message="custom OMS-is-down message")

    with pytest.raises(requests.exceptions.ConnectionError, match="custom OMS-is-down message"):
        _data()


def test_reset_clears_an_armed_fault():
    omsapi.set_failure("connection_error", times=None)

    omsapi.reset()

    assert _data().json() == {"data": []}


@pytest.mark.parametrize("kwargs", [{"mode": "no_such_mode"}, {"mode": "http_error"}, {"mode": "http_error", "status": 200}])
def test_set_failure_rejects_what_the_vocabulary_would(kwargs):
    with pytest.raises(ValueError):
        omsapi.set_failure(**kwargs)


# --- the file-based path ---------------------------------------------------------------


def test_a_file_armed_fault_is_consumed_like_the_in_memory_one(monkeypatch, tmp_path):
    """scenario_player.py arms faults through seed.py's arm_fault(), which writes this file;
    the stub polls it on every query, like NGT_OMS_STUB_RUNS_FILE."""
    faults_file = tmp_path / "oms.json"
    faults.atomic_write_json(faults_file, [{"mode": "connection_error", "times_remaining": 1, "run": RUN}])
    monkeypatch.setenv("NGT_OMS_STUB_FAULTS_FILE", str(faults_file))

    assert _data(run=RUN + 1).json() == {"data": []}  # another run: not consumed
    with pytest.raises(requests.exceptions.ConnectionError):
        _data(run=RUN)

    assert json.loads(faults_file.read_text(encoding="utf-8")) == []  # single-shot: removed
    assert _data(run=RUN).json() == {"data": []}


def test_a_file_armed_fault_counts_down(monkeypatch, tmp_path):
    faults_file = tmp_path / "oms.json"
    faults.atomic_write_json(faults_file, [{"mode": "timeout", "times_remaining": 2}])
    monkeypatch.setenv("NGT_OMS_STUB_FAULTS_FILE", str(faults_file))

    with pytest.raises(requests.exceptions.Timeout):
        _data()
    assert json.loads(faults_file.read_text(encoding="utf-8"))[0]["times_remaining"] == 1
    with pytest.raises(requests.exceptions.Timeout):
        _data()
    assert json.loads(faults_file.read_text(encoding="utf-8")) == []
