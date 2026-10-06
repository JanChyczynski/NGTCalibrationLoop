"""
Minimal in-memory stand-in for the CERN oms-api-client (gitlab.cern.ch/cmsoms/oms-api-client).

The real client is not published on PyPI and requires CERN GitLab access, so it can't be
installed in a plain local/CI environment. NGTLoopStep2.py only relies on a small slice of
its query-builder interface (query().filter().sort().paginate().data().json()), which is
reimplemented here against an in-memory list of "runs" that tests populate directly.

This is a test double, not a general-purpose OMS client -- it has no network calls, no
authentication, and no real REST semantics beyond simple equality-filter/sort/paginate.

Two ways to feed it data:
- configure_runs() / reset() -- used by the pytest suite, in-memory only.
- the NGT_OMS_STUB_RUNS_FILE env var -- used by the live demos (exported by the $NGT_DEV_HOME/env.sh
  scenario-player/sim_env.sh writes), points at a JSON file
  containing a list of run dicts that is re-read on every query, so a live NGTLoopStep2.py
  process picks up edits to the file (e.g. "end" a run) without needing a restart.

Faults follow the same split: set_failure() in-process, $NGT_FAULTS_DIR/oms.json (written by
scenario-player/seed.py's arm_fault()) for the live demo.
"""

import json
import os
from http import HTTPStatus

import requests

_RUNS = []
_FAIL = False
_FAULTS = []  # set_failure()'s armed fault, as a fault-file entry (scenario-player/faults.py)

def configure_runs(runs):
    """Replace the in-memory set of OMS "runs" records returned by queries.

    Each run is a plain dict of attributes, e.g.:
        {"run_number": 398600, "fill_type_runtime": "PROTONS",
         "l1_hlt_mode": "collisions2026", "stable_beam": True,
         "start_time": "2026-08-10T10:00:00Z", "end_time": None,
         "last_lumisection_number": 120}
    """
    global _RUNS
    _RUNS = list(runs)


def set_failure(mode=True, *, status=None, times=1, run=None):
    """Simulate OMS trouble on the next query()...data() call(s).

    set_failure(True)/(False): every query raises RuntimeError / none does.
    set_failure("connection_error"|"timeout"|"http_error"|"malformed_json", ...): the
    `requests` exception a real outage raises, validated like a scenario's `faults:` entry
    (status for http_error; times=None for no limit; run= only hits queries filtering on
    that run_number, so never a search for a new run).
    """
    global _FAIL
    if mode is True or mode is False:
        _FAIL = mode
        _FAULTS.clear()
        return
    import faults as ngt_faults  # see _consume_fault

    spec = ngt_faults.parse_fault_spec(
        {"target": "oms", "mode": mode, "status": status, "times": times, "run": run}
    )
    _FAIL = False
    _FAULTS[:] = [ngt_faults.fault_spec_to_entry(spec)]


def reset():
    """Clear all configured runs/failure mode. Call between tests to avoid state leaking."""
    configure_runs([])
    set_failure(False)


def _current_runs():
    """The runs to query against: live-reloaded from NGT_OMS_STUB_RUNS_FILE if set
    (live-demo usage), otherwise the in-memory list from configure_runs()
    (pytest usage)."""
    runs_file = os.environ.get("NGT_OMS_STUB_RUNS_FILE")
    if not runs_file:
        return _RUNS
    try:
        with open(runs_file, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return []


def _run_number_filter(query_filters):
    for key, value in query_filters:
        if key == "run_number":
            return value
    return None


def _consume_fault(run_number):
    """The armed fault this query hits, with one use counted off it: set_failure()'s,
    else one from $NGT_FAULTS_DIR/oms.json, re-read on every query like the runs file."""
    faults_dir = os.environ.get("NGT_FAULTS_DIR")
    faults_file = os.path.join(faults_dir, "oms.json") if faults_dir else None
    if not _FAULTS and not faults_file:
        return None
    # scenario-player/faults.py: on PYTHONPATH via env.sh, and tests/conftest.py's sys.path.
    import faults as ngt_faults

    scope = {"run": run_number}
    fault = ngt_faults.take_fault(_FAULTS, scope)
    if fault is None and faults_file:
        with ngt_faults.locked_json_file(faults_file):
            fault = ngt_faults.consume_fault(faults_file, scope)
    return fault


def _default_message(mode, status=None):
    """Canned error text that reads like a genuine OMS outage."""
    if mode == "connection_error":
        return (
            "HTTPSConnectionPool(host='cmsoms.cms', port=443): Max retries exceeded with "
            "url: /agg/api/v1/runs (Caused by NewConnectionError('<urllib3.connection."
            "HTTPSConnection object>: Failed to establish a new connection: "
            "[Errno 111] Connection refused'))"
        )
    if mode == "timeout":
        return "HTTPSConnectionPool(host='cmsoms.cms', port=443): Read timed out. (read timeout=None)"
    if mode == "http_error":
        kind = "Server" if status is not None and status >= 500 else "Client"
        reason = {s.value: s.phrase for s in HTTPStatus}.get(status, "Error")
        return f"{status} {kind} Error: {reason} for url: https://cmsoms.cms/agg/api/v1/runs"
    if mode == "malformed_json":
        return "Expecting value: line 1 column 1 (char 0)"
    return "stub OMS simulated failure"


class _Response:
    def __init__(self, payload, status_code=200, malformed_json=False):
        self._payload = payload
        self.status_code = status_code
        self._malformed_json = malformed_json
        if malformed_json:
            # an unhealthy OMS (or the proxy in front of it) answers with an HTML error page
            self.text = (
                "<html><head><title>503 Service Unavailable</title></head><body><h1>Service Unavailable</h1>"
                "<p>The server is temporarily unable to service your request.</p></body></html>"
            )
        else:
            self.text = json.dumps(payload)

    def json(self):
        if self._malformed_json:
            raise requests.exceptions.JSONDecodeError(_default_message("malformed_json"), self.text, 0)
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.exceptions.HTTPError(_default_message("http_error", self.status_code), response=self)


class _Query:
    def __init__(self, entity):
        self.entity = entity
        self._filters = []
        self._sort_key = None
        self._sort_asc = True
        self._page = 1
        self._per_page = None

    def filter(self, key, value):
        self._filters.append((key, value))
        return self

    def sort(self, key, asc=True):
        self._sort_key = key
        self._sort_asc = asc
        return self

    def paginate(self, page=1, per_page=None):
        self._page = page
        self._per_page = per_page
        return self

    def data_query(self):
        return f"<stub query on '{self.entity}' filters={self._filters}>"

    def _matching_runs(self):
        results = _current_runs()
        for key, value in self._filters:
            results = [r for r in results if r.get(key) == value]
        if self._sort_key:
            results = sorted(
                results, key=lambda r: r.get(self._sort_key), reverse=not self._sort_asc
            )
        if self._per_page:
            start = (self._page - 1) * self._per_page
            results = results[start : start + self._per_page]
        return results

    def data(self):
        if _FAIL:
            raise RuntimeError("stub OMS is configured to fail (omsapi.set_failure(True))")

        run_number = _run_number_filter(self._filters)
        fault = _consume_fault(run_number)

        if fault and fault["mode"] in ("connection_error", "timeout"):
            cls = requests.exceptions.ConnectionError if fault["mode"] == "connection_error" else requests.exceptions.Timeout
            raise cls(_default_message(fault["mode"]))

        matched = self._matching_runs()
        payload = {"data": [{"attributes": dict(r)} for r in matched]}

        status = fault["status"] if fault and fault["mode"] == "http_error" else 200
        malformed = bool(fault) and fault["mode"] == "malformed_json"
        return _Response(payload, status_code=status, malformed_json=malformed)


class OMSAPI:
    """Drop-in stand-in for omsapi.OMSAPI, backed by `configure_runs()`."""

    def __init__(self, base_url, version="v1", cert_verify=True):
        self.base_url = base_url
        self.version = version
        self.cert_verify = cert_verify

    def query(self, entity):
        return _Query(entity)
