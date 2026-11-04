"""
The cern-sso layer end to end: logging in the way a browser does, on the emulated one-pod deployment,
against a stand-in for CERN's SSO, a Keycloak set up like it (cern_sso_standin/). It shows that the
roles in the ID token decide who's an Admin. It can't show that the real SSO is like the stand-in. Slow,
needs Docker with Compose v2 and pulls images, so it's deselected from the default `pytest` run (see
pytest.ini) -- run it with `pytest -m container`. Uses its own Compose project and ports.
"""

import html
import http.cookiejar
import os
import re
import shutil
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

import pytest
from test_containers_live import IMAGE, ONEPOD_DIR, _run, _status, _wait_healthy

STANDIN = Path(__file__).resolve().parent / "cern_sso_standin"
PORT = 18293  # the one realm-cern.json lets people come back to
BASE = f"http://localhost:{PORT}"
ISSUER = "http://localhost:18180/auth/realms/cern"

pytestmark = [
    pytest.mark.container,
    pytest.mark.skipif(shutil.which("docker") is None, reason="needs docker"),
]


def _wait_for_the_sso(timeout=240):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if _status(f"{ISSUER}/.well-known/openid-configuration") == 200:
                return
        except OSError:
            pass
        time.sleep(2)
    pytest.fail("the stand-in SSO never came up")


class _LikeABrowser(http.cookiejar.DefaultCookiePolicy):
    """Browsers count http://localhost as secure, and Keycloak relies on that for its login's cookies."""

    def return_ok_secure(self, cookie, request):
        return True


def _log_in(username):
    """A browser's part of the login: where Airflow's button goes, the SSO's form, and back. Returns the session's token."""
    cookies = http.cookiejar.CookieJar(policy=_LikeABrowser())
    browser = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cookies))
    with browser.open(f"{BASE}/auth/login/cern", timeout=30) as response:
        assert response.url.startswith(ISSUER), response.url
        form = re.search(r'<form[^>]*action="([^"]+)"', response.read().decode()).group(1)
    credentials = urllib.parse.urlencode({"username": username, "password": username}).encode()
    with browser.open(html.unescape(form), data=credentials, timeout=30) as response:
        assert response.url.startswith(BASE), response.url
    return next(cookie.value for cookie in cookies if cookie.name == "_token")


def test_the_roles_in_the_id_token_decide_who_is_an_admin():
    env = dict(
        os.environ,
        COMPOSE_PROJECT_NAME="ngt_cern_sso_pytest",
        NGT_ONEPOD_PORT=str(PORT),
        NGT_AIRFLOW_IMAGE=IMAGE,
        NGT_SSO_STANDIN_DIR=str(STANDIN),
    )
    compose = ["docker", "compose", "-f", "docker-compose.yaml", "-f", str(STANDIN / "docker-compose.yaml")]
    dag = f"{BASE}/api/v2/dags/ngt_watch_run_primer"
    change = {"method": "PATCH", "body": {"is_paused": False}}
    try:
        up = _run([*compose, "up", "-d", "--build"], env, cwd=ONEPOD_DIR)
        assert up.returncode == 0, up.stdout + up.stderr
        _wait_healthy(PORT)
        _wait_for_the_sso()

        assert _status(f"{BASE}/api/v2/dags") == 200, "reading still takes no login"
        assert _status(dag, **change) == 403
        assert _status(dag, token=_log_in("a-viewer"), **change) == 403
        assert _status(dag, token=_log_in("an-admin"), **change) == 200
    finally:
        _run([*compose, "down", "-v"], env, cwd=ONEPOD_DIR)
