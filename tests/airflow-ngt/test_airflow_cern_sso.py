"""
The cern-sso layer in airflow_automation/airflow_containers/cern-sso/, without a container runtime: what
FAB is told, and that the kustomize component selects it. test_airflow_cern_sso_live.py exercises it
against a stand-in for the real SSO, deselected by default.
"""

import json
import os
import runpy
import shutil
import subprocess
import sys
import types
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
CONTAINERS = REPO_ROOT / "airflow_automation" / "airflow_containers"
LAYER = CONTAINERS / "cern-sso"
STANDIN = Path(__file__).resolve().parent / "cern_sso_standin"

needs_kubectl = pytest.mark.skipif(shutil.which("kubectl") is None, reason="needs kubectl, for its kustomize")


@pytest.fixture
def config(monkeypatch):
    """webserver_config.py's settings. What it imports is faked: it runs where neither FAB nor Airflow is installed."""
    override = types.ModuleType("airflow.providers.fab.auth_manager.security_manager.override")
    override.FabAirflowSecurityManagerOverride = object
    const = types.ModuleType("flask_appbuilder.const")
    const.AUTH_OAUTH = "FAB's OAuth"
    monkeypatch.setitem(sys.modules, override.__name__, override)
    monkeypatch.setitem(sys.modules, const.__name__, const)
    monkeypatch.setenv("sso_clientID", "the-client")
    monkeypatch.setenv("sso_clientSecret", "the-secret")
    monkeypatch.setenv("sso_issuerURL", "https://sso.example/auth/realms/cern")
    monkeypatch.delenv("NGT_SSO_ADMIN_ROLE", raising=False)
    return runpy.run_path(str(LAYER / "webserver_config.py"))


def _component_container():
    component = yaml.safe_load((LAYER / "kustomization.yaml").read_text(encoding="utf-8"))
    assert component["kind"] == "Component"
    return yaml.safe_load(component["patches"][0]["patch"])["spec"]["template"]["spec"]["containers"][0]


def _component_env():
    return {e["name"]: e["value"] for e in _component_container()["env"]}


def test_roles_come_from_the_id_token_not_the_userinfo_endpoint(config):
    """CERN's docs list `cern_roles` for the tokens, and not for that endpoint. Nothing here may call it."""
    manager = config["CernSecurityManager"]()
    claims = {
        "preferred_username": "jdoe",
        "given_name": "Jane",
        "family_name": "Doe",
        "email": "jane.doe@cern.ch",
        "cern_roles": ["default-role", "admin"],
    }
    assert manager.get_oauth_user_info("cern", {"userinfo": claims}) == {
        "username": "jdoe",
        "first_name": "Jane",
        "last_name": "Doe",
        "email": "jane.doe@cern.ch",
        "role_keys": ["default-role", "admin"],
    }


def test_no_role_at_all_is_still_a_login(config):
    manager = config["CernSecurityManager"]()
    assert manager.get_oauth_user_info("cern", {"userinfo": {"preferred_username": "jdoe"}})["role_keys"] == []


def test_whoever_logs_in_is_a_viewer_and_only_the_portals_role_makes_an_admin(config):
    assert config["AUTH_TYPE"] == "FAB's OAuth"
    assert config["SECURITY_MANAGER_CLASS"] is config["CernSecurityManager"]
    assert config["AUTH_USER_REGISTRATION"] is True
    assert config["AUTH_USER_REGISTRATION_ROLE"] == "Viewer"
    assert config["AUTH_ROLES_MAPPING"] == {"admin": ["Admin"]}
    assert config["AUTH_ROLES_SYNC_AT_LOGIN"] is True, "or losing the role wouldn't lose Admin"


def test_the_sso_is_whatever_the_secret_says(config):
    remote_app = config["OAUTH_PROVIDERS"][0]["remote_app"]
    assert remote_app["client_id"] == "the-client"
    assert remote_app["client_secret"] == "the-secret"
    assert remote_app["server_metadata_url"] == "https://sso.example/auth/realms/cern/.well-known/openid-configuration"
    assert "openid" in remote_app["client_kwargs"]["scope"].split(), "no ID token without it"


def test_component_selects_the_config_as_it_is_in_the_image():
    in_image = _component_env()["AIRFLOW__FAB__CONFIG_FILE"]
    assert in_image.startswith("/opt/ngt/")
    assert (REPO_ROOT / in_image[len("/opt/ngt/") :]).is_file()


def test_component_gets_the_settings_from_the_secret_of_the_registration():
    assert _component_container()["envFrom"] == [{"prefix": "sso_", "secretRef": {"name": "oidc-client-secret"}}]


def test_return_address_follows_the_tls_in_front_of_the_pod():
    assert _component_env()["AIRFLOW__FAB__ENABLE_PROXY_FIX"] == "True"


def test_image_can_do_oidc():
    # FAB's OAuth imports authlib, which the FAB provider doesn't depend on: a crash at the first start.
    dockerfile = (CONTAINERS / "Dockerfile").read_text(encoding="utf-8")
    assert "authlib" in dockerfile


def test_standin_has_the_components_settings():
    environment = yaml.safe_load((STANDIN / "docker-compose.yaml").read_text(encoding="utf-8"))["services"]["airflow"]["environment"]
    for name, value in _component_env().items():
        assert environment[name] == value, name


def test_standin_only_has_the_roles_where_the_real_sso_does():
    realm = json.loads((STANDIN / "realm-cern.json").read_text(encoding="utf-8"))
    mapper = realm["clients"][0]["protocolMappers"][0]["config"]
    assert mapper["claim.name"] == "cern_roles"
    assert mapper["id.token.claim"] == mapper["access.token.claim"] == "true"
    assert mapper["userinfo.token.claim"] == "false"


@needs_kubectl
def test_component_goes_on_the_plain_manifest(tmp_path):
    overlay = tmp_path / "my-cluster"
    overlay.mkdir()
    (overlay / "kustomization.yaml").write_text(
        yaml.safe_dump(
            {
                "apiVersion": "kustomize.config.k8s.io/v1beta1",
                "kind": "Kustomization",
                "resources": [os.path.relpath(CONTAINERS / "onepod", overlay)],
                "components": [os.path.relpath(LAYER, overlay)],
            }
        ),
        encoding="utf-8",
    )
    result = subprocess.run(["kubectl", "kustomize", str(overlay)], capture_output=True, text=True, check=True)
    docs = {doc["kind"]: doc for doc in yaml.safe_load_all(result.stdout)}
    containers = {c["name"]: c for c in docs["Deployment"]["spec"]["template"]["spec"]["containers"]}
    env = {e["name"]: e.get("value") for e in containers["airflow"]["env"]}
    assert env["AIRFLOW__FAB__CONFIG_FILE"] == _component_env()["AIRFLOW__FAB__CONFIG_FILE"]
    assert env["AIRFLOW__FAB__AUTH_ROLE_PUBLIC"] == "Viewer", "the base's own settings are still there"
    assert containers["airflow"]["envFrom"] == _component_container()["envFrom"]
    assert "envFrom" not in containers["postgres"]
