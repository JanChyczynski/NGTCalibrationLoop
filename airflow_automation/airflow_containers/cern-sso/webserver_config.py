"""
FAB's settings for logging in through CERN's SSO instead of the one-pod deployment's generated
password. Whoever logs in is a Viewer, and an Admin if the Application Portal gives them that role.
kustomization.yaml next to this file selects it, and README.md says what else it takes.
"""

import os

from airflow.providers.fab.auth_manager.security_manager.override import FabAirflowSecurityManagerOverride
from flask_appbuilder.const import AUTH_OAUTH


class CernSecurityManager(FabAirflowSecurityManagerOverride):
    def get_oauth_user_info(self, _provider, resp):
        # From the ID token, which authlib has already validated. FAB's default reads `groups` from
        # the UserInfo endpoint, and CERN's docs list `cern_roles` for the tokens, not for that.
        claims = resp["userinfo"]
        return {
            "username": claims["preferred_username"],
            "first_name": claims.get("given_name", ""),
            "last_name": claims.get("family_name", ""),
            "email": claims.get("email", ""),
            "role_keys": claims.get("cern_roles", []),
        }


SECURITY_MANAGER_CLASS = CernSecurityManager
AUTH_TYPE = AUTH_OAUTH
AUTH_USER_REGISTRATION = True
AUTH_USER_REGISTRATION_ROLE = "Viewer"
AUTH_ROLES_MAPPING = {os.environ.get("NGT_SSO_ADMIN_ROLE", "admin"): ["Admin"]}
AUTH_ROLES_SYNC_AT_LOGIN = True  # so that losing the role in the portal loses Admin at the next login
OAUTH_PROVIDERS = [
    {
        "name": "cern",
        "icon": "fa-key",
        "token_key": "access_token",
        "remote_app": {
            "client_id": os.environ["sso_clientID"],
            "client_secret": os.environ["sso_clientSecret"],
            "server_metadata_url": f"{os.environ['sso_issuerURL']}/.well-known/openid-configuration",
            "client_kwargs": {"scope": "openid profile email"},
        },
    }
]
