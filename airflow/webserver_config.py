import os

from airflow.www.security import AirflowSecurityManager
from flask_appbuilder.security.manager import AUTH_OAUTH

AUTH_TYPE = AUTH_OAUTH
AUTH_USER_REGISTRATION = True
AUTH_USER_REGISTRATION_ROLE = os.getenv("AIRFLOW_OAUTH_DEFAULT_ROLE", "Admin")

_OAUTH_BASE_URL = os.getenv(
    "AIRFLOW_OAUTH_BASE_URL",
    "http://keycloak:8090/realms/odp/protocol/openid-connect",
).rstrip("/")

OAUTH_PROVIDERS = [
    {
        "name": "keycloak",
        "icon": "fa-key",
        "token_key": "access_token",
        "remote_app": {
            "client_id": os.getenv("AIRFLOW_OAUTH_CLIENT_ID", "airflow"),
            "client_secret": os.getenv("AIRFLOW_OAUTH_CLIENT_SECRET", ""),
            "api_base_url": f"{_OAUTH_BASE_URL}/",
            "access_token_url": os.getenv("AIRFLOW_OAUTH_TOKEN_URL", f"{_OAUTH_BASE_URL}/token"),
            "authorize_url": os.getenv("AIRFLOW_OAUTH_AUTHORIZE_URL", f"{_OAUTH_BASE_URL}/auth"),
            "client_kwargs": {"scope": "openid profile email"},
        },
    }
]


class KeycloakSecurityManager(AirflowSecurityManager):
    def get_oauth_user_info(self, provider, resp):  # pylint: disable=unused-argument
        if provider != "keycloak":
            return {}

        remote_app = self.oauth_remotes.get(provider)
        if not remote_app:
            return {}

        userinfo_response = remote_app.get("userinfo")
        if not userinfo_response or userinfo_response.status_code >= 400:
            return {}

        userinfo = userinfo_response.json() or {}
        username = userinfo.get("preferred_username") or userinfo.get("email") or userinfo.get("sub")
        return {
            "name": userinfo.get("name") or username,
            "email": userinfo.get("email"),
            "first_name": userinfo.get("given_name"),
            "last_name": userinfo.get("family_name"),
            "username": username,
        }


SECURITY_MANAGER_CLASS = KeycloakSecurityManager
