import os
from base64 import urlsafe_b64decode
from json import loads as json_loads
from typing import List

from flask import redirect, request
from flask_appbuilder import expose
from airflow.www.security import AirflowSecurityManager
from flask_appbuilder.security.manager import AUTH_OAUTH
from flask_appbuilder.security.views import AuthOAuthView

AUTH_TYPE = AUTH_OAUTH
AUTH_USER_REGISTRATION = True


def _env_nonempty(name: str, default: str) -> str:
    value = os.getenv(name)
    if value is None:
        return default
    stripped = value.strip()
    return stripped or default


AUTH_USER_REGISTRATION_ROLE = _env_nonempty("AIRFLOW_OAUTH_DEFAULT_ROLE", "Viewer")
AUTH_ROLES_SYNC_AT_LOGIN = True
AUTH_ROLES_MAPPING = {
    "admin": ["Admin"],
    "airflow_admin": ["Admin"],
    "airflow_op": ["Op"],
    "airflow_user": ["User"],
    "airflow_viewer": ["Viewer"],
}

_OAUTH_BASE_URL = os.getenv(
    "AIRFLOW_OAUTH_BASE_URL",
    "http://keycloak:8090/realms/odp/protocol/openid-connect",
).rstrip("/")
_OAUTH_DISCOVERY_URL = os.getenv(
    "AIRFLOW_OAUTH_DISCOVERY_URL",
    "http://keycloak:8090/realms/odp/.well-known/openid-configuration",
)
_OAUTH_ISSUER = os.getenv("AIRFLOW_OAUTH_ISSUER", "http://localhost:8090/realms/odp")
_OAUTH_JWKS_URL = os.getenv(
    "AIRFLOW_OAUTH_JWKS_URL",
    "http://keycloak:8090/realms/odp/protocol/openid-connect/certs",
)

OAUTH_PROVIDERS = [
    {
        "name": "keycloak",
        "icon": "fa-key",
        "token_key": "access_token",
        "remote_app": {
            "client_id": os.getenv("AIRFLOW_OAUTH_CLIENT_ID", "airflow"),
            "client_secret": os.getenv("AIRFLOW_OAUTH_CLIENT_SECRET", ""),
            "server_metadata_url": _OAUTH_DISCOVERY_URL,
            "server_metadata": {
                "issuer": _OAUTH_ISSUER,
                "jwks_uri": _OAUTH_JWKS_URL,
            },
            "api_base_url": f"{_OAUTH_BASE_URL}/",
            "access_token_url": os.getenv("AIRFLOW_OAUTH_TOKEN_URL", f"{_OAUTH_BASE_URL}/token"),
            "authorize_url": os.getenv("AIRFLOW_OAUTH_AUTHORIZE_URL", f"{_OAUTH_BASE_URL}/auth"),
            "client_kwargs": {"scope": "profile email"},
        },
    }
]


class AutoRedirectOAuthView(AuthOAuthView):
    """Skip the login page and redirect straight to the Keycloak provider."""

    @expose("/login/")
    @expose("/login/<provider>")
    @expose("/login/<provider>/<register>")
    def login(self, provider=None, register=None):
        if provider is not None:
            return super().login(provider=provider)
        return redirect(request.url_root.rstrip("/") + "/login/keycloak")


class KeycloakSecurityManager(AirflowSecurityManager):
    authoauthview = AutoRedirectOAuthView

    @staticmethod
    def _decode_access_token_claims(access_token: str) -> dict:
        try:
            parts = access_token.split(".")
            if len(parts) != 3:
                return {}
            payload = parts[1] + "=" * (-len(parts[1]) % 4)
            return json_loads(urlsafe_b64decode(payload).decode("utf-8"))
        except Exception:  # noqa: BLE001
            return {}

    @staticmethod
    def _extract_claim_roles(claims: dict) -> List[str]:
        realm_roles = (claims.get("realm_access") or {}).get("roles") or []
        if not isinstance(realm_roles, list):
            realm_roles = []

        airflow_client_id = os.getenv("AIRFLOW_OAUTH_CLIENT_ID", "airflow")
        client_roles = ((claims.get("resource_access") or {}).get(airflow_client_id) or {}).get("roles") or []
        if not isinstance(client_roles, list):
            client_roles = []

        role_keys = [str(role) for role in [*realm_roles, *client_roles] if isinstance(role, str) and role]
        # Keep order stable while removing duplicates.
        return list(dict.fromkeys(role_keys))

    def get_oauth_user_info(self, provider, resp):  # pylint: disable=unused-argument
        if provider != "keycloak":
            return {}

        if isinstance(resp, dict):
            access_token = resp.get("access_token")
            if access_token:
                claims = self._decode_access_token_claims(access_token)
                if claims:
                    username = claims.get("preferred_username") or claims.get("email") or claims.get("sub")
                    email = claims.get("email")
                    if email == "admin@example.com" and username and username != "admin":
                        email = f"{username}@example.com"
                    return {
                        "name": claims.get("name") or username,
                        "email": email or (f"{username}@example.com" if username else None),
                        "first_name": claims.get("given_name"),
                        "last_name": claims.get("family_name"),
                        "username": username,
                        "role_keys": self._extract_claim_roles(claims),
                    }

        remote_app = self.oauth_remotes.get(provider)
        if not remote_app:
            return {}

        userinfo_response = remote_app.get("userinfo")
        if not userinfo_response or userinfo_response.status_code >= 400:
            return {}

        userinfo = userinfo_response.json() or {}
        username = userinfo.get("preferred_username") or userinfo.get("email") or userinfo.get("sub")
        email = userinfo.get("email")
        if email == "admin@example.com" and username and username != "admin":
            email = f"{username}@example.com"
        return {
            "name": userinfo.get("name") or username,
            "email": email or (f"{username}@example.com" if username else None),
            "first_name": userinfo.get("given_name"),
            "last_name": userinfo.get("family_name"),
            "username": username,
            "role_keys": [],
        }


SECURITY_MANAGER_CLASS = KeycloakSecurityManager
