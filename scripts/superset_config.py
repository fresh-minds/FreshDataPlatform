import os
from base64 import urlsafe_b64decode
from json import loads as json_loads

from flask_appbuilder.security.manager import AUTH_OAUTH
from superset.security import SupersetSecurityManager

ROW_LIMIT = 5000
SECRET_KEY = os.getenv("SUPERSET_SECRET_KEY", "change_me_superset_secret_key")
WTF_CSRF_ENABLED = False
# Allow data upload for testing
CSV_EXTENSIONS = {"csv"}
ALLOWED_EXTENSIONS = {"csv"}
# Force SQL Lab to run synchronously; no Celery worker configured in this stack.
FEATURE_FLAGS = {
    "ENABLE_ASYNC_QUERIES": False,
}


def _strip_trailing_slash(value: str) -> str:
    return value.rstrip("/")


KEYCLOAK_OIDC_BASE_URL = _strip_trailing_slash(
    os.getenv(
        "KEYCLOAK_OIDC_BASE_URL",
        "http://keycloak:8090/realms/odp/protocol/openid-connect",
    )
)
KEYCLOAK_OIDC_TOKEN_URL = os.getenv("KEYCLOAK_OIDC_TOKEN_URL", f"{KEYCLOAK_OIDC_BASE_URL}/token")
KEYCLOAK_OIDC_DISCOVERY_URL = os.getenv(
    "KEYCLOAK_OIDC_DISCOVERY_URL",
    "http://keycloak:8090/realms/odp/.well-known/openid-configuration",
)
KEYCLOAK_OIDC_SUPERSET_BROWSER_AUTHORIZE_URL = os.getenv(
    "KEYCLOAK_OIDC_SUPERSET_BROWSER_AUTHORIZE_URL",
    os.getenv(
        "KEYCLOAK_OIDC_BROWSER_AUTHORIZE_URL",
        "http://localhost:8090/realms/odp/protocol/openid-connect/auth",
    ),
)

AUTH_TYPE = AUTH_OAUTH
AUTH_USER_REGISTRATION = True
AUTH_USER_REGISTRATION_ROLE = os.getenv("SUPERSET_OAUTH_DEFAULT_ROLE", "Admin")
AUTH_ROLES_SYNC_AT_LOGIN = True
ENABLE_PROXY_FIX = os.getenv("SUPERSET_ENABLE_PROXY_FIX", "true").lower() == "true"
PREFERRED_URL_SCHEME = os.getenv("SUPERSET_PREFERRED_URL_SCHEME", "http")


class KeycloakSecurityManager(SupersetSecurityManager):
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

    def oauth_user_info(self, provider, response=None):  # noqa: ANN001,ANN201
        if provider != "keycloak":
            return {}

        if isinstance(response, dict):
            access_token = response.get("access_token")
            if access_token:
                claims = self._decode_access_token_claims(access_token)
                if claims:
                    return {
                        "username": claims.get("preferred_username") or claims.get("email"),
                        "first_name": claims.get("given_name", ""),
                        "last_name": claims.get("family_name", ""),
                        "email": claims.get("email"),
                    }

        me = self.appbuilder.sm.oauth_remotes[provider].get("userinfo")
        me.raise_for_status()
        data = me.json()

        return {
            "username": data.get("preferred_username") or data.get("email"),
            "first_name": data.get("given_name", ""),
            "last_name": data.get("family_name", ""),
            "email": data.get("email"),
        }


CUSTOM_SECURITY_MANAGER = KeycloakSecurityManager

OAUTH_PROVIDERS = [
    {
        "name": "keycloak",
        "icon": "fa-key",
        "token_key": "access_token",
        "remote_app": {
            "client_id": os.getenv("KEYCLOAK_SUPERSET_CLIENT_ID", "superset"),
            "client_secret": os.getenv(
                "KEYCLOAK_SUPERSET_CLIENT_SECRET",
                "change_me_keycloak_superset_secret",
            ),
            "server_metadata_url": KEYCLOAK_OIDC_DISCOVERY_URL,
            "api_base_url": f"{KEYCLOAK_OIDC_BASE_URL}/",
            "access_token_url": KEYCLOAK_OIDC_TOKEN_URL,
            "authorize_url": KEYCLOAK_OIDC_SUPERSET_BROWSER_AUTHORIZE_URL,
            "client_kwargs": {
                "scope": "profile email",
            },
        },
    },
]
