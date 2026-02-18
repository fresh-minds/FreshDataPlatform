# SSO Test Inventory

## 1) Keycloak Base URL
- Local (docker-compose / kind port-forward): `http://keycloak:8090` (browser requires `/etc/hosts` entry `127.0.0.1 keycloak`)
- Local direct (port-forward alternative): `http://localhost:8090`
- AKS/stage-style (inferred from manifests): `https://keycloak.<FRONTEND_DOMAIN>`

## 2) Realms
- Realm discovered: `odp`
- Realm import source: `/Users/karelgoense/Documents/programming/FreshMinds_Programming/production/ai_trial/ops/keycloak/odp-realm.json`

## 3) Clients, Redirect URIs, Origins

| Client ID | Protocol | Type | Redirect URIs (local) | Redirect URIs (AKS) | Web Origins |
|---|---|---|---|---|---|
| `airflow` | OIDC | Confidential | `http://localhost:8080/oauth-authorized/keycloak`, `http://localhost:8080/auth/oauth-authorized/keycloak` | `https://airflow.<FRONTEND_DOMAIN>/oauth-authorized/keycloak`, `https://airflow.<FRONTEND_DOMAIN>/auth/oauth-authorized/keycloak` | `*` |
| `datahub` | OIDC | Confidential | `http://localhost:9002/callback/oidc` | `https://datahub.<FRONTEND_DOMAIN>/callback/oidc` | `*` |
| `minio` | OIDC | Confidential | `http://localhost:9001/oauth_callback` | `https://minio.<FRONTEND_DOMAIN>/oauth_callback` | `*` |

Additional notes:
- `minio` has protocol mapper `minio-policy` injecting claim `policy=consoleAdmin` into access/id/userinfo tokens.
- Discovery endpoint template: `<KEYCLOAK_BASE_URL>/realms/odp/.well-known/openid-configuration`.
- SAML clients: none discovered.

## 4) App Inventory (Web, API, Services)

### Web apps using Keycloak SSO
- Airflow UI: `http://localhost:8080` or `https://airflow.<FRONTEND_DOMAIN>`
- DataHub Frontend: `http://localhost:9002` (AKS redirect configured for `https://datahub.<FRONTEND_DOMAIN>`, but ingress route not present in `k8s/aks/frontend-ingress.yaml`)
- MinIO Console: `http://localhost:9001` or `https://minio.<FRONTEND_DOMAIN>`

### Related APIs / gateways
- Keycloak OIDC endpoints under `/realms/odp/protocol/openid-connect/*`
- DataHub GMS API: `http://localhost:8081` (manifest sets `METADATA_SERVICE_AUTH_ENABLED=false`)
- MinIO API: `http://localhost:9000` or `https://minio-api.<FRONTEND_DOMAIN>`

### Non-SSO services (for scope clarity)
- Platform frontend `portal` (`http://localhost:3000`) has no Keycloak integration discovered.
- Airflow scheduler/workers are not interactive login surfaces but rely on web auth for UI/API access.

### Environments discovered
- Local docker-compose
- Local kind (`k8s/dev`)
- AKS (`k8s/aks`) with public DNS/TLS

## 5) Users / Test Identities
- Discovered default user in realm import: `odp-admin` with password `${KEYCLOAK_DEFAULT_USER_PASSWORD}`.
- Required but not found in repo config:
  - `user_basic`
  - `user_admin`
  - `user_no_access`
  - MFA-enabled test identity

## 6) Expected Roles / Groups / Scopes Per App
- Airflow default FAB role mapping: env `AIRFLOW_OAUTH_DEFAULT_ROLE` (default `Admin`).
- Airflow OIDC scopes: `openid profile email`.
- MinIO expects claim `policy` (`consoleAdmin`) from Keycloak mapper.
- DataHub role/group expectation not explicitly declared in repo.
- Required role/group matrix for least-privilege validation is missing and must be provided or added to test realm.

## 7) Token Style
- OIDC discovered for all configured clients.
- `standardFlowEnabled=true` (authorization code flow enabled).
- `directAccessGrantsEnabled=true` (password grant enabled).
- Implicit flow not explicitly configured in realm template.
- SAML: not present.

## 8) Session Constraints
- Not explicitly configured in checked-in realm template/manifests:
  - SSO idle timeout
  - SSO max session lifespan
  - refresh token rotation policy
  - `offline_access` requirements
- These must be validated via Keycloak realm settings (Admin API/UI) in target env.

## 9) Trust & Security Posture Inputs
- TLS:
  - Local: HTTP for Keycloak/apps.
  - AKS ingress enforces TLS with Let's Encrypt cert (`frontend-tls`).
- Proxy headers: managed by nginx ingress; no explicit `X-Forwarded-*` override annotations in repo.
- Cookies:
  - SameSite/secure attributes are runtime-generated; must be browser-verified.
- CORS:
  - Client `webOrigins` currently `*` for all clients.
- CSP:
  - No explicit CSP policy settings found for Keycloak or dependent apps in manifests.

## Source Files Used
- `/Users/karelgoense/Documents/programming/FreshMinds_Programming/production/ai_trial/ops/keycloak/odp-realm.json`
- `/Users/karelgoense/Documents/programming/FreshMinds_Programming/production/ai_trial/docker-compose.yml`
- `/Users/karelgoense/Documents/programming/FreshMinds_Programming/production/ai_trial/.env.template`
- `/Users/karelgoense/Documents/programming/FreshMinds_Programming/production/ai_trial/airflow/webserver_config.py`
- `/Users/karelgoense/Documents/programming/FreshMinds_Programming/production/ai_trial/k8s/dev/keycloak.yaml`
- `/Users/karelgoense/Documents/programming/FreshMinds_Programming/production/ai_trial/k8s/aks/keycloak.yaml`
- `/Users/karelgoense/Documents/programming/FreshMinds_Programming/production/ai_trial/k8s/aks/frontend-ingress.yaml`
- `/Users/karelgoense/Documents/programming/FreshMinds_Programming/production/ai_trial/k8s/README.md`
