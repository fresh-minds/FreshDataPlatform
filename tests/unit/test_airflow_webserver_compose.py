from pathlib import Path


def _extract_service_block(compose_text: str, service_name: str) -> str:
    lines = compose_text.splitlines()
    start_token = f"  {service_name}:"

    start_index = next((idx for idx, line in enumerate(lines) if line.startswith(start_token)), None)
    if start_index is None:
        raise AssertionError(f"Service {service_name!r} not found in docker-compose.yml")

    end_index = len(lines)
    for idx in range(start_index + 1, len(lines)):
        line = lines[idx]
        if line.startswith("  ") and not line.startswith("    ") and line.endswith(":"):
            end_index = idx
            break

    return "\n".join(lines[start_index:end_index])


def test_airflow_webserver_sets_gunicorn_header_limit():
    compose_path = Path(__file__).resolve().parents[2] / "docker-compose.yml"
    service_block = _extract_service_block(compose_path.read_text(), "airflow-webserver")

    assert "GUNICORN_CMD_ARGS=--limit-request-field_size 32768" in service_block


def test_airflow_webserver_oauth_urls_split_browser_and_internal():
    compose_path = Path(__file__).resolve().parents[2] / "docker-compose.yml"
    service_block = _extract_service_block(compose_path.read_text(), "airflow-webserver")

    assert (
        "AIRFLOW_OAUTH_AUTHORIZE_URL=${KEYCLOAK_OIDC_BROWSER_AUTHORIZE_URL:-http://localhost:8090/realms/odp/protocol/openid-connect/auth}"
        in service_block
    )
    assert (
        "AIRFLOW_OAUTH_TOKEN_URL=${KEYCLOAK_OIDC_TOKEN_URL:-http://keycloak:8090/realms/odp/protocol/openid-connect/token}"
        in service_block
    )
