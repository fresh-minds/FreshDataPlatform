from pathlib import Path


def test_makefile_exposes_bootstrap_all_alias() -> None:
    makefile = (Path(__file__).resolve().parents[2] / "Makefile").read_text()
    assert "bootstrap_all: bootstrap-all" in makefile


def test_bootstrap_script_verifies_keycloak_resources() -> None:
    script = (Path(__file__).resolve().parents[2] / "scripts" / "bootstrap_all.sh").read_text()
    assert "verify_keycloak_resources.py" in script
    assert "--skip-keycloak-check" in script
